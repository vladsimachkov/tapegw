package backend_test

// Приёмочные тесты multipart с шифрованием (T2 PR#2).
//
// Главный сюжет здесь — не «части склеились», а то, что склейка не сломала
// криптографию: Range должен попадать в сегменты внутри нужной части, порча в
// любой части обязана быть видна, а размер части обязан быть привязан к её
// шифротексту. Всё это проверяется на частях НЕровного размера: если бы части
// были кратны 64 KiB, граница части совпала бы с границей сегмента, и половина
// ошибок осталась бы невидимой.

import (
	"bytes"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	vbackend "github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3response"

	"github.com/vladsimachkov/tapegw/internal/backend"
	"github.com/vladsimachkov/tapegw/internal/encrypt"
	"github.com/vladsimachkov/tapegw/internal/meta"
)

// mpuSizes — набор размеров частей для всех тестов.
//
// Первая часть ровно MinPartSize (меньше нельзя, кроме последней), вторая —
// MinPartSize + 1000, чтобы её конец пришёлся на середину сегмента, третья —
// маленькая, как реальный хвост загрузки.
// Последняя часть — кратная размеру сегмента намеренно: только так проверяется
// привязка размера части к AAD (см. TestMultipartPartSizeBound).
var mpuSizes = []int{vbackend.MinPartSize, vbackend.MinPartSize + 1000, 2 * encrypt.SegmentSize}

// mpuData — содержимое всех частей одним куском: то, что обязан вернуть GET.
func mpuData() []byte {
	var all []byte
	for _, n := range mpuSizes {
		all = append(all, filler(n)...)
	}
	return all
}

// startMPU заводит загрузку.
func startMPU(t *testing.T, be *backend.TapeGW, bucket, key string) string {
	t.Helper()
	out, err := be.CreateMultipartUpload(testCtx(), s3response.CreateMultipartUploadInput{
		Bucket: ptr(bucket), Key: ptr(key),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	if out.UploadId == "" {
		t.Fatal("CreateMultipartUpload вернул пустой UploadId")
	}
	return out.UploadId
}

// uploadPart пишет одну часть и возвращает её ETag (в кавычках, как в ответе).
func uploadPart(t *testing.T, be *backend.TapeGW, bucket, key, uploadID string, number int, data []byte) string {
	t.Helper()
	out, err := be.UploadPart(testCtx(), &s3.UploadPartInput{
		Bucket:        ptr(bucket),
		Key:           ptr(key),
		UploadId:      ptr(uploadID),
		PartNumber:    ptr(int32(number)),
		Body:          bytes.NewReader(data),
		ContentLength: ptr(int64(len(data))),
	})
	if err != nil {
		t.Fatalf("UploadPart(%d): %v", number, err)
	}
	if out.ETag == nil || *out.ETag == "" {
		t.Fatalf("UploadPart(%d) без ETag", number)
	}
	return *out.ETag
}

// completeMPU завершает загрузку списком частей.
func completeMPU(t *testing.T, be *backend.TapeGW, bucket, key, uploadID string,
	etags []string,
) (s3response.CompleteMultipartUploadResult, string) {
	t.Helper()
	parts := make([]types.CompletedPart, 0, len(etags))
	for i, etag := range etags {
		parts = append(parts, types.CompletedPart{ETag: ptr(etag), PartNumber: ptr(int32(i + 1))})
	}
	res, versionID, err := be.CompleteMultipartUpload(testCtx(), &s3.CompleteMultipartUploadInput{
		Bucket:          ptr(bucket),
		Key:             ptr(key),
		UploadId:        ptr(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}
	return res, versionID
}

// fullMPU — полный цикл: загрузка, части, завершение.
func fullMPU(t *testing.T, be *backend.TapeGW, bucket, key string) (string, []byte) {
	t.Helper()
	uploadID := startMPU(t, be, bucket, key)
	data := mpuData()

	etags := make([]string, 0, len(mpuSizes))
	off := 0
	for i, n := range mpuSizes {
		etags = append(etags, uploadPart(t, be, bucket, key, uploadID, i+1, data[off:off+n]))
		off += n
	}
	res, _ := completeMPU(t, be, bucket, key, uploadID, etags)
	if res.ETag == nil {
		t.Fatal("Complete без ETag")
	}
	return *res.ETag, data
}

// Цикл целиком: собранный объект читается целиком и совпадает с исходником.
func TestMultipartRoundTrip(t *testing.T) {
	be, m, _ := newBackend(t)
	ctx := testCtx()
	mustBucket(t, be)

	etag, data := fullMPU(t, be, "b", "mp")

	// ETag обязан быть составным: md5-N.
	if !strings.HasSuffix(strings.Trim(etag, "\""), "-3") {
		t.Errorf("ETag %q не похож на составной md5-3", etag)
	}

	obj := currentObject(t, m, "b", "mp")
	if int(obj.Size) != len(data) {
		t.Errorf("размер %d, хотели %d", obj.Size, len(data))
	}
	parts, err := m.ObjectParts(ctx, obj.ID)
	if err != nil {
		t.Fatalf("ObjectParts: %v", err)
	}
	if len(parts) != len(mpuSizes) {
		t.Fatalf("частей %d, хотели %d", len(parts), len(mpuSizes))
	}
	var acc int64
	for i, p := range parts {
		if int(p.PlainSize) != mpuSizes[i] {
			t.Errorf("часть %d: размер %d, хотели %d", i+1, p.PlainSize, mpuSizes[i])
		}
		if p.CipherOffset != acc {
			t.Errorf("часть %d: смещение %d, хотели %d", i+1, p.CipherOffset, acc)
		}
		acc += encrypt.CipherSize(p.PlainSize)
	}

	get, err := be.GetObject(ctx, &s3.GetObjectInput{Bucket: ptr("b"), Key: ptr("mp")})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer get.Body.Close()
	got, err := io.ReadAll(get.Body)
	if err != nil {
		t.Fatalf("чтение тела: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("тело не совпало: %d байт против %d", len(got), len(data))
	}
}

// Range по собранному объекту: границы частей и границы сегментов.
//
// Тест обязан проваливаться, если смещение сегмента считать от начала файла, а
// не от начала части, или если номер части подставить не в nonce, а только в
// AAD. На частях кратных 64 KiB такая ошибка была бы невидима.
func TestMultipartRangeAcrossParts(t *testing.T) {
	be, _, _ := newBackend(t)
	ctx := testCtx()
	mustBucket(t, be)

	fullMPU(t, be, "b", "mp")
	data := mpuData()

	// Границы: концы частей и середина сегмента вокруг них.
	bounds := []int{0, 1, encrypt.SegmentSize - 1, encrypt.SegmentSize,
		encrypt.SegmentSize + 1}
	off := 0
	for _, n := range mpuSizes[:len(mpuSizes)-1] {
		off += n
		bounds = append(bounds, off-1, off, off+1)
	}
	bounds = append(bounds, len(data)-1)

	for _, start := range bounds {
		for _, ln := range []int{1, 5, 4096} {
			if start >= len(data) {
				continue
			}
			if start+ln > len(data) {
				ln = len(data) - start
			}
			spec := itoa(start) + "-" + itoa(start+ln-1)
			get, err := be.GetObject(ctx, &s3.GetObjectInput{
				Bucket: ptr("b"), Key: ptr("mp"), Range: ptr("bytes=" + spec),
			})
			if err != nil {
				t.Fatalf("range %s: %v", spec, err)
			}
			got, rerr := readBody(t, get.Body)
			get.Body.Close()
			if rerr != nil {
				t.Fatalf("range %s: чтение: %v", spec, rerr)
			}
			if !bytes.Equal(got, data[start:start+ln]) {
				t.Errorf("range %s: байты не совпали", spec)
			}
		}
	}
}

// Порча в любой части обязана быть видна.
//
// Проверяем по первому байту каждой части и по последнему байту файла: если бы
// части читались одной последовательностью без привязки к номеру, порча в
// середине файла могла бы «сдвинуться» и остаться незамеченной.
func TestMultipartCorruptionPerPart(t *testing.T) {
	data := mpuData()
	offsets := []int{0}
	off := 0
	for _, n := range mpuSizes {
		off += n
		offsets = append(offsets, off-1)
	}
	// Смещения в шифротексте: к каждому открытому смещению добавляется по
	// 16-байтному тегу на предшествующие сегменты.
	cipherOffsets := func(plain int) int {
		full := plain / encrypt.SegmentSize
		tail := 0
		if plain%encrypt.SegmentSize != 0 {
			tail = plain%encrypt.SegmentSize + encrypt.TagSize
		}
		return full*encrypt.CipherSegmentSize + tail
	}

	for _, pos := range offsets {
		t.Run(itoa(pos), func(t *testing.T) {
			be, m, land := newBackend(t)
			mustBucket(t, be)
			fullMPU(t, be, "b", "mp")

			obj := currentObject(t, m, "b", "mp")
			path := land.Path(obj.ID)
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("файл спула: %v", err)
			}
			at := cipherOffsets(pos)
			if at >= len(raw) {
				t.Fatalf("смещение %d вне файла (%d байт)", at, len(raw))
			}
			raw[at] ^= 0x01
			if err := os.WriteFile(path, raw, 0o600); err != nil {
				t.Fatalf("запись испорченного файла: %v", err)
			}

			get, err := be.GetObject(testCtx(), &s3.GetObjectInput{Bucket: ptr("b"), Key: ptr("mp")})
			if err != nil {
				t.Fatalf("GET метаданные не должен портить: %v", err)
			}
			defer get.Body.Close()
			if _, rerr := readBody(t, get.Body); rerr == nil {
				t.Errorf("порча в части на открытом смещении %d не обнаружена", pos)
			}
			_ = data
		})
	}
}

// Размер части в object_parts привязан к её шифротексту через AAD.
//
// Уменьшать надо ровно на сегмент и одновременно размер объекта. Иначе тест
// ловит не AAD, а более грубую проверку: на единицу — откажет конструктор
// читателя («части дают N, объявлено M»), а на неполный сегмент — сам GCM,
// потому что тег считается по всему сегменту.
//
// При ровной потере целого сегмента читатель видит только целые сегменты,
// каждый сам по себе корректен, и без привязки размера в AAD вернул бы усечённый
// объект как настоящий. Именно это и проверяет тест.
func TestMultipartPartSizeBound(t *testing.T) {
	be, m, _ := newBackend(t)
	ctx := testCtx()
	mustBucket(t, be)
	fullMPU(t, be, "b", "mp")

	obj := currentObject(t, m, "b", "mp")
	parts, err := m.ObjectParts(ctx, obj.ID)
	if err != nil {
		t.Fatalf("ObjectParts: %v", err)
	}
	last := parts[len(parts)-1]
	if last.PlainSize < 2*encrypt.SegmentSize {
		t.Fatalf("последняя часть %d байт: тест требует двух целых сегментов", last.PlainSize)
	}
	if _, err := m.Pool.Exec(ctx,
		`UPDATE object_parts SET plain_size = plain_size - $3
		  WHERE version_id = $1 AND part_number = $2`,
		obj.ID, last.Number, encrypt.SegmentSize); err != nil {
		t.Fatalf("уменьшить plain_size: %v", err)
	}
	// Размер объекта обязан уменьшен согласованно: иначе откажет сверка
	// «сумма частей против размера», а не криптография.
	if _, err := m.Pool.Exec(ctx,
		`UPDATE objects SET size = size - $2 WHERE id = $1`,
		obj.ID, encrypt.SegmentSize); err != nil {
		t.Fatalf("уменьшить size: %v", err)
	}

	get, err := be.GetObject(ctx, &s3.GetObjectInput{Bucket: ptr("b"), Key: ptr("mp")})
	if err != nil {
		return // отказ на уровне метаданных — тоже правильный ответ
	}
	defer get.Body.Close()
	got, rerr := readBody(t, get.Body)
	if rerr == nil {
		t.Errorf("усечённая часть принята молча: прочитано %d байт", len(got))
	}
}

// DEK один на загрузку, а после Complete привязан к UUID версии.
func TestMultipartSingleDEK(t *testing.T) {
	be, m, _ := newBackend(t)
	ctx := testCtx()
	mustBucket(t, be)

	uploadID := startMPU(t, be, "b", "mp")
	data := mpuData()
	etags := make([]string, 0, len(mpuSizes))
	off := 0
	for i, n := range mpuSizes {
		etags = append(etags, uploadPart(t, be, "b", "mp", uploadID, i+1, data[off:off+n]))
		off += n
	}

	up, err := m.GetUpload(ctx, meta.NewTenant(testProjectID), uploadID)
	if err != nil {
		t.Fatalf("GetUpload: %v", err)
	}
	// Все части зашифрованы одним DEK: проверить это напрямую нельзя, но можно
	// убедиться, что обёртка у загрузки ровно одна и она не пустая.
	if len(up.WrappedDEK) == 0 {
		t.Fatal("у загрузки нет обёртки DEK")
	}

	res, _ := completeMPU(t, be, "b", "mp", uploadID, etags)
	_ = res
	obj := currentObject(t, m, "b", "mp")

	// Обёртка объекта обязана отличаться от обёртки загрузки: переоборачивание
	// меняет AAD с upload_id на UUID версии. Если бы Complete просто перенёс
	// байты обёртки, чтение потом не развернуло бы ключ.
	if bytes.Equal(up.WrappedDEK, obj.WrappedDEK) {
		t.Errorf("обёртка DEK перенесена из загрузки без переобёртки")
	}
	// И загрузка обязана исчезнуть вместе с Complete.
	if _, err := m.GetUpload(ctx, meta.NewTenant(testProjectID), uploadID); !errors.Is(err, meta.ErrNotFound) {
		t.Errorf("загрузка пережила Complete: %v", err)
	}
}

// ETag собранного объекта считается по ETag частей — как в versitygw.
func TestMultipartEtag(t *testing.T) {
	be, _, _ := newBackend(t)
	mustBucket(t, be)

	uploadID := startMPU(t, be, "b", "mp")
	data := mpuData()
	etags := make([]string, 0, len(mpuSizes))
	off := 0
	for i, n := range mpuSizes {
		etags = append(etags, uploadPart(t, be, "b", "mp", uploadID, i+1, data[off:off+n]))
		off += n
	}

	want, err := vbackend.ComputeMultipartETagFromPartETags(uploadID,
		completedParts(etags))
	if err != nil {
		t.Fatalf("helper versitygw: %v", err)
	}
	got, _ := completeMPU(t, be, "b", "mp", uploadID, etags)
	if *got.ETag != want {
		t.Errorf("ETag %q, а helper versitygw даёт %q", *got.ETag, want)
	}
}

func completedParts(etags []string) []types.CompletedPart {
	out := make([]types.CompletedPart, 0, len(etags))
	for i, e := range etags {
		out = append(out, types.CompletedPart{ETag: ptr(e), PartNumber: ptr(int32(i + 1))})
	}
	return out
}

// Плохой список частей: не по порядку, несуществующая, чужой ETag, слишком
// маленькая не-последняя часть.
func TestCompleteRejectsBadPartList(t *testing.T) {
	// ETag четвёртой части — её подгружает один кейс, см. комментарий ниже.
	extraBigETag := ""

	cases := []struct {
		name  string
		parts func(etags []string) []types.CompletedPart
		code  string
	}{
		{"пусто", func([]string) []types.CompletedPart { return nil }, "MalformedXML"},
		{"не по порядку", func(e []string) []types.CompletedPart {
			return []types.CompletedPart{
				{ETag: e2ptr(e[1]), PartNumber: ptr(int32(2))},
				{ETag: e2ptr(e[0]), PartNumber: ptr(int32(1))},
			}
		}, "InvalidPartOrder"},
		{"нет такой части", func(e []string) []types.CompletedPart {
			return []types.CompletedPart{{ETag: e2ptr(e[0]), PartNumber: ptr(int32(9))}}
		}, "InvalidPart"},
		{"чужой ETag", func(e []string) []types.CompletedPart {
			return []types.CompletedPart{{ETag: ptr(`"deadbeef"`), PartNumber: ptr(int32(1))}}
		}, "InvalidPart"},
		// Маленькая часть не-последней. Нужна четвёртая часть: из трёх
		// загруженных маленькая (№3) стоит последней, а попытка поставить её
		// первой падает на порядке, а не на размере — это другой кейс.
		{"маленькая не-последняя", func(e []string) []types.CompletedPart {
			return []types.CompletedPart{
				{ETag: e2ptr(e[2]), PartNumber: ptr(int32(3))},
				{ETag: e2ptr(extraBigETag), PartNumber: ptr(int32(4))},
			}
		}, "EntityTooSmall"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			be, m, _ := newBackend(t)
			ctx := testCtx()
			mustBucket(t, be)
			uploadID := startMPU(t, be, "b", "mp")
			data := mpuData()
			etags := make([]string, 0, len(mpuSizes))
			off := 0
			for i, n := range mpuSizes {
				etags = append(etags, uploadPart(t, be, "b", "mp", uploadID, i+1, data[off:off+n]))
				off += n
			}
			if tc.name == "маленькая не-последняя" {
				extraBigETag = uploadPart(t, be, "b", "mp", uploadID, 4, filler(vbackend.MinPartSize))
			}

			_, _, err := be.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
				Bucket: ptr("b"), Key: ptr("b"), UploadId: ptr(uploadID),
				MultipartUpload: &types.CompletedMultipartUpload{Parts: tc.parts(etags)},
			})
			if !isS3CodeString(err, tc.code) {
				t.Errorf("ошибка %v, хотели %s", err, tc.code)
			}
			// Отказ обязан оставить загрузку живой: клиент вправе прислать
			// правильный список повторно.
			if _, gerr := m.GetUpload(ctx, meta.NewTenant(testProjectID), uploadID); gerr != nil {
				t.Errorf("загрузка пропала после отказа: %v", gerr)
			}
		})
	}
}

// Abort удаляет загрузку и файлы её частей.
func TestAbortRemovesPartFiles(t *testing.T) {
	be, m, land := newBackend(t)
	ctx := testCtx()
	mustBucket(t, be)
	uploadID := startMPU(t, be, "b", "mp")

	data := mpuData()
	off := 0
	var files []string
	for i, n := range mpuSizes {
		uploadPart(t, be, "b", "mp", uploadID, i+1, data[off:off+n])
		off += n
	}
	up, err := m.GetUpload(ctx, meta.NewTenant(testProjectID), uploadID)
	if err != nil {
		t.Fatalf("GetUpload: %v", err)
	}
	for _, p := range up.Parts {
		files = append(files, land.Path(p.FileID))
	}
	for _, f := range files {
		if _, err := os.Stat(f); err != nil {
			t.Fatalf("файл части %s должен быть на месте до Abort: %v", f, err)
		}
	}

	if err := be.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket: ptr("b"), Key: ptr("mp"), UploadId: ptr(uploadID),
	}); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	for _, f := range files {
		if _, err := os.Stat(f); !os.IsNotExist(err) {
			t.Errorf("файл части %s пережил Abort", f)
		}
	}
}

// UploadPartCopy перешифровывает копию DEKом загрузки.
//
// Если бы копия делалась клонированием файла источника, часть была бы
// зашифрована ключом исходного объекта, и собранный из таких частей объект
// читался бы только пока живёт тот ключ — то есть DEK объекта протекал бы в
// чужую загрузку.
func TestUploadPartCopyReencrypts(t *testing.T) {
	be, m, land := newBackend(t)
	ctx := testCtx()
	mustBucket(t, be)

	src := filler(vbackend.MinPartSize)
	putBytes(t, be, "b", "src", src)
	srcObj := currentObject(t, m, "b", "src")
	srcRaw, err := os.ReadFile(land.Path(srcObj.ID))
	if err != nil {
		t.Fatalf("файл источника: %v", err)
	}

	uploadID := startMPU(t, be, "b", "dst")
	res, err := be.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
		Bucket:     ptr("b"),
		Key:        ptr("dst"),
		UploadId:   ptr(uploadID),
		PartNumber: ptr(int32(1)),
		CopySource: ptr("b/src"),
	})
	if err != nil {
		t.Fatalf("UploadPartCopy: %v", err)
	}
	if res.ETag == nil {
		t.Fatal("UploadPartCopy без ETag")
	}

	up, err := m.GetUpload(ctx, meta.NewTenant(testProjectID), uploadID)
	if err != nil {
		t.Fatalf("GetUpload: %v", err)
	}
	partRaw, err := os.ReadFile(land.Path(up.Parts[0].FileID))
	if err != nil {
		t.Fatalf("файл части: %v", err)
	}
	if bytes.Equal(partRaw, srcRaw) {
		t.Errorf("часть — байтовая копия файла источника: перешифрования не было")
	}

	// И собранная из такой части версия читается: значит, DEK загрузки подошёл.
	if _, _, err := be.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: ptr("b"), Key: ptr("dst"), UploadId: ptr(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{
			{ETag: res.ETag, PartNumber: ptr(int32(1))},
		}},
	}); err != nil {
		t.Fatalf("Complete с копированной частью: %v", err)
	}
	get, err := be.GetObject(ctx, &s3.GetObjectInput{Bucket: ptr("b"), Key: ptr("dst")})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer get.Body.Close()
	got, err := readBody(t, get.Body)
	if err != nil {
		t.Fatalf("чтение: %v", err)
	}
	if !bytes.Equal(got, src) {
		t.Errorf("копия не совпала с источником: %d против %d байт", len(got), len(src))
	}
}

// Правило бакета по умолчанию применяется и к собранной из частей версии.
func TestMultipartLockAtComplete(t *testing.T) {
	be, m, _ := newBackend(t)
	mustBucket(t, be)
	enableLock(t, be, m, "b", govCfg)

	fullMPU(t, be, "b", "mp")
	obj := currentObject(t, m, "b", "mp")
	if obj.RetentionMode == nil || *obj.RetentionMode != "GOVERNANCE" {
		t.Errorf("retention не проставлен: %+v", obj.RetentionMode)
	}
	if obj.RetainUntil == nil {
		t.Error("retain_until пустой")
	}
}

// Список частей и загрузок виден только своему тенанту и показывает то, что
// реально загружено.
func TestMultipartListPartsAndUploads(t *testing.T) {
	be, _, _ := newBackend(t)
	ctx := testCtx()
	mustBucket(t, be)
	uploadID := startMPU(t, be, "b", "mp")
	data := mpuData()
	off := 0
	for i, n := range mpuSizes {
		uploadPart(t, be, "b", "mp", uploadID, i+1, data[off:off+n])
		off += n
	}

	parts, err := be.ListParts(ctx, &s3.ListPartsInput{
		Bucket: ptr("b"), Key: ptr("mp"), UploadId: ptr(uploadID),
	})
	if err != nil {
		t.Fatalf("ListParts: %v", err)
	}
	if len(parts.Parts) != len(mpuSizes) {
		t.Fatalf("частей %d, хотели %d", len(parts.Parts), len(mpuSizes))
	}
	for i, p := range parts.Parts {
		if p.PartNumber != i+1 {
			t.Errorf("часть %d по счёту имеет номер %d", i, p.PartNumber)
		}
		if int(p.Size) != mpuSizes[i] {
			t.Errorf("часть %d: размер %d, хотели %d", i+1, p.Size, mpuSizes[i])
		}
	}

	uploads, err := be.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
		Bucket: ptr("b"),
	})
	if err != nil {
		t.Fatalf("ListMultipartUploads: %v", err)
	}
	if len(uploads.Uploads) != 1 || uploads.Uploads[0].UploadID != uploadID {
		t.Errorf("загрузки бакета: %+v", uploads.Uploads)
	}

	// Маркер по номеру отсекает первые части.
	marker := itoa(1)
	tail, err := be.ListParts(ctx, &s3.ListPartsInput{
		Bucket: ptr("b"), Key: ptr("mp"), UploadId: ptr(uploadID),
		PartNumberMarker: &marker,
	})
	if err != nil {
		t.Fatalf("ListParts с маркером: %v", err)
	}
	if len(tail.Parts) != len(mpuSizes)-1 {
		t.Errorf("после маркера %d частей, хотели %d", len(tail.Parts), len(mpuSizes)-1)
	}
}

// UploadPart без объявленной длины не шифруется: размер входит в AAD.
func TestUploadPartRequiresContentLength(t *testing.T) {
	be, _, _ := newBackend(t)
	mustBucket(t, be)
	uploadID := startMPU(t, be, "b", "mp")

	_, err := be.UploadPart(testCtx(), &s3.UploadPartInput{
		Bucket: ptr("b"), Key: ptr("mp"), UploadId: ptr(uploadID),
		PartNumber: ptr(int32(1)), Body: strings.NewReader("abc"),
	})
	if !isS3CodeString(err, "MissingContentLength") {
		t.Errorf("ошибка %v, хотели MissingContentLength", err)
	}
}

// e2ptr — ETag из ответа уже в кавычках, а CompletedPart их терпит в любом
// виде: helper versitygw срезает.
func e2ptr(v string) *string { return aws.String(v) }

// Повторная загрузка части не повторяет nonce — на боевом пути.
//
// Форматный тест проверяет криптографию, этот — что соль действительно
// заводится на каждую попытку и доезжает до object_parts: если UploadPart
// переиспользует соль прежней попытки, собранный объект читаться будет (соль
// та же и там и там) и на этом тесте мутация пройдёт. Поэтому проверяем
// шифротексты частей и содержимое после Complete.
func TestMultipartReuploadPartDoesNotReuseNonce(t *testing.T) {
	be, m, land := newBackend(t)
	ctx := testCtx()
	mustBucket(t, be)
	uploadID := startMPU(t, be, "b", "mp")

	// Две попытки части №1: разный содержимый, ОДИНАКОВАЯ длина — именно так
	// выглядит атака на повтор nonce (XOR шифротекстов равен XOR открытых
	// текстов только при совпадении длины).
	first := filler(vbackend.MinPartSize)
	second := make([]byte, len(first))
	for i := range second {
		second[i] = first[i] ^ byte(i*13+1)
	}

	uploadPart(t, be, "b", "mp", uploadID, 1, first)
	up, err := m.GetUpload(ctx, meta.NewTenant(testProjectID), uploadID)
	if err != nil {
		t.Fatalf("GetUpload: %v", err)
	}
	firstFile := land.Path(up.Parts[0].FileID)
	firstRaw, err := os.ReadFile(firstFile)
	if err != nil {
		t.Fatalf("файл первой попытки: %v", err)
	}
	firstSalt := append([]byte(nil), up.Parts[0].Salt...)

	etag2 := uploadPart(t, be, "b", "mp", uploadID, 1, second)
	up, err = m.GetUpload(ctx, meta.NewTenant(testProjectID), uploadID)
	if err != nil {
		t.Fatalf("GetUpload после перезаписи: %v", err)
	}
	if len(up.Parts) != 1 {
		t.Fatalf("после перезаписи частей %d, хотели одну", len(up.Parts))
	}
	secondRaw, err := os.ReadFile(land.Path(up.Parts[0].FileID))
	if err != nil {
		t.Fatalf("файл второй попытки: %v", err)
	}

	// Соль обязана смениться: это и есть носитель уникальности nonce.
	if bytes.Equal(firstSalt, up.Parts[0].Salt) {
		t.Errorf("соль части не изменилась при перезаписи: nonce повторятся")
	}
	// А шифротексты — различаться. При повторе nonce XOR шифротекстов равен XOR
	// открытых текстов, и это прямое свидетельство утечки.
	if bytes.Equal(firstRaw, secondRaw) {
		t.Errorf("шифротексты двух попыток идентичны")
	}
	n := len(first)
	if len(secondRaw) < n || len(firstRaw) < n {
		t.Fatalf("файлы частей короче открытого текста")
	}
	xorCT := make([]byte, n)
	xorPT := make([]byte, n)
	for i := 0; i < n; i++ {
		xorCT[i] = firstRaw[i] ^ secondRaw[i]
		xorPT[i] = first[i] ^ second[i]
	}
	if bytes.Equal(xorCT, xorPT) {
		t.Errorf("XOR шифротекстов равен XOR открытых текстов: nonce повторились")
	}
	// Прежний файл части удалён: он больше нигде не участвует.
	if _, err := os.Stat(firstFile); !os.IsNotExist(err) {
		t.Errorf("файл первой попытки %s остался в спуле", firstFile)
	}

	// И собранный объект содержит данные ВТОРОЙ попытки.
	if _, _, err := be.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: ptr("b"), Key: ptr("mp"), UploadId: ptr(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{
			{ETag: ptr(etag2), PartNumber: ptr(int32(1))},
		}},
	}); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	get, err := be.GetObject(ctx, &s3.GetObjectInput{Bucket: ptr("b"), Key: ptr("mp")})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer get.Body.Close()
	got, err := readBody(t, get.Body)
	if err != nil {
		t.Fatalf("чтение: %v", err)
	}
	if !bytes.Equal(got, second) {
		t.Errorf("объект содержит не данные второй попытки (%d байт)", len(got))
	}
}

// У обычного PUT соль тоже своя и тоже доезжает до object_parts.
func TestPutStoresPartSalt(t *testing.T) {
	be, m, _ := newBackend(t)
	ctx := testCtx()
	mustBucket(t, be)
	putBytes(t, be, "b", "k", filler(1000))

	obj := currentObject(t, m, "b", "k")
	parts, err := m.ObjectParts(ctx, obj.ID)
	if err != nil {
		t.Fatalf("ObjectParts: %v", err)
	}
	if len(parts) != 1 {
		t.Fatalf("частей %d, хотели одну (обычный PUT — это одна часть)", len(parts))
	}
	if parts[0].Number != 1 {
		t.Errorf("номер части %d, хотели 1 (CHECK part_number >= 1)", parts[0].Number)
	}
	if len(parts[0].Salt) != encrypt.SaltSize {
		t.Errorf("соль %d байт, хотели %d", len(parts[0].Salt), encrypt.SaltSize)
	}
	if int(parts[0].PlainSize) != 1000 || parts[0].CipherOffset != 0 {
		t.Errorf("часть: %+v", parts[0])
	}
}
