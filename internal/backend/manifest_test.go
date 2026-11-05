package backend_test

// Тесты манифеста объекта (ADR-0019).
//
// Сюжет не в том, что «хвост дописался», а в трёх свойствах:
//
//   - манифест описывает ровно ту версию, которая легла в БД: те же части, те
//     же соли, та же блокировка. Иначе rebuild-meta поднял бы объект, который
//     отличается от исходного, и заметил бы это только при чтении;
//   - в открытом файле не осталось ничего, что называет объект: атрибуты Phobos
//     мы сознательно оставили без имён, и файл не должен возвращать их обратно
//     открытым текстом;
//   - хвост проверен так же строго, как данные: правка байта манифеста обязана
//     быть ошибкой аутентичности, а не «манифест не распарсился».

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"

	"github.com/versity/versitygw/s3response"

	"github.com/vladsimachkov/tapegw/internal/encrypt"
	"github.com/vladsimachkov/tapegw/internal/keys"
	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/spool"
)

// manifestOf — развернуть манифест версии из файла спула. Ровно то, что
// сделает rebuild-meta: файл, DEK версии, больше ничего.
func manifestOf(t *testing.T, m *meta.Meta, land *spool.Spool, obj *meta.Object) *encrypt.ManifestDoc {
	t.Helper()
	dek, err := keys.New(m, testKeyRing(t)).OpenVersionDEK(context.Background(),
		obj.TenantID, obj.WrappedDEK, obj.ID)
	if err != nil {
		t.Fatalf("DEK версии не развернулся: %v", err)
	}
	defer encrypt.Zero(dek)

	raw, err := os.ReadFile(land.Path(obj.ID))
	if err != nil {
		t.Fatalf("файл спула: %v", err)
	}
	doc, err := encrypt.OpenManifestAt(bytes.NewReader(raw), int64(len(raw)), dek, obj.ID)
	if err != nil {
		t.Fatalf("манифест не разобран: %v", err)
	}
	return doc
}

func TestPutManifestMatchesDB(t *testing.T) {
	be, m, land := newBackend(t)
	mustBucket(t, be)

	body := filler(1000)
	if _, err := be.PutObject(testCtx(), s3response.PutObjectInput{
		Bucket: ptr("b"), Key: ptr("вложенный/ключ.txt"),
		Body: bytes.NewReader(body), ContentLength: ptr(int64(len(body))),
		ContentType: ptr("text/plain; charset=utf-8"),
		Metadata:    map[string]string{"x-amz-meta-заказ": "42"},
	}); err != nil {
		t.Fatalf("PUT: %v", err)
	}

	obj := currentObject(t, m, "b", "вложенный/ключ.txt")
	doc := manifestOf(t, m, land, obj)

	if doc.Bucket != "b" || doc.Key != "вложенный/ключ.txt" {
		t.Errorf("манифест про другой объект: %s/%s", doc.Bucket, doc.Key)
	}
	if doc.ETag != obj.ETag {
		t.Errorf("ETag: манифест %q, база %q", doc.ETag, obj.ETag)
	}
	if doc.Size != obj.Size {
		t.Errorf("размер: манифест %d, база %d", doc.Size, obj.Size)
	}
	if doc.ContentType != "text/plain; charset=utf-8" {
		t.Errorf("content-type потерян: %q", doc.ContentType)
	}
	if doc.Meta["x-amz-meta-заказ"] != "42" {
		t.Errorf("пользовательские метаданные потеряны: %v", doc.Meta)
	}

	// Части обычного PUT: одна часть №1, её соль обязана быть той же, что в
	// object_parts, — иначе nonce сегментов не собрать.
	parts, err := m.ObjectParts(context.Background(), obj.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(doc.Parts) != len(parts) || len(parts) != 1 {
		t.Fatalf("частей %d в манифесте и %d в базе, ждали одну", len(doc.Parts), len(parts))
	}
	if doc.Parts[0].Number != uint32(parts[0].Number) || doc.Parts[0].Plain != parts[0].PlainSize ||
		doc.Parts[0].Off != parts[0].CipherOffset {
		t.Errorf("часть разъехалась: манифест %+v, база %+v", doc.Parts[0], parts[0])
	}
	if !bytes.Equal(doc.Parts[0].Salt, parts[0].Salt) {
		t.Error("соль части в манифесте не та: nonce сегментов не соберётся")
	}
}

func TestMultipartManifestMatchesParts(t *testing.T) {
	be, m, land := newBackend(t)
	mustBucket(t, be)

	uploadID, _ := fullMPU(t, be, "b", "big.bin")
	_ = uploadID

	obj := currentObject(t, m, "b", "big.bin")
	doc := manifestOf(t, m, land, obj)

	dbParts, err := m.ObjectParts(context.Background(), obj.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(doc.Parts) != len(dbParts) {
		t.Fatalf("частей %d в манифесте и %d в базе", len(doc.Parts), len(dbParts))
	}
	for i := range dbParts {
		mp, dp := doc.Parts[i], dbParts[i]
		if mp.Number != uint32(dp.Number) || mp.Plain != dp.PlainSize || mp.Off != dp.CipherOffset {
			t.Fatalf("часть %d разъехалась: манифест %+v, база %+v", i, mp, dp)
		}
		if !bytes.Equal(mp.Salt, dp.Salt) {
			t.Fatalf("соль части %d в манифесте не та", dp.Number)
		}
	}
	if doc.Size != obj.Size {
		t.Errorf("размер: манифест %d, база %d", doc.Size, obj.Size)
	}
	if doc.ETag != obj.ETag {
		t.Errorf("ETag: манифест %q, база %q", doc.ETag, obj.ETag)
	}
}

// TestManifestHidesNames — в открытом файле не должно быть ни ключа объекта, ни
// имени бакета. Смысл манифеста именно в этом: атрибуты Phobos оставили без
// имён, и файл не должен возвращать их обратно открытым текстом.
func TestManifestHidesNames(t *testing.T) {
	be, m, land := newBackend(t)
	mustBucket(t, be)

	body := filler(500)
	if _, err := be.PutObject(testCtx(), s3response.PutObjectInput{
		Bucket: ptr("b"), Key: ptr("договор-4.pdf"),
		Body: bytes.NewReader(body), ContentLength: ptr(int64(len(body))),
	}); err != nil {
		t.Fatalf("PUT: %v", err)
	}
	obj := currentObject(t, m, "b", "договор-4.pdf")
	raw, err := os.ReadFile(land.Path(obj.ID))
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"договор-4.pdf", "договор", "photos"} {
		if bytes.Contains(raw, []byte(secret)) {
			t.Errorf("в открытом файле нашёлось %q", secret)
		}
	}
}

// TestManifestTamperDetected — правка любого байта манифеста обязана быть
// ошибкой аутентичности. Это единственная защита таблицы частей: соль и
// смещения из подделанного манифеста дали бы «успешное» чтение мусора.
func TestManifestTamperDetected(t *testing.T) {
	be, m, land := newBackend(t)
	mustBucket(t, be)

	body := filler(700)
	if _, err := be.PutObject(testCtx(), s3response.PutObjectInput{
		Bucket: ptr("b"), Key: ptr("k"),
		Body: bytes.NewReader(body), ContentLength: ptr(int64(len(body))),
	}); err != nil {
		t.Fatalf("PUT: %v", err)
	}
	obj := currentObject(t, m, "b", "k")
	raw, err := os.ReadFile(land.Path(obj.ID))
	if err != nil {
		t.Fatal(err)
	}
	dek, err := keys.New(m, testKeyRing(t)).OpenVersionDEK(context.Background(),
		obj.TenantID, obj.WrappedDEK, obj.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer encrypt.Zero(dek)

	foot, err := encrypt.ParseFooter(raw[len(raw)-encrypt.FooterSize:])
	if err != nil {
		t.Fatal(err)
	}
	start := int64(len(raw)) - encrypt.FooterSize - int64(foot.CipherLen)

	for _, off := range []int64{0, int64(foot.CipherLen) / 2, int64(foot.CipherLen) - 1} {
		bad := append([]byte(nil), raw...)
		bad[start+off] ^= 0x01
		_, err := encrypt.OpenManifestAt(bytes.NewReader(bad), int64(len(bad)), dek, obj.ID)
		if !errors.Is(err, encrypt.ErrManifestAuth) {
			t.Errorf("правка байта манифеста +%d не поймана: %v", off, err)
		}
	}
}

// TestManifestLockMatchesDB — восстановленный объект обязан быть заперт так же,
// как исходный: иначе rebuild-meta выдал бы удаляемый объект там, где стояла
// блокировка. Правило бакета здесь не случайность: CompleteMultipartUpload не
// несёт заголовков Object Lock, и для multipart манифест может знать только
// правило бакета.
func TestManifestLockMatchesDB(t *testing.T) {
	be, m, land := newBackend(t)
	mustBucket(t, be)
	enableLock(t, be, m, "b", govCfg)

	body := filler(300)
	if _, err := be.PutObject(testCtx(), s3response.PutObjectInput{
		Bucket: ptr("b"), Key: ptr("locked.bin"),
		Body: bytes.NewReader(body), ContentLength: ptr(int64(len(body))),
	}); err != nil {
		t.Fatalf("PUT: %v", err)
	}

	obj := currentObject(t, m, "b", "locked.bin")
	if obj.RetentionMode == nil || *obj.RetentionMode != "GOVERNANCE" {
		t.Fatalf("тест настроен неверно: в базе режим %v", obj.RetentionMode)
	}
	doc := manifestOf(t, m, land, obj)
	if doc.Lock == nil {
		t.Fatal("блокировки в манифесте нет")
	}
	if doc.Lock.Mode != "GOVERNANCE" {
		t.Errorf("режим: %q", doc.Lock.Mode)
	}
	if doc.Lock.RetainUntil == "" {
		t.Error("дата блокировки потеряна")
	}
}
