package backend_test

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"

	"github.com/vladsimachkov/tapegw/internal/backend"
	"github.com/vladsimachkov/tapegw/internal/config"
	"github.com/vladsimachkov/tapegw/internal/spool"
)

// M2 PR #1:PutObject / HeadObject / GetObject по дисковому буферу.
// Ленты ещё нет, поэтому весь набор — про online-путь и про то, что порядок
// «файл → commit» соблюдён.

// mustBucket создаёт бакет «b», который используют почти все тесты файла.
func mustBucket(t *testing.T, be *backend.TapeGW) {
	t.Helper()
	if err := be.CreateBucket(context.Background(),
		&s3.CreateBucketInput{Bucket: ptr("b")}, []byte(`{}`)); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
}

func putBody(t *testing.T, be *backend.TapeGW, bucket, key, body string) s3response.PutObjectOutput {
	t.Helper()
	in := s3response.PutObjectInput{
		Bucket:        ptr(bucket),
		Key:           ptr(key),
		Body:          strings.NewReader(body),
		ContentLength: ptr(int64(len(body))),
		ContentType:   ptr("text/plain"),
	}
	out, err := be.PutObject(context.Background(), in)
	if err != nil {
		t.Fatalf("PutObject(%s): %v", key, err)
	}
	return out
}

func TestPutGetObjectRoundTrip(t *testing.T) {
	be, _, _ := newBackend(t)
	ctx := context.Background()
	mustBucket(t, be)

	body := "сорок два символа для проверки ленты и хэша"
	out := putBody(t, be, "b", "dir/obj.txt", body)

	sum := md5.Sum([]byte(body))
	// ETag наружу в кавычках — так делает versitygw (backend.GenerateEtag).
	if want := fmt.Sprintf("%q", fmt.Sprintf("%x", sum)); out.ETag != want {
		t.Errorf("ETag = %s, хотим %s", out.ETag, want)
	}
	if out.Size == nil || *out.Size != int64(len(body)) {
		t.Errorf("Size = %v", out.Size)
	}

	get, err := be.GetObject(ctx, &s3.GetObjectInput{Bucket: ptr("b"), Key: ptr("dir/obj.txt")})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer get.Body.Close()

	got, err := io.ReadAll(get.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != body {
		t.Errorf("содержимое разошлось: %q", got)
	}
	if *get.ContentLength != int64(len(body)) {
		t.Errorf("ContentLength = %d", *get.ContentLength)
	}
	// ADR-0003: класс всегда GLACIER, даже если клиент просил другой.
	if get.StorageClass != types.StorageClassGlacier {
		t.Errorf("storage class = %v", get.StorageClass)
	}
	// Онлайновый объект не обязан врать про восстановление: у scoutfs тут
	// подставлена дата 2050 года, из-за чего обещание было ложным.
	if get.Restore != nil {
		t.Errorf("у онлайнового объекта не должно быть x-amz-restore, получили %q", *get.Restore)
	}
}

func TestHeadObject(t *testing.T) {
	be, _, _ := newBackend(t)
	ctx := context.Background()
	mustBucket(t, be)
	putBody(t, be, "b", "k", "12345")

	head, err := be.HeadObject(ctx, &s3.HeadObjectInput{Bucket: ptr("b"), Key: ptr("k")})
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if *head.ContentLength != 5 {
		t.Errorf("ContentLength = %d", *head.ContentLength)
	}
	if head.StorageClass != types.StorageClassGlacier {
		t.Errorf("storage class = %v", head.StorageClass)
	}
	if head.AcceptRanges == nil || *head.AcceptRanges != "bytes" {
		t.Errorf("AcceptRanges = %v", head.AcceptRanges)
	}
}

func TestGetMissingKey(t *testing.T) {
	be, _, _ := newBackend(t)
	ctx := context.Background()
	mustBucket(t, be)

	_, err := be.GetObject(ctx, &s3.GetObjectInput{Bucket: ptr("b"), Key: ptr("нет")})
	if !isS3Code(err, s3err.ErrNoSuchKey) {
		t.Errorf("ожидали NoSuchKey, получили %v", err)
	}
}

// Файл без строки в БД — то, что остаётся после падения между rename и commit.
// Он обязан быть невидимым: читаем по строке, значит GET отдаст NoSuchKey,
// а не содержимое или 500.
func TestOrphanFileIsInvisible(t *testing.T) {
	be, _, land := newBackend(t)
	ctx := context.Background()
	mustBucket(t, be)

	// Пишем файл в spuл напрямую, минуя транзакцию — ровно то состояние,
	// в котором окажется диск после kill -9 между шагом 1 и шагом 2 PutObject.
	orphan := uuid.New()
	if _, err := land.Write(orphan, strings.NewReader("никогда не будет прочитано"), spool.SumSpec{}, 0); err != nil {
		t.Fatalf("spool.Write: %v", err)
	}
	if !land.Exists(orphan) {
		t.Fatalf("файл не записан")
	}

	if _, err := be.GetObject(ctx, &s3.GetObjectInput{Bucket: ptr("b"), Key: ptr("любой")}); !isS3Code(err, s3err.ErrNoSuchKey) {
		t.Errorf("сирота не должен читаться, получили %v", err)
	}
}

func TestPutRejectsOversized(t *testing.T) {
	be, _, _ := newBackend(t)
	ctx := context.Background()
	mustBucket(t, be)

	// Отдельный бэкенд с маленьким лимитом: проверять EntityTooLarge на 100 GiB
	// было бы долго.
	be2, _, _ := newBackendWith(t, func(c *config.Config) { c.S3.MaxObjectSize = 8 })
	mustBucket(t, be2)

	// Отказ по заголовку — до чтения тела.
	_, err := be2.PutObject(ctx, s3response.PutObjectInput{
		Bucket: ptr("b"), Key: ptr("big"),
		Body: strings.NewReader("123456789"), ContentLength: ptr(int64(9)),
	})
	if !isS3Code(err, s3err.ErrEntityTooLarge) {
		t.Errorf("ожидали EntityTooLarge, получили %v", err)
	}

	// Врёт заголовок — ловит spuл на фактическом размере.
	_, err = be2.PutObject(ctx, s3response.PutObjectInput{
		Bucket: ptr("b"), Key: ptr("liar"),
		Body: strings.NewReader("123456789"), ContentLength: ptr(int64(1)),
	})
	if !isS3Code(err, s3err.ErrEntityTooLarge) {
		t.Errorf("расхождение заголовка и тела должно ловиться, получили %v", err)
	}
}

func TestPutRejectsForeignStorageClass(t *testing.T) {
	be, _, _ := newBackend(t)
	ctx := context.Background()
	mustBucket(t, be)

	// ADR-0003: чужой класс отвергается, а не игнорируется молча.
	for _, cls := range []types.StorageClass{types.StorageClassStandard, types.StorageClassIntelligentTiering} {
		_, err := be.PutObject(ctx, s3response.PutObjectInput{
			Bucket: ptr("b"), Key: ptr("k"), Body: strings.NewReader("x"),
			ContentLength: ptr(int64(1)), StorageClass: cls,
		})
		if !isS3Code(err, s3err.ErrNotImplemented) {
			t.Errorf("класс %v должны отвергать, получили %v", cls, err)
		}
	}

	// Свои классы проходят, и в ответе всё равно GLACIER.
	for _, cls := range []types.StorageClass{types.StorageClassGlacier, types.StorageClassDeepArchive} {
		if _, err := be.PutObject(ctx, s3response.PutObjectInput{
			Bucket: ptr("b"), Key: ptr("k"), Body: strings.NewReader("x"),
			ContentLength: ptr(int64(1)), StorageClass: cls,
		}); err != nil {
			t.Errorf("класс %v должны принимать: %v", cls, err)
		}
	}
}

func TestPutRejectsBadContentMD5(t *testing.T) {
	be, _, _ := newBackend(t)
	ctx := context.Background()
	mustBucket(t, be)

	wrong := md5.Sum([]byte("другие данные"))
	_, err := be.PutObject(ctx, s3response.PutObjectInput{
		Bucket: ptr("b"), Key: ptr("k"), Body: strings.NewReader("настоящие"),
		ContentLength: ptr(int64(len("настоящие"))),
		ContentMD5:    ptr(base64.StdEncoding.EncodeToString(wrong[:])),
	})
	if err == nil {
		t.Fatalf("ожидали ошибку несовпадения Content-MD5")
	}
	if !isS3Code(err, s3err.ErrBadDigest) {
		t.Errorf("ожидали BadDigest, получили %v", err)
	}

	// После отказа объекта быть не должно — ни строки, ни файла.
	if _, err := be.HeadObject(ctx, &s3.HeadObjectInput{Bucket: ptr("b"), Key: ptr("k")}); !isS3Code(err, s3err.ErrNoSuchKey) {
		t.Errorf("объект появился после неудачной записи")
	}
}

func TestPutAcceptsGoodContentMD5(t *testing.T) {
	be, _, _ := newBackend(t)
	ctx := context.Background()
	mustBucket(t, be)

	body := "данные"
	sum := md5.Sum([]byte(body))
	if _, err := be.PutObject(ctx, s3response.PutObjectInput{
		Bucket: ptr("b"), Key: ptr("k"), Body: strings.NewReader(body),
		ContentLength: ptr(int64(len(body))),
		ContentMD5:    ptr(base64.StdEncoding.EncodeToString(sum[:])),
	}); err != nil {
		t.Fatalf("корректный Content-MD5 должен проходить: %v", err)
	}
}

// Офлайновый объект (архивирован, копии на диске нет, restore не запрашивали)
// обязан отвечать InvalidObjectState, а не 500 и не пустым содержимым.
func TestGetOfflineObject(t *testing.T) {
	be, m, _ := newBackend(t)
	ctx := context.Background()
	mustBucket(t, be)
	putBody(t, be, "b", "k", "данные")

	// Переводим строку в «архивен, копии нет» — так это сделает janitor после
	// вытеснения (ADR-0004).
	if _, err := m.Pool.Exec(ctx,
		`UPDATE objects SET state='archived', landing=false, archived_at=now(),
		        online_until = now() - interval '1 hour'`); err != nil {
		t.Fatalf("update: %v", err)
	}

	if _, err := be.GetObject(ctx, &s3.GetObjectInput{Bucket: ptr("b"), Key: ptr("k")}); !isS3Code(err, s3err.ErrInvalidObjectState) {
		t.Errorf("GET офлайна: ожидали InvalidObjectState, получили %v", err)
	}
	if _, err := be.HeadObject(ctx, &s3.HeadObjectInput{Bucket: ptr("b"), Key: ptr("k")}); !isS3Code(err, s3err.ErrInvalidObjectState) {
		t.Errorf("HEAD офлайна: ожидали InvalidObjectState, получили %v", err)
	}
}

// Архивный объект с живой копией читается, и x-amz-restore показывает реальный
// срок, а не 2050 год.
func TestArchivedWithCopyIsReadable(t *testing.T) {
	be, m, _ := newBackend(t)
	ctx := context.Background()
	mustBucket(t, be)
	putBody(t, be, "b", "k", "данные")

	if _, err := m.Pool.Exec(ctx,
		`UPDATE objects SET state='archived', landing=true, archived_at=now(),
		        online_until = now() + interval '2 hours'`); err != nil {
		t.Fatalf("update: %v", err)
	}

	get, err := be.GetObject(ctx, &s3.GetObjectInput{Bucket: ptr("b"), Key: ptr("k")})
	if err != nil {
		t.Fatalf("архив с живой копией обязан читаться: %v", err)
	}
	defer get.Body.Close()
	if b, _ := io.ReadAll(get.Body); string(b) != "данные" {
		t.Errorf("содержимое разошлось: %q", b)
	}
}

// pending/archiving защищены состоянием, а не датой: migrator может стоять
// сколько угодно, объект с диска не исчезнет.
func TestPendingSurvivesExpiredDate(t *testing.T) {
	be, m, _ := newBackend(t)
	ctx := context.Background()
	mustBucket(t, be)
	putBody(t, be, "b", "k", "данные")

	if _, err := m.Pool.Exec(ctx,
		`UPDATE objects SET online_until = now() - interval '1 year'`); err != nil {
		t.Fatalf("update: %v", err)
	}

	if _, err := be.GetObject(ctx, &s3.GetObjectInput{Bucket: ptr("b"), Key: ptr("k")}); err != nil {
		t.Errorf("pending обязан читаться независимо от даты: %v", err)
	}
}

// x-amz-restore по ADR-0003: во время восстановления ongoing-request="true",
// после — реальный срок из restores.expires_at.
func TestRestoreHeader(t *testing.T) {
	be, m, _ := newBackend(t)
	ctx := context.Background()
	mustBucket(t, be)
	putBody(t, be, "b", "k", "данные")

	var id uuid.UUID
	if err := m.Pool.QueryRow(ctx, `SELECT id FROM objects LIMIT 1`).Scan(&id); err != nil {
		t.Fatalf("id: %v", err)
	}

	insert := func(state string, expires string) {
		t.Helper()
		if _, err := m.Pool.Exec(ctx, `TRUNCATE restores`); err != nil {
			t.Fatalf("truncate: %v", err)
		}
		var exp any
		if expires != "" {
			exp = expires
		}
		if _, err := m.Pool.Exec(ctx,
			`INSERT INTO restores (object_id, state, tier, days, requested_at, restored_at, expires_at)
			 VALUES ($1,$2,'Standard',7,now(), CASE WHEN $3::timestamptz IS NULL THEN NULL ELSE now() END, $3::timestamptz)`,
			id, state, exp); err != nil {
			t.Fatalf("insert restore: %v", err)
		}
	}

	insert("requested", "")
	head, err := be.HeadObject(ctx, &s3.HeadObjectInput{Bucket: ptr("b"), Key: ptr("k")})
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if head.Restore == nil || *head.Restore != `ongoing-request="true"` {
		t.Errorf("во время восстановления заголовок = %v", head.Restore)
	}

	insert("restored", "2030-01-02T03:04:05Z")
	head, err = be.HeadObject(ctx, &s3.HeadObjectInput{Bucket: ptr("b"), Key: ptr("k")})
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if head.Restore == nil || !strings.Contains(*head.Restore, `ongoing-request="false"`) {
		t.Fatalf("заголовок = %v", head.Restore)
	}
	// Дата обязана быть реальной. Фиктивная 2050 — тот самый баг, из-за которого
	// обещание доступности становилось ложным.
	if strings.Contains(*head.Restore, "2050") {
		t.Errorf("в заголовке снова 2050 год: %v", head.Restore)
	}
	if !strings.Contains(*head.Restore, "2030") {
		t.Errorf("ожидали срок из expires_at, получили %v", head.Restore)
	}
}

func TestGetObjectRange(t *testing.T) {
	be, _, _ := newBackend(t)
	ctx := context.Background()
	mustBucket(t, be)
	putBody(t, be, "b", "k", "0123456789")

	get, err := be.GetObject(ctx, &s3.GetObjectInput{
		Bucket: ptr("b"), Key: ptr("k"), Range: ptr("bytes=2-5"),
	})
	if err != nil {
		t.Fatalf("GetObject range: %v", err)
	}
	defer get.Body.Close()

	body, err := io.ReadAll(get.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(body) != "2345" {
		t.Errorf("range дал %q, хотели %q", body, "2345")
	}
	if *get.ContentLength != 4 {
		t.Errorf("ContentLength = %d, хотели 4", *get.ContentLength)
	}
	if get.ContentRange == nil || *get.ContentRange != "bytes 2-5/10" {
		t.Errorf("ContentRange = %v", get.ContentRange)
	}
}

// Невалидный Range трамбуем как versitygw: разбор в backend.ParseObjectRange
// на непонятный формат возвращает «весь объект без ошибки» — так делает и
// posix-бэкенд, и придумывать своё поведение здесь значит разойтись с AWS
// (правило 5). А вот неудовлетворимый диапазон — уже ошибка.
func TestGetObjectRangeSemantics(t *testing.T) {
	be, _, _ := newBackend(t)
	ctx := context.Background()
	mustBucket(t, be)
	putBody(t, be, "b", "k", "0123456789")

	// Мусор в Range — весь объект, не 500.
	get, err := be.GetObject(ctx, &s3.GetObjectInput{
		Bucket: ptr("b"), Key: ptr("k"), Range: ptr("bytes=abc"),
	})
	if err != nil {
		t.Fatalf("некорректный Range не должен быть ошибкой: %v", err)
	}
	body, _ := io.ReadAll(get.Body)
	get.Body.Close()
	if string(body) != "0123456789" {
		t.Errorf("при непонятном Range ожидали весь объект, получили %q", body)
	}

	// Диапазон за концом объекта — RequestedRangeNotSatisfiable.
	if _, err := be.GetObject(ctx, &s3.GetObjectInput{
		Bucket: ptr("b"), Key: ptr("k"), Range: ptr("bytes=100-200"),
	}); err == nil {
		t.Errorf("неудовлетворимый Range обязан быть ошибкой")
	}
}

// Перезапись через бэкенд: GET обязан отдать новое содержимое, а не старое.
func TestPutOverwriteThroughBackend(t *testing.T) {
	be, _, _ := newBackend(t)
	ctx := context.Background()
	mustBucket(t, be)
	putBody(t, be, "b", "k", "первое")
	putBody(t, be, "b", "k", "второе")

	get, err := be.GetObject(ctx, &s3.GetObjectInput{Bucket: ptr("b"), Key: ptr("k")})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer get.Body.Close()
	body, _ := io.ReadAll(get.Body)
	if string(body) != "второе" {
		t.Errorf("после перезаписи получили %q", body)
	}
}

// online_until обязан выставляться при PUT: NOT NULL в колонке не спасает от
// нуля, а нулевой срок означал бы «можно вытеснить немедленно».
func TestPutSetsOnlineUntil(t *testing.T) {
	be, m, _ := newBackend(t)
	ctx := context.Background()
	mustBucket(t, be)
	putBody(t, be, "b", "k", "x")

	var until time.Time
	if err := m.Pool.QueryRow(ctx, `SELECT online_until FROM objects`).Scan(&until); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if until.Before(time.Now().Add(time.Hour)) {
		t.Errorf("online_until = %v, ожидали в будущем на срок удержания", until)
	}
}
