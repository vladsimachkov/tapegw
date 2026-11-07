package backend_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/versity/versitygw/s3api/utils"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"

	"github.com/vladsimachkov/tapegw/internal/backend"
	"github.com/vladsimachkov/tapegw/internal/config"
	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/spool"
)

// M2 PR #1:PutObject / HeadObject / GetObject по дисковому буферу.
// Ленты ещё нет, поэтому весь набор — про online-путь и про то, что порядок
// «файл → commit» соблюдён.

// mustBucket создаёт бакет «b», который используют почти все тесты файла.
func mustBucket(t *testing.T, be *backend.TapeGW) {
	t.Helper()
	if err := be.CreateBucket(testCtx(),
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
	out, err := be.PutObject(testCtx(), in)
	if err != nil {
		t.Fatalf("PutObject(%s): %v", key, err)
	}
	return out
}

func TestPutGetObjectRoundTrip(t *testing.T) {
	be, _, _ := newBackend(t)
	ctx := testCtx()
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
	ctx := testCtx()
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
	ctx := testCtx()
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
	ctx := testCtx()
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
	ctx := testCtx()
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
	ctx := testCtx()
	mustBucket(t, be)

	// ADR-0003: чужой класс отвергается, а не игнорируется молча.
	for _, cls := range []types.StorageClass{types.StorageClassStandard, types.StorageClassIntelligentTiering} {
		_, err := be.PutObject(ctx, s3response.PutObjectInput{
			Bucket: ptr("b"), Key: ptr("k"), Body: strings.NewReader("x"),
			ContentLength: ptr(int64(1)), StorageClass: cls,
		})
		// AWS отвечает InvalidStorageClass (400), а не NotImplemented (501).
		// Код вне перечисления versitygw, поэтому сравниваем строку кода.
		if !isS3CodeString(err, "InvalidStorageClass") {
			t.Errorf("класс %v должны отвергать кодом InvalidStorageClass, получили %v", cls, err)
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
	ctx := testCtx()
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
	// GetChecksumBadDigestErr даёт код BadDigest с другим описанием, поэтому
	// сравниваем строку кода, а не структуру целиком.
	if !isS3CodeString(err, "BadDigest") {
		t.Errorf("ожидали BadDigest, получили %v", err)
	}

	// После отказа объекта быть не должно — ни строки, ни файла.
	if _, err := be.HeadObject(ctx, &s3.HeadObjectInput{Bucket: ptr("b"), Key: ptr("k")}); !isS3Code(err, s3err.ErrNoSuchKey) {
		t.Errorf("объект появился после неудачной записи")
	}
}

func TestPutAcceptsGoodContentMD5(t *testing.T) {
	be, _, _ := newBackend(t)
	ctx := testCtx()
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

// Офлайновый объект (архивирован, копии на диске нет, restore не запрашивали):
// GET обязан ответить InvalidObjectState, а HEAD — нет.
func TestGetOfflineObject(t *testing.T) {
	be, m, _ := newBackend(t)
	ctx := testCtx()
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
}

// Блокер B1, третий расклад GET и вытеснения: строку прочитали, когда landing
// ещё был true (и срок не наступил), а открыть файл пришли уже после того, как
// janitor снял флаг и сделал unlink. Open отдаёт ENOENT, и только перечитывание
// состояния (spool.evict_retries в objects.go) превращает это в
// InvalidObjectState, а не в 500.
//
// Окно создаётся детерминированно, без гонок по таймингам: шов бэкенда
// выполняет полный проход janitor ровно между чтением строки и spool.Open.
func TestGetEvictedBetweenReadAndOpen(t *testing.T) {
	be, m, land := newBackend(t)
	ctx := testCtx()
	mustBucket(t, be)
	putBody(t, be, "b", "k", "данные")

	var id uuid.UUID
	if err := m.Pool.QueryRow(ctx, `SELECT id FROM objects WHERE key='k'`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	// Читаемое состояние на момент первого чтения строки: archived, но копия
	// на месте и срок ещё не наступил — иначе GET ушёл бы в InvalidObjectState
	// до шва, и тест ничего не проверил бы.
	if _, err := m.Pool.Exec(ctx,
		`UPDATE objects SET state='archived', landing=true, archived_at=now(),
		        online_until = now() + interval '1 hour' WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	if !land.Exists(id) {
		t.Fatal("файла нет в landing — вытеснять нечего, тест ничего не проверит")
	}

	// Крючок: срок истёк между чтением строки и открытием файла, janitor
	// отработал. ClaimEvictBatch снимает флаг в транзакции и возвращается уже
	// после commit, unlink — вне транзакции (ADR-0004). Повтор прохода
	// идемпотентен: ноль строк и Remove без файла — не ошибка.
	var hookCalls int
	var claimed map[uuid.UUID]bool
	be.SetEvictHookForTest(func(uuid.UUID) {
		hookCalls++
		if _, err := m.Pool.Exec(ctx,
			`UPDATE objects SET online_until = now() - interval '1 hour' WHERE id = $1`, id); err != nil {
			t.Errorf("истечение срока в крючке: %v", err)
			return
		}
		cands, err := m.ClaimEvictBatch(ctx, 10)
		if err != nil {
			t.Errorf("ClaimEvictBatch: %v", err)
			return
		}
		claimed = map[uuid.UUID]bool{}
		for _, c := range cands {
			claimed[c.ID] = true
			if err := land.Remove(c.ID); err != nil {
				t.Errorf("unlink: %v", err)
			}
		}
	})

	_, err := be.GetObject(ctx, &s3.GetObjectInput{Bucket: ptr("b"), Key: ptr("k")})
	if !isS3Code(err, s3err.ErrInvalidObjectState) {
		t.Errorf("GET в окне вытеснения: %v, ждали InvalidObjectState, а не 500 и не ENOENT", err)
	}
	// Проверки окна: без них зелёный тест мог бы ничего не проверить —
	// отказ пришёл бы до шва.
	if hookCalls != 1 {
		t.Errorf("шов сработал %d раз(а), ждали ровно один: окно не создано", hookCalls)
	}
	if !claimed[id] {
		t.Errorf("крючок не вытеснил объект %v: claimed=%v", id, claimed)
	}
	if land.Exists(id) {
		t.Error("файл остался на диске — окно «открыли после unlink» не создано")
	}
}

// Blocker ревью PR #6: HEAD на вытесненный архивный объект обязан быть 200.
// Клиенты опрашивают ongoing-request именно по HEAD, и если HEAD отдаёт ошибку,
// весь сценарий restore для них сломан: объект «восстанавливается» выглядит
// как ошибка, а не как объект с ongoing-request="true".
func TestHeadOfflineObjectIs200(t *testing.T) {
	be, m, _ := newBackend(t)
	ctx := testCtx()
	mustBucket(t, be)
	putBody(t, be, "b", "k", "данные")

	if _, err := m.Pool.Exec(ctx,
		`UPDATE objects SET state='archived', landing=false, archived_at=now(),
		        online_until = now() - interval '1 hour'`); err != nil {
		t.Fatalf("update: %v", err)
	}

	head, err := be.HeadObject(ctx, &s3.HeadObjectInput{Bucket: ptr("b"), Key: ptr("k")})
	if err != nil {
		t.Fatalf("HEAD вытесненного объекта обязан быть 200, получили %v", err)
	}
	if head.StorageClass != types.StorageClassGlacier {
		t.Errorf("storage class = %v, хотели GLACIER", head.StorageClass)
	}
	// Запроса восстановления не было — заголовка быть не должно.
	if head.Restore != nil {
		t.Errorf("у объекта без запроса restore заголовка быть не должно, получили %q", *head.Restore)
	}
	want := int64(len("данные"))
	if head.ContentLength == nil || *head.ContentLength != want {
		t.Errorf("размер обязан отдаваться и для офлайна: %v, хотели %d", *head.ContentLength, want)
	}
}

// А вот во время восстановления HEAD обязан показать ongoing-request="true" —
// тот самый сценарий, ради которого HEAD и не должен ошибаться.
func TestHeadRestoringObjectShowsOngoing(t *testing.T) {
	be, m, _ := newBackend(t)
	ctx := testCtx()
	mustBucket(t, be)
	putBody(t, be, "b", "k", "данные")

	var id uuid.UUID
	if err := m.Pool.QueryRow(ctx, `SELECT id FROM objects LIMIT 1`).Scan(&id); err != nil {
		t.Fatalf("id: %v", err)
	}
	// Объект уже архивирован и вытеснен, и запрошен restore.
	if _, err := m.Pool.Exec(ctx,
		`UPDATE objects SET state='archived', landing=false, archived_at=now(),
		        online_until = now() - interval '1 hour'`); err != nil {
		t.Fatalf("update: %v", err)
	}
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO restores (object_id, state, tier, days, requested_at, tenant_id)
		 VALUES ($1,'requested','Standard',7,now(),$2)`, id, testTenantID); err != nil {
		t.Fatalf("insert restore: %v", err)
	}

	head, err := be.HeadObject(ctx, &s3.HeadObjectInput{Bucket: ptr("b"), Key: ptr("k")})
	if err != nil {
		t.Fatalf("HEAD восстанавливаемого объекта обязан быть 200, получили %v", err)
	}
	if head.Restore == nil || *head.Restore != `ongoing-request="true"` {
		t.Errorf("ожидали ongoing-request=\"true\", получили %v", head.Restore)
	}
}

// Архивный объект с живой копией читается, и x-amz-restore показывает реальный
// срок, а не 2050 год.
func TestArchivedWithCopyIsReadable(t *testing.T) {
	be, m, _ := newBackend(t)
	ctx := testCtx()
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
	ctx := testCtx()
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
	ctx := testCtx()
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
			`INSERT INTO restores (object_id, state, tier, days, requested_at, restored_at, expires_at, tenant_id)
			 VALUES ($1,$2,'Standard',7,now(), CASE WHEN $3::timestamptz IS NULL THEN NULL ELSE now() END, $3::timestamptz, $4)`,
			id, state, exp, testTenantID); err != nil {
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
	ctx := testCtx()
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
	ctx := testCtx()
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
	ctx := testCtx()
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
	ctx := testCtx()
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

// --- Условная запись (If-Match / If-None-Match) ---

func putWith(t *testing.T, be *backend.TapeGW, bucket, key, body string,
	ifMatch, ifNoneMatch *string) error {
	t.Helper()
	_, err := be.PutObject(testCtx(), s3response.PutObjectInput{
		Bucket: ptr(bucket), Key: ptr(key), Body: strings.NewReader(body),
		ContentLength: ptr(int64(len(body))),
		IfMatch:       ifMatch, IfNoneMatch: ifNoneMatch,
	})
	return err
}

// If-None-Match: * — «записать, только если ключа нет». На существующем ключе
// обязан быть PreconditionFailed, иначе молча затираются данные, ради защиты
// от которых этот заголовок и ставят.
func TestPutIfNoneMatchStar(t *testing.T) {
	be, _, _ := newBackend(t)
	ctx := testCtx()
	mustBucket(t, be)

	// На пустом месте проходит.
	if err := putWith(t, be, "b", "k", "первое", nil, ptr("*")); err != nil {
		t.Fatalf("первая запись с If-None-Match: * должна проходить: %v", err)
	}
	// Повтор — отказ.
	err := putWith(t, be, "b", "k", "второе", nil, ptr("*"))
	if !isS3Code(err, s3err.ErrPreconditionFailed) {
		t.Errorf("повтор с If-None-Match: * должны отклонять, получили %v", err)
	}
	// И данные не должны измениться.
	get, err := be.GetObject(ctx, &s3.GetObjectInput{Bucket: ptr("b"), Key: ptr("k")})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer get.Body.Close()
	body, _ := io.ReadAll(get.Body)
	if string(body) != "первое" {
		t.Errorf("отказанная запись всё равно затёрла данные: %q", body)
	}
}

// If-Match по совпавшему ETag проходит, по несовпавшему — PreconditionFailed.
func TestPutIfMatch(t *testing.T) {
	be, _, _ := newBackend(t)
	mustBucket(t, be)
	putBody(t, be, "b", "k", "первое")

	sum := md5.Sum([]byte("первое"))
	etag := fmt.Sprintf("%x", sum)

	if err := putWith(t, be, "b", "k", "второе", ptr(etag), nil); err != nil {
		t.Fatalf("If-Match по совпавшему ETag должен проходить: %v", err)
	}
	if err := putWith(t, be, "b", "k", "третье", ptr(etag), nil); !isS3Code(err, s3err.ErrPreconditionFailed) {
		t.Errorf("If-Match по устаревшему ETag должны отклонять, получили %v", err)
	}
	// If-Match на несуществующем ключе — NoSuchKey, так делает versitygw.
	if err := putWith(t, be, "b", "нет-такого", "x", ptr(etag), nil); !isS3Code(err, s3err.ErrNoSuchKey) {
		t.Errorf("If-Match на отсутствующем ключе: ожидали NoSuchKey, получили %v", err)
	}
}

// Оба заголовка сразу versitygw не поддерживает — это NotImplemented, а не
// «проигнорируем один из двух».
func TestPutBothConditionsNotImplemented(t *testing.T) {
	be, _, _ := newBackend(t)
	mustBucket(t, be)
	err := putWith(t, be, "b", "k", "x", ptr("abc"), ptr("*"))
	if !isS3Code(err, s3err.ErrNotImplemented) {
		t.Errorf("ожидали NotImplemented на оба заголовка, получили %v", err)
	}
}

// Гонка двух If-None-Match: * на один ключ. Ровно один должен победить:
// проверка внутри транзакции под advisory-локом, поэтому проигравший получает
// PreconditionFailed, а не «оба записали, и победил последний».
func TestPutIfNoneMatchRace(t *testing.T) {
	be, m, _ := newBackend(t)
	ctx := testCtx()
	mustBucket(t, be)

	const n = 12
	var wg sync.WaitGroup
	ok := make(chan struct{}, n)
	failed := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := putWith(t, be, "b", "hot", "кто-то один", nil, ptr("*")); err == nil {
				ok <- struct{}{}
			} else if isS3Code(err, s3err.ErrPreconditionFailed) {
				failed <- struct{}{}
			} else {
				t.Errorf("неожиданная ошибка в гонке: %v", err)
			}
		}()
	}
	wg.Wait()
	close(ok)
	close(failed)

	nOK, nFail := 0, 0
	for range ok {
		nOK++
	}
	for range failed {
		nFail++
	}
	if nOK != 1 {
		t.Errorf("победителей %d, должен быть ровно один", nOK)
	}
	if nFail != n-1 {
		t.Errorf("проигравших %d, ожидали %d", nFail, n-1)
	}

	var cur int
	if err := m.Pool.QueryRow(ctx,
		`SELECT count(*) FROM objects WHERE key='hot' AND is_current`).Scan(&cur); err != nil {
		t.Fatalf("count: %v", err)
	}
	if cur != 1 {
		t.Errorf("текущих строк %d, должна быть одна", cur)
	}
}

// --- Условные заголовки чтения (Minor ревью PR #6) ---

// Контроллер versitygw их не вычисляет, решает бэкенд. Проверяем и 304, и 412.
func TestGetObjectPreconditions(t *testing.T) {
	be, _, _ := newBackend(t)
	ctx := testCtx()
	mustBucket(t, be)
	putBody(t, be, "b", "k", "данные")

	sum := md5.Sum([]byte("данные"))
	etag := fmt.Sprintf("%x", sum)

	// If-None-Match по совпавшему ETag — 304 Not Modified.
	_, err := be.GetObject(ctx, &s3.GetObjectInput{
		Bucket: ptr("b"), Key: ptr("k"), IfNoneMatch: ptr(etag),
	})
	if !isS3Code(err, s3err.ErrNotModified) {
		t.Errorf("If-None-Match по совпавшему ETag: ожидали 304 NotModified, получили %v", err)
	}

	// If-Match по несовпавшему ETag — 412 Precondition Failed.
	_, err = be.GetObject(ctx, &s3.GetObjectInput{
		Bucket: ptr("b"), Key: ptr("k"), IfMatch: ptr("не-тот-etag"),
	})
	if !isS3Code(err, s3err.ErrPreconditionFailed) {
		t.Errorf("If-Match по несовпавшему ETag: ожидали PreconditionFailed, получили %v", err)
	}

	// If-Match по совпавшему — читается нормально.
	get, err := be.GetObject(ctx, &s3.GetObjectInput{
		Bucket: ptr("b"), Key: ptr("k"), IfMatch: ptr(etag),
	})
	if err != nil {
		t.Fatalf("If-Match по совпавшему ETag должен читаться: %v", err)
	}
	get.Body.Close()

	// If-Unmodified-Since в прошлом — объект новее, 412.
	old := time.Now().Add(-time.Hour)
	if _, err := be.GetObject(ctx, &s3.GetObjectInput{
		Bucket: ptr("b"), Key: ptr("k"), IfUnmodifiedSince: ptr(old),
	}); !isS3Code(err, s3err.ErrPreconditionFailed) {
		t.Errorf("If-Unmodified-Since в прошлом: ожидали PreconditionFailed, получили %v", err)
	}

	// If-Modified-Since в будущем — новее нет ничего, 304.
	future := time.Now().Add(time.Hour)
	if _, err := be.GetObject(ctx, &s3.GetObjectInput{
		Bucket: ptr("b"), Key: ptr("k"), IfModifiedSince: ptr(future),
	}); !isS3Code(err, s3err.ErrNotModified) {
		t.Errorf("If-Modified-Since в будущем: ожидали NotModified, получили %v", err)
	}
}

// --- versionId (Minor ревью PR #6) ---

// Версионирования нет. versionId, отличный от null, обязан быть ошибкой:
// иначе клиент, который просит конкретную версию, молча получил бы текущую.
func TestVersionIDUnknownRejected(t *testing.T) {
	be, _, _ := newBackend(t)
	ctx := testCtx()
	mustBucket(t, be)
	putBody(t, be, "b", "k", "данные")

	// Несуществующая версия — NoSuchVersion. Отдать текущую означало бы
	// «клиент ищет одну версию, получил другую и молчит».
	for _, vid := range []string{"v1", "0002", "junk", uuid.NewString()} {
		if _, err := be.GetObject(ctx, &s3.GetObjectInput{
			Bucket: ptr("b"), Key: ptr("k"), VersionId: ptr(vid),
		}); !isS3Code(err, s3err.ErrNoSuchVersion) {
			t.Errorf("GET с versionId=%q: ожидали NoSuchVersion, получили %v", vid, err)
		}
		if _, err := be.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: ptr("b"), Key: ptr("k"), VersionId: ptr(vid),
		}); !isS3Code(err, s3err.ErrNoSuchVersion) {
			t.Errorf("HEAD с versionId=%q: ожидали NoSuchVersion, получили %v", vid, err)
		}
	}

	// "null" — единственная версия в бакете без versioning, обязана читаться.
	if _, err := be.GetObject(ctx, &s3.GetObjectInput{
		Bucket: ptr("b"), Key: ptr("k"), VersionId: ptr("null"),
	}); err != nil {
		t.Errorf("versionId=null должен читаться: %v", err)
	}
}

// setVersioning — состояние версионирования бакета.
func setVersioningDB(t *testing.T, m *meta.Meta, bucket, state string) {
	t.Helper()
	if _, err := m.Pool.Exec(context.Background(),
		"UPDATE buckets SET versioning = $2 WHERE name = $1", bucket, state); err != nil {
		t.Fatalf("versioning %s: %v", state, err)
	}
}

// Адресация версий при чтении: конкретная версия читается по своему id,
// "null" — это null-версия, а не «текущая», и x-amz-version-id в ответе
// соответствует состоянию бакета.
func TestReadByVersionID(t *testing.T) {
	be, m, _ := newBackend(t)
	ctx := testCtx()
	mustBucket(t, be)
	setVersioningDB(t, m, "b", meta.VersioningEnabled)

	// null-версия рядом с обычной: она записана ДО включения версий.
	setVersioningDB(t, m, "b", meta.VersioningUnversioned)
	putBody(t, be, "b", "null-версия", "старые данные")
	setVersioningDB(t, m, "b", meta.VersioningEnabled)
	putBody(t, be, "b", "null-версия", "новые данные")

	// Обычная версия с двумя поколениями: старая остаётся нетекущей.
	putBody(t, be, "b", "k", "первая")
	oldID := mustCurrentID(t, m, "b", "k")
	putBody(t, be, "b", "k", "вторая версия")

	// Нетекущая версия читается по id и отдаёт именно свои байты.
	g, err := be.GetObject(ctx, &s3.GetObjectInput{Bucket: ptr("b"), Key: ptr("k"),
		VersionId: ptr(oldID)})
	if err != nil {
		t.Fatalf("GET по id нетекущей версии: %v", err)
	}
	if got := readAll(t, g); got != "первая" {
		t.Errorf("нетекущая версия отдала %q, хотели «первая»", got)
	}
	if aws.ToString(g.VersionId) != oldID {
		t.Errorf("x-amz-version-id = %q, хотели id запрошенной версии", aws.ToString(g.VersionId))
	}

	// Текущая версия — другие данные, и заголовок показывает её id.
	cur := mustCurrentID(t, m, "b", "k")
	g, err = be.GetObject(ctx, &s3.GetObjectInput{Bucket: ptr("b"), Key: ptr("k")})
	if err != nil {
		t.Fatalf("GET текущей версии: %v", err)
	}
	if got := readAll(t, g); got != "вторая версия" {
		t.Errorf("текущая версия отдала %q", got)
	}
	if aws.ToString(g.VersionId) != cur {
		t.Errorf("x-amz-version-id = %q, хотели id текущей версии %s",
			aws.ToString(g.VersionId), cur)
	}

	// versionId=null — именно null-версия, а не текущая. Это главный смысл
	// теста: раньше null игнорировался, и клиент получал «новые данные» молча.
	g, err = be.GetObject(ctx, &s3.GetObjectInput{Bucket: ptr("b"), Key: ptr("null-версия"),
		VersionId: ptr("null")})
	if err != nil {
		t.Fatalf("GET versionId=null: %v", err)
	}
	if got := readAll(t, g); got != "старые данные" {
		t.Errorf("versionId=null отдала %q, хотели «старые данные» — null проигнорирован", got)
	}
	if aws.ToString(g.VersionId) != "null" {
		t.Errorf("x-amz-version-id = %q, хотели литеральный null", aws.ToString(g.VersionId))
	}

	// HEAD по той же схеме.
	h, err := be.HeadObject(ctx, &s3.HeadObjectInput{Bucket: ptr("b"), Key: ptr("k"),
		VersionId: ptr(oldID)})
	if err != nil {
		t.Fatalf("HEAD по id версии: %v", err)
	}
	if aws.ToInt64(h.ContentLength) != int64(len("первая")) {
		t.Errorf("HEAD нетекущей версии вернул размер %d", aws.ToInt64(h.ContentLength))
	}
	if aws.ToString(h.VersionId) != oldID {
		t.Errorf("HEAD x-amz-version-id = %q, хотели %s", aws.ToString(h.VersionId), oldID)
	}
}

// Три состояния бакета определяют, какой x-amz-version-id в ответе: для
// unversioned заголовка нет, для suspended — "null", для enabled — id.
func TestReadVersionIDHeaderByBucketState(t *testing.T) {
	for _, tc := range []struct{ state, want string }{
		{meta.VersioningUnversioned, ""},
		{meta.VersioningSuspended, "null"},
		{meta.VersioningEnabled, "id"},
	} {
		t.Run(tc.state, func(t *testing.T) {
			be, m, _ := newBackend(t)
			ctx := testCtx()
			mustBucket(t, be)
			setVersioningDB(t, m, "b", tc.state)
			putBody(t, be, "b", "k", "данные")

			h, err := be.HeadObject(ctx, &s3.HeadObjectInput{Bucket: ptr("b"), Key: ptr("k")})
			if err != nil {
				t.Fatalf("HEAD: %v", err)
			}
			got := aws.ToString(h.VersionId)
			if tc.want == "" && got != "" {
				t.Errorf("unversioned: заголовка быть не должно, получили %q", got)
			}
			if tc.want == "null" && got != "null" {
				t.Errorf("suspended: x-amz-version-id = %q, хотели null", got)
			}
			if tc.want == "id" && len(got) != 36 {
				t.Errorf("enabled: x-amz-version-id = %q, хотели UUID", got)
			}
		})
	}
}

// Маркер удаления на чтении. Без versionId ключ выглядит удалённым — 404 с
// заголовками маркера; по id самого маркера — 405, потому что тела у него нет.
func TestReadDeleteMarker(t *testing.T) {
	be, m, _ := newBackend(t)
	ctx := testCtx()
	mustBucket(t, be)
	setVersioningDB(t, m, "b", meta.VersioningEnabled)
	putBody(t, be, "b", "k", "данные")
	dataID := mustCurrentID(t, m, "b", "k")

	if _, err := be.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: ptr("b"), Key: ptr("k")}); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	markerID := mustCurrentID(t, m, "b", "k")
	if markerID == dataID {
		t.Fatalf("маркер должен быть отдельной версией")
	}

	// Без versionId: 404 и заголовки маркера.
	_, err := be.GetObject(ctx, &s3.GetObjectInput{Bucket: ptr("b"), Key: ptr("k")})
	if !isS3Code(err, s3err.ErrNoSuchKey) {
		t.Errorf("GET по ключу под маркером: ожидали NoSuchKey, получили %v", err)
	}
	_, err = be.HeadObject(ctx, &s3.HeadObjectInput{Bucket: ptr("b"), Key: ptr("k")})
	if !isS3Code(err, s3err.ErrNoSuchKey) {
		t.Errorf("HEAD по ключу под маркером: ожидали NoSuchKey, получили %v", err)
	}

	// По id маркера: 405.
	if _, err := be.GetObject(ctx, &s3.GetObjectInput{Bucket: ptr("b"), Key: ptr("k"),
		VersionId: ptr(markerID)}); !isS3Code(err, s3err.ErrMethodNotAllowed) {
		t.Errorf("GET по id маркера: ожидали MethodNotAllowed, получили %v", err)
	}
	if _, err := be.HeadObject(ctx, &s3.HeadObjectInput{Bucket: ptr("b"), Key: ptr("k"),
		VersionId: ptr(markerID)}); !isS3Code(err, s3err.ErrMethodNotAllowed) {
		t.Errorf("HEAD по id маркера: ожидали MethodNotAllowed, получили %v", err)
	}

	// Данные под маркером живы и читаются по своему id — удаление без
	// versionId к данным не прикоснулось.
	g, err := be.GetObject(ctx, &s3.GetObjectInput{Bucket: ptr("b"), Key: ptr("k"),
		VersionId: ptr(dataID)})
	if err != nil {
		t.Fatalf("GET версии под маркером: %v", err)
	}
	if got := readAll(t, g); got != "данные" {
		t.Errorf("версия под маркером отдала %q", got)
	}

	// Удаление самого маркера — штатный способ S3 вернуть объект.
	if _, err := be.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: ptr("b"), Key: ptr("k"),
		VersionId: ptr(markerID)}); err != nil {
		t.Fatalf("удаление маркера по versionId: %v", err)
	}
	g, err = be.GetObject(ctx, &s3.GetObjectInput{Bucket: ptr("b"), Key: ptr("k")})
	if err != nil {
		t.Fatalf("GET после удаления маркера: %v", err)
	}
	if got := readAll(t, g); got != "данные" {
		t.Errorf("после удаления маркера GET отдал %q, хотели «данные»", got)
	}
	if aws.ToString(g.VersionId) != dataID {
		t.Errorf("x-amz-version-id = %q, хотели id восстановленной версии %s",
			aws.ToString(g.VersionId), dataID)
	}
}

// mustCurrentID — id текущей версии ключа.
func mustCurrentID(t *testing.T, m *meta.Meta, bucket, key string) string {
	t.Helper()
	var id string
	err := m.Pool.QueryRow(context.Background(),
		`SELECT o.id::text FROM objects o JOIN buckets b ON b.id = o.bucket_id
		  WHERE b.name = $1 AND o.key = $2 AND o.is_current`, bucket, key).Scan(&id)
	if err != nil {
		t.Fatalf("текущая версия %s/%s: %v", bucket, key, err)
	}
	return id
}

// readAll прочитывает тело ответа целиком.
func readAll(t *testing.T, out *s3.GetObjectOutput) string {
	t.Helper()
	if out.Body == nil {
		t.Fatalf("пустое тело ответа")
	}
	defer out.Body.Close()
	b, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatalf("чтение тела: %v", err)
	}
	return string(b)
}

// --- Trailing checksums (Major ревью PR #6) ---

// checksumReaderFake имитирует то, во что versitygw превращает тело запроса с
// трейлером aws-chunked: middlewares.ChecksumReader, который после чтения
// отдаёт Algorithm() и Checksum(). Заставить сам SDK пойти этим путём из теста
// нельзя — для расчёта суммы он требует seekable тело, а aws-chunked включает
// только для не-seekable. Поэтому проверяем свою ветку напрямую.
type checksumReaderFake struct {
	r    io.Reader
	algo string
	sum  string
}

func (f *checksumReaderFake) Read(p []byte) (int, error) { return f.r.Read(p) }
func (f *checksumReaderFake) Algorithm() string          { return f.algo }
func (f *checksumReaderFake) Checksum() string           { return f.sum }

// Трейлер объявляет алгоритм, но значения в заголовке нет — сумма обязана
// сохраниться вычисленной и вернуться в ответе.
func TestPutTrailingChecksum(t *testing.T) {
	be, m, _ := newBackend(t)
	ctx := testCtx()
	mustBucket(t, be)

	body := "тело с trailing crc32"
	// Значение, которое вернул бы чанк-ридер: тот же CRC32 в base64.
	hr, err := utils.NewHashReader(bytes.NewReader([]byte(body)), "", utils.HashTypeCRC32)
	if err != nil {
		t.Fatalf("NewHashReader: %v", err)
	}
	if _, err := io.ReadAll(hr); err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	out, err := be.PutObject(ctx, s3response.PutObjectInput{
		Bucket:        ptr("b"),
		Key:           ptr("k"),
		Body:          &checksumReaderFake{r: strings.NewReader(body), algo: "crc32", sum: hr.Sum()},
		ContentLength: ptr(int64(len(body))),
	})
	if err != nil {
		t.Fatalf("PutObject с trailing checksum: %v", err)
	}
	if aws.ToString(out.ChecksumCRC32) != hr.Sum() {
		t.Errorf("Put вернул crc32=%q, ожидали %q", aws.ToString(out.ChecksumCRC32), hr.Sum())
	}

	// И на HEAD она обязана вернуться: без этой ветки сумма терялась.
	head, err := be.HeadObject(ctx, &s3.HeadObjectInput{Bucket: ptr("b"), Key: ptr("k")})
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if aws.ToString(head.ChecksumCRC32) != hr.Sum() {
		t.Errorf("HEAD вернул crc32=%q, ожидали %q", aws.ToString(head.ChecksumCRC32), hr.Sum())
	}

	// И в базе она лежит, иначе на следующем запросе взять её было бы неоткуда.
	var stored map[string]string
	if err := m.Pool.QueryRow(ctx, `SELECT checksums FROM objects`).Scan(&stored); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if stored["CRC32"] != hr.Sum() {
		t.Errorf("в базе checksums=%v, ожидали CRC32=%q", stored, hr.Sum())
	}
}

// Если наша сумма не совпала со значением из трейлера — это BadDigest, а не
// молчаливая запись расходящихся данных.
func TestPutTrailingChecksumMismatch(t *testing.T) {
	be, _, _ := newBackend(t)
	mustBucket(t, be)

	body := "тело с trailing crc32"
	_, err := be.PutObject(testCtx(), s3response.PutObjectInput{
		Bucket:        ptr("b"),
		Key:           ptr("k"),
		Body:          &checksumReaderFake{r: strings.NewReader(body), algo: "crc32", sum: "////"},
		ContentLength: ptr(int64(len(body))),
	})
	if err == nil {
		t.Fatalf("ожидали отказ при расхождении нашей суммы и трейлера")
	}
	// GetChecksumBadDigestErr даёт код BadDigest с другим описанием, поэтому
	// сравниваем строку кода, а не структуру целиком.
	if !isS3CodeString(err, "BadDigest") {
		t.Errorf("ожидали BadDigest, получили %v", err)
	}
}
