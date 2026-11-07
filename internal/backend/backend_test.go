package backend_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"crypto/sha256"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"

	"github.com/vladsimachkov/tapegw/internal/backend"
	"github.com/vladsimachkov/tapegw/internal/config"
	"github.com/vladsimachkov/tapegw/internal/iam"
	"github.com/vladsimachkov/tapegw/internal/keys"
	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/spool"
	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// Критерий готовности M1 — «aws s3 mb / ls / rb работают против tapegw».
// Здесь проверяется слой, на который эти команды опираются: бакетные операции
// бэкенда. Сквозной прогон живым aws cli — test/e2e (M2+).

// newBackend поднимает бэкенд поверх тестовой базы и временного landing-буфера.
// meta возвращается отдельно: часть тестов готовит данные напрямую через SQL,
// минуя бэкенд.
func newBackend(t *testing.T) (*backend.TapeGW, *meta.Meta, *spool.Spool) {
	t.Helper()
	return newBackendWith(t, nil)
}

// newBackendWith то же самое, но конфиг можно поправить до сборки бэкенда —
// например уменьшить s3.max_object_size, чтобы проверить EntityTooLarge.
func newBackendWith(t *testing.T, mutate func(*config.Config)) (*backend.TapeGW, *meta.Meta, *spool.Spool) {
	t.Helper()
	be, m, land, _ := newBackendFull(t, mutate)
	return be, m, land
}

// newBackendFull — тот же бэкенд, но возвращает и cache-буфер: тесты S5
// кладут в него восстановленную копию и проверяют, что GET читает оттуда.
func newBackendFull(t *testing.T, mutate func(*config.Config)) (*backend.TapeGW, *meta.Meta, *spool.Spool, *spool.Spool) {
	t.Helper()
	m := testdb.Connect(t)
	testdb.Clean(t, m)

	cfg := config.Default()
	if mutate != nil {
		mutate(cfg)
	}
	// Буфер в каталоге теста: go test удаляет его сам, а проверка st_dev внутри
	// spool.Open на реальном ФС всё равно проходит.
	land, err := spool.Open(t.TempDir())
	if err != nil {
		t.Fatalf("spool.Open: %v", err)
	}
	cache, err := spool.Open(t.TempDir())
	if err != nil {
		t.Fatalf("spool.Open(cache): %v", err)
	}
	return backend.New(m, land, cache, cfg, keys.New(m, testKeyRing(t))), m, land, cache
}

// testKEK — фиксированный KEK всего пакета бэкенд-тестов.
//
// Случайный на каждый тест не годится: TEK тенанта 1 переживает тест (Clean
// оставляет seed-строки и не восстанавливает колонки), и второй тест получил бы
// обёртку, завернутую уже другим KEK, — «ключ не подошёл» там, где бага нет.
// Фиксированный KEK делает это состояние безвредным.
var testKEK = func() []byte {
	sum := sha256.Sum256([]byte("tapegw backend tests kek v1"))
	return sum[:]
}()

func testKeyRing(t *testing.T) *iam.KeyRing {
	t.Helper()
	ring, err := iam.NewKeyRing(1, map[int][]byte{1: testKEK})
	if err != nil {
		t.Fatalf("кольцо KEK: %v", err)
	}
	return ring
}

// testProjectID — ProjectID учётки тестового тенанта, а заодно и tenants.id:
// миграция 0005 заводит единственный тенант `default` с id = 1 и строит
// соответствие tenants.id == Account.ProjectID.
const testProjectID = 1

// testTenantID — то же число в виде bigint: тесты, которые готовят строки
// прямым SQL, подставляют его в колонку tenant_id.
const testTenantID int64 = testProjectID

// accountCtx имитирует то, что делает versitygw: кладёт учётку запроса в
// контекст под ключом "account" (см. tenant_guard.go). Ключ — строка, своего
// типа у versitygw для бэкенда нет.
//
// Заодно дублируем учётку под "bucket-owner": его читает owner() при создании
// бакета, и в бою его ставит админский CreateBucket.
func accountCtx(acct auth.Account) context.Context {
	//lint:ignore SA1029 ключ диктует versitygw, своего типа у него нет
	ctx := context.WithValue(context.Background(), "account", acct)
	//lint:ignore SA1029 ключ диктует versitygw, своего типа у него нет
	return context.WithValue(ctx, "bucket-owner", acct)
}

// ownerCtx — обычный ключ тестового тенанта с указанным access.
func ownerCtx(access string) context.Context {
	return accountCtx(auth.Account{Access: access, Role: auth.RoleUser, ProjectID: testProjectID})
}

// testCtx — контекст «просто пользователя» тестового тенанта: большинство
// тестов работает под ним.
func testCtx() context.Context { return ownerCtx("tester") }

// adminCtx — учётка администратора. Root в versitygw выглядит ровно так:
// RoleAdmin при незаполненном ProjectID (authentication.go:44).
func adminCtx() context.Context {
	return accountCtx(auth.Account{Access: "root", Role: auth.RoleAdmin})
}

func ptr[T any](v T) *T { return &v }

func TestBucketLifecycle(t *testing.T) {
	be, _, _ := newBackend(t)
	ctx := ownerCtx("alice")

	if err := be.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: ptr("photos")}, []byte(`{"owner":"alice"}`)); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	if _, err := be.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: ptr("photos")}); err != nil {
		t.Fatalf("HeadBucket: %v", err)
	}

	list, err := be.ListBuckets(ctx, s3response.ListBucketsInput{Owner: "alice"})
	if err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}
	if len(list.Buckets.Bucket) != 1 || list.Buckets.Bucket[0].Name != "photos" {
		t.Fatalf("ListBuckets вернул %+v", list.Buckets.Bucket)
	}

	if err := be.DeleteBucket(ctx, "photos"); err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}

	if _, err := be.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: ptr("photos")}); err == nil {
		t.Fatal("после удаления HeadBucket должен ошибаться")
	} else if !isS3Code(err, s3err.ErrNoSuchBucket) {
		t.Errorf("ожидали NoSuchBucket, получили %v", err)
	}
}

// Коды при повторном CreateBucket берём один в один из posix-бэкенда
// versitygw (backend/posix/posix.go): свой бакет — BucketAlreadyOwnedByYou,
// чужой — BucketAlreadyExists. Оба 409, но разные: по коду клиент решает,
// повторять ли попытку.

func TestCreateBucketAlreadyOwnedByYou(t *testing.T) {
	be, _, _ := newBackend(t)
	ctx := ownerCtx("alice")

	if err := be.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: ptr("dup")}, nil); err != nil {
		t.Fatalf("первый CreateBucket: %v", err)
	}

	err := be.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: ptr("dup")}, nil)
	if err == nil {
		t.Fatal("повторный CreateBucket своего бакета должен ошибаться, как в AWS")
	}
	if !isS3Code(err, s3err.ErrBucketAlreadyOwnedByYou) {
		t.Errorf("ожидали BucketAlreadyOwnedByYou, получили %v", err)
	}
}

func TestCreateBucketOtherOwner(t *testing.T) {
	be, _, _ := newBackend(t)

	if err := be.CreateBucket(ownerCtx("alice"), &s3.CreateBucketInput{Bucket: ptr("shared")}, nil); err != nil {
		t.Fatalf("CreateBucket alice: %v", err)
	}

	err := be.CreateBucket(ownerCtx("bob"), &s3.CreateBucketInput{Bucket: ptr("shared")}, nil)
	if err == nil {
		t.Fatal("чужой бакет должен быть ошибкой")
	}
	if !isS3Code(err, s3err.ErrBucketAlreadyExists) {
		t.Errorf("ожидали BucketAlreadyExists, получили %v", err)
	}
}

// Мягкое удаление освобождает имя: rb -> mb должно работать, хотя строка
// бакета остаётся в таблице, потому что на неё ссылаются objects.
func TestBucketNameReusableAfterDelete(t *testing.T) {
	be, _, _ := newBackend(t)
	ctx := ownerCtx("alice")

	if err := be.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: ptr("reuse")}, nil); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if err := be.DeleteBucket(ctx, "reuse"); err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}
	if _, err := be.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: ptr("reuse")}); err == nil {
		t.Fatal("удалённый бакет не должен отвечать на HEAD")
	}
	if err := be.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: ptr("reuse")}, nil); err != nil {
		t.Fatalf("повторный CreateBucket после удаления: %v", err)
	}
}

// Префикс и max-buckets должны фильтроваться в SQL, а не в Go.
func TestListBucketsPrefix(t *testing.T) {
	be, _, _ := newBackend(t)
	ctx := ownerCtx("alice")

	for _, name := range []string{"aa-1", "aa-2", "aa-3", "bb-1"} {
		if err := be.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: ptr(name)}, nil); err != nil {
			t.Fatalf("CreateBucket %s: %v", name, err)
		}
	}

	filtered, err := be.ListBuckets(ctx, s3response.ListBucketsInput{Owner: "alice", Prefix: "aa-"})
	if err != nil {
		t.Fatalf("ListBuckets prefix: %v", err)
	}
	if len(filtered.Buckets.Bucket) != 3 {
		t.Errorf("по префиксу aa- получили %d, ожидали 3", len(filtered.Buckets.Bucket))
	}
}

// Пагинация обязана завершаться. Это не придирка к форме ответа: в
// ListAllMyBucketsResult versitygw нет отдельного NextContinuationToken, и
// если эхом вернуть входной токен, пагинатор SDK и `aws s3 ls` будут
// запрашивать последнюю страницу вечно.
func TestListBucketsPaginationTerminates(t *testing.T) {
	be, _, _ := newBackend(t)
	ctx := ownerCtx("alice")

	want := []string{"bkt-1", "bkt-2", "bkt-3", "bkt-4", "bkt-5"}
	for _, name := range want {
		if err := be.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: ptr(name)}, nil); err != nil {
			t.Fatalf("CreateBucket %s: %v", name, err)
		}
	}

	// Разные Max, чтобы поймать и «ровно последняя страница», и «обрезали посередине».
	for _, maxBuckets := range []int32{1, 2, 3, 5, 7} {
		got := []string{}
		token := ""
		// Потолок заведомо больше числа бакетов: если цикл не завершается,
		// тест должен упасть, а не висеть.
		for page := 0; ; page++ {
			if page > len(want)+2 {
				t.Fatalf("MaxBuckets=%d: пагинация не завершилась за %d страниц, собрано %v",
					maxBuckets, page, got)
			}
			out, err := be.ListBuckets(ctx, s3response.ListBucketsInput{
				Owner: "alice", MaxBuckets: maxBuckets, ContinuationToken: token,
			})
			if err != nil {
				t.Fatalf("MaxBuckets=%d страница %d: %v", maxBuckets, page, err)
			}
			for _, b := range out.Buckets.Bucket {
				got = append(got, b.Name)
			}
			token = out.ContinuationToken
			if token == "" {
				break
			}
		}

		if len(got) != len(want) {
			t.Errorf("MaxBuckets=%d: собрано %d бакетов (%v), ожидали %d",
				maxBuckets, len(got), got, len(want))
			continue
		}
		seen := map[string]bool{}
		for _, name := range got {
			if seen[name] {
				t.Errorf("MaxBuckets=%d: %s попал в выдачу дважды: %v", maxBuckets, name, got)
			}
			seen[name] = true
		}
	}
}

// На последней странице токен обязан быть пустым — иначе пагинатор не остановится.
func TestListBucketsLastPageHasNoToken(t *testing.T) {
	be, _, _ := newBackend(t)
	ctx := ownerCtx("alice")

	for _, name := range []string{"one", "two"} {
		if err := be.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: ptr(name)}, nil); err != nil {
			t.Fatalf("CreateBucket %s: %v", name, err)
		}
	}

	// Ровно столько же, сколько есть: дублировать «последнюю» страницу не за чем.
	out, err := be.ListBuckets(ctx, s3response.ListBucketsInput{Owner: "alice", MaxBuckets: 2})
	if err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}
	if len(out.Buckets.Bucket) != 2 {
		t.Fatalf("вернулось %d бакетов", len(out.Buckets.Bucket))
	}
	if out.ContinuationToken != "" {
		t.Errorf("на странице, которой всё покрыто, токен = %q, ожидали пусто", out.ContinuationToken)
	}
}

func TestDeleteBucketNotEmpty(t *testing.T) {
	be, m, _ := newBackend(t)
	ctx := ownerCtx("alice")

	if err := be.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: ptr("full")}, nil); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	// online_until обязателен с миграции 0004: NULL в этой колонке молча
	// выключал бы вытеснение, поэтому колонка NOT NULL.
	if _, err := m.Pool.Exec(ctx, `
		INSERT INTO objects (id, bucket_id, key, size, etag, state, online_until,
		                     tenant_id, pool_id, wrapped_dek)
		SELECT gen_random_uuid(), (SELECT id FROM buckets WHERE name='full'),
		       'a.txt', 3, 'd41d8cd98f00b204e9800998ecf8427e', 'archived',
		       now() + interval '1 day', $1, $2, 'x'::bytea`,
		testTenantID, meta.DefaultPoolID); err != nil {
		t.Fatalf("подготовка объекта: %v", err)
	}

	err := be.DeleteBucket(ctx, "full")
	if err == nil {
		t.Fatal("непустой бакет должен отказывать")
	}
	if !isS3Code(err, s3err.ErrBucketNotEmpty) {
		t.Errorf("ожидали BucketNotEmpty, получили %v", err)
	}
}

func TestBucketTagging(t *testing.T) {
	be, _, _ := newBackend(t)
	ctx := ownerCtx("alice")

	if err := be.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: ptr("tagged"),
		CreateBucketConfiguration: &types.CreateBucketConfiguration{
			Tags: []types.Tag{{Key: ptr("team"), Value: ptr("storage")}},
		},
	}, nil); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	tags, err := be.GetBucketTagging(ctx, "tagged")
	if err != nil {
		t.Fatalf("GetBucketTagging: %v", err)
	}
	if tags["team"] != "storage" {
		t.Errorf("теги из CreateBucket потеряны: %v", tags)
	}

	if err := be.DeleteBucketTagging(ctx, "tagged"); err != nil {
		t.Fatalf("DeleteBucketTagging: %v", err)
	}
	if _, err := be.GetBucketTagging(ctx, "tagged"); err == nil {
		t.Error("после удаления тегов ожидали ошибку")
	}
}

func TestBucketACL(t *testing.T) {
	be, _, _ := newBackend(t)
	ctx := ownerCtx("alice")

	if err := be.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: ptr("acl")}, []byte(`{"acl":"private"}`)); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	got, err := be.GetBucketAcl(ctx, &s3.GetBucketAclInput{Bucket: ptr("acl")})
	if err != nil {
		t.Fatalf("GetBucketAcl: %v", err)
	}
	// acl хранится как jsonb, а jsonb нормализует пробелы и порядок ключей.
	// Сравниваем по смыслу: versitygw всё равно парсит эти байты в ACL, побайтовая
	// копия исходного запроса не нужна.
	if !sameJSON(t, got, `{"acl":"private"}`) {
		t.Errorf("ACL исказился: %s", got)
	}

	if err := be.PutBucketAcl(ctx, "acl", []byte(`{"acl":"public-read"}`)); err != nil {
		t.Fatalf("PutBucketAcl: %v", err)
	}
	got, err = be.GetBucketAcl(ctx, &s3.GetBucketAclInput{Bucket: ptr("acl")})
	if err != nil {
		t.Fatalf("GetBucketAcl после Put: %v", err)
	}
	if !sameJSON(t, got, `{"acl":"public-read"}`) {
		t.Errorf("ACL не обновился: %s", got)
	}
}

// Неподдержанные операции обязаны отвечать NotImplemented, а не «молча
// что-то делать» (правило 4).
//
// Проверяем на CopyObject. Метод выбирается по одному критерию: он обязан
// оставаться заглушкой. Multipart для этой роли больше не годится — его
// реализовали в T2 PR#2, и тест проверял бы не заглушку, а реализацию.
func TestUnsupportedOperationsReportNotImplemented(t *testing.T) {
	be, _, _ := newBackend(t)

	_, err := be.CopyObject(context.Background(), s3response.CopyObjectInput{
		Bucket: ptr("any"), Key: ptr("k"), CopySource: ptr("any/k"),
	})
	if err == nil {
		t.Fatal("ожидали NotImplemented")
	}
	if !isS3Code(err, s3err.ErrNotImplemented) {
		t.Errorf("ожидали NotImplemented, получили %v", err)
	}
}

// sameJSON сравнивает два JSON-документа по смыслу, а не по байтам.
func sameJSON(t *testing.T, got []byte, want string) bool {
	t.Helper()
	var a, b any
	if err := json.Unmarshal(got, &a); err != nil {
		t.Fatalf("unmarshal got: %v", err)
	}
	if err := json.Unmarshal([]byte(want), &b); err != nil {
		t.Fatalf("unmarshal want: %v", err)
	}
	return reflect.DeepEqual(a, b)
}

// isS3Code сравнивает код S3-ошибки. Сравниваем BaseError с эталоном, а не
// строку: код в ответе и константа ErrorCode — разные представления одного.
// isS3CodeString сравнивает строковый код S3-ошибки. Нужно для кодов, которых
// нет в перечислении versitygw (например InvalidStorageClass).
func isS3CodeString(err error, code string) bool {
	var se s3err.S3Error
	if !errors.As(err, &se) {
		return false
	}
	return se.BaseError().Code == code
}

func isS3Code(err error, code s3err.ErrorCode) bool {
	var se s3err.S3Error
	if !errors.As(err, &se) {
		return false
	}
	return se.BaseError() == s3err.GetAPIError(code)
}
