package backend_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"

	"github.com/vladsimachkov/tapegw/internal/backend"
	"github.com/vladsimachkov/tapegw/internal/config"
	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// Критерий готовности M1 — «aws s3 mb / ls / rb работают против tapegw».
// Здесь проверяется слой, на который эти команды опираются: бакетные операции
// бэкенда. Сквозной прогон живым aws cli — test/e2e (M2+).

// newBackend поднимает бэкенд поверх тестовой базы. meta возвращается отдельно:
// часть тестов готовит данные напрямую через SQL, минуя бэкенд.
func newBackend(t *testing.T) (*backend.TapeGW, *meta.Meta) {
	t.Helper()
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	return backend.New(m, config.Default()), m
}

// ownerCtx имитирует то, что делает versitygw: кладёт аккаунт запроса в контекст.
//
// Ключ — строка, а не типизированный struct{}: именно так делает сам versitygw
// (см. backend/posix/posix.go), а нам нужно прочитать то же значение.
func ownerCtx(access string) context.Context {
	//lint:ignore SA1029 ключ диктует versitygw, своего типа у него нет
	return context.WithValue(context.Background(), "bucket-owner", auth.Account{Access: access})
}

func ptr[T any](v T) *T { return &v }

func TestBucketLifecycle(t *testing.T) {
	be, _ := newBackend(t)
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

// Повторный CreateBucket тем же владельцем в AWS — 200, а не ошибка: на это
// опираются `aws s3 mb --region ...` в скриптах и идемпотентные повторные
// попытки клиента.
func TestCreateBucketIdempotentForSameOwner(t *testing.T) {
	be, _ := newBackend(t)
	ctx := ownerCtx("alice")

	for i := range 3 {
		if err := be.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: ptr("dup")}, nil); err != nil {
			t.Fatalf("CreateBucket #%d: %v", i+1, err)
		}
	}
}

func TestCreateBucketOtherOwner(t *testing.T) {
	be, _ := newBackend(t)

	if err := be.CreateBucket(ownerCtx("alice"), &s3.CreateBucketInput{Bucket: ptr("shared")}, nil); err != nil {
		t.Fatalf("CreateBucket alice: %v", err)
	}

	err := be.CreateBucket(ownerCtx("bob"), &s3.CreateBucketInput{Bucket: ptr("shared")}, nil)
	if err == nil {
		t.Fatal("чужой бакет должен быть ошибкой")
	}
	if !isS3Code(err, s3err.ErrBucketAlreadyOwnedByYou) {
		t.Errorf("ожидали BucketAlreadyOwnedByYou, получили %v", err)
	}
}

func TestDeleteBucketNotEmpty(t *testing.T) {
	be, m := newBackend(t)
	ctx := ownerCtx("alice")

	if err := be.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: ptr("full")}, nil); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	if _, err := m.Pool.Exec(ctx, `
		INSERT INTO objects (id, bucket_id, key, size, etag, state)
		SELECT gen_random_uuid(), (SELECT id FROM buckets WHERE name='full'),
		       'a.txt', 3, 'd41d8cd98f00b204e9800998ecf8427e', 'archived'`); err != nil {
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
	be, _ := newBackend(t)
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
	be, _ := newBackend(t)
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
// что-то делать» (правило 4). Проверяем на multipart: он появится только в M3.
func TestUnsupportedOperationsReportNotImplemented(t *testing.T) {
	be, _ := newBackend(t)

	_, err := be.CreateMultipartUpload(context.Background(), s3response.CreateMultipartUploadInput{
		Bucket: ptr("any"), Key: ptr("k"),
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
func isS3Code(err error, code s3err.ErrorCode) bool {
	var se s3err.S3Error
	if !errors.As(err, &se) {
		return false
	}
	return se.BaseError() == s3err.GetAPIError(code)
}
