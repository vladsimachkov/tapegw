// Package e2e — сценарии против живого tapegw.
//
// Тесты пропускаются, если TAPEGW_E2E_ENDPOINT не задан: правило 2 из AGENTS.md
// требует, чтобы `go test ./...` проходило где угодно. Запуск:
//
//	docker run -d --name tapegw-pg -p 55432:5432 \
//	  -e POSTGRES_USER=tapegw -e POSTGRES_PASSWORD=tapegw -e POSTGRES_DB=phobos \
//	  postgres:16-alpine
//	go run ./cmd/tapegw -config <(printf 'db:\n  dsn: postgres://tapegw:tapegw@localhost:55432/phobos?search_path=tapegw\nroot:\n  access: acc\n  secret: secret\n')
//	TAPEGW_E2E_ENDPOINT=http://127.0.0.1:7070 \
//	TAPEGW_E2E_ACCESS=acc TAPEGW_E2E_SECRET=secret \
//	  go test ./test/e2e -v
package e2e

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// client собирает S3-клиент против тестового tapegw. Path-style обязателен:
// виртуальные хосты требуют DNS и в стенде не настроены.
func client(t *testing.T) *s3.Client {
	t.Helper()
	return clientWith(t, os.Getenv("TAPEGW_E2E_ACCESS"), os.Getenv("TAPEGW_E2E_SECRET"))
}

// clientWith — клиент под конкретные ключи. Нужен, чтобы проверять доступ не
// только от root: путь root в versitygw идёт мимо политик и ACL вообще
// (authorizationApplies возвращает false для root/admin), поэтому e2e на root
// ничего про проверки доступа не говорит.
func clientWith(t *testing.T, access, secret string) *s3.Client {
	t.Helper()

	ep := os.Getenv("TAPEGW_E2E_ENDPOINT")
	if ep == "" {
		t.Skip("TAPEGW_E2E_ENDPOINT не задан — пропускаем e2e")
	}
	if access == "" || secret == "" {
		t.Fatal("TAPEGW_E2E_ACCESS / TAPEGW_E2E_SECRET обязательны, когда задан endpoint")
	}

	return s3.New(s3.Options{
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider(access, secret, ""),
		BaseEndpoint: aws.String(ep),
		UsePathStyle: true,
	})
}

// userClient — обычный ключ без прав администратора. Стенд обязан поднять
// tapegw с s3.iam_dir и положить в ту директорию users.json с этим
// пользователем. Иначе никак: в режиме одной учётки versitygw знает только
// root, и политика на другого principal не прошла бы даже валидацию —
// ResolveAccounts в single-account режиме считает любой не-root ключ
// несуществующим, а это MalformedPolicy.
func userClient(t *testing.T) *s3.Client {
	t.Helper()
	access, secret := os.Getenv("TAPEGW_E2E_USER_ACCESS"), os.Getenv("TAPEGW_E2E_USER_SECRET")
	if access == "" || secret == "" {
		t.Skip("TAPEGW_E2E_USER_ACCESS / TAPEGW_E2E_USER_SECRET не заданы — пропускаем проверку доступа")
	}
	return clientWith(t, access, secret)
}

// userAccess — имя principal для политики: в файловом IAM им считается access
// key, а не ARN (ARN умеет только standalone-IAM).
func userAccess(t *testing.T) string {
	t.Helper()
	access := os.Getenv("TAPEGW_E2E_USER_ACCESS")
	if access == "" {
		t.Skip("TAPEGW_E2E_USER_ACCESS не задан")
	}
	return access
}

// uniqueBucket даёт уникальное имя, чтобы тесты можно было запускать повторно
// против одного и того же живого сервера.
func uniqueBucket(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano()%1_000_000_000)
}

// Критерий готовности M1: aws s3 mb / ls / rb работают против tapegw.
func TestBucketCreateListDelete(t *testing.T) {
	c := client(t)
	ctx := context.Background()
	bucket := uniqueBucket("m1")

	if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	t.Cleanup(func() {
		_, _ = c.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	})

	if _, err := c.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("HeadBucket: %v", err)
	}

	out, err := c.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}
	found := false
	for _, b := range out.Buckets {
		if aws.ToString(b.Name) == bucket {
			found = true
			if b.CreationDate.IsZero() {
				t.Error("у бакета нет CreationDate — листинг отдаёт неполные данные")
			}
		}
	}
	if !found {
		t.Errorf("бакет %s не появился в ListBuckets", bucket)
	}

	if _, err := c.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}

	_, err = c.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	if err == nil {
		t.Fatal("HeadBucket удалённого бакета должен ошибаться")
	}
	var nf *types.NotFound
	if !errors.As(err, &nf) {
		t.Errorf("ожидали 404 NotFound, получили %T: %v", err, err)
	}
}

func TestBucketTagging(t *testing.T) {
	c := client(t)
	ctx := context.Background()
	bucket := uniqueBucket("m1tags")

	if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	t.Cleanup(func() {
		_, _ = c.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	})

	want := map[string]string{"team": "storage", "env": "test"}
	if _, err := c.PutBucketTagging(ctx, &s3.PutBucketTaggingInput{
		Bucket: aws.String(bucket),
		Tagging: &types.Tagging{
			TagSet: []types.Tag{{Key: aws.String("team"), Value: aws.String("storage")},
				{Key: aws.String("env"), Value: aws.String("test")}},
		},
	}); err != nil {
		t.Fatalf("PutBucketTagging: %v", err)
	}

	got, err := c.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatalf("GetBucketTagging: %v", err)
	}
	if len(got.TagSet) != len(want) {
		t.Fatalf("тегов %d, ожидали %d", len(got.TagSet), len(want))
	}
	for _, tag := range got.TagSet {
		if want[aws.ToString(tag.Key)] != aws.ToString(tag.Value) {
			t.Errorf("лишний или неверный тег %s=%s", aws.ToString(tag.Key), aws.ToString(tag.Value))
		}
	}
}

// Неподдержанная операция обязана отвечать NotImplemented, а не 200 «как-нибудь».
func TestUnsupportedIsNotImplemented(t *testing.T) {
	c := client(t)
	bucket := uniqueBucket("m1ni")

	if _, err := c.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	t.Cleanup(func() {
		_, _ = c.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	})

	_, err := c.PutObjectRetention(context.Background(), &s3.PutObjectRetentionInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("a.txt"),
		// Дату контроллер versitygw валидирует до обращения к бэкенду, и с
		// пустой датой мы получили бы InvalidArgument вместо NotImplemented —
		// то есть проверяли бы не то.
		Retention: &types.ObjectLockRetention{
			Mode:            types.ObjectLockRetentionModeGovernance,
			RetainUntilDate: aws.Time(time.Now().Add(24 * time.Hour)),
		},
	})
	if err == nil {
		t.Fatal("Object Lock вне MVP (см. «Чего нет в MVP»), а операция вернула успех")
	}
	// Код в aws-sdk-go-v2 приходит не типизированным типом, а строкой ошибки
	// API, поэтому сравниваем строку. Важно именно «NotImplemented», а не любой
	// отказ: 500 тоже отказ, но он означает баг, а не честное «не поддерживаем».
	if !strings.Contains(err.Error(), "NotImplemented") {
		t.Errorf("ожидали NotImplemented, получили %T: %v", err, err)
	}
}
