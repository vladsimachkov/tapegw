package e2e

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Два e2e на обход Object Lock — отложены с ADR-0015, пока не появилась
// конфигурация лока через API. Теперь она есть: версионирование и лок ставятся
// PutBucketVersioning / PutObjectLockConfiguration, а не прямой записью в БД.
//
// 1. Обычный ключ без права s3:BypassGovernanceRetention + заголовок обхода →
//    отказ на версии под GOVERNANCE. Бэкенд заголовок доверяет (ADR-0015), и
//    этот тест — единственная проверка, что доверие оправдано маршрутом.
// 2. Root удаляет версию под COMPLIANCE → отказ. Root идёт мимо IAM вообще,
//    поэтому здесь бэкенд — единственный барьер.
//
// Обход бэкенд берёт из заголовка запроса: контроллер versitygw поля
// BypassGovernanceRetention не передаёт (object-delete.go:221). Положительный
// контроль — TestGovernanceBypassWorksForAdmin.
//
// Не проверяется: выдача `s3:BypassGovernanceRetention` в политике бакета
// обычному ключу. Право из политики бакета до бэкенда не доходит — versitygw
// отказывает сам, и отказ приходит до вызова бэкенда. Это отдельный вопрос про
// интеграцию с versitygw, а не про наш бэкенд: путь через роль (root/admin)
// работает.

// lockBucket — бакет клиента с включёнными версиями и Object Lock.
func lockBucket(t *testing.T, owner *s3.Client, prefix string) string {
	t.Helper()
	ctx := context.Background()
	bucket := uniqueBucket(prefix)

	if _, err := owner.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if _, err := owner.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucket),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	}); err != nil {
		t.Fatalf("PutBucketVersioning: %v", err)
	}
	if _, err := owner.PutObjectLockConfiguration(ctx, &s3.PutObjectLockConfigurationInput{
		Bucket:                  aws.String(bucket),
		ObjectLockConfiguration: &types.ObjectLockConfiguration{ObjectLockEnabled: types.ObjectLockEnabledEnabled},
	}); err != nil {
		t.Fatalf("PutObjectLockConfiguration: %v", err)
	}
	return bucket
}

// putRetention ставит GOVERNANCE/COMPLIANCE на версию.
func putRetention(t *testing.T, c *s3.Client, bucket, key, vid string,
	mode types.ObjectLockRetentionMode, until time.Time) {

	t.Helper()
	if _, err := c.PutObjectRetention(context.Background(), &s3.PutObjectRetentionInput{
		Bucket: aws.String(bucket), Key: aws.String(key), VersionId: aws.String(vid),
		Retention: &types.ObjectLockRetention{Mode: mode, RetainUntilDate: &until},
	}); err != nil {
		t.Fatalf("PutObjectRetention(%s): %v", mode, err)
	}
}

// Обычный ключ без права на обход не обходит GOVERNANCE заголовком.
func TestGovernanceBypassNeedsRight(t *testing.T) {
	ownerAccess := os.Getenv("TAPEGW_E2E_OWNER_ACCESS")
	plainAccess := os.Getenv("TAPEGW_E2E_USER_ACCESS")
	if ownerAccess == "" || plainAccess == "" {
		t.Skip("нужны TAPEGW_E2E_OWNER_ACCESS и TAPEGW_E2E_USER_ACCESS")
	}
	owner := clientWith(t, ownerAccess, os.Getenv("TAPEGW_E2E_OWNER_SECRET"))
	plain := clientWith(t, plainAccess, os.Getenv("TAPEGW_E2E_USER_SECRET"))
	ctx := context.Background()
	bucket := lockBucket(t, owner, "govbyp")

	// Права на запись и удаление есть, права на обход — нет.
	policy := fmt.Sprintf(
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"%s"},`+
			`"Action":["s3:PutObject","s3:GetObject","s3:DeleteObject"],`+
			`"Resource":["arn:aws:s3:::%s/*"]}]}`,
		plainAccess, bucket)
	if _, err := owner.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{
		Bucket: aws.String(bucket), Policy: aws.String(policy),
	}); err != nil {
		t.Fatalf("PutBucketPolicy: %v", err)
	}

	put, err := owner.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k"), Body: strings.NewReader("данные"),
	})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	vid := aws.ToString(put.VersionId)
	if vid == "" {
		t.Fatalf("версия не вернула versionId")
	}
	putRetention(t, owner, bucket, "k", vid, types.ObjectLockRetentionModeGovernance,
		time.Now().Add(24*time.Hour))

	// Без заголовка — отказ, и это ожидаемо.
	if _, err := plain.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k"), VersionId: aws.String(vid),
	}); err == nil {
		t.Errorf("удаление версии под GOVERNANCE без обхода прошло")
	}

	// С заголовком, но без права — тоже отказ.
	_, err = plain.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k"), VersionId: aws.String(vid),
		BypassGovernanceRetention: aws.Bool(true),
	})
	if err == nil {
		t.Errorf("обход GOVERNANCE прошёл без права s3:BypassGovernanceRetention — " +
			"бэкенд доверяет заголовку, и это стала дыра")
	}

	// И без заголовка, и с ним — отказ: права на обход у ключа нет ни в
	// политике бакета, ни в роли.

	// То же удаление тем же ключом после того, как право появилось. Меняется
	// одно право — значит, отказ выше был про право, а не про личность.
	//
	// Про с3:DeleteObjectVersion отдельно: удаление по versionId — это именно
	// оно, а не s3:DeleteObject (object-delete.go:175). Политика с одним только
	// s3:DeleteObject отклоняет удаление конкретной версии ещё до проверки
	// блокировки, и отказ — общий AccessDenied, а не ErrObjectLocked. По этому
	// различию и видно, какой слой отказал.
	withBypass := fmt.Sprintf(
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"%s"},`+
			`"Action":["s3:PutObject","s3:GetObject","s3:DeleteObject",`+
			`"s3:DeleteObjectVersion","s3:BypassGovernanceRetention"],`+
			`"Resource":["arn:aws:s3:::%s/*"]}]}`,
		plainAccess, bucket)
	if _, err := owner.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{
		Bucket: aws.String(bucket), Policy: aws.String(withBypass),
	}); err != nil {
		t.Fatalf("PutBucketPolicy с правом обхода: %v", err)
	}
	if _, err := plain.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k"), VersionId: aws.String(vid),
		BypassGovernanceRetention: aws.Bool(true),
	}); err != nil {
		t.Errorf("обход GOVERNANCE с правом из политики бакета не прошёл: %v", err)
	}
}

// Обход GOVERNANCE работает, когда право есть. Root получает его по роли, и
// это положительный контроль к TestGovernanceBypassNeedsRight: без него отказ
// в том тесте нельзя отличить от «версия вообще неудаляема».
//
// Бэкенд узнаёт о запросе обхода из заголовка: контроллер versitygw поля
// BypassGovernanceRetention не передаёт (object-delete.go:221).
func TestGovernanceBypassWorksForAdmin(t *testing.T) {
	root := client(t)
	ctx := context.Background()
	bucket := lockBucket(t, root, "govok")

	put, err := root.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k"), Body: strings.NewReader("данные"),
	})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	vid := aws.ToString(put.VersionId)
	putRetention(t, root, bucket, "k", vid, types.ObjectLockRetentionModeGovernance,
		time.Now().Add(24*time.Hour))

	if _, err := root.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k"), VersionId: aws.String(vid),
		BypassGovernanceRetention: aws.Bool(true),
	}); err != nil {
		t.Errorf("обход GOVERNANCE с правом не прошёл: %v", err)
	}
}

// Root идёт мимо IAM: против COMPLIANCE он бессилен, потому что бэкенд —
// единственный барьер.
func TestRootCannotDeleteCompliance(t *testing.T) {
	root := client(t)
	ctx := context.Background()
	bucket := lockBucket(t, root, "comprot")

	put, err := root.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k"), Body: strings.NewReader("данные"),
	})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	vid := aws.ToString(put.VersionId)
	putRetention(t, root, bucket, "k", vid, types.ObjectLockRetentionModeCompliance,
		time.Now().Add(24*time.Hour))

	if _, err := root.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k"), VersionId: aws.String(vid),
		BypassGovernanceRetention: aws.Bool(true),
	}); err == nil {
		t.Errorf("root удалил версию под COMPLIANCE — бэкенд не удержал")
	}

	// Данные на месте: отказ не должен был ничего изменить.
	if _, err := root.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k"), VersionId: aws.String(vid),
	}); err != nil {
		t.Errorf("версия под COMPLIANCE недоступна после отказа: %v", err)
	}
}
