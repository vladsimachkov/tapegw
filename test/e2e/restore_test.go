package e2e

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Дыра, которую проверяет этот тест, — в контроллере versitygw: он не кладёт
// query versionId в RestoreObjectInput (s3api/controllers/object-post.go,
// v1.10.0), и бэкенд дочитывает версию из строки запроса сам (приём #35).
// Без дочитывания restore нетекущей версии ответил бы 200 и молча поставил
// в очередь текущую: клиент ждал бы не те данные. Проверять это надо
// настоящим HTTP — прямой вызов бэкенда дыру в контроллере не воспроизводит.
func TestRestoreVersionViaHTTPAddressesThatVersion(t *testing.T) {
	owner := ownerClient(t)
	db := standDB(t)
	ctx := context.Background()

	bucket := uniqueBucket("restv")
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

	old := putObject(t, owner, bucket, "k", "старые данные")
	vid1 := aws.ToString(old.VersionId)
	cur := putObject(t, owner, bucket, "k", "новые данные")
	vid2 := aws.ToString(cur.VersionId)
	if vid1 == "" || vid2 == "" || vid1 == vid2 {
		t.Fatalf("версии не различаются: %q / %q", vid1, vid2)
	}

	// Первую версию считаем заархивированной и вытесненной — состояние, в
	// котором restore принимается. id версии — id строки objects, см.
	// versionIDHeader в бэкенде.
	if _, err := db.Exec(ctx,
		`UPDATE objects SET state='archived', landing=false, archived_at=now(),
		        online_until = now() - interval '1 hour' WHERE id = $1`, vid1); err != nil {
		t.Fatalf("подготовка версии: %v", err)
	}

	if _, err := owner.RestoreObject(ctx, &s3.RestoreObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k"), VersionId: aws.String(vid1),
		RestoreRequest: &types.RestoreRequest{Days: aws.Int32(7),
			GlacierJobParameters: &types.GlacierJobParameters{Tier: types.TierStandard}},
	}); err != nil {
		t.Fatalf("RestoreObject нетекущей версии: %v", err)
	}

	// Очередь — на запрошенную версию, текущая не тронута.
	var n1, n2 int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM restores WHERE object_id = $1`, vid1).Scan(&n1); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(ctx, `SELECT count(*) FROM restores WHERE object_id = $1`, vid2).Scan(&n2); err != nil {
		t.Fatal(err)
	}
	if n1 != 1 {
		t.Errorf("для запрошенной версии строк restores: %d, ждали 1 — restore уехал на другую версию", n1)
	}
	if n2 != 0 {
		t.Errorf("текущая версия получила restore (%d строк) — проигнорирован versionId запроса", n2)
	}

	// HEAD запрошенной версии показывает восстановление.
	head, err := owner.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k"), VersionId: aws.String(vid1),
	})
	if err != nil {
		t.Fatalf("HEAD версии: %v", err)
	}
	if head.Restore == nil || *head.Restore != `ongoing-request="true"` {
		t.Errorf("HEAD версии: x-amz-restore=%v, ждали ongoing-request=\"true\"", head.Restore)
	}
}
