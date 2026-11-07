package backend_test

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/backend"
	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/spool"
)

// GET из восстановленной копии (S5): landing-файла нет, в restores живой
// 'restored' — читать надо cache, а после истечения срока — снова
// InvalidObjectState.

// restoreToCache имитирует работу recaller'а: файл переезжает из landing в
// cache, строка становится archived без landing с живой строкой restores.
func restoreToCache(t *testing.T, m *meta.Meta, land, cache *spool.Spool, id uuid.UUID, expires string) {
	t.Helper()
	ctx := context.Background()

	src, err := land.Open(id)
	if err != nil {
		t.Fatal(err)
	}
	tmp, err := cache.Tmp()
	if err != nil {
		src.Close()
		t.Fatal(err)
	}
	if _, err := io.Copy(tmp, src); err != nil {
		src.Close()
		tmp.Close()
		t.Fatal(err)
	}
	src.Close()
	if err := cache.Publish(tmp, id); err != nil {
		t.Fatal(err)
	}
	if err := land.Remove(id); err != nil {
		t.Fatal(err)
	}

	if _, err := m.Pool.Exec(ctx,
		`UPDATE objects SET state = 'archived', landing = false, archived_at = now(),
		        online_until = now() + interval '1 day' WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO restores (object_id, tenant_id, state, tier, days, requested_at,
		                       restored_at, expires_at)
		 VALUES ($1, $2, 'restored', 'Standard', 7, now() - interval '2 hours',
		         now() - interval '1 hour', now() + $3::interval)`,
		id, testProjectID, expires); err != nil {
		t.Fatal(err)
	}
}

func objectID(t *testing.T, m *meta.Meta, bucket, key string) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	if err := m.Pool.QueryRow(context.Background(),
		`SELECT o.id FROM objects o JOIN buckets b ON b.id = o.bucket_id
		 WHERE b.name = $1 AND o.key = $2`, bucket, key).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func getBody(t *testing.T, be *backend.TapeGW, bucket, key string) (string, *string) {
	t.Helper()
	get, err := be.GetObject(testCtx(), &s3.GetObjectInput{
		Bucket: ptr(bucket), Key: ptr(key),
	})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer get.Body.Close()
	data, err := io.ReadAll(get.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(data), get.Restore
}

// Восстановленный объект отдаётся из cache и показывает срок в x-amz-restore.
func TestGetFromRestoredCache(t *testing.T) {
	be, m, land, cache := newBackendFull(t, nil)
	mustBucket(t, be)
	body := "данные, восстановленные с ленты в cache"
	putBody(t, be, "b", "k", body)

	id := objectID(t, m, "b", "k")
	restoreToCache(t, m, land, cache, id, "6 days")

	got, restore := getBody(t, be, "b", "k")
	if got != body {
		t.Errorf("из cache отдали %q, ждали исходные байты", got)
	}
	if restore == nil || !strings.HasPrefix(*restore, `ongoing-request="false"`) {
		t.Errorf("x-amz-restore=%v, ждали ongoing-request=\"false\" со сроком", restore)
	}
}

// Срок restore истёк — объект снова отдаёт InvalidObjectState, даже если
// файл в cache ещё не снят janitor'ом (решение по expires_at, как в заголовке).
func TestGetExpiredRestoreIsInvalidState(t *testing.T) {
	be, m, land, cache := newBackendFull(t, nil)
	mustBucket(t, be)
	putBody(t, be, "b", "k", "данные")

	id := objectID(t, m, "b", "k")
	restoreToCache(t, m, land, cache, id, "-1 seconds")

	_, err := be.GetObject(testCtx(), &s3.GetObjectInput{Bucket: ptr("b"), Key: ptr("k")})
	if !isS3CodeString(err, "InvalidObjectState") {
		t.Errorf("истёкший restore: %v, ждали InvalidObjectState", err)
	}
}

// Файл в cache исчез, а restore активен — это несогласованность, а не штатный
// Glacier: громкая ошибка, а не InvalidObjectState (то же правило, что с
// файлом landing в #122).
func TestGetMissingCacheFileWithActiveRestoreIsLoud(t *testing.T) {
	be, m, land, cache := newBackendFull(t, nil)
	mustBucket(t, be)
	putBody(t, be, "b", "k", "данные")

	id := objectID(t, m, "b", "k")
	restoreToCache(t, m, land, cache, id, "6 days")
	if err := cache.Remove(id); err != nil {
		t.Fatal(err)
	}

	_, err := be.GetObject(testCtx(), &s3.GetObjectInput{Bucket: ptr("b"), Key: ptr("k")})
	if err == nil {
		t.Fatal("GET без файла при активном restore прошёл")
	}
	if isS3CodeString(err, "InvalidObjectState") {
		t.Error("несогласованность спрятана под InvalidObjectState")
	}
}
