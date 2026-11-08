package backend_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/backend"
	"github.com/vladsimachkov/tapegw/internal/config"
	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/spool"
)

// GET из восстановленной копии (S5): landing-файла нет, в restores живой
// 'restored' — читать надо cache, а после истечения срока — снова
// InvalidObjectState. Решает одна колонка objects.online_until (ADR-0003):
// та же, что в заголовке x-amz-restore и по которой janitor снимает файл.

// restoreToCache имитирует работу recaller'а: файл переезжает из landing в
// cache, строка становится archived без landing с живой строкой restores.
// online и expires — интервалы от now() раздельно: они обязаны расходиться
// (GREATEST допускает online_until > expires_at), и решение принимается по
// online_until.
func restoreToCache(t *testing.T, m *meta.Meta, land, cache *spool.Spool, id uuid.UUID,
	online, expires string) {
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
		        online_until = now() + $2::interval WHERE id = $1`, id, online); err != nil {
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
	restoreToCache(t, m, land, cache, id, "6 days", "6 days")

	got, restore := getBody(t, be, "b", "k")
	if got != body {
		t.Errorf("из cache отдали %q, ждали исходные байты", got)
	}
	if restore == nil || !strings.HasPrefix(*restore, `ongoing-request="false"`) {
		t.Errorf("x-amz-restore=%v, ждали ongoing-request=\"false\" со сроком", restore)
	}
}

// online_until истёк — объект снова отдаёт InvalidObjectState, даже если
// файл в cache ещё не снят janitor'ом. Решает показанная клиенту дата, а не
// expires_at (ADR-0003).
func TestGetExpiredRestoreIsInvalidState(t *testing.T) {
	be, m, land, cache := newBackendFull(t, nil)
	mustBucket(t, be)
	putBody(t, be, "b", "k", "данные")

	id := objectID(t, m, "b", "k")
	restoreToCache(t, m, land, cache, id, "-1 seconds", "-1 seconds")

	_, err := be.GetObject(testCtx(), &s3.GetObjectInput{Bucket: ptr("b"), Key: ptr("k")})
	if !isS3CodeString(err, "InvalidObjectState") {
		t.Errorf("истёкший restore: %v, ждали InvalidObjectState", err)
	}
}

// Расхождение expires_at и online_until — не баг, а следствие GREATEST:
// запрос на 30 дней продлил online_until, чтение не удалось, повторный
// restore на 1 день поставил expires_at раньше. Показанная в заголовке дата
// и решение о чтении обязаны смотреть на online_until: иначе через сутки GET
// отвечал бы InvalidObjectState, а заголовок обещал бы данные ещё 29 дней
// (ревью S5, major 2).
func TestGetCacheReadableByOnlineUntilNotExpires(t *testing.T) {
	be, m, land, cache := newBackendFull(t, nil)
	mustBucket(t, be)
	body := "копия живёт дольше expires_at"
	putBody(t, be, "b", "k", body)

	id := objectID(t, m, "b", "k")
	// expires_at уже прошёл, online_until — через 6 дней.
	restoreToCache(t, m, land, cache, id, "6 days", "-1 seconds")

	got, _ := getBody(t, be, "b", "k")
	if got != body {
		t.Fatalf("из cache отдали %q — решение принято по expires_at, а не по online_until", got)
	}

	// HEAD обязан показывать тот же срок, по которому GET отдал данные:
	// online_until, а не expires_at.
	head, err := be.HeadObject(testCtx(), &s3.HeadObjectInput{Bucket: ptr("b"), Key: ptr("k")})
	if err != nil {
		t.Fatal(err)
	}
	if head.Restore == nil {
		t.Fatal("x-amz-restore пропал при живом online_until")
	}
	const marker = `expiry-date="`
	i := strings.Index(*head.Restore, marker)
	if i < 0 {
		t.Fatalf("x-amz-restore=%q, ждали expiry-date", *head.Restore)
	}
	shown, err := http.ParseTime(strings.Trim((*head.Restore)[i+len(marker):], `"`))
	if err != nil {
		t.Fatalf("expiry-date не парсуется: %q", *head.Restore)
	}
	if d := time.Until(shown); d < 5*24*time.Hour || d > 7*24*time.Hour {
		t.Errorf("в заголовке срок через %v, ждали ~6 дней (online_until), а не прошлый expires_at", d)
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
	restoreToCache(t, m, land, cache, id, "6 days", "6 days")
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

// Окно B1 для cache (аналог TestGetEvictedAtLastAttemptIsInvalidState из
// #122): janitor вытесняет restore ровно между решением о читаемости и
// открытием файла — строка снята, файл удалён. Перечитывание после последнего
// ENOENT обязано увидеть штатный офлайн и ответить InvalidObjectState,
// а не 500: клиенту не за чем знать, что он не успел за вытеснением.
func TestCacheEvictedAtLastAttemptIsInvalidState(t *testing.T) {
	be, m, land, cache := newBackendFull(t, func(c *config.Config) { c.Spool.EvictRetries = 0 })
	ctx := testCtx()
	mustBucket(t, be)
	putBody(t, be, "b", "k", "данные")

	id := objectID(t, m, "b", "k")
	restoreToCache(t, m, land, cache, id, "6 days", "6 days")

	// Вытеснение — ровно в окне между чтением строки и открытием файла.
	// С EvictRetries=0 этот виток последний: решение принимает до-чтение
	// после ENOENT, а не следующий проход цикла.
	be.SetEvictHookForTest(func(uuid.UUID) {
		if _, err := m.Pool.Exec(ctx,
			`UPDATE objects SET online_until = now() - interval '1 hour' WHERE id = $1`, id); err != nil {
			t.Errorf("истечение срока в крючке: %v", err)
			return
		}
		cands, err := m.ClaimCacheEvictBatch(ctx, 10)
		if err != nil {
			t.Errorf("ClaimCacheEvictBatch: %v", err)
			return
		}
		for _, c := range cands {
			if err := cache.Remove(c); err != nil {
				t.Errorf("unlink: %v", err)
			}
		}
	})

	if _, err := be.GetObject(ctx, &s3.GetObjectInput{Bucket: ptr("b"), Key: ptr("k")}); !isS3CodeString(err, "InvalidObjectState") {
		t.Errorf("GET, вытесненный на последней попытке: %v, ждали InvalidObjectState", err)
	}
}
