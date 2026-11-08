package meta_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// Тест миграции 0024 (идентичность воркера, держящего строку restore).
//
// Правило 11: Up и Down текстом файла и на таблице с данными, проверка по
// каталогу БД. Колонка nullable по той же причине, что у file_bytes в 0023:
// у строк, записанных до колонки, владельца нет — их захват ещё не закреплялся,
// и выдумывать владельца миграция не вправе.

const migration0024 = "migrations/0024_restores_claimed_by.sql"

func TestMigration0024RestoresClaimedBy(t *testing.T) {
	m := testdb.MigrationScratch(t, testdb.AllVersions)
	ctx := context.Background()

	if n := columnIn(t, ctx, m, "restores", "claimed_by"); n != 1 {
		t.Fatal("restores.claimed_by отсутствует")
	}

	mustBucket(t, m, "cb24-bucket")
	var bucketID int64
	if err := m.Pool.QueryRow(ctx,
		`SELECT id FROM buckets WHERE name = 'cb24-bucket'`).Scan(&bucketID); err != nil {
		t.Fatalf("id бакета: %v", err)
	}

	// Строка restore с данными: Down не имеет права её потерять.
	objID := uuid.New()
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO objects (id, bucket_id, key, size, etag, storage_class, state,
		                      landing, online_until, tenant_id, pool_id, is_current,
		                      wrapped_dek)
		 VALUES ($1, $2, 'cb24', 100, 'etag', 'GLACIER', 'archived', false,
		         now() - interval '1 hour', 1, 1, true, 'x'::bytea)`,
		objID, bucketID); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO restores (object_id, tenant_id, state, tier, days, priority, requested_at)
		 VALUES ($1, 1, 'in_progress', 'Standard', 7, 1, now())`, objID); err != nil {
		t.Fatal(err)
	}

	// Down → Up текстом файла на таблице с данными: колонка уходит и
	// возвращается, строка очереди выживает.
	if _, err := m.Pool.Exec(ctx, section(t, migration0024, "Down")); err != nil {
		t.Fatalf("Down 0024 на таблице с данными: %v", err)
	}
	if n := columnIn(t, ctx, m, "restores", "claimed_by"); n != 0 {
		t.Error("после Down claimed_by осталась")
	}
	if _, err := m.Pool.Exec(ctx, section(t, migration0024, "Up")); err != nil {
		t.Fatalf("повторный Up 0024: %v", err)
	}
	if n := columnIn(t, ctx, m, "restores", "claimed_by"); n != 1 {
		t.Error("после повторного Up claimed_by нет")
	}
	var live int
	if err := m.Pool.QueryRow(ctx,
		`SELECT count(*) FROM restores WHERE object_id = $1`, objID).Scan(&live); err != nil {
		t.Fatal(err)
	}
	if live != 1 {
		t.Error("Down+Up потеряли строку restore")
	}
}
