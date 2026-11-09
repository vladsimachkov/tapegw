package meta_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// Тест миграции 0026 (след обхода GOVERNANCE при удалении).
//
// Правило 11: Up и Down — текстом файла и на таблице с данными, проверка по
// каталогу БД. Колонка nullable без DEFAULT: у строк, удалённых штатно, следа
// обхода нет, и выдумывать его миграция не вправе.

const migration0026 = "migrations/0026_retention_bypass.sql"

func TestMigration0026RetentionBypass(t *testing.T) {
	m := testdb.MigrationScratch(t, testdb.AllVersions)
	ctx := context.Background()

	if n := columnIn(t, ctx, m, "objects", "retention_bypassed_at"); n != 1 {
		t.Fatal("objects.retention_bypassed_at отсутствует")
	}

	mustBucket(t, m, "rb26-bucket")
	var bucketID int64
	if err := m.Pool.QueryRow(ctx,
		`SELECT id FROM buckets WHERE name = 'rb26-bucket'`).Scan(&bucketID); err != nil {
		t.Fatalf("id бакета: %v", err)
	}

	// Версия под GOVERNANCE с действующим сроком: именно такие строки и
	// переносит колонка, Down не имеет права её потерять.
	objID := uuid.New()
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO objects (id, bucket_id, key, size, etag, storage_class, state,
		                      landing, online_until, tenant_id, pool_id, is_current,
		                      wrapped_dek, retention_mode, retain_until)
		 VALUES ($1, $2, 'rb26', 100, 'etag', 'GLACIER', 'archived', false,
		         now() - interval '1 hour', 1, 1, true, 'x'::bytea,
		         'GOVERNANCE', $3)`,
		objID, bucketID, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Pool.Exec(ctx,
		`UPDATE objects SET retention_bypassed_at = now() WHERE id = $1`, objID); err != nil {
		t.Fatal(err)
	}

	if _, err := m.Pool.Exec(ctx, section(t, migration0026, "Down")); err != nil {
		t.Fatalf("Down 0026 на таблице с данными: %v", err)
	}
	if n := columnIn(t, ctx, m, "objects", "retention_bypassed_at"); n != 0 {
		t.Error("после Down колонка осталась")
	}
	if _, err := m.Pool.Exec(ctx, section(t, migration0026, "Up")); err != nil {
		t.Fatalf("повторный Up 0026: %v", err)
	}
	if n := columnIn(t, ctx, m, "objects", "retention_bypassed_at"); n != 1 {
		t.Error("после повторного Up колонки нет")
	}

	var live int
	var bypassed bool
	if err := m.Pool.QueryRow(ctx,
		`SELECT count(*), bool_or(retention_bypassed_at IS NOT NULL)
		   FROM objects WHERE id = $1`, objID).Scan(&live, &bypassed); err != nil {
		t.Fatal(err)
	}
	if live != 1 {
		t.Fatal("Down+Up потеряли версию")
	}
	if bypassed {
		t.Error("после Down+Up след обхода восстановился из ниоткуда: значение должно быть null")
	}
}
