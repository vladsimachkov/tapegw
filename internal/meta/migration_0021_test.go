package meta_test

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// Тест миграции 0021 (захват запечатанной пачки).
//
// Правило 11: Up и Down исполняются текстом файла и на таблице с данными.
// Здесь это не формальность: CHECK пары claimed_by/claim_expires_at проверяется
// на строке, где захват уже был, — на пустой таблице он ничего не знает про
// «половина снята».

const migration0021 = "migrations/0021_pack_claim.sql"

func TestMigration0021PackClaim(t *testing.T) {
	m := testdb.MigrationScratch(t, testdb.AllVersions)
	ctx := context.Background()

	if n := columnIn(t, ctx, m, "packs", "claimed_by"); n != 1 {
		t.Fatal("packs.claimed_by отсутствует")
	}
	if n := columnIn(t, ctx, m, "packs", "claim_expires_at"); n != 1 {
		t.Fatal("packs.claim_expires_at отсутствует")
	}

	packID := uuid.New()
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO packs (id, pool_id, kind, state, total_bytes, live_bytes, members_total, members_live)
		  VALUES ($1, 1, 'data', 'sealed', 1000, 1000, 1, 1)`, packID); err != nil {
		t.Fatalf("пачка: %v", err)
	}

	// Захват и снятие — штатный путь.
	if _, err := m.Pool.Exec(ctx,
		`UPDATE packs SET claimed_by = 'p1', claim_expires_at = now() + interval '1 minute' WHERE id = $1`, packID); err != nil {
		t.Fatalf("захват: %v", err)
	}

	// Половина пары — отказ: «взято» без срока означает «взято навсегда», а
	// «срок без владельца» — пачку никто не ведёт, но и забрать нельзя.
	if _, err := m.Pool.Exec(ctx,
		`UPDATE packs SET claimed_by = NULL WHERE id = $1`, packID); err == nil {
		t.Error("CHECK пары не поймал захват без срока")
	}
	if _, err := m.Pool.Exec(ctx,
		`UPDATE packs SET claim_expires_at = NULL WHERE id = $1`, packID); err == nil {
		m.Pool.Exec(ctx, `UPDATE packs SET claimed_by = NULL WHERE id = $1`) //nolint:errcheck
		t.Error("CHECK пары не поймал срок без владельца")
	}
	if _, err := m.Pool.Exec(ctx,
		`UPDATE packs SET claimed_by = NULL, claim_expires_at = NULL WHERE id = $1`, packID); err != nil {
		t.Fatalf("снять захват целиком: %v", err)
	}

	// Индекс частичный и ровно по 'sealed': именно так ищется продолжение.
	var expr string
	// Фильтр по current_schema() обязателен: имя индекса в Postgres уникально
	// внутри схемы, а pg_index их не разделяет — без фильтра можно поймать
	// индекс из схемы другого пакета тестов и проверить чужой предикат.
	if err := m.Pool.QueryRow(ctx, `
SELECT coalesce(pg_get_expr(i.indpred, i.indrelid), '')
FROM pg_index i
JOIN pg_class c ON c.oid = i.indexrelid
JOIN pg_class t ON t.oid = i.indrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE c.relname = 'packs_sealed' AND n.nspname = current_schema()`).Scan(&expr); err != nil {
		t.Fatalf("индекс packs_sealed: %v", err)
	}
	if !strings.Contains(expr, "state = 'sealed'") {
		t.Errorf("предикат индекса packs_sealed: %q — ждали частичный по 'sealed'", expr)
	}

	// Down → Up текстом файла, на пачке с данными.
	if _, err := m.Pool.Exec(ctx, section(t, migration0021, "Down")); err != nil {
		t.Fatalf("Down 0021 на таблице с данными: %v", err)
	}
	if n := columnIn(t, ctx, m, "packs", "claimed_by"); n != 0 {
		t.Error("после Down claimed_by осталась")
	}
	if _, err := m.Pool.Exec(ctx, section(t, migration0021, "Up")); err != nil {
		t.Fatalf("повторный Up: %v", err)
	}
	if n := columnIn(t, ctx, m, "packs", "claimed_by"); n != 1 {
		t.Error("после повторного Up claimed_by нет")
	}
	var live int
	if err := m.Pool.QueryRow(ctx, `SELECT count(*) FROM packs WHERE id = $1`, packID).Scan(&live); err != nil {
		t.Fatal(err)
	}
	if live != 1 {
		t.Error("Down+Up потеряли строку пачки")
	}
}
