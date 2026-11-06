package meta_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// Тест миграции 0022 (блокировка пачки до разбора).
//
// Правило 11: Up и Down текстом файла и на таблице с данными. Плюс проверка
// пары «причина + момент»: блок без объяснения — строка, по которой непонятно,
// что смотреть.

const migration0022 = "migrations/0022_pack_blocked.sql"

func TestMigration0022PackBlocked(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	if n := columnIn(t, ctx, m, "packs", "blocked_reason"); n != 1 {
		t.Fatal("packs.blocked_reason отсутствует")
	}

	packID := uuid.New()
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO packs (id, pool_id, kind, state, total_bytes, live_bytes, members_total, members_live)
		  VALUES ($1, 1, 'data', 'sealed', 10, 10, 1, 1)`, packID); err != nil {
		t.Fatalf("пачка: %v", err)
	}

	if err := m.BlockPack(ctx, packID, "exists"); err != nil {
		t.Fatalf("BlockPack: %v", err)
	}
	if err := m.BlockPack(ctx, packID, ""); err == nil {
		t.Error("BlockPack принял пустую причину")
	}
	// Половина пары — отказ.
	if _, err := m.Pool.Exec(ctx,
		`UPDATE packs SET blocked_at = NULL WHERE id = $1`, packID); err == nil {
		t.Error("CHECK не поймал причину без момента")
	}
	if err := m.UnblockPack(ctx, packID); err != nil {
		t.Fatalf("UnblockPack: %v", err)
	}
	if err := m.UnblockPack(ctx, packID); err == nil {
		t.Error("повторный UnblockPack прошёл — а блокировки уже нет")
	}

	// Down → Up текстом файла, на пачке с данными.
	if _, err := m.Pool.Exec(ctx, section(t, migration0022, "Down")); err != nil {
		t.Fatalf("Down 0022 на таблице с данными: %v", err)
	}
	if n := columnIn(t, ctx, m, "packs", "blocked_reason"); n != 0 {
		t.Error("после Down blocked_reason осталась")
	}
	if _, err := m.Pool.Exec(ctx, section(t, migration0022, "Up")); err != nil {
		t.Fatalf("повторный Up 0022: %v", err)
	}
	if n := columnIn(t, ctx, m, "packs", "blocked_reason"); n != 1 {
		t.Error("после повторного Up blocked_reason нет")
	}
	var live int
	if err := m.Pool.QueryRow(ctx, `SELECT count(*) FROM packs WHERE id = $1`, packID).Scan(&live); err != nil {
		t.Fatal(err)
	}
	if live != 1 {
		t.Error("Down+Up потеряли строку пачки")
	}
}
