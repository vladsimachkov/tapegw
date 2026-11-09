package meta_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// Тест миграции 0025 (признак «пачка из одного крупного объекта»).
//
// Правило 11: Up и Down текстом файла и на таблице с данными, проверка по
// каталогу БД. Колонка NOT NULL DEFAULT false, а не nullable: у строк,
// записанных до колонки, признак известен — это обычные пачки, и false для
// них не догадка, а факт.

const migration0025 = "migrations/0025_packs_single_object.sql"

func TestMigration0025PacksSingleObject(t *testing.T) {
	m := testdb.MigrationScratch(t, testdb.AllVersions)
	ctx := context.Background()

	if n := columnIn(t, ctx, m, "packs", "single_object"); n != 1 {
		t.Fatal("packs.single_object отсутствует")
	}

	// Пачка с данными: Down не имеет права её потерять, а повторный Up обязан
	// вернуть признак со значением false (не NULL и не true).
	packID := uuid.New()
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO packs (id, pool_id, kind, state, total_bytes, members_total)
		 VALUES ($1, 1, 'data', 'sealed', 100, 1)`, packID); err != nil {
		t.Fatal(err)
	}

	if _, err := m.Pool.Exec(ctx, section(t, migration0025, "Down")); err != nil {
		t.Fatalf("Down 0025 на таблице с данными: %v", err)
	}
	if n := columnIn(t, ctx, m, "packs", "single_object"); n != 0 {
		t.Error("после Down single_object осталась")
	}
	if _, err := m.Pool.Exec(ctx, section(t, migration0025, "Up")); err != nil {
		t.Fatalf("повторный Up 0025: %v", err)
	}
	if n := columnIn(t, ctx, m, "packs", "single_object"); n != 1 {
		t.Error("после повторного Up single_object нет")
	}
	var single bool
	if err := m.Pool.QueryRow(ctx,
		`SELECT single_object FROM packs WHERE id = $1`, packID).Scan(&single); err != nil {
		t.Fatalf("строка пачки после Down+Up: %v", err)
	}
	if single {
		t.Error("существовавшая до колонки пачка стала single_object — DEFAULT false не сработал")
	}
}
