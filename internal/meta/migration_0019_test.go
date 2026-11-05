package meta_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// Тест миграции 0019 (ожидаемая контрольная сумма копии).
//
// Сумма нужна локальной передаче (ADR-0021, Ф0.4): сверять надо ожидаемую сумму
// оригинала, а не ту, что mover посчитал при чтении с ленты, иначе проверка
// замкнута сама на себя и повреждение записывается как проверенное.

const migration0019 = "migrations/0019_copy_sha256.sql"

func TestMigration0019CopySha256(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	if n := columnIn(t, ctx, m, "archive_copies", "sha256"); n != 1 {
		t.Fatalf("archive_copies.sha256 отсутствует")
	}

	packID := uuid.New()
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO packs (id, pool_id, kind, total_bytes, live_bytes, members_total, members_live)
		  VALUES ($1, 1, 'data', 1000, 600, 2, 1)`, packID); err != nil {
		t.Fatalf("пачка: %v", err)
	}
	var mediumID int64
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO media (library_cached, label, generation, medium_type, lifecycle)
		  VALUES ('l', 'M19TAPE', 'LTO-8', 'LTO-8', 'active')`); err != nil {
		t.Fatalf("лента: %v", err)
	}
	if err := m.Pool.QueryRow(ctx,
		`SELECT id FROM media WHERE label = 'M19TAPE'`).Scan(&mediumID); err != nil {
		t.Fatalf("id ленты: %v", err)
	}

	sum := func(n int) []byte {
		b := make([]byte, n)
		for i := range b {
			b[i] = byte(i)
		}
		return b
	}

	// Готовая копия обязана знать ожидаемую сумму: без неё она не годится в
	// источник копии или repack — сверять нечем.
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO archive_copies (pack_id, spec, phobos_oid, format, medium_id, state)
		  VALUES ($1, 'side:left', 'm19-done-nosum', 1, $2, 'done')`,
		packID, mediumID); err == nil {
		t.Error("копия done без суммы прошла: в источник она не годится")
	}

	// bytea принимает что угодно, и сумма в 20 байтов прошла бы как «есть»,
	// а сверять её было бы нечем.
	for _, n := range []int{16, 20, 31, 33, 64} {
		if _, err := m.Pool.Exec(ctx,
			`INSERT INTO archive_copies (pack_id, spec, phobos_oid, format, medium_id, state, sha256)
			  VALUES ($1, 'side:left', 'm19-len-' || $2, 1, $3, 'done', $4)`,
			packID, string(rune('a'+n)), mediumID, sum(n)); err == nil {
			t.Errorf("сумма длиной %d байтов прошла: длина не проверена", n)
		}
	}

	// Сумма в 32 байта проходит, а копия в ожидании может жить и без суммы:
	// её объявляют при переходе в done.
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO archive_copies (pack_id, spec, phobos_oid, format, medium_id, state, sha256)
		  VALUES ($1, 'side:left', 'm19-ok', 1, $2, 'done', $3)`,
		packID, mediumID, sum(32)); err != nil {
		t.Errorf("копия с суммой: %v", err)
	}
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO archive_copies (pack_id, spec, phobos_oid, format, state)
		  VALUES ($1, 'side:right', 'm19-pending', 1, 'pending')`, packID); err != nil {
		t.Errorf("копия в ожидании без суммы: %v", err)
	}

	// Down и повторный Up — текстом файла и на наполненной таблице (правило 11).
	// 0019 зависит от archive_copies, а 0018 — от packs: снимаем в обратном
	// порядке и возвращаем в прямом, иначе колонка исчезнет вместе с таблицей
	// и не вернётся.
	for _, mig := range []string{migration0019, migration0018} {
		if _, err := m.Pool.Exec(ctx, section(t, mig, "Down")); err != nil {
			t.Fatalf("Down %s: %v", mig, err)
		}
	}
	if n := columnIn(t, ctx, m, "archive_copies", "sha256"); n != 0 {
		t.Errorf("после Down колонка sha256 осталась")
	}
	for _, mig := range []string{migration0018, migration0019} {
		if _, err := m.Pool.Exec(ctx, section(t, mig, "Up")); err != nil {
			t.Fatalf("Up %s: %v", mig, err)
		}
	}
	if n := columnIn(t, ctx, m, "archive_copies", "sha256"); n != 1 {
		t.Errorf("после Up колонка sha256 не вернулась")
	}
	// Ограничение обязано вернуться работающим: NOT NULL-подобный CHECK,
	// поставленный без проверки, означал бы «сумма не обязательна».
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO archive_copies (pack_id, spec, phobos_oid, format, state)
		  VALUES ($1, 'side:left', 'm19-after', 1, 'done')`, packID); err == nil {
		t.Error("после Down/Up копия done без суммы проходит: ограничение не вернулось")
	}
}
