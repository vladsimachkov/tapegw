package meta_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// Тест миграции 0017 (пачки и изменяемая ссылка на место хранения).
//
// Правило 11: Up и Down исполняются текстом файла, и на таблице с данными, а
// не на пустой схеме. Здесь это не формальность: Down роняет колонки objects,
// и только на наполненной таблице видно, что они уходят вместе с ограничениями
// и внешним ключом, а повторный Up ставит всё обратно.

const migration0017 = "migrations/0017_packs.sql"

func TestMigration0017Packs(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	// Таблица и её колонки.
	for _, col := range []string{"id", "pool_id", "kind", "state", "total_bytes",
		"live_bytes", "members_total", "members_live", "medium",
		"created_at", "sealed_at", "archived_at", "retired_at"} {
		var n int
		if err := m.Pool.QueryRow(ctx,
			`SELECT count(*) FROM information_schema.columns
			  WHERE table_schema = current_schema() AND table_name = 'packs'
			    AND column_name = $1`, col).Scan(&n); err != nil {
			t.Fatalf("колонка packs.%s: %v", col, err)
		}
		if n != 1 {
			t.Errorf("packs.%s отсутствует", col)
		}
	}
	if n := indexCount(t, ctx, m, "packs_open"); n != 1 {
		t.Errorf("индекс packs_open отсутствует: открытую пачку пула ищут на каждую запись")
	}
	if n := indexCount(t, ctx, m, "objects_pack"); n != 1 {
		t.Errorf("индекс objects_pack отсутствует: им же проверяется инвариант live_bytes")
	}

	// Счётчики живого не могут выехать за записанное. Отрицательный live_bytes
	// или live_bytes > total_bytes означают, что учёт сломался, и уплотнение
	// начнёт выбирать не те ленты.
	mustBucket(t, m, "packs-bucket")
	var bucketID int64
	if err := m.Pool.QueryRow(ctx,
		`SELECT id FROM buckets WHERE name = 'packs-bucket'`).Scan(&bucketID); err != nil {
		t.Fatalf("id бакета: %v", err)
	}

	packID := uuid.New()
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO packs (id, pool_id, kind, total_bytes, live_bytes, members_total, members_live, medium)
		  VALUES ($1, 1, 'data', 1000, 600, 2, 1, ARRAY['TST001','TST002'])`, packID); err != nil {
		t.Fatalf("вставка пачки: %v", err)
	}
	if _, err := m.Pool.Exec(ctx,
		`UPDATE packs SET live_bytes = 2000 WHERE id = $1`, packID); err == nil {
		t.Errorf("live_bytes больше total_bytes прошёл: CHECK не работает")
	}
	if _, err := m.Pool.Exec(ctx,
		`UPDATE packs SET members_live = -1 WHERE id = $1`, packID); err == nil {
		t.Errorf("отрицательный members_live прошёл: CHECK не работает")
	}
	if _, err := m.Pool.Exec(ctx,
		`UPDATE packs SET live_bytes = 600, members_live = 1 WHERE id = $1`, packID); err != nil {
		t.Fatalf("вернуть счётчики: %v", err)
	}

	// medium — массив: при repl_count = 2 объект лежит на двух носителях.
	var med []string
	if err := m.Pool.QueryRow(ctx, `SELECT medium FROM packs WHERE id = $1`, packID).Scan(&med); err != nil {
		t.Fatalf("medium: %v", err)
	}
	if len(med) != 2 {
		t.Errorf("medium = %v, ожидали два носителя при repl_count = 2", med)
	}

	// Участник пачки: ссылка обязана нести смещение и длину.
	memberOK := uuid.New()
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO objects (id, bucket_id, key, size, etag, storage_class, state,
		                      landing, online_until, tenant_id, pool_id, is_current,
		                      wrapped_dek, pack_id, pack_offset, pack_length)
		  VALUES ($1, $2, 'member-ok', 100, 'etag', 'GLACIER', 'archived', false,
		          now() + interval '1 day', 1, 1, true, 'x'::bytea, $3, 0, 100)`,
		memberOK, bucketID, packID); err != nil {
		t.Fatalf("вставка участника пачки: %v", err)
	}

	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO objects (id, bucket_id, key, size, etag, storage_class, state,
		                      landing, online_until, tenant_id, pool_id, is_current,
		                      wrapped_dek, pack_id)
		  VALUES ($1, $2, 'member-bad', 100, 'etag', 'GLACIER', 'archived', false,
		          now() + interval '1 day', 1, 1, true, 'x'::bytea, $3)`,
		uuid.New(), bucketID, packID); err == nil {
		t.Errorf("участник пачки без pack_offset прошёл: потерянное смещение читает чужие байты молча")
	}

	// Носитель пачки лежит в packs.medium, а не в строке участника: иначе про
	// одну ленту будет два источника правды.
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO objects (id, bucket_id, key, size, etag, storage_class, state,
		                      landing, online_until, tenant_id, pool_id, is_current,
		                      wrapped_dek, pack_id, pack_offset, pack_length, phobos_medium)
		  VALUES ($1, $2, 'member-medium', 100, 'etag', 'GLACIER', 'archived', false,
		          now() + interval '1 day', 1, 1, true, 'x'::bytea, $3, 0, 100, ARRAY['TST001'])`,
		uuid.New(), bucketID, packID); err == nil {
		t.Errorf("участник пачки со своим phobos_medium прошёл: два источника правды о ленте")
	}

	// Крупный объект вне пачки носит свой носитель — это путь ADR-0008.
	big := uuid.New()
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO objects (id, bucket_id, key, size, etag, storage_class, state,
		                      landing, online_until, tenant_id, pool_id, is_current,
		                      wrapped_dek, phobos_medium)
		  VALUES ($1, $2, 'big-object', 100, 'etag', 'GLACIER', 'archived', false,
		          now() + interval '1 day', 1, 1, true, 'x'::bytea, ARRAY['TST002'])`,
		big, bucketID); err != nil {
		t.Fatalf("крупный объект со своим носителем: %v", err)
	}

	// Down и повторный Up — текстом файла и на наполненной таблице.
	if _, err := m.Pool.Exec(ctx, section(t, migration0017, "Down")); err != nil {
		t.Fatalf("Down 0017 на таблице с данными: %v", err)
	}
	for _, col := range []string{"pack_id", "pack_offset", "pack_length", "phobos_medium"} {
		if n := columnCount(t, ctx, m, col); n != 0 {
			t.Errorf("после Down колонка objects.%s осталась", col)
		}
	}
	if n := tableCount(t, ctx, m, "packs"); n != 0 {
		t.Errorf("после Down таблица packs осталась")
	}
	if _, err := m.Pool.Exec(ctx, section(t, migration0017, "Up")); err != nil {
		t.Fatalf("повторный Up 0017: %v", err)
	}
	if n := tableCount(t, ctx, m, "packs"); n != 1 {
		t.Errorf("после повторного Up таблица packs не вернулась")
	}
	for _, col := range []string{"pack_id", "pack_offset", "pack_length", "phobos_medium"} {
		if n := columnCount(t, ctx, m, col); n != 1 {
			t.Errorf("после повторного Up колонка objects.%s не вернулась", col)
		}
	}
	// Ограничения возвращаются валидными: NOT VALID без VALIDATE означал бы,
	// что после отката и повтора проверка спит.
	if n := constraintCount(t, ctx, m, "objects_pack_member"); n != 1 {
		t.Errorf("ограничение objects_pack_member отсутствует после повторного Up")
	}
	var validated bool
	if err := m.Pool.QueryRow(ctx,
		`SELECT conisvalid FROM pg_constraint WHERE conname = 'objects_pack_member'`).
		Scan(&validated); err == nil && !validated {
		t.Errorf("objects_pack_member осталось NOT VALID после повторного Up")
	}
}

func tableCount(t *testing.T, ctx context.Context, m *meta.Meta, tbl string) int {
	t.Helper()
	var n int
	if err := m.Pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_tables
		  WHERE schemaname = current_schema() AND tablename = $1`, tbl).Scan(&n); err != nil {
		t.Fatalf("счётчик таблиц: %v", err)
	}
	return n
}

func constraintCount(t *testing.T, ctx context.Context, m *meta.Meta, name string) int {
	t.Helper()
	var n int
	if err := m.Pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_constraint c
		   JOIN pg_namespace n ON n.oid = c.connamespace
		  WHERE c.conname = $1 AND n.nspname = current_schema()`, name).Scan(&n); err != nil {
		t.Fatalf("счётчик ограничений: %v", err)
	}
	return n
}
