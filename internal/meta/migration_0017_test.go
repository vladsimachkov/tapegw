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
		"live_bytes", "members_total", "members_live",
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
		`INSERT INTO packs (id, pool_id, kind, total_bytes, live_bytes, members_total, members_live)
		  VALUES ($1, 1, 'data', 1000, 600, 2, 1)`, packID); err != nil {
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

	// Носителя в схеме нет ни у пачки, ни у объекта: место хранения каждой копии
	// описывает таблица archive_copies (предложение 0013), и одна колонка их не
	// описывает. Проверка обратная — колонки быть не должно. Пока таблица пуста,
	// это дешёвое решение; после первой записи на ленту возврат к колонке стал бы
	// миграцией всего архива, поэтому он и закрыт тестом.
	for _, col := range []string{"medium", "phobos_medium"} {
		if n := columnOutside(t, ctx, m, col, "archive_copies"); n != 0 {
			t.Errorf("колонка %s существует вне archive_copies: носитель должен жить в archive_copies, "+
				"а не колонкой (предложение 0013)", col)
		}
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

	// Границы смещения и длины. Отрицательный offset и нулевая длина проходят
	// как «не NULL» и обнаруживаются только при чтении, где выглядят порчей
	// шифротекста, а не ошибкой учётных данных.
	for _, bad := range []struct {
		name    string
		off, ln int64
	}{
		{"отрицательный offset", -1, 100},
		{"нулевая длина", 0, 0},
	} {
		if _, err := m.Pool.Exec(ctx,
			`INSERT INTO objects (id, bucket_id, key, size, etag, storage_class, state,
			                      landing, online_until, tenant_id, pool_id, is_current,
			                      wrapped_dek, pack_id, pack_offset, pack_length)
			  VALUES ($1, $2, '`+bad.name+`', 100, 'etag', 'GLACIER', 'archived', false,
			          now() + interval '1 day', 1, 1, true, 'x'::bytea, $4, $3, $5)`,
			uuid.New(), bucketID, bad.off, packID, bad.ln); err == nil {
			t.Errorf("%s в пачке прошёл: CHECK не работает", bad.name)
		}
	}

	// Объект не может лечь в пачку чужого пула. Для выделенного пула это дыра в
	// изоляции: ошибка в планировщике положит объект тенанта в общую пачку, а
	// значит на общую ленту, и ни одна проверка этого не заметит — видно это
	// будет только по содержимому ленты.
	otherPool := int64(77)
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO pools (id, name, kind, phobos_tag)
		  VALUES ($1, 'm17-other-pool', 'dedicated', 'm17-other')`,
		otherPool); err != nil {
		t.Fatalf("второй пул: %v", err)
	}
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO objects (id, bucket_id, key, size, etag, storage_class, state,
		                      landing, online_until, tenant_id, pool_id, is_current,
		                      wrapped_dek, pack_id, pack_offset, pack_length)
		  VALUES ($1, $2, 'cross-pool', 100, 'etag', 'GLACIER', 'archived', false,
		          now() + interval '1 day', 1, $3, true, 'x'::bytea, $4, 0, 100)`,
		uuid.New(), bucketID, otherPool, packID); err == nil {
		t.Errorf("объект чужого пула лёг в пачку: изоляция выделенного пула держится только на коде планировщика")
	}

	// Крупный объект вне пачки — путь ADR-0008, ссылки на пачку нет.
	big := uuid.New()
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO objects (id, bucket_id, key, size, etag, storage_class, state,
		                      landing, online_until, tenant_id, pool_id, is_current,
		                      wrapped_dek)
		  VALUES ($1, $2, 'big-object', 100, 'etag', 'GLACIER', 'archived', false,
		          now() + interval '1 day', 1, 1, true, 'x'::bytea)`,
		big, bucketID); err != nil {
		t.Fatalf("крупный объект вне пачки: %v", err)
	}

	// Down и повторный Up — текстом файла и на наполненной таблице.
	//
	// 0018 снимается первым: archive_copies.pack_id ссылается на packs, и
	// Down-миграции всегда идут в обратном порядке. Соблазн закрыть это
	// DROP TABLE ... CASCADE в самом 0017 здесь вреден: он бы молча уносил
	// реестр копий вместе с пачками, а это ровно то, что должно быть видно.
	if _, err := m.Pool.Exec(ctx, section(t, migration0018, "Down")); err != nil {
		t.Fatalf("Down 0018 перед 0017: %v", err)
	}
	if _, err := m.Pool.Exec(ctx, section(t, migration0017, "Down")); err != nil {
		t.Fatalf("Down 0017 на таблице с данными: %v", err)
	}
	for _, col := range []string{"pack_id", "pack_offset", "pack_length"} {
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
	// 0018 возвращается следом: без неё TestCleanCoversEveryTable не найдёт
	// таблиц, которые Clean перечисляет.
	if _, err := m.Pool.Exec(ctx, section(t, migration0018, "Up")); err != nil {
		t.Fatalf("повторный Up 0018 после 0017: %v", err)
	}
	for _, col := range []string{"pack_id", "pack_offset", "pack_length"} {
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
