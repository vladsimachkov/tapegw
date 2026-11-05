package meta_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// Тест миграции 0018 (реестр копий, носителей и драйвов).
//
// Правило 11: Up и Down исполняются текстом файла и на таблице с данными.
// Здесь это не формальность: media и drives связаны друг с другом, и только на
// наполненной таблице видно, что Down уносит оба внешних ключа, а повторный Up
// ставит кольцо обратно.

const migration0018 = "migrations/0018_media_registry.sql"

func TestMigration0018MediaRegistry(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	for _, tc := range []struct{ table, cols string }{
		{"drives", "name library state media_id scsi_id attrs updated_at"},
		{"media", "id library label barcode generation medium_type state magazine " +
			"home_slot in_library_cache current_drive writable read_only_reason " +
			"capacity_bytes free_bytes format_version attrs created_at updated_at"},
		{"archive_copies", "id pack_id object_id spec phobos_oid phobos_version " +
			"format medium state attrs created_at updated_at done_at"},
		{"media_events", "id media_id event detail occurred_at"},
		{"drive_events", "id drive_name event detail occurred_at"},
	} {
		for _, col := range splitFields(tc.cols) {
			if n := columnIn(t, ctx, m, tc.table, col); n != 1 {
				t.Errorf("%s.%s отсутствует", tc.table, col)
			}
		}
	}

	// Носитель живёт в archive_copies и больше нигде. Тест перенесён из 0017 и
	// теперь обязан пропускать ровно archive_copies: если «medium» заведётся в
	// третьей таблице, у неё появится второе, расходящееся с первой мнение о
	// том, где лежит данные.
	for _, col := range []string{"medium", "phobos_medium"} {
		if n := columnOutside(t, ctx, m, col, "archive_copies"); n != 0 {
			t.Errorf("колонка %s появилась вне archive_copies: носитель описывает только копия", col)
		}
	}

	// --- media ---

	drive := "M18DRIVE1"
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO drives (name, library, state, scsi_id)
		  VALUES ($1, 'm18lib', 'empty', '/dev/tape/by-id/m18-1')`, drive); err != nil {
		t.Fatalf("драйв: %v", err)
	}

	mustInsertMedia(t, ctx, m, "M18TAPE01", "LTO-8", true)

	// Метка уникальна на уровне схемы. «Договорились не повторять» не работает:
	// две строки на одну метку — это планировщик, который выберет ленту, уже
	// занятую другой копией, и запишет поверх живых данных.
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO media (library, label, generation, medium_type)
		  VALUES ('m18lib', 'M18TAPE01', 'LTO-8', 'LTO-8')`); err == nil {
		t.Error("вторая строка на метку M18TAPE01 прошла: уникальность метки не в схеме")
	}

	// Слой 2 запрета не может спорить с состоянием: retired-лента не бывает
	// «доступна для записи». Иначе победит тот слой, который спросили последним.
	for _, st := range []string{"retired", "lost", "foreign", "blank"} {
		if _, err := m.Pool.Exec(ctx,
			`INSERT INTO media (library, label, generation, medium_type, state, writable)
			  VALUES ('m18lib', 'M18BAD-' || $1, 'LTO-8', 'LTO-8', $1, true)`, st); err == nil {
			t.Errorf("лента в состоянии %s доступна для записи: два слоя запрета разошлись", st)
		}
	}

	// Свободных байт не больше ёмкости: перевёрнутая пара читается как
	// «места много» и уводит пачку на ленту, которая её не примет.
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO media (library, label, generation, medium_type, capacity_bytes, free_bytes)
		  VALUES ('m18lib', 'M18BADGEO', 'LTO-8', 'LTO-8', 100, 200)`); err == nil {
		t.Error("free_bytes > capacity_bytes прошёл: геометрия не проверена")
	}

	// Закрытая на запись лента с местом — тот самый случай из спайка M0-12,
	// где Phobos отвечает ENOSPC и «полка закрыта» неотличима от «полка полна».
	// Схема обязана это различие хранить.
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO media (library, label, generation, medium_type, writable,
		                    read_only_reason, capacity_bytes, free_bytes)
		  VALUES ('m18lib', 'M18RO', 'LTO-7', 'LTO-7', false,
		          'поколение выведено', 1000, 900)`); err != nil {
		t.Errorf("закрытая лента с местом: %v", err)
	}

	// --- archive_copies ---

	var packID uuid.UUID
	if err := m.Pool.QueryRow(ctx,
		`SELECT id FROM packs LIMIT 1`).Scan(&packID); err != nil {
		packID = uuid.New()
		if _, err := m.Pool.Exec(ctx,
			`INSERT INTO packs (id, pool_id, kind, total_bytes, live_bytes, members_total, members_live)
			  VALUES ($1, 1, 'data', 1000, 600, 2, 1)`, packID); err != nil {
			t.Fatalf("пачка: %v", err)
		}
	}

	// Версия формата копии обязательна: при двух копиях разные версии формата
	// легально сосуществуют, и читатель выбирает декодер по ней, а не по
	// «текущей версии системы».
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO archive_copies (pack_id, spec, phobos_oid, medium, state)
		  VALUES ($1, 'side:left', 'oid-no-format', ARRAY['M18TAPE01'], 'done')`,
		packID); err == nil {
		t.Error("копия без версии формата прошла: формат копии обязан быть в схеме")
	}

	// Готовая копия обязана знать носитель.
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO archive_copies (pack_id, spec, phobos_oid, format, state)
		  VALUES ($1, 'side:left', 'oid-no-medium', 1, 'done')`, packID); err == nil {
		t.Error("копия state=done без носителя прошла: её нельзя ни прочитать, ни удалить")
	}

	// Копия без носителя легальна, пока не готова.
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO archive_copies (pack_id, spec, phobos_oid, format, state)
		  VALUES ($1, 'side:left', 'oid-pending', 1, 'pending')`, packID); err != nil {
		t.Errorf("копия в ожидании: %v", err)
	}

	// Ровно один владелец: ни того, ни другого — висящая копия, которую никто
	// не удалит; оба — ошибка маршрутизации. Объект для второго случая берётся
	// настоящий: иначе проверка падает по внешнему ключу, а не по CHECK, и тест
	// зелёный по неверной причине.
	mustBucket(t, m, "m18-bucket")
	var bucketID int64
	if err := m.Pool.QueryRow(ctx,
		`SELECT id FROM buckets WHERE name = 'm18-bucket'`).Scan(&bucketID); err != nil {
		t.Fatalf("id бакета: %v", err)
	}
	realObject := uuid.New()
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO objects (id, bucket_id, key, size, etag, storage_class, state,
		                      landing, online_until, tenant_id, pool_id, is_current, wrapped_dek)
		  VALUES ($1, $2, 'm18-object', 100, 'etag', 'GLACIER', 'archived', false,
		          now() + interval '1 day', 1, 1, true, 'x'::bytea)`,
		realObject, bucketID); err != nil {
		t.Fatalf("объект: %v", err)
	}
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO archive_copies (spec, phobos_oid, format)
		  VALUES ('side:x', 'oid-owner-none', 1)`); err == nil {
		t.Error("копия без владельца прошла: ровно один владелец не в схеме")
	}
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO archive_copies (pack_id, object_id, spec, phobos_oid, format)
		  VALUES ($1, $2, 'side:x', 'oid-owner-both', 1)`,
		packID, realObject); err == nil {
		t.Error("копия с двумя владельцами прошла: ровно один владелец не в схеме")
	}

	// Повтор той же копии — та же строка, а не вторая: на этом держится
	// идемпотентность перехода состояния (правило 7).
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO archive_copies (pack_id, spec, phobos_oid, format, state)
		  VALUES ($1, 'side:left', 'oid-pending', 1, 'pending')`, packID); err == nil {
		t.Error("повтор копии создал вторую строку: идемпотентность не на чём строить")
	}

	// --- журналы только на добавление ---

	var mediaID int64
	if err := m.Pool.QueryRow(ctx,
		`SELECT id FROM media WHERE label = 'M18TAPE01'`).Scan(&mediaID); err != nil {
		t.Fatalf("id ленты: %v", err)
	}
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO media_events (media_id, event, detail)
		  VALUES ($1, 'loaded', '{"drive": "M18DRIVE1"}')`, mediaID); err != nil {
		t.Fatalf("запись в журнал: %v", err)
	}
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO drive_events (drive_name, event) VALUES ($1, 'loaded')`, drive); err != nil {
		t.Fatalf("запись в журнал драйва: %v", err)
	}

	// Правка истории больной ленты должна выглядеть как здоровая — ровно то,
	// чего журнал не должен позволять.
	if _, err := m.Pool.Exec(ctx,
		`UPDATE media_events SET event = 'loaded' WHERE media_id = $1`, mediaID); err == nil {
		t.Error("UPDATE журнала прошёл: журнал не защищён в схеме")
	}
	if _, err := m.Pool.Exec(ctx,
		`DELETE FROM drive_events WHERE drive_name = $1`, drive); err == nil {
		t.Error("DELETE из журнала драйва прошёл: журнал не защищён в схеме")
	}

	// Обход возможен, но только явный — с указанием причины. Он остаётся в коде
	// и в логе, а не в «мы же договорились».
	if err := inTxAllowingJournalMutation(ctx, m, func(tx pgx.Tx) error {
		_, e := tx.Exec(ctx,
			`UPDATE media_events SET event = 'unloaded' WHERE media_id = $1`, mediaID)
		return e
	}); err != nil {
		t.Errorf("явный обход не сработал: %v", err)
	}

	// --- Down / повторный Up на наполненной схеме ---

	if _, err := m.Pool.Exec(ctx, section(t, migration0018, "Down")); err != nil {
		t.Fatalf("Down 0018 на схеме с данными: %v", err)
	}
	for _, tbl := range []string{"media_events", "drive_events", "archive_copies", "media", "drives"} {
		if n := tableCount(t, ctx, m, tbl); n != 0 {
			t.Errorf("после Down таблица %s осталась", tbl)
		}
	}
	if _, err := m.Pool.Exec(ctx, section(t, migration0018, "Up")); err != nil {
		t.Fatalf("повторный Up 0018: %v", err)
	}
	// Триггер обязан вернуться вместе со схемой: Down/Up без него — это
	// «журнал снова только на добавление» только на словах.
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO media (library, label, generation, medium_type)
		  VALUES ('m18lib', 'M18AFTER', 'LTO-8', 'LTO-8')`); err != nil {
		t.Fatalf("лента после Up: %v", err)
	}
	if err := m.Pool.QueryRow(ctx,
		`SELECT id FROM media WHERE label = 'M18AFTER'`).Scan(&mediaID); err != nil {
		t.Fatalf("id ленты после Up: %v", err)
	}
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO media_events (media_id, event) VALUES ($1, 'loaded')`, mediaID); err != nil {
		t.Fatalf("журнал после Up: %v", err)
	}
	if _, err := m.Pool.Exec(ctx,
		`DELETE FROM media_events WHERE media_id = $1`, mediaID); err == nil {
		t.Error("после Down/Up журнал перестал быть только на добавление")
	}
}

// mustInsertMedia кладёт ленту в состоянии registered.
func mustInsertMedia(t *testing.T, ctx context.Context, m *meta.Meta, label, gen string, writable bool) {
	t.Helper()
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO media (library, label, generation, medium_type, writable,
		                    magazine, home_slot, capacity_bytes, free_bytes, format_version)
		  VALUES ('m18lib', $1, $2, $2, $3, 'mag-1', 'S03', 1000, 800, 1)`,
		label, gen, writable); err != nil {
		t.Fatalf("лента %s: %v", label, err)
	}
}

// inTxAllowingJournalMutation выполняет fn в транзакции с явным обходом
// защиты журнала. SET LOCAL — значит обход живёт ровно эту транзакцию.
//
// fn обязана работать с tx, а не с пулом: SET LOCAL действует на соединение и
// транзакцию, а запрос из пула возьмёт другое соединение и protections не
// увидит — тест тогда проверяет собственную невнимательность, а не триггер.
func inTxAllowingJournalMutation(ctx context.Context, m *meta.Meta, fn func(pgx.Tx) error) error {
	tx, err := m.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SET LOCAL tapegw.journal_override = 'тест исправления истории'`); err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// columnOutside считает колонку во всех таблицах схемы, кроме указанной.
func columnOutside(t *testing.T, ctx context.Context, m *meta.Meta, col, except string) int {
	t.Helper()
	var n int
	if err := m.Pool.QueryRow(ctx,
		`SELECT count(*) FROM information_schema.columns
		  WHERE table_schema = current_schema() AND column_name = $1
		    AND table_name <> $2`, col, except).Scan(&n); err != nil {
		t.Fatalf("счётчик колонок %s: %v", col, err)
	}
	return n
}

func columnIn(t *testing.T, ctx context.Context, m *meta.Meta, table, col string) int {
	t.Helper()
	var n int
	if err := m.Pool.QueryRow(ctx,
		`SELECT count(*) FROM information_schema.columns
		  WHERE table_schema = current_schema() AND table_name = $1 AND column_name = $2`,
		table, col).Scan(&n); err != nil {
		t.Fatalf("колонка %s.%s: %v", table, col, err)
	}
	return n
}

func splitFields(s string) []string {
	var out []string
	f := ""
	for _, r := range s {
		if r == ' ' {
			if f != "" {
				out = append(out, f)
			}
			f = ""
			continue
		}
		f += string(r)
	}
	if f != "" {
		out = append(out, f)
	}
	return out
}
