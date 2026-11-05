package meta_test

import (
	"context"
	"fmt"
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
		{"drives", "name serial library generation model firmware state media_id " +
			"scsi_id attrs updated_at"},
		{"media", "id label barcode library_cached kind generation medium_type worm pool_id side " +
			"lifecycle location health last_verified_at magazine home_slot " +
			"in_library_cache current_drive read_only_reason capacity_bytes_cached " +
			"free_bytes_cached format_version attrs created_at updated_at"},
		{"archive_copies", "id pack_id object_id spec phobos_oid phobos_version " +
			"format medium_id state attrs created_at updated_at done_at"},
		{"media_events", "id media_id event detail occurred_at"},
		{"drive_events", "id drive_name event detail occurred_at"},
	} {
		for _, col := range splitFields(tc.cols) {
			if n := columnIn(t, ctx, m, tc.table, col); n != 1 {
				t.Errorf("%s.%s отсутствует", tc.table, col)
			}
		}
	}

	// Носитель описывается ссылкой archive_copies.medium_id и больше нигде:
	// колонка с меткой ленты в третьей таблице — второе, расходящееся с первой
	// мнение о том, где лежат данные. Исключений нет: ADR-0022 делает копию
	// отдельным объектом Phobos на одной ленте, поэтому даже массив носителей
	// у копии не нужен.
	for _, col := range []string{"medium", "phobos_medium"} {
		if n := columnAnywhere(t, ctx, m, col); n != 0 {
			t.Errorf("колонка %s существует: носитель — ссылка medium_id у копии", col)
		}
	}

	// --- drives и media ---

	const drive = "M18DRIVE1"
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO drives (name, serial, library, generation, model, firmware, state, scsi_id)
		  VALUES ($1, 'SN-M18-1', 'm18lib', 'LTO-8', 'TestModel', '1.0', 'online',
		          '/dev/tape/by-id/m18-1')`, drive); err != nil {
		t.Fatalf("драйв: %v", err)
	}
	// Два имени на один серийник — два mover'а на один привод.
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO drives (name, serial, library, generation)
		  VALUES ('M18DRIVE2', 'SN-M18-1', 'm18lib', 'LTO-8')`); err == nil {
		t.Error("второе имя на серийник прошло: один привод может получить два mover'а")
	}

	mustInsertMedia(t, ctx, m, "M18TAPE01", "LTO-8")

	// Метка уникальна на уровне схемы (см. комментарий в миграции).
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO media (library_cached, label, generation, medium_type)
		  VALUES ('m18lib', 'M18TAPE01', 'LTO-8', 'LTO-8')`); err == nil {
		t.Error("вторая строка на метку M18TAPE01 прошла: уникальность метки не в схеме")
	}

	// Жизненный цикл и местоположение независимы (0013): «сливается и сейчас в
	// драйве» — нормальное состояние при сливе, и оно должно выражаться.
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO media (library_cached, label, generation, medium_type, lifecycle,
		                    location, current_drive)
		  VALUES ('m18lib', 'M18DRAIN', 'LTO-7', 'LTO-7', 'evacuating', 'in_drive', $1)`,
		drive); err != nil {
		t.Errorf("сливаемая лента в драйве: %v", err)
	}
	// «В драйве» без указания в каком — состояние, которое нельзя ни
	// проверить, ни починить.
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO media (library_cached, label, generation, medium_type, location)
		  VALUES ('m18lib', 'M18NODRIVE', 'LTO-7', 'LTO-7', 'in_drive')`); err == nil {
		t.Error("location=in_drive без current_drive прошло: состояние непроверяемо")
	}
	// Носитель недоступен — состояние, которым живёт recaller (Ф1.3).
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO media (library_cached, label, generation, medium_type, location)
		  VALUES ('m18lib', 'M18LOST', 'LTO-7', 'LTO-7', 'lost')`); err != nil {
		t.Errorf("носитель потерян: %v", err)
	}

	// Закрытая политикой лента обязана иметь причину: «закрыта» без причины
	// неотличима от закрытой по ошибке.
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO media (library_cached, label, generation, medium_type, lifecycle)
		  VALUES ('m18lib', 'M18RO-NOREASON', 'LTO-7', 'LTO-7', 'read_only')`); err == nil {
		t.Error("read_only без причины прошёл: слой 2 запрета не объяснён")
	}
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO media (library_cached, label, generation, medium_type, read_only_reason)
		  VALUES ('m18lib', 'M18REASON-RO', 'LTO-7', 'LTO-7', 'поколение выведено')`); err == nil {
		t.Error("причина без lifecycle=read_only прошла: два источника правды")
	}

	// Свободных байт не больше ёмкости.
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO media (library_cached, label, generation, medium_type,
		                    capacity_bytes_cached, free_bytes_cached)
		  VALUES ('m18lib', 'M18BADGEO', 'LTO-8', 'LTO-8', 100, 200)`); err == nil {
		t.Error("free > capacity прошёл: геометрия не проверена")
	}

	// --- archive_copies ---

	mustBucket(t, m, "m18-bucket")
	var bucketID int64
	if err := m.Pool.QueryRow(ctx,
		`SELECT id FROM buckets WHERE name = 'm18-bucket'`).Scan(&bucketID); err != nil {
		t.Fatalf("id бакета: %v", err)
	}
	packID := uuid.New()
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO packs (id, pool_id, kind, total_bytes, live_bytes, members_total, members_live)
		  VALUES ($1, 1, 'data', 1000, 600, 2, 1)`, packID); err != nil {
		t.Fatalf("пачка: %v", err)
	}
	var mediumID int64
	if err := m.Pool.QueryRow(ctx,
		`SELECT id FROM media WHERE label = 'M18TAPE01'`).Scan(&mediumID); err != nil {
		t.Fatalf("id ленты: %v", err)
	}

	// Версия формата копии обязательна.
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO archive_copies (pack_id, spec, phobos_oid, medium_id, state)
		  VALUES ($1, 'side:left', 'oid-no-format', $2, 'done')`,
		packID, mediumID); err == nil {
		t.Error("копия без версии формата прошла: формат копии обязан быть в схеме")
	}
	// Готовая копия обязана знать носитель.
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO archive_copies (pack_id, spec, phobos_oid, format, state)
		  VALUES ($1, 'side:left', 'oid-no-medium', 1, 'done')`, packID); err == nil {
		t.Error("копия state=done без носителя прошла: её нельзя ни прочитать, ни удалить")
	}
	// Носитель обязан быть из реестра: метка в массиве не была связана ни с чем,
	// и опечатка прошла бы незамеченной.
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO archive_copies (pack_id, spec, phobos_oid, format, medium_id, state, sha256)
		  VALUES ($1, 'side:left', 'oid-bad-medium', 1, 999999, 'done',
		          decode('000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f', 'hex'))`,
		packID); err == nil {
		t.Error("копия на несуществующий носитель прошла: связь с реестром не работает")
	}
	// Копия без носителя легальна, пока не готова.
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO archive_copies (pack_id, spec, phobos_oid, format, state)
		  VALUES ($1, 'side:left', 'oid-pending', 1, 'pending')`, packID); err != nil {
		t.Errorf("копия в ожидании: %v", err)
	}

	// Ровно один владелец. Объект для второго случая берётся настоящий: иначе
	// проверка падает по внешнему ключу, а не по CHECK.
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

	// Повтор той же копии — та же строка, а не вторая (правило 7).
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO archive_copies (pack_id, spec, phobos_oid, format, state)
		  VALUES ($1, 'side:left', 'oid-pending', 1, 'pending')`, packID); err == nil {
		t.Error("повтор копии создал вторую строку: идемпотентность не на чём строить")
	}

	// Одна живая копия на спецификацию. Другой phobos_oid — это ровно тот
	// случай, который UNIQUE (phobos_oid, phobos_version, spec) пропускает:
	// две копии 'left' одной пачки, обе живые, и reconciler не знает, какая
	// настоящая.
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO archive_copies (pack_id, spec, phobos_oid, format, state)
		  VALUES ($1, 'side:left', 'oid-pending-dup', 1, 'writing')`, packID); err == nil {
		t.Error("вторая живая копия 'side:left' прошла: reconciler не отличит настоящую")
	}
	// Мёртвая копия той же спецификации не мешает: именно так выглядит замена.
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO archive_copies (pack_id, spec, phobos_oid, format, state)
		  VALUES ($1, 'side:left', 'oid-pending-dead', 1, 'dead')`, packID); err != nil {
		t.Errorf("мёртвая копия той же спецификации: %v", err)
	}

	// --- журналы только на добавление ---

	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO media_events (media_id, event, detail)
		  VALUES ($1, 'loaded', '{"drive": "M18DRIVE1"}')`, mediumID); err != nil {
		t.Fatalf("запись в журнал: %v", err)
	}
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO drive_events (drive_name, event) VALUES ($1, 'loaded')`, drive); err != nil {
		t.Fatalf("запись в журнал драйва: %v", err)
	}

	if _, err := m.Pool.Exec(ctx,
		`UPDATE media_events SET event = 'loaded' WHERE media_id = $1`, mediumID); err == nil {
		t.Error("UPDATE журнала прошёл: журнал не защищён в схеме")
	}
	if _, err := m.Pool.Exec(ctx,
		`DELETE FROM drive_events WHERE drive_name = $1`, drive); err == nil {
		t.Error("DELETE из журнала драйва прошёл: журнал не защищён в схеме")
	}

	// Ленты по модели не удаляются, а выводятся, и история живёт дольше строки:
	// иначе вывод поколения стирал бы доказательства, по которым его решали.
	if _, err := m.Pool.Exec(ctx,
		`DELETE FROM media WHERE id = $1`, mediumID); err == nil {
		t.Error("удаление ленты с журналом прошло: история не переживёт вывод поколения")
	}
	// Действие ссылки проверяется напрямую, а не через попытку удаления:
	// каскад упирается в тот же триггер «только на добавление», и тест на
	// DELETE прошёл бы зелёным и при ON DELETE CASCADE — то есть проверял
	// собственную невнимательность, а не ограничение.
	for _, tbl := range []string{"media_events", "drive_events"} {
		var action string
		if err := m.Pool.QueryRow(ctx,
			`SELECT rc.delete_rule FROM information_schema.referential_constraints rc
			  JOIN information_schema.table_constraints tc ON rc.constraint_name = tc.constraint_name
			  WHERE tc.table_name = $1 AND tc.constraint_type = 'FOREIGN KEY'
			    AND tc.table_schema = current_schema()
			    AND rc.constraint_schema = current_schema()`, tbl).Scan(&action); err != nil {
			t.Fatalf("действие ссылки для %s: %v", tbl, err)
		}
		if action != "RESTRICT" {
			t.Errorf("%s: действие при удалении родителя %s, ожидалось RESTRICT — "+
				"история не переживёт вывод поколения", tbl, action)
		}
	}

	// Явный обход. Проверяем не «не было исключения», а что строка **действительно**
	// изменилась: BEFORE-триггер, возвращающий NULL, исключения не даёт и
	// молча отменяет операцию, и такой «работающий обход» выглядел бы правдой.
	if err := inTxAllowingJournalMutation(ctx, m, func(tx pgx.Tx) error {
		tag, e := tx.Exec(ctx,
			`UPDATE media_events SET event = 'unloaded' WHERE media_id = $1`, mediumID)
		if e != nil {
			return e
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("UPDATE с обходом затронул %d строк, ожидалась 1", tag.RowsAffected())
		}
		return nil
	}); err != nil {
		t.Errorf("явный обход UPDATE: %v", err)
	}
	var eventNow string
	if err := m.Pool.QueryRow(ctx,
		`SELECT event FROM media_events WHERE media_id = $1`, mediumID).Scan(&eventNow); err != nil {
		t.Fatalf("чтение журнала после обхода: %v", err)
	}
	if eventNow != "unloaded" {
		t.Errorf("после обхода в журнале %q, ожидалось unloaded: обход не работает", eventNow)
	}

	if err := inTxAllowingJournalMutation(ctx, m, func(tx pgx.Tx) error {
		tag, e := tx.Exec(ctx, `DELETE FROM media_events WHERE media_id = $1`, mediumID)
		if e != nil {
			return e
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("DELETE с обходом затронул %d строк, ожидалась 1", tag.RowsAffected())
		}
		return nil
	}); err != nil {
		t.Errorf("явный обход DELETE: %v", err)
	}
	var left int
	if err := m.Pool.QueryRow(ctx,
		`SELECT count(*) FROM media_events WHERE media_id = $1`, mediumID).Scan(&left); err != nil {
		t.Fatalf("чтение журнала после обхода: %v", err)
	}
	if left != 0 {
		t.Errorf("после обхода в журнале осталось %d строк: обход DELETE не работает", left)
	}

	// --- Down / повторный Up на наполненной схеме ---

	if _, err := m.Pool.Exec(ctx, section(t, migration0020, "Down")); err != nil {
		t.Fatalf("Down 0020 перед 0019: %v", err)
	}
	if _, err := m.Pool.Exec(ctx, section(t, migration0019, "Down")); err != nil {
		t.Fatalf("Down 0019 перед 0018: %v", err)
	}
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
	if _, err := m.Pool.Exec(ctx, section(t, migration0020, "Up")); err != nil {
		t.Fatalf("Up 0020: %v", err)
	}
	if _, err := m.Pool.Exec(ctx, section(t, migration0019, "Up")); err != nil {
		t.Fatalf("повторный Up 0019 после 0018: %v", err)
	}
	// Триггер обязан вернуться вместе со схемой: Down/Up без него — это
	// «журнал снова только на добавление» только на словах.
	if err := m.Pool.QueryRow(ctx,
		`SELECT id FROM media WHERE label = 'M18TAPE01'`).Scan(&mediumID); err != nil {
		// Down унёс media и media_events вместе, лента вернулась пустой.
		if _, err := m.Pool.Exec(ctx,
			`INSERT INTO media (library_cached, label, generation, medium_type)
			  VALUES ('m18lib', 'M18AFTER', 'LTO-8', 'LTO-8')`); err != nil {
			t.Fatalf("лента после Up: %v", err)
		}
		if err := m.Pool.QueryRow(ctx,
			`SELECT id FROM media WHERE label = 'M18AFTER'`).Scan(&mediumID); err != nil {
			t.Fatalf("id ленты после Up: %v", err)
		}
	}
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO media_events (media_id, event) VALUES ($1, 'loaded')`, mediumID); err != nil {
		t.Fatalf("журнал после Up: %v", err)
	}
	if _, err := m.Pool.Exec(ctx,
		`DELETE FROM media_events WHERE media_id = $1`, mediumID); err == nil {
		t.Error("после Down/Up журнал перестал быть только на добавление")
	}
}

// mustInsertMedia кладёт ленту в состоянии active/in_slot с кэшем геометрии.
func mustInsertMedia(t *testing.T, ctx context.Context, m *meta.Meta, label, gen string) {
	t.Helper()
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO media (library_cached, label, generation, medium_type, lifecycle,
		                    magazine, home_slot, side, capacity_bytes_cached,
		                    free_bytes_cached, format_version)
		  VALUES ('m18lib', $1, $2, $2, 'active', 'mag-1', 'S03', 'left', 1000, 800, 1)`,
		label, gen); err != nil {
		t.Fatalf("лента %s: %v", label, err)
	}
}

// inTxAllowingJournalMutation выполняет fn в транзакции с явным обходом
// защиты журнала. SET LOCAL — значит обход живёт ровно эту транзакцию.
//
// fn обязана работать с tx, а не с пулом: SET LOCAL действует на соединение и
// транзакцию, а запрос из пула возьмёт другое соединение и защиту не увидит —
// тест тогда проверяет собственную невнимательность, а не триггер.
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

// columnAnywhere считает колонку во всех таблицах схемы без исключений.
func columnAnywhere(t *testing.T, ctx context.Context, m *meta.Meta, col string) int {
	t.Helper()
	var n int
	if err := m.Pool.QueryRow(ctx,
		`SELECT count(*) FROM information_schema.columns
		  WHERE table_schema = current_schema() AND column_name = $1`, col).Scan(&n); err != nil {
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
