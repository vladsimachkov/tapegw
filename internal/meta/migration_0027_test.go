package meta_test

import (
	"context"
	"testing"

	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// Тест миграции 0027 (события журнала метаданных).
//
// Правило 11: Up и Down — текстом файла, на таблице с данными, проверка по
// каталогу БД. Здесь каталог важен особенно: enum и триггер «только на
// добавление» не видны по коду возврата goose, а без них журнал перестаёт быть
// журналом молча.

// Имя файла объявлено рядом с section (migration_0007_test.go): тот же файл
// нужен тестам 0017–0019, чтобы снимать 0027 перед Down 0018.

func TestMigration0027JournalEvents(t *testing.T) {
	m := testdb.MigrationScratch(t, testdb.AllVersions)
	ctx := context.Background()

	// Данные для проверки Down: Down обязан пройти на заполненной таблице, а не
	// на пустой схеме. Посев идёт до снятия 0028 и через путь кода: код пишет
	// событие без номера (так задано миграцией 0028), и на схеме 0027, где
	// номер обязателен, он не пройдёт — ниже номера расставляются явно.
	mustBucket(t, m, "j27-bucket")
	var bucketID int64
	if err := m.Pool.QueryRow(ctx,
		`SELECT id FROM buckets WHERE name = 'j27-bucket'`).Scan(&bucketID); err != nil {
		t.Fatal(err)
	}
	// Создание бакета — событие журнала: оно и данные, и заодно проверка, что
	// путь кода работает на схеме из миграций, а не только на общей.
	if n := countRow(t, ctx, m, `SELECT count(*) FROM journal_events`); n != 1 {
		t.Fatalf("после CreateBucket событий %d, хотели 1 — посева нет", n)
	}

	// 0028 снимается до Down 0027: она убирает `journal_seq` — колонку, на
	// которой стоит первичный ключ 0027, и без снятия тест проверял бы не схему
	// 0027. В настоящем `goose down` порядок обратный по номерам.
	if _, err := m.Pool.Exec(ctx, section(t, migration0028, "Down")); err != nil {
		t.Fatalf("снять 0028 перед Down 0027: %v", err)
	}

	// Каталог, а не «миграция вернула ноль»: goose умеет записать версию
	// применённой, не выполнив шагов.
	if n := columnIn(t, ctx, m, "journal_events", "journal_seq"); n != 1 {
		t.Fatal("journal_events.journal_seq отсутствует")
	}
	if n := columnIn(t, ctx, m, "tenants", "journal_seq"); n != 1 {
		t.Fatal("tenants.journal_seq отсутствует")
	}
	var notNull, hasDefault bool
	if err := m.Pool.QueryRow(ctx,
		`SELECT is_nullable = 'NO', column_default IS NOT NULL
		   FROM information_schema.columns
		  WHERE table_schema = current_schema()
		    AND table_name = 'tenants' AND column_name = 'journal_seq'`).
		Scan(&notNull, &hasDefault); err != nil {
		t.Fatal(err)
	}
	if !notNull || !hasDefault {
		t.Error("tenants.journal_seq обязан быть NOT NULL с DEFAULT 0: " +
			"пропущенное значение должно означать «событий не было», а не null")
	}

	var kinds []string
	if err := func() error {
		rows, err := m.Pool.Query(ctx,
			`SELECT e.enumlabel FROM pg_enum e
			   JOIN pg_type t ON t.oid = e.enumtypid
			  WHERE t.typname = 'journal_event_kind'
			    AND t.typnamespace = current_schema()::regnamespace
			  ORDER BY e.enumsortorder`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var s string
			if err := rows.Scan(&s); err != nil {
				return err
			}
			kinds = append(kinds, s)
		}
		return rows.Err()
	}(); err != nil {
		t.Fatalf("значения enum: %v", err)
	}
	want := []string{"retention", "legal_hold", "delete_marker", "delete_version", "bucket_config"}
	if len(kinds) != len(want) {
		t.Fatalf("значений enum %d (%v), хотели %d", len(kinds), kinds, len(want))
	}
	for i := range want {
		if kinds[i] != want[i] {
			t.Errorf("значение %d = %q, хотели %q", i, kinds[i], want[i])
		}
	}

	// Триггер «только на добавление» обязан висеть: без него журнал — обычная
	// таблица, которую можно переписать задним числом.
	if n := countRow(t, ctx, m,
		`SELECT count(*) FROM pg_trigger tg
		   JOIN pg_class c ON c.oid = tg.tgrelid
		  WHERE c.relname = 'journal_events' AND NOT tg.tgisinternal
		    AND c.relnamespace = current_schema()::regnamespace`); n != 1 {
		t.Errorf("триггеров append-only у journal_events: %d, хотели 1", n)
	}

	// Ещё одно событие — уже с явным номером: на схеме 0027 номер обязан
	// называть вызывающий. Счётчик тенанта при этом 0 (путь кода его больше не
	// пишет), и ниже он поднимается до номера последнего события, как это делал
	// код до 0028.
	if _, err := m.Pool.Exec(ctx,
		`UPDATE tenants SET journal_seq = 2 WHERE id = 1`); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO journal_events (tenant_id, journal_seq, kind, bucket_id, payload)
		 VALUES (1, 2, 'bucket_config', $1, '{"bucket":"j27-bucket"}')`, bucketID); err != nil {
		t.Fatalf("событие в заполненную таблицу: %v", err)
	}

	if _, err := m.Pool.Exec(ctx, section(t, migration0027, "Down")); err != nil {
		t.Fatalf("Down 0027 на таблице с данными: %v", err)
	}
	if n := tableCount(t, ctx, m, "journal_events"); n != 0 {
		t.Error("после Down таблица journal_events осталась")
	}
	if n := columnIn(t, ctx, m, "tenants", "journal_seq"); n != 0 {
		t.Error("после Down tenants.journal_seq осталась")
	}
	if n := countRow(t, ctx, m,
		`SELECT count(*) FROM pg_type
		  WHERE typname = 'journal_event_kind'
		    AND typnamespace = current_schema()::regnamespace`); n != 0 {
		t.Error("после Down тип journal_event_kind остался")
	}

	if _, err := m.Pool.Exec(ctx, section(t, migration0027, "Up")); err != nil {
		t.Fatalf("повторный Up 0027: %v", err)
	}
	if n := tableCount(t, ctx, m, "journal_events"); n != 1 {
		t.Fatal("после повторного Up таблицы journal_events нет")
	}
	if n := columnIn(t, ctx, m, "tenants", "journal_seq"); n != 1 {
		t.Error("после повторного Up tenants.journal_seq нет")
	}
	// Счётчик после Down+Up обязан быть пригоден к работе: NOT NULL и 0, а не
	// null — иначе первое же событие упало бы по NOT NULL.
	if _, err := m.Pool.Exec(ctx,
		`UPDATE tenants SET journal_seq = journal_seq + 1 WHERE id = 1`); err != nil {
		t.Fatalf("счётчик после Up не работает: %v", err)
	}
	// CHECK про «снимок бакета без версии» обязан вернуться: без него в журнал
	// начнут писаться события с перепутанными полями.
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO journal_events (tenant_id, journal_seq, kind, bucket_id, payload)
		 VALUES (1, 2, 'bucket_config', $1, '{}')`, bucketID); err != nil {
		t.Fatalf("снимок бакета после Up: %v", err)
	}
	// Обратный случай того же CHECK: событие про версию обязано версию назвать.
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO journal_events (tenant_id, journal_seq, kind, bucket_id, payload)
		 VALUES (1, 3, 'delete_version', $1, '{}')`, bucketID); err == nil {
		t.Error("удаление версии без object_id прошло: CHECK не на месте")
	}

	// Вернуть схему к тому виду, который оставил полный набор миграций. Это не
	// уборка: Up 0028 поверх только что пересозданной 0027 таблицы — отдельный
	// случай, которого в линейном `goose up` не бывает.
	if _, err := m.Pool.Exec(ctx, section(t, migration0028, "Up")); err != nil {
		t.Fatalf("вернуть 0028 после Up 0027: %v", err)
	}
}

func countRow(t *testing.T, ctx context.Context, m *meta.Meta, q string, args ...any) int {
	t.Helper()
	var n int
	if err := m.Pool.QueryRow(ctx, q, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return n
}
