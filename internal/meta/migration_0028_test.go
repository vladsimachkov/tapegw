package meta_test

import (
	"context"
	"testing"

	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// Тест миграции 0028 (служебный id события журнала вместо плотного номера).
//
// Правило 11: Up и Down — текстом файла, на таблице с данными, проверка по
// каталогу БД. Здесь каталог важен особенно: первичный ключ, identity и
// триггер «только на добавление» не видны по коду возврата goose, а без них
// журнал перестаёт быть журналом молча.
//
// Down в этой миграции не «вернуть как было», а **перенумерать** живую таблицу:
// плотные номера восстанавливаются порядком id внутри тенанта. На пустой схеме
// такой Down ничего не проверяет, поэтому данные сеются до, а плотность
// проверяется после — и по двум тенантам, потому что на одном «плотно внутри
// тенанта» и «просто по порядку» неразличимы.

// indexCols — колонки индекса в порядке индексирования, через запятую. Пустая
// строка означает «индекса нет»: COALESCE здесь затем, чтобы отсутствующий
// индекс описывался как «коколок нет», а не падал сканом NULL в *string.
func indexCols(t *testing.T, ctx context.Context, m *meta.Meta, index string) string {
	t.Helper()
	var cols string
	if err := m.Pool.QueryRow(ctx,
		`SELECT COALESCE(string_agg(a.attname, ',' ORDER BY k.ord), '')
		   FROM pg_index i
		   JOIN pg_class c ON c.oid = i.indexrelid
		   JOIN pg_class tbl ON tbl.oid = i.indrelid
		   JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
		   JOIN pg_attribute a ON a.attrelid = tbl.oid AND a.attnum = k.attnum
		  WHERE c.relname = $1
		    AND tbl.relnamespace = current_schema()::regnamespace`, index).
		Scan(&cols); err != nil {
		t.Fatalf("колонки индекса %s: %v", index, err)
	}
	return cols
}

// pkCols — колонки первичного ключа таблицы в порядке индексирования.
func pkCols(t *testing.T, ctx context.Context, m *meta.Meta, table string) string {
	t.Helper()
	var cols string
	if err := m.Pool.QueryRow(ctx,
		`SELECT COALESCE(string_agg(a.attname, ',' ORDER BY k.ord), '')
		   FROM pg_constraint con
		   JOIN pg_class tbl ON tbl.oid = con.conrelid
		   JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord) ON true
		   JOIN pg_attribute a ON a.attrelid = tbl.oid AND a.attnum = k.attnum
		  WHERE tbl.relname = $1 AND con.contype = 'p'
		    AND tbl.relnamespace = current_schema()::regnamespace`, table).
		Scan(&cols); err != nil {
		t.Fatalf("первичный ключ %s: %v", table, err)
	}
	return cols
}

// appendOnlyTriggers — число неслужебных триггеров у таблицы. Для
// journal_events это триггер «только на добавление»: без него журнал —
// обычная таблица, которую можно переписать задним числом.
func appendOnlyTriggers(t *testing.T, ctx context.Context, m *meta.Meta) int {
	t.Helper()
	return countRow(t, ctx, m,
		`SELECT count(*) FROM pg_trigger tg
		   JOIN pg_class c ON c.oid = tg.tgrelid
		  WHERE c.relname = 'journal_events' AND NOT tg.tgisinternal
		    AND c.relnamespace = current_schema()::regnamespace`)
}

// seedJournalEvents — данные для проверки Down: два тенанта с разным числом
// событий. Тенант 777 нужен именно вторым: перенумерация обязана быть плотной
// **внутри** тенанта, и на одном тенанте это не проверяется вовсе.
func seedJournalEvents(t *testing.T, ctx context.Context, m *meta.Meta) {
	t.Helper()

	// mustBucket уже оставил одно событие создания бакета — оно тоже в счёт.
	mustBucket(t, m, "j28-bucket")
	var bucketID int64
	if err := m.Pool.QueryRow(ctx,
		`SELECT id FROM buckets WHERE name = 'j28-bucket'`).Scan(&bucketID); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO tenants (id, name, status, pool_id)
		 VALUES (777, 'тестовый тенант 0028', 'active', 1)
		 ON CONFLICT (id) DO NOTHING`); err != nil {
		t.Fatalf("второй тенант: %v", err)
	}
	// bucket_id без внешнего ключа сознательно (миграция 0027: событие обязано
	// пережить строку, о которой оно), поэтому события второго тенанта могут
	// указать любой бакет — плотность нумерации от этого не зависит.
	for _, e := range []struct {
		tenant int64
		n      int
	}{{1, 2}, {777, 4}} {
		for i := 0; i < e.n; i++ {
			if _, err := m.Pool.Exec(ctx,
				`INSERT INTO journal_events (tenant_id, kind, object_id, bucket_id, payload)
				 VALUES ($1, 'delete_version', gen_random_uuid(), $2, '{"key":"k"}')`,
				e.tenant, bucketID); err != nil {
				t.Fatalf("событие тенанта %d: %v", e.tenant, err)
			}
		}
	}
}

func TestMigration0028JournalServiceId(t *testing.T) {
	m := testdb.MigrationScratch(t, testdb.AllVersions)
	ctx := context.Background()

	// --- Каталог после Up -----------------------------------------------

	// id есть, он bigint и он identity со строгой выдачей. ALWAYS, а не BY
	// DEFAULT, — договорённость «у события id выдан схемой», иначе снаружи
	// подставят свой номер и порядок перестанет совпадать с порядком коммитов.
	var isIdentity, generation, dataType string
	if err := m.Pool.QueryRow(ctx,
		`SELECT is_identity, COALESCE(identity_generation, ''), data_type
		   FROM information_schema.columns
		  WHERE table_schema = current_schema()
		    AND table_name = 'journal_events' AND column_name = 'id'`).
		Scan(&isIdentity, &generation, &dataType); err != nil {
		t.Fatal(err)
	}
	if isIdentity != "YES" || generation != "ALWAYS" {
		t.Errorf("journal_events.id: is_identity=%q generation=%q, хотели YES/ALWAYS — "+
			"подменить номер снаружи нельзя", isIdentity, generation)
	}
	if dataType != "bigint" {
		t.Errorf("тип journal_events.id = %q, хотели bigint", dataType)
	}
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO journal_events (id, tenant_id, kind, bucket_id, payload)
		 VALUES (1, 1, 'bucket_config', 1, '{}')`); err == nil {
		t.Error("подставной id в журнал прошёл: identity не ALWAYS")
	}

	if got := pkCols(t, ctx, m, "journal_events"); got != "id" {
		t.Errorf("первичный ключ journal_events = (%s), хотели (id)", got)
	}
	if got := indexCols(t, ctx, m, "journal_events_tenant_id"); got != "tenant_id,id" {
		t.Errorf("индекс journal_events_tenant_id = (%s), хотели (tenant_id,id): по нему "+
			"воркер сегментов и rebuild-meta читают события тенанта по порядку выдачи", got)
	}
	if n := columnIn(t, ctx, m, "journal_events", "journal_seq"); n != 0 {
		t.Error("journal_events.journal_seq осталась: плотный номер теперь на сегменте")
	}
	if n := columnIn(t, ctx, m, "tenants", "journal_seq"); n != 1 {
		t.Error("tenants.journal_seq исчезла: её пишет воркер сегментов, колонка обязана остаться")
	}
	if n := appendOnlyTriggers(t, ctx, m); n != 1 {
		t.Errorf("триггеров «только на добавление» у journal_events: %d, хотели 1", n)
	}

	// --- Данные и Down ---------------------------------------------------

	seedJournalEvents(t, ctx, m)
	// События на месте — иначе Down проверялся бы на пустой таблице, где порядок
	// шагов перенумерации не виден.
	total := countRow(t, ctx, m, `SELECT count(*) FROM journal_events`)
	if total != 7 {
		t.Fatalf("событий %d, хотели 7 — посева нет", total)
	}
	// Триггер обязан не висеть для красоты, а работать. Проверка идёт на
	// заполненной таблице: на пустой UPDATE не задевает ни одной строки и
	// построчный триггер не вызывается вовсе — «ошибки нет» ничего не значит.
	if _, err := m.Pool.Exec(ctx,
		`UPDATE journal_events SET payload = payload`); err == nil {
		t.Error("UPDATE событий журнала прошёл: после 0028 журнал не append-only")
	}

	if _, err := m.Pool.Exec(ctx, section(t, migration0028, "Down")); err != nil {
		t.Fatalf("Down 0028 на таблице с данными: %v", err)
	}

	if n := columnIn(t, ctx, m, "journal_events", "journal_seq"); n != 1 {
		t.Fatal("после Down journal_seq не вернулась")
	}
	if got := pkCols(t, ctx, m, "journal_events"); got != "tenant_id,journal_seq" {
		t.Errorf("первичный ключ после Down = (%s), хотели (tenant_id,journal_seq)", got)
	}
	if n := countRow(t, ctx, m,
		`SELECT count(*) FROM pg_indexes
		  WHERE schemaname = current_schema() AND indexname = 'journal_events_tenant_id'`); n != 0 {
		t.Error("после Down индекс journal_events_tenant_id остался")
	}
	if n := columnIn(t, ctx, m, "journal_events", "id"); n != 0 {
		t.Error("после Down колонка id осталась")
	}
	if n := countRow(t, ctx, m,
		`SELECT count(*) FROM pg_class
		  WHERE relname = 'journal_events_id_seq'
		    AND relnamespace = current_schema()::regnamespace`); n != 0 {
		t.Error("после Down последовательность identity осталась: её создала колонка id, " +
			"и без неё повторный Up возьмёт номера «в продолжение»")
	}
	if n := appendOnlyTriggers(t, ctx, m); n != 1 {
		t.Errorf("триггеров «только на добавление» после Down: %d, хотели 1: Down снимает "+
			"триггер ради перенумерации и обязан поставить обратно", n)
	}
	// Тот же обход, что и в Up, но после Down: перенумерация не должна была
	// оставить защиту выключенной.
	if _, err := m.Pool.Exec(ctx,
		`UPDATE journal_events SET payload = payload`); err == nil {
		t.Error("после Down UPDATE событий журнала прошёл: триггер не возвращён")
	}

	// Перенумерация плотная **внутри** каждого тенанта: номера от 1 до числа
	// событий и без повторов. Формулировка «min=1, max=count, все различны»
	// сильнее, чем «сумма равна», и именно она падает, если row_number()
	// перестал разбиваться по tenant_id.
	if bad := countRow(t, ctx, m,
		`SELECT count(*) FROM (
		    SELECT tenant_id, count(*) AS n, min(journal_seq) AS lo, max(journal_seq) AS hi,
		           count(DISTINCT journal_seq) AS d
		      FROM journal_events GROUP BY tenant_id
		 ) s WHERE s.lo <> 1 OR s.hi <> s.n OR s.d <> s.n`); bad != 0 {
		t.Errorf("тенантов с неплотной нумерацией после Down: %d — номера восстанавливаются "+
			"порядком id внутри тенанта, иначе первичный ключ (tenant_id, journal_seq) "+
			"не соберётся", bad)
	}
	if n := countRow(t, ctx, m,
		`SELECT count(*) FROM journal_events WHERE journal_seq IS NULL OR journal_seq <= 0`); n != 0 {
		t.Errorf("незаполненных или нулевых номеров после Down: %d", n)
	}
	if n := countRow(t, ctx, m, `SELECT count(*) FROM journal_events`); n != total {
		t.Errorf("после Down событий %d, было %d: перенумерация потеряла строки", n, total)
	}

	// --- Повторный Up -----------------------------------------------------

	if _, err := m.Pool.Exec(ctx, section(t, migration0028, "Up")); err != nil {
		t.Fatalf("повторный Up 0028 на таблице с данными: %v", err)
	}
	if got := pkCols(t, ctx, m, "journal_events"); got != "id" {
		t.Errorf("первичный ключ после повторного Up = (%s), хотели (id)", got)
	}
	if got := indexCols(t, ctx, m, "journal_events_tenant_id"); got != "tenant_id,id" {
		t.Errorf("индекс после повторного Up = (%s), хотели (tenant_id,id)", got)
	}
	if n := columnIn(t, ctx, m, "journal_events", "journal_seq"); n != 0 {
		t.Error("после повторного Up journal_seq осталась")
	}
	if n := countRow(t, ctx, m, `SELECT count(*) FROM journal_events`); n != total {
		t.Errorf("после повторного Up событий %d, было %d", n, total)
	}
	// События пережили Down → Up и остались различимы: у каждого свой id, и по
	// нему же читается порядок.
	if n := countRow(t, ctx, m,
		`SELECT count(DISTINCT id) FROM journal_events`); n != total {
		t.Errorf("различных id после Down → Up: %d, событий %d", n, total)
	}
	if n := appendOnlyTriggers(t, ctx, m); n != 1 {
		t.Errorf("триггеров «только на добавление» после повторного Up: %d, хотели 1", n)
	}
	if _, err := m.Pool.Exec(ctx,
		`UPDATE journal_events SET payload = payload`); err == nil {
		t.Error("после повторного Up UPDATE событий журнала прошёл: защита осталась выключенной")
	}
	// События второго тенанта никуда не делись: Down не должен был их перемешать
	// с первым тенантом.
	if n := countRow(t, ctx, m,
		`SELECT count(*) FROM journal_events WHERE tenant_id = 777`); n != 4 {
		t.Errorf("событий второго тенанта %d, хотели 4", n)
	}
}
