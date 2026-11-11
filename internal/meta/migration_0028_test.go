package meta_test

import (
	"context"
	"fmt"
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
//
// Порядок уже записанных событий при обратном ходе (Up по заполненной таблице)
// сторожит отдельный тест ниже: там посев идёт вперемешку, а здесь — по порядку
// номеров, и проверка порядка прошла бы в нём сама у себя.

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
// **внутри** тенанта, и на одном тенанте это не проверяется вовсе. Возвращает id
// бакета посева.
func seedJournalEvents(t *testing.T, ctx context.Context, m *meta.Meta) int64 {
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
	return bucketID
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

	bucketID := seedJournalEvents(t, ctx, m)
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
	// CHECK «номер положительный» обязан вернуться в каталог, а не только в
	// пересказ: без него в журнал ложится ноль, а код до 0028 выдаёт номера как
	// `journal_seq + 1` именно от нуля.
	if n := countRow(t, ctx, m,
		`SELECT count(*) FROM pg_constraint c
		   JOIN pg_class tbl ON tbl.oid = c.conrelid
		  WHERE tbl.relname = 'journal_events' AND c.contype = 'c'
		    AND pg_get_constraintdef(c.oid) LIKE '%journal_seq%'
		    AND tbl.relnamespace = current_schema()::regnamespace`); n != 1 {
		t.Errorf("CHECK на journal_seq после Down: %d, хотели 1", n)
	}
	// Счётчик обязан догнать номера у **всех** тенантов, а не только у того, на
	// котором ниже что-то попробуют: проверка по данным, а не по одной строке.
	if bad := countRow(t, ctx, m,
		`SELECT count(*) FROM tenants t
		   JOIN (SELECT tenant_id, max(journal_seq) AS mx
		           FROM journal_events GROUP BY tenant_id) s ON s.tenant_id = t.id
		  WHERE t.journal_seq <> s.mx`); bad != 0 {
		t.Errorf("тенантов, у которых tenants.journal_seq <> max(journal_seq): %d — "+
			"после отката первое же событие получит занятый номер", bad)
	}
	// И главное — что после отката работает **старый** путь записи. Пока схема
	// жила на 0028, счётчик тенанта не писался и отстаёт на число событий;
	// если Down его не подтянет, первое же событие получит занятый номер и
	// упадёт по первичному ключу — откат схемы уронил бы все журналируемые
	// операции шлюза. Прогон в транзакции с откатом, чтобы не добавить строку
	// в таблицу, которую дальше считают.
	if err := func() error {
		tx, err := m.Pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()
		var seq int64
		if err := tx.QueryRow(ctx,
			`UPDATE tenants SET journal_seq = journal_seq + 1
			  WHERE id = 1 RETURNING journal_seq`).Scan(&seq); err != nil {
			return fmt.Errorf("счётчик после Down: %w", err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO journal_events (tenant_id, journal_seq, kind, object_id, bucket_id, payload)
			 VALUES (1, $1, 'delete_version', gen_random_uuid(), $2, '{}')`,
			seq, bucketID); err != nil {
			return fmt.Errorf("событие «старым кодом» после Down (seq=%d): %w", seq, err)
		}
		return tx.Rollback(ctx)
	}(); err != nil {
		t.Error(err)
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

// TestMigration0028UpKeepsJournalSeqOrder — Up обязан раздавать id уже
// записанным событиям в порядке прежнего journal_seq, а не в порядке
// физического расположения строк.
//
// Почему тест отдельный: TestMigration0028JournalServiceId сеет события по
// порядку номеров, и там физический порядок совпадает с порядком номеров —
// проверка порядка прошла бы в нём даже без CLUSTER в миграции. Здесь посев
// идёт вперемешку (3, 1, 2), и тест сначала убеждается, что перемешивание
// состоялось: иначе зелёный цвет означал бы «проверяли порядок, которого нет».
//
// Что ломается без порядка: до 0028 события читались по journal_seq, после — по
// id. Штатный путь вставки сериализован блокировкой строки тенанта, поэтому там
// порядки совпадают; расходятся они у строк, вставленных не по номерам, — ручная
// правка журнала (tapegw.journal_override, на которую указывает HINT триггера)
// либо восстановление из дампа в другом порядке вставки. Тогда rebuild-meta
// применит изменения одного объекта в ином порядке: продление retention раньше
// удаления той же версии, например.
func TestMigration0028UpKeepsJournalSeqOrder(t *testing.T) {
	// Схема ровно до 0027: Up применяется к таблице, где journal_seq ещё есть, —
	// так же, как при первом накатывании миграции на живую базу.
	m := testdb.MigrationScratch(t, 27)
	ctx := context.Background()

	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO tenants (id, name, status, pool_id)
		 VALUES (777, 'тестовый тенант 0028', 'active', 1)
		 ON CONFLICT (id) DO NOTHING`); err != nil {
		t.Fatalf("второй тенант: %v", err)
	}

	// Посев вперемешку: порядок вставки и порядок номеров не совпадают. Тег
	// кладётся в payload, чтобы строку можно было опознать после Up, когда
	// journal_seq уже удалена.
	for _, s := range []struct {
		tenant int64
		seqs   []int64
	}{{1, []int64{3, 1, 2}}, {777, []int64{2, 1}}} {
		for _, seq := range s.seqs {
			if _, err := m.Pool.Exec(ctx,
				`INSERT INTO journal_events (tenant_id, journal_seq, kind, bucket_id, payload)
				 VALUES ($1, $2, 'bucket_config', 1, jsonb_build_object('tag', $2::bigint::text))`,
				s.tenant, seq); err != nil {
				t.Fatalf("событие тенанта %d с номером %d: %v", s.tenant, seq, err)
			}
		}
	}

	// tagsIn — теги событий тенанта в указанном порядке. orderBy — литерал этого
	// файла, а не ввод вызывающего: подстановка в SQL здесь только своя.
	tagsIn := func(orderBy string, tenant int64) string {
		t.Helper()
		var s string
		if err := m.Pool.QueryRow(ctx,
			`SELECT string_agg(jsonb_extract_path_text(payload, 'tag'), ',' ORDER BY `+orderBy+`)
			   FROM journal_events WHERE tenant_id = $1`, tenant).Scan(&s); err != nil {
			t.Fatalf("порядок по %s: %v", orderBy, err)
		}
		return s
	}

	wants := []struct {
		tenant int64
		bySeq  string
	}{{1, "1,2,3"}, {777, "1,2"}}

	for _, w := range wants {
		if got := tagsIn("journal_seq", w.tenant); got != w.bySeq {
			t.Fatalf("порядок по journal_seq у тенанта %d = %q, хотели %q — посев неверный",
				w.tenant, got, w.bySeq)
		}
		// Контроль контроля: физический порядок обязан отличаться, иначе
		// «порядок сохранён» ниже ничего не проверяет.
		if phys := tagsIn("ctid", w.tenant); phys == w.bySeq {
			t.Fatalf("у тенанта %d физический порядок (%s) совпал с порядком номеров: "+
				"посев не перемешал строки, и тест проверял порядок, которого нет",
				w.tenant, phys)
		}
	}

	if _, err := m.Pool.Exec(ctx, section(t, migration0028, "Up")); err != nil {
		t.Fatalf("Up 0028 на перемешанной таблице: %v", err)
	}

	for _, w := range wants {
		if got := tagsIn("id", w.tenant); got != w.bySeq {
			t.Errorf("порядок событий тенанта %d по id = %q, а по прежним номерам был %q: "+
				"Up раздаёт id порядком физического расположения строк, и rebuild-meta "+
				"применит эти изменения не в той последовательности, в какой их читали "+
				"по journal_seq (в Up обязан стоять CLUSTER)", w.tenant, got, w.bySeq)
		}
	}
	if n := countRow(t, ctx, m, `SELECT count(DISTINCT id) FROM journal_events`); n != 5 {
		t.Errorf("различных id: %d, событий 5 — перемешанный посев пережил Up не полностью", n)
	}
}
