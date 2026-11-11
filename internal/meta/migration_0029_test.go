package meta_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// Тест миграции 0029 (сегменты журнала метаданных).
//
// Правило 11: Up и Down — текстом файла, на таблице с данными, проверка по
// каталогу БД. Здесь это особенно важно: смысл миграции в ограничениях
// («диапазон равен числу событий», «событие ровно в одном сегменте», «признак
// только на добавление»), а их по коду возврата goose не видно вовсе.
//
// Ограничения проверяются и каталогом, и поведением: каталог говорит, что
// ограничение заведено, поведение — что оно работает на живых строках. CHECK на
// пустой таблице и триггер на пустой таблице молчат, поэтому посев идёт до
// проверки, а не после.

// uniqueConstraints — уникальные ограничения таблицы как «колонки» через ';'.
func uniqueConstraints(t *testing.T, ctx context.Context, m *meta.Meta, table string) string {
	t.Helper()
	var s string
	if err := m.Pool.QueryRow(ctx,
		`SELECT COALESCE(string_agg(cols, ';' ORDER BY cols), '')
		   FROM (
		     SELECT string_agg(a.attname, ',' ORDER BY k.ord) AS cols
		       FROM pg_constraint con
		       JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord) ON true
		       JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.attnum
		      WHERE con.contype = 'u' AND con.conrelid = to_regclass($1)
		      GROUP BY con.oid
		   ) u`, table).Scan(&s); err != nil {
		t.Fatalf("уникальные ограничения %s: %v", table, err)
	}
	return s
}

// foreignKeys — определения внешних ключов таблицы, как их видит каталог.
// pg_get_constraintdef, а не разложенные поля: определение читается и в падении
// теста говорит правду про правило удаления.
func foreignKeys(t *testing.T, ctx context.Context, m *meta.Meta, table string) string {
	t.Helper()
	var s string
	if err := m.Pool.QueryRow(ctx,
		`SELECT COALESCE(string_agg(pg_get_constraintdef(oid), ';' ORDER BY conname), '')
		   FROM pg_constraint
		  WHERE contype = 'f' AND conrelid = to_regclass($1)`, table).Scan(&s); err != nil {
		t.Fatalf("внешние ключи %s: %v", table, err)
	}
	return s
}

// checkConstraints — определения CHECK-ограничений таблицы.
func checkConstraints(t *testing.T, ctx context.Context, m *meta.Meta, table string) string {
	t.Helper()
	var s string
	if err := m.Pool.QueryRow(ctx,
		`SELECT COALESCE(string_agg(pg_get_constraintdef(oid), ';' ORDER BY conname), '')
		   FROM pg_constraint
		  WHERE contype = 'c' AND conrelid = to_regclass($1)`, table).Scan(&s); err != nil {
		t.Fatalf("CHECK таблицы %s: %v", table, err)
	}
	return s
}

// tableTriggers — число неслужебных триггеров таблицы.
func tableTriggers(t *testing.T, ctx context.Context, m *meta.Meta, table string) int {
	t.Helper()
	return countRow(t, ctx, m,
		`SELECT count(*) FROM pg_trigger tg
		   JOIN pg_class c ON c.oid = tg.tgrelid
		  WHERE c.relname = $1 AND NOT tg.tgisinternal
		    AND c.relnamespace = current_schema()::regnamespace`, table)
}

// seedSegments — данные для проверки Down: тенант с сегментом и признаками.
// События пишутся напрямую (миграция 0027: bucket_id без внешнего ключа).
func seedSegments(t *testing.T, ctx context.Context, m *meta.Meta, tenant int64) int64 {
	t.Helper()

	// Имя тенанта уникально по схеме, поэтому оно с номером: seedSegments вызывают
	// два разных тенанта.
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO tenants (id, name, status, pool_id)
		 VALUES ($1, $2, 'active', 1) ON CONFLICT (id) DO NOTHING`,
		tenant, fmt.Sprintf("тенант 0029-%d", tenant)); err != nil {
		t.Fatalf("тенант %d: %v", tenant, err)
	}
	var ids []int64
	for i := 0; i < 3; i++ {
		var id int64
		if err := m.Pool.QueryRow(ctx,
			`INSERT INTO journal_events (tenant_id, kind, bucket_id, payload)
			 VALUES ($1, 'bucket_config', 1, '{}') RETURNING id`, tenant).Scan(&id); err != nil {
			t.Fatalf("событие %d: %v", i, err)
		}
		ids = append(ids, id)
	}
	var seg int64
	if err := m.Pool.QueryRow(ctx,
		`INSERT INTO journal_segments (tenant_id, seq_first, seq_last, event_count,
		                              event_id_last, event_id_first)
		 VALUES ($1, 1, 3, 3, $2, $3) RETURNING id`, tenant, ids[2], ids[0]).Scan(&seg); err != nil {
		t.Fatalf("сегмент: %v", err)
	}
	for _, id := range ids {
		if _, err := m.Pool.Exec(ctx,
			`INSERT INTO journal_segment_event (segment_id, event_id) VALUES ($1, $2)`, seg, id); err != nil {
			t.Fatalf("признак события %d: %v", id, err)
		}
	}
	return seg
}

func TestMigration0029JournalSegments(t *testing.T) {
	m := testdb.MigrationScratch(t, testdb.AllVersions)
	ctx := context.Background()

	// --- Каталог после Up -------------------------------------------------

	// id сегмента — identity со строгой выдачей, как id события после 0028:
	// «у сегмента номер из схемы», а не «код не подставит свой».
	var isIdentity, generation, dataType string
	if err := m.Pool.QueryRow(ctx,
		`SELECT is_identity, COALESCE(identity_generation, ''), data_type
		   FROM information_schema.columns
		  WHERE table_schema = current_schema()
		    AND table_name = 'journal_segments' AND column_name = 'id'`).
		Scan(&isIdentity, &generation, &dataType); err != nil {
		t.Fatal(err)
	}
	if isIdentity != "YES" || generation != "ALWAYS" {
		t.Errorf("journal_segments.id: is_identity=%q generation=%q, хотели YES/ALWAYS",
			isIdentity, generation)
	}
	if dataType != "bigint" {
		t.Errorf("тип journal_segments.id = %q, хотели bigint", dataType)
	}

	if got := pkCols(t, ctx, m, "journal_segments"); got != "id" {
		t.Errorf("первичный ключ journal_segments = (%s), хотели (id)", got)
	}
	if got := pkCols(t, ctx, m, "journal_segment_event"); got != "segment_id,event_id" {
		t.Errorf("первичный ключ journal_segment_event = (%s), хотели (segment_id,event_id)", got)
	}
	// «Ровно в один сегмент» и «диапазон не выдан дважды» — схемой, а не воркером.
	if got := uniqueConstraints(t, ctx, m, "journal_segment_event"); got != "event_id" {
		t.Errorf("уникальные ограничения journal_segment_event = (%s), хотели (event_id)", got)
	}
	if got := uniqueConstraints(t, ctx, m, "journal_segments"); got != "tenant_id,seq_first;uuid" {
		t.Errorf("уникальные ограничения journal_segments = (%s), хотели "+
			"(tenant_id,seq_first) и (uuid)", got)
	}
	// Индекс под рубеж: max(event_id_last) по тенанту ищут с конца.
	if got := indexCols(t, ctx, m, "journal_segments_tenant_event"); got != "tenant_id,event_id_last" {
		t.Errorf("индекс journal_segments_tenant_event = (%s), хотели (tenant_id,event_id_last)", got)
	}
	// Каскад по тенанту обязана быть на обеих таблицах журнала: иначе удаление
	// тенанта оставило бы сегменты без владельца.
	fks := foreignKeys(t, ctx, m, "journal_segments")
	if !strings.Contains(fks, "REFERENCES tenants(id) ON DELETE CASCADE") {
		t.Errorf("внешние ключи journal_segments: %s — ждём tenant_id -> tenants(id) ON DELETE CASCADE", fks)
	}
	fks = foreignKeys(t, ctx, m, "journal_segment_event")
	for _, want := range []string{"REFERENCES journal_segments(id)", "REFERENCES journal_events(id)"} {
		if !strings.Contains(fks, want) {
			t.Errorf("внешние ключи journal_segment_event: %s — ждём %s", fks, want)
		}
	}
	// CHECK на плотность диапазона обязан быть в каталоге, а не только в коде.
	// Имя Postgres генерирует (journal_segments_check1), поэтому ищем определение,
	// где участвуют все три колонки сразу: CHECK вида `event_count > 0` сам по
	// себе плотность не сторожит, а слово event_count в строке есть.
	checks := checkConstraints(t, ctx, m, "journal_segments")
	dense := false
	for _, c := range strings.Split(checks, ";") {
		if strings.Contains(c, "seq_first") && strings.Contains(c, "seq_last") &&
			strings.Contains(c, "event_count") {
			dense = true
		}
	}
	if !dense {
		t.Errorf("в CHECK journal_segments нет сверки диапазона с числом событий: %s", checks)
	}
	// Границы id сегмента: по ним видно, совпадает ли плотный порядок с
	// id-порядком, то есть можно ли применять сегменты склейкой (ADR-0023,
	// поправка от 2026-11-11).
	if n := columnIn(t, ctx, m, "journal_segments", "event_id_first"); n != 1 {
		t.Error("journal_segments.event_id_first отсутствует: перестановку плотного " +
			"порядка нечем будет заметить")
	}
	orderCheck := false
	for _, c := range strings.Split(checks, ";") {
		if strings.Contains(c, "event_id_first") && strings.Contains(c, "event_id_last") {
			orderCheck = true
		}
	}
	if !orderCheck {
		t.Errorf("в CHECK journal_segments нет сверки event_id_first с event_id_last: %s", checks)
	}
	if n := tableTriggers(t, ctx, m, "journal_segment_event"); n != 1 {
		t.Errorf("триггеров у journal_segment_event: %d, хотели 1 («только на добавление»)", n)
	}
	// tenants.journal_seq остаётся единственным источником плотного номера.
	if n := columnIn(t, ctx, m, "tenants", "journal_seq"); n != 1 {
		t.Error("tenants.journal_seq исчезла: её выдаёт силер, колонка обязана остаться")
	}

	// --- Данные: ограничения работают, а не висят -------------------------

	const tenant = int64(777)
	seedSegments(t, ctx, m, tenant)
	if n := countRow(t, ctx, m,
		`SELECT count(*) FROM journal_segment_event WHERE segment_id =
		   (SELECT id FROM journal_segments WHERE tenant_id = $1)`, tenant); n != 3 {
		t.Fatalf("признаков %d, хотели 3 — посева нет", n)
	}

	// Подставной id сегмента невозможен (identity ALWAYS).
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO journal_segments (id, tenant_id, seq_first, seq_last, event_count,
		                              event_id_last, event_id_first)
		 VALUES (1, $1, 100, 100, 1, 1, 1)`, tenant); err == nil {
		t.Error("подставной id сегмента прошёл: identity не ALWAYS")
	}
	// Диапазон обязан равняться числу событий — по нему rebuild-meta сверяет
	// смежность и видит пропуск.
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO journal_segments (tenant_id, seq_first, seq_last, event_count, event_id_last, event_id_first)
		 VALUES ($1, 30, 32, 2, 999, 1)`, tenant); err == nil {
		t.Error("сегмент с диапазоном 30..32 и двумя событиями прошёл: CHECK на плотность не работает")
	}
	// «Минимальный id больше максимального» бывает только посчитанным не по
	// сегменту — а на этих двух колонках держится вывод о порядке применения.
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO journal_segments (tenant_id, seq_first, seq_last, event_count, event_id_last, event_id_first)
		 VALUES ($1, 40, 40, 1, 999, 1000)`, tenant); err == nil {
		t.Error("сегмент с event_id_first > event_id_last прошёл: CHECK по границам id не работает")
	}
	for _, bad := range []struct {
		what                            string
		first, last, count, eventIDLast int64
	}{
		{"seq_first = 0", 0, 1, 1, 1},
		{"seq_last < seq_first", 5, 4, 1, 1},
		{"event_count = 0", 20, 20, 0, 1},
	} {
		if _, err := m.Pool.Exec(ctx,
			`INSERT INTO journal_segments (tenant_id, seq_first, seq_last, event_count, event_id_last, event_id_first)
			 VALUES ($1, $2, $3, $4, $5, 1)`, tenant, bad.first, bad.last, bad.count, bad.eventIDLast); err == nil {
			t.Errorf("сегмент с %s прошёл: CHECK не на месте", bad.what)
		}
	}
	// Два сегмента одного тенанта не могут начинаться с одного номера.
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO journal_segments (tenant_id, seq_first, seq_last, event_count, event_id_last, event_id_first)
		 VALUES ($1, 1, 1, 1, 999, 1)`, tenant); err == nil {
		t.Error("второй сегмент с тем же seq_first прошёл: плотный номер могли бы выдать дважды")
	}
	// uuid сегмента — AAD шифрования: он выдаётся схемой и не повторяется.
	var segUUID string
	if err := m.Pool.QueryRow(ctx,
		`SELECT uuid FROM journal_segments WHERE tenant_id = $1`, tenant).Scan(&segUUID); err != nil {
		t.Fatal(err)
	}
	if segUUID == "" {
		t.Error("uuid сегмента пустой: его обязана выдавать схема")
	}
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO journal_segments (tenant_id, seq_first, seq_last, event_count, event_id_last,
		                              event_id_first, uuid)
		 VALUES ($1, 50, 50, 1, 999, 1, $2::uuid)`, tenant, segUUID); err == nil {
		t.Error("второй сегмент с тем же uuid прошёл: uuid не UNIQUE, и шифротекст одного " +
			"сегмента подошёл бы другому")
	}
	// Событие — ровно в одном сегменте. Второй сегмент заведён отдельно: если
	// ткнуть тем же событием в тот же сегмент, сработает первичный ключ, а не
	// UNIQUE (event_id), и проверка ловила бы не то ограничение.
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO journal_segments (tenant_id, seq_first, seq_last, event_count, event_id_last, event_id_first)
		 VALUES ($1, 10, 10, 1, 999, 1)`, tenant); err != nil {
		t.Fatalf("второй сегмент: %v", err)
	}
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO journal_segment_event (segment_id, event_id)
		 VALUES ((SELECT id FROM journal_segments WHERE tenant_id = $1 AND seq_first = 10),
		         (SELECT event_id FROM journal_segment_event LIMIT 1))`, tenant); err == nil {
		t.Error("событие записали в второй сегмент: UNIQUE (event_id) не работает")
	}
	// Триггер «только на добавление» обязан работать на живых строках: на пустой
	// таблице построчный триггер не вызывается вовсе.
	if _, err := m.Pool.Exec(ctx,
		`UPDATE journal_segment_event SET event_id = event_id`); err == nil {
		t.Error("UPDATE признака прошёл: журнал запечатывания не append-only")
	}
	if _, err := m.Pool.Exec(ctx, `DELETE FROM journal_segment_event WHERE true`); err == nil {
		t.Error("DELETE признака прошёл: признак можно снять и запечатать событие заново")
	}
	// Сегменты уходят вместе с тенантом. Проверка узкая сознательно: тенанта с
	// событиями удалить нельзя в принципе — каскад на journal_events упирается в
	// триггер «только на добавление» (проверено: DELETE tenants падает с ним),
	// поэтому каскад на сегменты проверяется там, где он вообще достижим: у тенанта
	// есть сегмент, но нет событий.
	{
		const doomed = int64(778)
		if _, err := m.Pool.Exec(ctx,
			`INSERT INTO tenants (id, name, status, pool_id)
			 VALUES ($1, 'тенант 0029-обречённый', 'active', 1)`, doomed); err != nil {
			t.Fatalf("тенант %d: %v", doomed, err)
		}
		if _, err := m.Pool.Exec(ctx,
			`INSERT INTO journal_segments (tenant_id, seq_first, seq_last, event_count, event_id_last, event_id_first)
			 VALUES ($1, 1, 1, 1, 0, 0)`, doomed); err != nil {
			t.Fatalf("сегмент обречённого тенанта: %v", err)
		}
		if _, err := m.Pool.Exec(ctx, `DELETE FROM tenants WHERE id = $1`, doomed); err != nil {
			t.Fatalf("удалить тенанта: %v", err)
		}
		if n := countRow(t, ctx, m,
			`SELECT count(*) FROM journal_segments WHERE tenant_id = $1`, doomed); n != 0 {
			t.Errorf("сегментов удалённого тенанта %d, хотели 0: ON DELETE CASCADE не работает", n)
		}
	}

	// --- Down и повторный Up на наполненной схеме -------------------------

	events := countRow(t, ctx, m, `SELECT count(*) FROM journal_events`)
	if _, err := m.Pool.Exec(ctx, section(t, migration0029, "Down")); err != nil {
		t.Fatalf("Down 0029 на схеме с данными: %v", err)
	}
	for _, tbl := range []string{"journal_segment_event", "journal_segments"} {
		if n := tableCount(t, ctx, m, tbl); n != 0 {
			t.Errorf("после Down таблица %s осталась", tbl)
		}
	}
	// Down сегментов не трогает журнал: снимается учёт запечатывания, а не события.
	if n := countRow(t, ctx, m, `SELECT count(*) FROM journal_events`); n != events {
		t.Errorf("событий после Down %d, было %d: Down сегментов задел журнал", n, events)
	}
	if n := columnIn(t, ctx, m, "tenants", "journal_seq"); n != 1 {
		t.Error("после Down tenants.journal_seq пропала: счётчик принадлежит не 0029")
	}

	if _, err := m.Pool.Exec(ctx, section(t, migration0029, "Up")); err != nil {
		t.Fatalf("повторный Up 0029 на схеме с данными: %v", err)
	}
	if got := uniqueConstraints(t, ctx, m, "journal_segment_event"); got != "event_id" {
		t.Errorf("после повторного Up уникальные ограничения journal_segment_event = (%s)", got)
	}
	if n := tableTriggers(t, ctx, m, "journal_segment_event"); n != 1 {
		t.Errorf("триггеров после повторного Up: %d, хотели 1", n)
	}
	if n := countRow(t, ctx, m, `SELECT count(*) FROM journal_events`); n != events {
		t.Errorf("событий после Down → Up %d, было %d", n, events)
	}
	// Честное следствие отката: учёт запечатывания потерян, события стали
	// незапечатанными. Это не авария, а смысл Down — но знать его надо.
	if n := countRow(t, ctx, m, `SELECT count(*) FROM journal_segments`); n != 0 {
		t.Errorf("сегментов после повторного Up %d, хотели 0", n)
	}
	// Триггер обязан вернуться **работающим**: на пустой таблице построчный
	// триггер не вызывается вовсе, поэтому проверка — на новой строке.
	seedSegments(t, ctx, m, 779)
	if _, err := m.Pool.Exec(ctx,
		`UPDATE journal_segment_event SET event_id = event_id`); err == nil {
		t.Error("после повторного Up UPDATE признака прошёл: защита осталась выключенной")
	}
}
