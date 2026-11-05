package meta_test

import (
	"context"
	"testing"

	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// Канон предиката «доступна для записи». Он живёт в миграции 0020 дважды — в
// представлении и в предикате частичного индекса, — и тест сверяет обе копии с
// этим текстом. Разойтись они могут только молча: индекс уже представления
// означает тихое деградирование плана, уже уже — нет.
const canonicalWritable = "lifecycle IN ('formatted', 'active')"

func TestWriteTargetsMatchesCanonical(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	seed := []struct {
		label  string
		life   string
		reason *string
	}{
		{"fmt-open", "formatted", nil},
		{"act-open", "active", nil},
		// «Активная, но закрытая» лента не существует: CHECK
		// media_read_only_consistent не даёт поставить причину, не переведя в
		// read_only. Поэтому закрытые ленты здесь именно read_only.
		{"act-closed", "read_only", strptr("снят put_access")},
		{"fmt-closed", "read_only", strptr("полка закрыта")},
		{"new", "new", nil},
		{"full", "full", nil},
		{"evac", "evacuating", nil},
		{"retired", "retired", nil},
	}
	for _, s := range seed {
		mustExec(t, m, `INSERT INTO media (label, lifecycle, read_only_reason, pool_id, generation, medium_type)
VALUES ($1, $2, $3, 1, 'L8', 'L8')`, s.label, s.life, s.reason)
	}

	var fromView []string
	rows, err := m.Pool.Query(ctx, `SELECT label FROM media_write_targets ORDER BY label`)
	if err != nil {
		t.Fatalf("представление: %v", err)
	}
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		fromView = append(fromView, s)
	}
	rows.Close()

	want := []string{"act-open", "fmt-open"}
	if len(fromView) != len(want) {
		t.Fatalf("в представление попали не те ленты: %v, ждали %v", fromView, want)
	}
	for i := range want {
		if fromView[i] != want[i] {
			t.Errorf("представление: %v, ждали %v", fromView, want)
			break
		}
	}

	// Отдельно: «закрыта» и «нет места» — разные вещи. Лента с нулём места всё
	// ещё доступна для записи с точки зрения определения, а вот поместится ли
	// на неё объект — уже вопрос к MaxFree.
	mustExec(t, m, `UPDATE media SET free_bytes_cached = 0 WHERE label = 'act-open'`)
	n, err := countQuery(t, m, `SELECT count(*) FROM media_write_targets WHERE label = 'act-open'`)
	if err != nil || n != 1 {
		t.Errorf("лента с нулём места выпала из определения: n=%d err=%v", n, err)
	}
}

func TestWritableIndexPredicateMatchesView(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()

	// Нормализацию обоих предикатов делает сама БД: IN сворачивается в = ANY,
	// скобки расставляются по своим правилам. Сравнивать текст из файла с
	// текстом из pg_get_expr — значит проиграть на первой же скобке.
	var real string
	if err := m.Pool.QueryRow(ctx,
		`SELECT pg_get_expr(indpred, indrelid) FROM pg_index
		  WHERE indexrelid = 'media_writable'::regclass`).Scan(&real); err != nil {
		t.Fatalf("предикат индекса: %v", err)
	}

	if _, err := m.Pool.Exec(ctx, `CREATE INDEX canonical_writable ON media (pool_id, generation) WHERE `+canonicalWritable); err != nil {
		t.Fatalf("эталонный индекс: %v", err)
	}
	t.Cleanup(func() {
		_, _ = m.Pool.Exec(context.Background(), `DROP INDEX IF EXISTS canonical_writable`)
	})

	var canon string
	if err := m.Pool.QueryRow(ctx,
		`SELECT pg_get_expr(indpred, indrelid) FROM pg_index
		  WHERE indexrelid = 'canonical_writable'::regclass`).Scan(&canon); err != nil {
		t.Fatalf("предикат эталона: %v", err)
	}

	if real != canon {
		t.Errorf("индекс media_writable больше не соответствует определению\n  индекс: %s\n  эталон: %s", real, canon)
	}
}

func TestPoolWriteSpace(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	// Пустой пул: лент нет вовсе.
	got, err := m.PoolWriteSpace(ctx, 1, nil)
	if err != nil {
		t.Fatalf("PoolWriteSpace: %v", err)
	}
	if got.Reason() != "no_media" {
		t.Errorf("пустой пул: %s, ждали no_media", got.Reason())
	}

	add := func(label, life string, free int64, reason *string, gen string) {
		mustExec(t, m, `INSERT INTO media (label, lifecycle, read_only_reason, pool_id, generation, medium_type, free_bytes_cached)
VALUES ($1, $2, $3, 1, $4, $4, $5)`, label, life, reason, gen, free)
	}

	// Одна лента с местом, одна полная, одна закрытая при полном месте.
	add("open", "active", 1000, nil, "L8")
	add("full", "active", 0, nil, "L8")
	add("closed", "read_only", 5000, strptr("снят put"), "L8")
	// Свежая отформатированная лента — тоже цель для записи. Именно её терял
	// старый индекс с lifecycle='active', и именно она исчезнет из определения
	// первым делом, если условие сузят.
	add("fresh", "formatted", 700, nil, "L8")

	got, err = m.PoolWriteSpace(ctx, 1, []string{"L8"})
	if err != nil {
		t.Fatalf("PoolWriteSpace: %v", err)
	}
	if got.Writable != 3 || got.FreeBytes != 1700 || got.MaxFree != 1000 {
		t.Errorf("место: writable=%d free=%d max=%d, ждали 3/1700/1000",
			got.Writable, got.FreeBytes, got.MaxFree)
	}
	if got.ReadOnly != 1 {
		t.Errorf("закрытых лент: %d, ждали 1", got.ReadOnly)
	}
	if got.Reason() != "ok" {
		t.Errorf("причина: %s, ждали ok", got.Reason())
	}

	// MaxFree важнее FreeBytes: запись идёт с no_split, объект целиком должен
	// влезть на ОДНУ ленту. Здесь суммарно 1700, а на самой просторной 1000 —
	// на 1500 не пролезет, хотя «места в пуле больше чем надо».
	if got.Fits(1500) {
		t.Error("объект больше самой просторной ленты, а Fits говорит «да»: no_split нарушится на середине записи")
	}
	if !got.Fits(1000) {
		t.Error("объект ровно по максимуму не проходит")
	}

	// Все ленты закрыты — это «closed», а не «no_space»: лечится не ожиданием,
	// а снятием запрета или добавлением лент.
	mustExec(t, m, `UPDATE media SET lifecycle = 'read_only', read_only_reason = 'полка закрыта'
WHERE pool_id = 1`)
	got, err = m.PoolWriteSpace(ctx, 1, []string{"L8"})
	if err != nil {
		t.Fatalf("PoolWriteSpace: %v", err)
	}
	if got.Reason() != "closed" {
		t.Errorf("все ленты закрыты: %s, ждали closed", got.Reason())
	}
	if got.ReadOnly != 4 {
		t.Errorf("закрытых: %d, ждали 4", got.ReadOnly)
	}

	// Запрет по поколению — тоже «closed», а не «нет места»: ленты есть, но
	// политикой писать на них нельзя.
	mustExec(t, m, `UPDATE media SET lifecycle = 'active', read_only_reason = NULL, generation = 'L7'`)
	got, err = m.PoolWriteSpace(ctx, 1, []string{"L8"})
	if err != nil {
		t.Fatalf("PoolWriteSpace: %v", err)
	}
	if got.Reason() != "closed" || got.Forbidden != 4 {
		t.Errorf("поколение запрещено: reason=%s forbidden=%d, ждали closed/4", got.Reason(), got.Forbidden)
	}

	// Пустой список поколений означает «любые», а не «ничего».
	got, err = m.PoolWriteSpace(ctx, 1, nil)
	if err != nil {
		t.Fatalf("PoolWriteSpace: %v", err)
	}
	if got.Reason() != "ok" || got.Writable != 4 {
		t.Errorf("пустой список поколений: reason=%s writable=%d, ждали ok/4", got.Reason(), got.Writable)
	}
}

func TestMediaReadOnlyRequiresReason(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	mustExec(t, m, `INSERT INTO media (label, lifecycle, pool_id, generation, medium_type) VALUES ('r1', 'active', 1, 'L8', 'L8')`)

	if err := m.MarkMediaReadOnly(ctx, mediaID(t, m, "r1"), ""); err == nil {
		t.Error("закрыть ленту без причины удалось: запрет без объяснения — второй источник правды")
	}
	if err := m.MarkMediaReadOnly(ctx, mediaID(t, m, "r1"), "полка 4 уходит"); err != nil {
		t.Fatalf("MarkMediaReadOnly: %v", err)
	}
	life, reason, err := mediaState(t, m, "r1")
	if err != nil {
		t.Fatal(err)
	}
	if life != "read_only" || reason != "полка 4 уходит" {
		t.Errorf("лента: %s/%q", life, reason)
	}
	if n := countEvents(t, m, "r1", "read_only"); n != 1 {
		t.Errorf("событий read_only: %d, ждали 1", n)
	}

	// Обратный ход: снять запрет можно, и причина обязана исчезнуть вместе с
	// состоянием, иначе CHECK про «причина только у read_only» не сойдётся.
	if err := m.MarkMediaWritable(ctx, mediaID(t, m, "r1")); err != nil {
		t.Fatalf("MarkMediaWritable: %v", err)
	}
	life, reason, err = mediaState(t, m, "r1")
	if err != nil {
		t.Fatal(err)
	}
	if life != "active" || reason != "" {
		t.Errorf("после снятия: %s/%q, ждали active/пусто", life, reason)
	}
}

func mediaID(t *testing.T, m *meta.Meta, label string) int64 {
	t.Helper()
	var id int64
	if err := m.Pool.QueryRow(context.Background(), `SELECT id FROM media WHERE label = $1`, label).Scan(&id); err != nil {
		t.Fatalf("id ленты %s: %v", label, err)
	}
	return id
}

func mediaState(t *testing.T, m *meta.Meta, label string) (string, string, error) {
	var life, reason string
	err := m.Pool.QueryRow(context.Background(),
		`SELECT lifecycle::text, coalesce(read_only_reason, '') FROM media WHERE label = $1`, label).
		Scan(&life, &reason)
	return life, reason, err
}

func countEvents(t *testing.T, m *meta.Meta, label, event string) int {
	t.Helper()
	var n int
	if err := m.Pool.QueryRow(context.Background(), `
SELECT count(*) FROM media_events e JOIN media m ON m.id = e.media_id
WHERE m.label = $1 AND e.event = $2`, label, event).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func strptr(s string) *string { return &s }

func mustExec(t *testing.T, m *meta.Meta, q string, args ...any) {
	t.Helper()
	if _, err := m.Pool.Exec(context.Background(), q, args...); err != nil {
		t.Fatalf("%v: %s", err, q)
	}
}

func countQuery(t *testing.T, m *meta.Meta, q string, args ...any) (int, error) {
	t.Helper()
	var n int
	err := m.Pool.QueryRow(context.Background(), q, args...).Scan(&n)
	return n, err
}

// Представление намеренно НЕ проверяет read_only_reason: это гарантирует CHECK
// из миграции 0018. Если CHECK исчезнет, мёртвое условие станет нужным, а
// молча оно не исчезает — этот тест и падает.
func TestReadOnlyConsistencyCheckIsEnforced(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()

	var n int
	if err := m.Pool.QueryRow(ctx, `
SELECT count(*) FROM pg_constraint
WHERE conname = 'media_read_only_consistent' AND contype = 'c'
  AND connamespace = (SELECT oid FROM pg_namespace WHERE nspname = current_schema())`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("CHECK media_read_only_consistent: %d, ждали 1 — вместе с ним из определения "+
			"media_write_targets выпадает смысл: закрытая лента перестанет быть read_only", n)
	}

	// И поведенчески: причина без read_only не заводится.
	testdb.Clean(t, m)
	_, err := m.Pool.Exec(ctx, `INSERT INTO media (label, lifecycle, read_only_reason, pool_id, generation, medium_type)
VALUES ('cheat', 'active', 'просто так', 1, 'L8', 'L8')`)
	if err == nil {
		t.Error("лента 'active' с причиной запрета создана: определение «доступна для записи» надо дополнять")
	}
}

func TestWritableTapesList(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	add := func(label, life, gen string, free int64, reason *string) {
		mustExec(t, m, `INSERT INTO media (label, lifecycle, read_only_reason, pool_id, generation, medium_type, free_bytes_cached)
VALUES ($1, $2, $3, 1, $4, $4, $5)`, label, life, reason, gen, free)
	}
	add("l8-big", "active", "L8", 900, nil)
	add("l8-small", "formatted", "L8", 100, nil)
	add("l7", "active", "L7", 500, nil)
	add("closed", "read_only", "L8", 999, strptr("снят put"))
	add("draining", "evacuating", "L8", 800, nil)

	// Только L8, по убыванию места; закрытая и эвакуируемая не попадают.
	got, err := m.WritableTapes(ctx, 1, []string{"L8"})
	if err != nil {
		t.Fatalf("WritableTapes: %v", err)
	}
	var labels []string
	for _, w := range got {
		labels = append(labels, w.Label)
	}
	want := []string{"l8-big", "l8-small"}
	if len(labels) != len(want) {
		t.Fatalf("ленты: %v, ждали %v", labels, want)
	}
	for i := range want {
		if labels[i] != want[i] {
			t.Fatalf("порядок лент: %v, ждали %v (по убыванию места)", labels, want)
		}
	}
	if got[0].FreeBytes != 900 {
		t.Errorf("место: %d, ждали 900", got[0].FreeBytes)
	}

	// Пустой список поколений — «любые», L7 возвращается.
	got, err = m.WritableTapes(ctx, 1, nil)
	if err != nil {
		t.Fatalf("WritableTapes: %v", err)
	}
	if len(got) != 3 {
		t.Errorf("без фильтра по поколению: %d лент, ждали 3", len(got))
	}
}
