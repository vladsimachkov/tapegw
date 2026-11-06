package meta_test

import (
	"context"
	"testing"

	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/testdb"
)

const migration0020 = "migrations/0020_write_targets.sql"

// Миграция проверяется текстом файла, а не пересказом (правило 11), и на
// наполненной таблице: на пустой Down/Up проходит даже когда роняет строки.

func TestMigration0020UpDown(t *testing.T) {
	m := testdb.MigrationScratch(t, testdb.AllVersions)
	ctx := context.Background()

	// Наполняем media до отката. Down обязан оставить строки в целости: он
	// роняет только представление и пересоздаёт индекс.
	for _, s := range []struct{ label, life string }{
		{"up-a", "active"}, {"up-b", "formatted"}, {"up-c", "read_only"},
	} {
		reason := any(nil)
		if s.life == "read_only" {
			reason = "проверка миграции"
		}
		if _, err := m.Pool.Exec(ctx, `INSERT INTO media
(label, lifecycle, read_only_reason, pool_id, generation, medium_type, free_bytes_cached)
VALUES ($1, $2, $3, 1, 'L8', 'L8', 100)`, s.label, s.life, reason); err != nil {
			t.Fatalf("посев %s: %v", s.label, err)
		}
	}

	run := func(want string) {
		t.Helper()
		if _, err := m.Pool.Exec(ctx, section(t, migration0020, want)); err != nil {
			t.Fatalf("%s %s: %v", migration0020, want, err)
		}
	}

	// Down: представления нет, индекс вернулся к старому предикату.
	run("Down")
	if n := viewExists(t, ctx, m, "media_write_targets"); n != 0 {
		t.Error("представление media_write_targets пережило Down")
	}
	if got := indexPredicate(t, ctx, m, "media_writable"); got != "(lifecycle = 'active'::media_lifecycle)" {
		t.Errorf("после Down предикат индекса: %s", got)
	}
	if n := countMedia(t, ctx, m); n != 3 {
		t.Fatalf("Down тронул строки media: %d, ждали 3", n)
	}

	// Второй Down сразу после первого обязан упасть: представление уже удалено.
	// Тихий «ничего не сделал» здесь означал бы, что Down не проверялся, а
	// недоделанный Down прячется именно так.
	if _, err := m.Pool.Exec(ctx, section(t, migration0020, "Down")); err == nil {
		t.Error("второй Down прошёл: значит он не удаляет то, что создаёт Up")
	}

	// Up: представление и индекс снова на месте, строки целые.
	run("Up")
	if n := viewExists(t, ctx, m, "media_write_targets"); n != 1 {
		t.Fatal("представление не вернулось после Up")
	}
	want := canonicalPredicate(t, ctx, m)
	if got := indexPredicate(t, ctx, m, "media_writable"); got != want {
		t.Errorf("после Up предикат индекса: %s, ждали %s", got, want)
	}
	if n := countMedia(t, ctx, m); n != 3 {
		t.Errorf("Up тронул строки media: %d, ждали 3", n)
	}

}

func viewExists(t *testing.T, ctx context.Context, m *meta.Meta, name string) int {
	t.Helper()
	var n int
	if err := m.Pool.QueryRow(ctx, `
SELECT count(*) FROM pg_views WHERE schemaname = current_schema() AND viewname = $1`, name).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func indexPredicate(t *testing.T, ctx context.Context, m *meta.Meta, idx string) string {
	t.Helper()
	var s string
	if err := m.Pool.QueryRow(ctx, `
SELECT coalesce(pg_get_expr(i.indpred, i.indrelid), '')
FROM pg_index i WHERE i.indexrelid = $1::regclass`, idx).Scan(&s); err != nil {
		t.Fatal(err)
	}
	return s
}

func countMedia(t *testing.T, ctx context.Context, m *meta.Meta) int {
	t.Helper()
	var n int
	if err := m.Pool.QueryRow(ctx, `SELECT count(*) FROM media`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
