package meta_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// Копия без зарегистрированного носителя. CHECK archive_copies_done_has_medium
// запрещает 'done' без ленты — и RecordPackCopy обязан подстраивать состояние,
// а не падать: на вертикальном срезе ci-dir extent пришёл с меткой носителя,
// которой нет в media, CHECK-ошибка вставила «продолжение пачки» в бесконечный
// цикл, и объект завис в 'archiving'.

func TestRecordPackCopyWithoutMedium(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	tenantWithObjects(t, m, 2, "copyb", 10, 1)
	packID, _ := packFixture(t, m, "copyb", 100, 2)

	sha := make([]byte, 32)
	copyID, err := m.RecordPackCopy(ctx, packID, "main", packID.String(), 1, 1, sha, 0)
	if err != nil {
		t.Fatalf("копия на ленту вне реестра не должна быть ошибкой: %v", err)
	}
	var state string
	var doneAt *string
	if err := m.Pool.QueryRow(ctx,
		`SELECT state::text, done_at::text FROM archive_copies WHERE id = $1`, copyID).
		Scan(&state, &doneAt); err != nil {
		t.Fatal(err)
	}
	if state != "writing" || doneAt != nil {
		t.Errorf("копия в %s (done_at=%v), ждали writing без done_at", state, doneAt)
	}

	// Лента зарегистрирована — BindCopyMedium доводит до 'done'.
	mustExec(t, m, `INSERT INTO media (label, lifecycle, pool_id, generation, medium_type, free_bytes_cached)
		VALUES ('copytape', 'active', 1, 'L8', 'L8', 1000000)`)
	var mediaID int64
	if err := m.Pool.QueryRow(ctx, `SELECT id FROM media WHERE label = 'copytape'`).Scan(&mediaID); err != nil {
		t.Fatal(err)
	}
	if err := m.BindCopyMedium(ctx, copyID, mediaID); err != nil {
		t.Fatal(err)
	}
	var hasDone bool
	if err := m.Pool.QueryRow(ctx,
		`SELECT state::text, done_at IS NOT NULL FROM archive_copies WHERE id = $1`, copyID).
		Scan(&state, &hasDone); err != nil {
		t.Fatal(err)
	}
	if state != "done" || !hasDone {
		t.Errorf("после BindCopyMedium копия в %s (done_at=%v), ждали done с отметкой", state, hasDone)
	}
}

func TestRecordPackCopyWithMedium(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	tenantWithObjects(t, m, 2, "copyb2", 10, 1)
	packID, _ := packFixture(t, m, "copyb2", 100, 1)
	mustExec(t, m, `INSERT INTO media (label, lifecycle, pool_id, generation, medium_type, free_bytes_cached)
		VALUES ('known-tape', 'active', 1, 'L8', 'L8', 1000000)`)
	var mediaID int64
	if err := m.Pool.QueryRow(ctx,
		`SELECT id FROM media WHERE label = 'known-tape'`).Scan(&mediaID); err != nil {
		t.Fatal(err)
	}

	sha := make([]byte, 32)
	copyID, err := m.RecordPackCopy(ctx, packID, "main", uuid.NewString(), 1, 1, sha, mediaID)
	if err != nil {
		t.Fatal(err)
	}
	var state string
	var hasDone bool
	if err := m.Pool.QueryRow(ctx,
		`SELECT state::text, done_at IS NOT NULL FROM archive_copies WHERE id = $1`, copyID).
		Scan(&state, &hasDone); err != nil {
		t.Fatal(err)
	}
	if state != "done" || !hasDone {
		t.Errorf("копия с носителем: %s done_at=%v, ждали done с отметкой", state, hasDone)
	}
}
