package meta_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/meta"
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

// «Архивный = все копии» означает также «копии на РАЗНЫХ носителях»: две
// копии на одном картридже — не две копии, отказ ленты забирает обе. Страж
// живёт в ArchivePackWhenCopiesDone, а не в вызывающем: инвариант обязан
// держаться для любого пути архивации.
func TestArchivePackCopiesSameMedium(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	ids := tenantWithObjects(t, m, 2, "collide", 10, 1)
	tx, packID, err := m.BeginPackArchive(ctx, 1, ids)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	pl := []meta.Placement{{Version: ids[0], Offset: 0, Length: 10}}
	if err := m.SealPack(ctx, packID, pl, 10, "w/1", time.Minute); err != nil {
		t.Fatal(err)
	}

	mustExec(t, m, `INSERT INTO media (label, lifecycle, pool_id, generation, medium_type, free_bytes_cached)
		VALUES ('collide-a', 'active', 1, 'L8', 'L8', 1000000), ('collide-b', 'active', 1, 'L8', 'L8', 1000000)`)
	var mediaA, mediaB int64
	if err := m.Pool.QueryRow(ctx,
		`SELECT id FROM media WHERE label = 'collide-a'`).Scan(&mediaA); err != nil {
		t.Fatal(err)
	}
	if err := m.Pool.QueryRow(ctx,
		`SELECT id FROM media WHERE label = 'collide-b'`).Scan(&mediaB); err != nil {
		t.Fatal(err)
	}

	sha := make([]byte, 32)
	if _, err := m.RecordPackCopy(ctx, packID, "left", uuid.NewString(), 1, 1, sha, mediaA); err != nil {
		t.Fatal(err)
	}
	if _, err := m.RecordPackCopy(ctx, packID, "right", uuid.NewString(), 1, 1, sha, mediaA); err != nil {
		t.Fatal(err)
	}

	done, err := m.ArchivePackWhenCopiesDone(ctx, packID, pl, []string{"left", "right"}, "w/1", time.Hour)
	if done {
		t.Fatal("пачка archived при двух копиях на одном носителе")
	}
	if !errors.Is(err, meta.ErrCopiesSameMedium) {
		t.Fatalf("ошибка %v, ожидали ErrCopiesSameMedium", err)
	}
	// Причина названа: какие именно копии столкнулись.
	if !strings.Contains(err.Error(), "left") || !strings.Contains(err.Error(), "right") {
		t.Errorf("в %v нет имени столкнувшихся копий", err)
	}
	// Состояния не тронуты: пачка запечатана, объект ждёт — вызывающий
	// решает (migrator блокирует), а не «полуархив».
	var packState, objState string
	if err := m.Pool.QueryRow(ctx, `SELECT state::text FROM packs WHERE id = $1`, packID).Scan(&packState); err != nil {
		t.Fatal(err)
	}
	if err := m.Pool.QueryRow(ctx, `SELECT state::text FROM objects WHERE id = $1`, ids[0]).Scan(&objState); err != nil {
		t.Fatal(err)
	}
	if packState != "sealed" || objState != "archiving" {
		t.Errorf("пачка %s, объект %s — ждали sealed/archiving", packState, objState)
	}

	// Позитивный контроль: те же копии на разных носителях архивируются —
	// иначе проверка была бы «запретить всё».
	mustExec(t, m, `UPDATE archive_copies SET medium_id = $1
		WHERE pack_id = $2 AND spec = 'right'`, mediaB, packID)
	done, err = m.ArchivePackWhenCopiesDone(ctx, packID, pl, []string{"left", "right"}, "w/1", time.Hour)
	if err != nil || !done {
		t.Fatalf("копии на разных носителях не архивировались: done=%v err=%v", done, err)
	}
}
