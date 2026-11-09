package meta_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// Deleter (M6): выборка из 'deleting', состояние пачки для решения о ленте,
// идемпотентные переходы.

func TestClaimDeletionBatch(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	ids := tenantWithObjects(t, m, 2, "delq", 10, 3)
	// Первая версия — под legal hold: выборка обязана это вернуть, а не
	// молча отдать её воркеру «на удаление».
	mustExec(t, m, `UPDATE objects SET legal_hold = true WHERE id = $1`, ids[0])
	// Переводим все три в deleting через честный путь API.
	for i := range ids {
		if _, err := m.DeleteObject(ctx, meta.NewTenant(2), meta.DeleteObjectInput{
			Bucket: "delq", Key: fmtKey(2, i),
		}); err != nil {
			t.Fatalf("DeleteObject %d: %v", i, err)
		}
	}

	got, err := m.ClaimDeletionBatch(ctx, 10, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("выборка вернула %d, ждали 3", len(got))
	}
	var locked int
	for _, c := range got {
		if c.Bucket != "delq" {
			t.Errorf("бакет %q", c.Bucket)
		}
		if c.ID == ids[0] {
			locked++
		}
		if c.PackID != nil {
			t.Errorf("у версии без пачки PackID непустой")
		}
	}
	if locked != 1 {
		t.Errorf("под lock'ом помечено %d, ждали 1", locked)
	}

	// Захваченное второму проходу не достаётся: next_try_at отодвинут.
	again, err := m.ClaimDeletionBatch(ctx, 10, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 0 {
		t.Errorf("повторная выборка взяла %d, ждали 0 — захват не держит", len(again))
	}

	// Истёкший захват (упавший процесс) снова берётся.
	mustExec(t, m, `UPDATE objects SET next_try_at = now() - interval '1 second' WHERE state = 'deleting'`)
	after, err := m.ClaimDeletionBatch(ctx, 10, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != 3 {
		t.Errorf("после истечения захвата взято %d, ждали 3", len(after))
	}
}

// Свойство «параллельные не мешают и не дублируют» проверяют два живых
// участника одновременно, а не один за другим.
func TestClaimDeletionBatchConcurrent(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	ids := tenantWithObjects(t, m, 2, "delcc", 10, 8)
	for i := range ids {
		if _, err := m.DeleteObject(ctx, meta.NewTenant(2), meta.DeleteObjectInput{
			Bucket: "delcc", Key: fmtKey(2, i),
		}); err != nil {
			t.Fatalf("DeleteObject: %v", err)
		}
	}

	var mu sync.Mutex
	seen := map[uuid.UUID]int{}
	var wg sync.WaitGroup
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := m.ClaimDeletionBatch(ctx, 4, time.Hour)
			if err != nil {
				t.Errorf("claim: %v", err)
				return
			}
			mu.Lock()
			for _, c := range got {
				seen[c.ID]++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()

	if len(seen) == 0 {
		t.Fatal("ни один воркер ничего не взял")
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("версия %s взята %d раза — SKIP LOCKED не развёл воркеров", id, n)
		}
	}
}

func TestPackDeletionState(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	mustBucket(t, m, "delpack")
	packID, ids := packFixture(t, m, "delpack", 100, 3)

	// Копия пачки на ленте.
	oid := uuid.New()
	sha := make([]byte, 32)
	mustExec(t, m, `INSERT INTO media (label, lifecycle, pool_id, generation, medium_type, free_bytes_cached)
		VALUES ('deltape', 'active', 1, 'L8', 'L8', 1000000)`)
	var mediaID int64
	if err := m.Pool.QueryRow(ctx, `SELECT id FROM media WHERE label = 'deltape'`).Scan(&mediaID); err != nil {
		t.Fatal(err)
	}
	if _, err := m.RecordPackCopy(ctx, packID, "main", oid.String(), 1, 1, sha, mediaID); err != nil {
		t.Fatal(err)
	}

	st, err := m.PackDeletionState(ctx, packID)
	if err != nil {
		t.Fatal(err)
	}
	if st.State != "archived" || st.MembersLive != 3 {
		t.Errorf("свежая пачка: %+v", st)
	}
	if len(st.Copies) != 1 || st.Copies[0] != oid {
		t.Errorf("копии: %v, ждали [%s]", st.Copies, oid)
	}

	// Удаляем двух участников, третий остаётся живым.
	for _, i := range []int{0, 1} {
		if _, err := m.DeleteObject(ctx, testTenant, meta.DeleteObjectInput{
			Bucket: "delpack", Key: keyN(i), VersionID: ids[i].String(),
		}); err != nil {
			t.Fatalf("DeleteObject: %v", err)
		}
	}
	st, err = m.PackDeletionState(ctx, packID)
	if err != nil {
		t.Fatal(err)
	}
	if st.MembersLive != 1 || st.LockedDeleting != 0 {
		t.Errorf("после двух удалений: %+v", st)
	}

	// Второй удаляемый — под retention: он блокирует снятие пачки.
	mustExec(t, m, `UPDATE objects SET retention_mode = 'COMPLIANCE', retain_until = now() + interval '1 day'
		WHERE id = $1`, ids[1])
	st, err = m.PackDeletionState(ctx, packID)
	if err != nil {
		t.Fatal(err)
	}
	if st.LockedDeleting != 1 {
		t.Errorf("LockedDeleting = %d, ждали 1", st.LockedDeleting)
	}

	// Восстановление в работе у живого участника.
	mustExec(t, m, `INSERT INTO restores (object_id, tenant_id, state, days) VALUES ($1, 1, 'requested', 1)`, ids[2])
	st, err = m.PackDeletionState(ctx, packID)
	if err != nil {
		t.Fatal(err)
	}
	if !st.RestoreBusy {
		t.Error("RestoreBusy не увидел восстановление в работе")
	}
	mustExec(t, m, `DELETE FROM restores WHERE object_id = $1`, ids[2])

	// Активный захват migrator'а.
	mustExec(t, m, `UPDATE packs SET claimed_by = 'node/1', claim_expires_at = now() + interval '5 min' WHERE id = $1`, packID)
	st, err = m.PackDeletionState(ctx, packID)
	if err != nil {
		t.Fatal(err)
	}
	if !st.ClaimActive {
		t.Error("ClaimActive не увидел захват migrator'а")
	}
}

func TestMarkVersionDeletedIdempotent(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	ids := tenantWithObjects(t, m, 2, "delmark", 10, 1)
	if _, err := m.DeleteObject(ctx, meta.NewTenant(2), meta.DeleteObjectInput{
		Bucket: "delmark", Key: fmtKey(2, 0),
	}); err != nil {
		t.Fatal(err)
	}

	ok, err := m.MarkVersionDeleted(ctx, ids[0])
	if err != nil || !ok {
		t.Fatalf("первый переход: ok=%v err=%v", ok, err)
	}
	var state string
	var retired bool
	if err := m.Pool.QueryRow(ctx,
		`SELECT state::text, retired_at IS NOT NULL FROM objects WHERE id = $1`, ids[0]).
		Scan(&state, &retired); err != nil {
		t.Fatal(err)
	}
	if state != "deleted" || !retired {
		t.Errorf("после перехода: %s retired=%v", state, retired)
	}

	ok, err = m.MarkVersionDeleted(ctx, ids[0])
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Error("повтор вернул true — переход не идемпотентен")
	}
}

func TestFinishPackDeletion(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	mustBucket(t, m, "delfin")
	packID, ids := packFixture(t, m, "delfin", 100, 3)
	for i := range ids {
		if _, err := m.DeleteObject(ctx, testTenant, meta.DeleteObjectInput{
			Bucket: "delfin", Key: keyN(i), VersionID: ids[i].String(),
		}); err != nil {
			t.Fatalf("DeleteObject: %v", err)
		}
	}
	// Средний участник — под legal hold: его байты внутри объекта пачки, и
	// переводить его в deleted нельзя ни при каком purge.
	mustExec(t, m, `UPDATE objects SET legal_hold = true WHERE id = $1`, ids[1])

	done, retired, err := m.FinishPackDeletion(ctx, packID)
	if err != nil {
		t.Fatal(err)
	}
	if len(done) != 2 || retired {
		t.Errorf("первый проход: переведено %d (ждали 2), ретайр %v (ждали false)", len(done), retired)
	}
	var state string
	if err := m.Pool.QueryRow(ctx, `SELECT state::text FROM packs WHERE id = $1`, packID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "archived" {
		t.Errorf("пачка под заблокированным участником ушла в %s", state)
	}
	var st string
	if err := m.Pool.QueryRow(ctx, `SELECT state::text FROM objects WHERE id = $1`, ids[1]).Scan(&st); err != nil {
		t.Fatal(err)
	}
	if st != "deleting" {
		t.Errorf("заблокированный участник в %s, должен ждать в deleting", st)
	}

	// Lock снят (истёк) — второй проход доводит пачку до конца.
	mustExec(t, m, `UPDATE objects SET legal_hold = false WHERE id = $1`, ids[1])
	done, retired, err = m.FinishPackDeletion(ctx, packID)
	if err != nil {
		t.Fatal(err)
	}
	if len(done) != 1 || !retired {
		t.Errorf("второй проход: переведено %d (ждали 1), ретайр %v (ждали true)", len(done), retired)
	}

	// Повтор ничего не меняет.
	done, retired, err = m.FinishPackDeletion(ctx, packID)
	if err != nil {
		t.Fatal(err)
	}
	if len(done) != 0 || retired {
		t.Errorf("повтор: %d/%v, ждали 0/false", len(done), retired)
	}
}

func TestDeletionFailedBackoff(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	ids := tenantWithObjects(t, m, 2, "delretry", 10, 1)
	if _, err := m.DeleteObject(ctx, meta.NewTenant(2), meta.DeleteObjectInput{
		Bucket: "delretry", Key: fmtKey(2, 0),
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.DeletionFailed(ctx, ids, "mover недоступен", time.Minute); err != nil {
		t.Fatal(err)
	}
	var state string
	var attempts int
	var future bool
	if err := m.Pool.QueryRow(ctx,
		`SELECT state::text, attempts, next_try_at > now() FROM objects WHERE id = $1`, ids[0]).
		Scan(&state, &attempts, &future); err != nil {
		t.Fatal(err)
	}
	if state != "deleting" || attempts != 1 || !future {
		t.Errorf("после сбоя: %s attempts=%d future=%v", state, attempts, future)
	}
}
