package meta_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// Очередь recaller'а (S5). Проверяются обещания из критериев ревью: переходы
// идемпотентны (правило 7), срок отсчитывается от завершения восстановления
// и только растёт, lease просроченный возвращается в работу с засчитанной
// попыткой, вытеснение cache — по online_until.

// recallFixture превращает объект в архивного участника пачки с готовой
// копией: packs, media, archive_copies(done, sha) и колонки objects.
// Возвращает id пачки и сумму потока копии.
func recallFixture(t *testing.T, m *meta.Meta, id uuid.UUID, size int64) (uuid.UUID, []byte) {
	t.Helper()
	ctx := context.Background()

	packID := uuid.New()
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO packs (id, pool_id, kind, state, total_bytes, live_bytes, members_total, members_live)
		 VALUES ($1, 1, 'data', 'archived', $2, $2, 1, 1)`, packID, size); err != nil {
		t.Fatal(err)
	}
	label := "recall-tape-" + uuid.NewString()
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO media (label, lifecycle, pool_id, generation, medium_type, free_bytes_cached)
		 VALUES ($1, 'active', 1, 'L8', 'L8', 1000000)`, label); err != nil {
		t.Fatal(err)
	}
	var mediaID int64
	if err := m.Pool.QueryRow(ctx, `SELECT id FROM media WHERE label = $1`, label).Scan(&mediaID); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Pool.Exec(ctx,
		`UPDATE objects SET pack_id = $2, pack_offset = 0, pack_length = $3, file_bytes = $3
		 WHERE id = $1`, id, packID, size); err != nil {
		t.Fatal(err)
	}
	sha := make([]byte, 32)
	sha[0] = 0xab
	if _, err := m.RecordPackCopy(ctx, packID, "main", uuid.NewString(), 1, 1, sha, mediaID); err != nil {
		t.Fatal(err)
	}
	return packID, sha
}

func restoreState(t *testing.T, m *meta.Meta, id uuid.UUID) (state string, attempts int, nextTry *time.Time) {
	t.Helper()
	err := m.Pool.QueryRow(context.Background(),
		`SELECT state, attempts, next_try_at FROM restores WHERE object_id = $1`, id).
		Scan(&state, &attempts, &nextTry)
	if err != nil {
		t.Fatal(err)
	}
	return state, attempts, nextTry
}

// Запрошенный архивный объект берётся в работу со всеми полями, нужными для
// чтения и сверки; повторный claim ничего не берёт (строка уже in_progress).
func TestClaimRestorePackQueued(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	id := tenantWithObjects(t, m, 2, "cb1", 10, 1)[0]
	setEvictState(t, m, id, "archived", false, "-1 hour")
	packID, _ := recallFixture(t, m, id, 10)
	requestRestore(t, m, id, 7, "Expedited")

	items, err := m.ClaimRestorePack(ctx, "w1", 10, time.Minute, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("участников %d, ждали 1", len(items))
	}
	it := items[0]
	if it.ObjectID != id || it.Days != 7 || it.PackID != packID ||
		it.PackOffset != 0 || it.FileBytes != 10 || it.Size != 10 {
		t.Errorf("участник %+v, ждали id/7/пачку/0/10/10", it)
	}
	if string(it.WrappedDEK) != "x" || it.Key == "" {
		t.Errorf("ключ/ключ-имя не доехали: %q %q", it.WrappedDEK, it.Key)
	}
	if st, att, _ := restoreState(t, m, id); st != "in_progress" || att != 0 {
		t.Errorf("после claim строка %s attempts=%d, ждали in_progress/0", st, att)
	}

	// Повтор: 'in_progress' с действующим lease не берётся.
	again, err := m.ClaimRestorePack(ctx, "w1", 10, time.Minute, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 0 {
		t.Errorf("живой захват перехвачен повторно: %d шт.", len(again))
	}
}

// Захват — по одной пачке: первая выборка берёт пачку самой приоритетной
// готовой строки и только её; чужие пачки остаются нетронутыми до следующего
// прохода. Это и есть ответ на «lease на батч из 500 объектов»: второй воркер
// не ждёт, пока первый прочитает чужие пачки.
func TestClaimRestorePackOrder(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	ids := tenantWithObjects(t, m, 2, "cbo", 10, 2)
	plain, fast := ids[0], ids[1]
	setEvictState(t, m, plain, "archived", false, "-1 hour")
	setEvictState(t, m, fast, "archived", false, "-1 hour")
	// Разные пачки: каждая — отдельное чтение ленты.
	recallFixture(t, m, plain, 10)
	recallFixture(t, m, fast, 10)

	// Обычный запрошен раньше срочного — порядок обязан определяться приоритетом.
	requestRestore(t, m, plain, 7, "Standard")
	time.Sleep(10 * time.Millisecond)
	requestRestore(t, m, fast, 7, "Expedited")

	items, err := m.ClaimRestorePack(ctx, "w1", 10, time.Minute, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].ObjectID != fast {
		t.Fatalf("первый захват %+v, ждали только пачку срочного запроса", items)
	}
	// Пачка обычного запроса не тронута и берётся следующим проходом.
	if st, att, _ := restoreState(t, m, plain); st != "requested" || att != 0 {
		t.Errorf("чужая пачка тронута: %s attempts=%d", st, att)
	}
	items, err = m.ClaimRestorePack(ctx, "w1", 10, time.Minute, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].ObjectID != plain {
		t.Errorf("второй захват %+v, ждали пачку обычного запроса", items)
	}
}

// T3 справедливость: пока у тенанта есть читаемые restore, следующий захват
// уходит к другому тенанту — бэклог одного не держит все драйвы. Два живых
// воркера: первый держит пачку тенанта 2 (её запросы старше), второй обязан
// взять тенанта 3, хотя у тенанта 2 остались запросы старше.
func TestClaimRestorePackFairnessAcrossTenants(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	a := tenantWithObjects(t, m, 2, "fair-a", 10, 2)
	b := tenantWithObjects(t, m, 3, "fair-b", 10, 1)
	for _, id := range []uuid.UUID{a[0], a[1], b[0]} {
		setEvictState(t, m, id, "archived", false, "-1 hour")
		recallFixture(t, m, id, 10)
	}
	requestRestore(t, m, a[0], 7, "Standard")
	requestRestore(t, m, a[1], 7, "Standard")
	if _, err := m.RequestRestore(ctx, meta.NewTenant(3), b[0], 7, "Standard"); err != nil {
		t.Fatal(err)
	}
	// Запросы тенанта 2 старше: без справедливости очередь шла бы строго по
	// времени, и тенант 3 ждал бы всего бэклога тенанта 2.
	if _, err := m.Pool.Exec(ctx,
		`UPDATE restores SET requested_at = requested_at - interval '1 hour' WHERE tenant_id = 2`); err != nil {
		t.Fatal(err)
	}

	// Воркер 1: при нулевой загрузке обоих — снова старый вперёд.
	first, err := m.ClaimRestorePack(ctx, "w1", 10, time.Minute, 5)
	if err != nil || len(first) != 1 || first[0].ObjectID != a[0] {
		t.Fatalf("первый захват %+v err=%v, ждали a[0]", first, err)
	}
	// Воркер 2, пока первый читает ленту: у тенанта 2 активный захват, и
	// выборка обязана уйти к тенанту 3.
	second, err := m.ClaimRestorePack(ctx, "w2", 10, time.Minute, 5)
	if err != nil || len(second) != 1 || second[0].ObjectID != b[0] {
		t.Fatalf("второй захват %+v err=%v, ждали b[0]: бэклог тенанта 2 держит очередь", second, err)
	}
	// Воркер 1 закончил — тенант 2 разгрузился и получил свой ход.
	if err := m.MarkRestoreRestored(ctx, a[0], 7, "w1"); err != nil {
		t.Fatal(err)
	}
	third, err := m.ClaimRestorePack(ctx, "w1", 10, time.Minute, 5)
	if err != nil || len(third) != 1 || third[0].ObjectID != a[1] {
		t.Fatalf("третий захват %+v err=%v, ждали a[1]", third, err)
	}
}

// Приоритет (tier) остаётся первым ключом: справедливость применяется внутри
// тира, а не поверх него. Expedited загруженного тенанта не уступает Standard
// свободного — тира контрактная, и обещание «час вместо часов» важнее
// очерёдности между тенантами.
func TestClaimRestorePackPriorityBeatsFairness(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	a := tenantWithObjects(t, m, 2, "pf-a", 10, 2)
	b := tenantWithObjects(t, m, 3, "pf-b", 10, 1)
	for _, id := range []uuid.UUID{a[0], a[1], b[0]} {
		setEvictState(t, m, id, "archived", false, "-1 hour")
		recallFixture(t, m, id, 10)
	}
	requestRestore(t, m, a[0], 7, "Standard")
	// Воркер забрал a[0]: у тенанта 2 активный захват (in-flight = 1).
	if _, err := m.ClaimRestorePack(ctx, "w1", 10, time.Minute, 5); err != nil {
		t.Fatal(err)
	}
	if _, err := m.RequestRestore(ctx, meta.NewTenant(3), b[0], 7, "Standard"); err != nil {
		t.Fatal(err)
	}
	requestRestore(t, m, a[1], 7, "Expedited")
	// Request b[0] старше запроса a[1]: без приоритета выиграл бы тенант 3.
	if _, err := m.Pool.Exec(ctx,
		`UPDATE restores SET requested_at = requested_at - interval '1 hour' WHERE tenant_id = 3`); err != nil {
		t.Fatal(err)
	}

	items, err := m.ClaimRestorePack(ctx, "w2", 10, time.Minute, 5)
	if err != nil || len(items) != 1 || items[0].ObjectID != a[1] {
		t.Fatalf("захват %+v err=%v, ждали Expedited загруженного тенанта", items, err)
	}
}

// Все готовые строки пачки уходят одним захватом: одно чтение ленты закрывает
// их целиком, а не по строке на проход.
func TestClaimRestorePackTakesWholePack(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	ids := tenantWithObjects(t, m, 2, "cwp", 10, 2)
	a, b := ids[0], ids[1]
	setEvictState(t, m, a, "archived", false, "-1 hour")
	setEvictState(t, m, b, "archived", false, "-1 hour")
	packID, _ := recallFixture(t, m, a, 10)
	// b — второй участник той же пачки.
	if _, err := m.Pool.Exec(ctx,
		`UPDATE objects SET pack_id = $2, pack_offset = 10, pack_length = 10, file_bytes = 10
		 WHERE id = $1`, b, packID); err != nil {
		t.Fatal(err)
	}
	requestRestore(t, m, a, 7, "Standard")
	requestRestore(t, m, b, 7, "Standard")

	items, err := m.ClaimRestorePack(ctx, "w1", 10, time.Minute, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("захвачено %d, ждали обоих участников пачки", len(items))
	}
	for _, it := range items {
		if it.PackID != packID {
			t.Errorf("в захват попала чужая пачка %s", it.PackID)
		}
	}
}

// Просроченный захват возвращается в работу с засчитанной попыткой; на
// исчерпании max_attempts — 'failed' и никому не отдаётся.
func TestClaimRestorePackLeaseExpiry(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	id := tenantWithObjects(t, m, 2, "cl1", 10, 1)[0]
	setEvictState(t, m, id, "archived", false, "-1 hour")
	recallFixture(t, m, id, 10)
	requestRestore(t, m, id, 7, "Standard")

	if items, err := m.ClaimRestorePack(ctx, "w1", 10, time.Minute, 2); err != nil || len(items) != 1 {
		t.Fatalf("первый claim: %d шт., %v", len(items), err)
	}
	// Lease «истёк» — строка возвращается, попытка засчитана.
	if _, err := m.Pool.Exec(ctx,
		`UPDATE restores SET next_try_at = now() - interval '1 second' WHERE object_id = $1`, id); err != nil {
		t.Fatal(err)
	}
	if items, err := m.ClaimRestorePack(ctx, "w1", 10, time.Minute, 2); err != nil || len(items) != 1 {
		t.Fatalf("повторный claim после lease: %d шт., %v", len(items), err)
	}
	if st, att, _ := restoreState(t, m, id); st != "in_progress" || att != 1 {
		t.Errorf("после перехвата lease: %s attempts=%d, ждали in_progress/1", st, att)
	}

	// Истёкший захват на исчерпании попыток — 'failed', не отдаётся.
	if _, err := m.Pool.Exec(ctx,
		`UPDATE restores SET next_try_at = now() - interval '1 second' WHERE object_id = $1`, id); err != nil {
		t.Fatal(err)
	}
	if items, err := m.ClaimRestorePack(ctx, "w1", 10, time.Minute, 2); err != nil || len(items) != 0 {
		t.Fatalf("просроченный на исчерпании отдан: %d шт., %v", len(items), err)
	}
	if st, _, _ := restoreState(t, m, id); st != "failed" {
		t.Errorf("состояние %s, ждали failed", st)
	}
}

// Захват закрепляется за воркером (claimed_by), и только владелец продлевает
// lease и завершает переходы. Без владельца продление неотличимо от чужого
// захвата, а чтение ленты длится дольше lease: чужое продление вернуло бы в
// работу строку, которой уже занимается другой, а чужой переход записал бы
// результат, которого на диске нет.
func TestRestoreClaimOwner(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	id := tenantWithObjects(t, m, 2, "own1", 10, 1)[0]
	setEvictState(t, m, id, "archived", false, "-1 hour")
	recallFixture(t, m, id, 10)
	requestRestore(t, m, id, 7, "Standard")

	if _, err := m.ClaimRestorePack(ctx, "nodeA/1", 10, time.Minute, 5); err != nil {
		t.Fatal(err)
	}
	claimedBy := func() string {
		var s string
		if err := m.Pool.QueryRow(ctx,
			`SELECT COALESCE(claimed_by, '') FROM restores WHERE object_id = $1`, id).Scan(&s); err != nil {
			t.Fatal(err)
		}
		return s
	}
	if claimedBy() != "nodeA/1" {
		t.Fatalf("claimed_by = %q, ждали nodeA/1", claimedBy())
	}

	nextTry := func() time.Time {
		var ts time.Time
		if err := m.Pool.QueryRow(ctx,
			`SELECT next_try_at FROM restores WHERE object_id = $1`, id).Scan(&ts); err != nil {
			t.Fatal(err)
		}
		return ts
	}
	ids := []uuid.UUID{id}

	// Чужое продление не проходит и ничего не сдвигает.
	before := nextTry()
	if ok, err := m.RenewRestoreClaim(ctx, ids, "nodeB/1", time.Hour); err != nil || ok {
		t.Errorf("чужак продлил захват: ok=%v err=%v", ok, err)
	}
	if !nextTry().Equal(before) {
		t.Errorf("чужое продление сдвинуло lease: %v → %v", before, nextTry())
	}

	// Владелец продлевает: lease уезжает в будущее.
	if ok, err := m.RenewRestoreClaim(ctx, ids, "nodeA/1", time.Hour); err != nil || !ok {
		t.Errorf("продление владельца: ok=%v err=%v", ok, err)
	}
	if d := time.Until(nextTry()); d < 55*time.Minute {
		t.Errorf("после продления владельца lease через %v, ждали ~час", d)
	}

	// Чужие переходы не трогают строку: ни restored, ни попытка, ни corrupt.
	if err := m.MarkRestoreRestored(ctx, id, 7, "nodeB/1"); err != meta.ErrRestoreLost {
		t.Errorf("restored чужим воркером: %v, ждали ErrRestoreLost", err)
	}
	if err := m.MarkRestoreFailure(ctx, id, 5, time.Second, "чужой сбой", "nodeB/1"); err != nil {
		t.Fatal(err)
	}
	if err := m.MarkRestoreCorrupt(ctx, id, "чужая порча", "nodeB/1"); err != nil {
		t.Fatal(err)
	}
	if st, att, _ := restoreState(t, m, id); st != "in_progress" || att != 0 {
		t.Errorf("чужие переходы тронули строку: %s attempts=%d", st, att)
	}

	// Владелец завершает; захват снят — продлить «своё» больше нельзя.
	if err := m.MarkRestoreRestored(ctx, id, 7, "nodeA/1"); err != nil {
		t.Fatal(err)
	}
	if claimedBy() != "" {
		t.Errorf("после restored claimed_by = %q, ждали NULL", claimedBy())
	}
	if ok, err := m.RenewRestoreClaim(ctx, ids, "nodeA/1", time.Hour); err != nil || ok {
		t.Errorf("продление завершённой строки: ok=%v err=%v", ok, err)
	}
}

// Не берутся: не-archived, без координат пачки, раньше next_try_at.
func TestClaimRestorePackSkips(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	ids := tenantWithObjects(t, m, 2, "cks", 10, 3)
	pending, noPack, future := ids[0], ids[1], ids[2]

	setEvictState(t, m, pending, "pending", true, "1 day")
	requestRestore(t, m, pending, 7, "Standard") // не архив — и в очередь не встанет; вставим вручную
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO restores (object_id, tenant_id, state, tier, days, priority, requested_at)
		 VALUES ($1, 2, 'requested', 'Standard', 7, 1, now())
		 ON CONFLICT (object_id) DO NOTHING`, pending); err != nil {
		t.Fatal(err)
	}

	setEvictState(t, m, noPack, "archived", false, "-1 hour")
	requestRestore(t, m, noPack, 7, "Standard") // pack-колонок нет — брать нечего

	setEvictState(t, m, future, "archived", false, "-1 hour")
	recallFixture(t, m, future, 10)
	requestRestore(t, m, future, 7, "Standard")
	if _, err := m.Pool.Exec(ctx,
		`UPDATE restores SET next_try_at = now() + interval '1 hour' WHERE object_id = $1`, future); err != nil {
		t.Fatal(err)
	}

	items, err := m.ClaimRestorePack(ctx, "w1", 10, time.Minute, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 0 {
		t.Errorf("взято %d участников там, где брать нечего: %+v", len(items), items)
	}
}

// Переход в restored: срок от now(), GREATEST не укорачивает больший срок,
// переход под защитой состояния.
func TestMarkRestoreRestored(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	ids := tenantWithObjects(t, m, 2, "mr1", 10, 2)
	past, big := ids[0], ids[1]
	setEvictState(t, m, past, "archived", false, "-1 hour")
	setEvictState(t, m, big, "archived", false, "30 days")
	for _, id := range ids {
		recallFixture(t, m, id, 10)
		requestRestore(t, m, id, 7, "Standard")
		if _, err := m.ClaimRestorePack(ctx, "w1", 10, time.Minute, 5); err != nil {
			t.Fatal(err)
		}
	}

	if err := m.MarkRestoreRestored(ctx, past, 7, "w1"); err != nil {
		t.Fatal(err)
	}
	var expires time.Time
	if err := m.Pool.QueryRow(ctx,
		`SELECT expires_at FROM restores WHERE object_id = $1`, past).Scan(&expires); err != nil {
		t.Fatal(err)
	}
	if d := time.Until(expires); d < 6*24*time.Hour || d > 8*24*time.Hour {
		t.Errorf("expires_at через %v, ждали ~7 дней от завершения", d)
	}
	if u := onlineUntil(t, m, past); time.Until(u) < 6*24*time.Hour {
		t.Errorf("online_until не продлён на срок от завершения: %v", u)
	}

	// Больший существующий срок не укорачивается семидневным restore.
	if err := m.MarkRestoreRestored(ctx, big, 7, "w1"); err != nil {
		t.Fatal(err)
	}
	if u := onlineUntil(t, m, big); time.Until(u) < 29*24*time.Hour {
		t.Errorf("online_until укорочен: %v, ждали прежние ~30 дней", u)
	}

	// Повтор: строка уже не 'in_progress' — переход не проходит (правило 7).
	if err := m.MarkRestoreRestored(ctx, past, 7, "w1"); err != meta.ErrRestoreLost {
		t.Errorf("повтор вернул %v, ждали ErrRestoreLost", err)
	}
	// days < 1 — ошибка вызывающего, не тихий ноль.
	if err := m.MarkRestoreRestored(ctx, past, 0, "w1"); err == nil {
		t.Error("days=0 принят")
	}
}

// Ошибка попытки: backoff и переход в 'failed' на исчерпании; чужое состояние
// не трогается.
func TestMarkRestoreFailure(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	id := tenantWithObjects(t, m, 2, "mf1", 10, 1)[0]
	setEvictState(t, m, id, "archived", false, "-1 hour")
	recallFixture(t, m, id, 10)
	requestRestore(t, m, id, 7, "Standard")
	if _, err := m.ClaimRestorePack(ctx, "w1", 10, time.Minute, 3); err != nil {
		t.Fatal(err)
	}

	if err := m.MarkRestoreFailure(ctx, id, 3, 30*time.Second, "лента легла", "w1"); err != nil {
		t.Fatal(err)
	}
	st, att, next := restoreState(t, m, id)
	if st != "requested" || att != 1 {
		t.Errorf("после сбоя: %s attempts=%d, ждали requested/1", st, att)
	}
	if next == nil || !next.After(time.Now()) {
		t.Errorf("next_try_at %v, ждали будущее (backoff)", next)
	}

	// Третья попытка из трёх — 'failed', next_try_at не нужен. Backoff первой
	// ошибки отодвигает next_try_at в будущее — в тесте он «уже наступил».
	if _, err := m.Pool.Exec(ctx,
		`UPDATE restores SET next_try_at = now() - interval '1 second', attempts = 2
		 WHERE object_id = $1`, id); err != nil {
		t.Fatal(err)
	}
	if _, err := m.ClaimRestorePack(ctx, "w1", 10, time.Minute, 3); err != nil {
		t.Fatal(err)
	}
	if err := m.MarkRestoreFailure(ctx, id, 3, 30*time.Second, "лента всё ещё легла", "w1"); err != nil {
		t.Fatal(err)
	}
	if st, att, next := restoreState(t, m, id); st != "failed" || att != 3 || next != nil {
		t.Errorf("на исчерпании: %s attempts=%d next=%v, ждали failed/3/nil", st, att, next)
	}

	// Сбой у строки не в 'in_progress' — ничего не меняем (правило 7).
	if err := m.MarkRestoreFailure(ctx, id, 3, 30*time.Second, "повтор сбоя", "w1"); err != nil {
		t.Fatal(err)
	}
	if st, att, _ := restoreState(t, m, id); st != "failed" || att != 3 {
		t.Errorf("сбой не в in_progress изменил строку: %s attempts=%d", st, att)
	}

	// Нулевой backoff — ошибка сборки вызова, а не «повтор в тот же момент».
	if err := m.MarkRestoreFailure(ctx, id, 3, 0, "нет backoff", "w1"); err == nil {
		t.Error("нулевой backoff принят")
	}
}

// Готовая копия пачки: oid и сумма; без готовой копии — ErrNotFound.
func TestPackCopy(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	id := tenantWithObjects(t, m, 2, "pc1", 10, 1)[0]
	packID, sha := recallFixture(t, m, id, 10)

	cp, err := m.PackCopy(ctx, packID)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(cp.SHA256, sha) {
		t.Errorf("сумма копии %x, ждали записанную %x", cp.SHA256, sha)
	}
	if cp.OID == uuid.Nil {
		t.Error("oid копии пустой")
	}

	if _, err := m.PackCopy(ctx, uuid.New()); err != meta.ErrNotFound {
		t.Errorf("для неизвестной пачки %v, ждали ErrNotFound", err)
	}
}

// Вытеснение cache: только 'restored', только archived без landing, только по
// истечении online_until; строка уходит вместе с выборкой (файл снимает
// вызывающий после commit).
func TestClaimCacheEvictBatch(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	ids := tenantWithObjects(t, m, 2, "ce1", 10, 4)
	expired, alive, onLanding, notRestored := ids[0], ids[1], ids[2], ids[3]
	setEvictState(t, m, expired, "archived", false, "-1 hour")
	setEvictState(t, m, alive, "archived", false, "1 day")
	setEvictState(t, m, onLanding, "archived", true, "-1 hour")
	setEvictState(t, m, notRestored, "archived", false, "-1 hour")

	insertRestore := func(id uuid.UUID, state string) {
		if _, err := m.Pool.Exec(ctx,
			`INSERT INTO restores (object_id, tenant_id, state, tier, days, requested_at,
			                       restored_at, expires_at)
			 VALUES ($1, 2, $2, 'Standard', 7, now() - interval '8 days',
			         now() - interval '8 days', now() - interval '1 day')`, id, state); err != nil {
			t.Fatal(err)
		}
	}
	insertRestore(expired, "restored")
	insertRestore(alive, "restored")
	insertRestore(onLanding, "restored")
	insertRestore(notRestored, "requested")

	got, err := m.ClaimCacheEvictBatch(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != expired {
		t.Fatalf("вытеснено %+v, ждали ровно истёкший restore", got)
	}
	var n int
	if err := m.Pool.QueryRow(ctx,
		`SELECT count(*) FROM restores WHERE object_id = $1`, expired).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Error("строка истёкшего restore осталась после выборки")
	}
	// Остальные три — на месте.
	if err := m.Pool.QueryRow(ctx,
		`SELECT count(*) FROM restores WHERE object_id <> $1`, expired).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Errorf("затронуты чужие строки: %d, ждали 3 нетронутых", n)
	}
}
