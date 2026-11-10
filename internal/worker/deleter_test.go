package worker_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/mover"
	"github.com/vladsimachkov/tapegw/internal/worker"
)

// Deleter (M6) на том же стенде, что и остальные воркеры: настоящая БД,
// настоящий mover-сервер movertest, fake-хранилище вместо Phobos.
//
// Главный критерий здесь — порядок «сначала лента, потом строка»: строка
// 'deleted' не имеет права появиться, пока mover не подтвердил снятие объекта,
// и наоборот — подтверждённое снятие не должно потеряться при падении между
// шагом и переходом.

func startDeleter(t *testing.T, h *harness, ctx context.Context) *worker.DeleterGroup {
	t.Helper()
	h.cfg.Deleter.Interval = 50 * time.Millisecond
	h.cfg.Deleter.ClaimTTL = 5 * time.Second
	h.cfg.Deleter.RetryBackoff = 10 * time.Millisecond
	h.cfg.Deleter.ShutdownGrace = 10 * time.Second
	return worker.StartDeleter(ctx, worker.DeleterDeps{
		Cfg: h.cfg, Meta: h.m, Spool: h.land, Cache: h.cache, Log: h.log,
	})
}

// archivedFixture — «объект уже на ленте» без прогона migrator'а: настоящий
// landing-файл через бэкенд, строка пачки и копии в БД, байты пачки в fake-
// хранилище через честный put mover'а (иначе Has/purge проверяли бы не то).
// Возвращает id пачки, id участников и oid копии.
func archivedFixture(t *testing.T, h *harness, bucket string, keys []string) (uuid.UUID, []uuid.UUID, uuid.UUID) {
	t.Helper()
	ctx := context.Background()

	var ids []uuid.UUID
	var sizes []int64
	for _, k := range keys {
		body := bytes.Repeat([]byte(k), 64)
		id := h.putObject(t, bucket, k, body)
		ids = append(ids, id)
		sizes = append(sizes, int64(len(body)))
	}

	var bucketID int64
	if err := h.m.Pool.QueryRow(ctx, `SELECT id FROM buckets WHERE name = $1`, bucket).Scan(&bucketID); err != nil {
		t.Fatal(err)
	}
	packID := uuid.New()
	oid := uuid.NewSHA1(packID, []byte("main"))
	var total int64
	for _, s := range sizes {
		total += s
	}
	var mediaID int64
	if err := h.m.Pool.QueryRow(ctx, `SELECT id FROM media WHERE label = 'TST-MOVER'`).Scan(&mediaID); err != nil {
		t.Fatal(err)
	}

	// Байты пачки — в fake-хранилище через mover: объект обязан быть целым,
	// иначе проверялась бы ветка «недозаписанная копия».
	c := h.env.Dial(t)
	body := bytes.Repeat([]byte("pack"), int(total/4)+1)
	sumBytes := sha256.Sum256(body)
	res, err := c.Put(mover.PutIn{Op: oid, ID: oid, Size: int64(len(body)),
		SHA256: hex.EncodeToString(sumBytes[:]), Body: bytes.NewReader(body)})
	if err != nil || res.Code != mover.CodeOK {
		t.Fatalf("запись пачки в fake: %v %+v", err, res)
	}

	sum := sha256.Sum256(body)
	if _, err := h.m.Pool.Exec(ctx,
		`INSERT INTO packs (id, pool_id, kind, state, total_bytes, live_bytes, members_total, members_live)
		  VALUES ($1, 1, 'data', 'archived', $2, $2, $3, $3)`,
		packID, total, len(ids)); err != nil {
		t.Fatal(err)
	}
	if _, err := h.m.Pool.Exec(ctx,
		`INSERT INTO archive_copies (pack_id, spec, phobos_oid, phobos_version, format, state, sha256, medium_id, done_at)
		  VALUES ($1, 'main', $2, 1, 1, 'done', $3, $4, now())`,
		packID, oid.String(), sum[:], mediaID); err != nil {
		t.Fatal(err)
	}
	var off int64
	for i, id := range ids {
		if _, err := h.m.Pool.Exec(ctx,
			`UPDATE objects SET state = 'archived', pack_id = $2, pack_offset = $3, pack_length = $4
			  WHERE id = $1`, id, packID, off, sizes[i]); err != nil {
			t.Fatal(err)
		}
		off += sizes[i]
	}
	return packID, ids, oid
}

func deleteVersion(t *testing.T, h *harness, bucket, key string) {
	t.Helper()
	if _, err := h.m.DeleteObject(context.Background(), meta.NewTenant(1), meta.DeleteObjectInput{
		Bucket: bucket, Key: key,
	}); err != nil {
		t.Fatalf("DeleteObject %s/%s: %v", bucket, key, err)
	}
}

// Полный путь M6: все участники удалены → purge подтверждён → строки в
// 'deleted', пачка ретайрена, объекта нет в Phobos, файлы сняты.
func TestDeleterPurgesEmptyPack(t *testing.T) {
	h := newHarness(t)
	packID, ids, oid := archivedFixture(t, h, "db1", []string{"a"})
	deleteVersion(t, h, "db1", "a")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startDeleter(t, h, ctx)

	waitFor(t, "версия в deleted", func() bool { return objectState(t, h.m, ids[0]) == "deleted" })
	waitFor(t, "пачка retired", func() bool { return packState(t, h.m, packID) == "retired" })
	if h.env.Store.Has(oid) {
		t.Error("объект пачки остался в Phobos после purge")
	}
	if fileExists(h, ids[0]) {
		t.Error("landing-файл удалённой версии остался на диске")
	}
	if landingFlag(t, h, ids[0]) {
		t.Error("флаг landing не снят")
	}
}

// Живой участник: ленту не трогаем, снимаем только файлы удалённого участника
// — байты на ленте ждут уплотнения (0011).
func TestDeleterKeepsTapeWhileMemberAlive(t *testing.T) {
	h := newHarness(t)
	packID, ids, oid := archivedFixture(t, h, "db2", []string{"a", "b"})
	deleteVersion(t, h, "db2", "a")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startDeleter(t, h, ctx)

	waitFor(t, "удалённый участник в deleted", func() bool { return objectState(t, h.m, ids[0]) == "deleted" })
	if !h.env.Store.Has(oid) {
		t.Error("пачка с живым участником удалена с ленты")
	}
	if packState(t, h.m, packID) != "archived" {
		t.Errorf("пачка в %s, должна ждать в archived", packState(t, h.m, packID))
	}
	if fileExists(h, ids[0]) {
		t.Error("файл удалённого участника не снят")
	}
	if !fileExists(h, ids[1]) {
		t.Error("файл живого участника пострадал")
	}
}

// Object Lock перечитывается перед удалением: версия под legal hold не
// удаляется ни в каком состоянии, даже уже стоя в 'deleting'.
func TestDeleterSkipsLockedVersion(t *testing.T) {
	h := newHarness(t)
	packID, ids, oid := archivedFixture(t, h, "db3", []string{"a"})
	deleteVersion(t, h, "db3", "a")
	if _, err := h.m.Pool.Exec(context.Background(),
		`UPDATE objects SET legal_hold = true WHERE id = $1`, ids[0]); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startDeleter(t, h, ctx)

	// Проход должен пройти и ничего не сделать: ни строки, ни ленты, ни файла.
	time.Sleep(500 * time.Millisecond)
	if objectState(t, h.m, ids[0]) != "deleting" {
		t.Error("версия под lock'ом ушла из deleting")
	}
	if !h.env.Store.Has(oid) || packState(t, h.m, packID) != "archived" {
		t.Error("пачка под заблокированным участником тронута")
	}
	if !fileExists(h, ids[0]) {
		t.Error("файл заблокированной версии снят — это единственная копия до purge")
	}
}

// Восстановление в работе: пачку не удалять, участника с активным restore —
// тоже (recaller читает ленту и пишет cache).
func TestDeleterSkipsRestoreBusy(t *testing.T) {
	h := newHarness(t)
	packID, ids, oid := archivedFixture(t, h, "db4", []string{"a"})
	deleteVersion(t, h, "db4", "a")
	if _, err := h.m.Pool.Exec(context.Background(),
		`INSERT INTO restores (object_id, tenant_id, state, days) VALUES ($1, 1, 'in_progress', 1)`, ids[0]); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startDeleter(t, h, ctx)

	time.Sleep(500 * time.Millisecond)
	if objectState(t, h.m, ids[0]) != "deleting" {
		t.Error("участник с восстановлением в работе удалён")
	}
	if !h.env.Store.Has(oid) || packState(t, h.m, packID) != "archived" {
		t.Error("пачка с восстановлением в работе удалена с ленты")
	}
}

// Версия под GOVERNANCE удалена с обходом (ADR-0015): срок на строке остаётся
// в будущем — его снимают по истечении, а не по факту досрочного удаления, — и
// без следа обхода удалитель читал бы такой срок как вечную блокировку. Тогда
// клиент получил успешный ответ, а байты остались на ленте и в landing
// бессрочно. Здесь проверяется следствие: след разблокирует снятие.
func TestDeleterRemovesGovernanceBypassed(t *testing.T) {
	h := newHarness(t)
	packID, ids, oid := archivedFixture(t, h, "db10", []string{"a"})
	ctx := context.Background()
	if _, err := h.m.Pool.Exec(ctx,
		`UPDATE objects SET retention_mode = 'GOVERNANCE',
		       retain_until = now() + interval '1 day'
		  WHERE id = $1`, ids[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := h.m.DeleteObject(ctx, meta.NewTenant(1), meta.DeleteObjectInput{
		Bucket: "db10", Key: "a", VersionID: ids[0].String(), BypassGovernance: true,
	}); err != nil {
		t.Fatalf("удаление с обходом GOVERNANCE: %v", err)
	}

	ctx2, cancel := context.WithCancel(context.Background())
	defer cancel()
	startDeleter(t, h, ctx2)

	waitFor(t, "версия с обходом ушла в deleted", func() bool { return objectState(t, h.m, ids[0]) == "deleted" })
	waitFor(t, "пачка retired", func() bool { return packState(t, h.m, packID) == "retired" })
	if h.env.Store.Has(oid) {
		t.Error("обход GOVERNANCE не разблокировал снятие пачки с ленты")
	}
	if fileExists(h, ids[0]) {
		t.Error("landing-файл версии с обходом остался на диске")
	}
}

// COMPLIANCE обхода не имеет (отказ проверяет meta.TestDeleteVersionUnderLock),
// а если срок появился уже после того, как версия встала в 'deleting',
// удалитель обязан её пропустить: след обхода GOVERNANCE на такие строки не
// ставится и не может их разблокировать.
func TestDeleterSkipsComplianceInDeleting(t *testing.T) {
	h := newHarness(t)
	packID, ids, oid := archivedFixture(t, h, "db11", []string{"a"})
	deleteVersion(t, h, "db11", "a")
	if _, err := h.m.Pool.Exec(context.Background(),
		`UPDATE objects SET retention_mode = 'COMPLIANCE',
		       retain_until = now() + interval '1 day'
		  WHERE id = $1`, ids[0]); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startDeleter(t, h, ctx)

	time.Sleep(500 * time.Millisecond)
	if objectState(t, h.m, ids[0]) != "deleting" {
		t.Error("версия под COMPLIANCE переведена в deleted — блокировку сняли не тем")
	}
	if !h.env.Store.Has(oid) || packState(t, h.m, packID) != "archived" {
		t.Error("пачка под COMPLIANCE-участником тронута")
	}
}

// Пачку держит migrator (активный claim): ленту под его руками не трогаем.
// Участники при этом обязаны ОСТАТЬСЯ в 'deleting' — если перевести их сейчас,
// под пачкой не останется строк очереди, и после истечения захвата перевызывать
// deleter будет некому: объект пачки пережил бы удаление навсегда.
func TestDeleterSkipsClaimedPack(t *testing.T) {
	h := newHarness(t)
	packID, ids, oid := archivedFixture(t, h, "db5", []string{"a"})
	deleteVersion(t, h, "db5", "a")
	if _, err := h.m.Pool.Exec(context.Background(),
		`UPDATE packs SET claimed_by = 'ci/drive_a', claim_expires_at = now() + interval '10 min'
		  WHERE id = $1`, packID); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startDeleter(t, h, ctx)

	time.Sleep(500 * time.Millisecond)
	if objectState(t, h.m, ids[0]) != "deleting" {
		t.Error("участник переведён в deleted под захватом migrator'а — пачка останется на ленте без очереди")
	}
	if !h.env.Store.Has(oid) || packState(t, h.m, packID) != "archived" {
		t.Error("пачка под захватом migrator'а тронута")
	}

	// Захват истёк — тот же состав очереди обязан дойти до результата.
	if _, err := h.m.Pool.Exec(context.Background(),
		`UPDATE packs SET claimed_by = NULL, claim_expires_at = NULL WHERE id = $1`, packID); err != nil {
		t.Fatal(err)
	}
	if _, err := h.m.Pool.Exec(context.Background(),
		`UPDATE objects SET next_try_at = now() WHERE id = $1`, ids[0]); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "версия в deleted после снятия захвата", func() bool { return objectState(t, h.m, ids[0]) == "deleted" })
	waitFor(t, "пачка retired", func() bool { return packState(t, h.m, packID) == "retired" })
	if h.env.Store.Has(oid) {
		t.Error("объект пачки остался в Phobos после purge")
	}
}

// «Упасть после шага N»: mover отказывает в purge — строка обязана остаться
// в 'deleting' с отметкой попытки, а после починки сходиться к результату.
func TestDeleterConvergesAfterPurgeFailure(t *testing.T) {
	h := newHarness(t)
	packID, ids, oid := archivedFixture(t, h, "db5", []string{"a"})
	deleteVersion(t, h, "db5", "a")

	h.env.Store.FailDelete(errors.New("phobosd недоступен"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startDeleter(t, h, ctx)

	waitFor(t, "попытка зафиксирована", func() bool {
		var attempts int
		if err := h.m.Pool.QueryRow(context.Background(),
			`SELECT attempts FROM objects WHERE id = $1`, ids[0]).Scan(&attempts); err != nil {
			t.Fatal(err)
		}
		return attempts >= 1
	})
	if objectState(t, h.m, ids[0]) != "deleting" {
		t.Fatal("purge не подтверждён, а строка ушла из deleting")
	}
	if !h.env.Store.Has(oid) {
		t.Error("объект удалён, хотя mover отказывал")
	}

	// Починили — повтор без вреда доводит дело до конца.
	h.env.Store.FailDelete(nil)
	if _, err := h.m.Pool.Exec(context.Background(),
		`UPDATE objects SET next_try_at = now() WHERE id = $1`, ids[0]); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "после починки версия в deleted", func() bool { return objectState(t, h.m, ids[0]) == "deleted" })
	waitFor(t, "пачка retired", func() bool { return packState(t, h.m, packID) == "retired" })
	if h.env.Store.Has(oid) {
		t.Error("объект остался в Phobos после успешного повтора")
	}
}

// Версия, не лежавшая на ленте (pending → deleting): только диск, без mover.
func TestDeleterRemovesLooseVersion(t *testing.T) {
	h := newHarness(t)
	id := h.putObject(t, "db6", "loose.bin", []byte("не архивировалась"))
	deleteVersion(t, h, "db6", "loose.bin")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startDeleter(t, h, ctx)

	waitFor(t, "версия в deleted", func() bool { return objectState(t, h.m, id) == "deleted" })
	if fileExists(h, id) {
		t.Error("landing-файл не снят")
	}
}

// Заблокированный участник блокирует только purge пачки: остальные её
// участники снимаются с диска и уходят в deleted, лента ждёт.
func TestDeleterDeletesUnlockedSiblings(t *testing.T) {
	h := newHarness(t)
	packID, ids, oid := archivedFixture(t, h, "db7", []string{"a", "b"})
	deleteVersion(t, h, "db7", "a")
	deleteVersion(t, h, "db7", "b")
	if _, err := h.m.Pool.Exec(context.Background(),
		`UPDATE objects SET legal_hold = true WHERE id = $1`, ids[1]); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startDeleter(t, h, ctx)

	waitFor(t, "разблокированный участник в deleted", func() bool { return objectState(t, h.m, ids[0]) == "deleted" })
	if objectState(t, h.m, ids[1]) != "deleting" {
		t.Error("заблокированный участник удалён")
	}
	if !h.env.Store.Has(oid) || packState(t, h.m, packID) != "archived" {
		t.Error("пачка с заблокированным участником снята с ленты")
	}
	if fileExists(h, ids[0]) {
		t.Error("файл разблокированного участника не снят")
	}
	if !fileExists(h, ids[1]) {
		t.Error("файл заблокированного участника пострадал")
	}
}

// Purge отсутствующего в Phobos объекта — успех: повтор после падения, когда
// объект уже снят, обязан довести строки до 'deleted'.
func TestDeleterPurgeMissingConverges(t *testing.T) {
	h := newHarness(t)
	packID, ids, oid := archivedFixture(t, h, "db8", []string{"a"})
	deleteVersion(t, h, "db8", "a")
	// «Падение после purge»: объекта в Phobos уже нет, строка ещё в deleting.
	if res := h.env.Store.Delete(context.Background(), []uuid.UUID{oid}); len(res) != 1 || res[0].Err != nil {
		t.Fatalf("подготовка: %v", res)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startDeleter(t, h, ctx)

	waitFor(t, "версия в deleted после purge отсутствующего", func() bool { return objectState(t, h.m, ids[0]) == "deleted" })
	waitFor(t, "пачка retired", func() bool { return packState(t, h.m, packID) == "retired" })
}
