package migrator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/mover"
	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// lockNotAvailable — SQLSTATE lock_not_available. Именно её получает
// FOR UPDATE NOWAIT, когда строку держит чужая транзакция.
const lockNotAvailable = "55P03"

// probeMover отвечает как mover по протоколу и заодно проверяет правило 13:
// в момент put он пытается взять блокировку строки объекта БЕЗ ожидания. Если
// migrator держит открытую транзакцию, NOWAIT падает сразу, а не висит до
// таймаута.
type probeMover struct {
	// target — id объекта, который проверяем в момент put.
	target uuid.UUID
	m      *meta.Meta

	// SQLState — что вернул NOWAIT: пусто, когда блокировки нет, 55P03 когда
	// есть. Тест проверяет конкретно это поле, а не «что-то упало».
	SQLState string
	Detail   string
	accept   bool
	medium   string // что mover сообщит в extents

	// written — что реально легло «на ленту», по oid. Именно по нему тест
	// считает дубли.
	written map[uuid.UUID]int
	replays map[uuid.UUID]int // повторы той же пачки: Phobos на них не пишет
	// dropReply — сымитировать обрыв после записи, но до ответа.
	dropReply bool
	// forget — mover «потерял реестр»: Status ничего не знает.
	forget      bool
	statusCalls int
	lastOp      uuid.UUID
	lastID      uuid.UUID     // oid, с которым mover реально вызвали
	failPut     error         // если задан — put падает ДО записи (м mover лёг)
	block       chan struct{} // если задан — put ждёт в него перед записи
	code        mover.Code    // если задан — put вернёт этот код вместо ok
	puts        int           // сколько раз put реально вызывали

	// С5/С6: держал ли mover буфер для копий, и из какого буфера писалась
	// копия; сколько раз буфер освобождали.
	lastHold   bool
	lastFromOp uuid.UUID
	lastTags   []string
	copyCode   mover.Code // если задан — put с from_op вернёт этот код
	holds      int        // сколько put приходили с hold

	// #101: ответы на stat/delete по oid пачки.
	statCode       mover.Code
	statVersion    int
	statExtents    []string
	statExtentInfo []mover.ExtentInfo
	statSize       int64 // 0 = «как в плане»
	lastSize       int64
	deleteCode     mover.Code
	statCalls      int
	deleteCalls    int
	releases       int
}

func (p *probeMover) StatObject(id uuid.UUID) (*mover.Result, error) {
	p.statCalls++
	code := p.statCode
	if code == "" {
		code = mover.CodeNotFound
	}
	size := p.statSize
	if size == 0 {
		// По умолчанию размер под «уже есть» совпадает с планом: иначе каждый
		// тест на приём целой копии падал бы по несверке. Неправильный размер
		// задаётся явно.
		size = p.lastSize
	}
	info := p.statExtentInfo
	if info == nil {
		// По умолчанию — ровно один экстент в sync: план migrator тоже
		// «одна реплика». Две реплики задаёт явно тот тест, который их проверяет.
		info = []mover.ExtentInfo{{Size: size, Media: "TST-MIG", State: "sync"}}
	}
	return &mover.Result{Op: id, Code: code, Version: p.statVersion,
		Extents: p.statExtents, ExtentInfo: info, Size: size}, nil
}

func (p *probeMover) DeleteObject(id uuid.UUID) (*mover.Result, error) {
	p.deleteCalls++
	code := p.deleteCode
	if code == "" {
		code = mover.CodeOK
	}
	return &mover.Result{Op: id, Code: code}, nil
}

func (p *probeMover) Release(op uuid.UUID) (*mover.Result, error) {
	p.releases++
	return &mover.Result{Op: op, Code: mover.CodeOK}, nil
}

func (p *probeMover) Put(in mover.PutIn) (*mover.Result, error) {
	p.lastSize = in.Size
	p.puts++
	// Отказ одноразовый: он описывает состояние, которое разбор как раз и
	// устраняет (неполную копию удалили, объекта нет). Вечный отказ изображает
	// другой сценарий — тот, где повтор бесполезен, и он проверяется отдельно.
	if p.code != "" {
		code := p.code
		p.code = ""
		return &mover.Result{Op: in.Op, Code: code, State: mover.StateFailed}, nil
	}
	p.lastHold, p.lastFromOp = in.Hold, in.FromOp
	if in.Hold {
		p.holds++
	}
	p.lastID, p.lastTags = in.ID, append([]string(nil), in.Tags...)
	if p.written == nil {
		p.written = map[uuid.UUID]int{}
	}
	// Fake обязан вести себя как настоящий mover там, где настоящее известно,
	// иначе тест зелёный по пути, которого в продукте нет. Правила — дословно
	// из handlePutFrom и из «oid объекта = UUID операции».
	if in.ID != in.Op {
		return nil, errors.New("fake: oid обязан равняться op")
	}
	if in.FromOp != uuid.Nil {
		if in.FromOp == in.Op {
			return &mover.Result{Op: in.Op, Code: mover.CodeOpConflict, State: mover.StateFailed}, nil
		}
		if p.written[in.FromOp] == 0 {
			// Буфер принадлежит конкретному mover'у: чужого источника нет.
			return &mover.Result{Op: in.Op, Code: mover.CodeNotFound, State: mover.StateFailed}, nil
		}
		if p.copyCode != "" {
			return &mover.Result{Op: in.Op, Code: p.copyCode, State: mover.StateFailed}, nil
		}
	}
	// from_op: байтов по сети нет, mover читает свой буфер. Сумму здесь не
	// сверяют — её сверяет mover с файла; тест фиксирует сам факт передачи.
	if in.Body == nil {
		if in.FromOp == uuid.Nil {
			return nil, errors.New("put без тела и без from_op")
		}
		if p.written == nil {
			p.written = map[uuid.UUID]int{}
		}
		p.written[in.Op]++
		return &mover.Result{Op: in.Op, Code: mover.CodeOK, Version: 1, Extents: []string{p.medium}}, nil
	}
	body, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(body)
	if hex.EncodeToString(sum[:]) != in.SHA256 || int64(len(body)) != in.Size {
		p.accept = false
		return &mover.Result{Op: in.Op, Code: mover.CodeChecksumMismatch}, nil
	}

	// Проверка идёт на отдельном соединении: со своего же транзакцию не
	// заблокируешь, и тест молчал бы зелёным.
	var got uuid.UUID
	err = p.m.Pool.QueryRow(context.Background(),
		`SELECT id FROM objects WHERE id = $1 FOR UPDATE NOWAIT`, p.target).Scan(&got)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		var pe *pgconn.PgError
		if errors.As(err, &pe) {
			p.SQLState = pe.SQLState()
			p.Detail = pe.Message
		} else {
			p.SQLState = "other"
			p.Detail = err.Error()
		}
		return nil, fmt.Errorf("правило 13 нарушено: SQLSTATE %s (%s) — во время вызова mover открыта транзакция",
			p.SQLState, p.Detail)
	}

	p.lastOp = in.Op
	p.lastID = in.ID
	if p.failPut != nil {
		return nil, p.failPut
	}
	if p.block != nil {
		<-p.block
	}
	if p.code != "" && p.code != mover.CodeOK {
		return &mover.Result{Op: in.Op, Code: p.code}, nil
	}
	p.accept = true
	if p.written == nil {
		p.written = map[uuid.UUID]int{}
	}
	if p.replays == nil {
		p.replays = map[uuid.UUID]int{}
	}
	// Так ведёт себя Phobos: повторная запись с тем же oid не создаёт второй
	// объект. Именно это и делает пересылку той же пачки безопасной.
	if _, ok := p.written[in.ID]; ok {
		p.replays[in.ID]++
	} else {
		p.written[in.ID] = 1
	}
	if p.dropReply {
		// Запись прошла, ответ не дошёл — ровно тот обрыв, на котором
		// «вернуть FailPackArchive» даёт дубль.
		p.dropReply = false
		return nil, errors.New("соединение оборвано после записи")
	}
	return &mover.Result{Op: in.Op, Code: mover.CodeOK, Version: 1, Extents: []string{p.medium}}, nil
}

func (p *probeMover) Status(op uuid.UUID) (*mover.Result, error) {
	p.statusCalls++
	if p.forget {
		return &mover.Result{Op: op, Code: mover.CodeNotFound}, nil
	}
	if n := p.written[op]; n > 0 {
		return &mover.Result{Op: op, Code: mover.CodeOK, State: mover.StateDone, Version: 1,
			Extents: []string{p.medium}}, nil
	}
	return &mover.Result{Op: op, Code: mover.CodeNotFound}, nil
}

// seedObjects заводит тенанта, бакет и n pending-объектов.
func seedObjects(t *testing.T, m *meta.Meta, bucket string, n int) []uuid.UUID {
	t.Helper()
	ctx := context.Background()

	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO tenants (id, name, pool_id) VALUES (2, 'mig', 1) ON CONFLICT (id) DO NOTHING`); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO buckets (name, tenant_id, owner, acl, versioning)
		  VALUES ($1, 2, 'o', '{}'::jsonb, 'unversioned')`, bucket); err != nil {
		t.Fatal(err)
	}
	var bucketID int64
	if err := m.Pool.QueryRow(ctx, `SELECT id FROM buckets WHERE name = $1`, bucket).Scan(&bucketID); err != nil {
		t.Fatal(err)
	}

	var ids []uuid.UUID
	for i := 0; i < n; i++ {
		id := uuid.New()
		if _, err := m.Pool.Exec(ctx,
			`INSERT INTO objects (id, bucket_id, key, size, etag, storage_class, state,
			                      landing, online_until, tenant_id, pool_id, is_current,
			                      wrapped_dek)
			  VALUES ($1,$2,$3,10,'etag','GLACIER','pending',true,
			          now() + interval '1 day', 2, 1, true, 'x'::bytea)`,
			id, bucketID, fmt.Sprintf("k%d", i)); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	return ids
}

func itemsFor(ids []uuid.UUID) []Item {
	var out []Item
	for i, id := range ids {
		body := []byte(fmt.Sprintf("содержимое-%d", i))
		out = append(out, Item{
			Version:    id,
			Tenant:     2,
			VersionSeq: uint64(i + 1),
			PlainLen:   uint64(len(body)),
			KekVersion: 1,
			WrappedDEK: []byte("wrapped-dek"),
			CipherLen:  uint64(len(body)),
			Open:       func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil },
		})
	}
	return out
}

func archiver(t *testing.T, m *meta.Meta, target uuid.UUID) (*Archiver, *probeMover) {
	t.Helper()
	pm := &probeMover{m: m, target: target, medium: "TST-MIG"}
	if _, err := m.Pool.Exec(context.Background(), `
INSERT INTO media (label, lifecycle, pool_id, generation, medium_type, free_bytes_cached)
VALUES ('TST-MIG', 'active', 1, 'L8', 'L8', 1000) ON CONFLICT (label) DO NOTHING`); err != nil {
		t.Fatal(err)
	}
	return &Archiver{Meta: m, Put: pm, Spec: "pool:archive", Format: 1,
		WorkerID: "test-host/mover-" + uuid.NewString(), ClaimTTL: 30 * time.Second}, pm
}

// С8: во время вызова mover транзакция Postgres не открыта.
//
// Тест проверяет конкретно SQLSTATE: если бы он просто ждал ошибку, мутация
// «перенести вызов mover внутрь транзакции» упала бы где угодно — например на
// откате перехода, — и доказывала бы не то.
func TestNoTxnOpenDuringMoverCall(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	ids := seedObjects(t, m, "c8", 3)
	a, pm := archiver(t, m, ids[0])

	if _, err := a.ArchivePack(ctx, 1, itemsFor(ids), nil); err != nil {
		t.Fatalf("ArchivePack: %v", err)
	}
	// Проверяем конкретно SQLSTATE, а не «что-то упало»: иначе мутация
	// «перенести вызов mover внутрь транзакции» могла бы упасть по другой
	// причине и доказать не то.
	if pm.SQLState == lockNotAvailable {
		t.Errorf("правило 13 нарушено: NOWAIT на строку объекта вернул %s (%s) — во время put транзакция открыта",
			pm.SQLState, pm.Detail)
	}
	if pm.SQLState != "" && pm.SQLState != lockNotAvailable {
		t.Errorf("NOWAIT вернул неожиданный %s (%s)", pm.SQLState, pm.Detail)
	}
	if !pm.accept {
		t.Error("mover не принял пачку")
	}

	// И состояние подтверждено: объекты действительно прошли archiving.
	var state string
	if err := m.Pool.QueryRow(ctx, `SELECT state::text FROM objects WHERE id = $1`, ids[0]).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "archived" {
		t.Errorf("объект в состоянии %s, ждали archived", state)
	}
}

// С7: ожидаемая сумма потока сохраняется в archive_copies при первой записи.
// Без неё reconciler нечем проверить чтение с ленты, а задним числом её не
// восстановить: данные уже на ленте.
func TestExpectedSHAStoredOnFirstWrite(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	ids := seedObjects(t, m, "c7", 2)
	a, _ := archiver(t, m, ids[0])

	plan, err := a.ArchivePack(ctx, 1, itemsFor(ids), nil)
	if err != nil {
		t.Fatalf("ArchivePack: %v", err)
	}

	var got []byte
	var spec string
	var state string
	err = m.Pool.QueryRow(ctx, `
SELECT c.sha256, c.spec, c.state::text FROM archive_copies c
JOIN packs p ON p.id = c.pack_id ORDER BY c.created_at DESC LIMIT 1`).Scan(&got, &spec, &state)
	if err != nil {
		t.Fatalf("копия не записана: %v", err)
	}
	want, _ := hex.DecodeString(plan.SHA256)
	if !bytes.Equal(got, want) {
		t.Errorf("сумма копии %x, ждали %x — reconciler сверял бы не то", got, want)
	}
	if spec != "pool:archive" || state != "done" {
		t.Errorf("копия: %s/%s, ждали pool:archive/done", spec, state)
	}
	if len(got) != 32 {
		t.Errorf("длина суммы %d, ждали 32", len(got))
	}
}

// Отказ mover не оставляет объектов в archiving: иначе они зависли бы там
// навсегда, а повторный батч их не возьмёт.
func TestRefusedPackReturnsObjectsToPending(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	ids := seedObjects(t, m, "refuse", 2)
	items := itemsFor(ids)
	// Подменённый участник: сумма второго прохода не совпадёт с объявленной,
	// и migrator откажется сам, не доходя до mover.
	items[0].Open = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader([]byte("иная длина"))), nil
	}

	a, _ := archiver(t, m, ids[0])
	if _, err := a.ArchivePack(ctx, 1, items, nil); err == nil {
		t.Fatal("пачка с подменой прошла")
	}

	var state string
	if err := m.Pool.QueryRow(ctx, `SELECT state::text FROM objects WHERE id = $1`, ids[0]).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "pending" {
		t.Errorf("объект после отказа в %s, ждали pending — в archiving его не возьмёт ни один повтор", state)
	}
}

// Блокер: обрыв после записи, но до ответа.
//
// Если на транспортной ошибке выбросить пачку, следующий цикл соберёт НОВУЮ
// пачку с новым UUID и новым oid, и первая запись останется на ленте никем не
// учтённой: данные дважды, live_bytes нет, reconciler не видит. Поэтому при
// неизвестном итоге пачка остаётся, а итог разбирается через Status.
func TestTransportBreakAfterWriteDoesNotDuplicate(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	ids := seedObjects(t, m, "break", 2)
	a, pm := archiver(t, m, ids[0])
	pm.dropReply = true

	if _, err := a.ArchivePack(ctx, 1, itemsFor(ids), nil); err != nil {
		t.Fatalf("ArchivePack должен разобрать итог сам, а вернул ошибку: %v", err)
	}
	if len(pm.written) != 1 {
		t.Errorf("на ленте %d объектов, ждали 1 — дубль, который никто не учёл", len(pm.written))
	}
	if pm.statusCalls == 0 {
		t.Error("итог не разбирались через Status: откуда migrator знает, что запись прошла?")
	}

	var state string
	if err := m.Pool.QueryRow(ctx, `SELECT state::text FROM objects WHERE id = $1`, ids[0]).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "archived" {
		t.Errorf("объект в %s, ждали archived", state)
	}

	// Место должно быть учтено: иначе планировщик ёмкости считает враньё.
	var live int64
	if err := m.Pool.QueryRow(ctx, `SELECT live_bytes FROM packs ORDER BY created_at DESC LIMIT 1`).Scan(&live); err != nil {
		t.Fatal(err)
	}
	if live == 0 {
		t.Error("live_bytes пачки равен нулю — байты на ленте есть, а в учёте их нет")
	}
}

// Тот же обрыв, но mover потерял реестр: Status ничего не знает. Тогда
// пересылается ТА ЖЕ пачка — тот же op и тот же oid, и Phobos на повтор не
// пишет. Дубля не будет даже без stat по oid.
func TestLostRegistryResendsSamePack(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	ids := seedObjects(t, m, "lostreg", 2)
	a, pm := archiver(t, m, ids[0])
	pm.dropReply = true
	pm.forget = true

	if _, err := a.ArchivePack(ctx, 1, itemsFor(ids), nil); err != nil {
		t.Fatalf("ArchivePack: %v", err)
	}
	if len(pm.written) != 1 {
		t.Errorf("объектов на ленте %d, ждали 1", len(pm.written))
	}
	if pm.replays[pm.lastOp] != 1 {
		t.Errorf("повторов той же пачки %d, ждали 1 — пересылать надо тот же op, а не новый", pm.replays[pm.lastOp])
	}
}

// Продолжение запечатанной пачки после настоящего перезапуска.
//
// Сбой между запечатыванием и итогом оставляет объекты в 'archiving' с pack_id:
// в батч они не попадут, в pending не вернутся. Поэтому вход у продолжения — от
// пачки. Если вместо продолжения заведётся новая пачка, первая запись останется
// на ленте никем не учтённой: данные дважды, live_bytes врёт, reconciler не видит.
func TestResumeContinuesSealedPack(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	ids := seedObjects(t, m, "resume", 3)
	a, pm := archiver(t, m, ids[0])
	pm.failPut = errors.New("mover недоступен")

	if _, err := a.ArchivePack(ctx, 1, itemsFor(ids), nil); err == nil {
		t.Fatal("первая попытка должна была упасть")
	}

	// После сбоя участник обязан быть запечатан: без этого повтор заведёт новую
	// пачку, и весь смысл запечатывания теряется.
	var sealed uuid.UUID
	var state string
	if err := m.Pool.QueryRow(ctx,
		`SELECT pack_id, state::text FROM objects WHERE id = $1`, ids[0]).Scan(&sealed, &state); err != nil {
		t.Fatal(err)
	}
	if sealed == uuid.Nil {
		t.Fatal("объект не запечатан до отправки — повтор заведёт вторую пачку")
	}
	var packState string
	if err := m.Pool.QueryRow(ctx, `SELECT state::text FROM packs WHERE id = $1`, sealed).Scan(&packState); err != nil {
		t.Fatal(err)
	}
	if packState != "sealed" {
		t.Errorf("пачка в %s, ждали sealed", packState)
	}

	// «Настоящий перезапуск»: новый Archiver, и список id НЕ передаётся —
	// migrator обязан найти пачку сам. Именно это и проверяет тест: если поиск
	// идёт от списка объектов, после перезапуска его взять неоткуда.
	a2, pm2 := archiver(t, m, ids[0])
	// Тот же слот воркера: «перезапуск» — это тот же воркер, вернувшийся за
	// своей пачкой, а не другой воркер того же процесса.
	a2.WorkerID = a.WorkerID
	load := func(ctx context.Context, ids []uuid.UUID) ([]Item, error) {
		return itemsFor(ids), nil
	}
	done, err := a2.ResumePacks(ctx, a.WorkerID, time.Minute, load, nil)
	if err != nil {
		t.Fatalf("ResumePacks: %v", err)
	}
	if done != 1 {
		t.Fatalf("доведено пачек %d, ждали 1", done)
	}
	if pm2.lastID != sealed {
		t.Errorf("продолжение отправило пачку %s вместо запечатанной %s", pm2.lastID, sealed)
	}
	if len(pm2.written) != 1 {
		t.Errorf("объектов на ленте %d, ждали 1", len(pm2.written))
	}
	var claimed int
	if err := m.Pool.QueryRow(ctx,
		`SELECT count(*) FROM packs WHERE id = $1 AND claimed_by IS NOT NULL`, sealed).Scan(&claimed); err != nil {
		t.Fatal(err)
	}
	if claimed != 0 {
		t.Error("захват не снят после успешного продолжения — следующая попытка придёт нескоро")
	}

	var archived int
	if err := m.Pool.QueryRow(ctx,
		`SELECT count(*) FROM objects WHERE id = ANY($1::uuid[]) AND state = 'archived'`, ids).Scan(&archived); err != nil {
		t.Fatal(err)
	}
	if archived != len(ids) {
		t.Errorf("archived %d из %d", archived, len(ids))
	}
}

// Две дыры захвата, обе про одновременную работу двух процессов.

// Только что запечатанную пачку не должен забирать другой процесс: её ещё
// отправляет тот, кто запечатал. Иначе два воркера пишут одну пачку параллельно.
func TestFreshlySealedPackIsNotStolen(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	ids := seedObjects(t, m, "steal", 2)
	a, pm := archiver(t, m, ids[0])
	pm.block = make(chan struct{})

	done := make(chan error, 1)
	go func() {
		_, err := a.ArchivePack(ctx, 1, itemsFor(ids), nil)
		done <- err
	}()

	// Ждём, пока пачка точно запечатана: без захвата в SealPack второй процесс
	// подхватил бы её именно здесь.
	var sealed uuid.UUID
	for i := 0; i < 200; i++ {
		if err := m.Pool.QueryRow(ctx,
			`SELECT pack_id FROM objects WHERE id = $1`, ids[0]).Scan(&sealed); err == nil && sealed != uuid.Nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if sealed == uuid.Nil {
		t.Fatal("пачка так и не запечатана")
	}

	other, _ := archiver(t, m, ids[0])
	got, err := other.Meta.ClaimSealedPack(ctx, "чужой-процесс", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if got != uuid.Nil {
		t.Errorf("второй процесс забрал пачку %s, которую первый уже отправляет", got)
	}

	close(pm.block)
	if err := <-done; err != nil {
		t.Fatalf("ArchivePack: %v", err)
	}
}

// Захват продлевается во время отправки. Без продления срок истёк бы посреди
// записи (ожидание драйва и монтирование — десятки минут по ADR-0021), и второй
// процесс отправил бы пачку повторно параллельно с первой.
func TestClaimRenewedDuringSend(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	ids := seedObjects(t, m, "renew", 2)
	a, pm := archiver(t, m, ids[0])
	a.ClaimTTL = 300 * time.Millisecond // истёк бы раньше конца отправки
	pm.block = make(chan struct{})

	done := make(chan error, 1)
	go func() {
		_, err := a.ArchivePack(ctx, 1, itemsFor(ids), nil)
		done <- err
	}()

	var sealed uuid.UUID
	for i := 0; i < 200; i++ {
		if err := m.Pool.QueryRow(ctx,
			`SELECT pack_id FROM objects WHERE id = $1`, ids[0]).Scan(&sealed); err == nil && sealed != uuid.Nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Больше срока захвата: без продления пачка стала бы доступной чужим.
	time.Sleep(2 * a.ClaimTTL)

	other, _ := archiver(t, m, ids[0])
	got, err := other.Meta.ClaimSealedPack(ctx, "чужой-процесс", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if got != uuid.Nil {
		t.Errorf("захват истёк посреди отправки: второй процесс получил %s", got)
	}

	close(pm.block)
	if err := <-done; err != nil {
		t.Fatalf("ArchivePack: %v", err)
	}
}

// Потерянный захват не даёт завершить пачку поверх чужой работы.
func TestFinishRequiresOwnership(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	ids := seedObjects(t, m, "owner", 1)
	a, _ := archiver(t, m, ids[0])
	tx, packID, err := m.BeginPackArchive(ctx, 1, ids)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := m.SealPack(ctx, packID, []meta.Placement{{Version: ids[0], Offset: 0, Length: 10}},
		4096, a.WorkerID, a.ClaimTTL); err != nil {
		t.Fatal(err)
	}
	if err := m.FinishPackArchive(ctx, packID, []meta.Placement{{Version: ids[0], Offset: 0, Length: 10}},
		"pool:archive", packID.String(), 1, 1, make([]byte, 32), 0, "не-владелец"); err == nil {
		t.Error("пачку завершил не владелец захвата")
	}
}

// Идентичность захвата — на воркера, а не на процесс.
//
// Внутри migrator воркеров архивации несколько (ADR-0007: по одному на mover),
// и если бы захват был на процесс, второй воркер того же процесса забрал бы
// пачку первого по правилу «своё забираем сразу» — ровно то, от чего захват и
// защищает. Пока воркер один, ошибка не видна.
func TestSecondWorkerOfSameProcessDoesNotSteal(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	ids := seedObjects(t, m, "twoworkers", 2)
	w1, pm := archiver(t, m, ids[0])
	w1.WorkerID = "host-a/mover-1"
	w1.ClaimTTL = 300 * time.Millisecond
	pm.block = make(chan struct{})

	done := make(chan error, 1)
	go func() {
		_, err := w1.ArchivePack(ctx, 1, itemsFor(ids), nil)
		done <- err
	}()

	var sealed uuid.UUID
	for i := 0; i < 200; i++ {
		if err := m.Pool.QueryRow(ctx,
			`SELECT pack_id FROM objects WHERE id = $1`, ids[0]).Scan(&sealed); err == nil && sealed != uuid.Nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if sealed == uuid.Nil {
		t.Fatal("пачка не запечатана")
	}

	// Тот же процесс, другой слот — и другой слот не «свой».
	w2, _ := archiver(t, m, ids[0])
	w2.WorkerID = "host-a/mover-2"
	got, err := w2.Meta.ClaimSealedPack(ctx, w2.WorkerID, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if got != uuid.Nil {
		t.Errorf("воркер %s забрал пачку %s у воркера %s — идентичность захвата должна быть на воркера",
			w2.WorkerID, got, w1.WorkerID)
	}

	close(pm.block)
	if err := <-done; err != nil {
		t.Fatalf("ArchivePack: %v", err)
	}
}

// EEXIST — не успех и не отказ: пачка блокируется, и её больше не пересылают.
//
// Дубля нет (oid пачки не меняется), но из exists не следует, что копия цела:
// incomplete после обрыва посреди записи выглядит так же. Без блокировки
// продолжение через срок захвата переслало бы пачку целиком (гигабайты по сети),
// получило бы тот же exists — и по кругу, бесконечно.
func TestExistsBlocksPackAndStopsResending(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	ids := seedObjects(t, m, "exists", 2)
	a, pm := archiver(t, m, ids[0])
	a.ClaimTTL = 100 * time.Millisecond
	pm.code = mover.CodeExists
	// stat не отвечает — разобрать итог нельзя, пачка блокируется
	pm.statCode = mover.CodeIO

	if _, err := a.ArchivePack(ctx, 1, itemsFor(ids), nil); err == nil {
		t.Fatal("exists не должен считаться успехом")
	}
	if pm.puts != 1 {
		t.Errorf("put вызван %d раз за первую попытку, ждали 1", pm.puts)
	}

	var reason string
	if err := m.Pool.QueryRow(ctx,
		`SELECT blocked_reason FROM packs ORDER BY created_at DESC LIMIT 1`).Scan(&reason); err != nil {
		t.Fatal(err)
	}
	if reason == "" {
		t.Error("пачка не заблокирована — продолжение перешлёт её по кругу")
	}

	// Участники не уехали в pending: пачка не отменена, а не разобрана.
	var st string
	if err := m.Pool.QueryRow(ctx, `SELECT state::text FROM objects WHERE id = $1`, ids[0]).Scan(&st); err != nil {
		t.Fatal(err)
	}
	if st != "archiving" {
		t.Errorf("объект в %s, ждали archiving", st)
	}

	// Срок захвата заведомо короткий: проверяем не «прямо сейчас», а «после его
	// истечения» — именно тогда продолжение попробовало бы снова, и именно это
	// отличает «остановили» от «замедлили пересылкой гигабайтов». Без блокировки
	// пачка стала бы доступной чужим ровно здесь.
	time.Sleep(2 * a.ClaimTTL)

	// Поиск продолжения не берёт заблокированную — и mover не получает второй
	// put.
	got, err := m.ClaimSealedPack(ctx, "host-a/mover-9", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if got != uuid.Nil {
		t.Errorf("продолжение взяло заблокированную пачку %s", got)
	}
	a2, pm2 := archiver(t, m, ids[0])
	a2.WorkerID = "host-a/mover-9"
	if _, err := a2.ResumePacks(ctx, a2.WorkerID, time.Minute,
		func(context.Context, []uuid.UUID) ([]Item, error) { return itemsFor(ids), nil }, nil); err != nil {
		t.Logf("ResumePacks: %v", err)
	}
	if pm2.puts != 0 {
		t.Errorf("заблокированная пачка ушла в mover %d раз — блокировка не работает", pm2.puts)
	}

	// Разблокировка возвращает пачку в оборот (её ведёт разбор: stat или человек).
	var packID uuid.UUID
	if err := m.Pool.QueryRow(ctx, `SELECT id FROM packs ORDER BY created_at DESC LIMIT 1`).Scan(&packID); err != nil {
		t.Fatal(err)
	}
	if err := m.UnblockPack(ctx, packID); err != nil {
		t.Fatalf("разблокировать: %v", err)
	}
	got, err = m.ClaimSealedPack(ctx, "host-a/mover-9", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if got != packID {
		t.Errorf("после разблокировки продолжение получила %s, ждали %s", got, packID)
	}
}

// Неполная копия на ленте — тот же класс, что и «уже есть»: нельзя принять и
// нельзя молча повторить. mover теперь различает «целая» (ok) и «неполная»
// (incomplete) сам, планировщик обязан разобрать второй код.
func TestIncompleteBlocksPack(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	ids := seedObjects(t, m, "incomplete", 2)
	a, pm := archiver(t, m, ids[0])
	a.ClaimTTL = 100 * time.Millisecond
	pm.code = mover.CodeIncomplete
	pm.statCode = mover.CodeIO // разобрать нельзя: блокировка

	if _, err := a.ArchivePack(ctx, 1, itemsFor(ids), nil); err == nil {
		t.Fatal("incomplete не должен считаться успехом")
	}
	var reason string
	if err := m.Pool.QueryRow(ctx,
		`SELECT blocked_reason FROM packs ORDER BY created_at DESC LIMIT 1`).Scan(&reason); err != nil {
		t.Fatal(err)
	}
	if reason == "" {
		t.Error("пачка с неполной копией не заблокирована")
	}
	// В pending участники не уехали: на ленте лежит неполный объект, и
	// пересылка той же пачки под тем же oid даст тот же incomplete.
	var st string
	if err := m.Pool.QueryRow(ctx, `SELECT state::text FROM objects WHERE id = $1`, ids[0]).Scan(&st); err != nil {
		t.Fatal(err)
	}
	if st != "archiving" {
		t.Errorf("объект в %s, ждали archiving", st)
	}
}

// --- С5/С6: несколько копий из одного буфера ---------------------------------

func objectState(t *testing.T, m *meta.Meta, ctx context.Context, id uuid.UUID) string {
	t.Helper()
	var st string
	if err := m.Pool.QueryRow(ctx, `SELECT state::text FROM objects WHERE id = $1`, id).Scan(&st); err != nil {
		t.Fatal(err)
	}
	return st
}

func copySpecsDone(t *testing.T, m *meta.Meta, ctx context.Context, packID uuid.UUID) []string {
	t.Helper()
	rows, err := m.Pool.Query(ctx,
		`SELECT spec FROM archive_copies WHERE pack_id = $1 AND state = 'done' ORDER BY spec`, packID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	return out
}

func copyOids(t *testing.T, m *meta.Meta, ctx context.Context, packID uuid.UUID) []string {
	t.Helper()
	rows, err := m.Pool.Query(ctx,
		`SELECT phobos_oid FROM archive_copies WHERE pack_id = $1 AND state = 'done' ORDER BY spec`, packID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	return out
}

func packIDOf(t *testing.T, m *meta.Meta, ctx context.Context) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	if err := m.Pool.QueryRow(ctx, `SELECT id FROM packs ORDER BY created_at DESC LIMIT 1`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

// С5: пачка 'archived' только когда записаны все копии. Первый put держит буфер,
// вторая копия пишется из него же (from_op), буфер освобождается после всех.
func TestPackArchivedOnlyAfterAllCopies(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	ids := seedObjects(t, m, "copies", 2)
	a, pm := archiver(t, m, ids[0])
	a.Spec = "side:left"
	a.Specs = []string{"side:left", "side:right"}
	a.SpecTags = map[string][]string{"side:left": {"side:left"}, "side:right": {"side:right"}}

	plan, err := a.ArchivePack(ctx, 1, itemsFor(ids), nil)
	if err != nil {
		t.Fatalf("архивация: %v", err)
	}
	_ = plan
	if pm.holds != 1 {
		t.Errorf("put с hold: %d, ждали 1 — первый put обязан держать буфер", pm.holds)
	}
	packID := packIDOf(t, m, ctx)
	if got := copySpecsDone(t, m, ctx, packID); len(got) != 2 {
		t.Errorf("готовых копий: %v, ждали две", got)
	}
	// У каждой копии свой объект Phobos. Общий oid на обе — тихая потеря:
	// вторая запись на тот же oid дала бы EEXIST, mover разобрал бы его как
	// «целая копия», и в БД появилась бы «готовая» копия без записи на ленту.
	oids := copyOids(t, m, ctx, packID)
	if len(oids) != 2 || oids[0] == oids[1] {
		t.Errorf("oid копий: %v — ждали два разных", oids)
	}
	if oids[1] == packID.String() {
		t.Error("копия записана под oid пачки")
	}
	if pm.lastFromOp != packID {
		t.Errorf("копия писалась не из буфера пачки: from_op=%s", pm.lastFromOp)
	}
	if strings.Join(pm.lastTags, ",") != "side:right" {
		t.Errorf("теги копии: %v — копии не разведены тегами", pm.lastTags)
	}
	for _, id := range ids {
		if st := objectState(t, m, ctx, id); st != "archived" {
			t.Errorf("объект в %s, ждали archived", st)
		}
	}
	if pm.releases != 1 {
		t.Errorf("буфер освобождён %d раз, ждали 1 — иначе он висит до result_ttl", pm.releases)
	}
}

// С6: сбой второй копии не откатывает первую и не роняет пачку. Объекты ждут в
// 'archiving', недостающая копия видна как недостающая — её догоняют точечно.
func TestSecondCopyFailureKeepsFirst(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	ids := seedObjects(t, m, "partial", 2)
	a, pm := archiver(t, m, ids[0])
	a.Spec = "side:left"
	a.Specs = []string{"side:left", "side:right"}
	pm.copyCode = mover.CodeNoSpace

	if _, err := a.ArchivePack(ctx, 1, itemsFor(ids), nil); err != nil {
		t.Fatalf("сбой копии не должен считаться отказом пачки: %v", err)
	}
	packID := packIDOf(t, m, ctx)
	if got := copySpecsDone(t, m, ctx, packID); len(got) != 1 || got[0] != "side:left" {
		t.Errorf("готовые копии: %v, ждали [side:left]", got)
	}
	for _, id := range ids {
		if st := objectState(t, m, ctx, id); st != "archiving" {
			t.Errorf("объект в %s: при неполном наборе копий архивировать нельзя", st)
		}
	}
	missing, err := m.MissingCopySpecs(ctx, packID, []string{"side:left", "side:right"})
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 1 || missing[0] != "side:right" {
		t.Errorf("недостающие копии: %v, ждали [side:right]", missing)
	}
	if pm.releases != 0 {
		t.Error("буфер освобождён при незаписанных копиях — догонять будет нечего")
	}

}

// Под oid пачки лежит целый объект другого размера. «Есть объект с нашим oid»
// — ещё не «записана наша пачка»: без сверки размера мигратор объявил бы
// archived поверх чужих байт.
func TestStatSizeMismatchBlocksPack(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	ids := seedObjects(t, m, "statsize", 2)
	a, pm := archiver(t, m, ids[0])
	pm.code = mover.CodeExists
	pm.statCode = mover.CodeOK
	pm.statSize = 1 // заведомо не размер пачки

	if _, err := a.ArchivePack(ctx, 1, itemsFor(ids), nil); err == nil {
		t.Fatal("несовпадение размера не должно считаться успехом")
	}
	if reason := packBlockedReason(t, m, ctx); reason == "" {
		t.Error("пачка не заблокирована при чужом размере объекта")
	}
	if pm.deleteCalls != 0 {
		t.Errorf("delete вызван %d раз: чужой объект удалять нельзя", pm.deleteCalls)
	}
}

// --- #101: разбор «уже есть» по stat ----------------------------------------

func packBlockedReason(t *testing.T, m *meta.Meta, ctx context.Context) string {
	t.Helper()
	var reason *string
	if err := m.Pool.QueryRow(ctx,
		`SELECT blocked_reason FROM packs ORDER BY created_at DESC LIMIT 1`).Scan(&reason); err != nil {
		t.Fatal(err)
	}
	if reason == nil {
		return ""
	}
	return *reason
}

// Целая копия на ленте — успех без пересылки: гнать гигабайты ради той же
// картинки незачем.
func TestStatCompleteIsAcceptedWithoutResend(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	ids := seedObjects(t, m, "statok", 2)
	a, pm := archiver(t, m, ids[0])
	pm.code = mover.CodeExists
	pm.statCode = mover.CodeOK
	pm.statVersion = 7
	pm.statExtents = []string{"TST-MIG"}

	if _, err := a.ArchivePack(ctx, 1, itemsFor(ids), nil); err != nil {
		t.Fatalf("целая копия должна приниматься: %v", err)
	}
	if pm.puts != 1 {
		t.Errorf("put вызван %d раз: целую копию не надо пересылать", pm.puts)
	}
	if pm.deleteCalls != 0 {
		t.Errorf("delete вызван %d раз: целое не удаляют", pm.deleteCalls)
	}
	if reason := packBlockedReason(t, m, ctx); reason != "" {
		t.Errorf("пачка заблокирована: %s", reason)
	}
	var st string
	if err := m.Pool.QueryRow(ctx, `SELECT state::text FROM objects WHERE id = $1`, ids[0]).Scan(&st); err != nil {
		t.Fatal(err)
	}
	if st != "archived" {
		t.Errorf("объект в %s, ждали archived", st)
	}
}

// Неполную копию надо убрать и отправить один раз.
func TestStatIncompleteDeletesAndResends(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	ids := seedObjects(t, m, "statinc", 2)
	a, pm := archiver(t, m, ids[0])
	pm.code = mover.CodeIncomplete
	pm.statCode = mover.CodeIO // разобрать нельзя: блокировка
	pm.statCode = mover.CodeIncomplete

	if _, err := a.ArchivePack(ctx, 1, itemsFor(ids), nil); err != nil {
		t.Fatalf("после удаления и повтора пачка должна пройти: %v", err)
	}
	if pm.deleteCalls != 1 {
		t.Errorf("delete вызван %d раз, ждали 1", pm.deleteCalls)
	}
	if pm.puts != 2 {
		t.Errorf("put вызван %d раз, ждали 2 (исходный и повтор)", pm.puts)
	}
	if reason := packBlockedReason(t, m, ctx); reason != "" {
		t.Errorf("пачка заблокирована после удачного повтора: %s", reason)
	}
}

// Объекта нет — просто отправляем, удалять нечего.
func TestStatNotFoundResendsWithoutDelete(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	ids := seedObjects(t, m, "statnf", 2)
	a, pm := archiver(t, m, ids[0])
	pm.code = mover.CodeExists
	pm.statCode = mover.CodeNotFound

	if _, err := a.ArchivePack(ctx, 1, itemsFor(ids), nil); err != nil {
		t.Fatalf("повтор должен пройти: %v", err)
	}
	if pm.deleteCalls != 0 {
		t.Errorf("delete вызван %d раз при not_found", pm.deleteCalls)
	}
	if pm.puts != 2 {
		t.Errorf("put вызван %d раз, ждали 2", pm.puts)
	}
}

// Если неполную копию не удалить, пачка блокируется: пересылка поверх неё дала
// бы тот же incomplete, и цикл был бы бесконечным.
func TestStatIncompleteUndeletableBlocksPack(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	ids := seedObjects(t, m, "statlock", 2)
	a, pm := archiver(t, m, ids[0])
	pm.code = mover.CodeIncomplete
	pm.statCode = mover.CodeIO // разобрать нельзя: блокировка
	pm.statCode = mover.CodeIncomplete
	pm.deleteCode = mover.CodeBusy

	if _, err := a.ArchivePack(ctx, 1, itemsFor(ids), nil); err == nil {
		t.Fatal("не удалённая неполная копия не должна считаться успехом")
	}
	if pm.puts != 1 {
		t.Errorf("put вызван %d раз: поверх неполной копии слать нельзя", pm.puts)
	}
	if reason := packBlockedReason(t, m, ctx); reason == "" {
		t.Error("пачка не заблокирована")
	}
}

// TestStatTwoReplicasBlocksReplCount — «объект записан двумя репликами» должен
// быть назван своим именем. Сверка по одному числу дала бы «размер не тот»:
// безопасно, но разбирать стали бы не с того конца.
//
// Пачка при этом остаётся заблокированной, а ArchivePack возвращает ошибку:
// «разобрали и заблокировали» — не успех, и цикл обязан это увидеть.
func TestStatTwoReplicasBlocksReplCount(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	ids := seedObjects(t, m, "tworep", 2)
	a, pm := archiver(t, m, ids[0])
	pm.code = mover.CodeExists
	pm.statCode = mover.CodeOK
	pm.statExtentInfo = []mover.ExtentInfo{
		{Size: 1024, Media: "TST-MIG", State: "sync"},
		{Size: 1024, Media: "TST-MIG-2", State: "sync"},
	}

	if _, err := a.ArchivePack(ctx, 1, itemsFor(ids), nil); err == nil {
		t.Error("пачка с чужим числом реплик не должна считаться заархивированной")
	}
	reason := packBlockedReason(t, m, ctx)
	if reason == "" {
		t.Fatal("пачка не заблокирована: две реплики вместо одной — это не наш объект")
	}
	if !strings.Contains(reason, "repl_count") {
		t.Errorf("причина %q: хотим указание на repl_count, а не «размер не тот»", reason)
	}
}
