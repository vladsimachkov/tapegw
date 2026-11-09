package worker_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/s3response"

	"github.com/vladsimachkov/tapegw/internal/backend"
	"github.com/vladsimachkov/tapegw/internal/config"
	"github.com/vladsimachkov/tapegw/internal/iam"
	"github.com/vladsimachkov/tapegw/internal/keys"
	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/mover/movertest"
	"github.com/vladsimachkov/tapegw/internal/spool"
	"github.com/vladsimachkov/tapegw/internal/testdb"
	"github.com/vladsimachkov/tapegw/internal/worker"
)

// Критерий S2: воркеры в tapegw — запуск, остановка по сигналу, загрузчик из
// landing, клиент mover из phobos.movers. Тесты гоняют тот же код, что
// включает main: PlanMigrators → StartMigrators → Wait, а mover — настоящий
// сервер movertest (протокол, mTLS, реестр), хранилище — fake без cgo.

func TestPlanMigrators(t *testing.T) {
	cfg := config.Default()
	cfg.Phobos.Library = "lib-a"
	cfg.Phobos.Movers = map[string]config.Mover{
		"drive_a":   {Addr: "a:1", Node: "n1", Library: "lib-a", Role: "migrator"},
		"drive_rec": {Addr: "r:1", Node: "n1", Library: "lib-a", Role: "recaller"},
		"drive_b":   {Addr: "b:1", Node: "n2", Library: "lib-b", Role: "migrator"},
	}
	got, err := worker.PlanMigrators(cfg)
	if err != nil {
		t.Fatalf("PlanMigrators: %v", err)
	}
	if len(got) != 1 || got[0].Drive != "drive_a" {
		t.Fatalf("выбрано %v, ожидался только drive_a: фильтр обязан быть по роли И библиотеке", got)
	}

	// Включён и некому: отказ на старте, а не молчаливый простой очереди.
	cfg.Phobos.Movers = map[string]config.Mover{
		"drive_rec": {Addr: "r:1", Node: "n1", Library: "lib-a", Role: "recaller"},
	}
	if _, err := worker.PlanMigrators(cfg); err == nil {
		t.Fatal("migrator включён без mover'а роли migrator — ожидался отказ")
	} else if !strings.Contains(err.Error(), "migrator") {
		t.Errorf("отказ без объяснения: %v", err)
	}
}

// harness — tapegw-стек поверх тестовой БД и fake-mover'а: backend пишет
// настоящие landing-файлы (шифрование, манифест, file_bytes), воркер их
// архивирует через movertest.
type harness struct {
	be   *backend.TapeGW
	m    *meta.Meta
	env  *movertest.Env
	cfg  *config.Config
	log  *slog.Logger
	ctx  context.Context
	land *spool.Spool
	// cache — буфер восстановленных копий: janitor-тесты проверяют вытеснение
	// истёкших restore, recaller-тесты — публикацию в него.
	cache *spool.Spool
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	m := testdb.Connect(t)
	testdb.Clean(t, m)

	// movertest по умолчанию кладёт extents «TST-MOVER»: копия становится done
	// только когда носитель есть в реестре БД (MediaIDByLabel).
	if _, err := m.Pool.Exec(context.Background(),
		`INSERT INTO media (label, lifecycle, pool_id, generation, medium_type, free_bytes_cached)
		 VALUES ('TST-MOVER', 'active', 1, 'L8', 'L8', 100000)
		 ON CONFLICT (label) DO NOTHING`); err != nil {
		t.Fatal(err)
	}

	env := movertest.New(t, 64<<20)

	cfg := config.Default()
	cfg.Phobos.Family = "dir"
	cfg.Phobos.Library = "lib-test"
	cfg.Phobos.Movers = map[string]config.Mover{
		env.PKI.Drive: {Addr: env.Addr(), Node: "testnode", Library: "lib-test", Role: "migrator"},
	}
	cfg.Phobos.MoverClient = config.MoverTLS{
		Cert: env.PKI.ClientCert, Key: env.PKI.ClientKey, CA: env.PKI.CA,
	}
	cfg.Migrator.BatchMaxWait = 50 * time.Millisecond
	cfg.Migrator.ClaimTTL = 5 * time.Second
	cfg.Migrator.ShutdownGrace = 10 * time.Second

	cache, err := spool.Open(filepath.Join(t.TempDir(), "cache"))
	if err != nil {
		t.Fatalf("spool.Open(cache): %v", err)
	}
	land, err := spool.Open(filepath.Join(t.TempDir(), "landing"))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte("worker test kek"))
	ring, err := iam.NewKeyRing(1, map[int][]byte{1: sum[:]})
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	return &harness{
		be: backend.New(m, land, cache, cfg, keys.New(m, ring)),
		m:  m, env: env, cfg: cfg, log: log,
		ctx: context.Background(), land: land, cache: cache,
	}
}

// putObject создаёт бакет и объект через бэкенд: файл обязан быть настоящим
// (шифротекст + манифест + file_bytes), иначе проверялась бы не боевая дорожка.
func (h *harness) putObject(t *testing.T, bucket, key string, body []byte) uuid.UUID {
	t.Helper()
	if err := h.be.CreateBucket(h.adminCtx(),
		&s3.CreateBucketInput{Bucket: aws.String(bucket)}, []byte(`{}`)); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if _, err := h.be.PutObject(h.adminCtx(), s3response.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
		Body: bytes.NewReader(body), ContentLength: aws.Int64(int64(len(body))),
	}); err != nil {
		t.Fatalf("PUT %s/%s: %v", bucket, key, err)
	}
	obj, err := h.m.GetObject(h.ctx, meta.NewTenant(1), bucket, key)
	if err != nil {
		t.Fatalf("строка объекта: %v", err)
	}
	return obj.ID
}

func (h *harness) adminCtx() context.Context {
	acct := auth.Account{Access: "tester", Role: auth.RoleUser, ProjectID: 1}
	//lint:ignore SA1029 ключ диктует versitygw
	ctx := context.WithValue(h.ctx, "account", acct)
	//lint:ignore SA1029 ключ диктует versitygw
	return context.WithValue(ctx, "bucket-owner", acct)
}

// waitFor ждёт условия в БД — воркер работает асинхронно.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("условие не наступило за 30 с: %s", what)
}

func objectState(t *testing.T, m *meta.Meta, id uuid.UUID) string {
	t.Helper()
	var st string
	if err := m.Pool.QueryRow(context.Background(),
		`SELECT state FROM objects WHERE id = $1`, id).Scan(&st); err != nil {
		t.Fatal(err)
	}
	return st
}

func packState(t *testing.T, m *meta.Meta, id uuid.UUID) string {
	t.Helper()
	var st string
	err := m.Pool.QueryRow(context.Background(),
		`SELECT state::text FROM packs WHERE id = $1`, id).Scan(&st)
	if errors.Is(err, sql.ErrNoRows) || errors.Is(err, pgx.ErrNoRows) {
		return ""
	}
	if err != nil {
		t.Fatal(err)
	}
	return st
}

// Обычный путь: PUT → воркер сам доводит объект до archived, пачка archived,
// на «ленте» (fake) один объект.
func TestMigratorWorkerArchivesObject(t *testing.T) {
	h := newHarness(t)
	id := h.putObject(t, "wb", "small.bin", []byte("воркер архивирует сам, без ручного цикла"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	targets, err := worker.PlanMigrators(h.cfg)
	if err != nil {
		t.Fatal(err)
	}
	g, err := worker.StartMigrators(ctx, worker.MigratorDeps{
		Cfg: h.cfg, Meta: h.m, Spool: h.land, Log: h.log,
	}, targets)
	if err != nil {
		t.Fatalf("StartMigrators: %v", err)
	}

	waitFor(t, "объект заархивирован", func() bool { return objectState(t, h.m, id) == "archived" })

	cancel()
	if !g.Wait(h.cfg.Migrator.ShutdownGrace) {
		t.Error("воркер не завершился по сигналу за grace")
	}
	if h.env.Store.Len() != 1 {
		t.Errorf("на ленте %d объектов, ждали 1", h.env.Store.Len())
	}
}

// Остановка посреди отправки (критерий ревью S2): сигнал не обрывает отправку.
// Пачка запечатана, запись в хранилище стоит на ловушке; отмена контекста не
// должна выпустить воркера из Wait, пока Put не отпустят. После отпускания —
// archived, а не «половина пачки».
func TestStopDoesNotAbortSend(t *testing.T) {
	h := newHarness(t)
	block := make(chan struct{})
	h.env.Store.SetPutBlock(block)
	id := h.putObject(t, "wb", "mid.bin", []byte("остановка не обрывает отправку пачки"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	targets, _ := worker.PlanMigrators(h.cfg)
	g, err := worker.StartMigrators(ctx, worker.MigratorDeps{
		Cfg: h.cfg, Meta: h.m, Spool: h.land, Log: h.log,
	}, targets)
	if err != nil {
		t.Fatal(err)
	}

	// Ловушка стоит на записи в хранилище: пачка успевает запечататься.
	var packID uuid.UUID
	waitFor(t, "пачка запечатана", func() bool {
		err := h.m.Pool.QueryRow(context.Background(),
			`SELECT pack_id FROM objects WHERE id = $1`, id).Scan(&packID)
		return err == nil && packState(t, h.m, packID) == "sealed"
	})

	cancel()
	// Grace заведомо короче ловушки: Wait обязан вернуться false — воркер всё
	// ещё в отправке, и сигнал его оттуда не вырывает. Это и есть критерий
	// «остановка не обрывает отправку посреди»: пачка на время ожидания стоит
	// запечатанной, а не «половиной на ленте».
	if g.Wait(1500 * time.Millisecond) {
		close(block)
		t.Fatal("воркер отпустил отправку раньше времени: сигнал прервал put")
	}
	if st := packState(t, h.m, packID); st != "sealed" {
		close(block)
		t.Fatalf("во время ожидания пачка в %s, ждали sealed", st)
	}

	close(block)
	if !g.Wait(15 * time.Second) {
		t.Fatal("после отпускания ловушки воркер не завершился")
	}
	waitFor(t, "пачка дошла до archived после остановки сигнала", func() bool {
		return packState(t, h.m, packID) == "archived" && objectState(t, h.m, id) == "archived"
	})
}

// Политика копий пула (Ф1.1): две копии в конфиге — воркер пишет обе (первую
// put'ом, вторую локальной передачей из удержанного буфера), теги каждой
// уходят в Phobos как заданы — именно так копии разводятся по носителям.
// archived при этом только когда записаны обе: «архивный = все копии».
func TestMigratorWorkerPolicyCopies(t *testing.T) {
	h := newHarness(t)
	// Имя пула в конфиге обязано совпасть с pools.name в БД: в сиде 'shared'.
	h.cfg.Pools = map[string]config.PoolWrite{"shared": {Copies: []config.CopySpec{
		{Name: "left", Tags: []string{"side:left"}},
		{Name: "right", Tags: []string{"side:right"}},
	}}}
	// Fake выбирает ленту по тегам записи, как Phobos: без этого обе копии
	// «легли» бы на TST-MOVER, и страж разных носителей сработал бы здесь.
	if _, err := h.m.Pool.Exec(context.Background(),
		`INSERT INTO media (label, lifecycle, pool_id, generation, medium_type, free_bytes_cached)
		 VALUES ('TST-LEFT', 'active', 1, 'L8', 'L8', 100000),
		        ('TST-RIGHT', 'active', 1, 'L8', 'L8', 100000)`); err != nil {
		t.Fatal(err)
	}
	h.env.Store.SetMediaByTag(map[string]string{"side:left": "TST-LEFT", "side:right": "TST-RIGHT"})
	id := h.putObject(t, "wb", "two.bin", []byte("две копии по политике пула"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	targets, err := worker.PlanMigrators(h.cfg)
	if err != nil {
		t.Fatal(err)
	}
	g, err := worker.StartMigrators(ctx, worker.MigratorDeps{
		Cfg: h.cfg, Meta: h.m, Spool: h.land, Log: h.log,
	}, targets)
	if err != nil {
		t.Fatalf("StartMigrators: %v", err)
	}

	waitFor(t, "объект заархивирован", func() bool { return objectState(t, h.m, id) == "archived" })
	cancel()
	if !g.Wait(h.cfg.Migrator.ShutdownGrace) {
		t.Error("воркер не завершился по сигналу за grace")
	}

	cctx := context.Background()
	var packID uuid.UUID
	if err := h.m.Pool.QueryRow(cctx,
		`SELECT pack_id FROM objects WHERE id = $1`, id).Scan(&packID); err != nil {
		t.Fatal(err)
	}

	// В реестре — обе копии, каждая со своей спецификацией, своим объектом
	// Phobos (у копии свой oid, ADR-0022) и на СВОЁМ носителе: «две копии»
	// на одной ленте архивацией не считаются.
	rows, err := h.m.Pool.Query(cctx,
		`SELECT c.spec, c.state::text, m.label
		   FROM archive_copies c JOIN media m ON m.id = c.medium_id
		  WHERE c.pack_id = $1 ORDER BY c.spec`, packID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var specs, states, labels []string
	for rows.Next() {
		var s, st, lb string
		if err := rows.Scan(&s, &st, &lb); err != nil {
			t.Fatal(err)
		}
		specs = append(specs, s)
		states = append(states, st)
		labels = append(labels, lb)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if strings.Join(specs, ",") != "left,right" {
		t.Fatalf("копии в реестре: %v, ожидали left,right", specs)
	}
	for i, st := range states {
		if st != "done" {
			t.Errorf("копия %s в %s, ждали done", specs[i], st)
		}
	}
	if strings.Join(labels, ",") != "TST-LEFT,TST-RIGHT" {
		t.Errorf("копии на носителях %v, ожидали TST-LEFT,TST-RIGHT — разведение по тегам не сработало", labels)
	}

	// Теги каждой копии ушли в Phobos своими: тег пула добавлен к тегу стороны
	// (ADR-0013), а не заменён им. Копия «right» — отдельный объект Phobos с
	// детерминированным oid от пачки и имени копии.
	rightOp := uuid.NewSHA1(packID, []byte("right"))
	if got := strings.Join(h.env.Store.Tags(packID), ","); got != "pool:shared,side:left" {
		t.Errorf("теги первой копии %q, ожидали pool:shared,side:left", got)
	}
	if got := strings.Join(h.env.Store.Tags(rightOp), ","); got != "pool:shared,side:right" {
		t.Errorf("теги второй копии %q, ожидали pool:shared,side:right", got)
	}
	if got := h.env.Store.Attrs(rightOp)["tapegw.copy"]; got != "right" {
		t.Errorf("атрибут tapegw.copy второй копии %q", got)
	}
	// Два объекта на «ленте», а не один: без второй копии тест бы проходил и
	// молчал бы о ней.
	if h.env.Store.Len() != 2 {
		t.Errorf("на ленте %d объектов, ждали 2 (пачка + вторая копия)", h.env.Store.Len())
	}
}

// Опечатка в имени пула в политике отсекается на старте: иначе «политика есть»
// молча означала бы «пишем одну копию» — ровно то, от чего политика защищает.
func TestStartMigratorsRejectsUnknownPool(t *testing.T) {
	h := newHarness(t)
	h.cfg.Pools = map[string]config.PoolWrite{"shared-d": {Copies: []config.CopySpec{
		{Name: "left", Tags: []string{"side:left"}},
	}}}
	_, err := worker.StartMigrators(context.Background(), worker.MigratorDeps{
		Cfg: h.cfg, Meta: h.m, Spool: h.land, Log: h.log,
	}, nil)
	if err == nil {
		t.Fatal("политика для несуществующего пула принята")
	}
	if !strings.Contains(err.Error(), "shared-d") {
		t.Errorf("в ошибке нет имени пула: %v", err)
	}
}

// Боевой путь стража «копии на разных носителях»: если ленты подобрались так,
// что обе копии легли на одну (теги подвели), пачка не становится архивной —
// она блокируется с причиной, а объекты ждут на диске. Тест не заводит
// разведения по тегам: fake отвечает обеим копиям один TST-MOVER.
func TestMigratorWorkerSameMediumBlocks(t *testing.T) {
	h := newHarness(t)
	h.cfg.Pools = map[string]config.PoolWrite{"shared": {Copies: []config.CopySpec{
		{Name: "left", Tags: []string{"side:left"}},
		{Name: "right", Tags: []string{"side:right"}},
	}}}
	id := h.putObject(t, "wb", "coll.bin", []byte("обе копии легли на одну ленту"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	targets, err := worker.PlanMigrators(h.cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := worker.StartMigrators(ctx, worker.MigratorDeps{
		Cfg: h.cfg, Meta: h.m, Spool: h.land, Log: h.log,
	}, targets); err != nil {
		t.Fatalf("StartMigrators: %v", err)
	}

	var packID uuid.UUID
	waitFor(t, "пачка заблокирована с причиной", func() bool {
		if err := h.m.Pool.QueryRow(context.Background(),
			`SELECT pack_id FROM objects WHERE id = $1`, id).Scan(&packID); err != nil {
			return false
		}
		var blocked string
		if err := h.m.Pool.QueryRow(context.Background(),
			`SELECT COALESCE(blocked_reason, '') FROM packs WHERE id = $1`, packID).Scan(&blocked); err != nil {
			return false
		}
		return blocked != ""
	})
	cancel()

	if st := objectState(t, h.m, id); st != "archiving" {
		t.Errorf("объект в %s, ждали archiving: пачка с одной лентой архивной не считается", st)
	}
}
