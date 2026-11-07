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
