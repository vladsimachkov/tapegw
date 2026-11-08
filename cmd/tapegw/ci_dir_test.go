//go:build phobos

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/vladsimachkov/tapegw/internal/mover/movertest"
)

// Вертикальный срез S2b (критерий ревью «тест реального пути»): настоящий
// tapegw-процесс, настоящий tapegw-mover, настоящий Phobos (family dir) и
// настоящий Postgres — объект крупнее одного кадра mover'а доходит до
// archived сам, без ручного цикла в тесте.
//
// Юнит-тесты internal/worker проверяют цикл на fake-хранилище; здесь
// проверяется то, чего fake не покрывает никогда: сборка main (миграции на
// старте, воркеры из phobos.movers, остановка по сигналу), протокол mover'а с
// живым сервером и запись в DSS Phobos.
//
// Прогон — в контейнере ci-dir (hack/ci-dir-up --test). Без TAPEGW_PHOBOS_TEST
// тест пропускается, а не проходит: без библиотеки он ничего не утверждает.
func TestVerticalSliceArchivesObject(t *testing.T) {
	if os.Getenv("TAPEGW_PHOBOS_TEST") != "1" {
		t.Skip("нужен настоящий Phobos: TAPEGW_PHOBOS_TEST=1 и стенд ci-dir")
	}
	dir := t.TempDir()

	// Имена драйвов — ключи phobos.movers, а они ограничены [a-z0-9_].
	p := movertest.NewPKIWithDrive(t, "drive_a", "drive_b")

	// --- бинари: проверяется артефакт сборки, а не пересказ внутри теста ---
	moverBin := filepath.Join(dir, "tapegw-mover")
	build(t, "go", "build", "-tags", "phobos", "-o", moverBin, "../tapegw-mover")
	gwBin := filepath.Join(dir, "tapegw")
	build(t, "go", "build", "-o", gwBin, ".")

	// --- mover ---
	moverAddr := freeAddr(t)
	moverCfg := filepath.Join(dir, "mover.yaml")
	writeFile(t, moverCfg, fmt.Sprintf(`phobos:
  family: dir
  library: dirlib
  layout: raid1
  layout_params: {repl_count: "1"}
  mover:
    listen: %q
    drive: %q
    role: migrator
    tls: {cert: %q, key: %q, ca: %q}
    staging_dir: %q
    buffer_bytes: 16MiB
    chunk_size: 64KiB
    inactivity_timeout: 1m
    op_timeout: 5m
    result_ttl: 1h
    op_ttl: 1h
    expire_interval: 1m
`, moverAddr, p.Drive, p.ServerCert, p.ServerKey, p.CA, filepath.Join(dir, "staging")))
	startProc(t, moverBin, "-config", moverCfg)
	waitTCP(t, moverAddr, 30*time.Second)

	// --- Postgres: DSN стенда ci-dir (роль и БД заводит init.sh) ---
	dsn := fmt.Sprintf(
		"postgres://phobos:phobos@127.0.0.1/phobos?search_path=tapegw_ci_%d&sslmode=disable",
		os.Getpid())

	// --- tapegw ---
	kek := filepath.Join(dir, "kek.json")
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	kb, err := json.Marshal(map[string]string{"1": base64.StdEncoding.EncodeToString(key)})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(kek, kb, 0o600); err != nil {
		t.Fatal(err)
	}

	s3Addr := freeAddr(t)
	metricsAddr := freeAddr(t)
	gwCfg := filepath.Join(dir, "tapegw.yaml")
	writeFile(t, gwCfg, fmt.Sprintf(`root: {access: slicer, secret: slicepass}
iam: {kek_file: %q, kek_version: 1}
db: {dsn: %q}
s3: {listen: [%q], region: us-east-1}
spool:
  landing: %q
  cache: %q
  landing_min_retention: 2s
phobos:
  family: dir
  library: dirlib
  movers:
    %s: {addr: %q, node: ci, library: dirlib, role: migrator}
  mover_client: {cert: %q, key: %q, ca: %q}
migrator:
  batch_max_wait: 1s
  claim_ttl: 30s
  shutdown_grace: 60s
# S2b ничего не восстанавливает, а с S5 recaller по умолчанию включён и его
# старт-гейт (PlanRecallers) требует mover роли recaller — здесь его нет.
# Сквозной restore-путь со вторым mover'ом проверяет TestFullRestorePath.
recaller:
  enabled: false
janitor:
  interval: 1s
metrics: {listen: %q}
`, kek, dsn, s3Addr,
		filepath.Join(dir, "landing"), filepath.Join(dir, "cache"),
		p.Drive, moverAddr, p.ClientCert, p.ClientKey, p.CA, metricsAddr))
	gw := startProc(t, gwBin, "-config", gwCfg)
	waitTCP(t, s3Addr, 60*time.Second) // миграции на старте — не мгновенны

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("подключение к БД стенда: %v", err)
	}
	defer pool.Close()

	// Носители регистрирует явный шаг стенда (см. standRegisterMedia) штатной
	// командой Ф1.5 — тот же путь оператора, каким лента заводится на установке.
	standRegisterMedia(t, gwBin, gwCfg, "/var/lib/phobos/dir1", "/var/lib/phobos/dir2")

	// --- PUT объекта крупнее одного кадра (кадр mover'а — 64 KiB) ---
	body := make([]byte, 5<<20)
	if _, err := rand.Read(body); err != nil {
		t.Fatal(err)
	}
	cli := s3.New(s3.Options{
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("slicer", "slicepass", ""),
		BaseEndpoint: aws.String("http://" + s3Addr),
		UsePathStyle: true,
	})
	waitOK(t, "CreateBucket", func() error {
		_, err := cli.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("slice")})
		return err
	})
	waitOK(t, "PutObject", func() error {
		_, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String("slice"), Key: aws.String("big.bin"),
			Body: bytes.NewReader(body), ContentLength: aws.Int64(int64(len(body))),
		})
		return err
	})

	// --- воркер сам доводит объект до archived ---
	var packID uuid.UUID
	deadline := time.Now().Add(90 * time.Second)
	for {
		var st string
		err := pool.QueryRow(ctx,
			`SELECT state, pack_id FROM objects WHERE key = 'big.bin'`).Scan(&st, &packID)
		if err == nil && st == "archived" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("объект не дошёл до archived за 90 с: state=%s err=%v", st, err)
		}
		time.Sleep(500 * time.Millisecond)
	}

	var packState string
	if err := pool.QueryRow(ctx, `SELECT state::text FROM packs WHERE id = $1`, packID).
		Scan(&packState); err != nil {
		t.Fatal(err)
	}
	if packState != "archived" {
		t.Errorf("пачка в %s, ждали archived", packState)
	}
	var copies int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM archive_copies WHERE pack_id = $1`, packID).Scan(&copies); err != nil {
		t.Fatal(err)
	}
	if copies != 1 {
		t.Errorf("копий %d, ждали 1", copies)
	}

	// --- пачка видна в DSS: тот же CLI, которым пользуется mover ---
	out, err := exec.CommandContext(ctx, "phobos", "extent", "list", "-f", "json",
		"-o", "oid,size,media_name,state", packID.String()).CombinedOutput()
	if err != nil {
		t.Fatalf("phobos extent list %s: %v: %s", packID, err, out)
	}
	t.Logf("extent list: %s", out)

	// --- S3: срок истёк (landing_min_retention=2s) — janitor сам убирает
	// файл из landing, HEAD остаётся GLACIER без x-amz-restore, GET отвечает
	// InvalidObjectState (не ENOENT и не 500) ---
	var objID uuid.UUID
	if err := pool.QueryRow(ctx, `SELECT id FROM objects WHERE key = 'big.bin'`).Scan(&objID); err != nil {
		t.Fatal(err)
	}
	landingFile := filepath.Join(dir, "landing", objID.String()[:2], objID.String())
	waitGone := time.Now().Add(60 * time.Second)
	for {
		_, err := os.Stat(landingFile)
		if os.IsNotExist(err) {
			break
		}
		if time.Now().After(waitGone) {
			t.Fatalf("файл landing не вытеснен за 60 с: err=%v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	head, err := cli.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String("slice"), Key: aws.String("big.bin"),
	})
	if err != nil {
		t.Fatalf("HEAD после вытеснения: %v", err)
	}
	if head.StorageClass != "GLACIER" {
		t.Errorf("storage class %q, ждали GLACIER", head.StorageClass)
	}
	if head.Restore != nil {
		t.Errorf("x-amz-restore=%q, ждали пустой: restore в срезе ещё нет", *head.Restore)
	}
	if _, err := cli.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("slice"), Key: aws.String("big.bin"),
	}); err == nil {
		t.Fatal("GET вытесненного объекта прошёл — ждали InvalidObjectState")
	} else {
		var apiErr interface{ ErrorCode() string }
		if !errors.As(err, &apiErr) || apiErr.ErrorCode() != "InvalidObjectState" {
			t.Errorf("GET вытесненного: %v, ждали InvalidObjectState", err)
		}
	}

	// --- остановка по сигналу: чистый выход, не паника и не вешалка ---
	if err := gw.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	waitExit(t, gw, 90*time.Second)
}

// startProc запускает процесс и гарантирует, что после теста от него ничего
// не останется, даже если тест оборвался раньше своей остановки.
func startProc(t *testing.T, name string, args ...string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("запуск %s: %v", name, err)
	}
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
	})
	return cmd
}

func build(t *testing.T, args ...string) {
	t.Helper()
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("%v: %v", args, err)
	}
}

func writeFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

func waitTCP(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			c.Close()
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("за %v на %s так и не удалось подключиться", timeout, addr)
}

func waitOK(t *testing.T, what string, fn func() error) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		if last = fn(); last == nil {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("%s: %v", what, last)
}

func waitExit(t *testing.T, cmd *exec.Cmd, timeout time.Duration) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("процесс завершился с ошибкой: %v", err)
		}
	case <-time.After(timeout):
		t.Fatalf("процесс не завершился за %v после сигнала", timeout)
	}
}
