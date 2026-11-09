//go:build phobos

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
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

// Ф1.1 «две копии в боевом пути» на настоящем Phobos: политика пула из конфига
// (две копии с тегами сторон) — и объект сам доходит до archived с ДВУМЯ
// копиями на РАЗНЫХ носителях: левая только на dir1, правая только на dir2.
//
// Это проверка именно разведения по тегам: Phobos выбирает носитель по тегам
// записи, и если бы migrator передал тег пула обеим копиям (или перепутал их),
// обе легли бы на одну ленту — а «две копии» на одной ленте не значит ничего.
// fake-хранилище (worker-тест) теги записывает, но не выбирает: выбор ленты
// здесь и только здесь.
func TestTwoCopiesPolicy(t *testing.T) {
	if os.Getenv("TAPEGW_PHOBOS_TEST") != "1" {
		t.Skip("нужен настоящий Phobos: TAPEGW_PHOBOS_TEST=1 и стенд ci-dir")
	}
	dir := t.TempDir()
	p := movertest.NewPKIWithDrive(t, "drive_a", "drive_b")

	moverBin := filepath.Join(dir, "tapegw-mover")
	build(t, "go", "build", "-tags", "phobos", "-o", moverBin, "../tapegw-mover")
	gwBin := filepath.Join(dir, "tapegw")
	build(t, "go", "build", "-o", gwBin, ".")

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

	dsn := fmt.Sprintf(
		"postgres://phobos:phobos@127.0.0.1/phobos?search_path=tapegw_ci_%d_copies&sslmode=disable",
		os.Getpid())

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
	writeFile(t, gwCfg, fmt.Sprintf(`root: {access: copier, secret: copypass}
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
  # deleter включён по умолчанию, а purge целого объекта — отдельная роль
  # сертификата (M6), на scheduler-клиенте её нет.
  deleter_client: {cert: %q, key: %q, ca: %q}
# Политика копий (Ф1.1): имя пула — из БД (сид заводит 'shared'), теги сторон
# — из конфига. Тег пула migrator добавляет к каждой копии сам (ADR-0013).
pools:
  shared:
    copies:
      - name: left
        tags: [side:left]
      - name: right
        tags: [side:right]
migrator:
  batch_max_wait: 1s
  claim_ttl: 30s
  shutdown_grace: 60s
recaller:
  enabled: false
janitor:
  enabled: false
metrics: {listen: %q}
`, kek, dsn, s3Addr,
		filepath.Join(dir, "landing"), filepath.Join(dir, "cache"),
		p.Drive, moverAddr, p.ClientCert, p.ClientKey, p.CA,
		p.DeleterCert, p.DeleterKey, p.CA, metricsAddr))
	gw := startProc(t, gwBin, "-config", gwCfg)
	waitTCP(t, s3Addr, 60*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("подключение к БД стенда: %v", err)
	}
	defer pool.Close()

	// Носители — с сторонами: тег side:<значение> кладёт штатная команда Ф1.5,
	// ровно так же ленту заводят на установке двухсторонней библиотеки.
	standRegisterMediaSide(t, gwBin, gwCfg, "/var/lib/phobos/dir1", "left")
	standRegisterMediaSide(t, gwBin, gwCfg, "/var/lib/phobos/dir2", "right")

	body := make([]byte, 5<<20)
	if _, err := rand.Read(body); err != nil {
		t.Fatal(err)
	}
	cli := s3.New(s3.Options{
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("copier", "copypass", ""),
		BaseEndpoint: aws.String("http://" + s3Addr),
		UsePathStyle: true,
	})
	waitOK(t, "CreateBucket", func() error {
		_, err := cli.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("copies")})
		return err
	})
	waitOK(t, "PutObject", func() error {
		_, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String("copies"), Key: aws.String("two.bin"),
			Body: bytes.NewReader(body), ContentLength: aws.Int64(int64(len(body))),
		})
		return err
	})

	// archived только когда записаны обе копии («архивный = все копии»).
	var packID uuid.UUID
	deadline := time.Now().Add(2 * time.Minute)
	for {
		var st string
		err := pool.QueryRow(ctx,
			`SELECT state, pack_id FROM objects WHERE key = 'two.bin'`).Scan(&st, &packID)
		if err == nil && st == "archived" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("объект не дошёл до archived за 2 мин: state=%s err=%v", st, err)
		}
		time.Sleep(500 * time.Millisecond)
	}

	// Две копии, каждая на своей ленте: левая — только dir1, правая — только
	// dir2. Если бы обе копии получили одинаковые теги, Phobos положил бы их на
	// одну метку и этот запрос показал бы одинаковые label.
	rows, err := pool.Query(ctx,
		`SELECT c.spec, c.state::text, m.label
		   FROM archive_copies c JOIN media m ON m.id = c.medium_id
		  WHERE c.pack_id = $1 ORDER BY c.spec`, packID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := map[string]string{}
	for rows.Next() {
		var spec, state, label string
		if err := rows.Scan(&spec, &state, &label); err != nil {
			t.Fatal(err)
		}
		if state != "done" {
			t.Errorf("копия %s в %s, ждали done", spec, state)
		}
		got[spec] = label
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("копий %d (%v), ждали 2", len(got), got)
	}
	if got["left"] != "/var/lib/phobos/dir1" || got["right"] != "/var/lib/phobos/dir2" {
		t.Fatalf("копии разведены не по сторонам: %v", got)
	}

	// Зеркало в DSS: у копии свой объект Phobos (oid = UUIDv5 от пачки и
	// имени копии), и extent list показывает на его ленте именно нужную метку.
	// Это проверка не только реестра tapegw: Phobos действительно положил
	// вторую копию на dir2, а не записал тег в нашу строку «на веру».
	rightOID := uuid.NewSHA1(packID, []byte("right"))
	out, err := exec.CommandContext(ctx, "phobos", "extent", "list", "-f", "json",
		"-o", "oid,size,media_name,state", rightOID.String()).CombinedOutput()
	if err != nil {
		t.Fatalf("phobos extent list %s: %v: %s", rightOID, err, out)
	}
	t.Logf("копия right: %s", out)
	if !bytes.Contains(out, []byte("dir2")) {
		t.Errorf("копия right в DSS не на dir2: %s", out)
	}

	// Остановка по сигналу — чистый выход.
	if err := gw.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	waitExit(t, gw, 90*time.Second)
}
