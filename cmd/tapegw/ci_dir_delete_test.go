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

// M6 «deleter» на настоящем Phobos: удалённый через S3 объект действительно
// снимается с ленты — purge проходит отдельной операцией протокола под ролью
// deleter, строка уходит в 'deleted' только после подтверждения mover'а, а
// DSS больше не знает объект пачки.
//
// fake-хранилище (worker-тест) проверяет логику переходов; здесь проверяется
// ровно то, чего fake не даёт: настоящий phobos_delete целого объекта и
// отсутствие объекта в DSS после него.
func TestDeleterRemovesFromPhobos(t *testing.T) {
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
		"postgres://phobos:phobos@127.0.0.1/phobos?search_path=tapegw_ci_%d_delete&sslmode=disable",
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
	writeFile(t, gwCfg, fmt.Sprintf(`root: {access: deler, secret: delpass}
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
  # Роль deleter отдельна от scheduler: purge целого объекта не должен уметь
  # планировщик архивации (M6).
  deleter_client: {cert: %q, key: %q, ca: %q}
migrator:
  batch_max_wait: 1s
  claim_ttl: 30s
  shutdown_grace: 60s
deleter:
  interval: 1s
  claim_ttl: 30s
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

	// Оба носителя DSS, а не один: Phobos сам выбирает, куда положить копию, а
	// копия на ленте вне реестра tapegw для migrator'а — «записана вне реестра»,
	// и пачка зависает в «ждёт догонки». Стенд ci-dir знает dir1 и dir2, поэтому
	// в реестр обязаны попасть оба.
	standRegisterMedia(t, gwBin, gwCfg, "/var/lib/phobos/dir1", "/var/lib/phobos/dir2")

	body := make([]byte, 3<<20)
	if _, err := rand.Read(body); err != nil {
		t.Fatal(err)
	}
	cli := s3.New(s3.Options{
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("deler", "delpass", ""),
		BaseEndpoint: aws.String("http://" + s3Addr),
		UsePathStyle: true,
	})
	waitOK(t, "CreateBucket", func() error {
		_, err := cli.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("delb")})
		return err
	})
	waitOK(t, "PutObject", func() error {
		_, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String("delb"), Key: aws.String("gone.bin"),
			Body: bytes.NewReader(body), ContentLength: aws.Int64(int64(len(body))),
		})
		return err
	})

	// Ждём archived и запоминаем uuid пачки — у основной копии oid Phobos равен
	// uuid пачки (migrator/archive.go: sendPack кладёт объект под id пачки).
	var packID, objID uuid.UUID
	deadline := time.Now().Add(2 * time.Minute)
	for {
		var st string
		err := pool.QueryRow(ctx,
			`SELECT id, state, pack_id FROM objects WHERE key = 'gone.bin'`).Scan(&objID, &st, &packID)
		if err == nil && st == "archived" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("объект не дошёл до archived за 2 мин: state=%s err=%v", st, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
	// До удаления объект обязан жить в namespace Phobos — иначе проверка
	// «исчез» ничего не стоит (зелёный тест, который ничего не проверил).
	if !standObjectPresent(ctx, t, packID) {
		t.Fatalf("пачка %s до удаления не живёт в Phobos — проверять нечего", packID)
	}
	landingFile := filepath.Join(dir, "landing", objID.String()[:2], objID.String())

	// Удаление через S3: строка уходит в 'deleting', deleter снимает ленту.
	waitOK(t, "DeleteObject", func() error {
		_, err := cli.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String("delb"), Key: aws.String("gone.bin"),
		})
		return err
	})

	deadline = time.Now().Add(2 * time.Minute)
	for {
		var st, packSt string
		err := pool.QueryRow(ctx,
			`SELECT o.state::text, p.state::text FROM objects o JOIN packs p ON p.id = o.pack_id
			  WHERE o.id = $1`, objID).Scan(&st, &packSt)
		if err == nil && st == "deleted" && packSt == "retired" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("удаление не завершилось за 2 мин: object=%s pack=%s err=%v", st, packSt, err)
		}
		time.Sleep(500 * time.Millisecond)
	}

	// Зеркало в DSS: объект пачки снят из namespace — это и есть «сначала
	// лента»: строка 'deleted' стоит только там, где Phobos подтвердил снятие.
	if standObjectPresent(ctx, t, packID) {
		t.Fatalf("объект %s после purge всё ещё жив в Phobos", packID)
	}

	// Файл в landing снят этим же прогоном: janitor в стенде выключен, значит
	// снял его deleter, а не кто-то другой.
	if _, err := os.Stat(landingFile); !os.IsNotExist(err) {
		t.Fatalf("файл landing после удаления на месте: err=%v", err)
	}

	// Остановка по сигналу — чистый выход (deleter обязан отпустить shutdown).
	if err := gw.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	waitExit(t, gw, 90*time.Second)
}

// standObjectPresent — живёт ли объект в namespace Phobos.
//
// Проверяется именно getmd, а не extent list: `phobos delete` снимает объект из
// namespace и кладёт его в deprecated-таблицу, а экстенты у deprecated-объекта
// остаются, пока их не уберёт очистка (замерено на ci-dir: после успешного
// delete extent list печатает строку со state sync, getmd отвечает ENOENT).
// tapegw Stat устроен так же — существование это getmd, экстенты только для
// размера и полноты (internal/store/phobos/store.go: Stat).
func standObjectPresent(ctx context.Context, t *testing.T, oid uuid.UUID) bool {
	t.Helper()
	var out, errb bytes.Buffer
	cmd := exec.CommandContext(ctx, "phobos", "getmd", oid.String())
	cmd.Stdout, cmd.Stderr = &out, &errb
	err := cmd.Run()
	t.Logf("getmd %s: err=%v stdout=%q stderr=%q", oid, err, out.String(), errb.String())
	return err == nil
}
