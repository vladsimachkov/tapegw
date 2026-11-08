//go:build phobos

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/vladsimachkov/tapegw/internal/mover/movertest"
)

// S6 — сквозной путь на настоящем стенде: PUT → archived → вытеснение landing
// → RestoreObject → GET отдаёт исходные байты → после срока снова
// InvalidObjectState. Критерий плана — «те же SHA-256 на входе и на выходе».
//
// Отличие от S2b (TestVerticalSliceArchivesObject) не в длине пути, а в осях:
//   - второй mover с ролью recaller: восстановление читает ленту тем же
//     протоколом, каким архивация её пишет;
//   - объект больше одного кадра И больше окна кредитов (кадр 64 KiB, окно
//     4×кадра = 256 KiB, объект 5 MiB) — поток идёт кредитованными порциями,
//     а не одним кадром;
//   - один из объектов — multipart: через весь путь проходит таблица частей
//     с солями (проверка участника пачки расшифрованием по object_parts).
//
// Прогон — в контейнере ci-dir (hack/ci-dir-up --test и job e2e-tape в CI).
// Без TAPEGW_PHOBOS_TEST тест пропускается, а не проходит.
func TestFullRestorePath(t *testing.T) {
	if os.Getenv("TAPEGW_PHOBOS_TEST") != "1" {
		t.Skip("нужен настоящий Phobos: TAPEGW_PHOBOS_TEST=1 и стенд ci-dir")
	}
	dir := t.TempDir()

	// drive_a — архивный драйв (migrator), drive_b — recovery-пул (recaller).
	// Клиентский сертификат один на оба mover'а (phobos.mover_client общий),
	// поэтому он обязан нести SAN-URI обеих ролей.
	p := movertest.NewPKIWithDrive(t, "drive_a", "drive_b")

	moverBin := filepath.Join(dir, "tapegw-mover")
	build(t, "go", "build", "-tags", "phobos", "-o", moverBin, "../tapegw-mover")
	gwBin := filepath.Join(dir, "tapegw")
	build(t, "go", "build", "-o", gwBin, ".")

	// --- два mover'а: migrator и recaller ---
	// Сертификат сервера у каждого свой: mover сверяет SAN-URI сертификата с
	// настроенным drive (checkDriveIdentity), чужой SAN — отказ на старте.
	startMover := func(drive, role, cert, kkey string) string {
		addr := freeAddr(t)
		cfg := filepath.Join(dir, "mover-"+role+".yaml")
		writeFile(t, cfg, fmt.Sprintf(`phobos:
  family: dir
  library: dirlib
  layout: raid1
  layout_params: {repl_count: "1"}
  mover:
    listen: %q
    drive: %q
    role: %s
    tls: {cert: %q, key: %q, ca: %q}
    staging_dir: %q
    buffer_bytes: 16MiB
    chunk_size: 64KiB
    inactivity_timeout: 1m
    op_timeout: 5m
    result_ttl: 1h
    op_ttl: 1h
    expire_interval: 1m
`, addr, drive, role, cert, kkey, p.CA, filepath.Join(dir, "staging-"+role)))
		startProc(t, moverBin, "-config", cfg)
		waitTCP(t, addr, 30*time.Second)
		return addr
	}
	migratorAddr := startMover(p.Drive, "migrator", p.ServerCert, p.ServerKey)
	recallerAddr := startMover(p.OtherDrive, "recaller", p.OtherDriveCert, p.OtherDriveKey)

	// --- Postgres: DSN стенда ci-dir; схема своя, чтобы не делить её с S2b ---
	dsn := fmt.Sprintf(
		"postgres://phobos:phobos@127.0.0.1/phobos?search_path=tapegw_ci_%d_s6&sslmode=disable",
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
	cacheDir := filepath.Join(dir, "cache")
	gwCfg := filepath.Join(dir, "tapegw.yaml")
	writeFile(t, gwCfg, fmt.Sprintf(`root: {access: s6er, secret: s6pass}
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
    %s: {addr: %q, node: ci, library: dirlib, role: recaller}
  mover_client: {cert: %q, key: %q, ca: %q}
migrator:
  batch_max_wait: 1s
  claim_ttl: 30s
  shutdown_grace: 60s
recaller:
  batch_max_wait: 1s
  claim_ttl: 30s
  shutdown_grace: 60s
janitor:
  interval: 1s
metrics: {listen: %q}
`, kek, dsn, s3Addr, filepath.Join(dir, "landing"), cacheDir,
		p.Drive, migratorAddr, p.OtherDrive, recallerAddr,
		p.BothRolesCert, p.BothRolesKey, p.CA, metricsAddr))
	gw := startProc(t, gwBin, "-config", gwCfg)
	waitTCP(t, s3Addr, 60*time.Second) // миграции на старте — не мгновенны

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("подключение к БД стенда: %v", err)
	}
	defer pool.Close()

	// --- шаг стенда: регистрация носителей (до Ф1.5) ---
	standRegisterMedia(t, ctx, pool)

	// --- шаг стенда: клиентский тенант и учётка ---
	//
	// Restore — клиентская операция: restores.tenant_id связан FK с tenants, а
	// у root-объектов клиентского тенанта нет (вставка в очередь падает на
	// 23503). Тот же порядок, что в e2e-стенде CI: строка tenants + учётка
	// через tapegw-iamctl, чтобы секрет заворачивался тем же кольцом KEK.
	// Роль userplus: бакет без identity-политик может создавать только она
	// (versitygw access-control), обычному user нужна политика бакета.
	if _, err := pool.Exec(ctx,
		`INSERT INTO tenants (id, name, status, pool_id)
		 VALUES (2, 's6-client', 'active', 1) ON CONFLICT (id) DO NOTHING`); err != nil {
		t.Fatal(err)
	}
	iamctl := filepath.Join(dir, "tapegw-iamctl")
	build(t, "go", "build", "-o", iamctl, "../tapegw-iamctl")
	run := exec.Command(iamctl, "-config", gwCfg, "-cmd", "create",
		"-access", "s6user", "-secret", "s6secret", "-role", "userplus", "-tenant", "2")
	run.Stdout, run.Stderr = os.Stderr, os.Stderr
	if err := run.Run(); err != nil {
		t.Fatalf("создание учётки стенда: %v", err)
	}

	cli := s3.New(s3.Options{
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("s6user", "s6secret", ""),
		BaseEndpoint: aws.String("http://" + s3Addr),
		UsePathStyle: true,
	})
	waitOK(t, "CreateBucket", func() error {
		_, err := cli.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("s6path")})
		return err
	})

	// --- PUT: одиночный объект больше кадра и окна кредитов + multipart ---
	single := make([]byte, 5<<20)
	if _, err := rand.Read(single); err != nil {
		t.Fatal(err)
	}
	waitOK(t, "PutObject", func() error {
		_, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String("s6path"), Key: aws.String("big.bin"),
			Body: bytes.NewReader(single), ContentLength: aws.Int64(int64(len(single))),
		})
		return err
	})

	// multipart: 5 MiB + 1 MiB (первая часть не меньше 5 MiB, как требует S3);
	// именно она несёт таблицу частей с солями через весь путь.
	part1 := make([]byte, 5<<20)
	part2 := make([]byte, 1<<20)
	if _, err := rand.Read(part1); err != nil {
		t.Fatal(err)
	}
	if _, err := rand.Read(part2); err != nil {
		t.Fatal(err)
	}
	mp, err := cli.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String("s6path"), Key: aws.String("multi.bin"),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	var parts []types.CompletedPart
	for i, body := range [][]byte{part1, part2} {
		num := int32(i + 1)
		up, err := cli.UploadPart(ctx, &s3.UploadPartInput{
			Bucket: aws.String("s6path"), Key: aws.String("multi.bin"),
			UploadId: mp.UploadId, PartNumber: &num,
			Body: bytes.NewReader(body), ContentLength: aws.Int64(int64(len(body))),
		})
		if err != nil {
			t.Fatalf("UploadPart %d: %v", num, err)
		}
		parts = append(parts, types.CompletedPart{PartNumber: &num, ETag: up.ETag})
	}
	if _, err := cli.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String("s6path"), Key: aws.String("multi.bin"),
		UploadId: mp.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	}); err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}
	multi := append(append([]byte(nil), part1...), part2...)

	// --- воркер сам доводит оба объекта до archived ---
	for _, k := range []string{"big.bin", "multi.bin"} {
		waitObjState(t, ctx, pool, k, "archived", 3*time.Minute)
	}

	// --- вытеснение landing (landing_min_retention=2s, janitor interval=1s) ---
	for _, k := range []string{"big.bin", "multi.bin"} {
		var objID uuid.UUID
		if err := pool.QueryRow(ctx, `SELECT id FROM objects WHERE key = $1`, k).Scan(&objID); err != nil {
			t.Fatal(err)
		}
		waitFileGone(t, filepath.Join(dir, "landing", objID.String()[:2], objID.String()), time.Minute)
	}
	// Вытеснены — GET отвечает InvalidObjectState, а не ENOENT и не 500.
	for _, k := range []string{"big.bin", "multi.bin"} {
		expectS3Code(t, func() error {
			_, err := cli.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("s6path"), Key: aws.String(k)})
			return err
		}, "InvalidObjectState")
	}

	// --- RestoreObject: очередь restores, recaller читает пачку в cache ---
	for _, k := range []string{"big.bin", "multi.bin"} {
		if _, err := cli.RestoreObject(ctx, &s3.RestoreObjectInput{
			Bucket: aws.String("s6path"), Key: aws.String(k),
			RestoreRequest: &types.RestoreRequest{
				Days:                 aws.Int32(1),
				GlacierJobParameters: &types.GlacierJobParameters{Tier: types.TierExpedited},
			},
		}); err != nil {
			t.Fatalf("RestoreObject %s: %v", k, err)
		}
	}
	for _, k := range []string{"big.bin", "multi.bin"} {
		waitRestoreState(t, ctx, pool, k, "restored", 3*time.Minute)
	}

	// Заголовок обещает восстановление до срока, а не «ongoing».
	head, err := cli.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String("s6path"), Key: aws.String("big.bin")})
	if err != nil {
		t.Fatal(err)
	}
	if head.Restore == nil || !strings.Contains(*head.Restore, `ongoing-request="false"`) {
		t.Errorf("x-amz-restore=%v, ждали ongoing-request=\"false\"", head.Restore)
	}

	// --- GET отдаёт ровно те же байты, что легли (критерий S6) ---
	for _, tc := range []struct {
		key  string
		want []byte
	}{{"big.bin", single}, {"multi.bin", multi}} {
		got := getS3Bytes(t, cli, ctx, tc.key)
		wantSum := sha256.Sum256(tc.want)
		gotSum := sha256.Sum256(got)
		if !bytes.Equal(got, tc.want) {
			t.Errorf("GET %s: %d байт, суммы %x != %x (на входе и на выходе должны быть одни)",
				tc.key, len(got), gotSum, wantSum)
		}
	}

	// --- срок restore истёк: janitor снимает строку и файл, GET снова отказывает ---
	//
	// Срок подставляется в БД, а не дожидается: days=1 — сутки, тест не имеет
	// права ждать сутки. Механизм «истекло → вытеснено» проверяется тестом
	// janitor'а на живой БД; здесь важна связка «после вытеснения GET снова
	// отвечает InvalidObjectState».
	if _, err := pool.Exec(ctx,
		`UPDATE objects SET online_until = now() - interval '1 second'
		 WHERE key IN ('big.bin', 'multi.bin')`); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"big.bin", "multi.bin"} {
		waitNoRow(t, ctx, pool,
			`SELECT count(*) FROM restores r JOIN objects o ON o.id = r.object_id
			 WHERE o.key = $1`, k, time.Minute)
		var objID uuid.UUID
		if err := pool.QueryRow(ctx, `SELECT id FROM objects WHERE key = $1`, k).Scan(&objID); err != nil {
			t.Fatal(err)
		}
		waitFileGone(t, filepath.Join(cacheDir, objID.String()[:2], objID.String()), time.Minute)
	}
	for _, k := range []string{"big.bin", "multi.bin"} {
		expectS3Code(t, func() error {
			_, err := cli.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("s6path"), Key: aws.String(k)})
			return err
		}, "InvalidObjectState")
	}

	// --- остановка по сигналу ---
	if err := gw.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	waitExit(t, gw, 90*time.Second)
}

// standRegisterMedia — ЯВНЫЙ ШАГ СТЕНДА: регистрация dir-носителей в реестре
// tapegw сразу после готовности шлюза, до тестового сценария.
//
// Так, а не вставкой внутри теста, потому что команда Ф1.5 («добавление ленты
// одной операцией tapegw») ещё не написана, а init.sh контейнера отрабатывает
// до появления схемы tapegw (миграции применяются на старте шлюза). Без
// строки в media копия остаётся в 'writing' (так и задумано), и объект не
// доходит до archived. Метки — пути, которые Phobos кладёт в extents для
// dir-устройств. С Ф1.5 этот шаг заменится на вызов команды.
func standRegisterMedia(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	for _, label := range []string{"/var/lib/phobos/dir1", "/var/lib/phobos/dir2"} {
		if _, err := pool.Exec(ctx,
			`INSERT INTO media (label, lifecycle, pool_id, generation, medium_type, free_bytes_cached)
			  VALUES ($1, 'active', 1, 'L8', 'L8', 100000000)
			  ON CONFLICT (label) DO NOTHING`, label); err != nil {
			t.Fatalf("регистрация носителя %s: %v", label, err)
		}
	}
}

// waitObjState ждёт, пока объект дойдёт до ожидаемого состояния; 'failed'/'error'
// обрывают ожидание сразу с причиной из очереди — ждать 90 секунд ради
// заведомо мёртвого объекта незачем.
func waitObjState(t *testing.T, ctx context.Context, pool *pgxpool.Pool, key, want string, to time.Duration) {
	t.Helper()
	deadline := time.Now().Add(to)
	for {
		var st string
		err := pool.QueryRow(ctx, `SELECT state FROM objects WHERE key = $1`, key).Scan(&st)
		if err == nil && st == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("объект %s за %v не дошёл до %s: state=%s err=%v", key, to, want, st, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// waitRestoreState ждёт состояния строки очереди restore; 'failed' обрывает
// ожидание с last_error — иначе тест ждёт три минуты ради отказа, который
// уже записан в БД.
func waitRestoreState(t *testing.T, ctx context.Context, pool *pgxpool.Pool, key, want string, to time.Duration) {
	t.Helper()
	deadline := time.Now().Add(to)
	for {
		var st string
		var lastErr string
		err := pool.QueryRow(ctx,
			`SELECT r.state::text, COALESCE(r.last_error, '')
			   FROM restores r JOIN objects o ON o.id = r.object_id
			  WHERE o.key = $1`, key).Scan(&st, &lastErr)
		if err == nil && st == want {
			return
		}
		if err == nil && st == "failed" {
			t.Fatalf("restore %s ушёл в failed: %s", key, lastErr)
		}
		if time.Now().After(deadline) {
			t.Fatalf("restore %s за %v не стал %s: state=%s err=%v", key, to, want, st, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// waitNoRow ждёт, пока запрос перестанет возвращать строки (вытеснение строки
// restore janitor'ом).
func waitNoRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string, arg any, to time.Duration) {
	t.Helper()
	deadline := time.Now().Add(to)
	for {
		var n int
		if err := pool.QueryRow(ctx, query, arg).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("за %v запрос %q всё ещё вернул %d строк", to, query, n)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func waitFileGone(t *testing.T, path string, to time.Duration) {
	t.Helper()
	deadline := time.Now().Add(to)
	for {
		_, err := os.Stat(path)
		if os.IsNotExist(err) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("файл %s не исчез за %v: err=%v", path, to, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func getS3Bytes(t *testing.T, cli *s3.Client, ctx context.Context, key string) []byte {
	t.Helper()
	out, err := cli.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("s6path"), Key: aws.String(key)})
	if err != nil {
		t.Fatalf("GET %s: %v", key, err)
	}
	defer out.Body.Close()
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(out.Body); err != nil {
		t.Fatalf("читать GET %s: %v", key, err)
	}
	return buf.Bytes()
}

// expectS3Code проверяет код S3-ошибки (та же форма, что в e2e).
func expectS3Code(t *testing.T, call func() error, code string) {
	t.Helper()
	err := call()
	if err == nil {
		t.Fatalf("запрос прошёл, ждали %s", code)
	}
	var apiErr interface{ ErrorCode() string }
	if !errors.As(err, &apiErr) || apiErr.ErrorCode() != code {
		t.Errorf("ошибка %v, ждали %s", err, code)
	}
}
