//go:build phobos

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/config"
	"github.com/vladsimachkov/tapegw/internal/mover"
	"github.com/vladsimachkov/tapegw/internal/mover/movertest"
)

// Критерий S1: mover стартует, и клиент пишет и читает через него настоящий
// Phobos (family dir). Прогон — в контейнере ci-dir (hack/ci-dir-up --test):
// без настоящей библиотеки тест ничего не утверждает, поэтому без
// TAPEGW_PHOBOS_TEST он пропускается, а не проходит.
//
// Тест поднимает ровно ту же сборку, что и main (newApp), а не её пересказ:
// иначе «бинарь стартует» проверялось бы памятью теста.
func TestMoverWritesAndReadsRealPhobos(t *testing.T) {
	if os.Getenv("TAPEGW_PHOBOS_TEST") != "1" {
		t.Skip("нужен настоящий Phobos: TAPEGW_PHOBOS_TEST=1 и стенд ci-dir")
	}
	p := movertest.NewPKI(t)

	cfg := config.Default()
	cfg.Phobos.Family = "dir"
	cfg.Phobos.Library = "dirlib"
	cfg.Phobos.Layout = "raid1"
	cfg.Phobos.LayoutParams = map[string]string{"repl_count": "1"}
	cfg.Phobos.Mover = config.MoverServer{
		Listen:         "127.0.0.1:0",
		Drive:          p.Drive,
		Role:           "migrator",
		TLS:            config.MoverTLS{Cert: p.ServerCert, Key: p.ServerKey, CA: p.CA},
		StagingDir:     filepath.Join(t.TempDir(), "staging"),
		BufferBytes:    16 << 20,
		ChunkSize:      64 << 10,
		Inactivity:     time.Minute,
		OpTimeout:      5 * time.Minute,
		ResultTTL:      time.Hour,
		OpTTL:          time.Hour,
		ExpireInterval: time.Minute,
	}
	if err := cfg.ValidateMover(); err != nil {
		t.Fatalf("конфиг тестового mover'а невалиден: %v", err)
	}
	if err := checkDriveIdentity(p.ServerCert, p.Drive); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	app, err := newApp(ctx, cfg, log)
	if err != nil {
		t.Fatalf("сборка mover'а: %v", err)
	}
	defer app.close(log)
	go app.srv.Serve()

	cliTLS, err := mover.ClientTLS(p.ClientCert, p.ClientKey, p.CA, p.Drive)
	if err != nil {
		t.Fatal(err)
	}
	// Роль проверяем на рукопожатии: клиент обязан убедиться, что подключился
	// к mover'у своего пула (ADR-0021), а не к «любому свободному».
	c, _, err := mover.DialAs(ctx, app.srv.Addr(), cliTLS, mover.RoleMigrator)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	body := []byte("S1: mover + настоящий Phobos на dir; " +
		"байты должны вернуться без изменений")
	sum := sha256.Sum256(body)
	id := uuid.New()

	res, err := c.Put(mover.PutIn{
		Op: id, ID: id, Size: int64(len(body)),
		SHA256: hex.EncodeToString(sum[:]),
		Body:   bytes.NewReader(body),
	})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if res.Code != mover.CodeOK {
		t.Fatalf("Put: код %s (%s)", res.Code, res.State)
	}

	// Stat идёт через getmd и extent list CLI: он доказывает, что объект
	// действительно в DSS Phobos, а не только в буфере mover'а.
	st, err := c.StatObject(id)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if st.Code != mover.CodeOK {
		t.Fatalf("Stat: код %s, ждали ok — копия не подтверждена как целая", st.Code)
	}

	// get — отдельная операция со своим op: op привязан к паре (size, sha256),
	// и переиспользовать op записи нельзя (op_conflict — правильный ответ).
	var got bytes.Buffer
	gres, err := c.Get(uuid.New(), id, &got)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if gres.Code != mover.CodeOK {
		t.Fatalf("Get: код %s", gres.Code)
	}
	if !bytes.Equal(got.Bytes(), body) {
		t.Fatalf("вернулись другие байты: %d байт, совпадение с исходными — %v",
			got.Len(), bytes.Equal(got.Bytes(), body))
	}
}

// blockingReader читается вечно: клиент передаёт тело, упирается в него и
// висит — операция остаётся в staged с неполным буфером.
type blockingReader struct{}

func (blockingReader) Read([]byte) (int, error) { select {} }

// waitStaged ждёт, пока реестр на диске не зафиксирует принятые байты
// операции: именно они переживут убийство процесса и станут точкой возобновления.
func waitStaged(t *testing.T, staging string, op uuid.UUID) {
	t.Helper()
	meta := filepath.Join(staging, op.String()+".json")
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		b, err := os.ReadFile(meta)
		if err == nil {
			var rec struct {
				Received int64 `json:"received"`
			}
			if json.Unmarshal(b, &rec) == nil && rec.Received > 0 {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("реестр не записал принятые байты — возобновлять нечего")
}

// freeAddr — свободный порт: процесс mover'а стартует по уже известному
// адресу, чтобы клиент подключался к тому же адресу между запусками.
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

// moverBin собирает бинарь из этого же пакета: проверяется артефакт, который
// ставят в продакшн, а не пересказ сборки внутри теста.
func moverBin(t *testing.T, dir string) string {
	t.Helper()
	bin := filepath.Join(dir, "tapegw-mover")
	build := exec.Command("go", "build", "-tags", "phobos", "-o", bin, ".")
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		t.Fatalf("сборка бинаря mover'а: %v", err)
	}
	return bin
}

// moverConfig пишет конфиг процесса под стенд ci-dir.
func moverConfig(t *testing.T, dir, addr, staging, buffer string, p movertest.PKI) string {
	t.Helper()
	cfgPath := filepath.Join(dir, "mover.yaml")
	cfgYAML := fmt.Sprintf(`phobos:
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
    buffer_bytes: %s
    chunk_size: 64KiB
    inactivity_timeout: 2m
    op_timeout: 10m
`, addr, p.Drive, p.ServerCert, p.ServerKey, p.CA, staging, buffer)
	if err := os.WriteFile(cfgPath, []byte(cfgYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	return cfgPath
}

// startMover запускает процесс и ждёт, пока адрес начнёт слушаться.
func startMover(t *testing.T, bin, cfgPath, addr string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(bin, "-config", cfgPath)
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("запуск mover'а: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			c.Close()
			return cmd
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("mover не слушает %s", addr)
	return cmd
}

// waitWriting ждёт, пока реестр на диске не перейдёт в state=writing: это
// окно между «буфер принят и сверен» и «phobos_put вернулся». Сигнал
// остановки шлётся именно в него — иначе тест проверял бы остановку простоя.
func waitWriting(t *testing.T, staging string, op uuid.UUID) {
	t.Helper()
	meta := filepath.Join(staging, op.String()+".json")
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		b, err := os.ReadFile(meta)
		if err == nil {
			var rec struct {
				State string `json:"state"`
			}
			if json.Unmarshal(b, &rec) == nil {
				switch rec.State {
				case "writing":
					return
				case "done", "failed":
					t.Fatal("операция завершилась раньше, чем поймался state=writing — окно слишком узкое для теста")
				}
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("state=writing так и не наступил")
}

// Вторая половина замечания про остановку: после обрыва mover обязан
// подниматься корректно. Процесс убивается по-настоящему (SIGKILL — без
// обработки сигналов и cleanup), посреди put, на том же staging-каталоге,
// и повтор той же операции докачивается с принятого смещения. Реестр
// переживает убийство потому, что persist идёт fsync-ом на каждом кадре.
func TestMoverKilledMidPutRecovers(t *testing.T) {
	if os.Getenv("TAPEGW_PHOBOS_TEST") != "1" {
		t.Skip("нужен настоящий Phobos: TAPEGW_PHOBOS_TEST=1 и стенд ci-dir")
	}
	p := movertest.NewPKI(t)
	dir := t.TempDir()
	staging := filepath.Join(dir, "staging")

	addr := freeAddr(t)
	bin := moverBin(t, dir)
	cfgPath := moverConfig(t, dir, addr, staging, "16MiB", p)
	start := func() *exec.Cmd { return startMover(t, bin, cfgPath, addr) }

	op := uuid.New()
	full := bytes.Repeat([]byte("kill-mid-put "), 100_000) // ~1.3 MiB
	sum := sha256.Sum256(full)
	const partial = 400 << 10

	proc := start()
	cliTLS, err := mover.ClientTLS(p.ClientCert, p.ClientKey, p.CA, p.Drive)
	if err != nil {
		t.Fatal(err)
	}
	c1, err := mover.Dial(context.Background(), addr, cliTLS)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		_, _ = c1.Put(mover.PutIn{Op: op, ID: op, Size: int64(len(full)),
			SHA256: hex.EncodeToString(sum[:]),
			Body:   io.MultiReader(bytes.NewReader(full[:partial]), blockingReader{})})
	}()

	waitStaged(t, staging, op)

	if err := proc.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = proc.Wait()
	// c1 не закрываем: Close ждёт мьютекс, который держит висящий на теле Put,
	// а отмены у клиента нет (ADR-0021). Соединение умерло вместе с процессом.

	proc2 := start()
	c2, _, err := mover.DialAs(context.Background(), addr, cliTLS, mover.RoleMigrator)
	if err != nil {
		t.Fatalf("подключение после обрыва: %v", err)
	}
	defer c2.Close()
	_ = proc2

	// Тот же op, полное тело: сервер обязан ответить resume с принятым
	// смещением, а не переписать с нуля и не отвергнуть операцию.
	res, err := c2.Put(mover.PutIn{Op: op, ID: op, Size: int64(len(full)),
		SHA256: hex.EncodeToString(sum[:]), Body: bytes.NewReader(full)})
	if err != nil {
		t.Fatalf("повторный Put после обрыва: %v", err)
	}
	if res.Code != mover.CodeOK {
		t.Fatalf("повторный Put: код %s (%s)", res.Code, res.State)
	}

	st, err := c2.StatObject(op)
	if err != nil || st.Code != mover.CodeOK {
		t.Fatalf("Stat после возобновления: %v / %v", err, st)
	}
	var got bytes.Buffer
	gres, err := c2.Get(uuid.New(), op, &got)
	if err != nil || gres.Code != mover.CodeOK {
		t.Fatalf("Get после возобновления: %v / %v", err, gres)
	}
	if !bytes.Equal(got.Bytes(), full) {
		t.Fatal("после возобновления на ленте (dir) лежат другие байты")
	}
}

// Третья грань остановки — штатная: SIGTERM посреди настоящего phobos_put.
// Механизм («Close ждёт обработчик с активным вызовом, fini — после»)
// детерминированно проверен TestCloseWaitsForInFlightCall на fake; здесь —
// настоящий Phobos и настоящий процесс: сигнал ловится в окне state=writing,
// процесс выходит нулём только после возврата put, а после рестарта копия
// целая — то есть прерванной записи нет. Если бы phobos_fini встал перед
// незавершённым phobos_put, копия осталась бы неполной или нулевой.
func TestMoverStoppedMidPutRecovers(t *testing.T) {
	if os.Getenv("TAPEGW_PHOBOS_TEST") != "1" {
		t.Skip("нужен настоящий Phobos: TAPEGW_PHOBOS_TEST=1 и стенд ci-dir")
	}
	p := movertest.NewPKI(t)
	dir := t.TempDir()
	staging := filepath.Join(dir, "staging")
	addr := freeAddr(t)
	bin := moverBin(t, dir)
	cfgPath := moverConfig(t, dir, addr, staging, "64MiB", p)

	// 48 MiB: phobos_put на dir идёт заметно дольше интервала опроса реестра,
	// окно writing не должен успевать закрыться между проверками.
	full := bytes.Repeat([]byte("sigterm-mid-put "), 3_000_000)
	sum := sha256.Sum256(full)
	op := uuid.New()

	proc := startMover(t, bin, cfgPath, addr)
	cliTLS, err := mover.ClientTLS(p.ClientCert, p.ClientKey, p.CA, p.Drive)
	if err != nil {
		t.Fatal(err)
	}
	c1, err := mover.Dial(context.Background(), addr, cliTLS)
	if err != nil {
		t.Fatal(err)
	}
	putDone := make(chan error, 1)
	go func() {
		_, err := c1.Put(mover.PutIn{Op: op, ID: op, Size: int64(len(full)),
			SHA256: hex.EncodeToString(sum[:]), Body: bytes.NewReader(full)})
		putDone <- err
	}()

	waitWriting(t, staging, op)
	if err := proc.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	// Выход — ноль: «остановились корректно, дождавшись операции». Ненулевой
	// код означал бы, что остановка споткнулась о собственный cleanup.
	if err := proc.Wait(); err != nil {
		t.Fatalf("mover завершился нештатно после SIGTERM посреди put: %v", err)
	}
	select {
	case <-putDone:
		// код не проверяем: кадр результата мог не успеть уйти до закрытия
		// соединения — это нормальный исход штатной остановки.
	case <-time.After(10 * time.Second):
		t.Fatal("клиентский Put не вернулся после остановки сервера")
	}
	c1.Close()

	// proc2 убирает t.Cleanup из startMover.
	startMover(t, bin, cfgPath, addr)
	c2, _, err := mover.DialAs(context.Background(), addr, cliTLS, mover.RoleMigrator)
	if err != nil {
		t.Fatalf("подключение после остановки: %v", err)
	}
	defer c2.Close()

	st, err := c2.StatObject(op)
	if err != nil || st.Code != mover.CodeOK {
		t.Fatalf("Stat после штатной остановки: %v / %v — копия не целая", err, st)
	}
	var got bytes.Buffer
	gres, err := c2.Get(uuid.New(), op, &got)
	if err != nil || gres.Code != mover.CodeOK {
		t.Fatalf("Get после штатной остановки: %v / %v", err, gres)
	}
	if !bytes.Equal(got.Bytes(), full) {
		t.Fatal("после штатной остановки на dir лежат другие байты")
	}
}
