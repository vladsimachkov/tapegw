//go:build phobos

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"os"
	"path/filepath"
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
