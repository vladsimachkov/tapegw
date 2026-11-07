package mover_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/mover"
	"github.com/vladsimachkov/tapegw/internal/mover/movertest"
	"github.com/vladsimachkov/tapegw/internal/store"
)

// blockingStore — обёртка, которая останавливает вызов Put внутри: тест
// ловит момент «вызов Phobos начался» и отпускает его вручную. Так проверка
// порядка остановки становится детерминированной, а не гоночной.
type blockingStore struct {
	*movertest.Store
	entered chan struct{}
	release chan struct{}

	mu    sync.Mutex
	order []string
}

func (b *blockingStore) Put(ctx context.Context, batch []store.PutItem, opt store.PutOptions) []store.Result {
	b.mu.Lock()
	b.order = append(b.order, "put_enter")
	b.mu.Unlock()
	b.entered <- struct{}{}
	<-b.release
	res := b.Store.Put(ctx, batch, opt)
	b.mu.Lock()
	b.order = append(b.order, "put_done")
	b.mu.Unlock()
	return res
}

func (b *blockingStore) Close() error {
	b.mu.Lock()
	b.order = append(b.order, "store_close")
	b.mu.Unlock()
	return b.Store.Close()
}

func (b *blockingStore) seen() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.order...)
}

// TestCloseWaitsForInFlightCall — штатная остановка (SIGTERM) не должна
// прерывать начатый вызов Phobos: app.shutdown вызывает srv.Close(), а затем
// st.Close() (phobos_fini), и если бы Close возвращался посреди операции, fini
// встал бы перед незавершённым phobos_put — на ленте осталась бы неполная
// копия, а процесс «корректно остановился».
//
// Здесь проверяется механизм: Close ждёт обработчик с активным вызовом, вызов
// доходит до конца, store.Close происходит строго после put_done. Порядок
// shutdown (сначала дождаться сервер, потом закрывать хранилище) — тот же,
// что в cmd/tapegw-mover; на настоящем Phobos его проверяет
// TestMoverStoppedMidPutRecovers в ci-dir.
func TestCloseWaitsForInFlightCall(t *testing.T) {
	pki := movertest.NewPKI(t)
	staging := filepath.Join(t.TempDir(), "staging")
	reg, err := mover.NewRegistry(staging, 1<<20, time.Hour, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	bs := &blockingStore{
		Store:   movertest.NewStore(),
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	srvTLS, err := mover.ServerTLS(pki.ServerCert, pki.ServerKey, pki.CA, mover.ClientScheduler)
	if err != nil {
		t.Fatal(err)
	}
	srv, err := mover.NewServer(bs, reg, mover.ServerConfig{
		Addr: "127.0.0.1:0", TLS: srvTLS, Role: mover.RoleMigrator,
		Version: "test", PhobosVer: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve()

	cl, err := mover.Dial(t.Context(), srv.Addr(), mustTLS(t, pki))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	body := bytes.Repeat([]byte("тело"), 256)
	sum := sha256.Sum256(body)
	op := uuid.New()
	go func() {
		_, _ = cl.Put(mover.PutIn{Op: op, ID: op, Size: int64(len(body)),
			SHA256: hex.EncodeToString(sum[:]), Body: bytes.NewReader(body)})
	}()

	select {
	case <-bs.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("вызов Put так и не начался")
	}

	closed := make(chan error, 1)
	go func() { closed <- srv.Close() }()
	select {
	case <-closed:
		t.Fatal("Close вернулся, пока вызов Phobos в разгаре — fini встанет перед незавершённым put")
	case <-time.After(200 * time.Millisecond):
		// так и задумано: ждём
	}

	close(bs.release)
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close не дождался операции после отпускания")
	}
	// Порядок из app.shutdown: хранилище закрываем только после сервера.
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}

	order := bs.seen()
	if len(order) != 3 || order[0] != "put_enter" || order[1] != "put_done" || order[2] != "store_close" {
		t.Errorf("порядок %v, ждали put_enter → put_done → store_close", order)
	}
	if bs.Store.Len() != 1 {
		t.Errorf("вызов был прерван: в хранилище %d объектов, ждали 1", bs.Store.Len())
	}
	rec, ok := reg.Get(op)
	if !ok || rec.State != mover.StateDone {
		t.Errorf("операция не дошла до done: %+v", rec)
	}
}

func mustTLS(t *testing.T, pki movertest.PKI) *tls.Config {
	t.Helper()
	cfg, err := mover.ClientTLS(pki.ClientCert, pki.ClientKey, pki.CA, pki.Drive)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}
