package mover_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/mover"
	"github.com/vladsimachkov/tapegw/internal/store"
)

// Тестовая PKI: сертификаты генерируются на лету, потому что проверять нужно
// именно отказ — чужой CA, клиент без сертификата, сертификат mover'а в роли
// клиента. Всё это нельзя проверить одним «правильным» сертификатом.
type testPKI struct {
	dir string
	ca  string

	// Сертификат mover'а: ServerAuth + SAN-URI драйва.
	serverCert, serverKey string
	// Клиентский сертификат tapegw-планировщика: ClientAuth + SAN-URI роли.
	clientCert, clientKey string
	// Клиент restorer'а — другая роль того же CA.
	restorerCert, restorerKey string
	// Сертификат mover'а другого драйва того же CA: клиент обязан отказаться.
	otherDriveCert, otherDriveKey string
	// Тот же CA, но подписанный как серверный: в роли клиента быть не должен.
	moverAsClientCert, moverAsClientKey string
	// SAN-URI роли на месте, ExtKeyUsage отсутствует.
	noExtKeyCert, noExtKeyKey string

	foreignCA      string
	foreignSrvCert string
	foreignSrvKey  string
	foreignCliCert string
	foreignCliKey  string
	drive          string
	otherDrive     string
}

func (p *testPKI) write(name string, b []byte) string {
	path := filepath.Join(p.dir, name)
	if err := os.WriteFile(path, b, 0o600); err != nil {
		panic(err)
	}
	return path
}

func newPKI(t *testing.T) *testPKI {
	t.Helper()
	dir := t.TempDir()
	p := &testPKI{dir: dir, drive: "drive-A", otherDrive: "drive-B"}

	newCA := func(name string) (*x509.Certificate, *ecdsa.PrivateKey, string) {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		tmpl := &x509.Certificate{
			SerialNumber:          big.NewInt(1),
			Subject:               pkix.Name{CommonName: name},
			NotBefore:             time.Now().Add(-time.Hour),
			NotAfter:              time.Now().Add(time.Hour),
			IsCA:                  true,
			KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
			BasicConstraintsValid: true,
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
		if err != nil {
			t.Fatal(err)
		}
		return tmpl, key, p.write(name+".pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	}

	sign := func(ca *x509.Certificate, caKey *ecdsa.PrivateKey, cn string, ext x509.ExtKeyUsage, uris []*url.URL, dns []string) (string, string) {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(time.Now().UnixNano()),
			Subject:      pkix.Name{CommonName: cn},
			NotBefore:    time.Now().Add(-time.Hour),
			NotAfter:     time.Now().Add(time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{ext},
			URIs:         uris,
			DNSNames:     dns,
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		kb, err := x509.MarshalECPrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		certPath := p.write(cn+".crt", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
		keyPath := p.write(cn+".key", pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}))
		return certPath, keyPath
	}

	signNoExt := func(ca *x509.Certificate, caKey *ecdsa.PrivateKey, cn string, uris []*url.URL, dns []string) (string, string) {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(time.Now().UnixNano()),
			Subject:      pkix.Name{CommonName: cn},
			NotBefore:    time.Now().Add(-time.Hour),
			NotAfter:     time.Now().Add(time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			URIs:         uris,
			DNSNames:     dns,
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		kb, err := x509.MarshalECPrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		return p.write(cn+".crt", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
			p.write(cn+".key", pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}))
	}

	ca, caKey, caPath := newCA("ca")
	p.ca = caPath

	p.serverCert, p.serverKey = sign(ca, caKey, "mover-A", x509.ExtKeyUsageServerAuth,
		[]*url.URL{mover.MoverURI(p.drive)}, []string{mover.MoverDNSName})
	p.otherDriveCert, p.otherDriveKey = sign(ca, caKey, "mover-B", x509.ExtKeyUsageServerAuth,
		[]*url.URL{mover.MoverURI(p.otherDrive)}, []string{mover.MoverDNSName})
	// Сертификат mover'а, предъявленный как клиентский: ServerAuth и SAN mover'а.
	p.moverAsClientCert, p.moverAsClientKey = p.serverCert, p.serverKey

	// Правильный SAN-URI роли, но расширение ExtKeyUsage отсутствует вовсе.
	// Go такой сертификат считает годным на любое назначение, поэтому отказ
	// обязан исходить от нас.
	p.noExtKeyCert, p.noExtKeyKey = signNoExt(ca, caKey, "tapegw-noext",
		[]*url.URL{mover.ClientURI(mover.ClientScheduler)}, nil)

	p.clientCert, p.clientKey = sign(ca, caKey, "tapegw-scheduler", x509.ExtKeyUsageClientAuth,
		[]*url.URL{mover.ClientURI(mover.ClientScheduler)}, nil)
	p.restorerCert, p.restorerKey = sign(ca, caKey, "tapegw-restorer", x509.ExtKeyUsageClientAuth,
		[]*url.URL{mover.ClientURI(mover.ClientRestorer)}, nil)

	fca, fcaKey, fcaPath := newCA("foreign-ca")
	p.foreignCA = fcaPath
	p.foreignSrvCert, p.foreignSrvKey = sign(fca, fcaKey, "foreign-mover", x509.ExtKeyUsageServerAuth,
		[]*url.URL{mover.MoverURI(p.drive)}, []string{mover.MoverDNSName})
	p.foreignCliCert, p.foreignCliKey = sign(fca, fcaKey, "foreign-client", x509.ExtKeyUsageClientAuth,
		[]*url.URL{mover.ClientURI(mover.ClientScheduler)}, nil)

	return p
}

// fakeStore — хранилище без cgo с известным числом вызовов: идемпотентность
// проверяется счётчиком, а не содержимым.
type fakeStore struct {
	mu       sync.Mutex
	data     map[[16]byte][]byte
	calls    map[string]int
	locateOK bool

	// exists и incomplete — рычаги для кода EEXIST: «объект уже есть» и
	// «есть, но неполный» (оборванная запись). Без них ветку Stat в mover
	// нельзя вызвать вообще.
	exists     map[uuid.UUID]bool
	incomplete map[uuid.UUID]bool

	// Что реально легло на «ленту»: теги и атрибуты по oid. Тест копий
	// проверяет ими, что копии — разные объекты с разными тегами.
	tags  map[uuid.UUID][]string
	attrs map[uuid.UUID]map[string]string
}

func (f *fakeStore) Tags(id uuid.UUID) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tags[id]
}

func (f *fakeStore) Attrs(id uuid.UUID) map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attrs[id]
}

func (f *fakeStore) forceExists(id uuid.UUID, incomplete bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.init()
	f.exists[id] = true
	if incomplete {
		f.incomplete[id] = true
	}
}

func (f *fakeStore) init() {
	if f.data == nil {
		f.data = map[[16]byte][]byte{}
		f.calls = map[string]int{}
		f.locateOK = true
		f.exists = map[uuid.UUID]bool{}
		f.incomplete = map[uuid.UUID]bool{}
		f.tags = map[uuid.UUID][]string{}
		f.attrs = map[uuid.UUID]map[string]string{}
	}
}

func (f *fakeStore) Put(_ context.Context, batch []store.PutItem, opt store.PutOptions) []store.Result {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.init()
	f.calls["put"]++
	out := make([]store.Result, 0, len(batch))
	for _, it := range batch {
		if f.exists[it.ID] {
			out = append(out, store.Result{ID: it.ID, Err: store.ErrExists})
			continue
		}
		b, err := io.ReadAll(it.Reader)
		if err != nil {
			out = append(out, store.Result{ID: it.ID, Err: err})
			continue
		}
		f.data[it.ID] = b
		f.tags[it.ID] = append([]string(nil), opt.Tags...)
		f.attrs[it.ID] = it.Attrs
		out = append(out, store.Result{ID: it.ID, Version: 1})
	}
	return out
}

func (f *fakeStore) Get(_ context.Context, items []store.GetItem) []store.Result {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.init()
	f.calls["get"]++
	out := make([]store.Result, 0, len(items))
	for _, it := range items {
		b, ok := f.data[it.ID]
		if !ok {
			out = append(out, store.Result{ID: it.ID, Err: store.ErrNotFound})
			continue
		}
		if it.Writer != nil {
			_, _ = it.Writer.Write(b)
		}
		out = append(out, store.Result{ID: it.ID, Version: 1})
	}
	return out
}

func (f *fakeStore) Stat(_ context.Context, id uuid.UUID) (store.Meta, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.init()
	b, ok := f.data[id]
	if !ok {
		return store.Meta{}, store.ErrNotFound
	}
	return store.Meta{ID: id, Size: int64(len(b)), Version: 1, Complete: !f.incomplete[id]}, nil
}

func (f *fakeStore) Delete(_ context.Context, ids []uuid.UUID) []store.Result {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.init()
	f.calls["delete"]++
	out := make([]store.Result, 0, len(ids))
	for _, id := range ids {
		delete(f.data, id)
		out = append(out, store.Result{ID: id})
	}
	return out
}

func (f *fakeStore) Extents(_ context.Context, ids []uuid.UUID) (map[uuid.UUID][]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.init()
	out := make(map[uuid.UUID][]string, len(ids))
	for _, id := range ids {
		if _, ok := f.data[id]; !ok {
			return nil, store.ErrNotFound
		}
		out[id] = []string{"TST001"}
	}
	return out, nil
}

func (f *fakeStore) Locate(_ context.Context, ids []uuid.UUID) ([]store.Location, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.init()
	if !f.locateOK {
		return nil, store.ErrNotSupported
	}
	out := make([]store.Location, 0, len(ids))
	for range ids {
		out = append(out, store.Location{TapeID: "TST001", DriveHint: "drive-A"})
	}
	return out, nil
}

func (f *fakeStore) Close() error { return nil }

func (f *fakeStore) Len() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.init()
	return len(f.data)
}

func (f *fakeStore) Calls(what string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.init()
	return f.calls[what]
}

// env — запущенный mover поверх настоящего TCP с настоящим mTLS. Отдельного
// режима «без шифрования» в пакете нет, поэтому и тест идёт по тому же пути,
// по которому поедут боевые соединения.
type env struct {
	staging string
	st      *fakeStore
	reg     *mover.Registry
	srv     *mover.Server
	pki     *testPKI
	addr    string
}

func startEnv(t *testing.T, bufMax int64) *env { return startEnvWith(t, bufMax, mover.ClientScheduler) }

func startEnvWith(t *testing.T, bufMax int64, allowed ...mover.ClientID) *env {
	return startEnvTimed(t, bufMax, 0, allowed...)
}

// startEnvTimed — env с заданным таймаутом бездействия (0 — значение по
// умолчанию). Нужен тесту «клиент молчит → соединение закрыто».
func startEnvTimed(t *testing.T, bufMax int64, inactivity time.Duration, allowed ...mover.ClientID) *env {
	t.Helper()
	if len(allowed) == 0 {
		allowed = []mover.ClientID{mover.ClientScheduler}
	}
	p := newPKI(t)
	st := &fakeStore{}
	staging := filepath.Join(t.TempDir(), "staging")
	reg, err := mover.NewRegistry(staging, bufMax, 64<<10, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	srvTLS, err := mover.ServerTLS(p.serverCert, p.serverKey, p.ca, allowed...)
	if err != nil {
		t.Fatal(err)
	}
	srv, err := mover.NewServer(st, reg, mover.ServerConfig{
		Addr:       "127.0.0.1:0",
		TLS:        srvTLS,
		Role:       mover.RoleMigrator,
		Version:    "test",
		PhobosVer:  "test",
		Inactivity: inactivity,
	})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() { _ = srv.Close() })
	return &env{staging: staging, st: st, reg: reg, srv: srv, pki: p, addr: srv.Addr()}
}

// dialWith подключается конкретным сертификатом. Ошибку возвращает, а не
// валит тест: половина проверок состоит в том, что подключение не должно
// состояться.
func (e *env) dialWith(t *testing.T, cert, key, ca string) (*mover.Client, error) {
	t.Helper()
	cfg, err := mover.ClientTLS(cert, key, ca, e.pki.drive)
	if err != nil {
		t.Fatal(err)
	}
	return mover.Dial(t.Context(), e.addr, cfg)
}

// rawConn — соединение с валидным сертификатом, но без протокольного клиента:
// им удобно молчать.
func (e *env) rawConn(t *testing.T) *tls.Conn {
	t.Helper()
	cfg, err := mover.ClientTLS(e.pki.clientCert, e.pki.clientKey, e.pki.ca, e.pki.drive)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := tls.Dial("tcp", e.addr, cfg)
	if err != nil {
		t.Fatalf("tls.Dial: %v", err)
	}
	return conn
}

func (e *env) dial(t *testing.T) *mover.Client {
	t.Helper()
	c, err := e.dialWith(t, e.pki.clientCert, e.pki.clientKey, e.pki.ca)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	return c
}

// dialNoCert — клиент вообще без сертификата. В TLS 1.3 клиент считает
// рукопожатие завершённым до того, как сервер проверил его сертификат, поэтому
// отказ виден не в Dial, а при первом чтении: mover здоровается первым, и
// оборванное рукопожатие приходит ошибкой чтения.
func (e *env) dialNoCert(t *testing.T) error {
	t.Helper()
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	pemCA, err := os.ReadFile(e.pki.ca)
	if err != nil {
		t.Fatal(err)
	}
	pool.AppendCertsFromPEM(pemCA)
	cfg := &tls.Config{MinVersion: tls.VersionTLS13, ServerName: mover.MoverDNSName, RootCAs: pool}
	conn, err := tls.Dial("tcp", e.addr, cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := mover.ReadFrame(conn, mover.MaxFrameBytes); err != nil {
		return err
	}
	return nil
}
