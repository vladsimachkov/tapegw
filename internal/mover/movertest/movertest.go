// Package movertest поднимает настоящий mover поверх поддельного хранилища:
// TCP, mTLS, генерируемая PKI, тот же сервер и тот же протокол, что в бою.
//
// Причина существования пакета — класс ошибок, который иначе возвращается
// снова и снова: тестовый fake знает правила mover'а ровно в том объёме, в
// каком их запомнил автор теста. Исходная ошибка копий (`op == from_op`) прошла
// именно так: fake принимал то, что настоящий mover отвергает. Если правила
// живут в одном месте и проверяются настоящим сервером, отстать они не могут.
//
// Fake в юнитах migrator остаётся: он быстрый и удобный для инъекции сбоев.
package movertest

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

// PKI — пути к сгенерированным сертификатам. Отказ проверяется именно так:
// чужой CA, клиент без сертификата, сертификат mover'а в роли клиента — одним
// «правильным» сертификатом этого не проверить.
type PKI struct {
	CA                string
	ServerCert        string
	ServerKey         string
	ClientCert        string
	ClientKey         string
	RestorerCert      string
	RestorerKey       string
	OtherDriveCert    string
	OtherDriveKey     string
	MoverAsClientCert string
	MoverAsClientKey  string
	NoExtKeyCert      string
	NoExtKeyKey       string
	ForeignCA         string
	ForeignClientCert string
	ForeignClientKey  string
	Drive             string
	OtherDrive        string
}

// Store — хранилище без cgo. Ведёт теги и атрибуты по oid: тест копий проверяет
// ими, что копии — разные объекты с разными тегами, а не одна запись дважды.
type Store struct {
	mu         sync.Mutex
	data       map[uuid.UUID][]byte
	tags       map[uuid.UUID][]string
	attrs      map[uuid.UUID]map[string]string
	calls      map[string]int
	exists     map[uuid.UUID]bool
	incomplete map[uuid.UUID]bool
	extents    []string
}

func NewStore() *Store {
	return &Store{
		data:       map[uuid.UUID][]byte{},
		tags:       map[uuid.UUID][]string{},
		attrs:      map[uuid.UUID]map[string]string{},
		calls:      map[string]int{},
		exists:     map[uuid.UUID]bool{},
		incomplete: map[uuid.UUID]bool{},
		extents:    []string{"TST-MOVER"},
	}
}

// SetExtents — что хранилище ответит на Extents. Тест пачки обязан назвать
// ленту, которая есть в реестре БД, иначе копия останется в 'writing'.
func (f *Store) SetExtents(labels ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.extents = labels
}

// ForceExists изображает «объект уже есть»: incomplete=true — «есть, но
// неполный» (оборванная запись). Без этого ветку Stat в mover не вызвать.
func (f *Store) ForceExists(id uuid.UUID, incomplete bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.exists[id] = true
	if incomplete {
		f.incomplete[id] = true
	}
}

func (f *Store) Len() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.data)
}

func (f *Store) Calls(what string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[what]
}

func (f *Store) Tags(id uuid.UUID) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.tags[id]...)
}

func (f *Store) Attrs(id uuid.UUID) map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attrs[id]
}

func (f *Store) Has(id uuid.UUID) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.data[id]
	return ok
}

func (f *Store) Put(_ context.Context, batch []store.PutItem, opt store.PutOptions) []store.Result {
	f.mu.Lock()
	defer f.mu.Unlock()
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

func (f *Store) Get(_ context.Context, items []store.GetItem) []store.Result {
	f.mu.Lock()
	defer f.mu.Unlock()
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

func (f *Store) Stat(_ context.Context, id uuid.UUID) (store.Meta, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.data[id]
	if !ok {
		return store.Meta{}, store.ErrNotFound
	}
	return store.Meta{ID: id, Size: int64(len(b)), Version: 1, Complete: !f.incomplete[id]}, nil
}

func (f *Store) Delete(_ context.Context, ids []uuid.UUID) []store.Result {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls["delete"]++
	out := make([]store.Result, 0, len(ids))
	for _, id := range ids {
		delete(f.data, id)
		out = append(out, store.Result{ID: id})
	}
	return out
}

func (f *Store) Extents(_ context.Context, ids []uuid.UUID) (map[uuid.UUID][]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[uuid.UUID][]string{}
	for _, id := range ids {
		if _, ok := f.data[id]; ok {
			out[id] = append([]string(nil), f.extents...)
		}
	}
	return out, nil
}

func (f *Store) Locate(_ context.Context, ids []uuid.UUID) ([]store.Location, error) {
	out := make([]store.Location, 0, len(ids))
	for range ids {
		out = append(out, store.Location{TapeID: "TST-MOVER", DriveHint: "drive-A"})
	}
	return out, nil
}

func (f *Store) Close() error { return nil }

// Env — запущенный mover с подключением к нему.
type Env struct {
	Store   *Store
	Reg     *mover.Registry
	Srv     *mover.Server
	PKI     PKI
	Staging string
	addr    string
}

// New поднимает mover на случайном порту. allowed — роли клиентов, которым
// сервер разрешает обслуживание.
func New(t *testing.T, bufMax int64, allowed ...mover.ClientID) *Env {
	return NewTimed(t, bufMax, 0, allowed...)
}

// NewTimed — то же, но с заданным таймаутом бездействия (0 — по умолчанию).
func NewTimed(t *testing.T, bufMax int64, inactivity time.Duration, allowed ...mover.ClientID) *Env {
	t.Helper()
	if len(allowed) == 0 {
		allowed = []mover.ClientID{mover.ClientScheduler}
	}
	p := newPKI(t)
	st := NewStore()
	staging := filepath.Join(t.TempDir(), "staging")
	reg, err := mover.NewRegistry(staging, bufMax, 64<<10, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	srvTLS, err := mover.ServerTLS(p.ServerCert, p.ServerKey, p.CA, allowed...)
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
	return &Env{Store: st, Reg: reg, Srv: srv, PKI: p, Staging: staging, addr: srv.Addr()}
}

func (e *Env) Addr() string { return e.addr }

// Dial подключается штатным клиентом планировщика.
func (e *Env) Dial(t *testing.T) *mover.Client {
	t.Helper()
	c, err := e.DialWith(t, e.PKI.ClientCert, e.PKI.ClientKey)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	return c
}

// DialWith подключается конкретным сертификатом. Ошибка возвращается, а не
// валит тест: половина проверок состоит в том, что подключение не должно
// состояться.
func (e *Env) DialWith(t *testing.T, cert, key string) (*mover.Client, error) {
	t.Helper()
	cfg, err := mover.ClientTLS(cert, key, e.PKI.CA, e.PKI.Drive)
	if err != nil {
		t.Fatal(err)
	}
	return mover.Dial(t.Context(), e.addr, cfg)
}

// DialAs — подключение с указанием ожидаемого драйва (для проверок «не тот mover»).
func (e *Env) DialAs(t *testing.T, cert, key, drive string) (*mover.Client, error) {
	t.Helper()
	cfg, err := mover.ClientTLS(cert, key, e.PKI.CA, drive)
	if err != nil {
		t.Fatal(err)
	}
	return mover.Dial(t.Context(), e.addr, cfg)
}

// RawConn — соединение с валидным сертификатом, но без протокольного клиента:
// им удобно молчать.
func (e *Env) RawConn(t *testing.T) *tls.Conn {
	t.Helper()
	cfg, err := mover.ClientTLS(e.PKI.ClientCert, e.PKI.ClientKey, e.PKI.CA, e.PKI.Drive)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := tls.Dial("tcp", e.addr, cfg)
	if err != nil {
		t.Fatalf("tls.Dial: %v", err)
	}
	return conn
}

// DialNoCert — клиент без сертификата. В TLS 1.3 клиент считает рукопожатие
// завершённым до проверки его сертификата сервером, поэтому отказ виден не в
// Dial, а при первом чтении.
func (e *Env) DialNoCert(t *testing.T) error {
	t.Helper()
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	pemCA, err := os.ReadFile(e.PKI.CA)
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
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 1)
	_, err = conn.Read(buf)
	return err
}

func newPKI(t *testing.T) PKI {
	t.Helper()
	dir := t.TempDir()
	p := PKI{Drive: "drive-A", OtherDrive: "drive-B"}

	write := func(name string, b []byte) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, b, 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}

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
		return tmpl, key, write(name+".pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
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
			URIs:         uris,
			DNSNames:     dns,
		}
		if ext != 0 {
			tmpl.ExtKeyUsage = []x509.ExtKeyUsage{ext}
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		kb, err := x509.MarshalECPrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		return write(cn+".crt", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
			write(cn+".key", pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}))
	}

	ca, caKey, caPath := newCA("ca")
	p.CA = caPath

	p.ServerCert, p.ServerKey = sign(ca, caKey, "mover-A", x509.ExtKeyUsageServerAuth,
		[]*url.URL{mover.MoverURI(p.Drive)}, []string{mover.MoverDNSName})
	p.OtherDriveCert, p.OtherDriveKey = sign(ca, caKey, "mover-B", x509.ExtKeyUsageServerAuth,
		[]*url.URL{mover.MoverURI(p.OtherDrive)}, []string{mover.MoverDNSName})
	p.MoverAsClientCert, p.MoverAsClientKey = p.ServerCert, p.ServerKey

	// SAN-URI роли на месте, ExtKeyUsage отсутствует вовсе: Go такой сертификат
	// считает годным на любое назначение, поэтому отказ обязан исходить от нас.
	p.NoExtKeyCert, p.NoExtKeyKey = sign(ca, caKey, "tapegw-noext", 0,
		[]*url.URL{mover.ClientURI(mover.ClientScheduler)}, nil)

	p.ClientCert, p.ClientKey = sign(ca, caKey, "tapegw-scheduler", x509.ExtKeyUsageClientAuth,
		[]*url.URL{mover.ClientURI(mover.ClientScheduler)}, nil)
	p.RestorerCert, p.RestorerKey = sign(ca, caKey, "tapegw-restorer", x509.ExtKeyUsageClientAuth,
		[]*url.URL{mover.ClientURI(mover.ClientRestorer)}, nil)

	fca, fcaKey, fcaPath := newCA("foreign-ca")
	p.ForeignCA = fcaPath
	p.ForeignClientCert, p.ForeignClientKey = sign(fca, fcaKey, "foreign-client", x509.ExtKeyUsageClientAuth,
		[]*url.URL{mover.ClientURI(mover.ClientScheduler)}, nil)

	return p
}
