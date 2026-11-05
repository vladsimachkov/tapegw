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
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vladsimachkov/tapegw/internal/mover"
	"github.com/vladsimachkov/tapegw/internal/store"
	"github.com/vladsimachkov/tapegw/internal/store/fake"
)

// Тесты идут поверх настоящего TCP и настоящего mTLS: мок транспорта проверял
// бы наш собственный мок, а не границу. Сертификаты генерируются в тесте —
// «выданный нашим CA» и «выданный чужим» должны быть различимы.

type pki struct {
	ca         string
	serverCert string
	serverKey  string
	clientCert string
	clientKey  string
	foreignCA  string
	evilCert   string
	evilKey    string
}

type env struct {
	srv  *mover.Server
	reg  *mover.Registry
	st   *fake.Store
	pki  *pki
	addr string
}

func startEnv(t *testing.T, bufBytes int64) *env {
	t.Helper()
	return startEnvWith(t, fake.New(), bufBytes)
}

func startEnvWith(t *testing.T, st store.Store, bufBytes int64) *env {
	t.Helper()
	dir := t.TempDir()

	caPEM, caKey, caCert := mustCA(t, "tapegw-test-ca")
	sCert, sKey := leaf(t, "mover.test", caCert, caKey)
	cCert, cKey := leaf(t, "tapegw.test", caCert, caKey)
	fcaPEM, fcaKey, fcaCert := mustCA(t, "someone-elses-ca")
	evCert, evKey := leaf(t, "tapegw.test", fcaCert, fcaKey)

	p := &pki{
		ca:         write(t, dir, "ca.pem", caPEM),
		serverCert: write(t, dir, "server.pem", sCert),
		serverKey:  write(t, dir, "server.key", sKey),
		clientCert: write(t, dir, "client.pem", cCert),
		clientKey:  write(t, dir, "client.key", cKey),
		foreignCA:  write(t, dir, "foreign-ca.pem", fcaPEM),
		evilCert:   write(t, dir, "evil.pem", evCert),
		evilKey:    write(t, dir, "evil.key", evKey),
	}

	srvTLS, err := mover.ServerTLS(p.serverCert, p.serverKey, p.ca)
	if err != nil {
		t.Fatal(err)
	}
	reg, err := mover.NewRegistry(filepath.Join(dir, "staging"), bufBytes, time.Hour, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	srv, err := mover.NewServer(st, reg, mover.ServerConfig{
		Addr: "127.0.0.1:0", TLS: srvTLS, Role: mover.RoleMigrator,
		Version: "test", PhobosVer: "test", Family: "dir", Chunk: 64 << 10,
		Log: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() { _ = srv.Close() })
	return &env{srv: srv, reg: reg, st: st.(*fake.Store), pki: p, addr: srv.Addr()}
}

// dial — штатное подключение tapegw к mover'у.
func (e *env) dial(t *testing.T) *mover.Client {
	t.Helper()
	tlsCfg, err := mover.ClientTLS(e.pki.clientCert, e.pki.clientKey, e.pki.ca, "mover.test")
	if err != nil {
		t.Fatal(err)
	}
	c, err := mover.Dial(context.Background(), mover.DialConfig{
		Addr: e.addr, TLS: tlsCfg, Want: mover.RoleMigrator, Chunk: 64 << 10,
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	return c
}

// dialWith — подключение с произвольными сертификатами: так проверяется, что
// чужой CA и отсутствие сертификата действительно отклоняются.
func (e *env) dialWith(t *testing.T, cert, key, ca string) (*mover.Client, error) {
	t.Helper()
	tlsCfg := &tls.Config{RootCAs: mustPool(t, ca), ServerName: "mover.test", MinVersion: tls.VersionTLS13}
	if cert != "" {
		pair, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			t.Fatal(err)
		}
		tlsCfg.Certificates = []tls.Certificate{pair}
	}
	return mover.Dial(context.Background(), mover.DialConfig{
		Addr: e.addr, TLS: tlsCfg, Want: mover.RoleMigrator, Chunk: 64 << 10,
	})
}

func mustPool(t *testing.T, caFile string) *x509.CertPool {
	t.Helper()
	b, err := os.ReadFile(caFile)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(b) {
		t.Fatal("CA не распарсился")
	}
	return pool
}

func mustCA(t *testing.T, cn string) ([]byte, *ecdsa.PrivateKey, *x509.Certificate) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), key, cert
}

// leaf — и серверный, и клиентский сертификат: ExtKeyUsage у обоих оба
// назначения, иначе mTLS не собрать разными ключами.
func leaf(t *testing.T, cn string, ca *x509.Certificate, caKey *ecdsa.PrivateKey) ([]byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{cn, "mover.test", "tapegw.test"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

func write(t *testing.T, dir, name string, b []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
