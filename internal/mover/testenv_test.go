package mover_test

import (
	"crypto/tls"
	"testing"
	"time"

	"github.com/vladsimachkov/tapegw/internal/mover"
	"github.com/vladsimachkov/tapegw/internal/mover/movertest"
)

// Стенд живёт в internal/mover/movertest: тот же сервер и та же PKI доступны и
// тестам других пакетов. Здесь только тонкая обёртка под имена, к которым
// привыкли тесты mover, — второго набора фейков быть не должно: в двух fake'ах
// правила всегда расходятся, и расходится проверка.

type pkiPaths struct {
	ca, serverCert, serverKey                string
	clientCert, clientKey                    string
	restorerCert, restorerKey                string
	otherDriveCert, otherDriveKey            string
	moverAsClientCert, moverAsClientKey      string
	noExtKeyCert, noExtKeyKey                string
	foreignCA, foreignCliCert, foreignCliKey string
	drive, otherDrive                        string
}

type env struct {
	staging string
	st      *movertest.Store
	reg     *mover.Registry
	srv     *mover.Server
	pki     pkiPaths
	addr    string
	e       *movertest.Env
}

func wrap(t *testing.T, e *movertest.Env) *env {
	t.Helper()
	p := e.PKI
	return &env{
		staging: e.Staging, st: e.Store, reg: e.Reg, srv: e.Srv, addr: e.Addr(), e: e,
		pki: pkiPaths{
			ca: p.CA, serverCert: p.ServerCert, serverKey: p.ServerKey,
			clientCert: p.ClientCert, clientKey: p.ClientKey,
			restorerCert: p.RestorerCert, restorerKey: p.RestorerKey,
			otherDriveCert: p.OtherDriveCert, otherDriveKey: p.OtherDriveKey,
			moverAsClientCert: p.MoverAsClientCert, moverAsClientKey: p.MoverAsClientKey,
			noExtKeyCert: p.NoExtKeyCert, noExtKeyKey: p.NoExtKeyKey,
			foreignCA: p.ForeignCA, foreignCliCert: p.ForeignClientCert, foreignCliKey: p.ForeignClientKey,
			drive: p.Drive, otherDrive: p.OtherDrive,
		},
	}
}

func startEnv(t *testing.T, bufMax int64) *env {
	return startEnvWith(t, bufMax, mover.ClientScheduler)
}

func startEnvWith(t *testing.T, bufMax int64, allowed ...mover.ClientID) *env {
	return wrap(t, movertest.New(t, bufMax, allowed...))
}

// startEnvTimed — env с заданным таймаутом бездействия. 0 — значение по
// умолчанию. Нужен тесту «клиент молчит → соединение закрыто».
func startEnvTimed(t *testing.T, bufMax int64, inactivity time.Duration, allowed ...mover.ClientID) *env {
	t.Helper()
	e := movertest.NewTimed(t, bufMax, inactivity, allowed...)
	return wrap(t, e)
}

func (e *env) dialWith(t *testing.T, cert, key, ca string) (*mover.Client, error) {
	t.Helper()
	return e.e.DialAs(t, cert, key, e.pki.drive)
}

func (e *env) rawConn(t *testing.T) *tls.Conn {
	t.Helper()
	return e.e.RawConn(t)
}

func (e *env) dial(t *testing.T) *mover.Client {
	t.Helper()
	return e.e.Dial(t)
}

func (e *env) dialNoCert(t *testing.T) error {
	t.Helper()
	return e.e.DialNoCert(t)
}
