package mover_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vladsimachkov/tapegw/internal/mover"
)

// Локальная передача (Ф0.4): копия пачки пишется из буфера уже записанной
// операции, без байтов по сети и без повторного чтения ленты.

type poisonReader struct{}

var errPoison = errors.New("местный: байты не должны читаться")

func (poisonReader) Read([]byte) (int, error) { return 0, errPoison }

func sum(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

// Копия пишется из буфера источника: клиент не отправляет ни байта тела.
func TestLocalTransferSendsNoBytes(t *testing.T) {
	e := startEnv(t, 1<<20)
	c := e.dial(t)

	body := bytes.Repeat([]byte("copyme"), 500)
	op1 := uuid.New()
	if _, err := c.Put(mover.PutIn{Op: op1, ID: op1, Size: int64(len(body)), SHA256: sum(body), Body: bytes.NewReader(body)}); err != nil {
		t.Fatal(err)
	}

	op2 := uuid.New()
	res, err := c.Put(mover.PutIn{
		Op: op2, ID: op2, Size: int64(len(body)), SHA256: sum(body),
		FromOp: op1,
		// Яд вместо тела: если сервер всё-таки попросит байты, клиент
		// упадёт с этой ошибкой, и тест это увидит.
		Body: poisonReader{},
	})
	if err != nil {
		t.Fatalf("локальная передача: %v", err)
	}
	if res.Code != mover.CodeOK {
		t.Fatalf("код: %s (%s)", res.Code, res.State)
	}
	if e.st.Len() != 2 {
		t.Fatalf("объектов в хранилище: %d, ждали 2", e.st.Len())
	}
}

// Источник обязан быть завершён: staged ещё не сверен, неизвестного нет вовсе.
func TestLocalTransferRefusesUnfinishedSource(t *testing.T) {
	e := startEnv(t, 1<<20)
	c := e.dial(t)

	for name, src := range map[string]uuid.UUID{"неизвестная": uuid.New()} {
		body := []byte("x")
		op2 := uuid.New()
		res, err := c.Put(mover.PutIn{Op: op2, ID: op2, Size: int64(len(body)), SHA256: sum(body), FromOp: src, Body: poisonReader{}})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if res.Code != mover.CodeNotFound {
			t.Errorf("%s: код %s, ждали not_found", name, res.Code)
		}
	}
}

// Чужой буфер не отдают: роль берётся из сертификата, а не из слов клиента.
func TestLocalTransferRefusesForeignClient(t *testing.T) {
	e := startEnvWith(t, 1<<20, mover.ClientScheduler, mover.ClientRestorer)
	c := e.dial(t)

	body := bytes.Repeat([]byte("mine"), 300)
	op1 := uuid.New()
	if _, err := c.Put(mover.PutIn{Op: op1, ID: op1, Size: int64(len(body)), SHA256: sum(body), Body: bytes.NewReader(body)}); err != nil {
		t.Fatal(err)
	}

	rest, err := e.dialWith(t, e.pki.restorerCert, e.pki.restorerKey, e.pki.ca)
	if err != nil {
		t.Fatalf("restorer: %v", err)
	}
	op2 := uuid.New()
	res, err := rest.Put(mover.PutIn{Op: op2, ID: op2, Size: int64(len(body)), SHA256: sum(body), FromOp: op1, Body: poisonReader{}})
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if res.Code != mover.CodeDenied {
		t.Fatalf("код: %s, ждали denied", res.Code)
	}
}

// Ожидаемая сумма обязательна и обязана совпасть с суммой источника: иначе op
// становится указателем на любой буфер, а «копия» перестаёт быть копией.
func TestLocalTransferRefusesWrongOrMissingSHA(t *testing.T) {
	e := startEnv(t, 1<<20)
	c := e.dial(t)

	body := bytes.Repeat([]byte("data"), 400)
	op1 := uuid.New()
	if _, err := c.Put(mover.PutIn{Op: op1, ID: op1, Size: int64(len(body)), SHA256: sum(body), Body: bytes.NewReader(body)}); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct{ name, sha string }{
		{"без суммы", ""},
		{"чужая сумма", sum([]byte("совсем другое содержимое"))},
		{"верный размер, другая сумма", sum([]byte("и это другое"))},
	} {
		op2 := uuid.New()
		res, err := c.Put(mover.PutIn{Op: op2, ID: op2, Size: int64(len(body)), SHA256: tc.sha, FromOp: op1, Body: poisonReader{}})
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if res.Code != mover.CodeOpConflict {
			t.Errorf("%s: код %s, ждали op_conflict", tc.name, res.Code)
		}
	}
}

// Буфер источника держат, пока на него есть ссылка: иначе result_ttl выбросит
// содержимое на середине записи копии, и на ленту ляжет неполный объект.
func TestSourceBufferHeldWhileReferenced(t *testing.T) {
	dir := t.TempDir()
	reg, err := mover.NewRegistry(dir, 1<<20, 64<<10, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	op := uuid.New()
	if _, _, err := reg.Begin(op, 10, "aa", nil, nil, "scheduler"); err != nil {
		t.Fatal(err)
	}
	f, err := reg.DataFile(op)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("0123456789")); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if err := reg.Update(op, func(r *mover.OpRecord) { r.State = mover.StateDone }); err != nil {
		t.Fatal(err)
	}

	if err := reg.Acquire(op); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	// Срок вышел, но ссылка держит буфер.
	time.Sleep(30 * time.Millisecond)
	if gone, err := reg.Expire(time.Now()); err != nil || len(gone) != 0 {
		t.Fatalf("буфер со ссылкой выброшен: %v (err=%v)", gone, err)
	}
	if _, err := reg.DataFile(op); err != nil {
		t.Fatalf("данные источника пропали при живой ссылке: %v", err)
	}

	if err := reg.Release(op); err != nil {
		t.Fatalf("release: %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	if gone, err := reg.Expire(time.Now()); err != nil || len(gone) == 0 {
		t.Fatalf("буфер без ссылки не выброшен по сроку: %v (err=%v)", gone, err)
	}
}

// Источник в staged — не источник: он ещё не дочитан и не сверен.
func TestAcquireRefusesUnfinished(t *testing.T) {
	reg, err := mover.NewRegistry(t.TempDir(), 1<<20, 64<<10, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	op := uuid.New()
	if _, _, err := reg.Begin(op, 10, "aa", nil, nil, "scheduler"); err != nil {
		t.Fatal(err)
	}
	if err := reg.Acquire(op); !errors.Is(err, mover.ErrSrcNotDone) {
		t.Fatalf("acquire по staged: %v, ждали ErrSrcNotDone", err)
	}
	if err := reg.Acquire(uuid.New()); err == nil {
		t.Fatal("acquire по неизвестной операции прошёл")
	}
}
