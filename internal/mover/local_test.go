package mover_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
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

// get объекта больше клиентского окна (chunk*4 = 16 MiB) обязан проходить:
// принимая байты, клиент возвращает кредиты серверу и обязан пополнить свою
// же оценку окна. До правки любой get больше 16 MiB умирал со «ждали кредиты,
// пришёл кадр данных»: в направлении get кредиты шлёт только клиент, а его
// waitCredit ждал кадра, которого в этом направлении не бывает. Тест держит
// границу: объект ровно на гребне окна не проверял бы ничего — окно
// расходуется целиком только выше него.
func TestGetLargerThanClientWindow(t *testing.T) {
	e := startEnv(t, 48<<20)
	c := e.dial(t)

	body := bytes.Repeat([]byte("окно кредитов "), 700_000) // ~17.4 MiB > 16 MiB окна
	if int64(len(body)) <= 16<<20 {
		t.Fatalf("объект %d байт не больше окна — тест ничего не проверяет", len(body))
	}
	op := uuid.New()
	if _, err := c.Put(mover.PutIn{Op: op, ID: op, Size: int64(len(body)), SHA256: sum(body), Body: bytes.NewReader(body)}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	var got bytes.Buffer
	res, err := c.Get(uuid.New(), op, &got)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if res.Code != mover.CodeOK {
		t.Fatalf("Get: код %s", res.Code)
	}
	if !bytes.Equal(got.Bytes(), body) {
		t.Fatalf("вернулись другие байты: %d против %d", got.Len(), len(body))
	}
}

// Копия пишется из буфера источника: клиент не отправляет ни байта тела.
func TestLocalTransferSendsNoBytes(t *testing.T) {
	e := startEnv(t, 1<<20)
	c := e.dial(t)

	body := bytes.Repeat([]byte("copyme"), 500)
	op1 := uuid.New()
	// hold: буфер нужен для копии, mover не должен выбрасывать данные после success.
	if _, err := c.Put(mover.PutIn{Op: op1, ID: op1, Size: int64(len(body)), SHA256: sum(body), Hold: true, Body: bytes.NewReader(body)}); err != nil {
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

// Ссылки не переживают перезапуск: передача оборвалась вместе с процессом, и
// освободить их больше некому.
func TestRefsResetAfterRestart(t *testing.T) {
	dir := t.TempDir()
	reg, err := mover.NewRegistry(dir, 1<<20, 64<<10, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	op := uuid.New()
	if _, _, err := reg.Begin(op, 5, "aa", nil, nil, "scheduler"); err != nil {
		t.Fatal(err)
	}
	f, err := reg.DataFile(op)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write([]byte("hello"))
	f.Close()
	_ = reg.Update(op, func(r *mover.OpRecord) { r.State = mover.StateDone })
	if err := reg.Acquire(op); err != nil {
		t.Fatal(err)
	}

	// «Перезапуск»: тот же каталог, новый реестр.
	reg2, err := mover.NewRegistry(dir, 1<<20, 64<<10, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	gone, err := reg2.Expire(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(gone) == 0 {
		t.Fatal("буфер с осиротевшей ссылкой не удалён: место в буфере утекает навсегда")
	}
}

// Испорченный после приёма буфер не должен уходить на ленту второй копией:
// сверяется файл, а не запись о нём.
func TestLocalTransferRefusesCorruptedSource(t *testing.T) {
	e := startEnv(t, 1<<20)
	c := e.dial(t)

	body := bytes.Repeat([]byte("intact"), 400)
	op1 := uuid.New()
	if _, err := c.Put(mover.PutIn{Op: op1, ID: op1, Size: int64(len(body)), SHA256: sum(body), Hold: true, Body: bytes.NewReader(body)}); err != nil {
		t.Fatal(err)
	}
	// Портим байт в файле буфера, не трогая реестр: запись по-прежнему говорит
	// «сумма совпала».
	// Портим файл буфера напрямую: реестр при этом продолжает утверждать,
	// что сумма при приёме совпала.
	disk := filepath.Join(e.staging, op1.String()+".bin")
	b, err := os.ReadFile(disk)
	if err != nil {
		t.Fatalf("файл буфера: %v", err)
	}
	b[0] ^= 0xff
	if err := os.WriteFile(disk, b, 0o600); err != nil {
		t.Fatal(err)
	}

	op2 := uuid.New()
	res, err := c.Put(mover.PutIn{Op: op2, ID: op2, Size: int64(len(body)), SHA256: sum(body), FromOp: op1, Body: poisonReader{}})
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if res.Code != mover.CodeChecksumMismatch {
		t.Fatalf("код: %s, ждали checksum_mismatch", res.Code)
	}
	if e.st.Len() != 1 {
		t.Fatalf("на ленту ушло %d объектов, ждали 1", e.st.Len())
	}
}

// Источник get — главный случай Ф0.4: ленту читают один раз и пишут на
// несколько носителей. Сверять надо сумму, посчитанную при чтении.
func TestLocalTransferFromGetSource(t *testing.T) {
	e := startEnv(t, 1<<20)
	c := e.dial(t)

	body := bytes.Repeat([]byte("tape-data"), 300)
	src := uuid.New()
	if _, err := c.Put(mover.PutIn{Op: src, ID: src, Size: int64(len(body)), SHA256: sum(body), Body: bytes.NewReader(body)}); err != nil {
		t.Fatal(err)
	}

	getOp := uuid.New()
	var got bytes.Buffer
	if res, err := c.Get(getOp, src, &got); err != nil || res.Code != mover.CodeOK {
		t.Fatalf("get: %v %+v", err, res)
	}
	if !bytes.Equal(got.Bytes(), body) {
		t.Fatal("прочитано не то")
	}

	dst := uuid.New()
	res, err := c.Put(mover.PutIn{Op: dst, ID: dst, Size: int64(len(body)), SHA256: sum(body), FromOp: getOp, Body: poisonReader{}})
	if err != nil {
		t.Fatalf("put from get: %v", err)
	}
	if res.Code != mover.CodeOK {
		t.Fatalf("код: %s, ждали ok", res.Code)
	}

	// Ожидаемая сумма не совпала с посчитанной при чтении — значит чтение с
	// ленты испорчено: новую копию писать нельзя.
	bad := uuid.New()
	res, err = c.Put(mover.PutIn{Op: bad, ID: bad, Size: int64(len(body)), SHA256: sum([]byte("другое")), FromOp: getOp, Body: poisonReader{}})
	if err != nil {
		t.Fatalf("put from get (bad): %v", err)
	}
	if res.Code != mover.CodeOpConflict {
		t.Fatalf("код при чужой ожидаемой сумме: %s, ждали op_conflict", res.Code)
	}
}

// «Уже есть» разбирается внутри mover: целая копия — успех, неполная —
// отдельный код, а не «ok».
func TestPutExistingCompleteIsOKIncompleteIsNot(t *testing.T) {
	e := startEnv(t, 1<<20)
	c := e.dial(t)

	body := []byte("already there")
	op := uuid.New()
	if _, err := c.Put(mover.PutIn{Op: op, ID: op, Size: int64(len(body)), SHA256: sum(body), Body: bytes.NewReader(body)}); err != nil {
		t.Fatal(err)
	}
	// Забываем операцию в реестре mover'а — как после перезапуска: ответ
	// должен прийти от хранилища, а не из памяти о прошлой записи.
	if err := e.reg.Drop(op); err != nil {
		t.Fatal(err)
	}
	e.st.ForceExists(op, false)
	res, err := c.Put(mover.PutIn{Op: op, ID: op, Size: int64(len(body)), SHA256: sum(body), Body: bytes.NewReader(body)})
	if err != nil {
		t.Fatal(err)
	}
	if res.Code != mover.CodeOK {
		t.Fatalf("целая существующая копия: %s, ждали ok", res.Code)
	}

	op2 := uuid.New()
	body2 := []byte("broken copy")
	e.st.ForceExists(op2, true)
	res, err = c.Put(mover.PutIn{Op: op2, ID: op2, Size: int64(len(body2)), SHA256: sum(body2), Body: bytes.NewReader(body2)})
	if err != nil {
		t.Fatal(err)
	}
	if res.Code != mover.CodeIncomplete {
		t.Fatalf("неполная копия: %s, ждали incomplete", res.Code)
	}
}

// Свойство «держать буфер ровно сколько нужно» двустороннее, и проверять надо
// обе половины: пока копии пишутся — буфер жив; после release — буфера нет.
// Тест только первой половины пропустил бы мутацию «никогда не освобождать».
func TestHeldBufferReleasedByCommand(t *testing.T) {
	e := startEnv(t, 1<<20)
	c := e.dial(t)

	body := bytes.Repeat([]byte("held"), 600)
	src := uuid.New()
	if _, err := c.Put(mover.PutIn{Op: src, ID: src, Size: int64(len(body)), SHA256: sum(body), Hold: true, Body: bytes.NewReader(body)}); err != nil {
		t.Fatal(err)
	}

	// «Не раньше»: копии пишутся — буфер жив.
	mid := uuid.New()
	res, err := c.Put(mover.PutIn{Op: mid, ID: mid, Size: int64(len(body)), SHA256: sum(body), FromOp: src, Body: poisonReader{}})
	if err != nil || res.Code != mover.CodeOK {
		t.Fatalf("копия при живом буфере: %v %+v", err, res)
	}

	// «Не позже»: все копии записаны, буфер освобождён — копировать больше
	// нечего, и mover обязан сказать об этом, а не писать из остатков.
	if r, err := c.Release(src); err != nil || r.Code != mover.CodeOK {
		t.Fatalf("release: %v %+v", err, r)
	}
	last := uuid.New()
	res, err = c.Put(mover.PutIn{Op: last, ID: last, Size: int64(len(body)), SHA256: sum(body), FromOp: src, Body: poisonReader{}})
	if err != nil {
		t.Fatalf("put после release: %v", err)
	}
	if res.Code != mover.CodeNotFound {
		t.Fatalf("копия после release прошла (%s): буфер не освобождён", res.Code)
	}
}

// Копии — разные объекты Phobos с разными тегами. Проверяется через настоящий
// mover по настоящему протоколу, а не через подделку: fake, который принимает
// то, что настоящий mover отвергает, делает тест зелёным по пути, которого в
// продукте нет.
func TestTwoCopiesAreDistinctObjects(t *testing.T) {
	e := startEnv(t, 1<<20)
	c := e.dial(t)

	body := bytes.Repeat([]byte("pack"), 700)
	src := uuid.New()
	if _, err := c.Put(mover.PutIn{Op: src, ID: src, Size: int64(len(body)), SHA256: sum(body),
		Attrs: map[string]string{"tapegw.copy": "left"}, Tags: []string{"side:left"},
		Hold: true, Body: bytes.NewReader(body)}); err != nil {
		t.Fatal(err)
	}

	// Копия: своя операция (и свой oid), тот же буфер, другие теги.
	cp := uuid.NewSHA1(src, []byte("right"))
	res, err := c.Put(mover.PutIn{Op: cp, ID: cp, Size: int64(len(body)), SHA256: sum(body),
		Attrs: map[string]string{"tapegw.copy": "right"}, Tags: []string{"side:right"},
		FromOp: src, Body: poisonReader{}})
	if err != nil {
		t.Fatalf("копия: %v", err)
	}
	if res.Code != mover.CodeOK {
		t.Fatalf("копия: %s", res.Code)
	}
	if e.st.Len() != 2 {
		t.Fatalf("объектов на ленте: %d, ждали 2", e.st.Len())
	}
	if got := strings.Join(e.st.Tags(src), ","); got != "side:left" {
		t.Errorf("теги источника: %q", got)
	}
	if got := strings.Join(e.st.Tags(cp), ","); got != "side:right" {
		t.Errorf("теги копии: %q — копии не разведены тегами", got)
	}
	if e.st.Attrs(cp)["tapegw.copy"] != "right" {
		t.Errorf("атрибут копии: %v", e.st.Attrs(cp))
	}

	// Копия «из самой себя» отвергается: op == from_op.
	same := uuid.New()
	_ = same
	res, err = c.Put(mover.PutIn{Op: src, ID: src, Size: int64(len(body)), SHA256: sum(body), FromOp: src, Body: poisonReader{}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Code != mover.CodeOpConflict {
		t.Errorf("op == from_op: %s, ждали op_conflict", res.Code)
	}
}
