package mover_test

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/mover"
)

// errReader обрывается на N байтах — так моделируется обрыв соединения
// посреди передачи.
type errReader struct {
	r     io.Reader
	limit int
}

func (e *errReader) Read(p []byte) (int, error) {
	if e.limit <= 0 {
		return 0, io.ErrUnexpectedEOF
	}
	if len(p) > e.limit {
		p = p[:e.limit]
	}
	n, err := e.r.Read(p)
	e.limit -= n
	if e.limit <= 0 {
		return n, io.ErrUnexpectedEOF
	}
	return n, err
}

func body(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('a' + i%26)
	}
	return b
}

func putIn(op uuid.UUID, data []byte) mover.PutIn {
	return mover.PutIn{
		Op: op, Body: strings.NewReader(string(data)), Size: int64(len(data)),
		SHA256: mover.SumBytes(data),
		Attrs:  map[string]string{"tapegw.version": op.String()},
	}
}

func TestPutGetRoundTrip(t *testing.T) {
	e := startEnv(t, 1<<20)
	c := e.dial(t)
	defer c.Close()

	data := body(1000)
	op := uuid.New()
	res, err := c.Put(putIn(op, data))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if res.Code != mover.CodeOK {
		t.Fatalf("код = %q, ожидали ok", res.Code)
	}
	if len(res.Extents) == 0 {
		t.Errorf("mover обязан вернуть носители (ADR-0008)")
	}
	if e.st.Len() != 1 {
		t.Errorf("на ленте %d объектов, ожидали 1", e.st.Len())
	}

	var got []byte
	buf := &countWriter{}
	if _, err := c.Get(uuid.New(), op, buf); err != nil {
		t.Fatalf("Get: %v", err)
	}
	got = buf.b
	if string(got) != string(data) {
		t.Errorf("получили %d байт, ожидали %d", len(got), len(data))
	}
}

type countWriter struct{ b []byte }

func (w *countWriter) Write(p []byte) (int, error) { w.b = append(w.b, p...); return len(p), nil }

// Обрыв посреди передачи. Повтор по тому же UUID обязан продолжиться с места
// обрыва, а на ленте должен оказаться ровно один объект: если mover
// перепишет объект целиком, migrator получит дубликат.
func TestResumeAfterBreak(t *testing.T) {
	e := startEnv(t, 1<<20)
	data := body(200_000)
	op := uuid.New()

	c1 := e.dial(t)
	in := putIn(op, data)
	in.Body = &errReader{r: strings.NewReader(string(data)), limit: 100_000}
	if _, err := c1.Put(in); err == nil {
		t.Fatalf("передача обязана оборваться")
	}
	c1.Close()

	if e.st.Len() != 0 {
		t.Fatalf("на ленте не должно быть ничего до конца передачи: %d", e.st.Len())
	}

	c2 := e.dial(t)
	defer c2.Close()
	res, err := c2.Put(putIn(op, data))
	if err != nil {
		t.Fatalf("повторный Put: %v", err)
	}
	if res.Code != mover.CodeOK {
		t.Fatalf("код = %q", res.Code)
	}
	if e.st.Len() != 1 {
		t.Errorf("на ленте %d объектов, ожидали 1", e.st.Len())
	}
	if got := e.st.Calls("put"); got != 1 {
		t.Errorf("вызовов Phobos %d, ожидали 1: докачка не должна перечитывать буфер целиком", got)
	}

	buf := &countWriter{}
	if _, err := c2.Get(uuid.New(), op, buf); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(buf.b) != string(data) {
		t.Errorf("после докачки содержимое расходится: %d байт вместо %d", len(buf.b), len(data))
	}
}

// Обрыв после «записано», до получения ответа tapegw. Повтор обязан вернуть
// успех без второй записи.
func TestRepeatAfterDoneDoesNotRewrite(t *testing.T) {
	e := startEnv(t, 1<<20)
	c := e.dial(t)
	defer c.Close()

	data := body(5000)
	op := uuid.New()
	if res, err := c.Put(putIn(op, data)); err != nil || res.Code != mover.CodeOK {
		t.Fatalf("первый Put: %v %+v", err, res)
	}
	res, err := c.Put(putIn(op, data))
	if err != nil {
		t.Fatalf("второй Put: %v", err)
	}
	if res.Code != mover.CodeOK {
		t.Errorf("повтор обязан вернуть ok, получили %q", res.Code)
	}
	if got := e.st.Calls("put"); got != 1 {
		t.Errorf("вызовов Phobos %d, ожидали 1: повтор не пишет второй раз", got)
	}
}

// Тот же op с другой парой (size, sha256) — ошибка вызывающей стороны.
// Итоговая сверка тут ничего бы не поймала: сумма пришла своя, правильная.
func TestOpConflict(t *testing.T) {
	e := startEnv(t, 1<<20)
	c := e.dial(t)
	defer c.Close()

	data := body(1000)
	op := uuid.New()
	c1 := putIn(op, data)
	c1.Body = &errReader{r: strings.NewReader(string(data)), limit: 300}
	if _, err := c.Put(c1); err == nil {
		t.Fatalf("передача обязана оборваться")
	}

	other := body(2000)
	res, err := c.Put(putIn(op, other))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if res.Code != mover.CodeOpConflict {
		t.Errorf("код = %q, ожидали op_conflict", res.Code)
	}
	if e.st.Len() != 0 {
		t.Errorf("на ленте ничего быть не должно, а %d объектов", e.st.Len())
	}
}

// Искажённый байт в потоке: mover отказывает по SHA-256, на ленту ничего не
// попадает. Это единственное, что делает безопасной докачку по смещению.
func TestChecksumMismatchWritesNothing(t *testing.T) {
	e := startEnv(t, 1<<20)
	c := e.dial(t)
	defer c.Close()

	data := body(4000)
	in := putIn(uuid.New(), data)
	in.SHA256 = mover.SumBytes([]byte("совсем другие байты"))
	res, err := c.Put(in)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if res.Code != mover.CodeChecksumMismatch {
		t.Errorf("код = %q, ожидали checksum_mismatch", res.Code)
	}
	if e.st.Len() != 0 {
		t.Errorf("на ленту не должно было попасть ничего, а %d объектов", e.st.Len())
	}
	if got := e.st.Calls("put"); got != 0 {
		t.Errorf("вызовов Phobos %d, ожидали 0", got)
	}
}

// Буфер втрое меньше объекта, но объект в буфер влезает целиком: кредиты
// кончаются раньше, чем данные, и передача ждёт. Переполнение буфера есть
// «подожди», а не ошибка записи.
func TestBackpressureStillTransfers(t *testing.T) {
	e := startEnv(t, 256<<10) // буфер 256 KiB, chunk 64 KiB
	c := e.dial(t)
	defer c.Close()

	data := body(200 << 10) // втрое больше окна кредитов
	res, err := c.Put(putIn(uuid.New(), data))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if res.Code != mover.CodeOK {
		t.Errorf("код = %q, ожидали ok: нехватка места в буфере — это «подожди»", res.Code)
	}
}

// Объект больше всего буфера — это ошибка конфигурации, а не «подожди».
func TestObjectBiggerThanBufferIsNoBuffer(t *testing.T) {
	e := startEnv(t, 64<<10)
	c := e.dial(t)
	defer c.Close()

	res, err := c.Put(putIn(uuid.New(), body(256<<10)))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if res.Code != mover.CodeNoBuffer {
		t.Errorf("код = %q, ожидали no_buffer", res.Code)
	}
}

// Клиент без сертификата обязан получить отказ на уровне рукопожатия, а не
// «соединение открылось, но ничего не работает».
func TestClientWithoutCertRejected(t *testing.T) {
	e := startEnv(t, 1<<20)
	if _, err := e.dialWith(t, "", "", e.pki.ca); err == nil {
		t.Fatalf("соединение без клиентского сертификата обязано отклоняться")
	}
}

// Сертификат, подписанный чужим CA, — тоже отказ. Иначе «любой в сети»
// получает возможность писать в архив и читать шифротекст.
func TestForeignCARejected(t *testing.T) {
	e := startEnv(t, 128<<10)
	if _, err := e.dialWith(t, e.pki.evilCert, e.pki.evilKey, e.pki.foreignCA); err == nil {
		t.Fatalf("чужой CA обязан отклоняться")
	}
	// Тот же чужой сертификат, но с нашим CA в доверенных: проверка не
	// «сертификат вообще валиден», а «выдан ли нашим CA».
	if _, err := e.dialWith(t, e.pki.evilCert, e.pki.evilKey, e.pki.ca); err == nil {
		t.Errorf("чужой сертификат не должен проходить проверку нашим CA")
	}
}

// Роль сверяется при подключении: migrator на месте recaller начал бы
// архивировать вместо восстановления, и молча.
func TestRoleMismatch(t *testing.T) {
	e := startEnv(t, 1<<20)
	tlsCfg, err := mover.ClientTLS(e.pki.clientCert, e.pki.clientKey, e.pki.ca, "mover.test")
	if err != nil {
		t.Fatal(err)
	}
	_, err = mover.Dial(context.Background(), mover.DialConfig{
		Addr: e.addr, TLS: tlsCfg, Want: mover.RoleRecaller,
	})
	if !errors.Is(err, mover.ErrRoleMismatch) {
		t.Errorf("ожидали ErrRoleMismatch, получили %v", err)
	}
}

// Версия протокола в рукопожатии: несовместимая версия есть отказ соединения,
// а не ошибки разбора посреди передачи.
func TestProtoMismatchRefused(t *testing.T) {
	e := startEnv(t, 1<<20)
	srvTLS, err := mover.ServerTLS(e.pki.serverCert, e.pki.serverKey, e.pki.ca)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", srvTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		_ = mover.WriteJSON(c, mover.Hello{Type: "hello", Proto: mover.ProtoVersion + 1, Role: mover.RoleMigrator})
	}()

	tlsCfg, err := mover.ClientTLS(e.pki.clientCert, e.pki.clientKey, e.pki.ca, "mover.test")
	if err != nil {
		t.Fatal(err)
	}
	_, err = mover.Dial(context.Background(), mover.DialConfig{Addr: ln.Addr().String(), TLS: tlsCfg, Want: mover.RoleMigrator})
	if !errors.Is(err, mover.ErrProtoMismatch) {
		t.Errorf("ожидали ErrProtoMismatch, получили %v", err)
	}
}

// Граница «без ключей» проверяется по набору полей, а не внимательностью:
// любое новое поле обязано быть перечислено здесь, и перечислить поле под
// ключ или имя объекта — значит явно пройти через ревью.
func TestMessageFieldsHaveNoKeys(t *testing.T) {
	allowed := map[string][]string{
		"Hello":    {"t", "proto", "mover", "phobos", "role", "buffer_free"},
		"HelloOk":  {"t", "proto"},
		"Put":      {"t", "op", "size", "sha256", "attrs", "tags", "no_split"},
		"Get":      {"t", "op", "id", "offset", "length"},
		"Status":   {"t", "op"},
		"Ready":    {"t", "op", "offset", "credits"},
		"Credit":   {"t", "op", "credits"},
		"StateMsg": {"t", "op", "state"},
		"Result":   {"t", "op", "code", "state", "version", "extents", "sha256", "items"},
		"Item":     {"id", "code", "version"},
	}
	forbidden := []string{"key", "dek", "tek", "kek", "secret", "password", "bucket",
		"object", "name", "path", "file", "dsn", "tenant_name", "content_type"}

	for name, want := range allowed {
		typ, ok := lookupMessage(name)
		if !ok {
			t.Errorf("сообщение %s не найдено в пакете mover", name)
			continue
		}
		var got []string
		for i := range typ.NumField() {
			tag := typ.Field(i).Tag.Get("json")
			tag = strings.Split(tag, ",")[0]
			got = append(got, tag)
			low := strings.ToLower(tag)
			for _, bad := range forbidden {
				if strings.Contains(low, bad) {
					t.Errorf("%s.%s: поле %q похоже на запрещённое (%q)", name, typ.Field(i).Name, tag, bad)
				}
			}
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s: набор полей изменился\nбыло:  %v\nстало: %v", name, want, got)
		}
	}
}

func lookupMessage(name string) (reflect.Type, bool) {
	switch name {
	case "Hello":
		return reflect.TypeOf(mover.Hello{}), true
	case "HelloOk":
		return reflect.TypeOf(mover.HelloOk{}), true
	case "Put":
		return reflect.TypeOf(mover.Put{}), true
	case "Get":
		return reflect.TypeOf(mover.Get{}), true
	case "Status":
		return reflect.TypeOf(mover.Status{}), true
	case "Ready":
		return reflect.TypeOf(mover.Ready{}), true
	case "Credit":
		return reflect.TypeOf(mover.Credit{}), true
	case "StateMsg":
		return reflect.TypeOf(mover.StateMsg{}), true
	case "Result":
		return reflect.TypeOf(mover.Result{}), true
	case "Item":
		return reflect.TypeOf(mover.Item{}), true
	}
	return nil, false
}

// Кадр с испорченной длиной не должен заставить выделить память под мусор.
func TestFrameRejectsBogusLength(t *testing.T) {
	if _, err := mover.ReadFrame(strings.NewReader("J\x7f\xff\xff\xffxx"), 1024); err == nil {
		t.Errorf("кадр с длиной больше потолка обязан отклоняться")
	}
	if _, err := mover.ReadFrame(strings.NewReader("Z\x00\x00\x00\x01x"), 1024); err == nil {
		t.Errorf("кадр неизвестного типа обязан отклоняться")
	}
}

var _ = json.Marshal
