package mover_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/vladsimachkov/tapegw/internal/mover"
)

// #101: stat по объекту и удаление неполного. Без них планировщик при EEXIST
// слеп: «объект есть» не отвечает, цела ли копия.

func TestStatObjectAnswers(t *testing.T) {
	e := startEnv(t, 1<<20)
	c := e.dial(t)

	missing := uuid.New()
	res, err := c.StatObject(missing)
	if err != nil {
		t.Fatal(err)
	}
	if res.Code != mover.CodeNotFound {
		t.Errorf("нет объекта: %s, ждали not_found", res.Code)
	}

	body := []byte("complete copy")
	op := uuid.New()
	if _, err := c.Put(mover.PutIn{Op: op, ID: op, Size: int64(len(body)), SHA256: sum(body), Body: bytes.NewReader(body)}); err != nil {
		t.Fatal(err)
	}
	res, err = c.StatObject(op)
	if err != nil {
		t.Fatal(err)
	}
	if res.Code != mover.CodeOK {
		t.Errorf("целая копия: %s, ждали ok", res.Code)
	}
	if res.Size != int64(len(body)) {
		t.Errorf("stat вернул размер %d, объект на %d", res.Size, len(body))
	}
	if len(res.Extents) == 0 {
		t.Error("stat целой копии не вернул носитель — копию не с чем сверять")
	}

	// Неполная: объект есть, но данные не полны.
	broken := uuid.New()
	e.st.ForceExists(broken, true)
	res, err = c.StatObject(broken)
	if err != nil {
		t.Fatal(err)
	}
	if res.Code != mover.CodeIncomplete {
		t.Errorf("неполная копия: %s, ждали incomplete", res.Code)
	}
}

// Целый объект удалить нельзя: «удалим и повторим» поверх целой копии — это
// потеря данных ради удобства кода.
func TestDeleteRefusesCompleteObject(t *testing.T) {
	e := startEnv(t, 1<<20)
	c := e.dial(t)

	body := []byte("do not delete me")
	op := uuid.New()
	if _, err := c.Put(mover.PutIn{Op: op, ID: op, Size: int64(len(body)), SHA256: sum(body), Body: bytes.NewReader(body)}); err != nil {
		t.Fatal(err)
	}
	res, err := c.DeleteObject(op)
	if err != nil {
		t.Fatal(err)
	}
	if res.Code != mover.CodeDenied {
		t.Errorf("удаление целой копии: %s, ждали denied", res.Code)
	}
	if !e.st.Has(op) {
		t.Error("целая копия всё равно удалена")
	}

	broken := uuid.New()
	e.st.ForceExists(broken, true)
	res, err = c.DeleteObject(broken)
	if err != nil {
		t.Fatal(err)
	}
	if res.Code != mover.CodeOK {
		t.Errorf("удаление неполной копии: %s, ждали ok", res.Code)
	}
}

// Ошибка Stat — это «мы не знаем», а не «объекта нет». Разрушительная операция
// при неизвестном состоянии выполняться не должна: иначе недоступный LRS
// превращается в потерю архивной копии.
func TestDeleteRefusedWhenStatFails(t *testing.T) {
	e := startEnv(t, 1<<20)
	c := e.dial(t)

	body := []byte("do not delete me")
	op := uuid.New()
	if _, err := c.Put(mover.PutIn{Op: op, ID: op, Size: int64(len(body)), SHA256: sum(body), Body: bytes.NewReader(body)}); err != nil {
		t.Fatal(err)
	}

	e.st.FailStat(errors.New("phobos: LRS недоступен"))
	res, err := c.DeleteObject(op)
	if err != nil {
		t.Fatal(err)
	}
	if res.Code != mover.CodeIO {
		t.Errorf("delete при ошибке stat: %s, ждали io", res.Code)
	}
	e.st.FailStat(nil)
	if !e.st.Has(op) {
		t.Error("объект удалён, хотя его состояние неизвестно")
	}

	// Нет объекта — удалять нечего, это успех, а не ошибка.
	res, err = c.DeleteObject(uuid.New())
	if err != nil {
		t.Fatal(err)
	}
	if res.Code != mover.CodeOK {
		t.Errorf("delete отсутствующего: %s, ждали ok", res.Code)
	}
}

// --- purge (M6): удаление целого объекта — отдельная операция и отдельная роль.

// Purge обязан удалять целый объект — в этом весь его смысл, — но только
// клиенту с ролью deleter. Планировщик архивации его получить не должен:
// иначе «почистить неполную копию» и «уничтожить архив» различаются только
// аккуратностью кода, а не сертификатом.
func TestPurgeOnlyForDeleter(t *testing.T) {
	e := startEnvWith(t, 1<<20, mover.ClientScheduler, mover.ClientDeleter)

	body := []byte("archived object")
	op := uuid.New()
	c := e.dial(t)
	if _, err := c.Put(mover.PutIn{Op: op, ID: op, Size: int64(len(body)), SHA256: sum(body), Body: bytes.NewReader(body)}); err != nil {
		t.Fatal(err)
	}

	dc, err := e.dialWith(t, e.pki.deleterCert, e.pki.deleterKey, e.pki.ca)
	if err != nil {
		t.Fatal(err)
	}
	res, err := dc.PurgeObject(op)
	if err != nil {
		t.Fatal(err)
	}
	if res.Code != mover.CodeOK {
		t.Errorf("purge от deleter: %s, ждали ok", res.Code)
	}
	if e.st.Has(op) {
		t.Error("purge от deleter обязан удалить целый объект")
	}
}

func TestPurgeDeniedForScheduler(t *testing.T) {
	e := startEnvWith(t, 1<<20, mover.ClientScheduler, mover.ClientDeleter)
	c := e.dial(t)

	body := []byte("archived object")
	op := uuid.New()
	if _, err := c.Put(mover.PutIn{Op: op, ID: op, Size: int64(len(body)), SHA256: sum(body), Body: bytes.NewReader(body)}); err != nil {
		t.Fatal(err)
	}

	res, err := c.PurgeObject(op)
	if err != nil {
		t.Fatal(err)
	}
	if res.Code != mover.CodeDenied {
		t.Errorf("purge от scheduler: %s, ждали denied", res.Code)
	}
	if !e.st.Has(op) {
		t.Error("объект удалён purge'ом от scheduler — роль не проверилась")
	}
}

// Старая операция не должна разъехаться вместе с новой: delete целого объекта
// остаётся denied даже для deleter'а — у purge для этого отдельная граница.
func TestDeleteStillRefusesCompleteForDeleter(t *testing.T) {
	e := startEnvWith(t, 1<<20, mover.ClientScheduler, mover.ClientDeleter)

	body := []byte("archived object")
	op := uuid.New()
	c := e.dial(t)
	if _, err := c.Put(mover.PutIn{Op: op, ID: op, Size: int64(len(body)), SHA256: sum(body), Body: bytes.NewReader(body)}); err != nil {
		t.Fatal(err)
	}

	dc, err := e.dialWith(t, e.pki.deleterCert, e.pki.deleterKey, e.pki.ca)
	if err != nil {
		t.Fatal(err)
	}
	res, err := dc.DeleteObject(op)
	if err != nil {
		t.Fatal(err)
	}
	if res.Code != mover.CodeDenied {
		t.Errorf("delete целого от deleter: %s, ждали denied — старая операция не должна пускать целое", res.Code)
	}
	if !e.st.Has(op) {
		t.Error("delete целого от deleter удалил объект")
	}
}

// Идемпотность для воркера, повторяющего шаг после падения:purge отсутствующего
// объекта — успех, а не ошибка.
func TestPurgeMissingIsSuccess(t *testing.T) {
	e := startEnvWith(t, 1<<20, mover.ClientScheduler, mover.ClientDeleter)
	dc, err := e.dialWith(t, e.pki.deleterCert, e.pki.deleterKey, e.pki.ca)
	if err != nil {
		t.Fatal(err)
	}
	res, err := dc.PurgeObject(uuid.New())
	if err != nil {
		t.Fatal(err)
	}
	if res.Code != mover.CodeOK {
		t.Errorf("purge отсутствующего: %s, ждали ok", res.Code)
	}
}
