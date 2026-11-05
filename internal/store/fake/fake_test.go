package fake_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/store"
	"github.com/vladsimachkov/tapegw/internal/store/fake"
)

// Смысл этих тестов не в том, что fake что-то хранит, а в том, что он
// предсказуемо отказывает **как настоящий Phobos**. Тесты migrator на fake не
// должны проходить там, где на ленте всё упало, поэтому здесь прямо закреплены
// четыре поведения, про которые есть замеры спайков: атомарность батча,
// неполная копия после обрыва (M0-02), ENOSPC без побочных эффектов и EEXIST
// на повторную запись.

func put(s *fake.Store, id uuid.UUID, body string, attrs map[string]string) store.Result {
	return s.Put(context.Background(),
		[]store.PutItem{{ID: id, Reader: strings.NewReader(body), Size: int64(len(body)), Attrs: attrs}},
		store.PutOptions{Family: "dir"})[0]
}

func get(s *fake.Store, id uuid.UUID) ([]byte, error) {
	var buf bytes.Buffer
	r := s.Get(context.Background(), []store.GetItem{{ID: id, Writer: &buf}})[0]
	return buf.Bytes(), r.Err
}

func TestPutGetRoundTrip(t *testing.T) {
	s := fake.New()
	id := uuid.New()

	res := put(s, id, "hello tape", map[string]string{"key": "a.txt"})
	if res.Err != nil {
		t.Fatalf("Put: %v", res.Err)
	}
	if res.Version != 1 {
		t.Errorf("Version = %d", res.Version)
	}

	meta, err := s.Stat(context.Background(), id)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if meta.Size != 10 || meta.Attrs["key"] != "a.txt" {
		t.Errorf("Meta = %+v", meta)
	}
	if !meta.Complete {
		t.Errorf("успешная запись обязана быть Complete")
	}

	data, err := get(s, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(data) != "hello tape" {
		t.Errorf("прочитали %q", data)
	}
}

func TestStatMissingIsNotFound(t *testing.T) {
	s := fake.New()
	if _, err := s.Stat(context.Background(), uuid.New()); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("ожидали ErrNotFound, получили %v", err)
	}
}

// FailOn считает вызовы, а не объекты: это позволяет воркеру упасть ровно на
// втором батче, что и проверяют тесты восстановления после сбоя.
func TestFaultFiresOnNthCall(t *testing.T) {
	s := fake.New()
	boom := errors.New("лента кончилась")
	s.InjectFault("put", &fake.Fault{FailOn: 2, Err: boom})

	call := func() error {
		return put(s, uuid.New(), "x", nil).Err
	}

	if err := call(); err != nil {
		t.Fatalf("первый Put не должен был упасть: %v", err)
	}
	if err := call(); !errors.Is(err, boom) {
		t.Fatalf("второй Put: ожидали boom, получили %v", err)
	}
	if err := call(); err != nil {
		t.Errorf("третий Put должен пройти: сбой разовый, а не постоянный; получили %v", err)
	}
	if got := s.Calls("put"); got != 3 {
		t.Errorf("Calls(put) = %d, ожидали 3", got)
	}
}

// Батч атомарен: если xfer упал, половины записанных объектов быть не должно.
// Без этого тест migrator покажет «2 из 3 заархивированы», а на настоящем
// Phobos не будет ни одного.
func TestBatchIsAtomicOnError(t *testing.T) {
	s := fake.New()
	boom := errors.New("xtfer упал")
	s.InjectFault("put", &fake.Fault{FailOn: 1, Err: boom})

	ids := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	items := make([]store.PutItem, 0, len(ids))
	for _, id := range ids {
		items = append(items, store.PutItem{ID: id, Reader: strings.NewReader("payload"), Size: 7})
	}
	for i, r := range s.Put(context.Background(), items, store.PutOptions{}) {
		if !errors.Is(r.Err, boom) {
			t.Errorf("элемент %d: ожидали boom, получили %v", i, r.Err)
		}
	}
	if s.Len() != 0 {
		t.Errorf("на ошибке батча не должно записаться ничего, а записано %d", s.Len())
	}
	if s.Used() != 0 {
		t.Errorf("Used = %d, ожидали 0", s.Used())
	}
}

// Сбой с nil Err имитирует смерть процесса посреди копирования. Замер M0-02:
// объект при этом остаётся в индексе, но copy_status = incomplete. Старая
// версия fake молчала «состояние не изменилось», и тест «не принять неполную
// копию за архивную» было невозможно написать.
func TestCrashLeavesIncompleteCopy(t *testing.T) {
	s := fake.New()
	id := uuid.New()

	s.InjectFault("put", &fake.Fault{FailOn: 1})
	if r := put(s, id, "data", nil); !errors.Is(r.Err, store.ErrIncomplete) {
		t.Errorf("ожидали ErrIncomplete, получили %v", r.Err)
	}
	if s.Len() != 1 {
		t.Fatalf("объект обязан остаться в индексе, Len = %d", s.Len())
	}

	meta, err := s.Stat(context.Background(), id)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if meta.Complete {
		t.Errorf("Stat врал бы: getmd возвращает объект и при неполной копии")
	}
	if _, err := get(s, id); !errors.Is(err, store.ErrIncomplete) {
		t.Errorf("Get неполной копии: ожидали ErrIncomplete, получили %v", err)
	}

	// Успешный повтор дописывает объект — так выглядит восстановление после
	// убийства mover'а.
	s.ResetFaults()
	s.MarkComplete(id)
	if !s.Complete(id) {
		t.Fatalf("после MarkComplete объект обязан стать полным")
	}
	data, err := get(s, id)
	if err != nil || string(data) != "data" {
		t.Errorf("Get после дописки: %q, %v", data, err)
	}
}

// ENOSPC не имеет побочных эффектов: не влезло — не записано ничего.
// Двусмысленность ENOSPC (M0-08: так же отвечает «нет ленты с тегом») —
// отдельный вопрос, здесь важна именно атомарность.
func TestNoSpaceHasNoSideEffects(t *testing.T) {
	s := fake.New()
	s.SetCapacity(10)

	items := []store.PutItem{
		{ID: uuid.New(), Reader: strings.NewReader("12345"), Size: 5},
		{ID: uuid.New(), Reader: strings.NewReader("67890"), Size: 5},
		{ID: uuid.New(), Reader: strings.NewReader("abcde"), Size: 5},
	}
	res := s.Put(context.Background(), items, store.PutOptions{})
	for i, r := range res {
		if !errors.Is(r.Err, store.ErrNoSpace) {
			t.Errorf("элемент %d: ожидали ErrNoSpace, получили %v", i, r.Err)
		}
	}
	if s.Len() != 0 || s.Used() != 0 {
		t.Errorf("ENOSPC не должен ничего оставить: Len=%d Used=%d", s.Len(), s.Used())
	}

	// Тот же батч по частям проходит — значит дело в объёме, а не в данных.
	if r := put(s, uuid.New(), "12345", nil); r.Err != nil {
		t.Fatalf("первый объект должен влезть: %v", r.Err)
	}
}

// Повторная запись того же oid — EEXIST, а не перезапись. Migrator обязан
// исключать уже подтверждённые объекты перед повтором, иначе батч вечно упрётся
// в EEXIST; поведение закреплено в ARCHITECTURE.md, пункт разбора итога батча.
func TestRewriteSameIDIsEEXIST(t *testing.T) {
	s := fake.New()
	id := uuid.New()
	if r := put(s, id, "original", nil); r.Err != nil {
		t.Fatalf("первый Put: %v", r.Err)
	}

	// Второй батч: один oid новый, второй — повтор. Отказывает весь батч.
	a, b := uuid.New(), id
	items := []store.PutItem{
		{ID: a, Reader: strings.NewReader("new"), Size: 3},
		{ID: b, Reader: strings.NewReader("clobber"), Size: 7},
	}
	for i, r := range s.Put(context.Background(), items, store.PutOptions{}) {
		if !errors.Is(r.Err, store.ErrExists) {
			t.Errorf("элемент %d: ожидали ErrExists, получили %v", i, r.Err)
		}
	}
	if s.Len() != 1 {
		t.Errorf("EEXIST отсекает весь батч, ожидали 1 объект, получили %d", s.Len())
	}
	data, err := get(s, id)
	if err != nil || string(data) != "original" {
		t.Errorf("данные существующего объекта не должны измениться: %q, %v", data, err)
	}
}

// Атрибуты проверяются на бюджет LTFS до обращения к ленте: ENOSPC в середине
// xfer неотличим от «нет ленты с тегом» (M2-01, M0-08).
func TestAttrsOverBudgetRejectedBeforeWrite(t *testing.T) {
	s := fake.New()
	id := uuid.New()
	big := map[string]string{"tapegw.dek": strings.Repeat("A", store.AttrBudgetBytes)}

	if r := put(s, id, "data", big); !errors.Is(r.Err, store.ErrAttrTooLarge) {
		t.Errorf("ожидали ErrAttrTooLarge, получили %v", r.Err)
	}
	if s.Len() != 0 {
		t.Errorf("на отказе по атрибутам ничего не записывается, Len = %d", s.Len())
	}
	if s.Calls("put") != 1 {
		t.Errorf("счётчик вызовов = %d, ожидали 1", s.Calls("put"))
	}

	// Граница бюджета: ровно в лимит — проходит.
	ok := map[string]string{"k": strings.Repeat("A", store.AttrBudgetBytes-len(`{"k":""}`))}
	if store.AttrSize(ok) > store.AttrBudgetBytes {
		t.Skipf("границу не удалось подобрать: %d", store.AttrSize(ok))
	}
	if r := put(s, uuid.New(), "data", ok); r.Err != nil {
		t.Errorf("атрибуты ровно в лимит обязаны проходить: %v", r.Err)
	}
}

// Delete уже удалённого объекта — успех. Без этого deleter не идемпотентен
// и будет вечно ретраить.
func TestDeleteIsIdempotent(t *testing.T) {
	s := fake.New()
	id := uuid.New()

	for i := range 3 {
		if r := s.Delete(context.Background(), []uuid.UUID{id}); r[0].Err != nil {
			t.Fatalf("Delete #%d: %v", i+1, r[0].Err)
		}
	}
}

func TestLocateDefaultsToUnsupported(t *testing.T) {
	s := fake.New()
	if _, err := s.Locate(context.Background(), nil); !errors.Is(err, store.ErrNotSupported) {
		t.Errorf("ожидали ErrNotSupported, получили %v", err)
	}
}

// Extents — то, чем закрыли отсутствие Locate: носитель узнаётся при
// архивации (ADR-0008).
func TestExtentsReportsMedia(t *testing.T) {
	s := fake.New()
	id := uuid.New()
	if r := put(s, id, "data", nil); r.Err != nil {
		t.Fatalf("Put: %v", r.Err)
	}
	ext, err := s.Extents(context.Background(), []uuid.UUID{id})
	if err != nil {
		t.Fatalf("Extents: %v", err)
	}
	if len(ext[id]) == 0 {
		t.Errorf("Extents пуст — migrator некуда записать objects.phobos_medium")
	}
}
