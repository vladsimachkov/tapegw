package fake_test

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/store"
	"github.com/vladsimachkov/tapegw/internal/store/fake"
)

// putFile — файл с данными для PutItem. Хранилище принимает открытые файлы,
// поэтому и тест готовит файл, а не bytes.Reader: иначе fake и настоящая
// реализация принимали бы разное.
func putFile(t *testing.T, body string) *os.File {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "put")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	if _, err := f.WriteString(body); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	return f
}

// getFile — файл для GetItem, из которого потом читают результат.
func getFile(t *testing.T) *os.File {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "get")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return f
}

func readBack(t *testing.T, f *os.File) string {
	t.Helper()
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// Смысл этих тестов не в том, что fake что-то хранит, а в том, что он
// предсказуемо отказывает **как настоящий Phobos**. Тесты migrator на fake не
// должны проходить там, где на ленте всё упало, поэтому здесь прямо закреплены
// четыре поведения, про которые есть замеры спайков: атомарность батча,
// неполная копия после обрыва (M0-02), ENOSPC без побочных эффектов и EEXIST
// на повторную запись.

func put(t *testing.T, s *fake.Store, id uuid.UUID, body string, attrs map[string]string) store.Result {
	return s.Put(context.Background(),
		[]store.PutItem{{ID: id, File: putFile(t, body), Size: int64(len(body)), Attrs: attrs}},
		store.PutOptions{Family: "dir"})[0]
}

func get(t *testing.T, s *fake.Store, id uuid.UUID) ([]byte, error) {
	dst := getFile(t)
	r := s.Get(context.Background(), []store.GetItem{{ID: id, File: dst}})[0]
	return []byte(readBack(t, dst)), r.Err
}

func TestPutGetRoundTrip(t *testing.T) {
	s := fake.New()
	id := uuid.New()

	res := put(t, s, id, "hello tape", map[string]string{"key": "a.txt"})
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

	data, err := get(t, s, id)
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
		return put(t, s, uuid.New(), "x", nil).Err
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
		items = append(items, store.PutItem{ID: id, File: putFile(t, "payload"), Size: 7})
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
	if r := put(t, s, id, "data", nil); !errors.Is(r.Err, store.ErrIncomplete) {
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
	if _, err := get(t, s, id); !errors.Is(err, store.ErrIncomplete) {
		t.Errorf("Get неполной копии: ожидали ErrIncomplete, получили %v", err)
	}

	// Успешный повтор дописывает объект — так выглядит восстановление после
	// убийства mover'а.
	s.ResetFaults()
	s.MarkComplete(id)
	if !s.Complete(id) {
		t.Fatalf("после MarkComplete объект обязан стать полным")
	}
	data, err := get(t, s, id)
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
		{ID: uuid.New(), File: putFile(t, "12345"), Size: 5},
		{ID: uuid.New(), File: putFile(t, "67890"), Size: 5},
		{ID: uuid.New(), File: putFile(t, "abcde"), Size: 5},
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
	if r := put(t, s, uuid.New(), "12345", nil); r.Err != nil {
		t.Fatalf("первый объект должен влезть: %v", r.Err)
	}
}

// Повторная запись того же oid — EEXIST, а не перезапись. Migrator обязан
// исключать уже подтверждённые объекты перед повтором, иначе батч вечно упрётся
// в EEXIST; поведение закреплено в ARCHITECTURE.md, пункт разбора итога батча.
func TestRewriteSameIDIsEEXIST(t *testing.T) {
	s := fake.New()
	id := uuid.New()
	if r := put(t, s, id, "original", nil); r.Err != nil {
		t.Fatalf("первый Put: %v", r.Err)
	}

	// Второй батч: один oid новый, второй — повтор. Отказывает весь батч.
	a, b := uuid.New(), id
	items := []store.PutItem{
		{ID: a, File: putFile(t, "new"), Size: 3},
		{ID: b, File: putFile(t, "clobber"), Size: 7},
	}
	for i, r := range s.Put(context.Background(), items, store.PutOptions{}) {
		if !errors.Is(r.Err, store.ErrExists) {
			t.Errorf("элемент %d: ожидали ErrExists, получили %v", i, r.Err)
		}
	}
	if s.Len() != 1 {
		t.Errorf("EEXIST отсекает весь батч, ожидали 1 объект, получили %d", s.Len())
	}
	data, err := get(t, s, id)
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

	if r := put(t, s, id, "data", big); !errors.Is(r.Err, store.ErrAttrTooLarge) {
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
	if r := put(t, s, uuid.New(), "data", ok); r.Err != nil {
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

// Extents — единственный способ узнать носитель: Locate в TLC-протоколе нет,
// носитель сообщается при архивации (ADR-0008).
func TestExtentsReportsMedia(t *testing.T) {
	s := fake.New()
	id := uuid.New()
	if r := put(t, s, id, "data", nil); r.Err != nil {
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

// Смещение в файле — контракт хранилища, а не вежливость вызывающего. Файл
// буфера к моменту записи уже прочитан до конца (по нему считают SHA-256), и
// если реализация не перематывает, на носитель уходит ноль байт вместо объекта.
func TestPutRewindsFile(t *testing.T) {
	s := fake.New()
	id := uuid.New()
	f := putFile(t, "полный объект")
	if _, err := io.ReadAll(f); err != nil {
		t.Fatal(err) // читаем до конца — как mover перед записью
	}

	if r := s.Put(context.Background(),
		[]store.PutItem{{ID: id, File: f, Size: 13}}, store.PutOptions{Family: "dir"})[0]; r.Err != nil {
		t.Fatalf("put: %v", r.Err)
	}
	got, err := get(t, s, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got) != "полный объект" {
		t.Errorf("сохранено %q: файл не перемотан, записан хвост или пустота", got)
	}
}

// Файл под результат обязан быть обнулён: иначе остатки прошлых данных
// смешаются с объектом, и лишний хвост не виден ни по размеру, ни по сумме,
// посчитанной по файлу.
func TestGetTruncatesFile(t *testing.T) {
	s := fake.New()
	id := uuid.New()
	if r := put(t, s, id, "короткий", nil); r.Err != nil {
		t.Fatalf("put: %v", r.Err)
	}

	dst := getFile(t)
	if _, err := dst.WriteString("мусор от прошлой операции"); err != nil {
		t.Fatal(err)
	}
	if r := s.Get(context.Background(), []store.GetItem{{ID: id, File: dst}})[0]; r.Err != nil {
		t.Fatalf("get: %v", r.Err)
	}
	if got := readBack(t, dst); got != "короткий" {
		t.Errorf("в файле %q: хвост прошлых данных не обрезан", got)
	}
}

// TestIncompleteMatchesDirSemantics — неполная копия в fake устроена так же,
// как на семействе dir: объект есть, экстентов нет, Complete = false. Иначе
// общий набор сценариев проверял бы на fake одно, а на настоящем Phobos — другое.
func TestIncompleteMatchesDirSemantics(t *testing.T) {
	ctx := context.Background()
	s := fake.New()
	s.InjectFault("put", &fake.Fault{FailOn: 1})

	id := uuid.New()
	res := s.Put(ctx, []store.PutItem{{ID: id, File: putFile(t, "abc"), Size: 3}},
		store.PutOptions{Family: "dir"})
	if res[0].Err == nil {
		t.Fatal("ожидался обрыв")
	}

	meta, err := s.Stat(ctx, id)
	if err != nil {
		t.Fatalf("объект после обрыва обязан находиться: %v", err)
	}
	if meta.Complete {
		t.Error("неполная копия помечена целой")
	}
	if len(meta.Extents) != 0 {
		t.Errorf("экстенты неполной копии: %+v — на dir их нет вовсе", meta.Extents)
	}
}

// TestMarkPendingIsExplicit — состояние «экстент есть, но не sync» на dir
// невоспроизводимо, поэтому в fake оно включается явно и проверяется здесь,
// а на ленте станет обычным сценарием в 1a-3.
func TestMarkPendingIsExplicit(t *testing.T) {
	ctx := context.Background()
	s := fake.New()
	id := uuid.New()
	if res := s.Put(ctx, []store.PutItem{{ID: id, File: putFile(t, "abc"), Size: 3}},
		store.PutOptions{Family: "dir"}); res[0].Err != nil {
		t.Fatal(res[0].Err)
	}

	s.MarkPending(id)
	meta, err := s.Stat(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Complete {
		t.Error("pending-копия помечена целой")
	}
	if len(meta.Extents) != 1 || meta.Extents[0].State != "pending" {
		t.Fatalf("экстенты: %+v", meta.Extents)
	}
	err = store.CheckExtents(meta.Extents, 3, 1)
	if err == nil || !strings.Contains(err.Error(), "sync") {
		t.Errorf("CheckExtents: %v — хотим отказ с указанием на sync", err)
	}
}
