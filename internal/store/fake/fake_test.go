package fake_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/store"
	"github.com/vladsimachkov/tapegw/internal/store/fake"
)

// Смысл этих тестов не в том, что fake что-то хранит, а в том, что он умеет
// предсказуемо отказывать: правило 7 из AGENTS.md требует для каждого воркера
// тест «упасть после шага N, перезапустить, сверить».

func tempFile(t *testing.T, content string) *os.File {
	t.Helper()
	f, err := os.Create(filepath.Join(t.TempDir(), "obj"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	if _, err := f.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}
	return f
}

func TestPutGetRoundTrip(t *testing.T) {
	s := fake.New()
	id := uuid.New()
	f := tempFile(t, "hello tape")

	res := s.Put(context.Background(),
		[]store.PutItem{{ID: id, File: f, Size: 10, Attrs: map[string]string{"key": "a.txt"}}},
		store.PutOptions{Family: "dir"})
	if res[0].Err != nil {
		t.Fatalf("Put: %v", res[0].Err)
	}
	if res[0].Version != 1 {
		t.Errorf("Version = %d", res[0].Version)
	}

	meta, err := s.Stat(context.Background(), id)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if meta.Size != 10 || meta.Attrs["key"] != "a.txt" {
		t.Errorf("Meta = %+v", meta)
	}

	out := tempFile(t, "")
	get := s.Get(context.Background(), []store.GetItem{{ID: id, File: out}})
	if get[0].Err != nil {
		t.Fatalf("Get: %v", get[0].Err)
	}
	data, _ := os.ReadFile(out.Name())
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
		f := tempFile(t, "x")
		r := s.Put(context.Background(), []store.PutItem{{ID: uuid.New(), File: f, Size: 1}}, store.PutOptions{})
		return r[0].Err
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

// Fault с nil Err имитирует «процесс умер посреди вызова»: состояние Phobos
// не изменилось, но и результат не получен. Именно так проверяется идемпотентность
// migrator-а после kill -9.
func TestCrashLikeFaultLeavesStateUntouched(t *testing.T) {
	s := fake.New()
	id := uuid.New()
	f := tempFile(t, "data")

	s.InjectFault("put", &fake.Fault{FailOn: 1})
	res := s.Put(context.Background(), []store.PutItem{{ID: id, File: f, Size: 4}}, store.PutOptions{})
	if res[0].Err != nil {
		t.Errorf("при «падении» ошибку возвращать не надо")
	}
	if s.Len() != 0 {
		t.Errorf("объект появился несмотря на «падение»: %d", s.Len())
	}

	s.ResetFaults()
	if res := s.Put(context.Background(), []store.PutItem{{ID: id, File: f, Size: 4}}, store.PutOptions{}); res[0].Err != nil {
		t.Fatalf("повтор после сбоя: %v", res[0].Err)
	}
	if s.Len() != 1 {
		t.Errorf("после повтора ожидали 1 объект, получили %d", s.Len())
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
