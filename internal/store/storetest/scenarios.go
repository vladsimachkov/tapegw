// Package storetest — общий набор сценариев для любой реализации store.Store.
//
// Смысл один: один и тот же набор проходит и на fake, и на настоящем Phobos.
// Расхождение между ними — не сюрприз на стенде, а красный тест здесь. Поэтому
// сценарии формулируются так, как их увидит воркер, и не знают, какая
// реализация под ними.
package storetest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/store"
)

// Env — реализация вместе со способом правильно её вызывать.
type Env struct {
	Store store.Store
	// Put — параметры записи, на которых реализация обязана работать: для
	// Phobos это family/library/layout и repl_count=1, для fake — что угодно.
	Put store.PutOptions
	// Close освобождает ресурсы реализации (настоящий Phobos — сокет и
	// горутину-владельца).
	Close func()
}

// Run прогоняет набор. newEnv вызывается на каждый подтест отдельно: сценарии
// не должны зависеть по состоянию друг от друга.
func Run(t *testing.T, newEnv func(*testing.T) Env) {
	t.Helper()

	env := func(t *testing.T) Env {
		t.Helper()
		e := newEnv(t)
		if e.Close != nil {
			t.Cleanup(e.Close)
		}
		return e
	}

	t.Run("записанное находится и цело", func(t *testing.T) {
		e := env(t)
		ctx := context.Background()
		body := bytes.Repeat([]byte("tapegw"), 1024) // 6 KiB
		id := uuid.New()

		if err := put(t, e, id, body); err != nil {
			t.Fatalf("put: %v", err)
		}
		meta, err := e.Store.Stat(ctx, id)
		if err != nil {
			t.Fatalf("stat: %v", err)
		}
		if !meta.Complete {
			t.Errorf("свежая копия помечена неполной: %+v", meta.Extents)
		}
		// План: одна реплика, размер тела. Проверка та же, что у migrator.
		if err := store.CheckExtents(meta.Extents, int64(len(body)), 1); err != nil {
			t.Errorf("экстенты не сходятся с планом: %v", err)
		}
	})

	t.Run("чтение возвращает ровно объект", func(t *testing.T) {
		e := env(t)
		ctx := context.Background()
		body := bytes.Repeat([]byte("roundtrip"), 512) // 4608 байт
		id := uuid.New()
		if err := put(t, e, id, body); err != nil {
			t.Fatalf("put: %v", err)
		}

		dst := tmpFile(t)
		// Файл заведомо грязный: хранилище обязано оставить только объект,
		// иначе «читает чисто» можно пройти на пустом выводе.
		if _, err := dst.Write(bytes.Repeat([]byte{0xFF}, 8192)); err != nil {
			t.Fatal(err)
		}
		if err := dst.Sync(); err != nil {
			t.Fatal(err)
		}
		if res := e.Store.Get(ctx, []store.GetItem{{ID: id, File: dst}}); res[0].Err != nil {
			t.Fatalf("get: %v", res[0].Err)
		}
		if _, err := dst.Seek(0, io.SeekStart); err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(dst)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, body) {
			t.Errorf("прочитано %d байт из %d; префикс совпадает: %v",
				len(got), len(body), bytes.HasPrefix(got, body))
		}
	})

	t.Run("неизвестного объекта нет", func(t *testing.T) {
		e := env(t)
		if _, err := e.Store.Stat(context.Background(), uuid.New()); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("stat неизвестного id вернул %v, хотим ErrNotFound", err)
		}
	})

	t.Run("повторное удаление не ошибка", func(t *testing.T) {
		e := env(t)
		ctx := context.Background()
		id := uuid.New()
		if err := put(t, e, id, []byte("delete me")); err != nil {
			t.Fatalf("put: %v", err)
		}
		for i := 1; i <= 2; i++ {
			if res := e.Store.Delete(ctx, []uuid.UUID{id}); res[0].Err != nil {
				t.Errorf("удаление %d: %v", i, res[0].Err)
			}
		}
		if _, err := e.Store.Stat(ctx, id); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("после удаления stat вернул %v, хотим ErrNotFound", err)
		}
	})

	t.Run("повторная запись тем же oid отклонена", func(t *testing.T) {
		e := env(t)
		id := uuid.New()
		if err := put(t, e, id, []byte("первая")); err != nil {
			t.Fatalf("put: %v", err)
		}
		// Phobos отвечает EEXIST, и fake обязан вести себя так же: иначе
		// migrator не проверит на fake разбор «уже есть».
		if err := put(t, e, id, []byte("вторая")); !errors.Is(err, store.ErrExists) {
			t.Errorf("вторая запись вернула %v, хотим ErrExists", err)
		}
	})

	t.Run("носитель сообщается", func(t *testing.T) {
		e := env(t)
		ctx := context.Background()
		id := uuid.New()
		if err := put(t, e, id, []byte("extents")); err != nil {
			t.Fatalf("put: %v", err)
		}
		ext, err := e.Store.Extents(ctx, []uuid.UUID{id})
		if err != nil {
			t.Fatalf("extents: %v", err)
		}
		if len(ext[id]) != 1 {
			t.Fatalf("носителей %d, ждём 1 (план — одна реплика)", len(ext[id]))
		}
		if ext[id][0] == "" {
			t.Error("имя носителя пустое: archive_copies.medium заполнить будет нечем")
		}
	})
}

func put(t *testing.T, e Env, id uuid.UUID, body []byte) error {
	t.Helper()
	f := tmpFile(t)
	if _, err := f.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	res := e.Store.Put(context.Background(),
		[]store.PutItem{{ID: id, File: f, Size: int64(len(body))}}, e.Put)
	if len(res) != 1 {
		t.Fatalf("результатов %d, ждём 1", len(res))
	}
	return res[0].Err
}

func tmpFile(t *testing.T) *os.File {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "storetest")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Remove(f.Name()) })
	return f
}
