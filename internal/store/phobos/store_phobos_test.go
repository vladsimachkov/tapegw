//go:build phobos

package phobos_test

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/store"
	"github.com/vladsimachkov/tapegw/internal/store/phobos"
)

// Тесты под тегом phobos: без настоящей библиотеки они ничего не утверждают.
// Прогон — на стенде (docs/ops/test-server.md), family dir.

func requirePhobos(t *testing.T) {
	t.Helper()
	if os.Getenv("TAPEGW_PHOBOS_TEST") != "1" {
		t.Skip("нужен настоящий Phobos: TAPEGW_PHOBOS_TEST=1 и стенд ci-dir")
	}
}

// dirOpts — значения, на которых спайк M0-02 писал на dir с этого же стенда:
// семейство dir, библиотека dirlib, layout raid1 с repl_count=1.
func dirOpts() store.PutOptions {
	return store.PutOptions{Family: "dir", Library: "dirlib", Layout: "raid1",
		LayoutParams: map[string]string{"repl_count": "1"}}
}

func TestPutThenStat(t *testing.T) {
	requirePhobos(t)
	ctx := context.Background()

	st, err := phobos.Open(ctx)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()

	body := []byte("содержимое объекта для 1a-1")
	f, err := os.Create(filepath.Join(t.TempDir(), "obj"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write(body); err != nil {
		t.Fatal(err)
	}

	id := uuid.New()
	res := st.Put(ctx, []store.PutItem{{ID: id, File: f, Size: int64(len(body)),
		Attrs: map[string]string{"tapegw.test": "1a-1"}}}, dirOpts())
	if len(res) != 1 || res[0].Err != nil {
		t.Fatalf("put: %v", res[0].Err)
	}

	meta, err := st.Stat(ctx, id)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if meta.Size != int64(len(body)) {
		t.Errorf("размер %d, ждали %d", meta.Size, len(body))
	}
	if !meta.Complete {
		t.Error("копия помечена неполной сразу после записи")
	}

	if _, err := st.Stat(ctx, uuid.New()); err != store.ErrNotFound {
		t.Errorf("чужой oid: %v, ждали ErrNotFound", err)
	}
}

// Спайк M0-13: все вызовы C обязаны приходить из одного потока ОС. Проверка
// идёт по tid, записанному на стороне C, а не по предположению о планировщике Go.
func TestAllCallsFromOneThread(t *testing.T) {
	requirePhobos(t)
	ctx := context.Background()

	st, err := phobos.Open(ctx)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()

	const n = 16
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := []byte(fmt.Sprintf("объект %d", i))
			f, err := os.Create(filepath.Join(t.TempDir(), fmt.Sprintf("o%d", i)))
			if err != nil {
				errs <- err
				return
			}
			defer f.Close()
			if _, err := f.Write(body); err != nil {
				errs <- err
				return
			}
			id := uuid.New()
			res := st.Put(ctx, []store.PutItem{{ID: id, File: f, Size: int64(len(body))}},
				store.PutOptions{Family: "dir"})
			if res[0].Err != nil {
				errs <- res[0].Err
				return
			}
			if _, err := st.Stat(ctx, id); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("вызов: %v", err)
	}

	tids := st.TIDs()
	// Требование «не меньше N» по числу put, а не put+stat: tid пишется на входе
	// в C, поэтому проверка потока не зависит от того, удался ли вызов.
	if len(tids) < n {
		t.Fatalf("записано %d вызовов, ожидали не меньше %d", len(tids), n)
	}
	seen := map[int64]bool{}
	for _, tid := range tids {
		seen[tid] = true
	}
	if len(seen) != 1 {
		t.Errorf("вызовы C пришли из %d потоков ОС (%v): горутина-владельца не удерживает поток", len(seen), seen)
	}
	t.Logf("M0-13: %d вызовов C, все из tid %d", len(tids), tids[0])
}

// TestReplicaCountIsOne — проверка «ровно одна реплика», не требующая ленты.
//
// extent list у семейства dir расширений не показывает (выводит только oid),
// поэтому считаем напрямую: носитель dir — обычный каталог, реплика объекта —
// файл в нём. На стенде два носителя, поэтому:
//   - put с repl_count=1 оставляет файл ровно в одном каталоге;
//   - put без параметров layout (raid1 по умолчанию берёт две реплики) —
//     в обоих.
//
// Второй случай заодно доказывает, что параметры действительно доходят до Phobos:
// если бы они игнорировались, обе строки вели себя одинаково.
func TestReplicaCountIsOne(t *testing.T) {
	requirePhobos(t)
	media := strings.Split(os.Getenv("TAPEGW_PHOBOS_DIR_MEDIA"), ",")
	if len(media) < 2 {
		t.Skip("нужно два носителя dir: TAPEGW_PHOBOS_DIR_MEDIA=dir1,dir2")
	}
	ctx := context.Background()
	st, err := phobos.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	// Размер выбираем некруглый и уникальный для теста: считаем файлом ровно
	// то, что записали, и не должны поймать чужой объект того же размера.
	size := int64(16391)

	put := func(name string, params map[string]string) {
		f, err := os.CreateTemp(t.TempDir(), "obj")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(f.Name())
		if _, err := f.Write(bytes.Repeat([]byte{0xAB}, int(size))); err != nil {
			t.Fatal(err)
		}
		if err := f.Sync(); err != nil {
			t.Fatal(err)
		}
		res := st.Put(ctx, []store.PutItem{{ID: uuid.New(), File: f, Size: size}},
			store.PutOptions{Family: "dir", Library: "dirlib", Layout: "raid1",
				LayoutParams: params})
		if len(res) != 1 {
			t.Fatalf("%s: результатов %d, ждём 1", name, len(res))
		}
		if res[0].Err != nil {
			t.Fatalf("%s: %v", name, res[0].Err)
		}
	}

	put("с repl_count=1", map[string]string{"repl_count": "1"})
	got := 0
	for _, m := range media {
		got += countFilesOfSize(m, size)
	}
	if got != 1 {
		t.Errorf("с repl_count=1: реплик %d, хотим 1", got)
	}

	put("без параметров layout", nil)
	got = 0
	for _, m := range media {
		got += countFilesOfSize(m, size)
	}
	if got != 3 { // одна из первого put плюс две новые
		t.Errorf("без параметров: файлов %d, хотим 3 (1 + 2 реплики)", got)
	}
}

func countFilesOfSize(root string, size int64) int {
	n := 0
	filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		if info, err := d.Info(); err == nil && info.Size() == size {
			n++
		}
		return nil
	})
	return n
}
