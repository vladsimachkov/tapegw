//go:build phobos

package phobos_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
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
		Attrs: map[string]string{"tapegw.test": "1a-1"}}}, store.PutOptions{Family: "dir"})
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
	if len(tids) < 2*n {
		t.Fatalf("записано %d вызовов, ожидали не меньше %d", len(tids), 2*n)
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
