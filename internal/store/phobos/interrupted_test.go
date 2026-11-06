//go:build phobos

package phobos_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/store"
	"github.com/vladsimachkov/tapegw/internal/store/phobos"
)

// TestInterruptedCopyCanBeReplaced — цепочка, на которой держится разбор EEXIST:
// stat → incomplete → delete → отправить снова.
//
// На fake она работает по построению. На настоящем Phobos надо убедиться, что
// обычный phobos_delete снимает объект с единственной неполной копией: в
// спайке M0-02 отдельной функции для незавершённых копий среди экспортированных
// не нашлось. Если обычный delete не справляется — это блокер пути
// «incomplete → удалить и отправить снова», и узнать это надо здесь, а не на
// ленте.
//
// Обрыв воспроизводится тем же способом, что в спайке: процесс убивается посреди
// записи большого файла.
// Обрыв бывает в разных фазах, и Phobos оставляет разное состояние:
//
//   - ранний: строка объекта уже есть, экстентов ещё нет (CLI печатает пусто
//     при rc=0);
//   - поздний: экстент уже писался, но остался не в sync — именно эта фаза
//     ближе к обрыву при записи пачки на 2–4 GiB.
//
// Замер на ci-dir: на семействе dir поздний обрыв (3 с записи 4 GiB) даёт ту же
// картину, что и ранний, — getmd пусто, экстентов нет. Вывод: Phobos регистрирует
// экстенты по завершении записи, и состояние «экстент есть, но не sync» на dir
// не воспроизводится вовсе. Оно ожидается на ленте, где копия проходит через
// промежуточные состояния, — проверка переходит в 1a-3 (mhVTL).
//
// Оба обязаны приводить к одному: delete -> put проходит.
func TestInterruptedCopyCanBeReplaced(t *testing.T) {
	requirePhobos(t)
	ctx := context.Background()
	st, err := phobos.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	for _, phase := range []struct {
		name string
		size int64
		kill time.Duration
	}{
		{"ранний обрыв", 256 << 20, 300 * time.Millisecond},
		{"поздний обрыв", 4 << 30, 3 * time.Second},
	} {
		t.Run(phase.name, func(t *testing.T) {
			src, err := os.CreateTemp(t.TempDir(), "big")
			if err != nil {
				t.Fatal(err)
			}
			defer os.Remove(src.Name())
			// Файл разреженный: реальных байт на диске не плодим, читает их Phobos.
			if err := src.Truncate(phase.size); err != nil {
				t.Fatal(err)
			}

			id := uuid.New()
			kctx, cancel := context.WithTimeout(ctx, phase.kill)
			defer cancel()
			cmd := exec.CommandContext(kctx, "phobos", "put", "-f", "dir", "-L", "dirlib",
				"-l", "raid1", "-P", "repl_count=1", src.Name(), id.String())
			out, _ := cmd.CombinedOutput()
			if kctx.Err() == nil {
				t.Skipf("запись успела завершиться, обрыва не вышло: %s", out)
			}

			// 1. Объект обязан быть НАЙДЕН и помечен неполным. Если бы Stat
			// вернул ErrNotFound, resolveByStat решил бы «объекта нет»,
			// отправил его снова и получил EEXIST — то самое зацикливание.
			meta, err := st.Stat(ctx, id)
			if errors.Is(err, store.ErrNotFound) {
				t.Fatal("Stat вернул ErrNotFound после обрыва: resolveByStat решит, что объекта нет, и зациклится")
			}
			if err != nil {
				t.Fatalf("Stat после обрыва: %v", err)
			}
			if meta.Complete {
				t.Fatalf("копия полна сразу после обрыва: %+v", meta.Extents)
			}
			// 2. План такую копию не принимает — и говорит почему.
			if err := store.CheckExtents(meta.Extents, phase.size, 1); err == nil {
				t.Error("CheckExtents принял неполную копию")
			} else if len(meta.Extents) > 0 && !strings.Contains(err.Error(), "sync") {
				t.Errorf("причина %q: при экстентах не sync хотим указание на sync", err)
			}

			// 3. Обычный delete обязан снять неполную копию...
			if got := st.Delete(ctx, []uuid.UUID{id}); got[0].Err != nil {
				t.Fatalf("delete неполной копии: %v — путь «incomplete -> удалить -> отправить снова» закрыт", got[0].Err)
			}

			// 4. ...и повторный put с тем же oid проходит: ни EEXIST, ни лока.
			body := bytes.Repeat([]byte{0x5A}, 4096)
			small, err := os.CreateTemp(t.TempDir(), "small")
			if err != nil {
				t.Fatal(err)
			}
			defer os.Remove(small.Name())
			if _, err := small.Write(body); err != nil {
				t.Fatal(err)
			}
			if err := small.Sync(); err != nil {
				t.Fatal(err)
			}
			res := st.Put(ctx, []store.PutItem{{ID: id, File: small, Size: int64(len(body))}},
				store.PutOptions{Family: "dir", Library: "dirlib", Layout: "raid1",
					LayoutParams: map[string]string{"repl_count": "1"}})
			if res[0].Err != nil {
				t.Fatalf("повторный put после delete: %v", res[0].Err)
			}
		})
	}
}
