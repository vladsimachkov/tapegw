//go:build phobos

package phobos_test

import (
	"bytes"
	"context"
	"os"
	"os/exec"
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
func TestInterruptedCopyCanBeReplaced(t *testing.T) {
	requirePhobos(t)
	ctx := context.Background()
	st, err := phobos.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	const big = 256 << 20 // 256 MiB: пишется секунды, убить успеваем
	src, err := os.CreateTemp(t.TempDir(), "big")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(src.Name())
	if err := src.Truncate(big); err != nil {
		t.Fatal(err)
	}

	id := uuid.New()
	kctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	cmd := exec.CommandContext(kctx, "phobos", "put", "-f", "dir", "-L", "dirlib",
		"-l", "raid1", "-P", "repl_count=1", src.Name(), id.String())
	out, _ := cmd.CombinedOutput()
	if kctx.Err() == nil {
		t.Skipf("запись успела завершиться, обрыва не вышло: %s", out)
	}

	// 1. объект после обрыва существует и помечен неполным.
	meta, err := st.Stat(ctx, id)
	if err != nil {
		t.Fatalf("после обрыва stat: %v — объект должен существовать, но быть неполным", err)
	}
	if meta.Complete {
		t.Fatalf("копия полна сразу после обрыва: %+v", meta.Extents)
	}

	// 2. обычный delete обязан снять неполную копию.
	if got := st.Delete(ctx, []uuid.UUID{id}); got[0].Err != nil {
		t.Fatalf("delete неполной копии: %v — путь «incomplete -> удалить -> отправить снова» закрыт", got[0].Err)
	}

	// 3. повторный put с тем же oid проходит: ни EEXIST, ни висящего лока.
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
}
