package store_test

import (
	"strings"
	"testing"

	"github.com/vladsimachkov/tapegw/internal/store"
)

// TestCheckExtentsRejectsNotSync — причина «копия не дописана» должна
// отличаться от «не то число реплик» и от «размер не тот». На семействе dir
// состояние вне sync не наблюдается (экстенты регистрируются по завершении
// записи), поэтому проверка покрывается здесь, а на ленте — в 1a-3.
func TestCheckExtentsRejectsNotSync(t *testing.T) {
	err := store.CheckExtents([]store.Extent{{Size: 10, Media: "T1", State: "pending"}}, 10, 1)
	if err == nil {
		t.Fatal("копия в pending принята как целая")
	}
	if !strings.Contains(err.Error(), "sync") {
		t.Errorf("причина %q: хотим указание на sync", err)
	}
	if strings.Contains(err.Error(), "repl_count") {
		t.Errorf("причина %q: число реплик тут ни при чём", err)
	}
}
