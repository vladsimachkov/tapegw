//go:build phobos

package phobos_test

import (
	"context"
	"testing"

	"github.com/vladsimachkov/tapegw/internal/store"
	"github.com/vladsimachkov/tapegw/internal/store/phobos"
	"github.com/vladsimachkov/tapegw/internal/store/storetest"
)

// TestStoreScenarios — тот же набор, что прогоняется на fake, на настоящем
// Phobos в ci-dir. Именно здесь ловится расхождение модели и реальности.
func TestStoreScenarios(t *testing.T) {
	requirePhobos(t)
	storetest.Run(t, func(t *testing.T) storetest.Env {
		st, err := phobos.Open(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		return storetest.Env{
			Store: st,
			Put: store.PutOptions{Family: "dir", Library: "dirlib", Layout: "raid1",
				LayoutParams: map[string]string{"repl_count": "1"}},
			Close: func() { st.Close() },
		}
	})
}
