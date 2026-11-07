package fake_test

import (
	"testing"

	"github.com/vladsimachkov/tapegw/internal/store"
	"github.com/vladsimachkov/tapegw/internal/store/fake"
	"github.com/vladsimachkov/tapegw/internal/store/storetest"
)

// TestStoreScenarios — fake обязан проходить тот же набор, что и настоящий
// Phobos. Расхождение здесь — это не «особенность fake», а будущий сюрприз на
// стенде.
func TestStoreScenarios(t *testing.T) {
	storetest.Run(t, func(t *testing.T) storetest.Env {
		return storetest.Env{
			Store: fake.New(),
			Put:   store.PutOptions{Family: "dir", Library: "dirlib", Layout: "raid1"},
		}
	})
}
