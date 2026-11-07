//go:build !phobos

package main

import (
	"context"
	"errors"

	"github.com/vladsimachkov/tapegw/internal/config"
	"github.com/vladsimachkov/tapegw/internal/store"
)

// openStore без тега `phobos` отказывает, а не поднимает fake: mover на fake
// в бою — это молчаливая подмена ленты, ровно то, чего требует избегать
// правило 4. Бинарь без cgo собирается (правило 2), но работать не может.
func openStore(context.Context, *config.Config) (store.Store, error) {
	return nil, errors.New("tapegw-mover собран без поддержки Phobos: соберите с -tags phobos")
}
