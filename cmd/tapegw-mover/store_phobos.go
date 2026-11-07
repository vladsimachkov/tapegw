//go:build phobos

package main

import (
	"context"

	"github.com/vladsimachkov/tapegw/internal/config"
	"github.com/vladsimachkov/tapegw/internal/store"
	"github.com/vladsimachkov/tapegw/internal/store/phobos"
)

// openStore поднимает libphobos: phobos_init выполняется внутри
// горутины-владельца (ADR-0007), конфиг Phobos берётся из phobos.mover.phobos_config,
// тот же файл получает CLI extent list — иначе «записали одним конфигом,
// ищем другим».
func openStore(ctx context.Context, cfg *config.Config) (store.Store, error) {
	return phobos.OpenWithConfig(ctx, cfg.Phobos.Mover.PhobosConfig)
}
