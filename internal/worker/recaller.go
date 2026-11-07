package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/vladsimachkov/tapegw/internal/config"
	"github.com/vladsimachkov/tapegw/internal/keys"
	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/mover"
	"github.com/vladsimachkov/tapegw/internal/recall"
	"github.com/vladsimachkov/tapegw/internal/spool"
)

// Recaller-воркер: поднимает запросы restore с ленты в cache (S5).
//
// Один воркер на mover роли recaller этой библиотеки (ADR-0007): mover — это
// драйв recovery-пула, и он возит одну операцию за раз. Остановка — как у
// мигратора: начатое чтение доходит до конца (контекст без отмены), а если
// процесс всё-таки ушёл посреди чтения, строки 'in_progress' вернёт в работу
// lease claim_ttl — колонок воркера у restores нет и продолжения по слоту,
// как у пачек, здесь нет.

// PlanRecallers — mover'ы роли recaller своей библиотеке.
// Пусто при включённом воркере — ошибка на старте: очередь restore, которую
// некому читать, выглядит для клиента вечным «ongoing-request="true"».
func PlanRecallers(cfg *config.Config) ([]MoverTarget, error) {
	var out []MoverTarget
	for drive, mv := range cfg.Phobos.Movers {
		if mv.Role != string(mover.RoleRecaller) || mv.Library != cfg.Phobos.Library {
			continue
		}
		out = append(out, MoverTarget{Drive: drive, Addr: mv.Addr, Node: mv.Node})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf(
			"worker: recaller включён, но в библиотеке %q нет ни одного mover с ролью recaller "+
				"(phobos.movers.<драйв>.role) — восстанавливать некому", cfg.Phobos.Library)
	}
	return out, nil
}

// DialRecaller — mTLS-подключение планировщика восстановления к mover'у.
// Роль проверяется на рукопожатии: подключиться к архивному драйву вместо
// recovery-пула — ошибка конфигурации, а не удача (ADR-0007).
func DialRecaller(ctx context.Context, cfg *config.Config, t MoverTarget) (*mover.Client, error) {
	tlsCfg, err := mover.ClientTLS(
		cfg.Phobos.MoverClient.Cert, cfg.Phobos.MoverClient.Key, cfg.Phobos.MoverClient.CA, t.Drive)
	if err != nil {
		return nil, err
	}
	c, _, err := mover.DialAs(ctx, t.Addr, tlsCfg, mover.RoleRecaller)
	if err != nil {
		return nil, fmt.Errorf("worker: mover %s (%s): %w", t.Drive, t.Addr, err)
	}
	return c, nil
}

// RecallerDeps — всё, что нужно воркерам восстановления.
type RecallerDeps struct {
	Cfg   *config.Config
	Meta  *meta.Meta
	Cache *spool.Spool
	Keys  *keys.Vault
	Log   *slog.Logger
}

// RecallerGroup — запущенные воркеры; Wait ждёт их при остановке.
type RecallerGroup struct {
	wg   sync.WaitGroup
	log  *slog.Logger
	desc []string
}

// StartRecallers поднимает по воркеру на каждый mover из targets. Подключение
// — до возврата, как у StartMigrators: «включено, а mover'а нет» слышно на
// старте процесса, а не по вечно длящемуся restore.
func StartRecallers(ctx context.Context, deps RecallerDeps, targets []MoverTarget) (*RecallerGroup, error) {
	log := deps.Log
	if log == nil {
		log = slog.Default()
	}
	g := &RecallerGroup{log: log}
	for _, t := range targets {
		if _, err := DialRecaller(ctx, deps.Cfg, t); err != nil {
			return nil, err
		}
		id := t.Node + "/" + t.Drive
		g.desc = append(g.desc, id)
		g.wg.Add(1)
		go func(t MoverTarget, id string) {
			defer g.wg.Done()
			runRecaller(ctx, deps, t, id)
		}(t, id)
	}
	log.Info("recaller: воркеры запущены",
		slog.Any("слоты", g.desc),
		slog.Duration("batch_max_wait", deps.Cfg.Recaller.BatchMaxWait),
		slog.Duration("claim_ttl", deps.Cfg.Recaller.ClaimTTL))
	return g, nil
}

// Wait ждёт завершения воркеров не дольше grace. Ложь — «ушли, не дождавшись»:
// начатое чтение ленты продолжится до result_ttl mover'а, а строки очереди
// вернёт lease, ничего не потеряно.
func (g *RecallerGroup) Wait(grace time.Duration) bool {
	done := make(chan struct{})
	go func() { g.wg.Wait(); close(done) }()
	select {
	case <-done:
		return true
	case <-time.After(grace):
		g.log.Warn("recaller: чтение ленты не завершилось за grace, процесс уходит; строки вернёт claim_ttl",
			slog.Duration("grace", grace))
		return false
	}
}

func runRecaller(ctx context.Context, deps RecallerDeps, t MoverTarget, id string) {
	cfg, log := deps.Cfg, deps.Log
	if log == nil {
		log = slog.Default()
	}
	// Чтение начатой пачки не обрывается по сигналу — тот же договор, что у
	// отправки мигратора.
	sendCtx := context.WithoutCancel(ctx)

	for {
		if ctx.Err() != nil {
			return
		}

		cli, err := DialRecaller(ctx, cfg, t)
		if err != nil {
			log.Error("recaller: подключение к mover'у", slog.String("слот", id), slog.String("err", err.Error()))
			select {
			case <-ctx.Done():
				return
			case <-time.After(cfg.Recaller.BatchMaxWait):
			}
			continue
		}

		rc := &recall.Recall{
			Meta:         deps.Meta,
			Get:          cli,
			Cache:        deps.Cache,
			Keys:         deps.Keys,
			BatchMax:     cfg.Recaller.BatchMaxObjects,
			ClaimTTL:     cfg.Recaller.ClaimTTL,
			MaxAttempts:  cfg.Recaller.MaxAttempts,
			RetryBackoff: cfg.Recaller.RetryBackoff,
			Log:          log,
		}
		n, err := rc.Once(sendCtx)
		cli.Close()
		if err != nil {
			log.Error("recaller: проход", slog.String("слот", id), slog.String("err", err.Error()))
		} else if n > 0 {
			log.Info("recaller: восстановлено объектов", slog.String("слот", id), slog.Int("объектов", n))
		}
		// Что-то восстановлено — сразу следующая пачка: очередь не обязана
		// ждать batch_max_wait между пачками, lease покрывает одно чтение.
		// Пусто или сбой — пауза: без неё пустой проход превратился бы в
		// горячий цикл по БД.
		if n == 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(cfg.Recaller.BatchMaxWait):
			}
		}
	}
}
