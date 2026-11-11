package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/vladsimachkov/tapegw/internal/config"
	"github.com/vladsimachkov/tapegw/internal/meta"
)

// Силер — воркер запечатывания сегментов журнала метаданных (миграция 0029,
// ADR-0023 п. 3–5).
//
// Он не трогает Phobos и не пишет файлы: силер превращает строки journal_events
// в запечатанные диапазоны с плотной нумерацией. Запись объекта сегмента на
// ленту — следующий шаг, и порядок «файл + fsync → commit» (правило 6) появится
// там; здесь каждая транзакция закрывается до возврата, а висящей транзакции на
// время чужого вызова нет вовсе (правило 13).
//
// Два прохода с разной ценой. Обычный — каждые interval, читает только события
// дальше рубежа тенанта. Проверка опоздавших — каждые sweep_interval, ищет
// события ниже рубежа (их транзакция была в полёте во время запечатывания);
// она стоит просмотра истории тенанта, поэтому редкая.

// SealerDeps — всё, что нужно силеру.
type SealerDeps struct {
	Cfg  *config.Config
	Meta *meta.Meta
	Log  *slog.Logger
}

// SealerGroup — запущенный силер; Wait ждёт его при остановке.
type SealerGroup struct {
	wg  sync.WaitGroup
	log *slog.Logger
}

// StartSealer поднимает единственный горутинный цикл запечатывания. Первый
// проход — сразу: процесс мог не работать, и события ждут с момента запуска.
func StartSealer(ctx context.Context, deps SealerDeps) *SealerGroup {
	log := deps.Log
	if log == nil {
		log = slog.Default()
	}
	g := &SealerGroup{log: log}
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		runSealer(ctx, deps, log)
	}()
	log.Info("sealer: запечатывание журнала включено",
		slog.Duration("interval", deps.Cfg.Journal.Sealer.Interval),
		slog.Duration("sweep_interval", deps.Cfg.Journal.Sealer.SweepInterval),
		slog.Int("segment_max_events", deps.Cfg.Journal.Segment.MaxEvents))
	return g
}

// Wait ждёт завершения прохода не дольше grace. Ложь означает «ушли, не
// дождавшись»: незапечатанные события остаются незапечатанными — это не потеря
// (они в БД), их запечатает следующий запуск.
func (g *SealerGroup) Wait(grace time.Duration) bool {
	done := make(chan struct{})
	go func() { g.wg.Wait(); close(done) }()
	select {
	case <-done:
		return true
	case <-time.After(grace):
		g.log.Warn("sealer: проход не завершился за grace, процесс уходит; остаток запечатает следующий",
			slog.Duration("grace", grace))
		return false
	}
}

func runSealer(ctx context.Context, deps SealerDeps, log *slog.Logger) {
	tick := time.NewTicker(deps.Cfg.Journal.Sealer.Interval)
	defer tick.Stop()
	// Отсчёт проверки опоздавших — от старта процесса, а не от первого тика:
	// после долгого простоя опоздавшие как раз вероятны.
	sweep := time.NewTicker(deps.Cfg.Journal.Sealer.SweepInterval)
	defer sweep.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		sealOnce(ctx, deps, log)
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		case <-sweep.C:
			sweepOnce(ctx, deps, log)
		}
	}
}

// limits — настройки сегмента из конфига одним пакетом для meta.
func limits(cfg *config.Config) meta.SealLimits {
	return meta.SealLimits{
		MaxEvents: cfg.Journal.Segment.MaxEvents,
		MaxBytes:  int64(cfg.Journal.Segment.MaxBytes),
	}
}

// sealOnce — один проход по тенантам: каждый допечатывает не больше
// max_segments_per_tenant сегментов, чтобы тенант с огромным backlog не отдал
// весь проход остальным.
func sealOnce(ctx context.Context, deps SealerDeps, log *slog.Logger) {
	cfg := deps.Cfg
	tenants, err := deps.Meta.JournalTenantsToSeal(ctx, cfg.Journal.Sealer.TenantsBatch)
	if err != nil {
		log.Error("sealer: список тенантов", slog.String("err", err.Error()))
		return
	}
	for _, tenant := range tenants {
		if ctx.Err() != nil {
			return
		}
		for i := 0; i < cfg.Journal.Sealer.MaxSegmentsPerTenant; i++ {
			seg, err := deps.Meta.SealJournalTenant(ctx, tenant, limits(cfg))
			if err != nil {
				// Ошибка — откат этой транзакции; события не запечатаны и
				// будут взяты следующим проходом целиком, полупустого
				// сегмента не бывает.
				log.Error("sealer: запечатывание тенанта",
					slog.Int64("tenant", tenant), slog.String("err", err.Error()))
				break
			}
			if seg == nil {
				break
			}
			log.Info("sealer: сегмент запечатан",
				slog.Int64("tenant", tenant), slog.Int64("segment", seg.ID),
				slog.String("uuid", seg.UUID.String()),
				slog.Int64("seq_first", seg.SeqFirst), slog.Int64("seq_last", seg.SeqLast),
				slog.Int("events", seg.EventCount))
		}
	}
}

// sweepOnce — проверка опоздавших. Находка здесь не авария, а штатная редкость:
// событие, чья транзакция шла слишком долго, запечатывается позже соседей.
// Порядок одного объекта от этого не меняется (см. комментарий SealJournalStragglers).
func sweepOnce(ctx context.Context, deps SealerDeps, log *slog.Logger) {
	cfg := deps.Cfg
	tenants, err := deps.Meta.JournalTenantsWithStragglers(ctx, cfg.Journal.Sealer.TenantsBatch)
	if err != nil {
		log.Error("sealer: список тенантов с опоздавшими", slog.String("err", err.Error()))
		return
	}
	for _, tenant := range tenants {
		if ctx.Err() != nil {
			return
		}
		for i := 0; i < cfg.Journal.Sealer.MaxSegmentsPerTenant; i++ {
			seg, err := deps.Meta.SealJournalStragglers(ctx, tenant, cfg.Journal.Sealer.SweepBatch)
			if err != nil {
				log.Error("sealer: проверка опоздавших",
					slog.Int64("tenant", tenant), slog.String("err", err.Error()))
				break
			}
			if seg == nil {
				break
			}
			log.Warn("sealer: опоздавшее событие запечатано отдельно",
				slog.Int64("tenant", tenant), slog.Int64("segment", seg.ID),
				slog.Int64("seq_first", seg.SeqFirst), slog.Int64("seq_last", seg.SeqLast),
				slog.Int("events", seg.EventCount))
		}
	}
}
