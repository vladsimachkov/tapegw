package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/vladsimachkov/tapegw/internal/config"
	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/spool"
)

// Janitor — фоновый вытеснитель дисковой копии (S3: только истечение
// online_until, watermark — отдельный пункт плана).
//
// Порядок прохода диктует ADR-0004: ClaimEvictBatch снимает landing в
// транзакции и возвращается уже после commit, unlink — здесь, вне всякой
// транзакции. Между commit и unlink процесс может упасть: останется файл без
// ссылки, его подберёт будущая сборка сирот. Обратный порядок хуже — строка
// «объект онлайн», а файла нет, и GET вместо штатного InvalidObjectState
// ловит ENOENT.

// JanitorDeps — всё, что нужно вытеснителю.
type JanitorDeps struct {
	Cfg  *config.Config
	Meta *meta.Meta
	// Spool — landing; Cache — восстановленные копии (S5). Cache обязателен,
	// пока включён janitor: без него истёкшие restore накапливались бы в
	// очереди и в дисковом буфере молча.
	Spool *spool.Spool
	Cache *spool.Spool
	Log   *slog.Logger
}

// JanitorGroup — запущенный вытеснитель; Wait ждёт его при остановке.
type JanitorGroup struct {
	wg  sync.WaitGroup
	log *slog.Logger
}

// StartJanitor поднимает единственный горутинный цикл вытеснения.
// Первый проход — сразу при старте: процесс мог долго не работать, и истёкшие
// объекты ждут с момента запуска, а не с первого тика.
func StartJanitor(ctx context.Context, deps JanitorDeps) *JanitorGroup {
	log := deps.Log
	if log == nil {
		log = slog.Default()
	}
	g := &JanitorGroup{log: log}
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		runJanitor(ctx, deps, log)
	}()
	log.Info("janitor: вытеснитель запущен",
		slog.Duration("interval", deps.Cfg.Janitor.Interval),
		slog.Int("batch_max_objects", deps.Cfg.Janitor.BatchMaxObjects))
	return g
}

// Wait ждёт завершения цикла не дольше grace. Ложь означает «ушли, не
// дождавшись»: неснятые объекты останутся с landing=true и будут вытеснены
// следующим прогоном — повтор ничего не портит.
func (g *JanitorGroup) Wait(grace time.Duration) bool {
	done := make(chan struct{})
	go func() { g.wg.Wait(); close(done) }()
	select {
	case <-done:
		return true
	case <-time.After(grace):
		g.log.Warn("janitor: проход не завершился за grace, процесс уходит; неснятое снимет следующий запуск",
			slog.Duration("grace", grace))
		return false
	}
}

func runJanitor(ctx context.Context, deps JanitorDeps, log *slog.Logger) {
	tick := time.NewTicker(deps.Cfg.Janitor.Interval)
	defer tick.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		evictOnce(ctx, deps, log)
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// evictOnce — один проход: выборка (флаг уже снят, транзакция закрыта),
// затем unlink каждого файла. Ошибка unlink не останавливает проход: файл
// стал сиротой (флаг-то снят), это работа для сборки сирот, а не причина
// держать остальные истёкшие файлы в landing.
func evictOnce(ctx context.Context, deps JanitorDeps, log *slog.Logger) {
	cands, err := deps.Meta.ClaimEvictBatch(ctx, deps.Cfg.Janitor.BatchMaxObjects)
	if err != nil {
		log.Error("janitor: выборка на вытеснение", slog.String("err", err.Error()))
		return
	}
	for _, c := range cands {
		if ctx.Err() != nil {
			// Остановка: неснятые останутся с landing=false — файлы подберёт
			// сборка сирот, данные не потеряны (объект уже archived).
			return
		}
		if err := deps.Spool.Remove(c.ID); err != nil {
			log.Error("janitor: файл не удалён — остался сиротой",
				slog.String("key", c.Key), slog.String("id", c.ID.String()),
				slog.String("err", err.Error()))
			continue
		}
		log.Info("janitor: дисковая копия вытеснена",
			slog.String("key", c.Key), slog.String("id", c.ID.String()))
	}

	evictCacheOnce(ctx, deps, log)
}

// evictCacheOnce — истёкшие restore (S5, критерий «после срока restore —
// снова InvalidObjectState»). ClaimCacheEvictBatch удаляет строку restores в
// транзакции выборки — с этого момента GET уже отвечает InvalidObjectState,
// и только потом здесь снимается файл из cache (ADR-0004: сначала строка
// перестала обещать копию, потом unlink).
func evictCacheOnce(ctx context.Context, deps JanitorDeps, log *slog.Logger) {
	if deps.Cache == nil {
		// До выборки: ClaimCacheEvictBatch удаляет строку, а файла без буфера
		// не снять — обещание «строка снята, файл удалён» должно выполняться.
		log.Error("janitor: cache не настроен — истёкшие restore не вытесняются")
		return
	}
	ids, err := deps.Meta.ClaimCacheEvictBatch(ctx, deps.Cfg.Janitor.BatchMaxObjects)
	if err != nil {
		log.Error("janitor: выборка истёкших restore", slog.String("err", err.Error()))
		return
	}
	for _, id := range ids {
		if ctx.Err() != nil {
			return
		}
		// Файла мог и не быть (сбой публикации между rename и падением) —
		// Remove идемпотентен, а строка уже удалена: состояние сходится.
		if err := deps.Cache.Remove(id); err != nil {
			log.Error("janitor: файл cache не удалён — остался сиротой",
				slog.String("id", id.String()), slog.String("err", err.Error()))
			continue
		}
		log.Info("janitor: восстановленная копия вытеснена из cache",
			slog.String("id", id.String()))
	}
}
