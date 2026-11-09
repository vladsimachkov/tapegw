package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/config"
	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/migrator"
	"github.com/vladsimachkov/tapegw/internal/mover"
	"github.com/vladsimachkov/tapegw/internal/pack"
	"github.com/vladsimachkov/tapegw/internal/spool"
)

// Мигратор-воркер: поднимает пачки с landing в Phobos через mover.
//
// Один воркер на mover роли migrator этой библиотеки (ADR-0007): mover — это
// драйв, а драйвов в библиотеке несколько, и каждый возит свою пачку. Слот
// воркера — ключ из phobos.movers, он же часть WorkerID ("<узел>/<слот>"):
// после перезапуска воркер того же слота забирает свою запечатанную пачку без
// ожидания истечения захвата (migrator.ResumePacks).
//
// Остановка (ревью S2): контекст воркера отменяется по сигналу, но начатая
// отправка не обрывается — она идёт на контексте без отмены. Либо пачка
// доходит до archived, либо процесс уходит, оставив пачку в sealed с захватом,
// у которого истекает срок, и её продолжает следующий запуск. Третьего исхода
// («обрезанная пачка на ленте») здесь нет намеренно.

// MoverTarget — mover роли migrator, выбранный из phobos.movers.
type MoverTarget struct {
	// Drive — ключ в phobos.movers: имя драйва, слот воркера и SAN mover'а.
	Drive string
	Addr  string
	Node  string
}

// PlanMigrators — mover'ы роли migrator своей библиотеки (фильтр по роли, а
// не только по библиотеке: в одной библиотеке стоят драйвы обоих пулов).
//
// Пусто при включённом воркере — ошибка, а не «молча простоим»: выключенная
// по недосмотру архивация видна не будет, а объекты тем временем стареют в
// landing до конца online_until.
func PlanMigrators(cfg *config.Config) ([]MoverTarget, error) {
	var out []MoverTarget
	for drive, mv := range cfg.Phobos.Movers {
		if mv.Role != string(mover.RoleMigrator) || mv.Library != cfg.Phobos.Library {
			continue
		}
		out = append(out, MoverTarget{Drive: drive, Addr: mv.Addr, Node: mv.Node})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf(
			"worker: migrator включён, но в библиотеке %q нет ни одного mover с ролью migrator "+
				"(phobos.movers.<драйв>.role) — архивировать некому", cfg.Phobos.Library)
	}
	return out, nil
}

// DialMover — mTLS-подключение планировщика архивации к mover'у. Роль
// проверяется на рукопожатии: подключиться к драйву recovery-пула вместо
// архивного — ошибка конфигурации, а не удача.
func DialMover(ctx context.Context, cfg *config.Config, t MoverTarget) (*mover.Client, error) {
	tlsCfg, err := mover.ClientTLS(
		cfg.Phobos.MoverClient.Cert, cfg.Phobos.MoverClient.Key, cfg.Phobos.MoverClient.CA, t.Drive)
	if err != nil {
		return nil, err
	}
	c, _, err := mover.DialAs(ctx, t.Addr, tlsCfg, mover.RoleMigrator)
	if err != nil {
		return nil, fmt.Errorf("worker: mover %s (%s): %w", t.Drive, t.Addr, err)
	}
	return c, nil
}

// MigratorDeps — всё, что нужно воркерам архивации.
type MigratorDeps struct {
	Cfg   *config.Config
	Meta  *meta.Meta
	Spool *spool.Spool
	Log   *slog.Logger
}

// MigratorGroup — запущенные воркеры; Wait ждёт их при остановке.
type MigratorGroup struct {
	wg   sync.WaitGroup
	log  *slog.Logger
	desc []string // "<узел>/<слот>" — для лога остановки
}

// StartMigrators поднимает по воркеру на каждый mover из targets.
//
// Подключение к mover'у — здесь же и строго до возврата: «включено, а mover'а
// нет» должно быть слышно на старте процесса, а не через час по неубывающей
// очереди pending.
func StartMigrators(ctx context.Context, deps MigratorDeps, targets []MoverTarget) (*MigratorGroup, error) {
	log := deps.Log
	if log == nil {
		log = slog.Default()
	}
	// Тег пула — из БД (pools.phobos_tag): без него пачка уедет на носители по
	// умолчанию библиотеки, а разведение копий по тегам (ADR-0013) — именно
	// цель, ради которой тег заводили.
	tag, err := deps.Meta.PoolTag(ctx, meta.DefaultPoolID)
	if err != nil {
		return nil, err
	}
	g := &MigratorGroup{log: log}
	load := migrator.NewLoader(deps.Meta, deps.Spool)
	for _, t := range targets {
		if _, err := DialMover(ctx, deps.Cfg, t); err != nil {
			return nil, err
		}
		id := t.Node + "/" + t.Drive
		g.desc = append(g.desc, id)
		g.wg.Add(1)
		go func(t MoverTarget, id string) {
			defer g.wg.Done()
			runMigrator(ctx, deps, t, id, load, tag)
		}(t, id)
	}
	log.Info("migrator: воркеры запущены",
		slog.Any("слоты", g.desc),
		slog.Duration("batch_max_wait", deps.Cfg.Migrator.BatchMaxWait))
	return g, nil
}

// Wait ждёт завершения воркеров не дольше grace: начатая отправка обычно
// доходит до конца быстрее, а зависший mover не должен превращать остановку
// в вечное ожидание. Ложь означает «ушли, не дождавшись» — пачки при этом
// остаются в sealed, и их продолжит следующий запуск.
func (g *MigratorGroup) Wait(grace time.Duration) bool {
	done := make(chan struct{})
	go func() { g.wg.Wait(); close(done) }()
	select {
	case <-done:
		return true
	case <-time.After(grace):
		g.log.Warn("migrator: воркеры не завершились за grace, процесс уходит; запечатанные пачки продолжит следующий запуск",
			slog.Duration("grace", grace))
		return false
	}
}

// runMigrator — цикл одного воркера.
func runMigrator(ctx context.Context, deps MigratorDeps, t MoverTarget, id string, load migrator.LoadItems, tag string) {
	cfg, log := deps.Cfg, deps.Log
	if log == nil {
		log = slog.Default()
	}
	// sendCtx не связан с отменой: правило остановки — «начатая пачка
	// доходит», а не «обрывается по сигналу».
	sendCtx := context.WithoutCancel(ctx)
	lim := meta.DefaultBatchLimits(cfg.Migrator.BatchMaxObjects, int64(cfg.Migrator.BatchMaxBytes))
	attrs := map[string][]string{copySpec: {tag}}

	for first := true; ; first = false {
		if !first {
			select {
			case <-ctx.Done():
				return
			case <-time.After(cfg.Migrator.BatchMaxWait):
			}
		}
		if ctx.Err() != nil {
			return
		}

		// Переподключение на каждой итерации: batch_max_wait между батчами
		// больше времени жизни соединения, а «протухший клиент» дешевле
		// переподключить, чем отличать его от зависшего.
		cli, err := DialMover(ctx, cfg, t)
		if err != nil {
			log.Error("migrator: подключение к mover'у", slog.String("слот", id), slog.String("err", err.Error()))
			select {
			case <-ctx.Done():
				return
			case <-time.After(cfg.Migrator.BatchMaxWait):
			}
			continue
		}

		a := &migrator.Archiver{
			Meta: deps.Meta, Put: cli,
			Spec:   copySpec,
			Format: pack.FormatVersion,
			// Одна копия по тегу пула — временное состояние среза; политика
			// из двух копий в боевом пути — пункт «Две копии в боевом пути»
			// плана после среза.
			Specs:               []string{copySpec},
			SpecTags:            attrs,
			WorkerID:            id,
			ClaimTTL:            cfg.Migrator.ClaimTTL,
			LandingMinRetention: cfg.Spool.LandingMinRetention,
			Log:                 log,
		}

		// Сначала продолжение запечатанных пачек этого слота: их участники уже
		// в 'archiving' и в батч не попадут, пока пачка не решится.
		if n, err := a.ResumePacks(sendCtx, id, cfg.Migrator.ClaimTTL, load, nil); err != nil {
			log.Error("migrator: продолжение пачек", slog.String("слот", id), slog.String("err", err.Error()))
		} else if n > 0 {
			log.Info("migrator: продолжены пачки", slog.String("слот", id), slog.Int("пачек", n))
		}

		// Временно: единственный пул (meta.DefaultPoolID). При втором пуле
		// (этап T2, ADR-0013) цикл обязан стать попульным — иначе объекты
		// второго пула молча никогда не попадут в батч.
		cands, err := deps.Meta.ClaimMigrationBatch(ctx, meta.DefaultPoolID, lim)
		if err != nil {
			log.Error("migrator: выборка батча", slog.String("слот", id), slog.String("err", err.Error()))
			cli.Close()
			continue
		}
		if len(cands) == 0 {
			cli.Close()
			continue
		}
		ids := make([]uuid.UUID, 0, len(cands))
		for _, c := range cands {
			ids = append(ids, c.ID)
		}
		items, err := load(ctx, ids)
		if err != nil {
			// Батч уже в 'archiving' — это не потеря: FailPackArchive сюда ещё
			// не дошёл, пачки нет, и строки вернёт в pending возврат состояния
			// по исчерпанию попыток. Логируем и идём дальше.
			log.Error("migrator: загрузка участников", slog.String("слот", id), slog.String("err", err.Error()))
			cli.Close()
			continue
		}
		// Разбивка батча по pack.threshold: мелкие — в пачку, крупные — каждый
		// своим объектом Phobos (ARCHITECTURE, «Упаковка»). Крупные идут
		// первыми: они не ограничены окном закрытия пачки и не должны ждать,
		// пока соберётся tar. Отказ одного крупного не отменяет пачку и
		// остальные объекты — учёт попыток построчный.
		small, large := migrator.SplitByThreshold(items, int64(cfg.Pack.Threshold))
		for _, it := range large {
			if _, err := a.ArchiveLargeObject(sendCtx, meta.DefaultPoolID, it); err != nil {
				log.Error("migrator: архивация крупного объекта",
					slog.String("слот", id), slog.String("объект", it.Version.String()),
					slog.String("err", err.Error()))
			}
		}
		if len(small) > 0 {
			if _, err := a.ArchivePack(sendCtx, meta.DefaultPoolID, small, nil); err != nil {
				log.Error("migrator: архивация пачки",
					slog.String("слот", id), slog.Int("объектов", len(small)), slog.String("err", err.Error()))
			}
		}
		cli.Close()
	}
}

// copySpec — имя единственной спецификации копии в срезе. Тег берётся из
// pools.phobos_tag (meta.PoolTag). Одна копия — временное состояние среза:
// «Две копии в боевом пути» — пункт плана после среза.
const copySpec = "main"
