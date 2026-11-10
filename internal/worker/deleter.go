package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/config"
	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/mover"
	"github.com/vladsimachkov/tapegw/internal/spool"
)

// Deleter-воркер (M6): снимает версии, ушедшие в 'deleting', — файлы на диске
// сразу, объект на ленте когда это безопасно.
//
// Порядок задаёт восстановление после сбоя, поэтому он один на весь проход:
// «сначала лента, потом строка». Строка уходит в 'deleted' только после
// подтверждения от mover; сбой между purge и переходом оставляет строку в
// 'deleting', и повтор сходится — purge отсутствующего объекта есть успех.
// Обратный порядок (сначала строка) терял бы данные: строка 'deleted', а на
// ленте объект, который никто не удалит, потому что очередь его не видит.
//
// Пачку на ленте deleter снимает только когда members_live = 0 и никто из
// участников не под lock'ом или восстановлением и migrator не держит захват.
// Пока есть живой участник, байты удалённых остаются на ленте до уплотнения
// (0011) — снимаются только их файлы в landing и cache.
//
// Сохранность после аварии: пока журнала на ленте нет (следующий пункт
// плана), удалённая версия после rebuild-meta воскреснет — удаление данных
// Phobos не отменяется откатом БД. Это осознанный разрыв до журнала, он
// зафиксирован в плане.

// DeleterDeps — всё, что нужно удалителю. Cache обязателен, пока включён
// deleter: без него восстановленные копии удалённых версий оставались бы в
// cache молча, ровно как у janitor.
type DeleterDeps struct {
	Cfg   *config.Config
	Meta  *meta.Meta
	Spool *spool.Spool
	Cache *spool.Spool
	Log   *slog.Logger
}

// DeleterGroup — запущенный удалитель; Wait ждёт его при остановке.
type DeleterGroup struct {
	wg  sync.WaitGroup
	log *slog.Logger
}

// PlanDeleters — mover'ы роли migrator своей библиотеки: копии живут в
// архивной библиотеке, и удалять их должен тот же mover, что пишет. В
// отличие от PlanMigrators пустой список — не ошибка: без mover'ов нечего
// и удалять на ленте, дисковая часть работы остаётся.
func PlanDeleters(cfg *config.Config) []MoverTarget {
	var out []MoverTarget
	for drive, mv := range cfg.Phobos.Movers {
		if mv.Role != string(mover.RoleMigrator) || mv.Library != cfg.Phobos.Library {
			continue
		}
		out = append(out, MoverTarget{Drive: drive, Addr: mv.Addr, Node: mv.Node})
	}
	return out
}

// DialDeleter — mTLS-подключение удалителя к mover'у. Отдельный сертификат
// (phobos.deleter_client, SAN-URI tapegw://deleter) — не бюрократия: права на
// purge целого объекта не должны жить на сертификате планировщика архивации.
func DialDeleter(ctx context.Context, cfg *config.Config, t MoverTarget) (*mover.Client, error) {
	tlsCfg, err := mover.ClientTLS(
		cfg.Phobos.DeleterClient.Cert, cfg.Phobos.DeleterClient.Key, cfg.Phobos.DeleterClient.CA, t.Drive)
	if err != nil {
		return nil, err
	}
	c, _, err := mover.DialAs(ctx, t.Addr, tlsCfg, mover.RoleMigrator)
	if err != nil {
		return nil, fmt.Errorf("worker: mover %s (%s): %w", t.Drive, t.Addr, err)
	}
	return c, nil
}

// StartDeleter поднимает единственный горутинный цикл удаления. Одна очередь
// 'deleting' не требует параллелизма: работа воркера — короткие вызовы mover,
// а два воркера на одной пачке сделали бы двойную работу без выигрыша.
func StartDeleter(ctx context.Context, deps DeleterDeps) *DeleterGroup {
	log := deps.Log
	if log == nil {
		log = slog.Default()
	}
	g := &DeleterGroup{log: log}
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		runDeleter(ctx, deps, log)
	}()
	log.Info("deleter: удалитель запущен",
		slog.Duration("interval", deps.Cfg.Deleter.Interval),
		slog.Int("batch_max_objects", deps.Cfg.Deleter.BatchMaxObjects))
	return g
}

// Wait ждёт завершения прохода не дольше grace. Ложь означает «ушли, не
// дождавшись»: незакрытые строки останутся в 'deleting' с истёкшим захватом,
// и следующий прогон повторит их без вреда.
func (g *DeleterGroup) Wait(grace time.Duration) bool {
	done := make(chan struct{})
	go func() { g.wg.Wait(); close(done) }()
	select {
	case <-done:
		return true
	case <-time.After(grace):
		g.log.Warn("deleter: проход не завершился за grace, процесс уходит; неснятое снимет следующий запуск",
			slog.Duration("grace", grace))
		return false
	}
}

func runDeleter(ctx context.Context, deps DeleterDeps, log *slog.Logger) {
	tick := time.NewTicker(deps.Cfg.Deleter.Interval)
	defer tick.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		deleteOnce(ctx, deps, log)
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// deleteOnce — один проход: выборка (транзакция закрыта), разбор по пачкам,
// purge через mover, переходы отдельными транзакциями. Правило 13: ни одна
// транзакция не открыта на время вызова mover.
func deleteOnce(ctx context.Context, deps DeleterDeps, log *slog.Logger) {
	cfg := deps.Cfg
	cands, err := deps.Meta.ClaimDeletionBatch(ctx, cfg.Deleter.BatchMaxObjects, cfg.Deleter.ClaimTTL)
	if err != nil {
		log.Error("deleter: выборка", slog.String("err", err.Error()))
		return
	}
	if len(cands) == 0 {
		return
	}

	// Группировка по пачке: решение о ленте принимается на пачку, а не на
	// версию, и два участника одной пачки не должны тянуть её состояние
	// дважды.
	byPack := map[uuid.UUID][]meta.DeletionCandidate{}
	var loose []meta.DeletionCandidate
	for _, c := range cands {
		if c.PackID == nil {
			loose = append(loose, c)
			continue
		}
		byPack[*c.PackID] = append(byPack[*c.PackID], c)
	}

	for _, c := range loose {
		if ctx.Err() != nil {
			return
		}
		deleteLoose(ctx, deps, c, log)
	}

	if len(byPack) == 0 {
		return
	}

	// mover нужен только для purge; подключаемся один раз на проход и
	// держим соединение. Подключение ленивое: дисковая работа не должна
	// зависеть от доступности драйва.
	var cli *mover.Client
	defer func() {
		if cli != nil {
			cli.Close()
		}
	}()
	dial := func() (*mover.Client, error) {
		if cli != nil {
			return cli, nil
		}
		targets := PlanDeleters(cfg)
		if len(targets) == 0 {
			return nil, errors.New("в библиотеке нет mover роли migrator — удалять на ленте некому")
		}
		var lastErr error
		for _, t := range targets {
			c, err := DialDeleter(ctx, cfg, t)
			if err != nil {
				lastErr = err
				continue
			}
			cli = c
			return cli, nil
		}
		return nil, fmt.Errorf("deleter: подключиться к mover'у: %w", lastErr)
	}

	for packID, members := range byPack {
		if ctx.Err() != nil {
			return
		}
		deletePack(ctx, deps, dial, packID, members, log)
	}
}

// deleteLoose снимает версию, которая никогда не лежала в пачке: только диск.
func deleteLoose(ctx context.Context, deps DeleterDeps, c meta.DeletionCandidate, log *slog.Logger) {
	if c.Locked || c.RestoreBusy {
		// Версия без пачки под lock'ом: её landing-файл — единственная копия
		// данных, снимать её нельзя. Ждём в 'deleting' до снятия блокировки.
		log.Info("deleter: версия под блокировкой, удаление отложено",
			slog.String("id", c.ID.String()), slog.String("key", c.Bucket+"/"+c.Key),
			slog.Bool("lock", c.Locked), slog.Bool("restore", c.RestoreBusy))
		return
	}
	ok, err := deps.Meta.MarkVersionDeleted(ctx, c.ID)
	if err != nil {
		log.Error("deleter: переход в deleted", slog.String("id", c.ID.String()), slog.String("err", err.Error()))
		return
	}
	if !ok {
		return
	}
	removeCopies(deps, c.ID, log)
	log.Info("deleter: версия снята с диска", slog.String("id", c.ID.String()), slog.String("key", c.Bucket+"/"+c.Key))
}

// deletePack разбирает группу участников одной пачки.
func deletePack(ctx context.Context, deps DeleterDeps, dial func() (*mover.Client, error),
	packID uuid.UUID, members []meta.DeletionCandidate, log *slog.Logger) {

	st, err := deps.Meta.PackDeletionState(ctx, packID)
	if err != nil {
		log.Error("deleter: состояние пачки", slog.String("pack", packID.String()), slog.String("err", err.Error()))
		requeueMembers(ctx, deps, members, "состояние пачки не прочитано", log)
		return
	}

	// Пачка уже ретайрена — на ленте делать нечего, доводим строки и файлы.
	if st.State == "retired" {
		for _, c := range members {
			finishMember(ctx, deps, c, log)
		}
		return
	}

	// Преходящие блоки: пачку держит migrator (печатает или дописывает копии),
	// идёт уплотнение, пачка ещё не запечатана. Таких участников нельзя
	// разбирать по одному: если перевести их в 'deleted' сейчас, под пачкой не
	// останется строк в 'deleting', и после снятия блока перевызывать deleter
	// будет некому — объект пачки остался бы на ленте навсегда (очередь
	// deletion'а смотрит в objects, а не в packs). Поэтому откладываем всю
	// группу: строки остаются в 'deleting' и вернутся сами по next_try_at.
	//
	// Блоки уровня участника (lock, restore) устроены иначе: сам заблокированный
	// остаётся в 'deleting' и продолжает вызывать deleter, поэтому остальные
	// допустимо разбирать по одному.
	if st.ClaimActive || st.State == "open" || st.State == "compacting" {
		requeueMembers(ctx, deps, members,
			fmt.Sprintf("пачка занята: state=%s claim=%v", st.State, st.ClaimActive), log)
		return
	}

	purgeable := st.MembersLive == 0 && st.LockedDeleting == 0 && !st.RestoreBusy &&
		(st.State == "archived" || st.State == "sealed")

	if purgeable {
		cli, err := dial()
		if err != nil {
			log.Error("deleter: mover для purge недоступен",
				slog.String("pack", packID.String()), slog.String("err", err.Error()))
			requeueMembers(ctx, deps, members, err.Error(), log)
			return
		}
		for _, oid := range st.Copies {
			res, err := cli.PurgeObject(oid)
			if err != nil {
				log.Error("deleter: purge прервался",
					slog.String("pack", packID.String()), slog.String("oid", oid.String()),
					slog.String("err", err.Error()))
				requeueMembers(ctx, deps, members, "purge прервался: "+err.Error(), log)
				return
			}
			if res.Code != mover.CodeOK {
				log.Error("deleter: mover отказал в purge",
					slog.String("pack", packID.String()), slog.String("oid", oid.String()),
					slog.String("code", string(res.Code)))
				requeueMembers(ctx, deps, members, fmt.Sprintf("purge: код %s", res.Code), log)
				return
			}
		}
		// Лента подтвердила. Теперь строки — одной транзакцией с ретайром
		// пачки (правило «сначала лента, потом строка»).
		ids, retired, err := deps.Meta.FinishPackDeletion(ctx, packID)
		if err != nil {
			log.Error("deleter: завершение пачки", slog.String("pack", packID.String()), slog.String("err", err.Error()))
			requeueMembers(ctx, deps, members, err.Error(), log)
			return
		}
		for _, id := range ids {
			removeCopies(deps, id, log)
		}
		log.Info("deleter: пачка снята с ленты",
			slog.String("pack", packID.String()), slog.Int("участников", len(ids)),
			slog.Bool("retired", retired))
		return
	}

	// Пачка на ленте ждёт: живой участник, lock или восстановление у одного из
	// участников. Удаляем только то, что безопасно по одному участнику.
	for _, c := range members {
		finishMember(ctx, deps, c, log)
	}
}

// finishMember снимает одного участника с диска и переводит строку, если он
// сам не заблокирован. Байты на ленте под живой пачкой остаются до
// уплотнения или до purge всей пачки — это разные ожидания, и здесь первое.
func finishMember(ctx context.Context, deps DeleterDeps, c meta.DeletionCandidate, log *slog.Logger) {
	if c.Locked || c.RestoreBusy {
		log.Info("deleter: участник под блокировкой, удаление отложено",
			slog.String("id", c.ID.String()), slog.String("key", c.Bucket+"/"+c.Key),
			slog.Bool("lock", c.Locked), slog.Bool("restore", c.RestoreBusy))
		return
	}
	ok, err := deps.Meta.MarkVersionDeleted(ctx, c.ID)
	if err != nil {
		log.Error("deleter: переход в deleted", slog.String("id", c.ID.String()), slog.String("err", err.Error()))
		return
	}
	if !ok {
		return
	}
	removeCopies(deps, c.ID, log)
}

// requeueMembers откладывает повтор для участников, чей проход не состоялся:
// строки остаются в 'deleting' — для удаления нет терминального 'failed',
// данные обязаны быть сняты когда-нибудь.
func requeueMembers(ctx context.Context, deps DeleterDeps, members []meta.DeletionCandidate, reason string, log *slog.Logger) {
	ids := make([]uuid.UUID, 0, len(members))
	for _, c := range members {
		ids = append(ids, c.ID)
	}
	if err := deps.Meta.DeletionFailed(ctx, ids, reason, deps.Cfg.Deleter.RetryBackoff); err != nil {
		log.Error("deleter: отложить повтор", slog.String("err", err.Error()))
	}
}

// removeCopies снимает дисковые копии версии: landing и cache. Remove
// идемпотентен — файла могло и не быть (уже вытеснен janitor'ом или снят
// предыдущим прогоном), и это не ошибка.
func removeCopies(deps DeleterDeps, id uuid.UUID, log *slog.Logger) {
	if err := deps.Spool.Remove(id); err != nil {
		log.Error("deleter: файл landing не удалён — остался сиротой",
			slog.String("id", id.String()), slog.String("err", err.Error()))
	}
	if deps.Cache != nil {
		if err := deps.Cache.Remove(id); err != nil {
			log.Error("deleter: файл cache не удалён — остался сиротой",
				slog.String("id", id.String()), slog.String("err", err.Error()))
		}
	}
}
