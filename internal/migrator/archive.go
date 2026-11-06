package migrator

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/mover"
)

// Putter — то, что migrator просит у mover'а. Интерфейс, а не *mover.Client:
// тест правила 13 подставляет того, кто в момент put проверяет блокировку строк
// (С8), и делать для этого TLS не нужно.
type Putter interface {
	Put(in mover.PutIn) (*mover.Result, error)

	// Status нужен не для отчётов, а для разбора неизвестного итога: после
	// обрыва соединения tapegw не знает, записалась пачка или нет.
	Status(op uuid.UUID) (*mover.Result, error)
}

// definiteRefusals — коды, по которым известно: на ленту ничего не попало.
// Только для них пачку можно выбросить. Всё остальное — «итог неизвестен»,
// и там пачка остаётся.
var definiteRefusals = map[mover.Code]bool{
	mover.CodeNoSpace:          true,
	mover.CodeAttrTooLarge:     true,
	mover.CodeChecksumMismatch: true,
}

// Archiver поднимает пачку объектов на ленту.
type Archiver struct {
	Meta *meta.Meta
	Put  Putter

	// Spec — спецификация копии (теги пула), Format — версия формата на ленте.
	// Оба пишутся в archive_copies: у копий одного объекта они могут
	// различаться (ADR-0022), и по format читатель выбирает декодер.
	Spec   string
	Format int

	// Log — куда писать про ленту вне реестра. По умолчанию slog.Default().
	Log *slog.Logger

	// WorkerID — идентичность захвата, и это НЕ процесс: внутри migrator
	// воркеров архивации несколько (ADR-0007 — по одному на mover, то есть на
	// драйв), и общий на всех идентификатор превратил бы «свою пачку забрать
	// сразу» в «чужую пачку забрать сразу»: второй воркер украл бы пачку у
	// первого, пока тот жив и продлевает захват.
	//
	// Формат — узел плюс слот воркера, например host-a/mover-2. Слот устойчив
	// между перезапусками (это драйв из конфига), поэтому «свою пачку после
	// рестарта забрать без ожидания ttl» продолжает работать: её заберёт воркер
	// того же слота. Уникальность слота между воркерами обязательна.
	//
	// ClaimTTL — на сколько ставится захват. Он продлевается во время отправки
	// (см. heartbeat), поэтому ttl может быть заметно меньше самой долгой
	// отправки; после сбоя именно ttl определяет, как долго пачка простоит без
	// продолжения.
	WorkerID string
	ClaimTTL time.Duration
}

// sendPack — одна попытка put. Ошибка здесь означает «итог неизвестен», а не
// «отказ»: различает их вызывающий, а не этот вызов.
func (a *Archiver) sendPack(ctx context.Context, packID uuid.UUID, plan *Plan, attrs map[string]string, body []byte) (*mover.Result, error) {
	return a.Put.Put(mover.PutIn{
		Op:     packID,
		ID:     packID, // oid Phobos = UUID пачки: повтор бьёт в тот же объект
		Size:   plan.Size,
		SHA256: plan.SHA256,
		Attrs:  attrs,
		Body:   bytes.NewReader(body),
	})
}

// resolveUnknown разбирает итог операции, по которой оборвалась связь.
//
// Спрашиваем mover про операцию; если он её не помнит (упал вместе с реестром)
// — пересылаем ТУ ЖЕ пачку. Повтор не создаёт дубль: oid объекта Phobos это
// UUID пачки, он не меняется, сборка детерминирована, байты те же, и Phobos на
// повтор отвечает EEXIST, а не пишет второй экземпляр.
//
// Запасной путь на случай «реестр потерян, и EEXIST не помог бы» — stat по oid
// через Phobos. Команды stat в протоколе mover'а пока нет (ADR-0021 её описывает,
// реализация не написана), поэтому он помечен, а не реализован.
func (a *Archiver) resolveUnknown(ctx context.Context, packID uuid.UUID, plan *Plan, attrs map[string]string, body []byte, firstErr error) (*mover.Result, error) {
	st, serr := a.Put.Status(packID)
	if serr == nil {
		switch st.State {
		case mover.StateDone:
			// Запись прошла, оборвался только ответ. Это успех, а не отказ.
			return &mover.Result{Op: packID, Code: mover.CodeOK, State: st.State,
				Version: st.Version, Extents: st.Extents}, nil
		case mover.StateFailed:
			return &mover.Result{Op: packID, Code: st.Code, State: st.State}, nil
		}
	}
	res, err := a.sendPack(ctx, packID, plan, attrs, body)
	if err != nil {
		return nil, fmt.Errorf("migrator: итог put неизвестен (%v), повтор тоже не прошёл (%v) — пачка остаётся в archiving",
			firstErr, err)
	}
	return res, nil
}

// heartbeat продлевает захват пачки раз в треть срока. Возвращает функцию
// остановки и проверку «захват потерян».
//
// Продление — короткий UPDATE ... WHERE claimed_by = me, вне всякой транзакции
// отправки (правило 13). Если строка уже не наша, дальше продлевать незачем и
// вызывающий обязан не завершать пачку.
func (a *Archiver) heartbeat(ctx context.Context, packID uuid.UUID) (func(), func() bool) {
	lost := false
	done := make(chan struct{})
	var mu sync.Mutex
	go func() {
		tick := time.NewTicker(a.ClaimTTL / 3)
		defer tick.Stop()
		for {
			select {
			case <-done:
				return
			case <-tick.C:
				ok, err := a.Meta.RenewPackClaim(ctx, packID, a.WorkerID, a.ClaimTTL)
				if err != nil || !ok {
					mu.Lock()
					lost = true
					mu.Unlock()
					return
				}
			}
		}
	}()
	return func() { close(done) }, func() bool { mu.Lock(); defer mu.Unlock(); return lost }
}

func (a *Archiver) logger() *slog.Logger {
	if a.Log != nil {
		return a.Log
	}
	return slog.Default()
}

func metaPlacements(ps []Placement) []meta.Placement {
	out := make([]meta.Placement, 0, len(ps))
	for _, p := range ps {
		out = append(out, meta.Placement{Version: p.Version, Offset: p.Offset, Length: p.Length})
	}
	return out
}

// ArchivePack — один цикл архивации пачки.
//
// Порядок шагов и есть правило 13: транзакция перехода коммитится ДО вызова
// mover. Батч на 200 GiB идёт минуты, и открытая в это время транзакция держит
// строки участников — параллельный PUT тех же ключей упрётся в advisory-лок.
func (a *Archiver) ArchivePack(ctx context.Context, poolID int64, items []Item, attrs map[string]string) (*Plan, error) {
	if a.Meta == nil || a.Put == nil {
		return nil, fmt.Errorf("migrator: Archiver без Meta или Put")
	}
	if a.WorkerID == "" || a.ClaimTTL <= 0 {
		return nil, fmt.Errorf("migrator: Archiver без ProcessID/ClaimTTL: пачку нельзя закрепить за собой")
	}
	ids := make([]uuid.UUID, 0, len(items))
	for _, it := range items {
		ids = append(ids, it.Version)
	}

	// Шаг 1 — завести пачку и перевести объекты в archiving. Продолжением
	// запечатанных пачек занимается ResumePacks: там вход от пачки, потому что
	// после перезапуска списка объектов ниоткуда не берётся.
	tx, freshID, err := a.Meta.BeginPackArchive(ctx, poolID, ids)
	if err != nil {
		return nil, err
	}
	packID := freshID
	// Страховка от утечки соединения: любой ранний выход до коммита обязан
	// закрыть транзакцию, иначе пул остаётся с занятым соединением и
	// pool.Close() в cleanup тестов висит навсегда. После коммита откат —
	// ErrTxClosed, его не смотрим.
	defer tx.Rollback(ctx) //nolint:errcheck

	// Коммит здесь — смысл правила 13. Если перенести вызов mover'а выше этой
	// строки, транзакция будет держать строки участников на всё время записи,
	// и тест С8 это ловит явно: NOWAIT на строку объекта 55P03.
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("migrator: закоммитить переход: %w", err)
	}

	// Шаг 2 — первый проход: размер, сумма и смещения.
	plan, err := Measure(items)
	if err != nil {
		a.Meta.FailPackArchive(ctx, packID, ids, "сборка: "+err.Error())
		return nil, err
	}

	// Шаг 3 — запечатать ДО отправки. Именно здесь у объектов появляется
	// pack_id с адресом, и с этого момента сбой оставляет достаточное, чтобы
	// пачку продолжил любой процесс (ResumePacks), а не только этот.
	if err := a.Meta.SealPack(ctx, packID, metaPlacements(plan.Placements), plan.Size, a.WorkerID, a.ClaimTTL); err != nil {
		a.Meta.FailPackArchive(ctx, packID, ids, "запечатывание: "+err.Error())
		return nil, err
	}

	// Шаги 4 и 5 — отправка и итог.
	return a.deliver(ctx, packID, items, plan, attrs)
}

// deliver — шаги 4 и 5: второй проход, отправка, разбор неизвестного итога и
// запись результата. Транзакции во время вызова mover'а нет (правило 13).
// Общий для ArchivePack и ResumePacks: продолжение обязано пройти ровно тот же
// путь, иначе «продолжили не так» превратится в вторую копию.
func (a *Archiver) deliver(ctx context.Context, packID uuid.UUID, items []Item, plan *Plan, attrs map[string]string) (*Plan, error) {
	ids := make([]uuid.UUID, 0, len(items))
	for _, it := range items {
		ids = append(ids, it.Version)
	}
	// Шаг 4 — второй проход и отправка. Транзакции нет (правило 13).
	var buf bytes.Buffer
	placements, err := BuildIndexed(&buf, items, plan)
	if err != nil {
		// Пачка запечатана, но на ленту ничего не шло: отказ определённый,
		// снимается членство.
		a.Meta.FailPackArchive(ctx, packID, ids, "второй проход: "+err.Error())
		return nil, err
	}

	// Захват продлевается на всё время отправки: она включает ожидание драйва и
	// монтирование (ADR-0021 — десятки минут), и срок, выставленный один раз,
	// истёк бы посреди неё. Второй процесс забрал бы пачку и отправил её повторно
	// параллельно с первой.
	stop, lost := a.heartbeat(ctx, packID)
	defer stop()

	res, err := a.sendPack(ctx, packID, plan, attrs, buf.Bytes())
	if lost() {
		// Захват потерян: пачкой занимается другой процесс. Саму отправку
		// прервать нельзя (вызов Phobos не прерывается), но и завершать её за
		// нас некому — FinishPackArchive всё равно не пропустил бы чужого.
		return nil, fmt.Errorf("migrator: захват пачки %s потерян — её ведёт другой процесс", packID)
	}
	if err != nil {
		// Итог неизвестен. Пачку НЕ выбрасываем: если запись успела пройти,
		// объекты остаются в archiving, а строка пачки живёт, и следующий
		// цикл придёт с тем же oid. Разбор — ниже.
		res, err = a.resolveUnknown(ctx, packID, plan, attrs, buf.Bytes(), err)
		if err != nil {
			return nil, err
		}
	}
	if res.Code == mover.CodeExists || res.Code == mover.CodeIncomplete {
		// «Уже есть» mover разбирает сам: целая копия — это ok, неполная —
		// incomplete. До mover'а доходил один EEXIST, и по нему нельзя было
		// сказать, цела ли копия; теперь неполная приходит отдельным кодом.
		// И тот, и другой для планировщика значат одно: на ленте лежит то, что
		// нельзя ни принять, ни молча повторить (повтор даёт тот же ответ,
		// объекты висят в 'archiving' и держат landing). Удалить неполную и
		// отправить заново — можно только умея надёжно узнать состояние
		// копии (#101, stat по oid), поэтому пока блокируем пачку.
		// через срок захвата переслало бы её целиком (гигабайты по сети),
		// получило бы тот же exists — и по кругу. Раз в несколько минут,
		// бесконечно.
		a.logger().Error("пачка существует в Phobos, итог требует проверки",
			"pack", packID.String(), "oid", packID.String(), "spec", a.Spec)
		reason := fmt.Sprintf("phobos exists (oid=%s): нужен разбор stat по oid", packID)
		if err := a.Meta.BlockPack(ctx, packID, reason); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("mover: пачка %s уже существует в Phobos — заблокирована, повтор её не решит", packID)
	}
	if res.Code != mover.CodeOK {
		if definiteRefusals[res.Code] {
			// Известно, что на ленту ничего не легло, — выбрасывать можно.
			a.Meta.FailPackArchive(ctx, packID, ids, "mover: "+string(res.Code))
			return nil, fmt.Errorf("mover отверг пачку: %s", res.Code)
		}
		// Неизвестный код — не «отказ», а «не понятно». Пачка остаётся.
		return nil, fmt.Errorf("mover не ответил ok: %s", res.Code)
	}

	raw, err := hex.DecodeString(plan.SHA256)
	if err != nil {
		return nil, fmt.Errorf("migrator: сумма плана не hex: %w", err)
	}
	// С7: ожидаемая сумма уезжает в БД вместе с итогом. Позже её не взять —
	// данные уже на ленте, а reconciler сверяет именно её.
	// Носитель приходит из extents ответа mover'а (ADR-0008). Если ленты в
	// реестре нет, копия остаётся в 'writing': выдумывать носитель нельзя.
	var mediumID int64
	if len(res.Extents) > 0 {
		mediumID, err = a.Meta.MediaIDByLabel(ctx, res.Extents[0])
		if err != nil {
			return nil, err
		}
		if mediumID == 0 {
			// Данные легли на ленту, которой нет в нашем реестре. Причина
			// всегда одна: ленту добавили в Phobos в обход tapegw, и теги
			// разъехались. Повторно писать нельзя — данные уже на ленте;
			// копия остаётся в 'writing' и будет доведена до 'done'
			// (BindCopyMedium), когда ленту зарегистрируют штатно.
			//
			// Счётчика метрик пока нет: пакет metrics — каркас до M9. Алерт
			// идёт ERROR-строкой в JSON-лог, на него и настраивается реакция,
			// пока метрики нет.
			a.logger().Error("копия записана на ленту вне реестра",
				"label", res.Extents[0], "pack", packID.String(), "spec", a.Spec)
		}
	}
	if err := a.Meta.FinishPackArchive(ctx, packID, metaPlacements(placements), a.Spec, packID.String(), res.Version, a.Format, raw, mediumID, a.WorkerID); err != nil {
		return nil, err
	}
	return plan, nil
}

// LoadItems поднимает участников по id: файлы на landing, обёрнутые DEK,
// длина шифротекста. Его пишет воркер M4 — и продолжению нужен ровно тот же
// загрузчик, что и обычной архивации, иначе «продолжили не так» даст другие
// байты и сумма не сойдётся.
type LoadItems func(ctx context.Context, ids []uuid.UUID) ([]Item, error)

// ResumePacks — точка входа восстановления после перезапуска.
//
// Вход от пачки, а не от объектов, и это принципиально: участники запечатанной
// пачки сидят в 'archiving' с pack_id, в батч не попадают (ClaimMigrationBatch
// берёт только 'pending') и в pending не возвращаются (инвариант 2). Если искать
// по списку объектов, список взять неоткуда, и пачка останется в 'sealed'
// навсегда — то самое тихое зависание, только в другом месте.
//
// Возвращает число доведённых пачек и первую ошибку, если она была: одна
// разбороненная пачка не должна мешать остальным.
func (a *Archiver) ResumePacks(ctx context.Context, processID string, ttl time.Duration,
	load LoadItems, attrs map[string]string) (int, error) {

	if load == nil {
		return 0, fmt.Errorf("migrator: ResumePacks без загрузчика участников")
	}
	done, firstErr := 0, error(nil)
	for {
		packID, err := a.Meta.ClaimSealedPack(ctx, processID, ttl)
		if err != nil {
			return done, err
		}
		if packID == uuid.Nil {
			return done, firstErr
		}
		if err := a.resumeOne(ctx, packID, load, attrs); err != nil {
			a.logger().Error("продолжение пачки не удалось",
				"pack", packID.String(), "err", err.Error())
			if firstErr == nil {
				firstErr = err
			}
		} else {
			done++
		}
		if err := a.Meta.ReleasePack(ctx, packID); err != nil {
			return done, err
		}
	}
}

func (a *Archiver) resumeOne(ctx context.Context, packID uuid.UUID, load LoadItems, attrs map[string]string) error {
	// Порядок берётся из пачки по смещениям: он определяет байты, а смещения
	// зафиксированы при запечатывании.
	members, err := a.Meta.SealedMembers(ctx, packID)
	if err != nil {
		return err
	}
	if len(members) == 0 {
		return fmt.Errorf("migrator: пачка %s запечатана без участников", packID)
	}
	items, err := load(ctx, members)
	if err != nil {
		return fmt.Errorf("migrator: загрузить участников пачки %s: %w", packID, err)
	}
	// Порядок — не деталь вежливости: он определяет байты. Загрузчик обязан
	// вернуть участников в том же порядке, что и запросили, и проверка здесь
	// стоит ровно потому, что перестановка всплывёт только как «сумма не
	// сошлась» где-то далеко от причины.
	for i, id := range members {
		if items[i].Version != id {
			return fmt.Errorf("migrator: загрузчик вернул %s на месте %s — порядок определяет байты пачки",
				items[i].Version, id)
		}
	}
	plan, err := Measure(items)
	if err != nil {
		return fmt.Errorf("migrator: пересобрать пачку %s: %w", packID, err)
	}
	_, err = a.deliver(ctx, packID, items, plan, attrs)
	return err
}
