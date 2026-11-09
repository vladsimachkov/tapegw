package migrator

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/archive"
	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/mover"
	"github.com/vladsimachkov/tapegw/internal/store"
)

// Putter — то, что migrator просит у mover'а. Интерфейс, а не *mover.Client:
// тест правила 13 подставляет того, кто в момент put проверяет блокировку строк
// (С8), и делать для этого TLS не нужно.
type Putter interface {
	Put(in mover.PutIn) (*mover.Result, error)

	// Status нужен не для отчётов, а для разбора неизвестного итога: после
	// обрыва соединения tapegw не знает, записалась пачка или нет.
	Status(op uuid.UUID) (*mover.Result, error)

	// Release говорит mover'у, что удержанный буфер больше не нужен: все
	// копии записаны. Без него буфер висел бы до result_ttl.
	Release(op uuid.UUID) (*mover.Result, error)

	// StatObject и DeleteObject нужны #101: «объект есть» не значит «копия
	// цела», а решать «принять / убрать и переслать» без ответа Phobos нельзя.
	StatObject(id uuid.UUID) (*mover.Result, error)
	DeleteObject(id uuid.UUID) (*mover.Result, error)
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

	// Specs — все спецификации копий политики; Spec — первая, она пишется
	// обычным put и держит буфер (hold) для остальных. Пусто — копия одна.
	Specs []string

	// SpecTags — теги Phobos по спецификации: копии разводят по носителям
	// именно тегами (ADR-0013, ADR-0022). Без них «копия left» и «копия right»
	// лягут туда, куда решит Phobos по умолчанию, в том числе на одну сторону,
	// а вся цель разведения пропадёт.
	SpecTags map[string][]string

	// Копии пишет тот же mover, что держит буфер: реестр операций и буфер
	// принадлежат конкретному mover'у, и другой на from_op ответит not_found.
	// Параллельная запись копий разными драйвами требует общего буфера и
	// реестра на сервер библиотеки — это записано в ADR-0021 как целевая форма,
	// а не как то, что здесь забыли подключить.

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

	// LandingMinRetention — на сколько продлевается online_until при переходе
	// в archived (ADR-0003): показанная в HEAD дата обязана выполняться и
	// после архивации, иначе janitor вытеснит копию раньше обещания.
	LandingMinRetention time.Duration
}

// sendPack — одна попытка put. Ошибка здесь означает «итог неизвестен», а не
// «отказ»: различает их вызывающий, а не этот вызов.
func (a *Archiver) sendPack(ctx context.Context, packID uuid.UUID, plan *Plan, attrs map[string]string, body []byte) (*mover.Result, error) {
	return a.Put.Put(mover.PutIn{
		Op:     packID,
		ID:     packID, // oid Phobos = UUID пачки: повтор бьёт в тот же объект
		Size:   plan.Size,
		SHA256: plan.SHA256,
		Attrs:  a.attrsFor(attrs, a.Spec),
		Tags:   a.SpecTags[a.Spec],
		// Буфер держим, только если с него пойдут копии: иначе mover освободит
		// данные сразу, и второй копии нечего читать и не с чем сверять.
		Hold: len(a.copySpecs()) > 1,
		Body: bytes.NewReader(body),
	})
}

// copyOp — операция (и oid Phobos) одной копии. Детерминированная: UUIDv5 от
// пачки и имени копии, чтобы повтор после обрыва бился в тот же объект Phobos,
// а не плодил второй.
//
// У копии обязан быть свой oid: по ADR-0022 каждая копия — отдельный объект
// Phobos. Общего oid на все копии быть не может: вторая запись на тот же oid
// дала бы EEXIST, mover разобрал бы его как «целая копия» и вернул ok — то есть
// копия считается записанной, а на ленте ничего не легло.
//
// Имя вида «<uuid>.left» здесь не используется: протокол связывает oid объекта с
// UUID операции (иначе «продолжить передачу» и «записать заново» неразличимы),
// поэтому уникальность даёт сам UUID. См. ADR-0022, поправка.
func copyOp(packID uuid.UUID, spec string) uuid.UUID {
	return uuid.NewSHA1(packID, []byte(spec))
}

// attrsFor — атрибуты конкретной копии: к общим добавляется tapegw.copy, он
// пишется и когда копия одна (иначе rebuild-meta находит несколько
// неразличимых записей об одном ключе).
func (a *Archiver) attrsFor(base map[string]string, spec string) map[string]string {
	out := make(map[string]string, len(base)+1)
	for k, v := range base {
		out[k] = v
	}
	out[archive.AttrCopy] = spec
	return out
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
	return a.deliver(ctx, packID, items, plan, attrs, packUnit{}, a.Format)
}

// ArchiveLargeObject — один цикл архивации крупного объекта (≥ pack.threshold).
//
// Путь проходит через ту же машину, что пачка, и это не переиспользование кода
// ради кода: у пути крупного объекта не должно быть отдельного запаса
// устойчивости к сбоям. Запечатывание до отправки (SealPack), захват с
// продлением (heartbeat), разбор неизвестного итога (resolveUnknown /
// resolveByStat) и копии из одного буфера (hold / from_op) — те же функции.
// Отличаются байты (сырой файл вместо tar-пачки), oid (UUID версии вместо
// свежего UUID пачки) и атрибуты (шесть полей версии плюс копия).
func (a *Archiver) ArchiveLargeObject(ctx context.Context, poolID int64, it Item) (*Plan, error) {
	if a.Meta == nil || a.Put == nil {
		return nil, fmt.Errorf("migrator: Archiver без Meta или Put")
	}
	if a.WorkerID == "" || a.ClaimTTL <= 0 {
		return nil, fmt.Errorf("migrator: Archiver без ProcessID/ClaimTTL: пачку нельзя закрепить за собой")
	}
	// Атрибуты выводятся из участника, а не приходят извне: продолжение после
	// рестарта пересобирает их из загрузчика и обязано получить те же байты.
	attrs, err := objectAttrs(it)
	if err != nil {
		return nil, err
	}
	ids := []uuid.UUID{it.Version}

	tx, err := a.Meta.BeginObjectArchive(ctx, poolID, it.Version)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // после коммита нечего откатывать
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("migrator: закоммитить переход: %w", err)
	}
	// oid первой копии — UUID версии (ADR-0020), он же id строки packs.
	packID := it.Version

	plan, err := MeasureFile(it)
	if err != nil {
		a.Meta.FailPackArchive(ctx, packID, ids, "сборка: "+err.Error())
		return nil, err
	}
	if err := a.Meta.SealPack(ctx, packID, metaPlacements(plan.Placements), plan.Size, a.WorkerID, a.ClaimTTL); err != nil {
		a.Meta.FailPackArchive(ctx, packID, ids, "запечатывание: "+err.Error())
		return nil, err
	}
	return a.deliver(ctx, packID, []Item{it}, plan, attrs, objectUnit{}, archive.FormatVersion)
}

// deliver — шаги 4 и 5: второй проход, отправка, разбор неизвестного итога и
// запись результата. Транзакции во время вызова mover'а нет (правило 13).
// Общий для ArchivePack, ArchiveLargeObject и ResumePacks: продолжение обязано
// пройти ровно тот же путь, иначе «продолжили не так» превратится в вторую
// копию.
func (a *Archiver) deliver(ctx context.Context, packID uuid.UUID, items []Item, plan *Plan, attrs map[string]string, b unitBuilder, format int) (*Plan, error) {
	ids := make([]uuid.UUID, 0, len(items))
	for _, it := range items {
		ids = append(ids, it.Version)
	}
	// Шаг 4 — второй проход и отправка. Транзакции нет (правило 13).
	var buf bytes.Buffer
	placements, err := b.build(&buf, items, plan)
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
		// «Уже есть» и «неполная» — не приговор и не успех: надо спросить
		// Phobos. Разбор ниже; если разобрать нельзя, пачка блокируется.
		r2, err := a.resolveByStat(ctx, packID, plan, attrs, buf.Bytes())
		if err != nil {
			return nil, err
		}
		if r2 == nil {
			return nil, fmt.Errorf("mover: пачка %s в состоянии, которое нельзя разобрать по stat", packID)
		}
		res = r2
	}

	if res.Code != mover.CodeOK {
		if res.Code == mover.CodeNoBuffer {
			// Архивный юнит больше всего буфера mover'а: отказ приходит до
			// приёма байт (ADR-0021), на ленту ничего не легло. Пересылка
			// бессмысленна — пока оператор не поднимет buffer_bytes, отказ
			// будет тот же. Поэтому пачка не возвращается в очередь (она
			// крутила бы круг) и не отменяется (объекты никуда не делись), а
			// блокируется с причиной: объекты остаются в 'archiving' с
			// pack_id, причина видна в реестре, продолжение такую пачку не
			// берёт.
			a.logger().Error("буфер mover'а меньше архивного юнита — поднимите phobos.mover.buffer_bytes",
				"pack", packID.String(), "bytes", plan.Size)
			if err := a.Meta.BlockPack(ctx, packID,
				"mover: no_buffer — архивный юнит больше всего буфера mover"); err != nil {
				return nil, err
			}
			return nil, fmt.Errorf("mover: пачка %s заблокирована: юнит больше буфера mover", packID)
		}
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
	// С5: первая копия записана. Пачка ещё НЕ archived — «архивирован» значит
	// «записаны все копии политики», а не «одна легла».
	if _, err := a.Meta.RecordPackCopy(ctx, packID, a.Spec, packID.String(), res.Version, format, raw, mediumID); err != nil {
		return nil, err
	}
	if err := a.writeExtraCopies(ctx, packID, plan, attrs, format); err != nil {
		return nil, err
	}
	done, err := a.Meta.ArchivePackWhenCopiesDone(ctx, packID, metaPlacements(placements), a.copySpecs(), a.WorkerID, a.LandingMinRetention)
	if errors.Is(err, meta.ErrCopiesSameMedium) {
		// Копии на одной ленте — это не «не дошло», повтор даст тот же исход:
		// данные уже записаны, носитель не поменяется. Окончательный отказ,
		// как no_buffer: блокируем с причиной, оператор чинит теги лент или
		// политику и снимает блокировку. Считать такую пачку архивной —
		// значит отчитаться защитой, которой нет.
		a.logger().Error("копии пачки легли на один носитель — пачка заблокирована, проверьте теги лент и политику копий",
			"pack", packID.String(), "err", err.Error())
		if berr := a.Meta.BlockPack(ctx, packID, err.Error()); berr != nil {
			return nil, berr
		}
		return nil, fmt.Errorf("migrator: пачка %s заблокирована: %w", packID, err)
	}
	if err != nil {
		return nil, err
	}
	if !done {
		// С6: часть копий не дошла. Записанные остаются записанными, объекты
		// ждут в 'archiving', недостающие догоняет отдельный проход.
		a.logger().Warn("пачка записана не полностью — ждёт догонки копий",
			"pack", packID.String())
		return plan, nil
	}
	// Буфер держали ради копий (hold); копии готовы — освобождаем. Оставлять до
	// result_ttl нельзя: буфер считается из «двух батчей на драйв», и при двух
	// копиях он заполнится висящими пачками.
	if len(a.copySpecs()) > 1 {
		if _, err := a.Put.Release(packID); err != nil {
			a.logger().Error("освободить буфер не вышло — снимет result_ttl",
				"pack", packID.String(), "err", err.Error())
		}
	}
	return plan, nil
}

// resolveByStat — #101: что делать, когда Phobos отвечает «уже есть» или
// «неполно». Три исхода и почему именно они:
//
//   - complete — принимаем как успех. Данные на ленте и они целые; переписывать
//     их значит гонять гигабайты ради той же картинки;
//   - incomplete — удалить и отправить один раз. Неполный объект нельзя ни
//     принять, ни переслать поверх (EEXIST вернётся снова);
//   - not_found — отправить: реестр mover'а мог потеряться, а объекта там и нет.
//
// Повтор ровно один. Второй тот же исход — блокировка: бесконечный цикл
// «переслали, снова exists» жрёт сеть и ленту и ничего не чинит.
func (a *Archiver) resolveByStat(ctx context.Context, packID uuid.UUID, plan *Plan, attrs map[string]string, body []byte) (*mover.Result, error) {
	block := func(why string) error {
		a.logger().Error("итог по пачке нельзя разобрать", "pack", packID.String(), "why", why)
		return a.Meta.BlockPack(ctx, packID, "stat: "+why)
	}

	st, err := a.Put.StatObject(packID)
	if err != nil {
		return nil, fmt.Errorf("migrator: stat по пачке %s: %w", packID, err)
	}
	switch st.Code {
	case mover.CodeOK:
		// Целый объект с нужным oid — ещё не наша пачка. Сверяем экстенты с
		// планом, а не одно число: «две реплики вместо одной» и «объект другого
		// размера» — разные неисправности, и искать их надо по-разному.
		ext := make([]store.Extent, 0, len(st.ExtentInfo))
		for _, e := range st.ExtentInfo {
			ext = append(ext, store.Extent{Size: e.Size, Media: e.Media, State: e.State})
		}
		if err := store.CheckExtents(ext, plan.Size, 1); err != nil {
			if err := block(fmt.Sprintf("под oid пачки лежит не то, что мы планировали: %v", err)); err != nil {
				return nil, err
			}
			return nil, nil
		}
		return &mover.Result{Code: mover.CodeOK, Version: st.Version, Extents: st.Extents}, nil
	case mover.CodeIncomplete:
		del, err := a.Put.DeleteObject(packID)
		if err != nil {
			return nil, fmt.Errorf("migrator: удалить неполную пачку %s: %w", packID, err)
		}
		if del.Code != mover.CodeOK {
			if err := block("неполная копия не удаляется: " + string(del.Code)); err != nil {
				return nil, err
			}
			return nil, nil
		}
	case mover.CodeNotFound:
	default:
		if err := block("stat вернул " + string(st.Code)); err != nil {
			return nil, err
		}
		return nil, nil
	}

	res, err := a.sendPack(ctx, packID, plan, attrs, body)
	if err != nil {
		return nil, err
	}
	if res.Code != mover.CodeOK {
		if err := block("повтор после stat дал " + string(res.Code)); err != nil {
			return nil, err
		}
		return nil, nil
	}
	return res, nil
}

// copySpecs — спецификации копий политики. Если задан только Spec, копия одна.
func (a *Archiver) copySpecs() []string {
	if len(a.Specs) > 0 {
		return a.Specs
	}
	return []string{a.Spec}
}

// writeExtraCopies пишет остальные копии из буфера первой (Ф0.4): ленту и
// буфер читают один раз, байты по сети не идут.
//
// Сбой копии не откатывает остальные (С6): записанное на ленту остаётся,
// недостающая копия помечается и догоняется отдельно. Пачка при этом не
// 'archived' — это и есть сигнал «чего-то не хватает».
func (a *Archiver) writeExtraCopies(ctx context.Context, packID uuid.UUID, plan *Plan, attrs map[string]string, format int) error {
	specs := a.copySpecs()
	if len(specs) <= 1 {
		return nil
	}
	raw, err := hex.DecodeString(plan.SHA256)
	if err != nil {
		return fmt.Errorf("migrator: сумма плана не hex: %w", err)
	}
	for _, spec := range specs[1:] {
		mv := a.Put
		op := copyOp(packID, spec)
		res, err := mv.Put(mover.PutIn{
			Op: op, ID: op, Size: plan.Size, SHA256: plan.SHA256,
			Attrs:  a.attrsFor(attrs, spec),
			Tags:   a.SpecTags[spec],
			FromOp: packID,
		})
		if err != nil || res == nil || res.Code != mover.CodeOK {
			code := "transport"
			if res != nil {
				code = string(res.Code)
			}
			a.logger().Error("копия не записана, будет догнана",
				"pack", packID.String(), "spec", spec, "code", code, "err", errString(err))
			continue
		}
		var mediumID int64
		if len(res.Extents) > 0 {
			mediumID, err = a.Meta.MediaIDByLabel(ctx, res.Extents[0])
			if err != nil {
				return err
			}
		}
		// oid копии — её собственный объект Phobos, не oid пачки.
		if _, err := a.Meta.RecordPackCopy(ctx, packID, spec, op.String(), res.Version, format, raw, mediumID); err != nil {
			return err
		}
	}
	return nil
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
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
	// Признак single_object решает, какие байты пересобирать: сырой файл или
	// tar-пачку. Ошибка в эту ветку — не «сумма не сошлась где-то потом», а
	// второй объект на ленте под тем же oid с чужими байтами, поэтому выборка
	// идёт по явному флагу строки, а не по числу участников.
	single, err := a.Meta.PackSingleObject(ctx, packID)
	if err != nil {
		return err
	}
	var b unitBuilder = packUnit{}
	format := a.Format
	if single {
		if len(items) != 1 {
			return fmt.Errorf("migrator: пачка %s помечена как крупный объект, участников %d", packID, len(items))
		}
		b = objectUnit{}
		format = archive.FormatVersion
		// Атрибуты крупного объекта выводятся из участника — их обязан уметь
		// восстановить любой процесс, а не только тот, что начал архивацию.
		attrs, err = objectAttrs(items[0])
		if err != nil {
			return err
		}
	}
	plan, err := b.measure(items)
	if err != nil {
		return fmt.Errorf("migrator: пересобрать пачку %s: %w", packID, err)
	}
	_, err = a.deliver(ctx, packID, items, plan, attrs, b, format)
	return err
}
