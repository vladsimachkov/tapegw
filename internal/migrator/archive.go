package migrator

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"

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

// inOrder расставляет участников в заданном порядке. Используется при
// продолжении: порядок запечатан в пачке, а батч мог прийти в другом.
func inOrder(items []Item, order []uuid.UUID) ([]Item, error) {
	byID := make(map[uuid.UUID]Item, len(items))
	for _, it := range items {
		byID[it.Version] = it
	}
	out := make([]Item, 0, len(order))
	for _, id := range order {
		it, ok := byID[id]
		if !ok {
			return nil, fmt.Errorf("migrator: участник %s запечатан, но его нет в батче — файл потерян?", id)
		}
		out = append(out, it)
	}
	if len(out) != len(items) {
		return nil, fmt.Errorf("migrator: в батче %d участников, в пачке %d — состав разбежался", len(items), len(out))
	}
	return out, nil
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
	ids := make([]uuid.UUID, 0, len(items))
	for _, it := range items {
		ids = append(ids, it.Version)
	}

	// Шаг 1 — или новый батч, или продолжение запечатанной пачки после сбоя.
	// Второй случай и есть причина всей схемы: без него следующий цикл завёл бы
	// новую пачку с новым oid, и первая запись осталась бы на ленте никем не
	// учтённой.
	packID, err := a.Meta.PackOfArchiving(ctx, ids)
	if err != nil {
		return nil, err
	}
	resumed := packID != uuid.Nil
	if resumed {
		// Порядок участников берётся из пачки, а не из пришедшего батча:
		// байты (и сумма) определяются порядком, а запечатаны они уже.
		order, err := a.Meta.SealedMembers(ctx, packID)
		if err != nil {
			return nil, err
		}
		items, err = inOrder(items, order)
		if err != nil {
			return nil, err
		}
	} else {
		tx, freshID, err := a.Meta.BeginPackArchive(ctx, poolID, ids)
		if err != nil {
			return nil, err
		}
		packID = freshID
		// Страховка от утечки соединения: любой ранний выход до коммита обязан
		// закрыть транзакцию, иначе пул остаётся с занятым соединением и
		// pool.Close() в cleanup тестов висит навсегда. После коммита откат —
		// ErrTxClosed, его не смотрим.
		defer tx.Rollback(ctx) //nolint:errcheck

		// Коммит здесь — смысл правила 13. Если перенести вызов mover'а выше
		// этой строки, транзакция будет держать строки участников на всё время
		// записи, и тест С8 это ловит явно: NOWAIT на строку объекта 55P03.
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("migrator: закоммитить переход: %w", err)
		}
	}

	// Шаг 2 — первый проход: размер, сумма и смещения.
	plan, err := Measure(items)
	if err != nil {
		a.Meta.FailPackArchive(ctx, packID, ids, "сборка: "+err.Error())
		return nil, err
	}

	// Шаг 3 — запечатать ДО отправки. Именно здесь у объектов появляется
	// pack_id с адресом, и с этого момента сбой оставляет достаточное, чтобы
	// повтор продолжил эту же пачку.
	if !resumed {
		if err := a.Meta.SealPack(ctx, packID, metaPlacements(plan.Placements)); err != nil {
			a.Meta.FailPackArchive(ctx, packID, ids, "запечатывание: "+err.Error())
			return nil, err
		}
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

	res, err := a.sendPack(ctx, packID, plan, attrs, buf.Bytes())
	if err != nil {
		// Итог неизвестен. Пачку НЕ выбрасываем: если запись успела пройти,
		// объекты остаются в archiving, а строка пачки живёт, и следующий
		// цикл придёт с тем же oid. Разбор — ниже.
		res, err = a.resolveUnknown(ctx, packID, plan, attrs, buf.Bytes(), err)
		if err != nil {
			return nil, err
		}
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
	if err := a.Meta.FinishPackArchive(ctx, packID, metaPlacements(placements), a.Spec, packID.String(), res.Version, a.Format, raw, mediumID); err != nil {
		return nil, err
	}
	return plan, nil
}
