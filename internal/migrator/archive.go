package migrator

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/mover"
)

// Putter — то, что migrator просит у mover'а. Интерфейс, а не *mover.Client:
// тест правила 13 подставляет того, кто в момент put проверяет блокировку строк
// (С8), и делать для этого TLS не нужно.
type Putter interface {
	Put(in mover.PutIn) (*mover.Result, error)
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

	tx, packID, err := a.Meta.BeginPackArchive(ctx, poolID, ids)
	if err != nil {
		return nil, err
	}
	// Страховка от утечки соединения: любой ранний выход до коммита обязан
	// закрыть транзакцию, иначе пул остаётся с занятым соединением и
	// pool.Close() в cleanup тестов висит навсегда. После коммита откат —
	// ErrTxClosed, его не смотрим.
	defer tx.Rollback(ctx) //nolint:errcheck

	// Коммит здесь — смысл правила 13. Если перенести вызов mover'а выше этой
	// строки, транзакция будет держать строки участников на всё время записи,
	// и тест С8 это ловит явно: NOWAIT на строку объекта падает с 55P03.
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("migrator: закоммитить переход: %w", err)
	}

	plan, err := Measure(items)
	if err != nil {
		a.Meta.FailPackArchive(ctx, packID, ids, "сборка: "+err.Error())
		return nil, err
	}

	var buf bytes.Buffer
	placements, err := BuildIndexed(&buf, items, plan)
	if err != nil {
		a.Meta.FailPackArchive(ctx, packID, ids, "второй проход: "+err.Error())
		return nil, err
	}

	res, err := a.Put.Put(mover.PutIn{
		Op:     packID,
		ID:     packID,
		Size:   plan.Size,
		SHA256: plan.SHA256,
		Attrs:  attrs,
		Body:   bytes.NewReader(buf.Bytes()),
	})
	if err != nil {
		// Транспортная ошибка — итог неизвестен, но транзакции мы уже не
		// держим, поэтому откат безопасен: повтор упрётся в EEXIST и
		// разберётся через stat.
		a.Meta.FailPackArchive(ctx, packID, ids, "put: "+err.Error())
		return nil, err
	}
	if res.Code != mover.CodeOK {
		a.Meta.FailPackArchive(ctx, packID, ids, "mover: "+string(res.Code))
		return nil, fmt.Errorf("mover отверг пачку: %s", res.Code)
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
	}
	if err := a.Meta.FinishPackArchive(ctx, packID, metaPlacements(placements), a.Spec, packID.String(), res.Version, a.Format, raw, mediumID); err != nil {
		return nil, err
	}
	return plan, nil
}
