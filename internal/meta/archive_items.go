package meta

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

// PackItem — набор данных об одной версии, нужный загрузчику пачки
// (migrator.LoadItems). Отдельная структура, а не Object: загрузчику не нужны
// etag, метаданные и блокировки, зато нужен kek_version тенанта — он лежит в
// tenants, и тащить его через Object означало бы второй источник версии KEK.
type PackItem struct {
	Version    uuid.UUID
	Tenant     int64
	VersionSeq int64
	PlainLen   int64
	KekVersion int64
	WrappedDEK []byte
	// FileBytes — длина файла на диске (миграция 0023). nil у строк,
	// записанных до колонки: загрузчик обязан отказать такой участник, а не
	// угадать длину — угаданная длина обрежет манифест на ленте.
	FileBytes *int64
}

// PackItems поднимает участников пачки по id **в том же порядке, что и ids**.
//
// Порядок — часть контракта: пачка детерминирована (ADR-0020), и перестановка
// участников меняет байты, а значит и сумму. Пропущенный id — ошибка, а не
// «просто его нет»: загрузчик вызывается для участников уже запечатанной
// пачки, и молча пропущенный участник дал бы пачку без него.
func (m *Meta) PackItems(ctx context.Context, ids []uuid.UUID) ([]PackItem, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := m.Pool.Query(ctx,
		`SELECT o.id, o.tenant_id, o.version_seq, o.size, o.wrapped_dek, o.file_bytes,
		        t.kek_version
		   FROM objects o
		   JOIN tenants t ON t.id = o.tenant_id
		  WHERE o.id = ANY($1)`, ids)
	if err != nil {
		return nil, fmt.Errorf("meta: участники пачки: %w", err)
	}
	defer rows.Close()

	// Собираем в map, а раскладываем по ids: ANY() не обещает порядок, а
	// порядок — часть контракта.
	byID := make(map[uuid.UUID]PackItem, len(ids))
	for rows.Next() {
		var it PackItem
		if err := rows.Scan(&it.Version, &it.Tenant, &it.VersionSeq, &it.PlainLen,
			&it.WrappedDEK, &it.FileBytes, &it.KekVersion); err != nil {
			return nil, fmt.Errorf("meta: участники пачки: %w", err)
		}
		byID[it.Version] = it
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("meta: участники пачки: %w", err)
	}

	out := make([]PackItem, 0, len(ids))
	for _, id := range ids {
		it, ok := byID[id]
		if !ok {
			return nil, fmt.Errorf("meta: участник пачки %s не найден", id)
		}
		out = append(out, it)
	}
	return out, nil
}
