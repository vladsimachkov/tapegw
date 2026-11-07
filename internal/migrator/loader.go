package migrator

import (
	"context"
	"fmt"
	"io"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/spool"
)

// NewLoader — загрузчик участников пачки из landing (контракт LoadItems).
//
// Участник — файл объекта целиком: шифротекст + зашифрованный манифест +
// футер (ADR-0019, ADR-0020), а не только шифротекст. Причина не в удобстве:
// соль частей, из которой строится nonce каждого сегмента (#38), имя бакета и
// ключа, ETag и пользовательские метаданные лежат только в манифесте — в
// pax-заголовках участника их нет. Без манифеста данные на ленте не
// расшифровать, если БД потеряна, а восстановитель по tar-заголовкам (ADR-0020)
// иначе просто нечем читать.
//
// Длину участника объявляет БД (objects.file_bytes — её пишет PUT в той же
// транзакции, что и строку объекта), а не stat файла: источник истины — та
// транзакция, что записывала файл. Файл при этом проверяется: сборщик пачки
// сверяет объявленную длину с фактической в обе стороны (#63), и расходится
// это на первом же проходе, до отправки mover'у.
func NewLoader(m *meta.Meta, sp *spool.Spool) LoadItems {
	return func(ctx context.Context, ids []uuid.UUID) ([]Item, error) {
		rows, err := m.PackItems(ctx, ids)
		if err != nil {
			return nil, err
		}
		items := make([]Item, 0, len(rows))
		for _, r := range rows {
			if r.FileBytes == nil || *r.FileBytes <= 0 {
				// Отказ, а не «возьмём по stat»: длина из БД — та, что
				// видела запись файла. Угадать её по месту значит молча
				// обрезать манифест, если файл и строка разошлись.
				return nil, fmt.Errorf(
					"migrator: у участника %s нет file_bytes — объект не архивируется", r.Version)
			}
			id := r.Version
			items = append(items, Item{
				Version:    id,
				Tenant:     uint64(r.Tenant),
				VersionSeq: uint64(r.VersionSeq),
				PlainLen:   uint64(r.PlainLen),
				KekVersion: uint64(r.KekVersion),
				WrappedDEK: r.WrappedDEK,
				FileLen:    uint64(*r.FileBytes),
				Open:       func() (io.ReadCloser, error) { return sp.Open(id) },
			})
		}
		return items, nil
	}
}
