package meta

import (
	"context"
	"fmt"
)

// WriteSpace — ответ на вопрос «есть ли куда писать» ДО отправки put.
//
// Определение «доступна для записи» живёт в представлении media_write_targets
// (миграция 0020) и нигде больше: запрос ниже присоединяет представление, а не
// пересказывает его предикат. Иначе через месяц их стало бы три, и потерялась бы
// именно свежая лента в formatted.
//
// Поколение — параметр, а не свойство ленты: запрет поколения берётся из политики
// пула в конфиге. Поэтому gen_ok считается снаружи и в определение не входит.
type WriteSpace struct {
	PoolID int64

	// Total — все ленты пула, любого состояния.
	Total int
	// Writable — ленты, доступные для записи по определению и не запрещённого
	// поколения.
	Writable int
	// FreeBytes — суммарное место на writable-лентах.
	FreeBytes uint64
	// MaxFree — место на самой просторной writable-ленте. Важно отдельно:
	// запись идёт с no_split, то есть объект целиком должен влезть на одну
	// ленту, и «суммарно много» при маленьком MaxFree ничего не значит.
	MaxFree uint64
	// ReadOnly — ленты нужного поколения, но с запретом записи (снят put_access
	// или read_only). Именно их migrator обязан отличить от «места нет»:
	// Phobos на такую ленту тоже отвечает ENOSPC (спайк M0-12).
	ReadOnly int
	// Forbidden — ленты пула с поколением, запрещённым политикой.
	Forbidden int
}

// Reason — короткая классификация для алерта и для решения «пробовать ли вообще».
func (w WriteSpace) Reason() string {
	switch {
	case w.Total == 0:
		return "no_media"
	case w.Writable > 0:
		return "ok"
	case w.ReadOnly > 0 || w.Forbidden > 0:
		// Ленты в пуле есть, но писать некуда: все закрыты или все запрещены по
		// поколению. Это не «нет места» и не лечится ожиданием.
		return "closed"
	default:
		return "no_space"
	}
}

// Fits reports whether an object of need bytes can be placed on a single tape.
func (w WriteSpace) Fits(need uint64) bool {
	return w.Writable > 0 && w.MaxFree >= need
}

// PoolWriteSpace считает, сколько места доступно для записи в пуле.
//
// generations — поколения, разрешённые политикой пула; пустой список означает
// «любые»: политика может ничего не запрещать, и тогда запрещать нечего.
//
// library — библиотека, в которую пул пишет по своей политике (library_write);
// nil означает «любая». Это фильтр того же ряда, что и поколение: свойство
// политики, а не строки media. Пока библиотек одна, фильтр ничего не меняет, но
// при переезде (С5) ленты пула окажутся в двух библиотеках, и место в старой не
// должно считаться местом для записи.
func (m *Meta) PoolWriteSpace(ctx context.Context, poolID int64, generations []string, library *string) (*WriteSpace, error) {
	const q = `
WITH s AS (
    SELECT (t.id IS NOT NULL)                              AS writable,
           (m.read_only_reason IS NOT NULL)                AS closed,
           -- Пустой список поколений означает «любые». Пустой — потому что
           -- nil-слайс в pgx уезжает как SQL NULL, а не '{}', и без coalesce
           -- сравнение дало бы NULL, а writable AND NULL перестал бы считать
           -- всё: пул показал бы «нет места» при полном-то пуле.
           (cardinality(coalesce($2::text[], '{}')) = 0
            OR m.generation = ANY($2::text[])) AS gen_ok,
           coalesce(m.free_bytes_cached, 0)                AS free
    FROM media m
    LEFT JOIN media_write_targets t ON t.id = m.id
    WHERE m.pool_id = $1
      AND ($3::text IS NULL OR m.library_cached = $3)
)
SELECT count(*)::int                                            AS total,
       count(*) FILTER (WHERE writable AND gen_ok)::int         AS writable,
       coalesce(sum(free) FILTER (WHERE writable AND gen_ok), 0)::bigint AS free_bytes,
       coalesce(max(free) FILTER (WHERE writable AND gen_ok), 0)::bigint AS max_free,
       count(*) FILTER (WHERE closed AND gen_ok)::int           AS read_only,
       count(*) FILTER (WHERE NOT gen_ok)::int                  AS forbidden
FROM s`

	var out WriteSpace
	err := m.Pool.QueryRow(ctx, q, poolID, generations, library).
		Scan(&out.Total, &out.Writable, &out.FreeBytes, &out.MaxFree, &out.ReadOnly, &out.Forbidden)
	if err != nil {
		return nil, fmt.Errorf("meta: место пула %d: %w", poolID, err)
	}
	out.PoolID = poolID
	return &out, nil
}

// WritableTape — лента, на которую можно писать, для отчётов и планировщика.
type WritableTape struct {
	ID         int64
	Label      string
	Generation string
	FreeBytes  uint64
}

// WritableTapes перечисляет ленты пула, доступные для записи, по убыванию места.
// Фильтры те же, что у PoolWriteSpace: поколение и библиотека записи.
func (m *Meta) WritableTapes(ctx context.Context, poolID int64, generations []string, library *string) ([]WritableTape, error) {
	const q = `
SELECT t.id, t.label, t.generation, t.free_bytes
FROM media_write_targets t
WHERE t.pool_id = $1
  AND (cardinality(coalesce($2::text[], '{}')) = 0 OR t.generation = ANY($2::text[]))
  AND ($3::text IS NULL OR t.library_cached = $3)
ORDER BY t.free_bytes DESC, t.id`

	rows, err := m.Pool.Query(ctx, q, poolID, generations, library)
	if err != nil {
		return nil, fmt.Errorf("meta: ленты для записи: %w", err)
	}
	defer rows.Close()

	var out []WritableTape
	for rows.Next() {
		var w WritableTape
		if err := rows.Scan(&w.ID, &w.Label, &w.Generation, &w.FreeBytes); err != nil {
			return nil, fmt.Errorf("meta: чтение ленты: %w", err)
		}
		out = append(out, w)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("meta: обход лент: %w", err)
	}
	return out, nil
}

// MarkMediaReadOnly снимает разрешение записи и обязательно объясняет почему.
//
// Причина не может остаться пустой: read_only без причины — это второй источник
// правды о запрете рядом с lifecycle, и CHECK в миграции 0018 это запрещает.
func (m *Meta) MarkMediaReadOnly(ctx context.Context, id int64, reason string) error {
	if reason == "" {
		return fmt.Errorf("meta: причина read_only обязательна")
	}
	tag, err := m.Pool.Exec(ctx, `
UPDATE media SET lifecycle = 'read_only', read_only_reason = $2
WHERE id = $1 AND lifecycle <> 'retired'`, id, reason)
	if err != nil {
		return fmt.Errorf("meta: закрыть ленту %d: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("meta: лента %d не найдена или уже выведена", id)
	}
	return m.logMediaEvent(ctx, id, "read_only", reason)
}

// MarkMediaWritable снимает запрет записи, возвращая ленту в active.
func (m *Meta) MarkMediaWritable(ctx context.Context, id int64) error {
	tag, err := m.Pool.Exec(ctx, `
UPDATE media SET lifecycle = 'active', read_only_reason = NULL
WHERE id = $1 AND lifecycle = 'read_only'`, id)
	if err != nil {
		return fmt.Errorf("meta: открыть ленту %d: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("meta: лента %d не в read_only", id)
	}
	return m.logMediaEvent(ctx, id, "writable", "")
}

func (m *Meta) logMediaEvent(ctx context.Context, mediaID int64, event, detail string) error {
	// detail — jsonb (миграция 0018), поэтому объект собирает БД: подстановка
	// строки в jsonb ловит ошибку только в рантайме и только на нетипичном
	// тексте причины.
	_, err := m.Pool.Exec(ctx, `
INSERT INTO media_events (media_id, event, detail)
VALUES ($1, $2, jsonb_build_object('reason', $3::text))`, mediaID, event, detail)
	if err != nil {
		return fmt.Errorf("meta: событие ленты %d: %w", mediaID, err)
	}
	return nil
}
