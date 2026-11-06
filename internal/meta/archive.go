package meta

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Placement — где участник лежит внутри пачки. Ставится в objects одной
// транзакцией с переходом в archived.
type Placement struct {
	Version uuid.UUID
	Offset  uint64
	Length  uint64
}

// BeginPackArchive заводит строку пачки и переводит участников в archiving.
//
// Возвращает открытую транзакцию: вызывающий обязан закоммитить её ДО вызова
// mover (правило 13). Батч на 200 GiB идёт минуты, и висящая в это время
// транзакция держит и строки участников, и пачку, а параллельный PUT тех же
// ключей упрётся в advisory-лок.
//
// Транзакция возвращается открытой намеренно: если бы она коммитилась здесь,
// у вызывающего не было бы способа нарушить правило 13, и тест на него
// проверял бы не то.
func (m *Meta) BeginPackArchive(ctx context.Context, poolID int64, ids []uuid.UUID) (pgx.Tx, uuid.UUID, error) {
	if len(ids) == 0 {
		return nil, uuid.Nil, fmt.Errorf("meta: пустая пачка")
	}
	tx, err := m.Pool.Begin(ctx)
	if err != nil {
		return nil, uuid.Nil, fmt.Errorf("meta: начать архивацию: %w", err)
	}

	// id генерируется здесь, а не DEFAULT-ом: у packs его нет (миграция 0017
	// уже слита, править её нельзя), а вставить пачку без id — NOT NULL.
	packID := uuid.New()
	_, err = tx.Exec(ctx, `
INSERT INTO packs (id, pool_id, kind, state)
VALUES ($1, $2, 'data', 'open')`, packID, poolID)
	if err != nil {
		tx.Rollback(ctx)
		return nil, uuid.Nil, fmt.Errorf("meta: завести пачку: %w", err)
	}

	// pack_id здесь НЕ ставится: CHECK objects_pack_member требует смещение и
	// длину вместе со ссылкой, а смещения становятся известны только после
	// второго прохода. Ссылка без адреса ничего не стоит, поэтому её ставит
	// FinishPackArchive — одной транзакцией с переходом в archived.
	//
	// Переход идемпотентен (правило 7): ноль затронутых строк означает, что
	// объект уже забрал другой воркер, и брать его нельзя.
	tag, err := tx.Exec(ctx, `
UPDATE objects SET state = 'archiving'
WHERE id = ANY($1::uuid[]) AND state = 'pending'`, ids)
	if err != nil {
		tx.Rollback(ctx)
		return nil, uuid.Nil, fmt.Errorf("meta: перевести в archiving: %w", err)
	}
	if int(tag.RowsAffected()) != len(ids) {
		tx.Rollback(ctx)
		return nil, uuid.Nil, fmt.Errorf("meta: из %d объектов в archiving ушло %d — часть уже забрана другим воркером",
			len(ids), tag.RowsAffected())
	}
	return tx, packID, nil
}

// FinishPackArchive записывает итог: копия принята, участники архивированы.
//
// sha256 — ожидаемая сумма потока Phobos, посчитанная migrator'ом. Она
// обязательна для done-копии (CHECK в миграции 0019): без неё reconciler не
// сможет проверить чтение с ленты, а задним числом сумму не восстановить —
// данные уже на ленте.
// mediumID — id ленты из media, 0 означает «носитель ещё не известен»:
// копия пишется в state='writing', а не 'done'. CHECK
// archive_copies_done_has_medium не позволяет объявить копию готовой без
// носителя, и это верно: 'done' без ответа на вопрос «где лежит» невозможно
// ни прочитать, ни проверить.
func (m *Meta) FinishPackArchive(ctx context.Context, packID uuid.UUID, placements []Placement, spec, phobosOID string, version, format int, sha256 []byte, mediumID int64) error {
	if len(sha256) != 32 {
		return fmt.Errorf("meta: сумма копии %d байт, ждали 32", len(sha256))
	}
	if len(placements) == 0 {
		return fmt.Errorf("meta: пачка без участников")
	}
	tx, err := m.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("meta: начать итог пачки: %w", err)
	}
	defer tx.Rollback(ctx)

	// Одна транзакция на пачку: копия и состояние участников должны появиться
	// вместе, иначе объект «архивирован», а копии нет, и восстановить её уже
	// нечем.
	state, doneAt := "writing", any(nil)
	if mediumID != 0 {
		state, doneAt = "done", "now()"
	}
	_, err = tx.Exec(ctx, fmt.Sprintf(`
INSERT INTO archive_copies (pack_id, spec, phobos_oid, phobos_version, format, state, sha256, medium_id, done_at)
VALUES ($1, $2, $3, $4, $5, '%s', $6, NULLIF($7, 0), %s)`, state, doneAt),
		packID, spec, phobosOID, version, format, sha256, mediumID)
	if err != nil {
		return fmt.Errorf("meta: записать копию пачки: %w", err)
	}

	for _, pl := range placements {
		if pl.Length == 0 {
			tx.Rollback(ctx)
			return fmt.Errorf("meta: участник %s нулевой длины — CHECK objects_pack_member не примет такую ссылку", pl.Version)
		}
		tag, err := tx.Exec(ctx, `
UPDATE objects SET state = 'archived', archived_at = now(),
       pack_id = $1, pack_offset = $2, pack_length = $3
WHERE id = $4 AND state = 'archiving'`,
			packID, pl.Offset, pl.Length, pl.Version)
		if err != nil {
			return fmt.Errorf("meta: перевести в archived: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("meta: участник %s не из этой пачки", pl.Version)
		}
	}
	if _, err := tx.Exec(ctx, `
-- live_bytes приравнивается total_bytes прямо здесь: пачка только что
    -- записана, мёртвых байт в ней быть не может, а планировщик ёмкости читает
    -- именно live_bytes.
UPDATE packs SET state = 'sealed',
       total_bytes = (SELECT coalesce(sum(pack_length), 0) FROM objects WHERE pack_id = $1),
       live_bytes  = (SELECT coalesce(sum(pack_length), 0) FROM objects WHERE pack_id = $1),
       members_total = (SELECT count(*) FROM objects WHERE pack_id = $1),
       members_live  = (SELECT count(*) FROM objects WHERE pack_id = $1)
WHERE id = $1`, packID); err != nil {
		return fmt.Errorf("meta: закрыть пачку: %w", err)
	}
	return tx.Commit(ctx)
}

// FailPackArchive возвращает участников в pending после отказа mover.
func (m *Meta) FailPackArchive(ctx context.Context, packID uuid.UUID, ids []uuid.UUID, reason string) error {
	tx, err := m.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("meta: начать откат пачки: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
UPDATE objects SET state = 'pending', pack_id = NULL, last_error = $2
WHERE id = ANY($1::uuid[]) AND state = 'archiving'`, ids, reason); err != nil {
		return fmt.Errorf("meta: вернуть в pending: %w", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM packs WHERE id = $1`, packID); err != nil {
		return fmt.Errorf("meta: убрать пустую пачку: %w", err)
	}
	return tx.Commit(ctx)
}

// MediaIDByLabel переводит имя носителя из extents mover'а в id строки media.
// Возвращает 0 без ошибки, если ленты в реестре нет: migrator тогда пишет копию
// в 'writing', а не выдумывает носитель.
func (m *Meta) MediaIDByLabel(ctx context.Context, label string) (int64, error) {
	var id int64
	err := m.Pool.QueryRow(ctx, `SELECT id FROM media WHERE label = $1`, label).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("meta: лента %s: %w", label, err)
	}
	return id, nil
}

// BindCopyMedium доводит копию из 'writing' до 'done', когда лента наконец
// зарегистрирована в реестре. Перезаписи не происходит: данные лежат на ленте
// с момента записи, не хватало только строки media.
func (m *Meta) BindCopyMedium(ctx context.Context, copyID, mediaID int64) error {
	tag, err := m.Pool.Exec(ctx, `
UPDATE archive_copies SET medium_id = $2, state = 'done', done_at = now()
WHERE id = $1 AND state = 'writing'`, copyID, mediaID)
	if err != nil {
		return fmt.Errorf("meta: привязать ленту к копии: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("meta: копия %d не в writing", copyID)
	}
	return nil
}
