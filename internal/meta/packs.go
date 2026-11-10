package meta

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Учёт живого в пачках (предложение 0011).
//
// Пачка — один объект для Phobos, поэтому `phobos tape repack` переносит её
// целиком, пока жив хоть один участник: уплотнение пачек — наша операция, и
// решает она по live_bytes. Если счётчик отстаёт от реальности, планировщик
// уплотняет полные пачки и оставляет пустые, а это ровно то, чего учёт и
// избегал.
//
// Поэтому уменьшение живых байт живёт не «рядом с удалением», а в том же
// запросе, который версию убирает: отдельным UPDATE после него — значит иметь
// окно, в котором строка уже удалена, а счётчик ещё нет, и падение в этом окне
// смещает учёт навсегда.

// markDeleting переводит подходящие строки в 'deleting' и в той же транзакции
// снимает их байты с живого остатка их пачек.
//
// Ключевой приём — data-modifying CTE: `gone` возвращает ровно те строки,
// которые переведены ЭТОЙ транзакцией. Отдельный UPDATE по pack_id после
// перевода выглядел бы так же, но при гонке (другой воркер уже перевёл строку,
// наш UPDATE не задел ни одной) уменьшил бы live_bytes повторно. Здесь ноль
// затронутых строк означает ноль списаний автоматически.
//
// extraSet — дополнительные присвоения для путей, которым нужно попутно снять
// признак null-версии; пустая строка — обычный случай.
func markDeleting(ctx context.Context, tx pgx.Tx, extraSet, where string, args ...any) (int64, error) {
	ids, err := markDeletingIDs(ctx, tx, extraSet, where, args...)
	return int64(len(ids)), err
}

// markDeletingIDs — тот же переход, но возвращает id переведённых версий.
//
// Нужен журналу: событие удаления обязано назвать версию, а отдельный SELECT
// «какие строки только что ушли в deleting» был бы вторым взглядом на уже
// изменённые данные — при гонке он вернул бы чужие строки. CTE отдаёт ровно
// то, что перевело ЭТО обращение.
func markDeletingIDs(ctx context.Context, tx pgx.Tx, extraSet, where string, args ...any) ([]uuid.UUID, error) {
	q := fmt.Sprintf(`
		WITH gone AS (
			UPDATE objects
			   SET is_current = false, state = 'deleting'%s
			 WHERE %s
			RETURNING id, pack_id, pack_length
		),
		drained AS (
			UPDATE packs p
			   SET live_bytes   = p.live_bytes - s.bytes,
			       members_live = p.members_live - s.n
			  FROM (SELECT pack_id,
			               COALESCE(SUM(pack_length), 0) AS bytes,
			               COUNT(*)                     AS n
			          FROM gone
			         WHERE pack_id IS NOT NULL
			         GROUP BY pack_id) s
			 WHERE p.id = s.pack_id
			RETURNING 1
		)
		SELECT id FROM gone`, extraSet, where)

	rows, err := tx.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("mark deleting: %w", err)
	}
	defer rows.Close()

	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("mark deleting: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("mark deleting: %w", err)
	}
	return ids, nil
}

// deleteMarkerRow физически убирает строку маркера удаления.
//
// Маркер нулевой и на ленту не едет, поэтому списывать байты не с чего, но
// запрос тот же: если маркер когда-нибудь получит собственное хранение, учёт
// не окажется забытым.
func deleteMarkerRow(ctx context.Context, tx pgx.Tx, id uuid.UUID) error {
	_, err := tx.Exec(ctx, `
		WITH gone AS (
			DELETE FROM objects WHERE id = $1 RETURNING pack_id, pack_length
		),
		drained AS (
			UPDATE packs p
			   SET live_bytes   = p.live_bytes - s.bytes,
			       members_live = p.members_live - s.n
			  FROM (SELECT pack_id,
			               COALESCE(SUM(pack_length), 0) AS bytes,
			               COUNT(*)                     AS n
			          FROM gone
			         WHERE pack_id IS NOT NULL
			         GROUP BY pack_id) s
			 WHERE p.id = s.pack_id
			RETURNING 1
		)
		SELECT 1`, id)
	if err != nil {
		return fmt.Errorf("delete marker row: %w", err)
	}
	return nil
}

// PackStats — состояние пачки глазами БД. Нужен планировщику уплотнения и
// `tapegw admin verify`: инвариант надо уметь проверить не только в тесте.
type PackStats struct {
	ID           uuid.UUID
	State        string
	TotalBytes   int64
	LiveBytes    int64
	MembersTotal int
	MembersLive  int
	// LiveFromMembers — сумма pack_length участников, которые ещё не удаляются.
	// С ней сверяется LiveBytes; расхождение значит, что учёт разошёлся.
	LiveFromMembers int64
}

// StatsPack возвращает пачку и независимый пересчёт её живых байт.
func (m *Meta) StatsPack(ctx context.Context, id uuid.UUID) (*PackStats, error) {
	row := m.Pool.QueryRow(ctx, `
		SELECT p.id, p.state::text, p.total_bytes, p.live_bytes,
		       p.members_total, p.members_live,
		       COALESCE((SELECT sum(o.pack_length) FROM objects o
		                  WHERE o.pack_id = p.id
		                    AND o.state NOT IN ('deleting', 'deleted')), 0)
		  FROM packs p WHERE p.id = $1`, id)

	var s PackStats
	var live int64
	if err := row.Scan(&s.ID, &s.State, &s.TotalBytes, &s.LiveBytes,
		&s.MembersTotal, &s.MembersLive, &live); err != nil {
		if err == pgx.ErrNoRows {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("pack stats %s: %w", id, err)
	}
	s.LiveFromMembers = live
	return &s, nil
}

// BrokenPacks — пачки, у которых учёт живого не сходится с суммой участников.
// Это не «мелкое расхождение»: по live_bytes планировщик выбирает, что
// уплотнять, и неверный счётчик означает уплотнение не тех лент.
func (m *Meta) BrokenPacks(ctx context.Context) ([]PackStats, error) {
	rows, err := m.Pool.Query(ctx, `
		SELECT p.id, p.state::text, p.total_bytes, p.live_bytes,
		       p.members_total, p.members_live,
		       COALESCE((SELECT sum(o.pack_length) FROM objects o
		                  WHERE o.pack_id = p.id
		                    AND o.state NOT IN ('deleting', 'deleted')), 0)
		  FROM packs p
		 WHERE p.live_bytes <> COALESCE((SELECT sum(o.pack_length) FROM objects o
		                                  WHERE o.pack_id = p.id
		                                    AND o.state NOT IN ('deleting', 'deleted')), 0)
		    OR p.members_live <> COALESCE((SELECT count(*) FROM objects o
		                                    WHERE o.pack_id = p.id
		                                      AND o.state NOT IN ('deleting', 'deleted')), 0)
		 ORDER BY p.id`)
	if err != nil {
		return nil, fmt.Errorf("broken packs: %w", err)
	}
	defer rows.Close()

	var out []PackStats
	for rows.Next() {
		var s PackStats
		if err := rows.Scan(&s.ID, &s.State, &s.TotalBytes, &s.LiveBytes,
			&s.MembersTotal, &s.MembersLive, &s.LiveFromMembers); err != nil {
			return nil, fmt.Errorf("broken packs: %w", err)
		}
		out = append(out, s)
	}
	return out, rows.Err()
}
