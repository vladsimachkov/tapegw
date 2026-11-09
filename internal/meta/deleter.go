package meta

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Deleter (M6): физическое снятие удалённых версий — с диска и с ленты.
//
// Строка попадает в 'deleting' на уровне S3 API (delete.go), а отсюда её
// разбирает deleter. Три правила держат весь файл:
//
//   - «сначала лента, потом строка»: строка уходит в 'deleted' только после
//     подтверждения от mover. Сбой посередине оставляет строку в 'deleting',
//     и повтор сходится к тому же результату — purge отсутствующего объекта
//     есть успех.
//   - Object Lock перечитывается перед каждым шагом: версия под legal hold или
//     действующим retention не удаляется ни в каком состоянии. В 'deleting'
//     такая версия уже может лежать: обход GOVERNANCE принимает удаление, не
//     снимая полей retention, — и снять данные раньше срока он не уполномочен.
//   - объект пачки на ленте удаляется только когда members_live = 0 и никто из
//     участников не заблокирован lock'ом или восстановлением. Пока есть живой
//     участник, байты удалённых ждут на ленте уплотнения (0011) — снимаются
//     только их файлы в landing и cache.

// lockedSQL — единый критерий «версия под блокировкой» для всех запросов
// deleter'а: legal hold или действующий retention. Тот же смысл, что в
// deleteVersion (ADR-0015), но без GOVERNANCE-обхода: обход — решение
// запроса клиента в момент удаления, у воркера запроса нет, а «не удаляется ни
// в каком состоянии» здесь важнее удобства. Держать одним фрагментом нужно,
// потому что проверка стоит в трёх местах (выборка, состояние пачки, переход),
// и расхождение между ними — это «одна проверка пропустила, другая нет».
// alias — префикс таблицы с точкой ("" — без префикса).
func lockedSQL(alias string) string {
	if alias == "" {
		return `legal_hold OR (retain_until IS NOT NULL AND retain_until > now())`
	}
	return alias + ".legal_hold OR (" + alias + ".retain_until IS NOT NULL AND " +
		alias + ".retain_until > now())"
}

// DeletionCandidate — версия, занятая из очереди 'deleting'.
type DeletionCandidate struct {
	ID uuid.UUID
	// PackID — пачка (для крупного объекта это single_object-пачка с id
	// версии). Nil означает «на ленту не ложилась»: снимать надо только диск.
	PackID *uuid.UUID
	Bucket string
	Key    string
	// Locked — legal hold или действующий retention. Такую версию воркер не
	// удаляет и оставляет в 'deleting' до снятия блокировки.
	Locked bool
	// RestoreBusy — у этой версии восстановление в работе ('requested' или
	// 'in_progress'): recaller читает её с ленты и пишет в cache, и снимать
	// её байты посередине нельзя.
	RestoreBusy bool
}

// ClaimDeletionBatch берёт до limit версий из 'deleting' и отодвигает
// next_try_at на claimTTL — это и захват: второй deleter не возьмёт строку до
// истечения срока, а после падения процесса захват истекает сам и строку
// подберёт следующий прогон.
//
// Отдельной колонки захвата нет намеренно: каждый шаг deleter'а идемпотентен
// (purge отсутствующего — успех, переход — UPDATE … WHERE state='deleting'),
// поэтому два воркера на одной версии ничего не испортят, и захват нужен
// только чтобы не дублировать работу.
//
// Транзакция закрывается здесь: правило 13 запрещает держать её во время
// вызова mover.
func (m *Meta) ClaimDeletionBatch(ctx context.Context, limit int, claimTTL time.Duration) ([]DeletionCandidate, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("claim deletion: limit %d, должно быть > 0", limit)
	}
	if claimTTL <= 0 {
		return nil, fmt.Errorf("claim deletion: claimTTL %v, должно быть > 0", claimTTL)
	}
	rows, err := m.Pool.Query(ctx, fmt.Sprintf(`
		WITH picked AS (
			UPDATE objects
			   SET next_try_at = now() + make_interval(secs => $2::double precision)
			 WHERE id IN (
				SELECT id FROM objects
				 WHERE state = 'deleting'
				   AND (next_try_at IS NULL OR next_try_at <= now())
				 ORDER BY next_try_at NULLS FIRST, created_at, id
				 LIMIT $1
				 FOR UPDATE SKIP LOCKED)
			RETURNING id, pack_id, bucket_id, key,
			          %[1]s AS locked,
			          EXISTS (SELECT 1 FROM restores r
			                   WHERE r.object_id = objects.id
			                     AND r.state IN ('requested', 'in_progress')) AS restore_busy
		)
		SELECT p.id, p.pack_id, b.name, p.key, p.locked, p.restore_busy
		  FROM picked p JOIN buckets b ON b.id = p.bucket_id`, lockedSQL("")),
		limit, claimTTL.Seconds())
	if err != nil {
		return nil, fmt.Errorf("claim deletion: %w", err)
	}
	defer rows.Close()

	var out []DeletionCandidate
	for rows.Next() {
		var c DeletionCandidate
		if err := rows.Scan(&c.ID, &c.PackID, &c.Bucket, &c.Key, &c.Locked, &c.RestoreBusy); err != nil {
			return nil, fmt.Errorf("claim deletion: scan: %w", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// PackDeletionState — всё, что deleter обязан знать о пачке до решения про
// ленту.
type PackDeletionState struct {
	State       string
	MembersLive int
	// Copies — phobos_oid всех копий пачки, независимо от состояния копии:
	// недозаписанную тоже надо снять, а отсутствующей в Phobos это не портит.
	Copies []uuid.UUID
	// LockedDeleting — число участников в 'deleting' под lock'ом. Их байты
	// внутри объекта пачки, поэтому при ненулевом счётчике пачку не трогают.
	LockedDeleting int
	// RestoreBusy — у кого-то из участников восстановление в работе
	// ('requested' или 'in_progress'): удалить объект под таким — значит
	// оборвать чужую операцию посередине.
	RestoreBusy bool
	// ClaimActive — пачку держит migrator (печатает или дописывает копии):
	// purge под его руками оставил бы на ленте сироту, который migrator
	// потом завершит как готовую копию.
	ClaimActive bool
}

// PackDeletionState читает состояние пачки для решения о ленте.
func (m *Meta) PackDeletionState(ctx context.Context, packID uuid.UUID) (*PackDeletionState, error) {
	var s PackDeletionState
	var oids []string
	err := m.Pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT p.state::text, p.members_live,
		       COALESCE((SELECT array_agg(c.phobos_oid)
		                   FROM archive_copies c WHERE c.pack_id = p.id), '{}'),
		       (SELECT count(*) FROM objects o
		         WHERE o.pack_id = p.id AND o.state = 'deleting'
		           AND (%[1]s)),
		       EXISTS (SELECT 1 FROM restores r JOIN objects o ON o.id = r.object_id
		                WHERE o.pack_id = p.id AND r.state IN ('requested', 'in_progress')),
		       p.claimed_by IS NOT NULL AND p.claim_expires_at > now()
		  FROM packs p WHERE p.id = $1`, lockedSQL("o")), packID).
		Scan(&s.State, &s.MembersLive, &oids, &s.LockedDeleting, &s.RestoreBusy, &s.ClaimActive)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("pack deletion state %s: %w", packID, err)
	}
	s.Copies = make([]uuid.UUID, 0, len(oids))
	for _, o := range oids {
		id, err := uuid.Parse(o)
		if err != nil {
			// Fail-closed: oid, который мы не можем назвать, мы не можем и
			// удалить — вернуть ошибку честнее, чем удалить «что поняли».
			return nil, fmt.Errorf("pack deletion state %s: phobos_oid %q не UUID: %w", packID, o, err)
		}
		s.Copies = append(s.Copies, id)
	}
	return &s, nil
}

// MarkVersionDeleted переводит одну версию в 'deleted'. false означает, что
// строку уже перевели без нас (правило 7: ноль затронутых строк — «опередили»,
// повтор безвреден).
//
// landing снимается здесь же: файл воркер снимает сразу после перехода, и если
// упасть между переходом и unlink, останется сирота на диске (флаг-то снят),
// а не строка, обещающую копию, которой нет.
func (m *Meta) MarkVersionDeleted(ctx context.Context, id uuid.UUID) (bool, error) {
	tag, err := m.Pool.Exec(ctx, `
		UPDATE objects
		   SET state = 'deleted', retired_at = now(), landing = false, next_try_at = NULL
		 WHERE id = $1 AND state = 'deleting'`, id)
	if err != nil {
		return false, fmt.Errorf("mark deleted %s: %w", id, err)
	}
	return tag.RowsAffected() == 1, nil
}

// FinishPackDeletion закрывает снятие пачки, копии которой уже выпущены из
// Phobos: все её участники в 'deleting' уходят в 'deleted', пачка — в
// 'retired'. Одна транзакция, потому что «объекта на ленте нет, а участники
// ещё живы» — состояние, в котором нельзя переживать падение.
//
// Проверка lock'а повторена внутри перехода: воркер перечитал её перед mover,
// но вторая стоит ничего, а окно между проверкой и переходом закрывает.
// Заблокированный участник не переводится, и пачка под ним не ретайрится.
//
// Возвращает (id переведённых участников — по ним воркер снимает файлы,
// пачка ретайрена).
func (m *Meta) FinishPackDeletion(ctx context.Context, packID uuid.UUID) ([]uuid.UUID, bool, error) {
	tx, err := m.Pool.Begin(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("finish pack deletion: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // после commit нечего откатывать

	rows, err := tx.Query(ctx, fmt.Sprintf(`
		UPDATE objects
		   SET state = 'deleted', retired_at = now(), landing = false, next_try_at = NULL
		 WHERE pack_id = $1 AND state = 'deleting'
		   AND NOT (%[1]s)
		RETURNING id`, lockedSQL("")), packID)
	if err != nil {
		return nil, false, fmt.Errorf("finish pack deletion: members: %w", err)
	}
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, false, fmt.Errorf("finish pack deletion: scan: %w", err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("finish pack deletion: members rows: %w", err)
	}

	// Ретайр только когда под пачкой не осталось ни живых участников, ни
	// удаляемых (заблокированных). members_live = 0 проверяется ещё раз:
	// переход участников и счётчик живут в одной строке objects, но условие
	// дешёвое, а ретайр пачки с живым участником — потеря его данных.
	t2, err := tx.Exec(ctx, `
		UPDATE packs p SET state = 'retired', retired_at = now()
		 WHERE p.id = $1 AND p.state IN ('sealed', 'archived') AND p.members_live = 0
		   AND NOT EXISTS (SELECT 1 FROM objects o
		                    WHERE o.pack_id = p.id AND o.state = 'deleting')`, packID)
	if err != nil {
		return nil, false, fmt.Errorf("finish pack deletion: retire: %w", err)
	}
	retired := t2.RowsAffected() == 1

	if err := tx.Commit(ctx); err != nil {
		return nil, false, fmt.Errorf("finish pack deletion: commit: %w", err)
	}
	return ids, retired, nil
}

// DeletionFailed назначает повтор снятия: строка остаётся в 'deleting' —
// 'failed' для удаления не существует намеренно, потому что неудалённая версия
// не терминальна: данные обязаны быть сняты когда-нибудь, а строка, выпавшая
// из очереди, осталась бы на ленте навсегда.
//
// base — база экспоненты (потолок сутки, как в RequeueArchiving): без потолка
// attempts=20 означал бы «попробуем через месяц», и «дождётся лент»
// превратилось бы в «забыт».
func (m *Meta) DeletionFailed(ctx context.Context, ids []uuid.UUID, reason string, base time.Duration) error {
	if len(ids) == 0 {
		return nil
	}
	if base <= 0 {
		return fmt.Errorf("deletion failed: base backoff %v, должно быть > 0", base)
	}
	secs := int64(base / time.Second)
	if secs < 1 {
		secs = 1
	}
	_, err := m.Pool.Exec(ctx, `
		UPDATE objects
		   SET attempts = attempts + 1,
		       last_error = $2,
		       next_try_at = now() + make_interval(secs =>
		           LEAST(86400, $3::bigint * power(2, LEAST(10, attempts))::bigint))
		 WHERE id = ANY($1::uuid[]) AND state = 'deleting'`,
		ids, reason, secs)
	if err != nil {
		return fmt.Errorf("deletion failed backoff: %w", err)
	}
	return nil
}
