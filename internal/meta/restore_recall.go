package meta

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Переходы очереди восстановления ради recaller'а (S5).
//
// Порядок диктует ADR-0004 в обратную сторону вытеснению: recaller сначала
// пишет файл в cache, fsync-ит и проверяет, и только потом переходит строку
// в 'restored' одной транзакцией. Строка, обещающая копию, которой на диске
// ещё нет, здесь недопустима — в отличие от вытеснения, где порядок обратный
// именно потому, что там «флаг снят, а файл ещё лежит» безвреден.
//
// Захват строки (claim) ограничен lease'ом claim_ttl: recaller мог упасть
// посреди чтения ленты. Строка 'in_progress' с истёкшим lease возвращается в
// работу тому же механизмом выборки, а попытка при этом засчитывается —
// иначе процесс, падающий на одном и том же объекте, крутил бы ленту вечно.

// ErrRestoreLost — строка restore уже не в 'in_progress' (объект удалили,
// строка ушла по каскаду, или перехватил другой воркер). Для вызывающего это
// «работу опередили», а не сбой: повтор ничего не испортит.
var ErrRestoreLost = errors.New("meta: строка restore не засталась в in_progress")

// RestoreItem — участник батча recaller'а: что читать с ленты и что сверять.
type RestoreItem struct {
	ObjectID   uuid.UUID
	TenantID   int64
	Days       int
	PackID     uuid.UUID
	PackOffset int64
	FileBytes  int64
	// Size — длина открытых байтов: именно её обязан дать расшифрованный
	// участник (ADR-0019: на ленте лежит шифротекст с манифестом).
	Size       int64
	ETag       string
	WrappedDEK []byte
	Key        string // только для журнала
}

// ClaimRestorePack переводит следующую пачку запросов в 'in_progress' и
// возвращает участников вместе со всем, что нужно для чтения и сверки.
//
// Захват — по одной пачке, а не батчем по очереди: lease обязан покрывать
// одно чтение ленты, а не серию чтений. Батч на весь claim_ttl означал бы,
// что один воркер забрал очередь и читает пачки по очереди, второй драйв
// простаивает, а непрочитанные пачки после истечения lease перехватываются
// посреди чтения — та же лента читается дважды, и каждый перехват засчитан
// как попытка.
//
// Выборка: самая приоритетная готовая строка (priority, requested_at), затем
// все готовые строки её пачки. Оба шага — FOR UPDATE SKIP LOCKED, занятый
// ряд пропускается (правило 7). Гонка «два воркера делят одну пачку»
// возможна в узком окне и безвредна: строки делятся непересекающе, худший
// исход — лишнее чтение пачки с ленты.
//
// Lease claim_ttl пишется в next_try_at: у restores нет колонок воркера, а
// next_try_at уже означает «не раньше этого момента брать в работу» — для
// 'in_progress' он читается как «до этого момента захват действителен».
// Просроченный захват засчитывает попытку (attempts+1) и на исчерпании
// max_attempts уходит в 'failed' — зависший навечно 'in_progress' клиент
// видел бы как «restore идёт» до конца жизни объекта.
//
// Объект обязан быть archived и знать, где лежит (pack_id, pack_offset,
// file_bytes): строка без них — не участник чтения, а рассинхрон, и брать
// её в работу нечего.
func (m *Meta) ClaimRestorePack(ctx context.Context, limit int, claimTTL time.Duration,
	maxAttempts int) ([]RestoreItem, error) {
	if limit < 1 {
		return nil, fmt.Errorf("meta: батч restore %d, должно быть >= 1", limit)
	}
	// Как с landing_min_retention в архивации: ноль здесь означал бы
	// «lease истёк в момент захвата», и второй воркер перехватил бы строку
	// немедленно. Так быть не должно, а молча оно выглядит почти работающим.
	if claimTTL <= 0 {
		return nil, fmt.Errorf("meta: claim_ttl %v, должно быть > 0", claimTTL)
	}
	if maxAttempts < 1 {
		return nil, fmt.Errorf("meta: max_attempts %d, должно быть >= 1", maxAttempts)
	}

	rows, err := m.Pool.Query(ctx, claimRestoreSQL, limit,
		claimTTL.Milliseconds(), maxAttempts)
	if err != nil {
		return nil, fmt.Errorf("meta: выборка restore-пачки: %w", err)
	}
	defer rows.Close()

	var out []RestoreItem
	for rows.Next() {
		var it RestoreItem
		if err := rows.Scan(&it.ObjectID, &it.TenantID, &it.Days,
			&it.PackID, &it.PackOffset, &it.FileBytes,
			&it.Size, &it.ETag, &it.WrappedDEK, &it.Key); err != nil {
			return nil, fmt.Errorf("meta: выборка restore-пачки: %w", err)
		}
		out = append(out, it)
	}
	return out, rows.Err()
}

// restoreReady — «строка готова к захвату»: архивный объект с координатами
// пачки и запрос, которому можно идти в работу ('requested' по наступлении
// next_try_at или 'in_progress' с просроченным lease). Предикат используется
// дважды в одном запросе (шаг head и шаг pick), поэтому он один в переменной,
// а не переписанный дважды.
const restoreReady = `
       o.state = 'archived'
   AND o.pack_id IS NOT NULL AND o.pack_offset IS NOT NULL AND o.file_bytes IS NOT NULL
   AND (
         (r.state = 'requested'   AND (r.next_try_at IS NULL OR r.next_try_at <= now()))
      OR (r.state = 'in_progress' AND r.next_try_at IS NOT NULL AND r.next_try_at <= now())
   )`

const claimRestoreSQL = `
WITH head AS (
    -- самая приоритетная готовая строка: её пачка и есть следующая на чтение
    SELECT o.pack_id
      FROM restores r
      JOIN objects o ON o.id = r.object_id
     WHERE ` + restoreReady + `
     ORDER BY r.priority, r.requested_at
     LIMIT 1
     FOR UPDATE OF r SKIP LOCKED
),
pick AS (
    -- все готовые строки этой пачки: одно чтение ленты закрывает их все
    SELECT r.object_id, r.state
      FROM restores r
      JOIN objects o ON o.id = r.object_id
      JOIN head h ON h.pack_id = o.pack_id
     WHERE ` + restoreReady + `
     ORDER BY r.priority, r.requested_at
     LIMIT $1
     FOR UPDATE OF r SKIP LOCKED
),
upd AS (
    UPDATE restores r
       SET state = (CASE WHEN p.state = 'in_progress' AND r.attempts + 1 >= $3
                         THEN 'failed' ELSE 'in_progress' END)::restore_state,
           attempts = r.attempts + (p.state = 'in_progress')::int,
           last_error = CASE WHEN p.state = 'in_progress'
                             THEN 'захват истёк (claim_ttl): восстановление начато заново'
                             ELSE r.last_error END,
           next_try_at = now() + $2 * interval '1 millisecond'
      FROM pick p
     WHERE r.object_id = p.object_id
     RETURNING r.object_id, r.tenant_id, r.days, r.state AS rstate
)
SELECT u.object_id, u.tenant_id, u.days,
       o.pack_id, o.pack_offset, o.file_bytes, o.size, o.etag, o.wrapped_dek, o.key
  FROM upd u
  JOIN objects o ON o.id = u.object_id
 WHERE u.rstate = 'in_progress'`

// MarkRestoreRestored завершает восстановление: строка в 'restored', срок
// отсчитывается от завершения (критерий ревью S5):
//
//	online_until = max(online_until, restored_at + days)
//
// GREATEST обязателен: RequestRestore уже продлил срок от запроса, а запрос
// на больший срок мог прийти, пока лента крутилась, — обещание не укорачивается.
// Решает именно online_until: по нему показывают дату (x-amz-restore), читают
// cache и вытесняют janitor — одна колонка (ADR-0003). expires_at ставится
// теми же now() в той же транзакции и остаётся справочным полем: из-за
// GREATEST он может быть меньше online_until, и по нему решение не принимается.
//
// Оба UPDATE под защитой состояния (правило 7): ноль затронутых строк —
// объект удалили или перехватили, — это ErrRestoreLost, а не тихий успех.
func (m *Meta) MarkRestoreRestored(ctx context.Context, objectID uuid.UUID, days int) error {
	if days < 1 {
		return fmt.Errorf("meta: days=%d в MarkRestoreRestored, должно быть >= 1", days)
	}
	return m.inTx(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE restores
			   SET state = 'restored', restored_at = now(),
			       expires_at = now() + $2 * interval '1 day',
			       next_try_at = NULL, last_error = NULL
			 WHERE object_id = $1 AND state = 'in_progress'`, objectID, days)
		if err != nil {
			return fmt.Errorf("meta: переход restore в restored: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return ErrRestoreLost
		}
		tag, err = tx.Exec(ctx, `
			UPDATE objects
			   SET online_until = GREATEST(online_until, now() + $2 * interval '1 day')
			 WHERE id = $1 AND state = 'archived'`, objectID, days)
		if err != nil {
			return fmt.Errorf("meta: продление online_until после restore: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return ErrRestoreLost
		}
		return nil
	})
}

// MarkRestoreFailure — ошибка одной попытки: attempts+1, backoff как у
// migrator (экспонента от base, потолок сутки), на исчерпании max_attempts —
// 'failed'. Переход под защитой 'in_progress' (правило 7).
func (m *Meta) MarkRestoreFailure(ctx context.Context, objectID uuid.UUID,
	maxAttempts int, base time.Duration, reason string) error {
	if maxAttempts < 1 {
		return fmt.Errorf("meta: max_attempts %d, должно быть >= 1", maxAttempts)
	}
	if base <= 0 {
		return fmt.Errorf("meta: base backoff %v, должно быть > 0", base)
	}
	secs := int64(base / time.Second)
	if secs < 1 {
		secs = 1
	}
	exp := fmt.Sprintf(`LEAST(86400, %d * power(2, LEAST(10, attempts))::bigint)`, secs)
	_, err := m.Pool.Exec(ctx, `
		UPDATE restores
		   SET attempts = attempts + 1,
		       last_error = $2,
		       state = (CASE WHEN attempts + 1 >= $3 THEN 'failed' ELSE 'requested' END)::restore_state,
		       next_try_at = CASE WHEN attempts + 1 >= $3 THEN NULL
		                          ELSE now() + make_interval(secs => `+exp+`)::interval
		                     END
		 WHERE object_id = $1 AND state = 'in_progress'`,
		objectID, reason, maxAttempts)
	if err != nil {
		return fmt.Errorf("meta: учёт попытки restore: %w", err)
	}
	return nil
}

// MarkRestoreCorrupt — участник не прошёл проверку при совпавшей сумме пачки.
// Повтор здесь бесполезен и дорог: сумма пачки доказывает, что с ленты пришли
// ровно те байты, что легли при архивации, — повторное чтение даст тот же
// результат и потратит до max_attempts прогонов ленты. Поэтому попытку
// засчитываем (правило 7 — переход под защитой 'in_progress'), но сразу в
// 'failed', без backoff-повторов. Ретраи остаются для ошибок чтения и
// несовпадения суммы пачки.
func (m *Meta) MarkRestoreCorrupt(ctx context.Context, objectID uuid.UUID, reason string) error {
	_, err := m.Pool.Exec(ctx, `
		UPDATE restores
		   SET attempts = attempts + 1,
		       last_error = $2,
		       state = 'failed',
		       next_try_at = NULL
		 WHERE object_id = $1 AND state = 'in_progress'`, objectID, reason)
	if err != nil {
		return fmt.Errorf("meta: учёт испорченного участника: %w", err)
	}
	return nil
}

// PackCopy — готовая копия пачки: объект Phobos и сумма потока, записанные
// при архивации. Recaller читает по phobos_oid (у первой копии он равен id
// пачки, у последующих — свой) и сверяет отданные байты с sha256 копии:
// это единственная сумма пачки, сохранённая при записи.
type PackCopy struct {
	OID    uuid.UUID
	SHA256 []byte
}

// PackCopy возвращает любую готовую копию пачки (детерминированно — первую по
// spec). Выбор копии по смонтированной ленте — пункт плана после среза; здесь
// важен сам факт: без готовой копии с суммой читать нечего и сверять не с чем.
func (m *Meta) PackCopy(ctx context.Context, packID uuid.UUID) (*PackCopy, error) {
	var oid string
	c := &PackCopy{}
	err := m.Pool.QueryRow(ctx, `
		SELECT phobos_oid, sha256 FROM archive_copies
		 WHERE pack_id = $1 AND state = 'done'
		 ORDER BY spec LIMIT 1`, packID).Scan(&oid, &c.SHA256)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("meta: копия пачки: %w", err)
	}
	if c.OID, err = uuid.Parse(oid); err != nil {
		return nil, fmt.Errorf("meta: phobos_oid %q пачки %s: %w", oid, packID, err)
	}
	if len(c.SHA256) == 0 {
		return nil, fmt.Errorf("meta: у копии пачки %s нет sha256 при state done", packID)
	}
	return c, nil
}

// ClaimCacheEvictBatch удаляет истёкшие restore: строку — здесь, в транзакции
// выборки, файл — вызывающий после возврата (janitor). Порядок как в
// вытеснении landing (ADR-0004): сначала строка перестала обещать копию,
// потом unlink.
//
// Условие — online_until < now(), а не expires_at: по online_until решают
// и заголовок x-amz-restore, и чтение из cache — одна колонка на все три
// решения (ADR-0003). restores.expires_at остаётся справочным полем:
// GREATEST в MarkRestoreRestored допускает online_until > expires_at
// (запрос на больший срок пришёл, пока лента крутилась), и решение по
// expires_at разошлось бы с показанной клиенту датой.
func (m *Meta) ClaimCacheEvictBatch(ctx context.Context, limit int) ([]uuid.UUID, error) {
	if limit < 1 {
		return nil, fmt.Errorf("meta: батч вытеснения cache %d, должно быть >= 1", limit)
	}
	rows, err := m.Pool.Query(ctx, `
		WITH pick AS (
		    SELECT r.object_id
		      FROM restores r
		      JOIN objects o ON o.id = r.object_id
		     WHERE r.state = 'restored'
		       AND o.state = 'archived' AND NOT o.landing
		       AND o.online_until < now()
		     ORDER BY o.online_until
		     LIMIT $1
		     FOR UPDATE OF r SKIP LOCKED
		)
		DELETE FROM restores r
		 WHERE object_id IN (SELECT object_id FROM pick)
		RETURNING r.object_id`, limit)
	if err != nil {
		return nil, fmt.Errorf("meta: выборка на вытеснение cache: %w", err)
	}
	defer rows.Close()

	var out []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("meta: выборка на вытеснение cache: %w", err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}
