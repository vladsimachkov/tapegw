package meta

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Запрос восстановления (S4, ADR-0001 и ADR-0003).
//
// Одно решение — одна транзакция (правило 6): состояние объекта, строка
// restores и продление online_until пишутся вместе, иначе клиент мог бы
// увидеть «restore принят» без строки в очереди, или наоборот.
//
// Развилка по наличию дисковой копии — из критерия ревью S4: объект с живой
// копией в landing в очередь recaller НЕ ставится, только продлевается срок.
// Поставить его в очередь значило бы гонять Phobos за данными, которые и так
// на диске.

// RestoreOutcome — чем закончился запрос восстановления.
type RestoreOutcome int

const (
	// RestoreQueued — новая очередь (или повтор после истечения/ошибки):
	// строка restores в 'requested', recaller подхватит.
	RestoreQueued RestoreOutcome = iota
	// RestoreExtended — копия жива или restore уже выдан и не истёк:
	// продлили срок, в очереди стоять нечего.
	RestoreExtended
	// RestoreInProgress — запрос уже стоит в 'requested'/'in_progress':
	// бэкенд отвечает RestoreAlreadyInProgress (409).
	RestoreInProgress
	// RestoreNotArchived — объект успел перестать быть archived (например,
	// ушёл в deleting между чтением бэкенда и этой транзакцией).
	RestoreNotArchived
)

// restorePriority — порядок выборки recaller'ом (миграция 0002): Expedited
// раньше Standard, Standard раньше Bulk.
func restorePriority(tier string) int {
	switch tier {
	case "Expedited":
		return 0
	case "Bulk":
		return 2
	default: // Standard и незнакомые — обычный приоритет, как в миграции 0002.
		return 1
	}
}

// RequestRestore принимает запрос восстановления архивного объекта.
//
// online_until продлевается во всех исходах по формуле ADR-0003
// max(online_until, now() + days): обещание «копия доступна до» не может
// уменьшаться от действия клиента. GREATEST, а не присваивание: повторный
// запрос на 7 дней не должен укорачивать уже обещанные 30.
func (m *Meta) RequestRestore(ctx context.Context, tenant TenantID, objectID uuid.UUID,
	days int, tier string) (RestoreOutcome, error) {
	if days < 1 {
		return 0, fmt.Errorf("meta: days=%d в запросе восстановления, должно быть >= 1", days)
	}

	var out RestoreOutcome
	err := m.inTx(ctx, func(tx pgx.Tx) error {
		// Строка объекта блокируется на всю транзакцию: параллельный RestoreObject
		// или переход состояния не протиснутся между чтением и записью.
		//
		// Тенант для строки очереди берётся из этой же строки, а не от
		// вызывающего: для root и admin страж отдаёт «все тенанты», и запись
		// этого значения в tenant_id нарушает FK — оператор не смог бы
		// восстановить объект клиента (находка ревью PR #127, та же ошибка,
		// что была с PutObject в #14). Тенант вызывающего нужен только стражу:
		// withTenant решает, можно ли трогать объект вовсе.
		q, args := withTenant(
			`SELECT state, landing, tenant_id FROM objects WHERE id = $1`, tenant, objectID)
		var state string
		var landing bool
		var objTenant int64
		err := tx.QueryRow(ctx, q+` FOR UPDATE`, args...).Scan(&state, &landing, &objTenant)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("meta: состояние объекта для restore: %w", err)
		}
		if state != "archived" {
			out = RestoreNotArchived
			return nil
		}

		extend := `UPDATE objects SET online_until =
		               GREATEST(online_until, now() + $2 * interval '1 day')
		           WHERE id = $1`

		// Живая дисковая копия — продлили и вышли, очередь не трогаем.
		if landing {
			if _, err := tx.Exec(ctx, extend, objectID, days); err != nil {
				return fmt.Errorf("meta: продление online_until: %w", err)
			}
			out = RestoreExtended
			return nil
		}

		var rState string
		var rActive bool
		err = tx.QueryRow(ctx,
			`SELECT state, expires_at IS NOT NULL AND expires_at > now()
			   FROM restores WHERE object_id = $1 FOR UPDATE`, objectID).
			Scan(&rState, &rActive)
		switch {
		case err == nil && (rState == "requested" || rState == "in_progress"):
			// Отказ без побочных эффектов: чужой незавершённый запрос не
			// продлеваем.
			out = RestoreInProgress
			return nil
		case err == nil && rState == "restored" && rActive:
			// ARCHITECTURE: «повторный RestoreObject продлевает expires_at».
			// GREATEST — по той же причине, что и online_until: не укорачиваем.
			if _, err := tx.Exec(ctx,
				`UPDATE restores SET days = GREATEST(days, $2),
				         expires_at = GREATEST(expires_at, now() + $2 * interval '1 day')
				 WHERE object_id = $1`, objectID, days); err != nil {
				return fmt.Errorf("meta: продление restore: %w", err)
			}
			if _, err := tx.Exec(ctx, extend, objectID, days); err != nil {
				return fmt.Errorf("meta: продление online_until: %w", err)
			}
			out = RestoreExtended
			return nil
		case err != nil && !errors.Is(err, pgx.ErrNoRows):
			return fmt.Errorf("meta: состояние restore: %w", err)
		}

		// Ни записи, ни живого restore ('failed' или истёкший 'restored' —
		// janitor ещё не успел убрать): ставим/перезаписываем 'requested'.
		// Переход идемпотентен (правило 7): повтор не создаёт вторую строку —
		// object_id первичный ключ.
		if rState == "" {
			_, err = tx.Exec(ctx,
				`INSERT INTO restores (object_id, tenant_id, state, tier, days, priority,
				                       requested_at, next_try_at)
				 VALUES ($1, $2, 'requested', $3, $4, $5, now(), now())`,
				objectID, objTenant, tier, days, restorePriority(tier))
		} else {
			_, err = tx.Exec(ctx,
				`UPDATE restores SET state = 'requested', tier = $2, days = $3,
				         priority = $4, requested_at = now(), restored_at = NULL,
				         expires_at = NULL, attempts = 0, next_try_at = now(),
				         last_error = NULL
				 WHERE object_id = $1`,
				objectID, tier, days, restorePriority(tier))
		}
		if err != nil {
			return fmt.Errorf("meta: постановка restore в очередь: %w", err)
		}
		// Срок считаем от запроса, а не от завершения recaller'а (ADR-0003):
		// дата в заголовке не может быть раньше реальной даты удаления файла,
		// а отсрочка выборки из очереди эту дату только приближает, не отдаляет.
		if _, err := tx.Exec(ctx, extend, objectID, days); err != nil {
			return fmt.Errorf("meta: продление online_until: %w", err)
		}
		out = RestoreQueued
		return nil
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}
