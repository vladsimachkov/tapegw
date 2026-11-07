package meta

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Вытеснение дисковой копии по истечении срока (ADR-0004, S3).
//
// Порядок здесь — обратный правилу 6 и он не случаен: сначала снимается флаг
// landing в транзакции, файл удаляется вызывающий ПОСЛЕ commit. Если процесс
// упадёт между ними, останется файл без ссылки — его подберёт будущая сборка
// сирот. Наоборот хуже: строка, обещающая объект онлайн, а файла нет, — это
// GET, который отдаёт то 500, то ENOENT вместо штатного InvalidObjectState.
//
// Защита по состоянию, а не по дате (ADR-0003): вытесняются только archived.
// pending и archiving с прошедшей датой — штатная картина при задержке
// migrator дольше landing_min_retention, и удалить их означало бы потерять
// данные, за которые клиент получил 200.

// EvictCandidate — участник вытеснения: файл на диске, флаг с которого уже снят.
type EvictCandidate struct {
	ID  uuid.UUID
	Key string
}

// evictClaimSQL — выборка и снятие флага одним UPDATE: переход идемпотенен
// (правило 7), ноль строк означает «опередили». now() — часы БД: online_until
// ставится теми же часами, и сравнивать надо ими же, а не часами tapegw-хоста.
//
// FOR UPDATE SKIP LOCKED — два janitor (когда процессов tapegw станет больше
// одного) не должны браться за один объект: занятой рядом просто пропускается.
//
// Строгое `<`: объект ровно в момент online_until ещё не вытесняется. Окно
// чтения задано тем же знаком у ReadableFromDisk (OnlineUntil.After(now)),
// и сдвигать его в любую сторону — значит либо нарушить обещание в
// x-amz-restore, либо оставить нечитаемый файл висеть в landing.
const evictClaimSQL = `
UPDATE objects SET landing = false
WHERE id IN (
    SELECT id FROM objects
     WHERE state = 'archived' AND landing AND online_until < now()
     ORDER BY online_until
     LIMIT $1
     FOR UPDATE SKIP LOCKED)
RETURNING id, key`

// ClaimEvictBatch снимает флаг landing у следующей пачки истёкших archived
// объектов. Возвращает участников, файлы которых вызывающий обязан удалить
// ПОСЛЕ возврата (то есть после commit): до него строка уже говорит «не
// читай с диска», и GET успевает переключиться на InvalidObjectState.
func (m *Meta) ClaimEvictBatch(ctx context.Context, limit int) ([]EvictCandidate, error) {
	return claimEvict(ctx, m.Pool, limit)
}

// ClaimEvictBatchTx — та же выборка внутри транзакции вызывающего. Нужна
// проверка границы: now() внутри транзакции постоянна, и объект с
// online_until = now() можно сравнить с теми же часами только в ней.
func (m *Meta) ClaimEvictBatchTx(ctx context.Context, tx pgx.Tx, limit int) ([]EvictCandidate, error) {
	return claimEvict(ctx, tx, limit)
}

type evictQuerier interface {
	Query(ctx context.Context, query string, args ...any) (pgx.Rows, error)
}

func claimEvict(ctx context.Context, q evictQuerier, limit int) ([]EvictCandidate, error) {
	if limit < 1 {
		return nil, fmt.Errorf("meta: батч вытеснения %d, должно быть >= 1", limit)
	}
	rows, err := q.Query(ctx, evictClaimSQL, limit)
	if err != nil {
		return nil, fmt.Errorf("meta: выборка на вытеснение: %w", err)
	}
	defer rows.Close()

	var out []EvictCandidate
	for rows.Next() {
		var c EvictCandidate
		if err := rows.Scan(&c.ID, &c.Key); err != nil {
			return nil, fmt.Errorf("meta: выборка на вытеснение: %w", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
