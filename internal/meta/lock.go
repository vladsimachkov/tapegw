package meta

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Retention и legal hold конкретной версии.
//
// Всё трое правил, ниже, нарушают обещание неизменяемости, поэтому они в meta,
// а не в бэкенде: бэкендов у tapegw будет несколько (S3 API, админ-API,
// возможно прямой вызов из воркера), и ни один не должен помнить их сам.

var (
	// ErrLockNotEnabled — в бакете не включён Object Lock. Ставить retention на
	// версию в таком бакете нечего: версии всё равно ничем не защищены.
	ErrLockNotEnabled = errors.New("meta: object lock is not enabled for this bucket")
	// ErrRetentionOnMarker — у маркера удаления нет данных, и защищать нечего.
	// В схеме это и так CHECK, но отказ здесь даёт внятный код, а не 500.
	ErrRetentionOnMarker = errors.New("meta: cannot put retention on a delete marker")
	// ErrRetentionShorten — срок COMPLIANCE нельзя сократить. Это смысл
	// комплаенса: написанное нельзя прочитать раньше срока, и «я передумал» не
	// входит в условия.
	ErrRetentionShorten = errors.New("meta: COMPLIANCE retention cannot be shortened")
	// ErrRetentionModeChange — режим COMPLIANCE нельзя ни снять, ни заменить:
	// переход из него отсутствует.
	ErrRetentionModeChange = errors.New("meta: COMPLIANCE retention mode cannot be changed")
)

// lockRow — версия вместе с состоянием лока бакета.
//
// bucketID и tenantID нужны журналу: событие обязано нести тенанта бакета, а
// не тенанта учётки запроса (под обходом по роли это разные тенанты), и второй
// запрос за ними был бы окном, в котором бакет уже успели изменить.
type lockRow struct {
	id          uuid.UUID
	mode        *string
	until       *time.Time
	legalHold   bool
	isMarker    bool
	lockEnabled bool
	bucketID    int64
	tenantID    int64
}

// findLockVersion ищет версию, к которой относятся retention и legal hold.
// versionID == "" — текущая версия; "null" — null-версия.
func findLockVersion(ctx context.Context, tx pgx.Tx, tenant TenantID,
	bucket, key, versionID string) (lockRow, error) {

	var row lockRow
	args := []any{bucket, key}
	cond := `o.is_current`
	if versionID != "" {
		if versionID == "null" {
			cond = `o.is_null_version`
		} else {
			id, err := uuid.Parse(versionID)
			if err != nil {
				return row, ErrNoSuchVersion
			}
			args = append(args, id)
			cond = fmt.Sprintf(`o.id = $%d::uuid`, len(args))
		}
	}

	base := fmt.Sprintf(`SELECT o.id, o.retention_mode::text, o.retain_until,
			                o.legal_hold, o.is_delete_marker, b.object_lock_enabled,
					                b.id, b.tenant_id
			           FROM objects o
			           JOIN buckets b ON b.id = o.bucket_id
			          WHERE b.name = $1 AND b.deleted_at IS NULL
			            AND o.key = $2 AND %s
			            AND o.state NOT IN ('deleting','deleted')`, cond)
	q, qargs := withTenantCol(base, "b.tenant_id", tenant, args...)

	err := tx.QueryRow(ctx, q+` FOR UPDATE OF o`, qargs...).
		Scan(&row.id, &row.mode, &row.until, &row.legalHold, &row.isMarker,
			&row.lockEnabled, &row.bucketID, &row.tenantID)
	if errors.Is(err, pgx.ErrNoRows) {
		return row, ErrNoSuchVersion
	}
	if err != nil {
		return row, fmt.Errorf("find lock version %s/%s@%s: %w", bucket, key, versionID, err)
	}
	return row, nil
}

// PutObjectRetention задаёт retention версии.
//
// GOVERNANCE можно менять как угодно — он и создан для «защиты от своей же
// ошибки». COMPLIANCE — нельзя ни сократить, ни снять, ни заменить режим:
// иначе «неизменяемый» срок превращается в подсказку.
func (m *Meta) PutObjectRetention(ctx context.Context, tenant TenantID,
	bucket, key, versionID, mode string, until time.Time) error {

	return m.inTx(ctx, func(tx pgx.Tx) error {
		row, err := findLockVersion(ctx, tx, tenant, bucket, key, versionID)
		if err != nil {
			return err
		}
		if !row.lockEnabled {
			return ErrLockNotEnabled
		}
		if row.isMarker {
			return ErrRetentionOnMarker
		}
		if row.mode != nil && *row.mode == "COMPLIANCE" {
			if mode != "COMPLIANCE" {
				return ErrRetentionModeChange
			}
			if row.until != nil && until.Before(*row.until) {
				return ErrRetentionShorten
			}
		}

		_, err = tx.Exec(ctx,
			`UPDATE objects SET retention_mode = $2::retention_mode, retain_until = $3
			  WHERE id = $1`, row.id, mode, until)
		if err != nil {
			return fmt.Errorf("put retention %s/%s@%s: %w", bucket, key, versionID, err)
		}
		// Событие — здесь, в этой же транзакции. Продленный COMPLIANCE, не
		// попавший в журнал, после восстановления снова стал бы удаляемым по
		// старому сроку, и это не «неточность», а снятие неизменяемости.
		return appendEvent(ctx, tx, row.tenantID, JournalKindRetention, uuidPtr(row.id), row.bucketID,
			JournalRetention{Bucket: bucket, Key: key, Mode: mode, Until: until})
	})
}

// GetObjectRetention отдаёт сохранённый retention версии. ErrNotConfigured —
// версии есть, retention нет: это не ошибка, а штатное состояние, и бэкенд
// обязан отличить его от «нет такой версии».
func (m *Meta) GetObjectRetention(ctx context.Context, tenant TenantID,
	bucket, key, versionID string) (mode string, until time.Time, err error) {

	err = m.inTx(ctx, func(tx pgx.Tx) error {
		row, err := findLockVersion(ctx, tx, tenant, bucket, key, versionID)
		if err != nil {
			return err
		}
		if row.mode == nil {
			return ErrNotConfigured
		}
		mode, until = *row.mode, *row.until
		return nil
	})
	return mode, until, err
}

// PutObjectLegalHold включает или снимает legal hold. В отличие от retention его
// снимают свободно — это «заморозить на время разбора», а не «нельзя никогда».
func (m *Meta) PutObjectLegalHold(ctx context.Context, tenant TenantID,
	bucket, key, versionID string, on bool) error {

	return m.inTx(ctx, func(tx pgx.Tx) error {
		row, err := findLockVersion(ctx, tx, tenant, bucket, key, versionID)
		if err != nil {
			return err
		}
		if !row.lockEnabled {
			return ErrLockNotEnabled
		}
		if row.isMarker {
			return ErrRetentionOnMarker
		}
		if _, err := tx.Exec(ctx,
			`UPDATE objects SET legal_hold = $2 WHERE id = $1`, row.id, on); err != nil {
			return fmt.Errorf("put legal hold %s/%s@%s: %w", bucket, key, versionID, err)
		}
		// Снятие legal hold — событие не меньше установки: восстановление
		// обязано вернуть и «заморозку сняли», иначе версия навсегда останется
		// неудаляемой.
		return appendEvent(ctx, tx, row.tenantID, JournalKindLegalHold, uuidPtr(row.id), row.bucketID,
			JournalLegalHold{Bucket: bucket, Key: key, On: on})
	})
}

// GetObjectLegalHold — состояние legal hold версии.
func (m *Meta) GetObjectLegalHold(ctx context.Context, tenant TenantID,
	bucket, key, versionID string) (bool, error) {

	var on bool
	err := m.inTx(ctx, func(tx pgx.Tx) error {
		row, err := findLockVersion(ctx, tx, tenant, bucket, key, versionID)
		if err != nil {
			return err
		}
		on = row.legalHold
		return nil
	})
	return on, err
}
