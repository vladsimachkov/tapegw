package meta

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Состояния версионирования бакета — значения enum `bucket_versioning` из
// миграции 0005. Константы объявлены здесь, а не берутся из
// versitygw `types.BucketVersioningStatus`, по той же причине, по которой
// условия условной записи приходят колбэком: пакет meta не должен зависеть от
// S3-обвязки.
const (
	// VersioningUnversioned — бакет никогда не имел версий; все объекты в нём
	// имеют versionId = "null" в смысле AWS.
	VersioningUnversioned = "unversioned"
	// VersioningEnabled — включённое версионирование: прежняя версия при
	// перезаписи сохраняется, а не удаляется.
	VersioningEnabled = "enabled"
	// VersioningSuspended — версии приостановлены: новая запись заменяет
	// предыдущую null-версию, остальные версии остаются.
	VersioningSuspended = "suspended"
)

// IsNullVersionBucket reports whether objects written into a bucket with this
// versioning state get AWS's "null" versionId. True for both unversioned and
// suspended buckets — that is what makes "overwrite replaces the previous null
// version" expressible at all, since our versionId is always a row UUID.
func IsNullVersionBucket(versioning string) bool {
	return versioning != VersioningEnabled
}

// BucketVersioningState — состояние версионирования бакета. Нужен чтению: по
// нему бэкенд решает, какой x-amz-version-id поставить (для unversioned
// заголовка нет, для suspended — "null").
func (m *Meta) BucketVersioningState(ctx context.Context, tenant TenantID, bucket string) (string, error) {
	base := `SELECT b.versioning FROM buckets b
	          WHERE b.name = $1 AND b.deleted_at IS NULL`
	q, args := withTenantCol(base, "b.tenant_id", tenant, bucket)

	var state string
	if err := m.Pool.QueryRow(ctx, q, args...).Scan(&state); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrNotFound
		}
		return "", fmt.Errorf("versioning of %s: %w", bucket, err)
	}
	return state, nil
}

// Ошибки переходов версионирования и лока. Каждая соответствует правилу AWS,
// которое нельзя выразить схемой: схема не видит разницы между «не меняли» и
// «попытались выключить».
var (
	// ErrSuspendWithLock — в бакете с Object Lock версионирование нельзя
	// приостановить: версии под retention должны читаться всегда.
	ErrSuspendWithLock = errors.New("meta: cannot suspend versioning with object lock")
	// ErrSuspendNeverEnabled — приостанавливать нечего: версий никогда не было.
	ErrSuspendNeverEnabled = errors.New("meta: versioning was never enabled")
	// ErrLockNeedsVersioning — Object Lock включается только поверх включённого
	// версионирования: без версий нет и неизменяемости.
	ErrLockNeedsVersioning = errors.New("meta: object lock requires versioning enabled")
	// ErrLockCannotDisable — Object Lock включается один раз и навсегда. Это
	// обещание неизменяемости, и снять его задним числом нельзя.
	ErrLockCannotDisable = errors.New("meta: object lock cannot be disabled")
	// ErrNotConfigured — бакет есть, настройки нет. Отдельно от ErrNotFound:
	// иначе «бакета нет» начнёт выглядеть как «не сконфигурировано», и проверка
	// неизменяемости пойдёт по пути «настройки нет».
	ErrNotConfigured = errors.New("meta: not configured")
)

// SetBucketVersioning меняет версионирование бакета.
//
// Переходы:
//
//	unversioned → enabled    — можно
//	unversioned → suspended  — нельзя: приостанавливать нечего
//	enabled     → suspended  — можно, если Object Lock не включён
//	suspended   → enabled    — можно
//	в любой → unversioned    — нельзя никогда (API такого статуса и не шлёт)
//
// Повторный вызов с тем же статусом — успех, а не ошибка: клиент мог отправить
// конфигурацию дважды, AWS на это отвечает 200.
//
// Бакет берётся FOR UPDATE: состояние и проверка «лок включён» должны быть
// атомарны, иначе параллельное включение лока пропустило бы запрет на
// приостановку.
func (m *Meta) SetBucketVersioning(ctx context.Context, tenant TenantID, bucket, status string) error {
	return m.inTx(ctx, func(tx pgx.Tx) error {
		var (
			id     int64
			cur    string
			lockOn bool
		)
		q, args := withTenant(
			`SELECT id, versioning, object_lock_enabled FROM buckets
			  WHERE name = $1 AND deleted_at IS NULL`, tenant, bucket)
		err := tx.QueryRow(ctx, q+` FOR UPDATE`, args...).Scan(&id, &cur, &lockOn)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("lock bucket %q: %w", bucket, err)
		}

		if status == VersioningSuspended {
			if lockOn {
				return ErrSuspendWithLock
			}
			if cur == VersioningUnversioned {
				return ErrSuspendNeverEnabled
			}
		}
		if cur == status {
			return nil
		}
		if _, err := tx.Exec(ctx,
			`UPDATE buckets SET versioning = $2 WHERE id = $1`, id, status); err != nil {
			return fmt.Errorf("set versioning of %q: %w", bucket, err)
		}
		return nil
	})
}

// SetObjectLockConfig включает Object Lock и (опционально) задаёт правило
// retentions по умолчанию. enabled=false принимается только до включения:
// выключить лок нельзя ни при каких обстоятельствах.
//
// config — байты конфигурации ровно в том виде, в котором их передал
// versitygw; отдавать наружу мы обязаны ими же.
func (m *Meta) SetObjectLockConfig(ctx context.Context, tenant TenantID,
	bucket string, enabled bool, config []byte) error {

	return m.inTx(ctx, func(tx pgx.Tx) error {
		var (
			id         int64
			versioning string
			lockOn     bool
		)
		q, args := withTenant(
			`SELECT id, versioning, object_lock_enabled FROM buckets
			  WHERE name = $1 AND deleted_at IS NULL`, tenant, bucket)
		err := tx.QueryRow(ctx, q+` FOR UPDATE`, args...).Scan(&id, &versioning, &lockOn)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("lock bucket %q: %w", bucket, err)
		}

		if !enabled {
			if lockOn {
				return ErrLockCannotDisable
			}
			return ErrNotConfigured
		}
		if versioning != VersioningEnabled {
			return ErrLockNeedsVersioning
		}

		if _, err := tx.Exec(ctx,
			`UPDATE buckets SET object_lock_enabled = true, lock_config = $2 WHERE id = $1`,
			id, string(config)); err != nil {
			return fmt.Errorf("set lock config of %q: %w", bucket, err)
		}
		return nil
	})
}

// GetObjectLockConfig отдаёт конфигурацию байт в байт. ErrNotConfigured —
// бакет есть, лока нет; это не ErrNotFound, и подменять их нельзя.
func (m *Meta) GetObjectLockConfig(ctx context.Context, tenant TenantID, bucket string) ([]byte, error) {
	var cfg *string
	q, args := withTenant(
		`SELECT lock_config FROM buckets WHERE name = $1 AND deleted_at IS NULL`,
		tenant, bucket)
	err := m.Pool.QueryRow(ctx, q, args...).Scan(&cfg)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("lock config of %q: %w", bucket, err)
	}
	if cfg == nil || *cfg == "" {
		return nil, ErrNotConfigured
	}
	return []byte(*cfg), nil
}
