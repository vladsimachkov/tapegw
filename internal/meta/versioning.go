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
