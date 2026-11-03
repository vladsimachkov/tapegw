package meta

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
