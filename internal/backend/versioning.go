package backend

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"

	"github.com/vladsimachkov/tapegw/internal/meta"
)

// Версионирование бакета и конфигурация Object Lock.
//
// Порядок вызовов диктует versitygw: перед каждым PUT он читает
// GetObjectLockConfiguration и GetBucketVersioning, поэтому оба метода обязаны
// отвечать корректно и на чужом, и на несуществующем бакете — иначе по их ответу
// чужой бакет выдаёт своё существование раньше, чем отказал бы настоящий метод
// (правило 4 в AGENTS.md: заглушка, возвращающая «не сконфигурировано», опаснее
// отсутствия метода).

// PutBucketVersioning включает или приостанавливает версионирование.
//
// Переход «в unversioned» невозможен в принципе: контроллер versitygw
// пропускает только Enabled и Suspended.
func (t *TapeGW) PutBucketVersioning(ctx context.Context, bucket string,
	status types.BucketVersioningStatus) error {

	tenant, err := tenantFromContext(ctx)
	if err != nil {
		return err
	}
	// Страж раньше разбора аргументов: по коду отказа чужой бакет не должен
	// отличаться от своего. Иначе MalformedXML отсюда выдаёт существование
	// чужого бакета — и наоборот, чужой бакет с битым аргументом выглядит как
	// «аргумент битый», хотя отказ должен быть один.
	if _, err := t.meta.GetBucket(ctx, tenant, bucket); err != nil {
		return t.bucketErr(ctx, bucket, tenant, err)
	}
	// Статусы API — «Enabled»/«Suspended» с заглавной, значения enum в БД —
	// строчными. Протащить строку как есть значит получить
	// `invalid input value for enum bucket_versioning` на штатном запросе.
	state, ok := versioningStateFromAPI(status)
	if !ok {
		return s3err.GetBucketErr(s3err.ErrMalformedXML, bucket)
	}
	err = t.meta.SetBucketVersioning(ctx, tenant, bucket, state)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, meta.ErrSuspendWithLock):
		return s3err.GetBucketErr(s3err.ErrSuspendedVersioningNotAllowed, bucket)
	case errors.Is(err, meta.ErrSuspendNeverEnabled):
		return s3err.GetBucketErr(s3err.ErrInvalidRequest, bucket)
	}
	return t.bucketErr(ctx, bucket, tenant, err)
}

// versioningStateFromAPI переводит статус S3 в значение enum БД. Неизвестное
// значение — отказ: контроллер versitygw пропускает только два статуса, но
// полагаться на это «ниже по потоку» нельзя.
func versioningStateFromAPI(status types.BucketVersioningStatus) (string, bool) {
	switch status {
	case types.BucketVersioningStatusEnabled:
		return meta.VersioningEnabled, true
	case types.BucketVersioningStatusSuspended:
		return meta.VersioningSuspended, true
	}
	return "", false
}

// GetBucketVersioning отдаёт текущее состояние. Для бакета без версий Status
// пустой — это «версий никогда не было», как у AWS.
func (t *TapeGW) GetBucketVersioning(ctx context.Context,
	bucket string) (s3response.GetBucketVersioningOutput, error) {

	out := s3response.GetBucketVersioningOutput{}
	tenant, err := tenantFromContext(ctx)
	if err != nil {
		return out, err
	}
	state, err := t.meta.BucketVersioningState(ctx, tenant, bucket)
	if err != nil {
		return out, t.bucketErr(ctx, bucket, tenant, err)
	}
	switch state {
	case meta.VersioningEnabled:
		out.Status = ptr(types.BucketVersioningStatusEnabled)
	case meta.VersioningSuspended:
		out.Status = ptr(types.BucketVersioningStatusSuspended)
	}
	return out, nil
}

// PutObjectLockConfiguration включает Object Lock. Конфигурация приходит уже
// разобранной и пересобранной versitygw (XML → auth.BucketLockConfig → JSON),
// и мы храним эти байты как есть.
//
// Включённый лок выключить нельзя; правило retention по умолчанию заменить
// можно — оно влияет только на новые записи.
func (t *TapeGW) PutObjectLockConfiguration(ctx context.Context,
	bucket string, config []byte) error {

	tenant, err := tenantFromContext(ctx)
	if err != nil {
		return err
	}
	// Порядок тот же, что у PutBucketVersioning: сначала видимость бакета,
	// потом разбор аргументов.
	if _, err := t.meta.GetBucket(ctx, tenant, bucket); err != nil {
		return t.bucketErr(ctx, bucket, tenant, err)
	}
	var cfg auth.BucketLockConfig
	if err := json.Unmarshal(config, &cfg); err != nil {
		return s3err.GetBucketErr(s3err.ErrMalformedXML, bucket)
	}
	err = t.meta.SetObjectLockConfig(ctx, tenant, bucket, cfg.Enabled, config)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, meta.ErrLockNeedsVersioning):
		return s3err.GetBucketErr(s3err.ErrObjectLockConfigurationNotAllowed, bucket)
	case errors.Is(err, meta.ErrLockCannotDisable):
		return s3err.GetBucketErr(s3err.ErrObjectLockConfigurationNotAllowed, bucket)
	case errors.Is(err, meta.ErrNotConfigured):
		return s3err.GetBucketErr(s3err.ErrMalformedXML, bucket)
	}
	return t.bucketErr(ctx, bucket, tenant, err)
}

// GetObjectLockConfiguration — «настройки нет» и «бакета нет» различаются
// обязательно. Если свести их вместе, versitygw в loadObjectLockState увидит
// отсутствие конфигурации для чужого бакета и пойдёт дальше вместо отказа:
// именно так заглушка тихо отключает неизменяемость.
func (t *TapeGW) GetObjectLockConfiguration(ctx context.Context, bucket string) ([]byte, error) {
	tenant, err := tenantFromContext(ctx)
	if err != nil {
		return nil, err
	}
	cfg, err := t.meta.GetObjectLockConfig(ctx, tenant, bucket)
	switch {
	case err == nil:
		return cfg, nil
	case errors.Is(err, meta.ErrNotConfigured):
		return nil, s3err.GetBucketErr(s3err.ErrObjectLockConfigurationNotFound, bucket)
	}
	return nil, t.bucketErr(ctx, bucket, tenant, err)
}
