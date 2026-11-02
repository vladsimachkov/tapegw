// Package s3err — маппинг внутренних ошибок в ошибки S3.
//
// Правило 5 из AGENTS.md: коды и заголовки берём из versitygw, а не придумываем.
// Здесь только перевод того, чего versitygw сам перевести не может: ошибок
// слоя meta и ошибок store.
package s3err

import (
	"errors"

	"github.com/versity/versitygw/s3err"

	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/store"
)

// FromMeta переводит ошибки пакета meta в S3-ошибки с указанием бакета.
// Неизвестную ошибку возвращает как есть: ProcessController в versitygw
// превратит её в InternalError и залогирует — терять первоначальную причину
// хуже, чем показать 500.
func FromMeta(err error, bucket string) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, meta.ErrNotFound):
		return s3err.GetBucketErr(s3err.ErrNoSuchBucket, bucket)
	case errors.Is(err, meta.ErrBucketNotEmpty):
		return s3err.GetBucketErr(s3err.ErrBucketNotEmpty, bucket)
	case errors.Is(err, meta.ErrBucketOwnedByOther):
		return s3err.GetBucketErr(s3err.ErrBucketAlreadyOwnedByYou, bucket)
	default:
		return err
	}
}

// Retryable сообщает, стоит ли воркеру ретраить ошибку store. ErrBusy и
// ErrNoSpace — временные, всё остальное фатально для конкретного объекта.
func Retryable(err error) bool {
	return errors.Is(err, store.ErrBusy) || errors.Is(err, store.ErrNoSpace)
}
