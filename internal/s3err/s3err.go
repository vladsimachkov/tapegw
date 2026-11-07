// Package s3err — маппинг внутренних ошибок в ошибки S3.
//
// Правило 5 из AGENTS.md: коды и заголовки берём из versitygw, а не придумываем.
// Здесь только перевод того, чего versitygw сам перевести не может: ошибок
// слоя meta и ошибок store.
package s3err

import (
	"errors"
	"net/http"

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
	case errors.Is(err, meta.ErrNoBucketPolicy):
		// Единственная ошибка, которую versitygw терпит от GetBucketPolicy как
		// штатную: проверка доступа продолжается по пути «политики нет».
		// Всё остальное обрывает запрос, поэтому маппинг обязателен.
		return s3err.GetBucketErr(s3err.ErrNoSuchBucketPolicy, bucket)
	default:
		return err
	}
}

// ErrInvalidStorageClass — AWS отвечает на чужой x-amz-storage-class кодом
// InvalidStorageClass (400), а не NotImplemented (501). В s3err у versitygw
// v1.10.0 такой константы нет (проверено по всему пакету), поэтому собираем
// APIError напрямую: поля экспортируемые, а контроллер трактует его так же,
// как любой встроенный код.
//
// 501 означает «сервер этого не умеет», и клиент на него может откатиться на
// другую операцию или перестать ретраить. 400 говорит правду: запрос плохой,
// с другим storage class повторить можно и он сработает.
func InvalidStorageClass(class string) error {
	return s3err.APIError{
		Code: "InvalidStorageClass",
		Description: "The storage class you specified is not supported by this " +
			"gateway: tapegw хранит всё на ленте и отвечает GLACIER (" + class + ")",
		HTTPStatusCode: http.StatusBadRequest,
	}
}

// RestoreAlreadyInProgress — 409 на повторный RestoreObject, когда запрос уже
// стоит в очереди или выполняется. В s3err у versitygw v1.10.0 такой
// константы нет (проверено по всему пакету), поэтому собираем APIError
// напрямую — тот же приём, что с InvalidStorageClass. AWS отдаёт именно этот
// код (Glacier API), и клиенты бэкап-софта на него ветвятся.
func RestoreAlreadyInProgress(bucket string) error {
	return s3err.APIError{
		Code:           "RestoreAlreadyInProgress",
		Description:    "Restore request is already in progress for object in bucket " + bucket,
		HTTPStatusCode: http.StatusConflict,
	}
}

// Retryable сообщает, стоит ли воркеру ретраить ошибку store. ErrBusy и
// ErrNoSpace — временные, всё остальное фатально для конкретного объекта.
func Retryable(err error) bool {
	return errors.Is(err, store.ErrBusy) || errors.Is(err, store.ErrNoSpace)
}
