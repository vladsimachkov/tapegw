package backend

import (
	"context"

	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"

	tapegwerr "github.com/vladsimachkov/tapegw/internal/s3err"
)

// Методы «у нас это не включено».
//
// Соблазн оставить их в BackendUnsupported (правило 4: неподдерживаемое —
// NotImplemented) здесь ловит на ловушке: versitygw сам обращается к этим
// операциям по пути обычного PUT. loadObjectLockState (auth/object_lock.go)
// вызывает GetObjectLockConfiguration перед записью любого объекта и терпит
// только ObjectLockConfigurationNotFound; NotImplemented превращается в 501 на
// PutObject, который вполне реализован.
//
// Разница между «операция не поддерживается» и «поддерживается, но у бакета
// ничего не включено» принципиальна, и обе честные. Версионирования и Object Lock
// в tapegw нет (вне MVP), поэтому корректный ответ — «не сконфигурировано»,
// а не «не умею». Тот же выбор сделал posix-бэкенд: он возвращает пустую
// конфигурацию versioning, потому что GUI-клиенты опрашивают её при просмотре
// бакета.

// GetObjectLockConfiguration — Object Lock вне MVP, значит конфигурации нет
// никогда. Проверяем существование бакета: для несуществующего должен быть
// NoSuchBucket, а не «не сконфигурировано».
func (t *TapeGW) GetObjectLockConfiguration(ctx context.Context, bucket string) ([]byte, error) {
	if _, err := t.meta.GetBucket(ctx, bucket); err != nil {
		return nil, tapegwerr.FromMeta(err, bucket)
	}
	return nil, s3err.GetBucketErr(s3err.ErrObjectLockConfigurationNotFound, bucket)
}

// GetBucketVersioning — версионирования нет (вне MVP). Отдаём 200 с пустой
// конфигурацией: это означает «версий никогда не было», как и делает AWS для
// бакета без versioning.
func (t *TapeGW) GetBucketVersioning(ctx context.Context, bucket string) (s3response.GetBucketVersioningOutput, error) {
	if _, err := t.meta.GetBucket(ctx, bucket); err != nil {
		return s3response.GetBucketVersioningOutput{}, tapegwerr.FromMeta(err, bucket)
	}
	return s3response.GetBucketVersioningOutput{}, nil
}
