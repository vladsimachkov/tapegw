package backend

import (
	"context"
	"encoding/json"
	"errors"

	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/valyala/fasthttp"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"

	"github.com/vladsimachkov/tapegw/internal/meta"
)

// Retention и legal hold версии, а также то, что попадает в версию при записи.
//
// Версию адресуют по versionId, и "null" — тоже адрес: null-версия в
// приостановленном бакете существует и может быть защищена.

// bypassGovernanceHeader — заголовок запроса на обход GOVERNANCE. Имя
// регистронезависимо, но пишем как в S3.
const bypassGovernanceHeader = "x-amz-bypass-governance-retention"

// bucketLockConfig — конфигурация лока бакета, либо nil, если лока нет.
// Ошибку «бакет чужой» не глотает: её обязан увидеть вызывающий.
func (t *TapeGW) bucketLockConfig(ctx context.Context, tenant meta.TenantID,
	bucket string) (*auth.BucketLockConfig, error) {

	raw, err := t.meta.GetObjectLockConfig(ctx, tenant, bucket)
	if errors.Is(err, meta.ErrNotConfigured) || errors.Is(err, meta.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var cfg auth.BucketLockConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, nil
	}
	if !cfg.Enabled {
		return nil, nil
	}
	return &cfg, nil
}

// lockAtWrite решает, что записать в retention и legal hold новой версии.
//
// Явные заголовки PUT важнее правила бакета — иначе клиент, который просит
// семь дней, получил бы тридцать только потому, что бакет настроен щедрее.
// Правило по умолчанию применяется, только когда лок в бакете включён.
func (t *TapeGW) lockAtWrite(ctx context.Context, tenant meta.TenantID,
	bucket string, po *s3response.PutObjectInput,
) (mode string, until *time.Time, hold bool, err error) {

	hold = po.ObjectLockLegalHoldStatus == types.ObjectLockLegalHoldStatusOn

	if po.ObjectLockMode != "" {
		if po.ObjectLockRetainUntilDate == nil {
			return "", nil, false, s3err.GetBucketErr(s3err.ErrInvalidRequest, bucket)
		}
		return string(po.ObjectLockMode), po.ObjectLockRetainUntilDate, hold, nil
	}

	cfg, err := t.bucketLockConfig(ctx, tenant, bucket)
	if err != nil || cfg == nil {
		return "", nil, hold, err
	}
	def := cfg.DefaultObjectRetention(time.Now())
	if def == nil {
		return "", nil, hold, nil
	}
	return string(def.Mode), def.RetainUntilDate, hold, nil
}

// lockHeaders — что показать в ответе чтения. Пустой режим — заголовков нет.
func lockHeaders(obj *meta.Object) (types.ObjectLockMode, *time.Time, types.ObjectLockLegalHoldStatus) {
	if obj.RetentionMode != nil {
		if obj.LegalHold {
			return types.ObjectLockMode(*obj.RetentionMode), obj.RetainUntil,
				types.ObjectLockLegalHoldStatusOn
		}
		return types.ObjectLockMode(*obj.RetentionMode), obj.RetainUntil, ""
	}
	if obj.LegalHold {
		return "", nil, types.ObjectLockLegalHoldStatusOn
	}
	return "", nil, ""
}

// lockVersionGuard: видимость бакета раньше разбора версии. Иначе для чужого
// бакета все версии «не существуют», и это уже другой ответ, чем для своего.
func (t *TapeGW) lockVersionGuard(ctx context.Context, bucket string) (meta.TenantID, error) {
	tenant, err := tenantFromContext(ctx)
	if err != nil {
		return tenant, err
	}
	if _, err := t.meta.GetBucket(ctx, tenant, bucket); err != nil {
		return tenant, t.bucketErr(ctx, bucket, tenant, err)
	}
	return tenant, nil
}

func (t *TapeGW) PutObjectRetention(ctx context.Context, bucket, object,
	versionID string, retention []byte) error {

	tenant, err := t.lockVersionGuard(ctx, bucket)
	if err != nil {
		return err
	}
	var r types.ObjectLockRetention
	if err := json.Unmarshal(retention, &r); err != nil || r.Mode == "" || r.RetainUntilDate == nil {
		return s3err.GetBucketErr(s3err.ErrMalformedXML, bucket)
	}

	err = t.meta.PutObjectRetention(ctx, tenant, bucket, object, versionID,
		string(r.Mode), *r.RetainUntilDate)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, meta.ErrLockNotEnabled):
		return s3err.GetBucketErr(s3err.ErrMissingObjectLockConfiguration, bucket)
	case errors.Is(err, meta.ErrRetentionShorten), errors.Is(err, meta.ErrRetentionModeChange):
		// Сократить или снять COMPLIANCE нельзя — отказ, а не «не умею».
		return s3err.GetBucketErr(s3err.ErrAccessDenied, bucket)
	case errors.Is(err, meta.ErrRetentionOnMarker):
		return s3err.GetBucketErr(s3err.ErrInvalidRequest, bucket)
	case errors.Is(err, meta.ErrNoSuchVersion):
		return s3err.GetBucketErr(s3err.ErrNoSuchVersion, bucket)
	}
	return t.bucketErr(ctx, bucket, tenant, err)
}

// GetObjectRetention — «retention нет» и «версии нет» различаются: первое это
// NoSuchObjectLockConfiguration (200-подобный ответ AWS на штатный вопрос),
// второе — NoSuchVersion.
func (t *TapeGW) GetObjectRetention(ctx context.Context,
	bucket, object, versionID string) ([]byte, error) {

	tenant, err := t.lockVersionGuard(ctx, bucket)
	if err != nil {
		return nil, err
	}
	mode, until, err := t.meta.GetObjectRetention(ctx, tenant, bucket, object, versionID)
	switch {
	case err == nil:
		out, merr := json.Marshal(types.ObjectLockRetention{
			Mode:            types.ObjectLockRetentionMode(mode),
			RetainUntilDate: &until,
		})
		if merr != nil {
			return nil, merr
		}
		return out, nil
	case errors.Is(err, meta.ErrNotConfigured):
		return nil, s3err.GetBucketErr(s3err.ErrNoSuchObjectLockConfiguration, bucket)
	case errors.Is(err, meta.ErrNoSuchVersion):
		return nil, s3err.GetBucketErr(s3err.ErrNoSuchVersion, bucket)
	}
	return nil, t.bucketErr(ctx, bucket, tenant, err)
}

func (t *TapeGW) PutObjectLegalHold(ctx context.Context, bucket, object,
	versionID string, status bool) error {

	tenant, err := t.lockVersionGuard(ctx, bucket)
	if err != nil {
		return err
	}
	err = t.meta.PutObjectLegalHold(ctx, tenant, bucket, object, versionID, status)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, meta.ErrLockNotEnabled):
		return s3err.GetBucketErr(s3err.ErrMissingObjectLockConfiguration, bucket)
	case errors.Is(err, meta.ErrRetentionOnMarker):
		return s3err.GetBucketErr(s3err.ErrInvalidRequest, bucket)
	case errors.Is(err, meta.ErrNoSuchVersion):
		return s3err.GetBucketErr(s3err.ErrNoSuchVersion, bucket)
	}
	return t.bucketErr(ctx, bucket, tenant, err)
}

func (t *TapeGW) GetObjectLegalHold(ctx context.Context,
	bucket, object, versionID string) (*bool, error) {

	tenant, err := t.lockVersionGuard(ctx, bucket)
	if err != nil {
		return nil, err
	}
	on, err := t.meta.GetObjectLegalHold(ctx, tenant, bucket, object, versionID)
	if errors.Is(err, meta.ErrNoSuchVersion) {
		return nil, s3err.GetBucketErr(s3err.ErrNoSuchVersion, bucket)
	}
	if err != nil {
		return nil, t.bucketErr(ctx, bucket, tenant, err)
	}
	return &on, nil
}

// bypassGovernanceRequested — запросил ли клиент обход GOVERNANCE.
//
// Источник — заголовок запроса, а не поле s3.DeleteObjectInput: контроллер
// versitygw это поле не заполняет (object-delete.go:221, bucket-post.go:153),
// поэтому по полю бэкенд всегда видел бы «обход не запрошен» и отклонял бы
// любой GOVERNANCE — то есть GOVERNANCE вел бы себя как COMPLIANCE.
//
// Правило то же, что в контроллере: EqualFold со "true"
// (object-delete.go:144). Право на обход здесь не проверяется: это сделал
// versitygw до вызова бэкенда (ADR-0015). COMPLIANCE и legal hold заголовком
// не обходятся никогда.
func bypassGovernanceRequested(ctx context.Context) bool {
	rc, ok := ctx.(*fasthttp.RequestCtx)
	if !ok {
		// Не HTTP-контекст: прямого вызова бэкенда из воркера или админ-API
		// с обходом не бывает, поэтому отсутствие запроса — безопасный ответ.
		return false
	}
	return strings.EqualFold(string(rc.Request.Header.Peek(bypassGovernanceHeader)), "true")
}
