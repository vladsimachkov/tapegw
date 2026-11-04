package backend

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/versity/versitygw/s3err"
	"github.com/vladsimachkov/tapegw/internal/meta"
)

// DeleteObject — удаление объекта или конкретной версии.
//
// Разложение по случаям живёт в meta (см. internal/meta/delete.go); здесь —
// страж, разбор входных полей и перевод ошибок в коды S3.
//
// Отказ Object Lock отдаётся как AccessDenied, а не NoSuchKey: версия
// существует, и клиент вправе знать, что она защищена, — иначе «удаление
// прошло» для заблокированной версии означало бы, что immutability молча не
// работает.
func (t *TapeGW) DeleteObject(ctx context.Context,
	input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {

	if input.Bucket == nil || input.Key == nil {
		return nil, s3err.GetAPIError(s3err.ErrInvalidRequest)
	}
	bucket, key := *input.Bucket, *input.Key

	tenant, err := tenantFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := validateBucketName(bucket); err != nil {
		return nil, s3err.GetBucketErr(s3err.ErrInvalidBucketName, bucket)
	}

	in := meta.DeleteObjectInput{
		Bucket: bucket,
		Key:    key,
		// BypassGovernanceRetention — явный запрос на обход GOVERNANCE. Право
		// на него (s3:BypassGovernanceRetention) проверяет versitygw на уровне
		// авторизации; здесь мы только принимаем сам запрос.
		BypassGovernance: input.BypassGovernanceRetention != nil && *input.BypassGovernanceRetention,
	}
	if input.VersionId != nil {
		in.VersionID = *input.VersionId
	}

	res, err := t.meta.DeleteObject(ctx, tenant, in)
	if err != nil {
		if errors.Is(err, meta.ErrObjectLocked) {
			return nil, s3err.GetBucketErr(s3err.ErrAccessDenied, bucket)
		}
		return nil, t.objectErr(ctx, bucket, tenant, err)
	}

	out := &s3.DeleteObjectOutput{}
	if res.VersionID != "" {
		ver := res.VersionID
		out.VersionId = &ver
	}
	if res.IsDeleteMarker {
		marker := true
		out.DeleteMarker = &marker
	}
	return out, nil
}
