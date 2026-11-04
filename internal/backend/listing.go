package backend

import (
	"context"
	"encoding/base64"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"

	"github.com/vladsimachkov/tapegw/internal/meta"
)

// deref — пустая строка вместо nil: все поля листинга в S3 опциональны, и
// «не передано» и «передана пустая строка» для них означают одно и то же.
func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// Листинг объектов и версий.
//
// Страж (ADR-0017) здесь работает иначе, чем в точечных операциях: он не
// проверяет бакет, а **фильтрует** выдачу. Для чужого тенанта запрос ничего не
// найдёт, и правильный ответ — пустая выдача, а не AccessDenied: по ответу
// «пусто» клиент не узнает, что бакет существует. Тот же приём, что в
// ListBuckets.
//
// Пустая выдача — не «бакета нет»: если бакет существует у другого тенанта,
// NoSuchBucket тоже был бы утечкой факта существования.

// defaultMaxKeys — столько же, сколько по умолчанию у S3.
const defaultMaxKeys = 1000

func listBucket(in *string) (string, error) {
	if in == nil {
		return "", s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	return *in, nil
}

func maxKeysOf(v *int32) int {
	if v == nil {
		return defaultMaxKeys
	}
	return int(*v)
}

// ownerOf берёт владельца бакета: S3 показывает Owner каждой записи, и берём
// мы его у бакета, а не у учётки запроса — объект принадлежит владельцу бакета.
func (t *TapeGW) ownerOf(ctx context.Context, tenant meta.TenantID, bucket string) *types.Owner {
	b, err := t.meta.GetBucket(ctx, tenant, bucket)
	if err != nil || b == nil {
		return nil
	}
	owner := b.Owner
	return &types.Owner{ID: &owner}
}

func listedObjects(res meta.ListResult, owner *types.Owner) []s3response.Object {
	out := make([]s3response.Object, 0, len(res.Entries))
	for _, e := range res.Entries {
		key := e.Key
		size := e.Size
		etag := e.ETag
		mod := e.LastModified
		class := types.ObjectStorageClass(e.StorageClass)
		out = append(out, s3response.Object{
			Key:          &key,
			Size:         &size,
			ETag:         &etag,
			LastModified: &mod,
			StorageClass: class,
			Owner:        owner,
		})
	}
	return out
}

func commonPrefixes(res meta.ListResult) []types.CommonPrefix {
	out := make([]types.CommonPrefix, 0, len(res.CommonPrefixes))
	for _, p := range res.CommonPrefixes {
		pfx := p
		out = append(out, types.CommonPrefix{Prefix: &pfx})
	}
	return out
}

func strPtr(s string) *string { return &s }

// encodeToken делает из ключа непрерывающий токен для ListObjectsV2. S3
// обещает, что ContinuationToken для клиента непрозрачен, поэтому ключ
// кодируем: клиент не должен строить по нему свои догадки и передавать то, чего
// мы никогда не отдавали.
func encodeToken(key string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(key))
}

func decodeToken(tok string) (string, error) {
	b, err := base64.RawURLEncoding.DecodeString(tok)
	if err != nil {
		return "", s3err.GetAPIError(s3err.ErrInvalidToken)
	}
	return string(b), nil
}

// ListObjects — ListObjects v1.
func (t *TapeGW) ListObjects(ctx context.Context,
	input *s3.ListObjectsInput) (s3response.ListObjectsResult, error) {

	bucket, err := listBucket(input.Bucket)
	if err != nil {
		return s3response.ListObjectsResult{}, err
	}
	tenant, err := tenantFromContext(ctx)
	if err != nil {
		return s3response.ListObjectsResult{}, err
	}

	marker := deref(input.Marker)
	res, err := t.meta.ListObjectsCurrent(ctx, tenant, bucket,
		deref(input.Prefix), deref(input.Delimiter), marker, maxKeysOf(input.MaxKeys))
	if err != nil {
		return s3response.ListObjectsResult{}, mapMetaErr(err, bucket)
	}

	out := s3response.ListObjectsResult{
		Name:           strPtr(bucket),
		Prefix:         input.Prefix,
		Marker:         input.Marker,
		MaxKeys:        input.MaxKeys,
		Delimiter:      input.Delimiter,
		IsTruncated:    &res.Truncated,
		Contents:       listedObjects(res, t.ownerOf(ctx, tenant, bucket)),
		CommonPrefixes: commonPrefixes(res),
		EncodingType:   input.EncodingType,
	}
	if res.Truncated {
		out.NextMarker = strPtr(res.NextKey)
	}
	return out, nil
}

// ListObjectsV2 — ListObjects V2. Маркер здесь непрозрачный ContinuationToken,
// а StartAfter — открытый «начать после этого ключа».
func (t *TapeGW) ListObjectsV2(ctx context.Context,
	input *s3.ListObjectsV2Input) (s3response.ListObjectsV2Result, error) {

	bucket, err := listBucket(input.Bucket)
	if err != nil {
		return s3response.ListObjectsV2Result{}, err
	}
	tenant, err := tenantFromContext(ctx)
	if err != nil {
		return s3response.ListObjectsV2Result{}, err
	}

	// ContinuationToken имеет приоритет над StartAfter: так ведёт себя S3, и
	// иначе клиент, передавший оба, получил бы не ту страницу, на которую
	// смотрит в своём же запросе.
	marker := deref(input.StartAfter)
	if input.ContinuationToken != nil && *input.ContinuationToken != "" {
		marker, err = decodeToken(*input.ContinuationToken)
		if err != nil {
			return s3response.ListObjectsV2Result{}, err
		}
	}

	res, err := t.meta.ListObjectsCurrent(ctx, tenant, bucket,
		deref(input.Prefix), deref(input.Delimiter), marker, maxKeysOf(input.MaxKeys))
	if err != nil {
		return s3response.ListObjectsV2Result{}, mapMetaErr(err, bucket)
	}

	keyCount := int32(len(res.Entries))
	out := s3response.ListObjectsV2Result{
		Name:              strPtr(bucket),
		Prefix:            input.Prefix,
		StartAfter:        input.StartAfter,
		ContinuationToken: input.ContinuationToken,
		KeyCount:          &keyCount,
		MaxKeys:           input.MaxKeys,
		Delimiter:         input.Delimiter,
		IsTruncated:       &res.Truncated,
		Contents:          listedObjects(res, t.ownerOf(ctx, tenant, bucket)),
		CommonPrefixes:    commonPrefixes(res),
		EncodingType:      input.EncodingType,
	}
	if res.Truncated {
		out.NextContinuationToken = strPtr(encodeToken(res.NextKey))
	}
	return out, nil
}

// ListObjectVersions — версии и маркеры удаления.
func (t *TapeGW) ListObjectVersions(ctx context.Context,
	input *s3.ListObjectVersionsInput) (s3response.ListVersionsResult, error) {

	bucket, err := listBucket(input.Bucket)
	if err != nil {
		return s3response.ListVersionsResult{}, err
	}
	tenant, err := tenantFromContext(ctx)
	if err != nil {
		return s3response.ListVersionsResult{}, err
	}

	res, err := t.meta.ListObjectVersions(ctx, tenant, bucket,
		deref(input.Prefix), deref(input.Delimiter),
		deref(input.KeyMarker), deref(input.VersionIdMarker),
		maxKeysOf(input.MaxKeys))
	if err != nil {
		return s3response.ListVersionsResult{}, mapMetaErr(err, bucket)
	}

	owner := t.ownerOf(ctx, tenant, bucket)
	var versions []s3response.ObjectVersion
	var markers []s3response.DeleteMarkerEntry
	for _, e := range res.Entries {
		key, mod := e.Key, e.LastModified
		ver := e.VersionID
		size := e.Size
		if e.IsDeleteMarker {
			markers = append(markers, s3response.DeleteMarkerEntry{
				Key:          &key,
				VersionId:    &ver,
				LastModified: &mod,
				Owner:        owner,
			})
			continue
		}
		etag := e.ETag
		class := types.ObjectVersionStorageClass(e.StorageClass)
		versions = append(versions, s3response.ObjectVersion{
			Key:          &key,
			VersionId:    &ver,
			ETag:         &etag,
			Size:         &size,
			LastModified: &mod,
			StorageClass: class,
			Owner:        owner,
		})
	}

	out := s3response.ListVersionsResult{
		Name:            strPtr(bucket),
		Prefix:          input.Prefix,
		KeyMarker:       input.KeyMarker,
		VersionIdMarker: input.VersionIdMarker,
		MaxKeys:         input.MaxKeys,
		Delimiter:       input.Delimiter,
		IsTruncated:     &res.Truncated,
		Versions:        versions,
		DeleteMarkers:   markers,
		CommonPrefixes:  commonPrefixes(res),
		EncodingType:    input.EncodingType,
	}
	if res.Truncated {
		out.NextKeyMarker = strPtr(res.NextKey)
		// У common prefix версии нет, поэтому и маркер версии пустой: следующая
		// страница начнётся с первого ключа после префикса.
		if res.NextVersionID != "" {
			out.NextVersionIdMarker = strPtr(res.NextVersionID)
		}
	}
	return out, nil
}
