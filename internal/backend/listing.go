package backend

import (
	"context"
	"encoding/base64"
	"net/url"
	"strings"

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
// Страж здесь ведёт себя так же, как в точечных операциях (ADR-0017): бакета
// нет — NoSuchBucket, бакет чужой — AccessDenied, аноним — AccessDenied.
//
// Соблазн «фильтровать выдачу, а не отказывать» (как в ListBuckets) здесь
// неверен по трём причинам. Листинг несуществующего бакета обязан быть ошибкой:
// клиенты проверяют им существование бакета, и опечатка в имени выглядела бы
// как «бакет пуст». Имена бакетов глобальны, и существование чужого бакета так
// или иначе выдаёт CreateBucket. А для обычной учётки versitygw ещё до листинга
// вызывает GetBucketAcl, который под стражом уже отказывает — то есть
// фильтрация ничего не прячет, но делает поведение разным для разных путей.
//
// Фильтровать по тенанту остаётся только ListBuckets: он и по смыслу показывает
// список СВОИХ бакетов.

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

// Кодирование ключей для encoding-type=url.
//
// Для ListObjects и ListObjectsV2 ключи кодирует контроллер versitygw
// (URLEncodeListObjectsResult и URLEncodeListObjectsV2Result в
// s3api/controllers/bucket-get.go), поэтому бэкенд обязан отдавать ключи
// как есть: иначе контроллер закодирует второй раз, и «a b» станет «a%2520b».
//
// Для ListObjectVersions контроллер кодирования НЕ делает вовсе — а клиент
// (botocore у aws cli и boto3) по умолчанию просит encoding-type=url и
// декодирует ответ, когда видит его. Значит некодированный ключ с «+» придёт
// обратно пробелом, а «%41» — как «A»: имени, которое клиент получил, не
// существует, и прочитать или удалить эту версию он уже не сможет.
//
// Рецепт повторяет versitygw дословно: url.QueryEscape плюс те же замены, чтобы
// «/» и «*» остались как есть, а «~» экранировалась.

// encodingRequested — запрошено ли кодирование. Сравнение нечувствительно к
// регистру: versitygw для v1/v2 принимает и «uRl» (есть такой случай в его
// тестах), а для ListObjectVersions контроллер параметр вообще не разбирает —
// то есть регистр доходит до нас как есть. Чувствительное сравнение молча
// вернула бы «URL» эхом без кодирования, и клиент стал бы декодировать то, что
// мы не кодировали.
func encodingRequested(et types.EncodingType) bool {
	return strings.EqualFold(string(et), string(types.EncodingTypeUrl))
}

// s3KeyNameEscaper — та же таблица замен, что у versitygw.
var s3KeyNameEscaper = strings.NewReplacer("%2F", "/", "%2A", "*", "~", "%7E")

func urlEncodeKeyName(name *string) *string {
	if name == nil {
		return nil
	}
	encoded := s3KeyNameEscaper.Replace(url.QueryEscape(*name))
	return &encoded
}

func urlEncodeCommonPrefixes(prefixes []types.CommonPrefix) []types.CommonPrefix {
	if prefixes == nil {
		return nil
	}
	out := make([]types.CommonPrefix, len(prefixes))
	for i, cp := range prefixes {
		cp.Prefix = urlEncodeKeyName(cp.Prefix)
		out[i] = cp
	}
	return out
}

// encodeVersionsResult кодирует выдачу ListObjectVersions. Поля из input
// не мутируются: указатели на них клиент переиспользует, а перекодированное
// значение по чужому указателю изменило бы запрос клиента.
func encodeVersionsResult(res *s3response.ListVersionsResult) {
	res.Delimiter = urlEncodeKeyName(res.Delimiter)
	res.Prefix = urlEncodeKeyName(res.Prefix)
	res.KeyMarker = urlEncodeKeyName(res.KeyMarker)
	res.NextKeyMarker = urlEncodeKeyName(res.NextKeyMarker)
	res.CommonPrefixes = urlEncodeCommonPrefixes(res.CommonPrefixes)

	for i := range res.Versions {
		res.Versions[i].Key = urlEncodeKeyName(res.Versions[i].Key)
	}
	for i := range res.DeleteMarkers {
		res.DeleteMarkers[i].Key = urlEncodeKeyName(res.DeleteMarkers[i].Key)
	}
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

	if ok, err := t.meta.BucketExists(ctx, tenant, bucket); err != nil {
		return s3response.ListObjectsResult{}, mapMetaErr(err, bucket)
	} else if !ok {
		// Отдельная проверка, а не «фильтр ничего не нашёл»: несуществующий
		// бакет обязан быть NoSuchBucket, а не пустой выдачей.
		return s3response.ListObjectsResult{}, t.bucketErr(ctx, bucket, tenant, meta.ErrNotFound)
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

	if ok, err := t.meta.BucketExists(ctx, tenant, bucket); err != nil {
		return s3response.ListObjectsV2Result{}, mapMetaErr(err, bucket)
	} else if !ok {
		// Отдельная проверка, а не «фильтр ничего не нашёл»: несуществующий
		// бакет обязан быть NoSuchBucket, а не пустой выдачей.
		return s3response.ListObjectsV2Result{}, t.bucketErr(ctx, bucket, tenant, meta.ErrNotFound)
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

	if ok, err := t.meta.BucketExists(ctx, tenant, bucket); err != nil {
		return s3response.ListVersionsResult{}, mapMetaErr(err, bucket)
	} else if !ok {
		// Отдельная проверка, а не «фильтр ничего не нашёл»: несуществующий
		// бакет обязан быть NoSuchBucket, а не пустой выдачей.
		return s3response.ListVersionsResult{}, t.bucketErr(ctx, bucket, tenant, meta.ErrNotFound)
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
	if encodingRequested(input.EncodingType) {
		// Возвращаем нормализованное значение: клиент, приславший «URL», обязан
		// увидеть в ответе то, что понимает botocore, а не собственный регистр.
		out.EncodingType = types.EncodingTypeUrl
		encodeVersionsResult(&out)
	}
	return out, nil
}
