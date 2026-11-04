package backend

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"

	"github.com/vladsimachkov/tapegw/internal/encrypt"
	"github.com/vladsimachkov/tapegw/internal/meta"
	tapegwerr "github.com/vladsimachkov/tapegw/internal/s3err"
	"github.com/vladsimachkov/tapegw/internal/spool"
)

// Multipart (M3 + T2 PR#2).
//
// Три вещи устроены здесь не так, как в posix-бэкенде, и все три — из-за
// шифрования:
//
//   - DEK один на всю загрузку, а не на часть: различает части номер части в
//     nonce и AAD (ADR-0014);
//   - размер каждой части входит в её AAD, поэтому размер обязан быть известен
//     заранее — UploadPart без ContentLength так же, как PutObject без него;
//   - при Complete части не перешифровываются: файл объекта — конкатенация
//     шифротекстов, а смещения и размеры ложатся в object_parts.
//
// Порядок «файл → строка» (правило 6) соблюдается и здесь: склейка и fsync
// заканчиваются до транзакции Postgres. Упавший между ними процесс оставит файл
// без строки — безвредного сироту, а не строку без файла.

const (
	// maxParts — ограничение S3: номера частей 1..10000.
	maxParts = 10000
)

// CreateMultipartUpload заводит загрузку: строку в Postgres и DEK для всех её
// частей.
func (t *TapeGW) CreateMultipartUpload(ctx context.Context,
	input s3response.CreateMultipartUploadInput,
) (s3response.InitiateMultipartUploadResult, error) {
	tenant, err := tenantFromContext(ctx)
	if err != nil {
		return s3response.InitiateMultipartUploadResult{}, err
	}
	bucket := aws.ToString(input.Bucket)
	key := aws.ToString(input.Key)

	// Бакет обязан существовать и принадлежать этому тенанту. Проверка до DEK:
	// заводить ключ под чужой бакет незачем.
	if _, err := t.meta.GetBucket(ctx, tenant, bucket); err != nil {
		// bucketErr, а не FromMeta: чужой бакет обязан ответить AccessDenied, а
		// не NoSuchBucket (ADR-0017, п. 1).
		return s3response.InitiateMultipartUploadResult{}, t.bucketErr(ctx, bucket, tenant, err)
	}
	ownerTenant, err := t.meta.BucketTenantID(ctx, tenant, bucket)
	if err != nil {
		return s3response.InitiateMultipartUploadResult{}, tapegwerr.FromMeta(err, bucket)
	}

	uploadID := uuid.NewString()
	// UUID версии заводится здесь, а не при Complete: он входит в nonce и AAD
	// каждого сегмента каждой части, а части грузятся до появления объекта.
	objectID := uuid.New()
	dek, wrapped, err := t.keys.NewUploadDEK(ctx, ownerTenant, uploadID)
	if err != nil {
		return s3response.InitiateMultipartUploadResult{}, fmt.Errorf("ключ загрузки: %w", err)
	}
	defer encrypt.Zero(dek)

	if err := t.meta.CreateUpload(ctx, tenant, meta.CreateUploadInput{
		Bucket:     bucket,
		Key:        key,
		ID:         uploadID,
		ObjectID:   objectID,
		WrappedDEK: wrapped,
		Meta:       uploadMetaFromCreate(input),
	}); err != nil {
		return s3response.InitiateMultipartUploadResult{}, tapegwerr.FromMeta(err, bucket)
	}

	return s3response.InitiateMultipartUploadResult{
		Bucket: bucket, Key: key, UploadId: uploadID,
	}, nil
}

// uploadMetaFromCreate — что запоминаем о загрузке. S3 разрешает задать это же
// при Complete, и там явные значения перекрывают сохранённые.
func uploadMetaFromCreate(input s3response.CreateMultipartUploadInput) meta.UploadMeta {
	m := meta.UploadMeta{
		ContentType:        input.ContentType,
		ContentEncoding:    input.ContentEncoding,
		ContentDisposition: input.ContentDisposition,
		CacheControl:       input.CacheControl,
		UserMeta:           input.Metadata,
		ChecksumAlgorithm:  string(input.ChecksumAlgorithm),
		ChecksumType:       string(input.ChecksumType),
	}
	if input.Tagging != nil {
		// Ошибку разбора тегов глотаем: versitygw проверяет их в контроллере,
		// а здесь теги — только сохранённое на будущее значение.
		if tags, err := backend.ParseObjectTags(aws.ToString(input.Tagging)); err == nil {
			m.Tags = tags
		}
	}
	return m
}

// UploadPart шифрует одну часть в отдельный файл спула.
//
// Файл — обычный файл спула с собственным UUID: так часть получает те же
// гарантии (fsync до ответа) и те же пути очистки, что и объект, и Complete не
// 需要 ничего знать о внутреннем устройстве записи.
func (t *TapeGW) UploadPart(ctx context.Context, input *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
	tenant, err := tenantFromContext(ctx)
	if err != nil {
		return nil, err
	}
	uploadID := aws.ToString(input.UploadId)
	up, err := t.meta.GetUpload(ctx, tenant, uploadID)
	if err != nil {
		return nil, t.uploadErr(ctx, aws.ToString(input.Bucket), tenant, err)
	}

	partNumber := int(aws.ToInt32(input.PartNumber))
	if partNumber < 1 || partNumber > maxParts {
		return nil, s3err.GetAPIError(s3err.ErrInvalidPartNumberRange)
	}
	declared := int64(0)
	if input.ContentLength != nil {
		declared = *input.ContentLength
	}
	if declared <= 0 {
		return nil, s3err.GetAPIError(s3err.ErrMissingContentLength)
	}

	dek, err := t.openUploadKey(ctx, tenant, up)
	if err != nil {
		return nil, err
	}
	defer encrypt.Zero(dek)

	id := uuid.New()
	limit := int64(t.cfg.S3.MaxObjectSize)
	// Соль — на каждую ПОПЫТКУ загрузки части. Переотправка части с тем же
	// номером штатна (retry после сетевой ошибки), а DEK на загрузку один:
	// без новой соли вторая попытка дала бы повтор nonce при том же ключе.
	salt, err := newSalt()
	if err != nil {
		return nil, err
	}
	info, err := t.spool.WritePartEncrypted(id, up.ObjectID, input.Body, spool.SumSpec{
		ContentMD5: str(input.ContentMD5),
		Algorithm:  input.ChecksumAlgorithm,
	}, limit, dek, declared, uint32(partNumber), salt)
	if err != nil {
		if errors.Is(err, spool.ErrTooLarge) {
			return nil, s3err.GetAPIError(s3err.ErrEntityTooLarge)
		}
		if errors.Is(err, spool.ErrSizeMismatch) {
			return nil, s3err.GetAPIError(s3err.ErrIncompleteBody)
		}
		var s3e s3err.S3Error
		if errors.As(err, &s3e) {
			return nil, err
		}
		return nil, fmt.Errorf("spool part write: %w", err)
	}

	old, err := t.meta.PutPart(ctx, tenant, uploadID, meta.PutPartInput{
		Number: partNumber,
		Size:   info.Size,
		ETag:   info.MD5Hex,
		FileID: id,
		Salt:   salt,
	})
	if err != nil {
		// Файл части без строки — сирота; убираем сразу.
		if rmErr := t.spool.Remove(id); rmErr != nil {
			return nil, fmt.Errorf("%w (и файл части %s не удалён: %v)", err, id, rmErr)
		}
		return nil, tapegwerr.FromMeta(err, up.Bucket)
	}
	// Переотправка части с тем же номером — штатный retry: прежний файл надо
	// стереть, иначе он останется в спуле навсегда.
	if old != uuid.Nil && old != id {
		if rmErr := t.spool.Remove(old); rmErr != nil {
			return nil, fmt.Errorf("spool remove old part %s: %w", old, rmErr)
		}
	}

	return &s3.UploadPartOutput{
		ETag: aws.String(quoteEtag(info.MD5Hex)),
	}, nil
}

// UploadPartCopy — копия диапазона существующей версии в часть загрузки.
//
// Данные расшифровываются ключом исходной версии и шифруются DEKом загрузки:
// части обязаны быть шифрованы одним ключом, иначе Complete не соберёт из них
// читаемый файл. Реflink здесь невозможен в принципе — открытого текста на
// диске нет.
//
// Источник проверяется стражем отдельно: копирование из чужого бакета — это
// чтение чужих данных, и «у меня есть право на загрузку» его не отменяет.
func (t *TapeGW) UploadPartCopy(ctx context.Context,
	input *s3.UploadPartCopyInput,
) (s3response.CopyPartResult, error) {
	tenant, err := tenantFromContext(ctx)
	if err != nil {
		return s3response.CopyPartResult{}, err
	}
	uploadID := aws.ToString(input.UploadId)
	up, err := t.meta.GetUpload(ctx, tenant, uploadID)
	if err != nil {
		return s3response.CopyPartResult{}, t.uploadErr(ctx, aws.ToString(input.Bucket), tenant, err)
	}
	partNumber := int(aws.ToInt32(input.PartNumber))
	if partNumber < 1 || partNumber > maxParts {
		return s3response.CopyPartResult{}, s3err.GetAPIError(s3err.ErrInvalidPartNumberRange)
	}

	srcBucket, srcKey, err := splitCopySource(aws.ToString(input.CopySource))
	if err != nil {
		return s3response.CopyPartResult{}, err
	}
	// Источник — текущая версия. Копирование по versionId невозможно: в этом SDK
	// у UploadPartCopyInput нет поля CopySourceVersionId (оно есть только у
	// CopyObjectInput), так что адресовать конкретную версию источника нечем.
	src, err := t.meta.GetObject(ctx, tenant, srcBucket, srcKey)
	if err != nil {
		return s3response.CopyPartResult{}, tapegwerr.FromMeta(err, srcBucket)
	}
	if src.IsDeleteMarker {
		return s3response.CopyPartResult{}, s3err.GetBucketErr(s3err.ErrNoSuchKey, srcBucket)
	}

	// Диапазон источника разбирает versitygw; мы только переводим байты в
	// размеры открытого текста.
	offset, length, err := copyRange(input.CopySourceRange, src.Size)
	if err != nil {
		return s3response.CopyPartResult{}, err
	}
	if length <= 0 {
		return s3response.CopyPartResult{}, s3err.GetBucketErr(s3err.ErrInvalidRange, srcBucket)
	}

	srcFile, err := t.spool.Open(src.ID)
	if err != nil {
		return s3response.CopyPartResult{}, fmt.Errorf("spool open %s: %w", src.ID, err)
	}
	// Диапазон отдаёт plainReader: он же умеет Range по обычным объектам, и
	// второй реализации «открытый диапазон → сегменты» здесь не нужно.
	body, err := t.plainReader(ctx, src, srcFile, offset, length)
	if err != nil {
		srcFile.Close()
		return s3response.CopyPartResult{}, err
	}
	defer body.Close()

	dek, err := t.openUploadKey(ctx, tenant, up)
	if err != nil {
		return s3response.CopyPartResult{}, err
	}
	defer encrypt.Zero(dek)

	id := uuid.New()
	salt, err := newSalt()
	if err != nil {
		return s3response.CopyPartResult{}, err
	}
	info, err := t.spool.WritePartEncrypted(id, up.ObjectID, body, spool.SumSpec{}, length, dek, length,
		uint32(partNumber), salt)
	if err != nil {
		var s3e s3err.S3Error
		if errors.As(err, &s3e) {
			return s3response.CopyPartResult{}, err
		}
		return s3response.CopyPartResult{}, fmt.Errorf("spool part copy: %w", err)
	}

	old, err := t.meta.PutPart(ctx, tenant, uploadID, meta.PutPartInput{
		Number: partNumber,
		Size:   info.Size,
		ETag:   info.MD5Hex,
		FileID: id,
		Salt:   salt,
	})
	if err != nil {
		if rmErr := t.spool.Remove(id); rmErr != nil {
			return s3response.CopyPartResult{}, fmt.Errorf("%w (и файл части %s не удалён: %v)", err, id, rmErr)
		}
		return s3response.CopyPartResult{}, tapegwerr.FromMeta(err, up.Bucket)
	}
	if old != uuid.Nil && old != id {
		if rmErr := t.spool.Remove(old); rmErr != nil {
			return s3response.CopyPartResult{}, fmt.Errorf("spool remove old part %s: %w", old, rmErr)
		}
	}

	return s3response.CopyPartResult{
		ETag:         aws.String(quoteEtag(info.MD5Hex)),
		LastModified: time.Now(),
	}, nil
}

// CompleteMultipartUpload собирает объект из частей.
//
// Порядок шагов выбран так, чтобы любая ошибка оставила состояние, из которого
// можно выйти: список частей проверяется до склейки (не пишем гигабайты
// впустую), склейка заканчивается до транзакции (правило 6), а удаление загрузки
// входит в ту же транзакцию, что и вставка объекта.
//
// Последнее важно не для порядка, а для атомарности: если бы загрузка удалялась
// отдельно, между «объект есть» и «загрузки нет» влез бы второй Complete с теми
// же частями — и из них выросла бы вторая версия.
func (t *TapeGW) CompleteMultipartUpload(ctx context.Context,
	input *s3.CompleteMultipartUploadInput,
) (s3response.CompleteMultipartUploadResult, string, error) {
	tenant, err := tenantFromContext(ctx)
	if err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", err
	}
	uploadID := aws.ToString(input.UploadId)
	up, err := t.meta.GetUpload(ctx, tenant, uploadID)
	if err != nil {
		return s3response.CompleteMultipartUploadResult{}, "",
			t.uploadErr(ctx, aws.ToString(input.Bucket), tenant, err)
	}

	var requested []types.CompletedPart
	if input.MultipartUpload != nil {
		requested = input.MultipartUpload.Parts
	}
	parts, err := validateCompleteParts(requested, up.Parts)
	if err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", err
	}

	var total int64
	for _, p := range parts {
		total += p.Size
	}
	if total <= 0 {
		return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrInvalidRequest)
	}

	// ETag собранного объекта считает versitygw (правило 5): md5 от конкатенации
	// md5 частей, суффикс — их число. Ему нужны CompletedPart с ETag из нашей
	// базы, а не из запроса клиента: запрос мы уже сверили с базой, но считать
	// итог по запросу означало бы доверять тому, что только что проверили.
	etagParts := make([]types.CompletedPart, 0, len(parts))
	for _, p := range parts {
		etagParts = append(etagParts, types.CompletedPart{
			PartNumber: aws.Int32(int32(p.Number)), ETag: aws.String(quoteEtag(p.ETag)),
		})
	}
	etag, err := backend.ComputeMultipartETagFromPartETags(uploadID, etagParts)
	if err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", err
	}

	dek, err := t.openUploadKey(ctx, tenant, up)
	if err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", err
	}
	defer encrypt.Zero(dek)

	ownerTenant, err := t.meta.BucketTenantID(ctx, tenant, up.Bucket)
	if err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", tapegwerr.FromMeta(err, up.Bucket)
	}

	id := up.ObjectID
	// Обёртка DEK переоборачивается под UUID версии: данные не меняются, а
	// привязка к версии появляется. Переставить такую обёртку в другой объект
	// нельзя — AAD не совпадёт.
	wrapped, err := t.keys.RewrapUploadDEKForVersion(ctx, ownerTenant, up.WrappedDEK, uploadID, id)
	if err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", fmt.Errorf("переобёртка DEK: %w", err)
	}

	// 1. Файл: склейка шифротекстов. До любой записи в Postgres.
	srcs := make([]uuid.UUID, 0, len(parts))
	for _, p := range parts {
		srcs = append(srcs, p.FileID)
	}
	cipherTotal, err := t.spool.Concatenate(id, srcs)
	if err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", fmt.Errorf("склейка частей: %w", err)
	}
	// Смещения шифротекста считаются от размеров открытого текста; если бы они
	// не сошлись с фактическим файлом, чтение потом читало бы мусор.
	var cipherAcc int64
	objParts := make([]meta.ObjectPart, 0, len(parts))
	for _, p := range parts {
		objParts = append(objParts, meta.ObjectPart{
			Number: p.Number, PlainSize: p.Size, CipherOffset: cipherAcc, Salt: p.Salt,
		})
		cipherAcc += encrypt.CipherSize(p.Size)
	}
	if cipherAcc != cipherTotal {
		if rmErr := t.spool.Remove(id); rmErr != nil {
			return s3response.CompleteMultipartUploadResult{}, "",
				fmt.Errorf("файл %s не удалён: %v (и смещения частей не сошлись: %d против %d)", id, rmErr, cipherAcc, cipherTotal)
		}
		return s3response.CompleteMultipartUploadResult{}, "",
			fmt.Errorf("смещения частей (%d) не совпали с файлом (%d)", cipherAcc, cipherTotal)
	}

	// Object Lock при завершении: явные заголовки Complete либо правило бакета.
	lockMode, lockUntil, lockHold, err := t.lockAtComplete(ctx, tenant, up.Bucket)
	if err != nil {
		t.removeQuietly(id)
		return s3response.CompleteMultipartUploadResult{}, "", err
	}

	tags := up.Meta.Tags

	// 2. Строки: объект, части, удаление загрузки — одна транзакция.
	obj, err := t.meta.PutObject(ctx, tenant, meta.PutObjectInput{
		ID:            id,
		Bucket:        up.Bucket,
		Key:           up.Key,
		Size:          total,
		ETag:          strings.Trim(etag, `"`),
		ContentType:   up.Meta.ContentType,
		UserMeta:      up.Meta.UserMeta,
		Tags:          tags,
		StorageClass:  "GLACIER",
		OnlineUntil:   time.Now().Add(t.cfg.Spool.LandingMinRetention),
		RetentionMode: lockMode,
		RetainUntil:   lockUntil,
		LegalHold:     lockHold,
		WrappedDEK:    wrapped,
		Parts:         objParts,
		InTx: func(ctx context.Context, tx pgx.Tx) error {
			return meta.DeleteUploadInTx(ctx, tx, uploadID)
		},
	})
	if err != nil {
		t.removeQuietly(id)
		return s3response.CompleteMultipartUploadResult{}, "", tapegwerr.FromMeta(err, up.Bucket)
	}

	versionID := ""
	switch obj.Versioning {
	case meta.VersioningEnabled:
		versionID = obj.ID.String()
	case meta.VersioningSuspended:
		versionID = "null"
	}

	// Файлы частей больше не нужны: их содержимое уже в файле объекта.
	for _, p := range parts {
		t.removeQuietly(p.FileID)
	}

	return s3response.CompleteMultipartUploadResult{
		Location: aws.String(up.Bucket),
		Bucket:   aws.String(up.Bucket),
		Key:      aws.String(up.Key),
		ETag:     aws.String(etag),
	}, versionID, nil
}

// AbortMultipartUpload удаляет загрузку и её файлы.
func (t *TapeGW) AbortMultipartUpload(ctx context.Context,
	input *s3.AbortMultipartUploadInput,
) error {
	tenant, err := tenantFromContext(ctx)
	if err != nil {
		return err
	}
	uploadID := aws.ToString(input.UploadId)
	ids, err := t.meta.DeleteUpload(ctx, tenant, uploadID)
	if err != nil {
		return t.uploadErr(ctx, aws.ToString(input.Bucket), tenant, err)
	}
	for _, id := range ids {
		t.removeQuietly(id)
	}
	return nil
}

// ListParts — части загрузки по номерам.
func (t *TapeGW) ListParts(ctx context.Context,
	input *s3.ListPartsInput,
) (s3response.ListPartsResult, error) {
	tenant, err := tenantFromContext(ctx)
	if err != nil {
		return s3response.ListPartsResult{}, err
	}
	uploadID := aws.ToString(input.UploadId)
	up, err := t.meta.GetUpload(ctx, tenant, uploadID)
	if err != nil {
		return s3response.ListPartsResult{}, t.uploadErr(ctx, aws.ToString(input.Bucket), tenant, err)
	}

	maxParts := 1000
	if input.MaxParts != nil {
		maxParts = int(*input.MaxParts)
	}
	marker := 0
	if input.PartNumberMarker != nil {
		if n, cerr := strconv.Atoi(*input.PartNumberMarker); cerr == nil {
			marker = n
		}
	}
	out := s3response.ListPartsResult{
		Bucket:               up.Bucket,
		Key:                  up.Key,
		UploadID:             uploadID,
		PartNumberMarker:     marker,
		StorageClass:         types.StorageClassGlacier,
		IsTruncated:          false,
		MaxParts:             maxParts,
		ChecksumAlgorithm:    types.ChecksumAlgorithm(up.Meta.ChecksumAlgorithm),
		ChecksumType:         types.ChecksumType(up.Meta.ChecksumType),
		Initiator:            s3response.Initiator{},
		NextPartNumberMarker: marker,
	}
	// Части всегда меньше тысячи, поэтому выборка целиком и срез по маркеру
	// здесь дешевле, чем отдельный запрос с LIMIT.
	sent := 0
	for _, p := range up.Parts {
		if p.Number <= marker {
			continue
		}
		if sent >= maxParts {
			out.IsTruncated = true
			break
		}
		out.Parts = append(out.Parts, s3response.Part{
			PartNumber:   p.Number,
			LastModified: up.Initiated,
			ETag:         quoteEtag(p.ETag),
			Size:         p.Size,
		})
		out.NextPartNumberMarker = p.Number
		sent++
	}
	return out, nil
}

// ListMultipartUploads — незавершённые загрузки бакета.
func (t *TapeGW) ListMultipartUploads(ctx context.Context,
	input *s3.ListMultipartUploadsInput,
) (s3response.ListMultipartUploadsResult, error) {
	tenant, err := tenantFromContext(ctx)
	if err != nil {
		return s3response.ListMultipartUploadsResult{}, err
	}
	bucket := aws.ToString(input.Bucket)
	if _, err := t.meta.GetBucket(ctx, tenant, bucket); err != nil {
		return s3response.ListMultipartUploadsResult{}, t.bucketErr(ctx, bucket, tenant, err)
	}

	max := 1000
	if input.MaxUploads != nil {
		max = int(*input.MaxUploads)
	}
	// +1: по лишней строке определяем, есть ли что показывать дальше.
	uploads, err := t.meta.UploadsByBucket(ctx, tenant, bucket, aws.ToString(input.Prefix),
		aws.ToString(input.KeyMarker), "", max+1)
	if err != nil {
		return s3response.ListMultipartUploadsResult{}, tapegwerr.FromMeta(err, bucket)
	}

	out := s3response.ListMultipartUploadsResult{
		Bucket:     bucket,
		KeyMarker:  aws.ToString(input.KeyMarker),
		Prefix:     aws.ToString(input.Prefix),
		Delimiter:  aws.ToString(input.Delimiter),
		MaxUploads: max,
	}
	truncated := len(uploads) > max
	if truncated {
		uploads = uploads[:max]
	}
	for _, u := range uploads {
		out.Uploads = append(out.Uploads, s3response.Upload{
			Key:          u.Key,
			UploadID:     u.ID,
			Initiated:    u.Initiated,
			StorageClass: types.StorageClassGlacier,
		})
	}
	if truncated && len(uploads) > 0 {
		last := uploads[len(uploads)-1]
		out.IsTruncated = true
		out.NextKeyMarker = last.Key
		out.NextUploadIDMarker = last.ID
	}
	return out, nil
}

// openUploadKey — DEK загрузки. Наружу не отдаётся: вызывающий обязан обнулить
// его сам, а это легче забыть.
func (t *TapeGW) openUploadKey(ctx context.Context, tenant meta.TenantID, up *meta.Upload) ([]byte, error) {
	ownerTenant, err := t.meta.BucketTenantID(ctx, tenant, up.Bucket)
	if err != nil {
		return nil, tapegwerr.FromMeta(err, up.Bucket)
	}
	dek, err := t.keys.OpenUploadDEK(ctx, ownerTenant, up.WrappedDEK, up.ID)
	if err != nil {
		// Не «шифруем открытым текстом», а отказ: ключ тенанта не подошёл, и
		// части этого тенанта нам больше не прочитать.
		return nil, fmt.Errorf("ключ загрузки %s: %w", up.ID, err)
	}
	return dek, nil
}

// validateCompleteParts сверяет список клиента с тем, что реально загружено.
//
// Проверяются четыре вещи, и каждая — про конкретную ошибку клиента:
// порядок (InvalidPartOrder), наличие и ETag (InvalidPart), минимальный размер
// кроме последней части (EntityTooSmall). ETag сверяется без кавычек: клиент
// шлёт их в XML, а в базе хранится чистый hex.
func validateCompleteParts(requested []types.CompletedPart, have []meta.UploadPart) ([]meta.UploadPart, error) {
	if len(requested) == 0 {
		return nil, s3err.GetAPIError(s3err.ErrMalformedXML)
	}
	byNumber := make(map[int]meta.UploadPart, len(have))
	for _, p := range have {
		byNumber[p.Number] = p
	}

	out := make([]meta.UploadPart, 0, len(requested))
	prev := 0
	for _, r := range requested {
		n := int(aws.ToInt32(r.PartNumber))
		if n < 1 || n > maxParts {
			return nil, s3err.GetAPIError(s3err.ErrInvalidPart)
		}
		if n <= prev {
			return nil, s3err.GetAPIError(s3err.ErrInvalidPartOrder)
		}
		prev = n
		h, ok := byNumber[n]
		if !ok {
			return nil, s3err.GetAPIError(s3err.ErrInvalidPart)
		}
		if !backend.AreEtagsSame(aws.ToString(r.ETag), h.ETag) {
			return nil, s3err.GetAPIError(s3err.ErrInvalidPart)
		}
		out = append(out, h)
	}

	// Минимальный размер не проверяют только последняя часть: она единственная
	// может быть произвольной длины.
	for i, p := range out {
		if i < len(out)-1 && p.Size < backend.MinPartSize {
			return nil, s3err.GetEntityTooSmallErr(p.Size, backend.MinPartSize)
		}
	}
	return out, nil
}

// lockAtComplete — Object Lock для версии, собранной из частей.
//
// Только правило бакета: в этом SDK CompleteMultipartUploadInput не несёт ни
// x-amz-object-lock-mode, ни legal hold, ни Tagging — S3 принимает их на
// CreateMultipartUpload. Явных заголовков здесь просто нет, поэтому и ветка
// «явное важнее правила» отсутствует, а не забыта.
func (t *TapeGW) lockAtComplete(ctx context.Context, tenant meta.TenantID, bucket string,
) (mode string, until *time.Time, hold bool, err error) {
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

// splitCopySource разбирает "bucket/key" из CopySource: ключ сам содержит
// слэши, поэтому режем по первому.
func splitCopySource(src string) (string, string, error) {
	bucket, key, ok := strings.Cut(src, "/")
	if !ok || bucket == "" || key == "" {
		return "", "", s3err.GetAPIError(s3err.ErrInvalidRequest)
	}
	return bucket, key, nil
}

// copyRange переводит "bytes=0-99" в (смещение, длину) открытого текста.
// Пустая строка — весь объект.
func copyRange(spec *string, size int64) (int64, int64, error) {
	if spec == nil || *spec == "" {
		return 0, size, nil
	}
	s := strings.TrimPrefix(*spec, "bytes=")
	dash := strings.IndexByte(s, '-')
	if dash < 0 {
		return 0, 0, s3err.GetAPIError(s3err.ErrInvalidRequest)
	}
	var start, end int64
	if _, err := fmt.Sscanf(s[:dash], "%d", &start); err != nil {
		return 0, 0, s3err.GetAPIError(s3err.ErrInvalidRequest)
	}
	if s[dash+1:] == "" {
		end = size - 1
	} else if _, err := fmt.Sscanf(s[dash+1:], "%d", &end); err != nil {
		return 0, 0, s3err.GetAPIError(s3err.ErrInvalidRequest)
	}
	if start >= size || end < start {
		return 0, 0, s3err.GetAPIError(s3err.ErrInvalidRange)
	}
	if end > size-1 {
		end = size - 1
	}
	return start, end - start + 1, nil
}

// removeQuietly — удалить файл и не превращать ошибку удаления в ошибку
// запроса. Данные уже на месте, а сироту подберёт janitor.
func (t *TapeGW) removeQuietly(id uuid.UUID) {
	_ = t.spool.Remove(id)
}
