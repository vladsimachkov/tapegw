package backend

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	vbackend "github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"

	"github.com/vladsimachkov/tapegw/internal/meta"
	tapegwerr "github.com/vladsimachkov/tapegw/internal/s3err"
	"github.com/vladsimachkov/tapegw/internal/spool"
)

// PutObject — единственный путь появления объекта.
//
// Порядок продиктован правилом 6 из AGENTS.md и менять его нельзя:
//
//  1. тело пишется в спул и fsync-ится;
//  2. только потом открывается транзакция Postgres и коммитится строка.
//
// Падение между шагами оставляет файл без строки — сироту. Он безвреден: читаем
// мы по строке БД, значит прочитан сирота не будет, а janitor подчистит.
// Обратный порядок был бы хуже во всех смыслах: строка без файла даёт 500 на
// каждом GET и HeadObject.
func (t *TapeGW) PutObject(ctx context.Context, po s3response.PutObjectInput) (s3response.PutObjectOutput, error) {
	// Шифрование клиентским ключом мы не делаем: ключ пришлось бы хранить вместе
	// с данными, а это уже не «прозрачная лента». Отказ явный, по правилу 4.
	if vbackend.HasSSEC(po.SSECustomerAlgorithm, po.SSECustomerKey, po.SSECustomerKeyMD5) {
		return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrNotImplemented)
	}

	bucket, key := str(po.Bucket), str(po.Key)
	if bucket == "" || key == "" {
		return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}

	// Чужой storage class отвергаем до чтения тела (ADR-0003): молча проигнорировать
	// STANDARD хуже, чем отказаться, — клиент решит, что у него быстрый класс,
	// а читать будет с ленты.
	class, err := t.cfg.StorageClassAction(string(po.StorageClass))
	if err != nil {
		return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrNotImplemented)
	}

	// Быстрый отказ по заголовку: тело при EntityTooLarge вообще не читаем.
	// Точную границу держит и спул — заголовок может врать.
	limit := int64(t.cfg.S3.MaxObjectSize)
	if po.ContentLength != nil && *po.ContentLength > limit {
		return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrEntityTooLarge)
	}

	if ok, err := t.meta.BucketExists(ctx, bucket); err != nil {
		return s3response.PutObjectOutput{}, tapegwerr.FromMeta(err, bucket)
	} else if !ok {
		return s3response.PutObjectOutput{}, s3err.GetBucketErr(s3err.ErrNoSuchBucket, bucket)
	}

	tags, err := vbackend.ParseObjectTags(str(po.Tagging))
	if err != nil {
		return s3response.PutObjectOutput{}, err
	}

	// UUID генерируем здесь, а не в базе: имя файла в спуле обязано совпасть со
	// строкой, и mover потом соберёт тот же путь по UUID из строки (ADR-0007).
	id := uuid.New()
	info, err := t.spool.Write(id, po.Body, spool.SumSpec{
		ContentMD5: str(po.ContentMD5),
		Algorithm:  po.ChecksumAlgorithm,
		Expected:   checksumExpected(&po, po.ChecksumAlgorithm),
	}, limit)
	if err != nil {
		if errors.Is(err, spool.ErrTooLarge) {
			return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrEntityTooLarge)
		}
		// Ошибку сверки контрольной суммы HashReader уже вернул типизированной
		// ошибкой versitygw (BadDigest и родственные) — оборачивать её в
		// «внутреннюю» нельзя, клиент потерял бы 400 и правильный код.
		var s3e s3err.S3Error
		if errors.As(err, &s3e) {
			return s3response.PutObjectOutput{}, err
		}
		return s3response.PutObjectOutput{}, fmt.Errorf("spool write: %w", err)
	}

	_, err = t.meta.PutObject(ctx, meta.PutObjectInput{
		ID:           id,
		Bucket:       bucket,
		Key:          key,
		Size:         info.Size,
		ETag:         info.MD5Hex,
		Checksums:    checksumMap(po.ChecksumAlgorithm, info.Checksum),
		ContentType:  po.ContentType,
		UserMeta:     po.Metadata,
		Tags:         tags,
		StorageClass: class,
		// Обещание клиенту: копия жива минимум landing_min_retention. Именно это
		// значение покажет x-amz-restore, и раньше срока вытеснять нельзя (ADR-0003).
		OnlineUntil: time.Now().Add(t.cfg.Spool.LandingMinRetention),
	})
	if err != nil {
		// Файл уже на диске, а строки нет. Убираем сразу: janitor разберёт
		// оставшиеся сироты медленнее, а таких после каждой ошибки клиента
		// накапливались бы гигабайты.
		if rmErr := t.spool.Remove(id); rmErr != nil {
			return s3response.PutObjectOutput{}, fmt.Errorf("%w (и спул-файл %s не удалён: %v)", err, id, rmErr)
		}
		return s3response.PutObjectOutput{}, tapegwerr.FromMeta(err, bucket)
	}

	out := s3response.PutObjectOutput{
		// ETag наружу — в кавычках, как делает versitygw (backend.GenerateEtag).
		// В базе храним чистый hex: он нужен для сравнений, а AreEtagsSame в
		// versitygw кавычки всё равно срезает.
		ETag: quoteEtag(info.MD5Hex),
		Size: &info.Size,
	}
	if slot := putChecksumSlot(&out, po.ChecksumAlgorithm); slot != nil {
		v := info.Checksum
		*slot = &v
	}
	return out, nil
}

// HeadObject отдаёт метаданные. Нечитаемый объект — InvalidObjectState:
// существование ключа не стоит скрывать, но и врать про доступность нельзя.
func (t *TapeGW) HeadObject(ctx context.Context, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	obj, err := t.meta.GetObject(ctx, str(input.Bucket), str(input.Key))
	if err != nil {
		return nil, objectErr(err, str(input.Bucket), str(input.Key))
	}
	if !obj.ReadableFromDisk(time.Now()) {
		return nil, s3err.GetBucketErr(s3err.ErrInvalidObjectState, str(input.Bucket))
	}

	restore, err := t.restoreHeader(ctx, obj)
	if err != nil {
		return nil, err
	}

	out := &s3.HeadObjectOutput{
		ContentLength: &obj.Size,
		ContentType:   obj.ContentType,
		ETag:          ptr(quoteEtag(obj.ETag)),
		LastModified:  &obj.CreatedAt,
		Metadata:      obj.UserMeta,
		Restore:       restore,
		AcceptRanges:  ptr("bytes"),
		// Всегда GLACIER, независимо от того, что клиент прислал при записи:
		// у нас другой класс был бы ложью. См. ADR-0003.
		StorageClass: types.StorageClassGlacier,
	}
	if slot := headChecksumSlot(out, checksumAlgo(obj.Checksums)); slot != nil {
		v := obj.Checksums[string(checksumAlgo(obj.Checksums))]
		*slot = &v
	}
	return out, nil
}

// GetObject читает содержимое с диска.
//
// Гонка с вытеснением решается по ADR-0004: мы могли решить, что объект онлайн,
// а спул уже снял флаг и удаляет файл. Поэтому при ENOENT состояние перечитывается
// и попытка повторяется EvictRetries раз; если объект стал нечитаем — отдаём
// InvalidObjectState, а не 500.
func (t *TapeGW) GetObject(ctx context.Context, input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	bucket, key := str(input.Bucket), str(input.Key)

	for attempt := 0; ; attempt++ {
		obj, err := t.meta.GetObject(ctx, bucket, key)
		if err != nil {
			return nil, objectErr(err, bucket, key)
		}
		if !obj.ReadableFromDisk(time.Now()) {
			return nil, s3err.GetBucketErr(s3err.ErrInvalidObjectState, bucket)
		}

		f, err := t.spool.Open(obj.ID)
		if errors.Is(err, os.ErrNotExist) {
			// Файл исчез между решением о читаемости и open. Перечитываем
			// состояние: если объект вытеснили, это штатный InvalidObjectState.
			if attempt >= t.cfg.Spool.EvictRetries {
				return nil, s3err.GetBucketErr(s3err.ErrInvalidObjectState, bucket)
			}
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("open object %s/%s: %w", bucket, key, err)
		}

		out, err := t.buildGetOutput(ctx, obj, f, str(input.Range))
		if err != nil {
			f.Close()
			return nil, err
		}
		return out, nil
	}
}

// buildGetOutput собирает ответ, включая Range. Файл остаётся открытым:
// versitygw закрывает Body сам.
func (t *TapeGW) buildGetOutput(ctx context.Context, obj *meta.Object, f *os.File, rng string) (*s3.GetObjectOutput, error) {
	start, length, hasRange, err := vbackend.ParseObjectRange(obj.Size, rng)
	if err != nil {
		return nil, err
	}

	var body io.ReadCloser = f
	contentRange := (*string)(nil)
	size := obj.Size
	if hasRange {
		if _, err := f.Seek(start, io.SeekStart); err != nil {
			return nil, fmt.Errorf("seek %s: %w", obj.ID, err)
		}
		body = &rangeReader{r: io.LimitReader(f, length), c: f}
		size = length
		v := fmt.Sprintf("bytes %d-%d/%d", start, start+length-1, obj.Size)
		contentRange = &v
	}

	restore, err := t.restoreHeader(ctx, obj)
	if err != nil {
		return nil, err
	}

	out := &s3.GetObjectOutput{
		ContentLength: &size,
		ContentType:   obj.ContentType,
		ETag:          ptr(quoteEtag(obj.ETag)),
		LastModified:  &obj.CreatedAt,
		Metadata:      obj.UserMeta,
		Restore:       restore,
		AcceptRanges:  ptr("bytes"),
		ContentRange:  contentRange,
		Body:          body,
		StorageClass:  types.StorageClassGlacier,
	}
	if slot := getChecksumSlot(out, checksumAlgo(obj.Checksums)); slot != nil {
		v := obj.Checksums[string(checksumAlgo(obj.Checksums))]
		*slot = &v
	}
	return out, nil
}

// restoreHeader — x-amz-restore по ADR-0003. nil, если запроса восстановления
// не было: для онлайнового объекта заголовок не нужен, а фиктивная дата
// (как делал scoutfs через 2050 год) обещала бы доступность, которой нет.
func (t *TapeGW) restoreHeader(ctx context.Context, obj *meta.Object) (*string, error) {
	r, err := t.meta.GetRestore(ctx, obj.ID)
	if errors.Is(err, meta.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("restore для %s: %w", obj.ID, err)
	}
	return r.HeaderValue(), nil
}

// rangeReader закрывает файл вместе с ограниченным читателем: иначе дескриптор
// утечёт на каждом Range-запросе.
type rangeReader struct {
	r io.Reader
	c io.Closer
}

func (r *rangeReader) Read(p []byte) (int, error) { return r.r.Read(p) }
func (r *rangeReader) Close() error               { return r.c.Close() }

// objectErr различает «нет такого ключа» и прочие ошибки. Для чужого бакета
// NoSuchKey был бы неправдой, поэтому бакет подставляется в GetBucketErr.
func objectErr(err error, bucket, key string) error {
	if errors.Is(err, meta.ErrNotFound) {
		return s3err.GetBucketErr(s3err.ErrNoSuchKey, bucket)
	}
	return tapegwerr.FromMeta(err, bucket)
}

func str(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// quoteEtag оборачивает hex-ETag в кавычки — формат, который отдаёт AWS и
// который собирает versitygw (backend.GenerateEtag).
func quoteEtag(hex string) string { return `"` + hex + `"` }

func ptr[T any](v T) *T { return &v }

// checksumExpected достаёт из запроса значение контрольной суммы для алгоритма.
func checksumExpected(po *s3response.PutObjectInput, algo types.ChecksumAlgorithm) string {
	switch algo {
	case types.ChecksumAlgorithmCrc32:
		return str(po.ChecksumCRC32)
	case types.ChecksumAlgorithmCrc32c:
		return str(po.ChecksumCRC32C)
	case types.ChecksumAlgorithmMd5:
		return str(po.ChecksumMD5)
	case types.ChecksumAlgorithmSha1:
		return str(po.ChecksumSHA1)
	case types.ChecksumAlgorithmSha256:
		return str(po.ChecksumSHA256)
	case types.ChecksumAlgorithmSha512:
		return str(po.ChecksumSHA512)
	case types.ChecksumAlgorithmCrc64nvme:
		return str(po.ChecksumCRC64NVME)
	case types.ChecksumAlgorithmXxhash64:
		return str(po.ChecksumXXHASH64)
	case types.ChecksumAlgorithmXxhash3:
		return str(po.ChecksumXXHASH3)
	case types.ChecksumAlgorithmXxhash128:
		return str(po.ChecksumXXHASH128)
	default:
		return ""
	}
}

// checksumMap хранит сумму под именем алгоритма: колодка checksums jsonb одна
// на все алгоритмы, а алгоритм в запросе может быть только один.
func checksumMap(algo types.ChecksumAlgorithm, val string) map[string]string {
	if algo == "" || val == "" {
		return nil
	}
	return map[string]string{string(algo): val}
}

// checksumAlgo возвращает единственный сохранённый алгоритм или "".
func checksumAlgo(m map[string]string) types.ChecksumAlgorithm {
	for k := range m {
		return types.ChecksumAlgorithm(k)
	}
	return ""
}

// Слоты под поля контрольной суммы. Имена полей в трёх выходных структурах
// совпадают по типу, но структуры разные, поэтому обобщение дало бы рефлекс —
// дешевле и нагляднее перечислить.

func putChecksumSlot(o *s3response.PutObjectOutput, algo types.ChecksumAlgorithm) **string {
	switch algo {
	case types.ChecksumAlgorithmCrc32:
		return &o.ChecksumCRC32
	case types.ChecksumAlgorithmCrc32c:
		return &o.ChecksumCRC32C
	case types.ChecksumAlgorithmMd5:
		return &o.ChecksumMD5
	case types.ChecksumAlgorithmSha1:
		return &o.ChecksumSHA1
	case types.ChecksumAlgorithmSha256:
		return &o.ChecksumSHA256
	case types.ChecksumAlgorithmSha512:
		return &o.ChecksumSHA512
	case types.ChecksumAlgorithmCrc64nvme:
		return &o.ChecksumCRC64NVME
	case types.ChecksumAlgorithmXxhash64:
		return &o.ChecksumXXHASH64
	case types.ChecksumAlgorithmXxhash3:
		return &o.ChecksumXXHASH3
	case types.ChecksumAlgorithmXxhash128:
		return &o.ChecksumXXHASH128
	default:
		return nil
	}
}

func headChecksumSlot(o *s3.HeadObjectOutput, algo types.ChecksumAlgorithm) **string {
	switch algo {
	case types.ChecksumAlgorithmCrc32:
		return &o.ChecksumCRC32
	case types.ChecksumAlgorithmCrc32c:
		return &o.ChecksumCRC32C
	case types.ChecksumAlgorithmMd5:
		return &o.ChecksumMD5
	case types.ChecksumAlgorithmSha1:
		return &o.ChecksumSHA1
	case types.ChecksumAlgorithmSha256:
		return &o.ChecksumSHA256
	case types.ChecksumAlgorithmSha512:
		return &o.ChecksumSHA512
	case types.ChecksumAlgorithmCrc64nvme:
		return &o.ChecksumCRC64NVME
	case types.ChecksumAlgorithmXxhash64:
		return &o.ChecksumXXHASH64
	case types.ChecksumAlgorithmXxhash3:
		return &o.ChecksumXXHASH3
	case types.ChecksumAlgorithmXxhash128:
		return &o.ChecksumXXHASH128
	default:
		return nil
	}
}

func getChecksumSlot(o *s3.GetObjectOutput, algo types.ChecksumAlgorithm) **string {
	switch algo {
	case types.ChecksumAlgorithmCrc32:
		return &o.ChecksumCRC32
	case types.ChecksumAlgorithmCrc32c:
		return &o.ChecksumCRC32C
	case types.ChecksumAlgorithmMd5:
		return &o.ChecksumMD5
	case types.ChecksumAlgorithmSha1:
		return &o.ChecksumSHA1
	case types.ChecksumAlgorithmSha256:
		return &o.ChecksumSHA256
	case types.ChecksumAlgorithmSha512:
		return &o.ChecksumSHA512
	case types.ChecksumAlgorithmCrc64nvme:
		return &o.ChecksumCRC64NVME
	case types.ChecksumAlgorithmXxhash64:
		return &o.ChecksumXXHASH64
	case types.ChecksumAlgorithmXxhash3:
		return &o.ChecksumXXHASH3
	case types.ChecksumAlgorithmXxhash128:
		return &o.ChecksumXXHASH128
	default:
		return nil
	}
}
