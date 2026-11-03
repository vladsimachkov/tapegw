package backend

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	vbackend "github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3api/middlewares"
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

	// Чужой storage class отвергаем до чтения тела (ADR-0003): молча
	// проигнорировать STANDARD хуже, чем отказаться, — клиент решит, что у него
	// быстрый класс, а читать будет с ленты. Код — InvalidStorageClass (400),
	// как у AWS, а не NotImplemented: сервер-то эту операцию умеет.
	class, err := t.cfg.StorageClassAction(string(po.StorageClass))
	if err != nil {
		return s3response.PutObjectOutput{}, tapegwerr.InvalidStorageClass(string(po.StorageClass))
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

	// Условная запись (If-Match / If-None-Match). Разбирает и оценивает
	// versitygw — мы только передаём ему etag текущей строки.
	//
	// Проверка делается дважды. Здесь, до чтения тела — чтобы не писать на диск
	// гигабайты, которые сейчас откажут (так же делает posix: быстрая
	// предварительная проверка). И внутри транзакции, уже под advisory-локом —
	// это авторитетная проверка: только она атомарна относительно записи,
	// поэтому между ней и вставкой не влезет параллельный PUT того же ключа.
	precondition := func(curEtag string, exists bool) error {
		return vbackend.EvaluateObjectPutPreconditions(curEtag, po.IfMatch, po.IfNoneMatch, exists)
	}
	if po.IfMatch != nil || po.IfNoneMatch != nil {
		switch cur, err := t.meta.GetObject(ctx, bucket, key); {
		case err == nil:
			if err := precondition(cur.ETag, true); err != nil {
				return s3response.PutObjectOutput{}, err
			}
		case errors.Is(err, meta.ErrNotFound):
			if err := precondition("", false); err != nil {
				return s3response.PutObjectOutput{}, err
			}
		default:
			return s3response.PutObjectOutput{}, tapegwerr.FromMeta(err, bucket)
		}
	}

	// Trailing-суммы (aws-chunked). Современный AWS SDK и aws cli по умолчанию
	// шлют CRC32/CRC64NVME не в заголовке, а в трейлере: в заголовке остаётся
	// только x-amz-sdk-checksum-algorithm, объявляющий алгоритм.
	//
	// Разбирает и проверяет трейлер сам versitygw: po.Body в этом случае —
	// middlewares.ChecksumReader, который по завершении чтения отдаёт
	// Algorithm() и Checksum(). Логику повторяем за posix (правило 5): если
	// алгоритм пришёл из трейлера, свою сверку пропускаем (чанк-ридер уже
	// сверил значение из трейлера с байтами), а итоговую сумму берём у него же.
	chRdr, chunkUpload := po.Body.(middlewares.ChecksumReader)
	trailing := chunkUpload && chRdr.Algorithm() != ""

	algo := po.ChecksumAlgorithm
	if trailing {
		algo = types.ChecksumAlgorithm(strings.ToUpper(chRdr.Algorithm()))
	}

	// UUID генерируем здесь, а не в базе: имя файла в спуле обязано совпасть со
	// строкой, и mover потом соберёт тот же путь по UUID из строки (ADR-0007).
	id := uuid.New()
	info, err := t.spool.Write(id, po.Body, spool.SumSpec{
		ContentMD5:              str(po.ContentMD5),
		Algorithm:               algo,
		Expected:                checksumExpected(&po, algo),
		SkipAlgorithmValidation: trailing,
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

	// На всякий случай сверяем свою сумму со значением из трейлера. Обычно это
	// самопроверка (чанк-ридер versitygw уже сверил трейлер с байтами), но она
	// ловит расхождение реализаций и неполное чтение тела.
	if trailing && chRdr.Checksum() != "" && chRdr.Checksum() != info.Checksum {
		if rmErr := t.spool.Remove(id); rmErr != nil {
			return s3response.PutObjectOutput{}, fmt.Errorf("spool remove %s: %w", id, rmErr)
		}
		return s3response.PutObjectOutput{}, s3err.GetChecksumBadDigestErr(algo)
	}

	_, err = t.meta.PutObject(ctx, meta.PutObjectInput{
		ID:           id,
		Bucket:       bucket,
		Key:          key,
		Size:         info.Size,
		ETag:         info.MD5Hex,
		Checksums:    checksumMap(algo, info.Checksum),
		ContentType:  po.ContentType,
		UserMeta:     po.Metadata,
		Tags:         tags,
		StorageClass: class,
		// Обещание клиенту: копия жива минимум landing_min_retention. Именно это
		// значение покажет x-amz-restore, и раньше срока вытеснять нельзя (ADR-0003).
		OnlineUntil: time.Now().Add(t.cfg.Spool.LandingMinRetention),
		// Авторитетная проверка условий — внутри транзакции, под локом ключа.
		Precondition: precondition,
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
	if slot := putChecksumSlot(&out, algo); slot != nil {
		v := info.Checksum
		*slot = &v
	}
	return out, nil
}

// HeadObject отдаёт метаданные и НИКОГДА не ошибается из-за того, что объект
// лежит на ленте.
//
// В AWS HEAD на Glacier-объект всегда 200: StorageClass GLACIER плюс
// x-amz-restore, а InvalidObjectState отвечает только GET. Это не мелочь:
// именно по HEAD клиенты опрашивают ongoing-request после RestoreObject, и
// если HEAD ошибётся, объект «в процессе восстановления» будет выглядеть для
// клиента ошибкой. В архитектуре то же правило: HEAD и LIST работают только по
// meta DB, ленту не трогают.
func (t *TapeGW) HeadObject(ctx context.Context, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	if err := checkVersionID(input.VersionId); err != nil {
		return nil, err
	}
	obj, err := t.meta.GetObject(ctx, str(input.Bucket), str(input.Key))
	if err != nil {
		return nil, objectErr(err, str(input.Bucket), str(input.Key))
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
	if err := checkVersionID(input.VersionId); err != nil {
		return nil, err
	}

	for attempt := 0; ; attempt++ {
		obj, err := t.meta.GetObject(ctx, bucket, key)
		if err != nil {
			return nil, objectErr(err, bucket, key)
		}
		if !obj.ReadableFromDisk(time.Now()) {
			return nil, s3err.GetBucketErr(s3err.ErrInvalidObjectState, bucket)
		}

		// Условные заголовки чтения. Контроллер versitygw их не вычисляет — он
		// разбирает и кладёт в input, а решает бэкенд (так же устроено в posix:
		// backend.EvaluatePreconditions). Оценку отдаём тому же хелперу: он
		// возвращает ErrNotModified (304) и PreconditionFailed (412), а
		// контроллер раскладывает их по HTTP-коду.
		//
		// Порядок: сначала «объект не читается с диска», потом условия. Иначе
		// условный GET офлайнового объекта отдал бы 304 вместо честного
		// InvalidObjectState.
		if err := vbackend.EvaluatePreconditions(obj.ETag, obj.CreatedAt, vbackend.PreConditions{
			IfMatch:       input.IfMatch,
			IfNoneMatch:   input.IfNoneMatch,
			IfModSince:    input.IfModifiedSince,
			IfUnmodeSince: input.IfUnmodifiedSince,
		}); err != nil {
			return nil, err
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

// checkVersionID — версионирования в tapegw нет (вне MVP). Пустой versionId и
// литеральный "null" — это и есть единственная версия, как в AWS для бакета без
// versioning. Любой другой значение молча проигнорировать значит отдать текущую
// версию вместо запрошенной: клиент, который ищет конкретную версию, получил бы
// другие данные и не узнал бы об этом.
func checkVersionID(v *string) error {
	if v == nil || *v == "" || *v == "null" {
		return nil
	}
	return s3err.GetAPIError(s3err.ErrVersioningNotConfigured)
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
