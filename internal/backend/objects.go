package backend

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
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

	"github.com/vladsimachkov/tapegw/internal/encrypt"
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
	// Страж — первым делом, до разбора тела и заголовков: отказ не должен
	// зависеть от того, что именно клиент прислал дальше.
	tenant, err := tenantFromContext(ctx)
	if err != nil {
		return s3response.PutObjectOutput{}, err
	}

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

	if ok, err := t.meta.BucketExists(ctx, tenant, bucket); err != nil {
		return s3response.PutObjectOutput{}, tapegwerr.FromMeta(err, bucket)
	} else if !ok {
		// Чужой бакет — AccessDenied, а не NoSuchBucket: см. ADR-0017, п. 1.
		return s3response.PutObjectOutput{}, t.bucketErr(ctx, bucket, tenant, meta.ErrNotFound)
	}

	tags, err := vbackend.ParseObjectTags(str(po.Tagging))
	if err != nil {
		return s3response.PutObjectOutput{}, err
	}

	// Ключ шифрования версии — до записи на диск. Обёртка нужна в той же
	// транзакции, что и строка объекта: файл без обёртки прочитать нельзя, а
	// обёртка без файла — это то, что остаётся после падения между rename и
	// commit, и оно безвредно (правило 6).
	//
	// DEK берётся от тенанта бакета, а не учётки: у root ProjectID не заполнен,
	// и ключ «-1» не существует.
	ownerTenant, err := t.meta.BucketTenantID(ctx, tenant, bucket)
	if err != nil {
		return s3response.PutObjectOutput{}, t.bucketErr(ctx, bucket, tenant, err)
	}
	// UUID версии генерируется здесь, а не прямо перед записью: он входит и в
	// nonce сегментов, и в AAD обёртки DEK, и в имя файла спула — всё это должно
	// быть одним и тем же значением.
	id := uuid.New()
	dek, wrappedDEK, err := t.versionKey(ctx, ownerTenant, id)
	if err != nil {
		return s3response.PutObjectOutput{}, fmt.Errorf("ключ версии: %w", err)
	}
	// Ключ больше не нужен: писатель скопировал его себе, а обёртка уже в руках.
	defer encrypt.Zero(dek)

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
		switch cur, err := t.meta.GetObject(ctx, tenant, bucket, key); {
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

	// Объявленный размер тела обязан быть. Он входит в AAD каждого сегмента
	// (ADR-0014), поэтому шифровать можно только зная длину заранее — а без
	// привязки к длине отрезанный по границе сегмента хвост прошёл бы проверку
	// аутентичности.
	//
	// Для aws-chunked versitygw кладёт в ContentLength декодированную длину
	// (object-put.go:941, x-amz-decoded-content-length), а не длину кадра,
	// поэтому поле годится и для чанкованных загрузок.
	declared := int64(0)
	if po.ContentLength != nil {
		declared = *po.ContentLength
	}
	if declared <= 0 {
		return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrMissingContentLength)
	}

	// Object Lock при записи: явные заголовки либо правило бакета по умолчанию.
	// Считается ДО записи тела: манифест обязан описывать ровно ту блокировку,
	// которая уйдёт в строку objects, а хвост дописывается в тот же проход по
	// телу. Плюс отказ до гигабайт на диске дешевле отказа после них.
	lockMode, lockUntil, lockHold, err := t.lockAtWrite(ctx, tenant, bucket, &po)
	if err != nil {
		return s3response.PutObjectOutput{}, err
	}

	// UUID генерируем здесь, а не в базе: имя файла в спуле обязано совпасть со
	// строкой, и mover потом соберёт тот же путь по UUID из строки (ADR-0007).
	// Соль — на каждую запись версии своя, она входит в nonce сегментов.
	salt, err := newSalt()
	if err != nil {
		return s3response.PutObjectOutput{}, err
	}
	// Хвост манифеста строится внутри записи, когда MD5 от тела уже посчитан:
	// ETag обычного PUT — этот MD5, а второго прохода по телу не будет.
	trailerFor := func(info spool.ObjectInfo) ([]byte, error) {
		return t.manifestTrailer(dek, id, manifestInput{
			Bucket: bucket, Key: key, ETag: info.MD5Hex, Size: declared,
			ContentType: str(po.ContentType), Meta: po.Metadata,
			Mode: lockMode, Until: lockUntil, Hold: lockHold,
			Parts: putManifestParts(declared, salt),
		})
	}
	info, err := t.spool.WriteEncrypted(id, po.Body, spool.SumSpec{
		ContentMD5:              str(po.ContentMD5),
		Algorithm:               algo,
		Expected:                checksumExpected(&po, algo),
		SkipAlgorithmValidation: trailing,
	}, limit, dek, declared, salt, trailerFor)
	if err != nil {
		if errors.Is(err, spool.ErrTooLarge) {
			return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrEntityTooLarge)
		}
		if errors.Is(err, spool.ErrSizeMismatch) {
			// Объявили одно, пришло другое. AWS за это же отвечает
			// IncompleteBody: недосланный под видом целого объект хуже отказа.
			return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrIncompleteBody)
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

	obj, err := t.meta.PutObject(ctx, tenant, meta.PutObjectInput{
		ID:           id,
		Bucket:       bucket,
		Key:          key,
		Size:         info.Size,
		FileBytes:    info.DiskBytes,
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
		Precondition:  precondition,
		RetentionMode: lockMode,
		RetainUntil:   lockUntil,
		LegalHold:     lockHold,
		WrappedDEK:    wrappedDEK,
		Salt:          salt,
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
	// x-amz-version-id. Без него версия ничем не адресуется: ни прочитать её
	// конкретной, ни удалить, ни поставить на неё retention. Для бакета без
	// версий заголовка нет вовсе, для приостановленного — "null", как в AWS.
	switch obj.Versioning {
	case meta.VersioningEnabled:
		out.VersionID = obj.ID.String()
	case meta.VersioningSuspended:
		out.VersionID = "null"
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
	tenant, err := tenantFromContext(ctx)
	if err != nil {
		return nil, err
	}
	obj, vid, err := t.readVersion(ctx, tenant, str(input.Bucket), str(input.Key),
		input.VersionId)
	if err != nil {
		if errors.Is(err, errDeleteMarker) {
			vid, marker, merr := markerRead(vid, input.VersionId != nil && *input.VersionId != "",
				str(input.Bucket))
			out := &s3.HeadObjectOutput{DeleteMarker: &marker}
			if vid != "" {
				out.VersionId = &vid
			}
			return out, merr
		}
		if errors.Is(err, meta.ErrNoSuchVersion) {
			return nil, s3err.GetAPIError(s3err.ErrNoSuchVersion)
		}
		return nil, t.objectErr(ctx, str(input.Bucket), tenant, err)
	}

	restore, err := t.restoreHeader(ctx, tenant, obj)
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
		VersionId:    strPtr(vid),
		// Как в GET: шифрование включено всегда (ADR-0014), и HEAD обязан
		// сообщать об этом — клиент сверяет способ хранения по HEAD так же,
		// как по GET.
		ServerSideEncryption: types.ServerSideEncryptionAes256,
	}
	if mode, until, hold := lockHeaders(obj); mode != "" || hold != "" {
		out.ObjectLockMode = mode
		out.ObjectLockRetainUntilDate = until
		out.ObjectLockLegalHoldStatus = hold
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
// и попытка повторяется EvictRetries раз; если перечитанное состояние говорит
// «не онлайн» — отдаём InvalidObjectState. Если же попытки кончились, а состояние
// по-прежнему обещает диск, — это несогласованность (файл пропал у онлайн- или
// неархивированного объекта): ERROR в журнал и InternalError, а не правдоподобный
// InvalidObjectState, который скрыл бы потерю данных за штатным Glacier.
func (t *TapeGW) GetObject(ctx context.Context, input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	bucket, key := str(input.Bucket), str(input.Key)
	tenant, err := tenantFromContext(ctx)
	if err != nil {
		return nil, err
	}
	byID := input.VersionId != nil && *input.VersionId != ""
	var vid string

	for attempt := 0; ; attempt++ {
		obj, v, err := t.readVersion(ctx, tenant, bucket, key, input.VersionId)
		vid = v
		if err != nil {
			if errors.Is(err, errDeleteMarker) {
				_, _, merr := markerRead(vid, byID, bucket)
				return &s3.GetObjectOutput{DeleteMarker: ptr(true), VersionId: strPtr(vid)}, merr
			}
			if errors.Is(err, meta.ErrNoSuchVersion) {
				return nil, s3err.GetAPIError(s3err.ErrNoSuchVersion)
			}
			return nil, t.objectErr(ctx, bucket, tenant, err)
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

		// Шов для теста окна гонки (блокер B1): в бою nil. Тест выполняет в
		// крючке вытеснение и получает расклад «строку прочитали online, а
		// открытием пришли уже после unlink» без гонок по таймингам.
		if t.evictHook != nil {
			t.evictHook(obj.ID)
		}

		f, err := t.spool.Open(obj.ID)
		if errors.Is(err, os.ErrNotExist) {
			// Файл исчез между решением о читаемости и open. Перечитываем
			// состояние: если объект вытеснили, это штатный InvalidObjectState.
			if attempt < t.cfg.Spool.EvictRetries {
				continue
			}
			// Попытки кончились. Решение «штатный офлайн или несогласованность»
			// принимаем по самому свежему состоянию, а не по прочитанному до
			// неудачного open: иначе вытеснение, закоммитившееся между чтением
			// строки и открытием файла, было бы записано клиенту как 500.
			fresh, _, rerr := t.readVersion(ctx, tenant, bucket, key, input.VersionId)
			if rerr == nil && !fresh.ReadableFromDisk(time.Now()) {
				return nil, s3err.GetBucketErr(s3err.ErrInvalidObjectState, bucket)
			}
			// Состояние обещает объект на диске, а файла нет. Это не штатный
			// Glacier: restore тут не поможет — на ленте объекта ещё нет.
			// Промолчать и ответить InvalidObjectState — значит выдать потерю
			// данных за нормальное состояние, поэтому при сомнении громкая
			// ошибка: ERROR в журнал и InternalError (500) клиенту.
			slog.Error("файл landing отсутствует у онлайн-объекта",
				"bucket", bucket, "key", key, "id", obj.ID.String(),
				"state", obj.State, "reread_err", rerr, "open_err", err)
			return nil, fmt.Errorf("landing file missing for online object %s/%s (id %s): %w",
				bucket, key, obj.ID.String(), err)
		}
		if err != nil {
			return nil, fmt.Errorf("open object %s/%s: %w", bucket, key, err)
		}

		out, err := t.buildGetOutput(ctx, tenant, obj, f, str(input.Range), vid)
		if err != nil {
			f.Close()
			return nil, err
		}
		return out, nil
	}
}

// buildGetOutput собирает ответ, включая Range. Файл остаётся открытым:
// versitygw закрывает Body сам.
func (t *TapeGW) buildGetOutput(ctx context.Context, tenant meta.TenantID, obj *meta.Object, f *os.File, rng, vid string) (*s3.GetObjectOutput, error) {
	start, length, hasRange, err := vbackend.ParseObjectRange(obj.Size, rng)
	if err != nil {
		return nil, err
	}

	contentRange := (*string)(nil)
	size := obj.Size
	// Один вызов на оба случая: Range или весь объект — различие только в
	// аргументах, а открывать ключ дважды было бы лишней работой на каждом
	// Range-запросе.
	rstart, rlen := int64(0), int64(-1)
	if hasRange {
		rstart, rlen = start, length
	}
	body, err := t.plainReader(ctx, obj, f, rstart, rlen)
	if err != nil {
		return nil, err
	}
	if hasRange {
		size = length
		v := fmt.Sprintf("bytes %d-%d/%d", start, start+length-1, obj.Size)
		contentRange = &v
	}

	restore, err := t.restoreHeader(ctx, tenant, obj)
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
		VersionId:     strPtr(vid),
		// Шифрование включено всегда, поэтому заголовок ставится безусловно:
		// клиент по нему видит, что сервер шифрует сам, и не пытается передать
		// свой ключ (SSE-C у нас NotImplemented).
		ServerSideEncryption: types.ServerSideEncryptionAes256,
	}
	if mode, until, hold := lockHeaders(obj); mode != "" || hold != "" {
		out.ObjectLockMode = mode
		out.ObjectLockRetainUntilDate = until
		out.ObjectLockLegalHoldStatus = hold
	}
	if slot := getChecksumSlot(out, checksumAlgo(obj.Checksums)); slot != nil {
		v := obj.Checksums[string(checksumAlgo(obj.Checksums))]
		*slot = &v
	}
	return out, nil
}

// restoreHeader — x-amz-restore по ADR-0003. Правила поверх одной колонки
// objects.online_until:
//
//   - restore идёт (`requested`/`in_progress`) — ongoing-request="true";
//   - дисковая копия есть (landing живой или restore выдан и не истёк) —
//     ongoing-request="false", expiry-date = показанный срок;
//   - копии нет и запроса нет — заголовка нет.
//
// Показанная дата — всегда online_until (для pending/archiving — не раньше
// now()+landing_min_retention, чтобы дата «не усыхала» под глазами клиента до
// конца архивации), а не restores.expires_at: вытесняет janitor именно по
// online_until, и обещание в заголовке не может быть позже реального удаления
// файла. Фиктивная дата 2050 из scoutfs ломалась ровно так (ADR-0003).
func (t *TapeGW) restoreHeader(ctx context.Context, tenant meta.TenantID, obj *meta.Object) (*string, error) {
	r, err := t.meta.GetRestore(ctx, tenant, obj.ID)
	if err != nil && !errors.Is(err, meta.ErrNotFound) {
		return nil, fmt.Errorf("restore для %s: %w", obj.ID, err)
	}
	if r != nil && r.Ongoing() {
		v := `ongoing-request="true"`
		return &v, nil
	}
	now := time.Now()
	if obj.ReadableFromDisk(now) || (r != nil && r.Active(now)) {
		until := obj.OnlineUntil
		if obj.State == "pending" || obj.State == "archiving" {
			if floor := now.Add(t.cfg.Spool.LandingMinRetention); floor.After(until) {
				until = floor
			}
		}
		v := fmt.Sprintf(`ongoing-request="false", expiry-date=%q`,
			until.UTC().Format(http.TimeFormat))
		return &v, nil
	}
	return nil, nil
}

// str — распаковать *string из структур versitygw: там каждое поле указатель.
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
