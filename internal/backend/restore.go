package backend

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/valyala/fasthttp"
	"github.com/versity/versitygw/s3err"

	"github.com/vladsimachkov/tapegw/internal/meta"
	tapegwerr "github.com/vladsimachkov/tapegw/internal/s3err"
)

// RestoreObject — запрос восстановления архивной копии (S4).
//
// Успех — всегда 200 (ADR-0001: бэкенд не может задать код успеха, а 202 из
// AWS недостижим без патча versitygw). Различие «новый запрос / продление»
// клиент видит по x-amz-restore в следующем HEAD.
//
// Версионирование: адрес версии — тот же путь readVersion, что у GET и HEAD
// (включая versionId=null). Контроллер versitygw v1.10.0 не переносит query
// versionId в input, поэтому бэкенд дочитывает его из строки запроса сам
// (restoreVersionID) — иначе restore нетекущей версии молча восстанавливал
// бы текущую, а для бакетов с версионированием это основной сценарий.

// Границы срока восстановления — протокольный предел AWS Glacier (1..365
// дней), а не настройка развертывания, поэтому константы, а не конфиг
// (правило 9 про конфиг — про таймауты и батчи).
const (
	restoreDaysMin = 1
	restoreDaysMax = 365
)

func (t *TapeGW) RestoreObject(ctx context.Context, input *s3.RestoreObjectInput) error {
	bucket, key := str(input.Bucket), str(input.Key)
	tenant, err := tenantFromContext(ctx)
	if err != nil {
		return err
	}

	// Контроллер versitygw не переносит versionId в input (проверено по
	// s3api/controllers/object-post.go v1.10.0), поэтому читаем запрос напрямую
	// — тот же приём, что с заголовком обхода governance в DeleteObject (#35).
	// Без этого restore нетекущей версии отвечал бы 200 и молча ставил в
	// очередь текущую версию: клиент ждал бы не те данные.
	versionID := restoreVersionID(ctx, input)

	// Страж раньше всего остального: чужой объект — AccessDenied, и ни кодом
	// состояния, ни валидацией тела этого не перекрыть (ADR-0017).
	byID := versionID != nil && *versionID != ""
	obj, vid, err := t.readVersion(ctx, tenant, bucket, key, versionID)
	if err != nil {
		if errors.Is(err, errDeleteMarker) {
			_, _, merr := markerRead(vid, byID, bucket)
			return merr
		}
		if errors.Is(err, meta.ErrNoSuchVersion) {
			return s3err.GetAPIError(s3err.ErrNoSuchVersion)
		}
		return t.objectErr(ctx, bucket, tenant, err)
	}

	days, tier, err := parseRestoreRequest(input.RestoreRequest)
	if err != nil {
		return err
	}

	// «Не архивный» — как AWS для не-Glacier объекта (ARCHITECTURE, раздел
	// «RestoreObject»): у pending/archiving данных на ленте ещё нет,
	// восстанавливать нечего, и очередь recaller'а для них — ложная работа.
	if obj.State != "archived" {
		return s3err.GetBucketErr(s3err.ErrInvalidObjectState, bucket)
	}

	outcome, err := t.meta.RequestRestore(ctx, tenant, obj.ID, days, tier)
	switch {
	case err != nil:
		return t.objectErr(ctx, bucket, tenant, err)
	case outcome == meta.RestoreInProgress:
		return tapegwerr.RestoreAlreadyInProgress(bucket)
	case outcome == meta.RestoreNotArchived:
		// Состояние успело уехать между чтением бэкенда и транзакцией meta
		// (например, DELETE параллельно) — отвечаем тем же кодом, что и по
		// прочитанной строке.
		return s3err.GetBucketErr(s3err.ErrInvalidObjectState, bucket)
	}
	return nil
}

// restoreVersionID — версия из запроса: сначала input.VersionId (на случай,
// когда versitygw добавит проброс), затем query versionId из исходного
// запроса, иначе nil — текущая версия. Приём с raw-запросом — тот же, что в
// bypassGovernanceRequested (delete.go, #35): бэкенду versitygw передаёт
// контекстом именно *fasthttp.RequestCtx.
func restoreVersionID(ctx context.Context, input *s3.RestoreObjectInput) *string {
	if input.VersionId != nil && *input.VersionId != "" {
		return input.VersionId
	}
	rc, ok := ctx.(*fasthttp.RequestCtx)
	if !ok {
		// Не HTTP-контекст (прямой вызов бэкенда): query читать негде,
		// остаётся input — то есть текущая версия.
		return nil
	}
	if v := string(rc.Request.URI().QueryArgs().Peek("versionId")); v != "" {
		return &v
	}
	return nil
}

// parseRestoreRequest разбирает тело RestoreRequest. Tier по умолчанию —
// Standard (как в AWS при отсутствии GlacierJobParameters).
func parseRestoreRequest(rr *types.RestoreRequest) (days int, tier string, err error) {
	if rr == nil {
		// До контроллера versitygw пустое тело не доходит (MalformedXML там),
		// nil — только при прямом вызове бэкенда.
		return 0, "", s3err.GetAPIError(s3err.ErrMalformedXML)
	}
	if rr.SelectParameters != nil || rr.OutputLocation != nil {
		// S3 Select и выгрузка restore в другой бакет вне MVP (правило 4:
		// неподдерживаемое выражаем явно).
		return 0, "", s3err.GetAPIError(s3err.ErrNotImplemented)
	}
	if rr.Days == nil {
		return 0, "", s3err.GetAPIError(s3err.ErrInvalidRequest)
	}
	days = int(*rr.Days)
	if days < restoreDaysMin || days > restoreDaysMax {
		return 0, "", s3err.GetAPIError(s3err.ErrInvalidRequest)
	}
	tier = string(types.TierStandard)
	if rr.GlacierJobParameters != nil && rr.GlacierJobParameters.Tier != "" {
		tier = string(rr.GlacierJobParameters.Tier)
	}
	switch tier {
	case string(types.TierExpedited), string(types.TierStandard), string(types.TierBulk):
	default:
		return 0, "", s3err.GetAPIError(s3err.ErrInvalidRequest)
	}
	return days, tier, nil
}
