package backend

import (
	"context"
	"errors"

	"github.com/versity/versitygw/s3err"
	"github.com/vladsimachkov/tapegw/internal/meta"
)

// errDeleteMarker — найденная версия является маркером удаления. Сигнал
// внутренний: вызывающий превращает его в 404 (чтение без versionId) или в 405
// (чтение по id самого маркера).
var errDeleteMarker = errors.New("backend: version is a delete marker")

// versionIDHeader — что положить в x-amz-version-id.
//
// Для бакета без версий заголовка нет вовсе; для приостановленного — "null";
// для включённого — id версии, а "null", если версия записана без версионирования
// (в AWS такие версии тоже живут с versionId = "null" и после включения).
func versionIDHeader(state string, obj *meta.Object) string {
	switch state {
	case meta.VersioningEnabled:
		if obj.IsNullVersion {
			return "null"
		}
		return obj.ID.String()
	case meta.VersioningSuspended:
		return "null"
	}
	return ""
}

// readVersion находит версию, которую просят GET или HEAD.
//
// Различия AWS, реализованные здесь:
//   - без versionId читаем текущую версию; если это маркер, ключ выглядит
//     удалённым, и ответ — 404 с заголовками маркера;
//   - versionId=null — это null-версия, а не «любая текущая». Раньше null
//     просто игнорировался, и клиент получал другие данные молча;
//   - GET по id самого маркера — 405: тела у маркера нет;
//   - несуществующая версия — NoSuchVersion, а не текущая версия.
//
// Ошибки meta возвращаются как есть: их мапит вызывающий, потому что у GET и
// HEAD разные способы превращать «не найдено» в AccessDenied для чужого бакета.
func (t *TapeGW) readVersion(ctx context.Context, tenant meta.TenantID,
	bucket, key string, versionID *string) (*meta.Object, string, error) {

	state, err := t.meta.BucketVersioningState(ctx, tenant, bucket)
	if err != nil {
		return nil, "", err
	}

	var obj *meta.Object
	switch {
	case versionID == nil || *versionID == "":
		obj, err = t.meta.GetObject(ctx, tenant, bucket, key)
	default:
		obj, err = t.meta.LookupVersion(ctx, tenant, bucket, key, *versionID)
	}
	if err != nil {
		return nil, "", err
	}

	vid := versionIDHeader(state, obj)
	if obj.IsDeleteMarker {
		return obj, vid, errDeleteMarker
	}
	return obj, vid, nil
}

// markerRead — ответ на попытку прочитать маркер. Заголовки кладём в
// непустой ответ: контроллер versitygw применяет заголовки ответа и на
// ошибочном пути, поэтому 404/405 приходят вместе с x-amz-delete-marker и
// x-amz-version-id.
func markerRead(vid string, byID bool, bucket string) (versionID string, deleteMarker bool, err error) {
	if byID {
		return vid, true, s3err.GetAPIError(s3err.ErrMethodNotAllowed)
	}
	return vid, true, s3err.GetBucketErr(s3err.ErrNoSuchKey, bucket)
}
