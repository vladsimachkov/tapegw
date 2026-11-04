package backend_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/versity/versitygw/s3err"

	"github.com/vladsimachkov/tapegw/internal/meta"
)

// GetBucketVersioning обязан различать три состояния: для бакета без версий
// Status пустой, у приостановленного — Suspended. Молча вернуть «пусто» для
// приостановленного значит сказать клиенту, что версий нет.
func TestGetBucketVersioningStates(t *testing.T) {
	for _, tc := range []struct {
		state string
		want  *types.BucketVersioningStatus
	}{
		{meta.VersioningUnversioned, nil},
		{meta.VersioningEnabled, ptr(types.BucketVersioningStatusEnabled)},
		{meta.VersioningSuspended, ptr(types.BucketVersioningStatusSuspended)},
	} {
		t.Run(tc.state, func(t *testing.T) {
			be, m, _ := newBackend(t)
			ctx := testCtx()
			mustBucket(t, be)
			setVersioningDB(t, m, "b", tc.state)

			out, err := be.GetBucketVersioning(ctx, "b")
			if err != nil {
				t.Fatalf("GetBucketVersioning: %v", err)
			}
			if tc.want == nil && out.Status != nil {
				t.Errorf("unversioned: Status = %v, хотели пустой", *out.Status)
			}
			if tc.want != nil && (out.Status == nil || *out.Status != *tc.want) {
				t.Errorf("Status = %v, хотели %v", out.Status, *tc.want)
			}
		})
	}
}

// Переходы через API: приостановить бакет с Object Lock нельзя, вернуться в
// unversioned нельзя в принципе.
func TestPutBucketVersioningThroughBackend(t *testing.T) {
	be, _, _ := newBackend(t)
	ctx := testCtx()
	mustBucket(t, be)

	if err := be.PutBucketVersioning(ctx, "b", types.BucketVersioningStatusSuspended); !isS3Code(err, s3err.ErrInvalidRequest) {
		t.Errorf("приостановка не включённого бакета: %v, хотели InvalidRequest", err)
	}
	if err := be.PutBucketVersioning(ctx, "b", types.BucketVersioningStatusEnabled); err != nil {
		t.Fatalf("включить версии: %v", err)
	}
	// Лок поверх выключенных версий невозможен, поэтому включаем его сейчас.
	if err := be.PutObjectLockConfiguration(ctx, "b", []byte(`{"Enabled":true}`)); err != nil {
		t.Fatalf("включить лок: %v", err)
	}
	if err := be.PutBucketVersioning(ctx, "b", types.BucketVersioningStatusSuspended); !isS3Code(err, s3err.ErrSuspendedVersioningNotAllowed) {
		t.Errorf("приостановка бакета с лока: %v, хотели InvalidBucketState", err)
	}
	// Несуществующий бакет — NoSuchBucket, а не «не удалось применить».
	if err := be.PutBucketVersioning(ctx, "нет-такого", types.BucketVersioningStatusEnabled); !isS3Code(err, s3err.ErrNoSuchBucket) {
		t.Errorf("чужого бакета нет: %v, хотели NoSuchBucket", err)
	}
}

// Коды контракта Object Lock. Это то место, где заглушка тихо отключает
// неизменяемость: «настройки нет» должно звучать только тогда, когда настройки
// действительно нет.
func TestObjectLockConfigurationCodes(t *testing.T) {
	be, _, _ := newBackend(t)
	ctx := testCtx()
	mustBucket(t, be)

	// Бакета нет — NoSuchBucket, а не «конфигурации нет». Иначе versitygw в
	// loadObjectLockState пошёл бы дальше по пути «настройки нет».
	if _, err := be.GetObjectLockConfiguration(ctx, "нет-такого"); !isS3Code(err, s3err.ErrNoSuchBucket) {
		t.Errorf("несуществующий бакет: %v, хотели NoSuchBucket", err)
	}

	// Бакет есть, лока нет — ObjectLockConfigurationNotFound.
	if _, err := be.GetObjectLockConfiguration(ctx, "b"); !isS3Code(err, s3err.ErrObjectLockConfigurationNotFound) {
		t.Errorf("без лока: %v, хотели ObjectLockConfigurationNotFound", err)
	}

	// Лок без включённых версий — InvalidBucketState.
	if err := be.PutObjectLockConfiguration(ctx, "b", []byte(`{"Enabled":true}`)); !isS3Code(err, s3err.ErrObjectLockConfigurationNotAllowed) {
		t.Errorf("лок без версий: %v, хотели InvalidBucketState", err)
	}

	if err := be.PutBucketVersioning(ctx, "b", types.BucketVersioningStatusEnabled); err != nil {
		t.Fatalf("включить версии: %v", err)
	}
	cfg := []byte(`{"Enabled":true,"DefaultRetention":{"Mode":"GOVERNANCE","Days":7}}`)
	if err := be.PutObjectLockConfiguration(ctx, "b", cfg); err != nil {
		t.Fatalf("включить лок: %v", err)
	}
	got, err := be.GetObjectLockConfiguration(ctx, "b")
	if err != nil {
		t.Fatalf("чтение конфигурации: %v", err)
	}
	if string(got) != string(cfg) {
		t.Errorf("конфигурация исказилась:\n  получено %s\n  записано  %s", got, cfg)
	}

	// Выключить нельзя.
	if err := be.PutObjectLockConfiguration(ctx, "b", []byte(`{"Enabled":false}`)); !isS3Code(err, s3err.ErrObjectLockConfigurationNotAllowed) {
		t.Errorf("выключение лока: %v, хотели InvalidBucketState", err)
	}

	// Битый JSON — MalformedXML, а не «включили наугад».
	if err := be.PutObjectLockConfiguration(ctx, "b", []byte(`{`)); !isS3Code(err, s3err.ErrMalformedXML) {
		t.Errorf("битая конфигурация: %v, хотели MalformedXML", err)
	}

	// Состояние бакета не пострадало после отказов.
	out, err := be.GetBucketVersioning(ctx, "b")
	if err != nil || out.Status == nil || *out.Status != types.BucketVersioningStatusEnabled {
		t.Errorf("версионирование пострадало от отказов: %v (%v)", out.Status, err)
	}
}
