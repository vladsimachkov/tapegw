package backend_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
	"github.com/vladsimachkov/tapegw/internal/backend"

	"github.com/vladsimachkov/tapegw/internal/meta"
)

// enableLock включает в бакете версионирование и Object Lock. cfg — JSON
// конфигурации в том же виде, в котором его передаёт versitygw.
func enableLock(t *testing.T, be *backend.TapeGW, m *meta.Meta, bucket, cfg string) {
	t.Helper()
	ctx := testCtx()
	setVersioningDB(t, m, bucket, meta.VersioningEnabled)
	if cfg == "" {
		return
	}
	if err := be.PutObjectLockConfiguration(ctx, bucket, []byte(cfg)); err != nil {
		t.Fatalf("включить Object Lock: %v", err)
	}
}

// putWith — PutObject с заголовками Object Lock.
func putLocked(t *testing.T, be *backend.TapeGW, bucket, key, body string,
	mode types.ObjectLockMode, until *time.Time, hold types.ObjectLockLegalHoldStatus) s3response.PutObjectOutput {
	t.Helper()
	out, err := be.PutObject(testCtx(), s3response.PutObjectInput{
		Bucket:                    ptr(bucket),
		Key:                       ptr(key),
		Body:                      strings.NewReader(body),
		ContentLength:             aws.Int64(int64(len(body))),
		ObjectLockMode:            mode,
		ObjectLockRetainUntilDate: until,
		ObjectLockLegalHoldStatus: hold,
	})
	if err != nil {
		t.Fatalf("PutObject(%s/%s): %v", bucket, key, err)
	}
	return out
}

const govCfg = `{"Enabled":true,"DefaultRetention":{"Mode":"GOVERNANCE","Days":7}}`

// lockNoRule — лок включён, но правила по умолчанию нет: PUT не получает
// retention, и тест может различить «retention поставили явно» и «его дал
// бакет».

// Retention на конкретной версии по versionId, включая null-версию.
func TestRetentionOnSpecificVersion(t *testing.T) {
	be, m, _ := newBackend(t)
	ctx := testCtx()
	mustBucket(t, be)
	enableLock(t, be, m, "b", `{"Enabled":true}`)

	putBody(t, be, "b", "k", "первая")
	first := mustCurrentID(t, m, "b", "k")
	putBody(t, be, "b", "k", "вторая версия")

	until := time.Now().Add(48 * time.Hour).UTC().Truncate(time.Second)
	if err := be.PutObjectRetention(ctx, "b", "k", first,
		mustRetentionJSON(t, types.ObjectLockRetentionModeGovernance, until)); err != nil {
		t.Fatalf("retention на нетекущей версии: %v", err)
	}

	raw, err := be.GetObjectRetention(ctx, "b", "k", first)
	if err != nil {
		t.Fatalf("GetObjectRetention: %v", err)
	}
	var got types.ObjectLockRetention
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("разбор ответа: %v", err)
	}
	if got.Mode != types.ObjectLockRetentionModeGovernance {
		t.Errorf("режим = %s, хотели GOVERNANCE", got.Mode)
	}
	if got.RetainUntilDate == nil || !got.RetainUntilDate.Equal(until) {
		t.Errorf("срок = %v, хотели %v", got.RetainUntilDate, until)
	}

	// Текущая версия не затронута: retention относится к версии, а не к ключу.
	if _, err := be.GetObjectRetention(ctx, "b", "k", ""); !isS3Code(err,
		s3err.ErrNoSuchObjectLockConfiguration) {
		t.Errorf("у текущей версии retention появился: %v", err)
	}

	// И по versionId читается именно та версия: HEAD нетекущей отдаёт её срок.
	h, err := be.HeadObject(ctx, &s3.HeadObjectInput{Bucket: ptr("b"), Key: ptr("k"),
		VersionId: ptr(first)})
	if err != nil {
		t.Fatalf("HEAD по versionId: %v", err)
	}
	if h.ObjectLockMode != types.ObjectLockModeGovernance {
		t.Errorf("HEAD x-amz-object-lock-mode = %s, хотели GOVERNANCE", h.ObjectLockMode)
	}
}

// Null-версия — такая же версия, и retention на неё ставится по versionId=null.
func TestRetentionOnNullVersion(t *testing.T) {
	be, m, _ := newBackend(t)
	ctx := testCtx()
	mustBucket(t, be)
	setVersioningDB(t, m, "b", meta.VersioningUnversioned)
	putBody(t, be, "b", "k", "до включения версий")
	setVersioningDB(t, m, "b", meta.VersioningEnabled)
	if err := be.PutObjectLockConfiguration(ctx, "b", []byte(govCfg)); err != nil {
		t.Fatalf("включить лок: %v", err)
	}

	until := time.Now().Add(24 * time.Hour).UTC().Truncate(time.Second)
	if err := be.PutObjectRetention(ctx, "b", "k", "null",
		mustRetentionJSON(t, types.ObjectLockRetentionModeGovernance, until)); err != nil {
		t.Fatalf("retention на null-версии: %v", err)
	}
	on, err := be.GetObjectLegalHold(ctx, "b", "k", "null")
	if err != nil {
		t.Fatalf("legal hold null-версии: %v", err)
	}
	if on == nil || *on {
		t.Errorf("legal hold = %v, хотели false", on)
	}
}

// COMPLIANCE нельзя сократить, снять или заменить; GOVERNANCE — можно.
func TestComplianceRetentionIsImmutable(t *testing.T) {
	be, m, _ := newBackend(t)
	ctx := testCtx()
	mustBucket(t, be)
	enableLock(t, be, m, "b", `{"Enabled":true}`)
	putBody(t, be, "b", "k", "данные")
	vid := mustCurrentID(t, m, "b", "k")

	long := time.Now().Add(30 * 24 * time.Hour).UTC().Truncate(time.Second)
	short := time.Now().Add(10 * 24 * time.Hour).UTC().Truncate(time.Second)
	comp := types.ObjectLockRetentionModeCompliance

	if err := be.PutObjectRetention(ctx, "b", "k", vid, mustRetentionJSON(t, comp, long)); err != nil {
		t.Fatalf("поставить COMPLIANCE: %v", err)
	}
	if err := be.PutObjectRetention(ctx, "b", "k", vid, mustRetentionJSON(t, comp, short)); !isS3Code(err, s3err.ErrAccessDenied) {
		t.Errorf("сокращение COMPLIANCE: %v, хотели AccessDenied", err)
	}
	if err := be.PutObjectRetention(ctx, "b", "k", vid,
		mustRetentionJSON(t, types.ObjectLockRetentionModeGovernance, short)); !isS3Code(err, s3err.ErrAccessDenied) {
		t.Errorf("замена COMPLIANCE на GOVERNANCE: %v, хотели AccessDenied", err)
	}
	// Продлить можно.
	if err := be.PutObjectRetention(ctx, "b", "k", vid,
		mustRetentionJSON(t, comp, long.Add(24*time.Hour))); err != nil {
		t.Errorf("продление COMPLIANCE: %v", err)
	}
	// Срок не изменился на отклонённых попытках.
	raw, err := be.GetObjectRetention(ctx, "b", "k", vid)
	if err != nil {
		t.Fatalf("чтение retention: %v", err)
	}
	var got types.ObjectLockRetention
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("разбор: %v", err)
	}
	if got.RetainUntilDate == nil || !got.RetainUntilDate.After(long) {
		t.Errorf("срок пострадал от отказов: %v", got.RetainUntilDate)
	}

	// GOVERNANCE сокращать можно — он для защиты от своей же ошибки.
	putBody(t, be, "b", "g", "другой ключ")
	gid := mustCurrentID(t, m, "b", "g")
	if err := be.PutObjectRetention(ctx, "b", "g", gid,
		mustRetentionJSON(t, types.ObjectLockRetentionModeGovernance, long)); err != nil {
		t.Fatalf("поставить GOVERNANCE: %v", err)
	}
	if err := be.PutObjectRetention(ctx, "b", "g", gid,
		mustRetentionJSON(t, types.ObjectLockRetentionModeGovernance, short)); err != nil {
		t.Errorf("сокращение GOVERNANCE: %v, хотели успех", err)
	}
}

// Retention по умолчанию бакета применяется при PUT, а явные заголовки важнее
// правила.
func TestDefaultRetentionAtPut(t *testing.T) {
	be, m, _ := newBackend(t)
	ctx := testCtx()
	mustBucket(t, be)
	enableLock(t, be, m, "b", govCfg)

	// Без заголовков — правило бакета (7 дней).
	putBody(t, be, "b", "default", "данные")
	h, err := be.HeadObject(ctx, &s3.HeadObjectInput{Bucket: ptr("b"), Key: ptr("default")})
	if err != nil {
		t.Fatalf("HEAD: %v", err)
	}
	if h.ObjectLockMode != types.ObjectLockModeGovernance {
		t.Fatalf("правило по умолчанию не применилось: режим %q", h.ObjectLockMode)
	}
	if h.ObjectLockRetainUntilDate == nil ||
		time.Until(*h.ObjectLockRetainUntilDate) < 6*24*time.Hour {
		t.Errorf("срок по умолчанию = %v, хотели ~7 дней", h.ObjectLockRetainUntilDate)
	}

	// Явный заголовок важнее правила: клиент просит один день и получает один.
	one := time.Now().Add(24 * time.Hour).UTC().Truncate(time.Second)
	putLocked(t, be, "b", "explicit", "данные",
		types.ObjectLockModeCompliance, &one, types.ObjectLockLegalHoldStatusOn)
	h, err = be.HeadObject(ctx, &s3.HeadObjectInput{Bucket: ptr("b"), Key: ptr("explicit")})
	if err != nil {
		t.Fatalf("HEAD: %v", err)
	}
	if h.ObjectLockMode != types.ObjectLockModeCompliance {
		t.Errorf("явный режим проигнорирован: %s", h.ObjectLockMode)
	}
	if h.ObjectLockRetainUntilDate == nil || !h.ObjectLockRetainUntilDate.Equal(one) {
		t.Errorf("явный срок проигнорирован: %v, хотели %v", h.ObjectLockRetainUntilDate, one)
	}
	if h.ObjectLockLegalHoldStatus != types.ObjectLockLegalHoldStatusOn {
		t.Errorf("legal hold не записан: %q", h.ObjectLockLegalHoldStatus)
	}

	// GET отдаёт то же, что и HEAD.
	g, err := be.GetObject(ctx, &s3.GetObjectInput{Bucket: ptr("b"), Key: ptr("explicit")})
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer g.Body.Close()
	if g.ObjectLockMode != types.ObjectLockModeCompliance ||
		g.ObjectLockLegalHoldStatus != types.ObjectLockLegalHoldStatusOn {
		t.Errorf("GET не отдал заголовки лока: mode=%q hold=%q",
			g.ObjectLockMode, g.ObjectLockLegalHoldStatus)
	}
}

// Legal hold включается и снимается свободно — в отличие от COMPLIANCE.
func TestLegalHoldToggle(t *testing.T) {
	be, m, _ := newBackend(t)
	ctx := testCtx()
	mustBucket(t, be)
	enableLock(t, be, m, "b", `{"Enabled":true}`)
	putBody(t, be, "b", "k", "данные")
	vid := mustCurrentID(t, m, "b", "k")

	if err := be.PutObjectLegalHold(ctx, "b", "k", vid, true); err != nil {
		t.Fatalf("включить legal hold: %v", err)
	}
	on, err := be.GetObjectLegalHold(ctx, "b", "k", vid)
	if err != nil || on == nil || !*on {
		t.Fatalf("legal hold не включился: %v (%v)", on, err)
	}
	if err := be.PutObjectLegalHold(ctx, "b", "k", vid, false); err != nil {
		t.Fatalf("снять legal hold: %v", err)
	}
	on, err = be.GetObjectLegalHold(ctx, "b", "k", vid)
	if err != nil || on == nil || *on {
		t.Errorf("legal hold не снялся: %v (%v)", on, err)
	}
}

// Без включённого лока retention ставить нечего.
func TestRetentionWithoutLock(t *testing.T) {
	be, m, _ := newBackend(t)
	ctx := testCtx()
	mustBucket(t, be)
	setVersioningDB(t, m, "b", meta.VersioningEnabled)
	putBody(t, be, "b", "k", "данные")
	vid := mustCurrentID(t, m, "b", "k")

	err := be.PutObjectRetention(ctx, "b", "k", vid,
		mustRetentionJSON(t, types.ObjectLockRetentionModeGovernance,
			time.Now().Add(time.Hour)))
	if !isS3Code(err, s3err.ErrMissingObjectLockConfiguration) {
		t.Errorf("retention без лока: %v, хотели MissingObjectLockConfiguration", err)
	}
}

func mustRetentionJSON(t *testing.T, mode types.ObjectLockRetentionMode,
	until time.Time) []byte {

	t.Helper()
	b, err := json.Marshal(types.ObjectLockRetention{
		Mode: mode, RetainUntilDate: &until})
	if err != nil {
		t.Fatalf("сбор retention: %v", err)
	}
	return b
}

// Обход GOVERNANCE на уровне бэкенда: с флагом версия удаляется, без него —
// нет. Это ровно то доверие к заголовку, которое записано в ADR-0015: бэкенд
// смотрит на заголовок, а право проверяет versitygw до вызова.
func TestGovernanceBypassAtBackend(t *testing.T) {
	be, m, _ := newBackend(t)
	ctx := testCtx()
	mustBucket(t, be)
	enableLock(t, be, m, "b", `{"Enabled":true}`)
	putBody(t, be, "b", "k", "данные")
	vid := mustCurrentID(t, m, "b", "k")
	until := time.Now().Add(24 * time.Hour)
	if err := be.PutObjectRetention(ctx, "b", "k", vid,
		mustRetentionJSON(t, types.ObjectLockRetentionModeGovernance, until)); err != nil {
		t.Fatalf("поставить GOVERNANCE: %v", err)
	}

	if _, err := be.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: ptr("b"), Key: ptr("k"),
		VersionId: ptr(vid)}); !isS3Code(err, s3err.ErrObjectLocked) {
		t.Errorf("удаление GOVERNANCE без обхода: %v, хотели ObjectLocked", err)
	}
	if _, err := be.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: ptr("b"), Key: ptr("k"),
		VersionId: ptr(vid), BypassGovernanceRetention: ptr(true)}); err != nil {
		t.Errorf("удаление GOVERNANCE с обходом: %v, хотели успех", err)
	}
}
