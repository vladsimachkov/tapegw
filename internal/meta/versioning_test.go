package meta_test

import (
	"context"
	"errors"
	"testing"

	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// versioningState — состояние бакета глазами meta.
func versioningState(t *testing.T, m *meta.Meta, bucket string) string {
	t.Helper()
	st, err := m.BucketVersioningState(context.Background(), testTenant, bucket)
	if err != nil {
		t.Fatalf("состояние %s: %v", bucket, err)
	}
	return st
}

// Переходы версионирования: таблица «из состояния в состояние».
//
// В unversioned вернуться нельзя; приостановить можно только уже включённый;
// повтор того же статуса — успех, а не ошибка (клиент мог отправить
// конфигурацию дважды, AWS отвечает 200).
func TestSetBucketVersioningTransitions(t *testing.T) {
	cases := []struct {
		from    string
		to      string
		want    string // ожидаемое состояние после вызова
		wantErr error
	}{
		{meta.VersioningUnversioned, meta.VersioningEnabled, meta.VersioningEnabled, nil},
		{meta.VersioningUnversioned, meta.VersioningSuspended,
			meta.VersioningUnversioned, meta.ErrSuspendNeverEnabled},
		{meta.VersioningEnabled, meta.VersioningSuspended, meta.VersioningSuspended, nil},
		{meta.VersioningSuspended, meta.VersioningEnabled, meta.VersioningEnabled, nil},
		{meta.VersioningEnabled, meta.VersioningEnabled, meta.VersioningEnabled, nil},
		{meta.VersioningSuspended, meta.VersioningSuspended, meta.VersioningSuspended, nil},
	}
	for _, tc := range cases {
		t.Run(tc.from+"→"+tc.to, func(t *testing.T) {
			m := testdb.Connect(t)
			testdb.Clean(t, m)
			ctx := context.Background()
			mustBucket(t, m, "vb")
			setBucketVersioning(t, m, "vb", tc.from)

			err := m.SetBucketVersioning(ctx, testTenant, "vb", tc.to)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("ошибка = %v, хотели %v", err, tc.wantErr)
			}
			if got := versioningState(t, m, "vb"); got != tc.want {
				t.Errorf("состояние = %s, хотели %s", got, tc.want)
			}
		})
	}
}

// Бакет с Object Lock приостановить нельзя: версии под retention обязаны
// читаться всегда.
func TestSuspendBlockedByObjectLock(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()
	mustBucket(t, m, "lockbox")

	if err := m.SetBucketVersioning(ctx, testTenant, "lockbox", meta.VersioningEnabled); err != nil {
		t.Fatalf("включить версии: %v", err)
	}
	if err := m.SetObjectLockConfig(ctx, testTenant, "lockbox", true,
		[]byte(`{"Enabled":true}`)); err != nil {
		t.Fatalf("включить лок: %v", err)
	}

	if err := m.SetBucketVersioning(ctx, testTenant, "lockbox", meta.VersioningSuspended); !errors.Is(err, meta.ErrSuspendWithLock) {
		t.Errorf("приостановка бакета с лока: ошибка %v, хотели ErrSuspendWithLock", err)
	}
	if versioningState(t, m, "lockbox") != meta.VersioningEnabled {
		t.Errorf("бакет всё равно приостановлен")
	}
}

// Object Lock включается только поверх включённого версионирования и не
// выключается никогда.
func TestObjectLockConfigLifecycle(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()
	mustBucket(t, m, "lifecycle")

	// Без версий лок не включается.
	if _, err := m.GetObjectLockConfig(ctx, testTenant, "lifecycle"); !errors.Is(err, meta.ErrNotConfigured) {
		t.Fatalf("до включения: ошибка %v, хотели ErrNotConfigured", err)
	}
	if err := m.SetObjectLockConfig(ctx, testTenant, "lifecycle", true, []byte(`{"Enabled":true}`)); !errors.Is(err, meta.ErrLockNeedsVersioning) {
		t.Fatalf("лок без версий: ошибка %v, хотели ErrLockNeedsVersioning", err)
	}

	if err := m.SetBucketVersioning(ctx, testTenant, "lifecycle", meta.VersioningEnabled); err != nil {
		t.Fatalf("включить версии: %v", err)
	}
	cfg := []byte(`{"Enabled":true,"DefaultRetention":{"Mode":"GOVERNANCE","Days":7}}`)
	if err := m.SetObjectLockConfig(ctx, testTenant, "lifecycle", true, cfg); err != nil {
		t.Fatalf("включить лок: %v", err)
	}

	// Байты возвращаются ровно теми, что записали: клиент сверяет конфигурацию
	// с тем, что отправлял.
	got, err := m.GetObjectLockConfig(ctx, testTenant, "lifecycle")
	if err != nil {
		t.Fatalf("чтение конфигурации: %v", err)
	}
	if string(got) != string(cfg) {
		t.Errorf("конфигурация исказилась:\n  получено %s\n  записано  %s", got, cfg)
	}

	// Выключить нельзя.
	if err := m.SetObjectLockConfig(ctx, testTenant, "lifecycle", false, nil); !errors.Is(err, meta.ErrLockCannotDisable) {
		t.Errorf("выключение лока: ошибка %v, хотели ErrLockCannotDisable", err)
	}
	// Правило retention заменить можно.
	update := []byte(`{"Enabled":true,"DefaultRetention":{"Mode":"COMPLIANCE","Days":30}}`)
	if err := m.SetObjectLockConfig(ctx, testTenant, "lifecycle", true, update); err != nil {
		t.Errorf("замена правила: %v", err)
	}
	got, _ = m.GetObjectLockConfig(ctx, testTenant, "lifecycle")
	if string(got) != string(update) {
		t.Errorf("правило не заменилось: %s", got)
	}
}
