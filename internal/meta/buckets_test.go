package meta_test

import (
	"context"
	"errors"
	"testing"

	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// Тенант строки берётся через insertArg: служебное «все тенанты» отвергается до
// вставки и не долетает до FK. Бэкенд заменяет обход на системный тенант до
// вызова (backend.CreateBucket), так что это вторая линия защиты — но она
// обязана отказывать честной ошибкой, а не 23503 из базы.
func TestCreateBucketRejectsBypassTenant(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)

	_, err := m.CreateBucket(context.Background(), meta.AllTenantsForGuard(),
		"bypass-bucket", "tester", []byte(`{}`), nil)
	if !errors.Is(err, meta.ErrTenantNotWritable) {
		t.Errorf("CreateBucket с обходом: %v, ждали ErrTenantNotWritable", err)
	}
}
