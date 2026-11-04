package meta_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// Миграция 0008: «отозван» и «без секрета» связаны в схеме, а не в коде.
// Иначе активная учётка без секрета даёт неразборчивую ошибку расшифровки, а
// отозванная с секретом оставляет в базе материал для офлайн-подбора.
func TestRevokedSecretCheck(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	cases := []struct {
		name   string
		secret string // SQL-выражение для secret_wrapped
		status string
		ok     bool
	}{
		{"активная с секретом", "'x'::bytea", "active", true},
		{"отозванная без секрета", "NULL", "revoked", true},
		{"активная без секрета", "NULL", "active", false},
		{"отозванная с секретом", "'x'::bytea", "revoked", false},
	}
	for i, tc := range cases {
		_, err := m.Pool.Exec(ctx,
			`INSERT INTO accounts (access_key, secret_wrapped, status, role, tenant_id, tenant_system)
			  VALUES ($1, `+tc.secret+`, '`+tc.status+`', 'admin', 1, true)`,
			fmt.Sprintf("chk-%d", i))
		if tc.ok && err != nil {
			t.Errorf("%s: вставка отклонена: %v", tc.name, err)
		}
		if !tc.ok && err == nil {
			t.Errorf("%s: вставка прошла — CHECK не работает", tc.name)
		}
	}
}
