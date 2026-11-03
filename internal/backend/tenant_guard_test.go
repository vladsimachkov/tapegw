package backend

// Белая коробка: tenantFromContext не экспортируется нарочно — наружу у бэкенда
// торчат только методы backend.Backend, а решение о тенанте принимает страж.

import (
	"context"
	"errors"
	"testing"

	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/s3err"

	"github.com/vladsimachkov/tapegw/internal/meta"
)

func ctxWithAccount(acct auth.Account) context.Context {
	//lint:ignore SA1029 ключ диктует versitygw, своего типа у него нет
	return context.WithValue(context.Background(), accountKey, acct)
}

func isAccessDenied(err error) bool {
	var se s3err.S3Error
	return errors.As(err, &se) && se.BaseError() == s3err.GetAPIError(s3err.ErrAccessDenied)
}

func TestTenantFromContext(t *testing.T) {
	tests := []struct {
		name    string
		ctx     context.Context
		wantErr bool
		bypass  bool
		project int64
	}{
		{
			name:    "учётки в контексте нет",
			ctx:     context.Background(),
			wantErr: true,
		},
		{
			// set-default-keys.go:33 кладёт именно пустой Account{}, поэтому
			// аноним — это ProjectID == 0, а не отсутствие значения.
			name:    "аноним: пустая учётка",
			ctx:     ctxWithAccount(auth.Account{}),
			wantErr: true,
		},
		{
			name:    "обычный ключ без тенанта",
			ctx:     ctxWithAccount(auth.Account{Access: "u", Role: auth.RoleUser}),
			wantErr: true,
		},
		{
			name:    "обычный ключ с тенантом",
			ctx:     ctxWithAccount(auth.Account{Access: "u", Role: auth.RoleUser, ProjectID: 7}),
			project: 7,
		},
		{
			name:    "userplus остаётся под стражом",
			ctx:     ctxWithAccount(auth.Account{Access: "u", Role: auth.RoleUserPlus, ProjectID: 3}),
			project: 3,
		},
		{
			// Root приходит именно так: authentication.go:44 собирает
			// rootAccount с RoleAdmin и без ProjectID.
			name:   "root: RoleAdmin при ProjectID 0",
			ctx:    ctxWithAccount(auth.Account{Access: "root", Role: auth.RoleAdmin}),
			bypass: true,
		},
		{
			name:   "администратор",
			ctx:    ctxWithAccount(auth.Account{Access: "adm", Role: auth.RoleAdmin, ProjectID: 9}),
			bypass: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tenantFromContext(tc.ctx)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ожидали отказ, получили тенанта %+v", got)
				}
				if !isAccessDenied(err) {
					t.Errorf("ожидали AccessDenied, получили %v", err)
				}
				// Отказ не должен приносить «продвинутый» тенант.
				if got.IsBypass() {
					t.Error("при отказе страж не обязан отдавать обход")
				}
				return
			}
			if err != nil {
				t.Fatalf("неожиданный отказ: %v", err)
			}
			if got.IsBypass() != tc.bypass {
				t.Errorf("IsBypass = %v, хотели %v", got.IsBypass(), tc.bypass)
			}
			if !tc.bypass && got != meta.NewTenant(int(tc.project)) {
				t.Errorf("тенант = %+v, хотели тенанта проекта %d", got, tc.project)
			}
		})
	}
}
