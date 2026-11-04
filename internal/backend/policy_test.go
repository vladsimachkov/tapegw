package backend_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/versity/versitygw/auth"
	vs3err "github.com/versity/versitygw/s3err"

	"github.com/vladsimachkov/tapegw/internal/meta"
)

// Смена владельца обязана менять ПРАВА, а не только колонку owner.
//
// versitygw решает, кто владелец, по ACL из GetBucketAcl, а не по
// buckets.owner. Если обновлять только колонку, операция снаружи выглядит
// успешной, а права остаются старыми: новый владелец (роль user, ради которой
// метод и нужен) доступа не получает, прежний сохраняет FULL_CONTROL — и через
// ACL, и через старую политику. Это та же форма тихой ошибки, что заглушки,
// возвращающие NotFound.
func TestChangeBucketOwnerReassignsRights(t *testing.T) {
	be, m, _ := newBackend(t)
	// Смена владельца — admin API, под администратором и работаем.
	ctx := adminCtx()

	// Новый владелец обязан существовать в accounts: с PR#2b его тенант берётся
	// оттуда, а несуществующему ключу бакет не передаётся. Тенант — клиентский:
	// не-admin в системном тенанте запрещён CHECK из миграции 0007.
	if _, err := m.Pool.Exec(context.Background(),
		`INSERT INTO tenants (id, name, pool_id) VALUES (2, 'client-po', 1)
		  ON CONFLICT (id) DO NOTHING`); err != nil {
		t.Fatalf("tenant 2: %v", err)
	}
	if _, err := m.Pool.Exec(context.Background(),
		`INSERT INTO accounts (access_key, secret_wrapped, role, tenant_id, tenant_system)
		  VALUES ('userplus', 'test-only-not-a-real-secret'::bytea, 'userplus', 2, false)
		  ON CONFLICT (access_key) DO NOTHING`); err != nil {
		t.Fatalf("account userplus: %v", err)
	}

	if err := be.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: ptr("own")}, nil); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	// И ACL, и политика — обе должны быть перебиты сменой владельца.
	b, err := json.Marshal(auth.ACL{Owner: "tester", Grantees: []auth.Grantee{{
		Permission: auth.PermissionFullControl, Access: "tester",
	}}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := be.PutBucketAcl(ctx, "own", b); err != nil {
		t.Fatalf("PutBucketAcl: %v", err)
	}
	policy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"AWS":"tester"},"Action":["s3:GetObject"],` +
		`"Resource":"arn:aws:s3:::own/*"}]}`
	if err := be.PutBucketPolicy(ctx, "own", []byte(policy)); err != nil {
		t.Fatalf("PutBucketPolicy: %v", err)
	}

	if err := be.ChangeBucketOwner(ctx, "own", "userplus"); err != nil {
		t.Fatalf("ChangeBucketOwner: %v", err)
	}

	// 1. ACL: владелец — новый аккаунт, и единственный грант FULL_CONTROL ему же.
	raw, err := be.GetBucketAcl(ctx, &s3.GetBucketAclInput{Bucket: ptr("own")})
	if err != nil {
		t.Fatalf("GetBucketAcl: %v", err)
	}
	var got auth.ACL
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal ACL: %v", err)
	}
	if got.Owner != "userplus" {
		t.Errorf("владелец ACL = %q, хотели userplus", got.Owner)
	}
	if len(got.Grantees) != 1 {
		t.Fatalf("грантов %d, ожидали один: %+v", len(got.Grantees), got.Grantees)
	}
	if g := got.Grantees[0]; g.Access != "userplus" || g.Permission != auth.PermissionFullControl {
		t.Errorf("грант = %+v, хотели FULL_CONTROL для userplus", g)
	}

	// 2. Политика обнулена: иначе прежний владелец сохранил бы доступ по ней.
	if _, err := be.GetBucketPolicy(ctx, "own"); !isS3Code(err, vs3err.ErrNoSuchBucketPolicy) {
		t.Errorf("политика после смены владельца: %v, хотели NoSuchBucketPolicy", err)
	}

	// 3. Инвариант: buckets.owner и acl.Owner не расходятся.
	bucket, err := m.GetBucket(ctx, meta.NewTenant(2), "own")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	if bucket.Owner != got.Owner {
		t.Errorf("buckets.owner=%q, acl.Owner=%q — инвариант нарушен", bucket.Owner, got.Owner)
	}
}
