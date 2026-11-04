package backend_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/s3response"

	"github.com/vladsimachkov/tapegw/internal/meta"
)

// clientTenantID — второй, клиентский тенант. testProjectID == 1 — системный
// (ADR-0018), и сценарий «клиент получил бакет» в нём не проверяется: бакет
// системного тенанта клиенту не принадлежит по построению.
const clientTenantID int64 = 5

func setupClientTenant(t *testing.T, m *meta.Meta, access string) context.Context {
	t.Helper()
	ctx := context.Background()
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO tenants (id, name, pool_id) VALUES ($1, 'client', 1)
		  ON CONFLICT (id) DO NOTHING`, clientTenantID); err != nil {
		t.Fatalf("tenant %d: %v", clientTenantID, err)
	}
	// Секрет здесь не нужен: ChangeBucketOwner смотрит только существование,
	// статус и тенант учётки.
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO accounts (access_key, secret_wrapped, role, tenant_id, tenant_system)
		  VALUES ($1, 'test-only-not-a-real-secret'::bytea, 'user', $2, false)
		  ON CONFLICT (access_key) DO UPDATE SET status = 'active', tenant_id = $2`,
		access, clientTenantID); err != nil {
		t.Fatalf("account %s: %v", access, err)
	}
	return accountCtx(auth.Account{Access: access, Role: auth.RoleUser, ProjectID: int(clientTenantID)})
}

// Основной сценарий роли user (план PR#2b, критерий ревьюера):
// admin создаёт бакет → передаёт владение ключу клиента → клиент пишет и читает.
//
// До переноса тенанта этот сценарий не работал вовсе: владелец менялся, а
// tenant_id оставался системным, и страж (ADR-0017) отказывал новому владельцу
// в каждом запросе.
func TestChangeOwnerHandsBucketToClient(t *testing.T) {
	be, m, _ := newBackend(t)
	ctx := context.Background()
	client := setupClientTenant(t, m, "clientkey")

	const bucket = "handed"
	if err := be.CreateBucket(adminCtx(),
		&s3.CreateBucketInput{Bucket: ptr(bucket)}, nil); err != nil {
		t.Fatalf("CreateBucket администратором: %v", err)
	}

	if err := be.ChangeBucketOwner(adminCtx(), bucket, "clientkey"); err != nil {
		t.Fatalf("ChangeBucketOwner: %v", err)
	}

	// Клиент обязан и записать, и прочитать: это и есть «бакет мой».
	if _, err := be.PutObject(client, s3response.PutObjectInput{
		Bucket: ptr(bucket), Key: ptr("k"), Body: strings.NewReader("payload"),
		ContentLength: ptr(int64(7)),
	}); err != nil {
		t.Fatalf("клиент не может писать в полученный бакет: %v", err)
	}
	out, err := be.GetObject(client, &s3.GetObjectInput{Bucket: ptr(bucket), Key: ptr("k")})
	if err != nil {
		t.Fatalf("клиент не может читать из полученного бакета: %v", err)
	}
	if out.ContentLength == nil || *out.ContentLength != 7 {
		t.Errorf("размер = %v, хотели 7", out.ContentLength)
	}

	// Тенант бакета действительно переехал, а не остался системным.
	var tenant int64
	if err := m.Pool.QueryRow(ctx,
		`SELECT tenant_id FROM buckets WHERE name = $1`, bucket).Scan(&tenant); err != nil {
		t.Fatalf("tenant_id бакета: %v", err)
	}
	if tenant != clientTenantID {
		t.Errorf("tenant_id бакета = %d, хотели %d", tenant, clientTenantID)
	}
}

// Перенос между тенантами запрещён, если в бакете что-то осталось: объекты,
// квоты и пул лент так просто не переходят.
func TestChangeOwnerRefusesNonEmpty(t *testing.T) {
	be, m, _ := newBackend(t)
	ctx := context.Background()
	client := setupClientTenant(t, m, "clientkey2")

	const bucket = "notempty"
	if err := be.CreateBucket(adminCtx(),
		&s3.CreateBucketInput{Bucket: ptr(bucket)}, nil); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if _, err := be.PutObject(adminCtx(), s3response.PutObjectInput{
		Bucket: ptr(bucket), Key: ptr("k"), Body: strings.NewReader("x"),
		ContentLength: ptr(int64(1)),
	}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	if err := be.ChangeBucketOwner(adminCtx(), bucket, "clientkey2"); err == nil {
		t.Fatalf("перенос непустого бакета прошёл")
	}
	// Бакет обязан остаться в прежнем тенанте.
	var tenant int64
	if err := m.Pool.QueryRow(ctx,
		`SELECT tenant_id FROM buckets WHERE name = $1`, bucket).Scan(&tenant); err != nil {
		t.Fatalf("tenant_id: %v", err)
	}
	if tenant != testTenantID {
		t.Errorf("после отказа tenant_id = %d, хотели прежний %d", tenant, testTenantID)
	}
	_ = client
}

// Отозванный ключ не может принять бакет: надгробие сохраняет строку, и передать
// бакет ключу, которым уже нельзя войти, — значит спрятать его в мёртвом
// тенанте.
func TestChangeOwnerRejectsRevokedAccount(t *testing.T) {
	be, m, _ := newBackend(t)
	ctx := context.Background()
	setupClientTenant(t, m, "deadkey")

	if _, err := m.Pool.Exec(ctx,
		`UPDATE accounts SET status = 'revoked', secret_wrapped = NULL WHERE access_key = 'deadkey'`); err != nil {
		t.Fatalf("отзыв: %v", err)
	}

	const bucket = "to-dead"
	if err := be.CreateBucket(adminCtx(),
		&s3.CreateBucketInput{Bucket: ptr(bucket)}, nil); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if err := be.ChangeBucketOwner(adminCtx(), bucket, "deadkey"); err == nil {
		t.Fatalf("бакет передан отозванному ключу")
	}
	if err := be.ChangeBucketOwner(adminCtx(), bucket, "never-existed"); err == nil {
		t.Fatalf("бакет передан несуществующему ключу")
	}
}
