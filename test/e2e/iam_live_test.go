package e2e

import (
	"context"
	"errors"
	"os"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/versity/versitygw/auth"

	"github.com/vladsimachkov/tapegw/internal/iam"
	"github.com/vladsimachkov/tapegw/internal/meta"
)

// Отзыв ключа на живом шлюзе (план PR#2a, находка 2).
//
// Юнит-тест проверяет немедленность отзыва на уровне IAMService. Здесь то же
// самое проверяется сквозь HTTP: шлюз поднят, учётка закеширована первым
// успешным запросом, и именно после этого она отзывается. Если кэш не сбросился,
// вход продолжал бы работать до TTL — юнит этого не увидит, потому что там нет
// реального запроса через контроллер.
//
// Ожидание не допускается: тест обязан отработать сразу, без sleep.
func TestLiveKeyRevokedImmediately(t *testing.T) {
	dsn := requireStand(t)
	kekFile := os.Getenv("TAPEGW_E2E_KEK_FILE")
	if kekFile == "" {
		t.Fatal("TAPEGW_E2E_KEK_FILE не задан")
	}

	ctx := context.Background()
	m, err := meta.Connect(ctx, meta.Options{DSN: dsn, MaxConns: 2})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer m.Close()

	ring, err := iam.LoadKeyRing(kekFile, envInt(t, "TAPEGW_E2E_KEK_VERSION", 1))
	if err != nil {
		t.Fatalf("kek: %v", err)
	}
	svc, err := iam.New(m, ring, iam.Options{RootAccess: os.Getenv("TAPEGW_E2E_ACCESS")})
	if err != nil {
		t.Fatalf("iam: %v", err)
	}

	const access, secret = "livekey", "livesecret"
	// Чистим после прошлого прогона: отзыв не удаляет строку, и повторный
	// выпуск того же ключа закономерно отвергается.
	_, _ = m.Pool.Exec(ctx, `DELETE FROM accounts WHERE access_key = $1`, access)

	if err := svc.CreateAccount(auth.Account{
		Access: access, Secret: secret, Role: auth.RoleUser, ProjectID: 2,
	}); err != nil {
		t.Fatalf("создать учётку: %v", err)
	}
	t.Cleanup(func() {
		_, _ = m.Pool.Exec(context.Background(),
			`DELETE FROM accounts WHERE access_key = $1`, access)
	})

	cl := clientWith(t, access, secret)

	// 1. Вход работает — и именно этот успех кладёт учётку в кэш шлюза.
	if _, err := cl.ListBuckets(ctx, &s3.ListBucketsInput{}); err != nil {
		t.Fatalf("вход новой учётки: %v", err)
	}

	// 2. Отзыв — и сразу, без ожидания, тот же шлюз обязан отказать.
	if err := svc.DeleteUserAccount(access); err != nil {
		t.Fatalf("отзыв: %v", err)
	}
	if _, err := cl.ListBuckets(ctx, &s3.ListBucketsInput{}); err == nil {
		t.Fatalf("отозванный ключ входит в шлюз после отзыва — кэш не сбросился")
	} else if !isAccessDenied(err) && !isInvalidAccessKey(err) {
		t.Fatalf("отказ не похож на отказ доступа: %v", err)
	}
}

func requireStand(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("TAPEGW_E2E_DSN")
	if dsn == "" {
		if os.Getenv("TAPEGW_REQUIRE_ACCESS_TESTS") == "1" {
			t.Fatal("TAPEGW_E2E_DSN не задан, а доступ в CI обязателен")
		}
		t.Skip("TAPEGW_E2E_DSN не задан — пропускаем")
	}
	return dsn
}

func envInt(t *testing.T, name string, def int) int {
	t.Helper()
	raw := os.Getenv(name)
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v < 1 {
		t.Fatalf("%s = %q, ожидалось целое >= 1", name, raw)
	}
	return v
}

// isInvalidAccessKey — отказ «ключа не существует»: versitygw отвечает именно
// так на неизвестный access key (authentication.go), и для отозванного ключа
// это правильный ответ.
func isInvalidAccessKey(err error) bool {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode() == "InvalidAccessKeyId"
	}
	return false
}
