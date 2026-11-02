// Package testdb — общий доступ к Postgres для интеграционных тестов.
//
// Тесты, которым нужна база, помечаются пропуском, если база недоступна:
// правило 2 из AGENTS.md требует, чтобы `go test ./...` проходило на любом
// Linux. Запускать так:
//
//	docker run -d --name tapegw-pg -p 55432:5432 \
//	  -e POSTGRES_USER=tapegw -e POSTGRES_PASSWORD=tapegw -e POSTGRES_DB=phobos \
//	  postgres:16-alpine
//	TAPEGW_TEST_DSN='postgres://tapegw:tapegw@localhost:55432/phobos?search_path=tapegw_test' \
//	  go test ./...
package testdb

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/vladsimachkov/tapegw/internal/meta"
)

// DSNEnv — переменная с DSN тестовой базы.
const DSNEnv = "TAPEGW_TEST_DSN"

// Connect возвращает подключённый пул и применяет миграции. Если TAPEGW_TEST_DSN
// не задан — t.Skip, а не ошибка.
func Connect(t *testing.T) *meta.Meta {
	t.Helper()

	dsn := os.Getenv(DSNEnv)
	if dsn == "" {
		t.Skipf("%s не задан — пропускаем тест, которому нужен Postgres", DSNEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)

	if err := meta.Migrate(ctx, dsn); err != nil {
		t.Fatalf("миграции: %v", err)
	}

	m, err := meta.Connect(ctx, meta.Options{DSN: dsn, MaxConns: 4, ConnectTimeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("подключение: %v", err)
	}
	t.Cleanup(m.Close)

	return m
}

// Clean удаляет данные из таблиц, чтобы тесты не зависели от порядка запуска.
// Схему не дропаем: параллельные тесты в одном пакете делят одну базу.
func Clean(t *testing.T, m *meta.Meta) {
	t.Helper()
	_, err := m.Pool.Exec(context.Background(), `
		TRUNCATE upload_parts, uploads, restores, objects, buckets CASCADE`)
	if err != nil {
		t.Fatalf("truncate: %v", err)
	}
}
