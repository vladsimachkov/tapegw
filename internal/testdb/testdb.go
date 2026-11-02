// Package testdb — общий доступ к Postgres для интеграционных тестов.
//
// Тесты, которым нужна база, помечаются пропуском, если база недоступна:
// правило 2 из AGENTS.md требует, чтобы `go test ./...` проходило на любом
// Linux. Запуск:
//
//	docker run -d --name tapegw-pg -p 55432:5432 \
//	  -e POSTGRES_USER=tapegw -e POSTGRES_PASSWORD=tapegw -e POSTGRES_DB=phobos \
//	  postgres:16-alpine
//	TAPEGW_TEST_DSN='postgres://tapegw:tapegw@localhost:55432/phobos?search_path=tapegw_test' \
//	  go test ./...
//
// Каждый тестовый пакет получает собственную схему. Пакеты гоняются
// параллельно и делят один сервер, а TRUNCATE общей схемы у одного пакета
// превратился бы в случайные падения у другого.
package testdb

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
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

	dsn, err := withSchema(dsn, packageSchema(t))
	if err != nil {
		t.Fatalf("%s: %v", DSNEnv, err)
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
// Схема своя на пакет, поэтому TRUNCATE безопасен.
func Clean(t *testing.T, m *meta.Meta) {
	t.Helper()
	_, err := m.Pool.Exec(context.Background(), `
		TRUNCATE upload_parts, uploads, restores, objects, buckets CASCADE`)
	if err != nil {
		t.Fatalf("truncate: %v", err)
	}
}

// packageSchema даёт схеме имя по имени тестового бинаря: backend.test →
// tapegw_test_backend. Детерминированно, поэтому схему можно почистить руками.
func packageSchema(t *testing.T) string {
	t.Helper()
	base := strings.TrimSuffix(filepath.Base(os.Args[0]), ".test")
	base = nonIdent.ReplaceAllString(base, "_")
	if base == "" {
		base = "default"
	}
	return "tapegw_test_" + base
}

var nonIdent = regexp.MustCompile(`[^a-z0-9_]`)

// withSchema подменяет search_path в DSN. Ошибку возвращаем наружу: кривой DSN
// в тесте должен быть виден сразу, а не превращаться в «тест работает не с той
// схемой».
func withSchema(dsn, schema string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("parse %s: %w", DSNEnv, err)
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	return u.String(), nil
}
