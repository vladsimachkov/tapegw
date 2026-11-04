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
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/vladsimachkov/tapegw/internal/meta"
)

// DSNEnv — переменная с DSN тестовой базы.
const DSNEnv = "TAPEGW_TEST_DSN"

// Connect возвращает подключённый пул и применяет миграции. Если TAPEGW_TEST_DSN
// не задан — t.Skip, а не ошибка.
func Connect(t *testing.T) *meta.Meta {
	t.Helper()
	return ConnectSchema(t, PackageSchema(t))
}

// ConnectSchema — то же, что Connect, но в схему с указанным именем.
//
// Нужен тестам самих миграций: они откатывают схему (Down) и применяют её
// заново, а в общей схеме пакета в этот момент работают другие тесты того же
// пакета. Для них Down означал бы «пропало окно», и падали бы они уже не по
// своей вине.
func ConnectSchema(t *testing.T, schema string) *meta.Meta {
	t.Helper()

	dsn := os.Getenv(DSNEnv)
	if dsn == "" {
		t.Skipf("%s не задан — пропускаем тест, которому нужен Postgres", DSNEnv)
	}

	dsn, err := withSchema(dsn, schema)
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

// DSN возвращает DSN схемы пакета — то же подключение, что поднимает Connect.
// Нужен тестам кэша IAM: листенер подключается отдельным соединением, а не из
// пула, и ему нужен свой DSN с той же search_path.
func DSN(t *testing.T) string {
	t.Helper()

	raw := os.Getenv(DSNEnv)
	if raw == "" {
		t.Skipf("%s не задан — пропускаем тест, которому нужен Postgres", DSNEnv)
	}
	dsn, err := withSchema(raw, PackageSchema(t))
	if err != nil {
		t.Fatalf("%s: %v", DSNEnv, err)
	}
	return dsn
}

// DropSchema удаляет схему вместе с содержимым, если она есть. Нужен тестам,
// которые обязаны стартовать с чистой схемы: имя схемы детерминировано, поэтому
// состояние прошлого прогона иначе переедает в следующий, и падение начнёт
// рассказывать не про текущий запуск.
func DropSchema(t *testing.T, schema string) {
	t.Helper()

	raw := os.Getenv(DSNEnv)
	if raw == "" {
		t.Skipf("%s не задан — пропускаем тест, которому нужен Postgres", DSNEnv)
	}

	dsn, err := withSchema(raw, "public")
	if err != nil {
		t.Fatalf("%s: %v", DSNEnv, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE",
		pgx.Identifier{schema}.Sanitize())); err != nil {
		t.Fatalf("drop schema %s: %v", schema, err)
	}
}

// Clean удаляет данные из таблиц, чтобы тесты не зависели от порядка запуска.
// Схема своя на пакет, поэтому TRUNCATE безопасен.
func Clean(t *testing.T, m *meta.Meta) {
	t.Helper()
	ctx := context.Background()
	q := "TRUNCATE " + strings.Join(CleanTables, ", ") + " CASCADE"
	if _, err := m.Pool.Exec(ctx, q); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	for _, seed := range SeedTables {
		d := fmt.Sprintf("DELETE FROM %s WHERE %s <> %d", seed.Table, seed.Column, SeedID)
		if _, err := m.Pool.Exec(ctx, d); err != nil {
			t.Fatalf("зачистка %s до строк сида: %v", seed.Table, err)
		}
	}
}

// CleanTables — таблицы, которые Clean очищает между тестами.
//
// Список вынесен в переменную, потому что правило «Clean обязан покрывать все
// таблицы схемы» на памяти не держится: кто-то добавит миграцию с новой
// таблицей и список не допишет, а тест начнёт проходить или падать в
// зависимости от предыдущего прогона — ровно так же, как падал второй прогон на
// accounts_pkey. TestCleanCoversEveryTable сверяет этот список с pg_tables.
//
// Строки сида миграций (тенант 1, пул 1, tenant_usage для тенанта 1) здесь не
// перечислены: они не мусор, и их проверяет тот же тест отдельно.
var CleanTables = []string{
	"upload_parts", "uploads", "object_parts", "restores", "objects", "buckets",
	"accounts",
}

// SeedID — id строк, которые в таблицы сида кладут миграции: системный
// тенант 1, пул 1, tenant_usage для тенанта 1.
const SeedID = 1

// SeedTable — таблица сида и колонка, по которой опознаётся сид-строка.
type SeedTable struct {
	Table  string
	Column string
}

// SeedTables — таблицы, которые Clean неtruncate-ит целиком: их наполняют
// миграции, и эти строки нужны тестам (внешние ключи на tenants/pools, квота
// тенанта 1).
//
// Но «не трогать целиком» — слишком широко: тесты кладут сюда и свои строки
// (тенанты 2 и 3, их tenant_usage, позже — свои пулы). Между прогонами они
// копятся, а тесты держатся только на ON CONFLICT — та же форма, в которой
// флейк с accounts молчал до второго прогона. Поэтому Clean оставляет в каждой
// такой таблице ровно строку сида.
//
// Порядок значим: это порядок удаления не-сид-строк по внешним ключам, от
// зависимых к родителю (tenant_usage → tenants → pools).
// IsSeedTable — проверяет, объявлена ли таблица как сид. Держать список и
// проверку в одном месте нужно затем, чтобы TestCleanCoversEveryTable не мог
// разойтись с Clean.
func IsSeedTable(name string) bool {
	for _, s := range SeedTables {
		if s.Table == name {
			return true
		}
	}
	return false
}

var SeedTables = []SeedTable{
	{"tenant_usage", "tenant_id"},
	{"tenants", "id"},
	{"pools", "id"},
}

// PackageSchema даёт схеме имя по имени тестового бинаря: backend.test →
// tapegw_test_backend. Детерминированно, поэтому схему можно почистить руками.
// Экспортирована, чтобы тест миграций мог взять свою схему рядом с общей
// (ConnectSchema) и не откатывать общую.
func PackageSchema(t *testing.T) string {
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
