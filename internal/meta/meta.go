// Package meta — доступ к Postgres: пул соединений, миграции и запросы очередей.
//
// Вся S3-метадата и очереди живут в схеме `tapegw` того же Postgres, где Phobos
// держит свою DSS (решение D2). Миграции только вперёд и встроены в бинарь.
package meta

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib" // региструет driver "pgx" для database/sql
	"github.com/pressly/goose/v3"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Meta — обёртка над пулом pgx. Пакет намеренно тонкий: SQL живёт рядом
// с сущностями (buckets.go, objects.go, ...), а не в абстракции репозитория.
type Meta struct {
	Pool *pgxpool.Pool
}

// Options — параметры подключения из конфигурации.
type Options struct {
	DSN            string
	MaxConns       int32
	ConnectTimeout time.Duration
}

// Connect поднимает пул и проверяет, что база действительно доступна:
// ленивое соединение pgx отложило бы ошибку на первый запрос.
func Connect(ctx context.Context, o Options) (*Meta, error) {
	cfg, err := pgxpool.ParseConfig(o.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	if o.MaxConns > 0 {
		cfg.MaxConns = o.MaxConns
	}
	if o.ConnectTimeout > 0 {
		cfg.ConnConfig.ConnectTimeout = o.ConnectTimeout
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	pingCtx := ctx
	if o.ConnectTimeout > 0 {
		var cancel context.CancelFunc
		pingCtx, cancel = context.WithTimeout(ctx, o.ConnectTimeout)
		defer cancel()
	}
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	return &Meta{Pool: pool}, nil
}

// Close освобождает соединения пула.
func (m *Meta) Close() { m.Pool.Close() }

// Migrate применяет все неприменённые миграции. Повторный вызов идемпотентен:
// goose ведёт учёт в таблице goose_db_version.
//
// Миграции выполняются на отдельном *sql.DB, а не на пуле: goose требует
// последовательные вызовы и сам управляет транзакциями.
//
// Схема из search_path создаётся автоматически: миграции пишут таблицы без
// квалификатора, и без схемы первый запуск на чистой базе упал бы с
// "schema tapegw does not exist".
func Migrate(ctx context.Context, dsn string) error {
	schema, err := schemaFromDSN(dsn)
	if err != nil {
		return err
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("open for migrations: %w", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping for migrations: %w", err)
	}

	if schema != "" {
		if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pgx.Identifier{schema}.Sanitize())); err != nil {
			return fmt.Errorf("create schema %q: %w", schema, err)
		}
	}

	goose.SetBaseFS(migrationsFS)
	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("goose dialect: %w", err)
	}
	if err := goose.RunContext(ctx, "up", db, "migrations"); err != nil {
		return fmt.Errorf("migrate up: %w", err)
	}
	return nil
}

// schemaFromDSN достаёт имя схемы из search_path строки подключения.
// Пустая строка означает «использовать схему по умолчанию базы».
func schemaFromDSN(dsn string) (string, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return "", fmt.Errorf("parse dsn: %w", err)
	}
	if cfg.RuntimeParams == nil {
		return "", nil
	}
	sp := strings.TrimSpace(cfg.RuntimeParams["search_path"])
	if sp == "" {
		return "", nil
	}
	// search_path — список; берём первую схему, она и есть «текущая».
	first, _, _ := strings.Cut(sp, ",")
	return strings.Trim(strings.TrimSpace(first), `"`), nil
}

// ErrNotFound — объект или бакет не найден. Отличаем от реальной ошибки БД,
// чтобы воркеры не ретраили «нет такой строки».
var ErrNotFound = errors.New("meta: not found")

// IsNotFound reports whether err is (or wraps) ErrNotFound.
func IsNotFound(err error) bool { return errors.Is(err, ErrNotFound) }
