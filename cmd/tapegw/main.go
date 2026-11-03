// Command tapegw — S3-поверхность с Glacier-семантикой поверх Phobos.
//
// Процесс поднимает versitygw через embedgw.RunVersityGW с бэкендом из
// internal/backend и (начиная с M4) фоновые воркеры. Флаги и конфигурация
// описаны в docs/ARCHITECTURE.md, раздел «Конфигурация, развёртывание, стенды».
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/versity/versitygw/embedgw"

	"github.com/vladsimachkov/tapegw/internal/backend"
	"github.com/vladsimachkov/tapegw/internal/config"
	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/spool"
)

// Заполняется сборкой: -ldflags "-X main.Version=... -X main.Build=..."
var (
	Version   = "dev"
	Build     = "none"
	BuildTime = "unknown"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "tapegw: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("tapegw", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "usage: tapegw [flags]\n\n")
		fs.PrintDefaults()
		fmt.Fprintf(fs.Output(), "\nполная конфигурация — docs/ARCHITECTURE.md, deploy/tapegw.yaml.example\n")
	}
	var (
		cfgPath     = fs.String("config", "", "путь к YAML-конфигурации (можно не задавать, всё из TAPEGW_*)")
		showVer     = fs.Bool("version", false, "напечатать версию и выйти")
		migrateOnly = fs.Bool("migrate", false, "применить миграции и выйти")
		skipMigrate = fs.Bool("skip-migrate", false, "не применять миграции на старте")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *showVer {
		fmt.Printf("tapegw %s (build %s, %s)\n", Version, Build, BuildTime)
		return nil
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	if *skipMigrate {
		no := false
		cfg.DB.MigrateOnStart = &no
	}

	logger := newLogger(cfg.Logging)
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *migrateOnly {
		return meta.Migrate(ctx, cfg.DB.DSN)
	}

	if cfg.MigrateOnStartValue() {
		if err := meta.Migrate(ctx, cfg.DB.DSN); err != nil {
			return fmt.Errorf("migrations: %w", err)
		}
		logger.Info("migrations applied")
	}

	pool, err := connectWithBackoff(ctx, logger, cfg)
	if err != nil {
		return err
	}
	defer pool.Close()

	land, err := spool.Open(cfg.Spool.Landing)
	if err != nil {
		return fmt.Errorf("landing: %w", err)
	}

	be := backend.New(pool, land, cfg)

	gwCfg := &embedgw.Config{
		Ports:             cfg.S3.Listen,
		RootUserAccess:    cfg.Root.Access,
		RootUserSecret:    cfg.Root.Secret,
		Region:            cfg.S3.Region,
		IAMDir:            cfg.S3.IAMDir,
		HealthPath:        cfg.S3.HealthPath,
		MaxConnections:    cfg.S3.MaxConnections,
		MaxRequests:       cfg.S3.MaxRequests,
		MultipartMaxParts: cfg.S3.MultipartMaxParts,
		CertFile:          cfg.S3.TLS.Cert,
		KeyFile:           cfg.S3.TLS.Key,
		AdminPathPrefix:   cfg.Admin.PathPrefix,
		Version:           Version,
		Build:             Build,
		BuildTime:         BuildTime,
	}
	if cfg.Admin.Listen != "" {
		gwCfg.AdminPorts = []string{cfg.Admin.Listen}
	}

	logger.Info("starting s3 gateway",
		slog.Any("listen", cfg.S3.Listen),
		slog.String("region", cfg.S3.Region),
		slog.String("backend", be.String()),
		slog.String("phobos_family", cfg.Phobos.Family))

	// Начиная с M4 здесь же стартуют воркеры (migrator, recaller, janitor,
	// deleter) и metrics-листенер. Пока их нет — только шлюз.
	if err := embedgw.RunVersityGW(ctx, be, gwCfg); err != nil &&
		!errors.Is(err, context.Canceled) {
		return fmt.Errorf("gateway: %w", err)
	}
	logger.Info("stopped")
	return nil
}

// connectWithBackoff ждёт Postgres. systemd-юнит задает After=/Requires=, но
// порядок старта не гарантирует готовности базы, поэтому ретраим с бэкоффом,
// а не падаем на первом неудачном подключении.
func connectWithBackoff(ctx context.Context, logger *slog.Logger, cfg *config.Config) (*meta.Meta, error) {
	backoff := 500 * time.Millisecond
	const maxBackoff = 15 * time.Second

	for attempt := 1; ; attempt++ {
		pool, err := meta.Connect(ctx, meta.Options{
			DSN:            cfg.DB.DSN,
			MaxConns:       cfg.DB.MaxConns,
			ConnectTimeout: cfg.DB.ConnectTimeout,
		})
		if err == nil {
			return pool, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		logger.Warn("postgres not ready, retrying",
			slog.Int("attempt", attempt),
			slog.Duration("retry_in", backoff),
			slog.String("err", err.Error()))

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

func newLogger(l config.Logging) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(l.Level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: lvl}
	if strings.EqualFold(l.Format, "text") {
		return slog.New(slog.NewTextHandler(os.Stderr, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, opts))
}
