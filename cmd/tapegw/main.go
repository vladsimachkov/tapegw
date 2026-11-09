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
	"github.com/vladsimachkov/tapegw/internal/iam"
	"github.com/vladsimachkov/tapegw/internal/keys"
	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/spool"
	"github.com/vladsimachkov/tapegw/internal/worker"
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
	// Операторские подкоманды (`tapegw admin media add`, Ф1.5) — не сервер:
	// разбираются до флагов шлюза.
	if len(args) > 0 && args[0] == "admin" {
		return runAdmin(args[1:])
	}
	fs := flag.NewFlagSet("tapegw", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "usage: tapegw [flags]\n       tapegw admin media add ...\n\n")
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
	// Cache — буфер восстановленных копий (S5): пишет recaller, читает GET,
	// вытесняет janitor. Обязателен наравне с landing (проверка в конфиге).
	cache, err := spool.Open(cfg.Spool.Cache)
	if err != nil {
		return fmt.Errorf("cache: %w", err)
	}

	// Кольцо KEK загружается ДО бэкенда и независимо от того, включён ли
	// Postgres-IAM: шифрование тела объекта обязательное (ADR-0014), и без
	// кольца нечем завернуть TEK. Раньше KEK был нужен только для разворота
	// секретов учёток и поле было опциональным.
	ring, err := iam.LoadKeyRing(cfg.IAM.KEKFile, cfg.IAM.KEKVersion)
	if err != nil {
		return err
	}

	vault := keys.New(pool, ring)
	be := backend.New(pool, land, cache, cfg, vault)

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

	// Учётки из Postgres включены, когда задан iam.kek_file: без кольца KEK
	// нечем разворачивать secret_wrapped, а «тихо работать без учёток» —
	// это отказ, который не видно. Пока поля нет, живёт прежний s3.iam_dir
	// (или режим одной учётки), поэтому переход в продакшене — один конфиг.
	if cfg.IAM.KEKFile != "" {
		svc, err := iam.New(pool, ring, iam.Options{RootAccess: cfg.Root.Access})
		if err != nil {
			return err
		}
		// Кэш обязателен: GetUserAccount вызывается на каждом запросе (ADR-0016).
		// Start не возвращает управление, пока подписка на уведомления не
		// поднята, — иначе первый же запрос мог бы закешировать учётку, про
		// изменение которой мы уже никогда не узнаем.
		cache, err := iam.NewCache(svc, iam.CacheOptions{
			TTL: cfg.IAM.CacheTTL,
			Max: cfg.IAM.CacheMaxEntries,
			DSN: cfg.DB.DSN,
		})
		if err != nil {
			return err
		}
		if err := cache.Start(ctx); err != nil {
			return err
		}
		// Поле обходит все прочие IAM-бэкенды versitygw, включая iam_dir
		// (embedgw.go:199), поэтому рядом оно уже не используется.
		gwCfg.IAMService = cache
		logger.Info("iam: учётки из Postgres",
			slog.String("kek_file", cfg.IAM.KEKFile),
			slog.Int("kek_version", cfg.IAM.KEKVersion),
			slog.Duration("cache_ttl", cfg.IAM.CacheTTL))
	}

	// Воркеры архивации. Включённость читаем явно: «выключено» должно быть
	// слышно (гремучая строка в логе + отметка в /health/ready ниже), а
	// «включено и некому» — валить процесс на старте (worker.PlanMigrators).
	var mig *worker.MigratorGroup
	if cfg.Migrator.EnabledValue() {
		targets, err := worker.PlanMigrators(cfg)
		if err != nil {
			return err
		}
		mig, err = worker.StartMigrators(ctx, worker.MigratorDeps{
			Cfg: cfg, Meta: pool, Spool: land, Log: logger,
		}, targets)
		if err != nil {
			return err
		}
	} else {
		logger.Warn("АРХИВАЦИЯ ВЫКЛЮЧЕНА: migrator.enabled=false — объекты не поедут на ленту, " +
			"landing будет расти до watermark")
	}

	// Воркеры восстановления (S5). Выключенность видна так же громко, как у
	// архивации: очередь restore без читателя — это вечное
	// «ongoing-request="true"» для клиента.
	var rec *worker.RecallerGroup
	if cfg.Recaller.EnabledValue() {
		targets, err := worker.PlanRecallers(cfg)
		if err != nil {
			return err
		}
		rec, err = worker.StartRecallers(ctx, worker.RecallerDeps{
			Cfg: cfg, Meta: pool, Cache: cache, Keys: vault, Log: logger,
		}, targets)
		if err != nil {
			return err
		}
	} else {
		logger.Warn("ВОССТАНОВЛЕНИЕ ВЫКЛЮЧЕНО: recaller.enabled=false — запросы restore " +
			"не будут читаться с ленты, очередь будет расти")
	}

	// Вытеснитель дисковых копий. Выключен так же виден наружу, как выключенная
	// архивация: без него landing растёт и после archived — молча. Сюда же
	// входит вытеснение истёкших restore из cache (S5).
	var jan *worker.JanitorGroup
	if cfg.Janitor.EnabledValue() {
		jan = worker.StartJanitor(ctx, worker.JanitorDeps{
			Cfg: cfg, Meta: pool, Spool: land, Cache: cache, Log: logger,
		})
	} else {
		logger.Warn("ВЫТЕСНЕНИЕ ВЫКЛЮЧЕНО: janitor.enabled=false — файлы landing не удалятся " +
			"после online_until, диск будет расти до watermark")
	}
	// Удалитель (M6): снимает с диска и с ленты версии, ушедшие в 'deleting'.
	// Выключен виден так же громко, как выключенная архивация: удалённые
	// версии продолжали бы занимать диск и ленту, и снаружи это не видно.
	var del *worker.DeleterGroup
	if cfg.Deleter.EnabledValue() {
		del = worker.StartDeleter(ctx, worker.DeleterDeps{
			Cfg: cfg, Meta: pool, Spool: land, Cache: cache, Log: logger,
		})
	} else {
		logger.Warn("УДАЛЕНИЕ ВЫКЛЮЧЕНО: deleter.enabled=false — удалённые версии останутся " +
			"на диске и на ленте, очередь deleting будет расти")
	}
	if cfg.Metrics.Listen != "" {
		rd, err := worker.StartReadiness(cfg.Metrics.Listen, cfg.Migrator.EnabledValue(), logger)
		if err != nil {
			return fmt.Errorf("readiness: %w", err)
		}
		defer rd.Close()
	}

	logger.Info("starting s3 gateway",
		slog.Any("listen", cfg.S3.Listen),
		slog.String("region", cfg.S3.Region),
		slog.String("backend", be.String()),
		slog.String("phobos_family", cfg.Phobos.Family))

	if err := embedgw.RunVersityGW(ctx, be, gwCfg); err != nil &&
		!errors.Is(err, context.Canceled) {
		return fmt.Errorf("gateway: %w", err)
	}
	// Шлюз встал на уши (ctx отменён) — воркеры ещё могут довозить начатую
	// пачку: их отправка живёт на контексте без отмены. Ждём, но не вечно.
	if mig != nil {
		mig.Wait(cfg.Migrator.ShutdownGrace)
	}
	if rec != nil {
		rec.Wait(cfg.Recaller.ShutdownGrace)
	}
	if jan != nil {
		jan.Wait(cfg.Janitor.ShutdownGrace)
	}
	if del != nil {
		del.Wait(cfg.Deleter.ShutdownGrace)
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
