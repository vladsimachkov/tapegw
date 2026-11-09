// Command tapegw-mover — единственный процесс в системе, который вызывает
// libphobos (ADR-0007).
//
// Один процесс = один phobos_init = одна операция Phobos за раз; пулы
// задаются числом процессов. tapegw подключается к mover'у по mTLS и передаёт
// данные в протоколе, путями на диск tapegw mover не оперирует (ADR-0021).
// Ключей у mover нет: он переносит шифротекст и обёрнутые DEK как
// непрозрачные данные.
//
// Без build tag `phobos` бинарь собирается, но на старте честно отказывает:
// заглушка «работаем на fake» в бою была бы молчаливой подменой ленты
// (правило 4).
package main

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/vladsimachkov/tapegw/internal/config"
	"github.com/vladsimachkov/tapegw/internal/mover"
	"github.com/vladsimachkov/tapegw/internal/store"
)

// Заполняется сборкой: -ldflags "-X main.Version=... -X main.Build=..."
var (
	Version   = "dev"
	Build     = "none"
	BuildTime = "unknown"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "tapegw-mover: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("tapegw-mover", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "usage: tapegw-mover [flags]\n\n")
		fs.PrintDefaults()
		fmt.Fprintf(fs.Output(), "\nполная конфигурация — docs/ARCHITECTURE.md, deploy/tapegw.yaml.example\n")
	}
	var (
		cfgPath = fs.String("config", "", "путь к YAML-конфигурации (можно не задавать, всё из TAPEGW_*)")
		showVer = fs.Bool("version", false, "напечатать версию и выйти")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *showVer {
		fmt.Printf("tapegw-mover %s (build %s, %s)\n", Version, Build, BuildTime)
		return nil
	}

	// LoadMover, а не Load: на сервере библиотеки у mover'а свой конфиг
	// (предложение 0012), и требовать в нём root S3 или DSN Postgres — значит
	// плодить фиктивные значения.
	cfg, err := config.LoadMover(*cfgPath)
	if err != nil {
		return err
	}

	logger := newLogger(cfg.Logging)
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Идентичность проверяется до старта: сертификат на драйв B у процесса,
	// настроенного на драйв A, — это «записать на A», уезжающее на B, и
	// клиент по своему ожиданию этого не поймает.
	if err := checkDriveIdentity(cfg.Phobos.Mover.TLS.Cert, cfg.Phobos.Mover.Drive); err != nil {
		return err
	}

	app, err := newApp(ctx, cfg, logger)
	if err != nil {
		return err
	}
	defer app.close(logger)

	go app.srv.Serve()
	go app.expireLoop(ctx, logger)

	logger.Info("mover запущен",
		slog.String("listen", app.srv.Addr()),
		slog.String("drive", cfg.Phobos.Mover.Drive),
		slog.String("role", cfg.Phobos.Mover.Role),
		slog.String("family", cfg.Phobos.Family),
		slog.String("library", cfg.Phobos.Library),
		slog.String("version", Version))

	<-ctx.Done()
	// Остановка — это «перестать принимать новое»: прерывать Phobos на
	// середине нельзя, сервер ждёт текущие операции.
	logger.Info("остановка: ждём текущие операции")
	return app.shutdown()
}

// app — собранный mover: сервер, реестр буфера и хранилище. Отдельный тип,
// чтобы тест поднимал ровно ту же сборку, что и main, а не её пересказ.
type app struct {
	srv  *mover.Server
	reg  *mover.Registry
	st   store.Store
	log  *slog.Logger
	tick time.Duration

	// Server.Close не идемпотентен (закрытый канал), а shutdown вызывается
	// и по штатной остановке, и по defer на аварийном пути.
	once        sync.Once
	shutdownErr error
}

func newApp(ctx context.Context, cfg *config.Config, log *slog.Logger) (*app, error) {
	m := cfg.Phobos.Mover

	st, err := openStore(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("mover: хранилище: %w", err)
	}

	reg, err := mover.NewRegistry(m.StagingDir, int64(m.BufferBytes), m.ResultTTL, m.OpTTL)
	if err != nil {
		st.Close()
		return nil, err
	}

	// Роли клиентов — по роли mover'а: migrator'у пишет планировщик
	// архивации и удаляет deleter (копии живут в архивной библиотеке, удалять
	// их должен тот же mover, что пишет), recaller'у — планировщик
	// восстановления. Пустой список ServerTLS отклоняет сам: «разрешили не
	// разрешать ничего» — не режим.
	var allowed []mover.ClientID
	switch mover.Role(m.Role) {
	case mover.RoleMigrator:
		allowed = []mover.ClientID{mover.ClientScheduler, mover.ClientDeleter}
	case mover.RoleRecaller:
		allowed = []mover.ClientID{mover.ClientRestorer}
	default:
		st.Close()
		return nil, fmt.Errorf("mover: роль %q", m.Role)
	}

	srvTLS, err := mover.ServerTLS(m.TLS.Cert, m.TLS.Key, m.TLS.CA, allowed...)
	if err != nil {
		st.Close()
		return nil, err
	}

	srv, err := mover.NewServer(st, reg, mover.ServerConfig{
		Addr:         m.Listen,
		TLS:          srvTLS,
		Role:         mover.Role(m.Role),
		Version:      Version,
		PhobosVer:    m.PhobosVersion,
		Family:       cfg.Phobos.Family,
		Library:      cfg.Phobos.Library,
		Layout:       cfg.Phobos.Layout,
		LayoutParams: cfg.Phobos.LayoutParams,
		Chunk:        int(m.ChunkSize),
		Log:          log,
		Inactivity:   m.Inactivity,
		OpTimeout:    m.OpTimeout,
	})
	if err != nil {
		st.Close()
		return nil, err
	}
	return &app{srv: srv, reg: reg, st: st, log: log, tick: m.ExpireInterval}, nil
}

// expireLoop подчищает реестр: завершённые — по result_ttl, брошенные — по
// op_ttl. Без этого цикла буфер рос бы всю жизнь процесса: TTL реестр хранит,
// но сам себя по нему не чистит — чистит тот, кто его держит.
func (a *app) expireLoop(ctx context.Context, log *slog.Logger) {
	t := time.NewTicker(a.tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			gone, err := a.reg.Expire(time.Now())
			if err != nil {
				log.Error("mover: чистка реестра", "err", err)
				continue
			}
			if len(gone) > 0 {
				log.Info("mover: реестр подчищен", slog.Int("removed", len(gone)))
			}
		}
	}
}

func (a *app) shutdown() error {
	a.once.Do(func() {
		err := a.srv.Close()
		if cerr := a.st.Close(); cerr != nil && err == nil {
			err = cerr
		}
		a.shutdownErr = err
	})
	return a.shutdownErr
}

func (a *app) close(log *slog.Logger) {
	// Аварийный путь (panic до shutdown) не должен оставлять Phobos без fini:
	// shutdown идемпотентен ровно на столько, на сколько идемпотентен Close.
	if err := a.shutdown(); err != nil {
		log.Error("mover: остановка", "err", err)
	}
}

// checkDriveIdentity — самопроверка: в сертификате слушателя обязан быть
// SAN-URI mover://<drive>, совпадающий с настроенным драйвом. Клиент проверяет
// SAN при подключении, но процесс, поднявшийся с чужим сертификатом, молча
// принял бы запись не на свой драйв.
func checkDriveIdentity(certFile, drive string) error {
	pemB, err := os.ReadFile(certFile)
	if err != nil {
		return fmt.Errorf("mover: чтение сертификата: %w", err)
	}
	block, _ := pem.Decode(pemB)
	if block == nil {
		return errors.New("mover: в файле сертификата нет PEM")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return fmt.Errorf("mover: разбор сертификата: %w", err)
	}
	want := mover.MoverURI(drive)
	for _, u := range cert.URIs {
		if strings.EqualFold(u.String(), want.String()) {
			return nil
		}
	}
	return fmt.Errorf("mover: в сертификате нет SAN-URI %s — сертификат от другого драйва? (%s)",
		want, certFile)
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
