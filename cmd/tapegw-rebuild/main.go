// tapegw-rebuild — восстановление метаданных из архива (ADR-0019).
//
// Сценарий один: база потеряна, остались ленты, KEK и экспорт ключей тенантов.
//
// Источник объектов выбирается флагом -archive:
//
//	dir:PATH    каталог с файлами объектов и соседними <oid>.attrs (JSON)
//
// Каталога достаточно для учений и для проверки DR-процедуры на копии, выгруженной
// с лент. Настоящий источник — Phobos — собирается отдельной реализацией под тегом
// `phobos` (правило 2: обычная сборка и тесты не должны требовать cgo), и
// появится вместе со слоем, который его читает.
//
// Экспорт ключей обязателен: без него объекты тенантов не развернуть, а их
// отсутствие на ленте — не ошибка, а признак крипто-уничтожения.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/vladsimachkov/tapegw/internal/archive"
	"github.com/vladsimachkov/tapegw/internal/config"
	"github.com/vladsimachkov/tapegw/internal/iam"
	"github.com/vladsimachkov/tapegw/internal/keys"
	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/rebuild"
)

func main() {
	cfgPath := flag.String("config", "", "путь к tapegw.yaml")
	arch := flag.String("archive", "", "источник объектов: dir:PATH")
	exportPath := flag.String("export", "", "файл экспорта ключей тенантов")
	dryRun := flag.Bool("dry-run", false, "только отчёт, без записи в базу")
	flag.Parse()

	if *cfgPath == "" || *arch == "" || *exportPath == "" {
		fatal("обязательны -config, -archive и -export")
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fatal("конфиг: %v", err)
	}
	ring, err := iam.LoadKeyRing(cfg.IAM.KEKFile, cfg.IAM.KEKVersion)
	if err != nil {
		fatal("кольцо KEK: %v", err)
	}

	src, err := openArchive(*arch)
	if err != nil {
		fatal("архив: %v", err)
	}

	f, err := os.Open(*exportPath)
	if err != nil {
		fatal("экспорт: %v", err)
	}
	defer f.Close()
	exp, err := keys.ReadExport(f, ring)
	if err != nil {
		fatal("экспорт: %v", err)
	}

	ctx := context.Background()
	m, err := meta.Connect(ctx, meta.Options{
		DSN: cfg.DB.DSN, MaxConns: cfg.DB.MaxConns, ConnectTimeout: cfg.DB.ConnectTimeout,
	})
	if err != nil {
		fatal("база: %v", err)
	}
	defer m.Close()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	rep, err := rebuild.Run(ctx, rebuild.Options{
		Archive: src, Meta: m, Ring: ring, Export: exp, Log: log,
		DryRun: *dryRun,
	})
	if err != nil {
		fatal("восстановление: %v", err)
	}

	fmt.Printf("просканировано: %d, восстановлено: %d, уже было: %d, пропущено: %d\n",
		rep.Scanned, rep.Restored, rep.Existing, len(rep.Skipped))
	if len(rep.BucketsCreated) > 0 {
		fmt.Printf("\nБакеты созданы с настройками по умолчанию — конфигурацию (versioning, Object Lock, ACL, владельца) надо вернуть вручную:\n")
		for _, b := range rep.BucketsCreated {
			fmt.Printf("  %s\n", b)
		}
	}
	if len(rep.Skipped) > 0 {
		fmt.Printf("\nПропущено объектов:\n")
		counts := map[string]int{}
		for _, s := range rep.Skipped {
			counts[string(s.Reason)]++
		}
		for reason, n := range counts {
			fmt.Printf("  %-16s %d\n", reason, n)
		}
		for _, s := range rep.Skipped {
			log.Info("пропущен", "отчёт", s.String())
		}
	}
	if *dryRun {
		fmt.Println("\n-dry-run: в базу ничего не записано")
	}
}

// openArchive — источник по строке вида dir:PATH.
func openArchive(spec string) (archive.Archive, error) {
	kind, path, ok := strings.Cut(spec, ":")
	if !ok {
		return nil, fmt.Errorf("ожидается dir:PATH, получено %q", spec)
	}
	switch kind {
	case "dir":
		if _, err := os.Stat(path); err != nil {
			return nil, err
		}
		return archive.NewFake(path), nil
	case "phobos":
		return nil, fmt.Errorf("источник phobos собирается только с -tags phobos (правило 2)")
	default:
		return nil, fmt.Errorf("неизвестный источник %q", kind)
	}
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
