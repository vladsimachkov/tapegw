// tapegw-iamctl — управление учётками IAM из командной строки.
//
// Зачем: админ-API появится только в T4, а учётки нужны уже сейчас — и в
// стендах CI, и для ручного ввода первого ключа. Секрет при этом обязан
// заворачиваться тем же кольцом KEK, что и у tapegw, поэтому сидинг нельзя
// сделать из shell: формат обёртки (AES-256-GCM, AAD = access_key) знает только
// пакет internal/iam.
//
// Утилита берёт тот же конфиг, что и tapegw: DSN и iam.kek_file задаются в одном
// месте, и рассинхрон «завели ключ не тем KEK» становится невозможен.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/versity/versitygw/auth"

	"github.com/vladsimachkov/tapegw/internal/config"
	"github.com/vladsimachkov/tapegw/internal/iam"
	"github.com/vladsimachkov/tapegw/internal/meta"
)

func main() {
	cfgPath := flag.String("config", "", "путь к tapegw.yaml (оттуда же DSN и iam.kek_file)")
	cmd := flag.String("cmd", "create", "create | revoke | list")
	access := flag.String("access", "", "access key (create, revoke)")
	secret := flag.String("secret", "", "секрет в открытом виде; если не задан — читается со stdin. "+
		"Флаг — только для тестов: аргументы видны в ps и истории оболочки")
	role := flag.String("role", "user", "роль: user | userplus | admin (create)")
	tenant := flag.Int64("tenant", 0, "тенант учётки (create)")
	flag.Parse()

	if *cfgPath == "" {
		fatal("-config обязателен")
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fatal("конфиг: %v", err)
	}
	if cfg.IAM.KEKFile == "" {
		fatal("в конфиге не задан iam.kek_file — учётки из Postgres выключены")
	}

	ctx := context.Background()
	m, err := meta.Connect(ctx, meta.Options{
		DSN:            cfg.DB.DSN,
		MaxConns:       cfg.DB.MaxConns,
		ConnectTimeout: cfg.DB.ConnectTimeout,
	})
	if err != nil {
		fatal("база: %v", err)
	}
	defer m.Close()

	ring, err := iam.LoadKeyRing(cfg.IAM.KEKFile, cfg.IAM.KEKVersion)
	if err != nil {
		fatal("кольцо KEK: %v", err)
	}
	svc, err := iam.New(m, ring, iam.Options{RootAccess: cfg.Root.Access})
	if err != nil {
		fatal("iam: %v", err)
	}

	switch *cmd {
	case "create":
		if *access == "" || *tenant == 0 {
			fatal("для create нужны -access и -tenant")
		}
		// Секрет из stdin, а не из флага: аргументы командной строки видны в
		// `ps` и в истории оболочки, а в бою этот ключ даёт доступ ко всему
		// тенанту. Флаг -secret остаётся только для тестов, где «секрет» —
		// заведомо не настоящий.
		sec := *secret
		if sec == "" {
			b, err := io.ReadAll(os.Stdin)
			if err != nil {
				fatal("прочитать секрет из stdin: %v", err)
			}
			sec = strings.TrimSpace(string(b))
			if sec == "" {
				fatal("секрет пуст: передайте его на stdin или флагом -secret (только для тестов)")
			}
		}
		err = svc.CreateAccount(auth.Account{
			Access: *access, Secret: sec, Role: auth.Role(*role), ProjectID: int(*tenant),
		})
		if err != nil {
			fatal("создать %q: %v", *access, err)
		}
		fmt.Printf("создана учётка %s (роль %s, тенант %d)\n", *access, *role, *tenant)
	case "revoke":
		if *access == "" {
			fatal("для revoke нужен -access")
		}
		if err := svc.DeleteUserAccount(*access); err != nil {
			fatal("отозвать %q: %v", *access, err)
		}
		fmt.Printf("отозвана учётка %s\n", *access)
	case "list":
		list, err := svc.ListUserAccounts()
		if err != nil {
			fatal("список: %v", err)
		}
		for _, a := range list {
			fmt.Printf("%s\t%s\t%d\n", a.Access, a.Role, a.ProjectID)
		}
	default:
		fatal("неизвестная команда %q", *cmd)
	}
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
