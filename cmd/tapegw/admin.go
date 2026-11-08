package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/vladsimachkov/tapegw/internal/admin"
	"github.com/vladsimachkov/tapegw/internal/config"
	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/phobosctl"
)

// runAdmin — подкоманды оператора `tapegw admin ...`. Отдельный процесс, а не
// HTTP: регистрация ленты — редкая операция на узле, где стоит Phobos (ей нужен
// CLI phobos и доступ к DSS), а не запрос с клиента. Тот же порядок, что с
// tapegw-iamctl: конфиг общий с шлюзом, БД — тем же кольцом и теми же
// миграциями, что у запущенного процесса.
func runAdmin(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: tapegw admin media add -config <yaml> -label <метка> -generation <поколение> [-pool N] [-medium-type T] [-side left|right]")
	}
	switch args[0] {
	case "media":
		if len(args) < 2 || args[1] != "add" {
			return fmt.Errorf("usage: tapegw admin media add [-config <yaml>] [-label <метка>] [-generation <поколение>]")
		}
		return runAdminMediaAdd(args[2:])
	default:
		return fmt.Errorf("tapegw admin: неизвестная команда %q (есть media add)", args[0])
	}
}

func runAdminMediaAdd(args []string) error {
	fs := flag.NewFlagSet("tapegw admin media add", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "usage: tapegw admin media add -config <yaml> -label <метка> -generation <поколение> [flags]\n\n")
		fs.PrintDefaults()
		fmt.Fprintf(fs.Output(), "\nНоситель регистрируется в Phobos и в реестре tapegw одной операцией;\n")
		fmt.Fprintf(fs.Output(), "повтор команды идемпотентен и доводит прерванную регистрацию до конца.\n")
	}
	var (
		cfgPath    = fs.String("config", "", "путь к YAML-конфигурации tapegw (общий с шлюзом)")
		label      = fs.String("label", "", "метка носителя: путь каталога для dir, имя тома для tape")
		pool       = fs.Int64("pool", int64(meta.DefaultPoolID), "id пула (pools.id)")
		generation = fs.String("generation", "", "поколение: тег gen:<значение> в Phobos и media.generation")
		mediumType = fs.String("medium-type", "", "технология носителя: обязательна для tape (-t в phobos tape add), для dir по умолчанию \"dir\"")
		side       = fs.String("side", "", "сторона: left|right (тег side:<значение>), если библиотека с двумя сторонами")
		adopt      = fs.Bool("adopt", false, "присвоить пул носителю, на котором уже есть данные без тега пула (данные могли быть записаны в обход tapegw)")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	mt := *mediumType
	if mt == "" {
		// Для dir колонка medium_type обязана чем-то быть, но технологии у
		// каталога нет. Значение номинальное — носители dir не выбирают по
		// типу.
		mt = "dir"
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	m, err := meta.Connect(ctx, meta.Options{
		DSN:            cfg.DB.DSN,
		MaxConns:       cfg.DB.MaxConns,
		ConnectTimeout: cfg.DB.ConnectTimeout,
	})
	if err != nil {
		return err
	}
	defer m.Close()

	deps := admin.MediaDeps{
		Meta: m,
		Phobos: &phobosctl.Client{
			Family:  cfg.Phobos.Family,
			Library: cfg.Phobos.Library,
		},
	}
	res, err := admin.MediaAdd(ctx, deps, admin.MediaAddParams{
		Label:      *label,
		Generation: *generation,
		MediumType: mt,
		Side:       *side,
		PoolID:     *pool,
		Adopt:      *adopt,
	})
	for _, e := range res.BindErrors {
		fmt.Fprintf(os.Stderr, "tapegw: не удалось проверить висящую копию: %v\n", e)
	}
	if err != nil {
		return err
	}
	fmt.Printf("носитель %q зарегистрирован: media.id=%d, пул %d, поколение %s, тип %s",
		*label, res.MediaID, *pool, *generation, mt)
	if *side != "" {
		fmt.Printf(", сторона %s", *side)
	}
	fmt.Println()
	if res.BoundCopies > 0 {
		fmt.Printf("привязано копий из 'writing': %d\n", res.BoundCopies)
	}
	if len(res.BindErrors) > 0 {
		// Регистрация состоялась, но доводка копий не закончена: повтор
		// команды идемпотентен и обязан довести её.
		return fmt.Errorf("носитель зарегистрирован (media.id=%d), но %d копий не проверено — повторите команду",
			res.MediaID, len(res.BindErrors))
	}
	return nil
}
