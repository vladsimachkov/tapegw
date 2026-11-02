// Command klipper-tlc — TLC-демон для Phobos: эмулирует штатный phobos_tlc,
// а перемещения выполняет через Klipper/Moonraker.
//
// Реализация — этапы M7 (протокол, inventory, бэкенды sim и smc) и M8 (бэкенд
// moonraker). Сейчас собран только каркас: разбор аргументов, версия и
// заглушки команд оператора, чтобы контракт CLI был виден с самого начала.
//
// Контракт CLI из docs/ARCHITECTURE.md:
//
//	klipper-tlc inventory list
//	klipper-tlc slot set <n> <label>
//	klipper-tlc slot clear <n>
//	klipper-tlc move <from> <to>
//	klipper-tlc home
//	klipper-tlc reset
//	klipper-tlc verify <slot>     загрузить, прочитать MAM, выгрузить
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
)

var (
	Version   = "dev"
	Build     = "none"
	BuildTime = "unknown"
)

// errNotImplemented отличается от реальной ошибки: для него уместно напомнить,
// что этап M7 ещё не сделан, а не искать баг.
var errNotImplemented = errors.New("не реализовано: этап M7, см. docs/ARCHITECTURE.md")

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "klipper-tlc: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("klipper-tlc", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "usage: klipper-tlc <command> [args]\n\n")
		fmt.Fprintf(fs.Output(), "команды оператора: %v\n\n", commands)
		fs.PrintDefaults()
	}
	showVer := fs.Bool("version", false, "напечатать версию и выйти")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *showVer {
		fmt.Printf("klipper-tlc %s (build %s, %s)\n", Version, Build, BuildTime)
		return nil
	}

	rest := fs.Args()
	if len(rest) == 0 {
		fs.Usage()
		return errors.New("не задана команда")
	}

	cmd := rest[0]
	if !known(cmd) {
		fs.Usage()
		return fmt.Errorf("неизвестная команда %q", cmd)
	}
	return fmt.Errorf("%w: команда %q", errNotImplemented, cmd)
}

// commands — список из контракта. «inventory» и «slot» — двухсловные, поэтому
// проверяем префикс.
var commands = []string{
	"inventory list", "slot set", "slot clear", "move", "home", "reset", "verify",
}

func known(cmd string) bool {
	for _, c := range commands {
		if cmd == c || len(c) > len(cmd) && c[:len(cmd)+1] == cmd+" " {
			return true
		}
	}
	switch cmd {
	case "inventory", "slot":
		return true
	}
	return false
}
