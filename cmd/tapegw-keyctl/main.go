// tapegw-keyctl — ключи тенантов: экспорт, проверка, ротация KEK.
//
// Утилита существует потому, что три операции нельзя сделать из shell: формат
// обёртки (AES-256-GCM, AAD = id тенанта) знает только internal/keys, а решение
// «эту версию KEK уже можно выбросить» требует знать, когда она уступила активной
// и сколько живут экспорты и бэкапы.
//
// Конфиг берётся тот же, что у tapegw: DSN и iam.kek_file в одном месте, и
// «экспортировали не тем кольцом» становится невозможно.
//
// Команды:
//
//	export     -out FILE     выгрузить обёртки TEK живых тенантов
//	verify     -in FILE      проверить файл экспорта тем же кольцом
//	list-kek                 что известно про версии KEK
//	rotate-kek               переобернуть TEK в активную версию KEK
//	forget-kek -version N    можно ли уже выбросить версию N из iam.kek_file
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/vladsimachkov/tapegw/internal/config"
	"github.com/vladsimachkov/tapegw/internal/iam"
	"github.com/vladsimachkov/tapegw/internal/keys"
	"github.com/vladsimachkov/tapegw/internal/meta"
)

func main() {
	cfgPath := flag.String("config", "", "путь к tapegw.yaml")
	cmd := flag.String("cmd", "", "export | verify | list-kek | rotate-kek | forget-kek")
	out := flag.String("out", "", "файл экспорта (export)")
	in := flag.String("in", "", "файл экспорта (verify)")
	version := flag.Int("version", 0, "версия KEK (forget-kek)")
	flag.Parse()

	if *cfgPath == "" {
		fatal("-config обязателен")
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fatal("конфиг: %v", err)
	}
	if cfg.IAM.KEKFile == "" {
		fatal("в конфиге не задан iam.kek_file")
	}
	ring, err := iam.LoadKeyRing(cfg.IAM.KEKFile, cfg.IAM.KEKVersion)
	if err != nil {
		fatal("кольцо KEK: %v", err)
	}

	ctx := context.Background()
	m, err := meta.Connect(ctx, meta.Options{
		DSN: cfg.DB.DSN, MaxConns: cfg.DB.MaxConns, ConnectTimeout: cfg.DB.ConnectTimeout,
	})
	if err != nil {
		fatal("база: %v", err)
	}
	defer m.Close()

	switch *cmd {
	case "export":
		doExport(ctx, m, ring, cfg, *out)
	case "verify":
		doVerify(ctx, ring, *in)
	case "list-kek":
		doListKEK(ctx, m, ring)
	case "rotate-kek":
		doRotate(ctx, m, ring)
	case "forget-kek":
		doForget(ctx, m, ring, cfg, *version)
	default:
		fatal("неизвестная команда %q", *cmd)
	}
}

// doExport выгружает обёртки. Файл обязан быть доступен только владельцу: в нём
// обёртки, а обёртка + KEK = доступ к данным тенанта.
func doExport(ctx context.Context, m *meta.Meta, ring *iam.KeyRing, cfg *config.Config, path string) {
	if path == "" {
		fatal("-out обязателен")
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		fatal("создать %s: %v", path, err)
	}
	defer f.Close()

	exp, err := keys.BuildExport(ctx, m, ring)
	if err != nil {
		fatal("собрать экспорт: %v", err)
	}
	if err := exp.Write(f); err != nil {
		fatal("записать %s: %v", path, err)
	}
	fmt.Printf("экспортировано тенантов: %d (KEK версии %v, активная %d)\n",
		len(exp.Entries), exp.KEKVersions, exp.ActiveKEK)
	// Срок берём из конфига, а не из памяти: именно это число называют
	// клиенту в договоре, и «30 по умолчанию» в выводе должно быть тем же
	// значением, что реально действует в системе.
	fmt.Printf("срок хранения по договору: %v (iam.export_retention), PITR: %v (iam.backup_retention)\n",
		cfg.IAM.ExportRetention, cfg.IAM.BackupRetention)
}

// doVerify проверяет файл. Отдельная команда нужна затем, чтобы экспорт
// проверяли ДО того, как на него начнут полагаться в DR-сценарии: «файл
// создался» не значит «файл читается тем кольцом, которое лежит в хранилище».
func doVerify(ctx context.Context, ring *iam.KeyRing, path string) {
	if path == "" {
		fatal("-in обязателен")
	}
	f, err := os.Open(path)
	if err != nil {
		fatal("открыть %s: %v", path, err)
	}
	defer f.Close()

	exp, err := keys.ReadExport(f, ring)
	if err != nil {
		fatal("проверка: %v", err)
	}
	fmt.Printf("экспорт от %s, тенантов: %d, версии KEK %v (активная %d)\n",
		exp.Created.Format(time.RFC3339), len(exp.Entries), exp.KEKVersions, exp.ActiveKEK)
}

func doListKEK(ctx context.Context, m *meta.Meta, ring *iam.KeyRing) {
	rows, err := m.KEKVersions(ctx)
	if err != nil {
		fatal("%v", err)
	}
	fmt.Printf("активная версия в кольце: %d\n", ring.Active())
	for _, v := range rows {
		state := "активна или срок неизвестен"
		if v.SupersededAt != nil {
			state = fmt.Sprintf("устарела с %s", v.SupersededAt.Format(time.RFC3339))
		}
		fmt.Printf("  KEK %-3d %s\n", v.Version, state)
	}
}

// doRotate переоборачивает TEK активной версией.
//
// Порядок важен: сначала заводим учёт новой версии, потом переоборачиваем, потом
// отмечаем устаревших. Обратный порядок открыл бы окно, в которой TEK уже
// переведён на N+1, а отметки нет — и «когда уступила версия N» снова пришлось бы
// вспоминать.
func doRotate(ctx context.Context, m *meta.Meta, ring *iam.KeyRing) {
	active := ring.Active()
	if err := m.EnsureKEKVersion(ctx, active); err != nil {
		fatal("%v", err)
	}
	v := keys.New(m, ring)
	n, err := v.RotateKEK(ctx)
	if err != nil {
		fatal("переоборачивание: %v", err)
	}
	// Момент уступки ставит сам RotateKEK — на ту версию, с которой реально
	// уехала последняя строка. «Все кроме активной» помечать нельзя: так часы
	// стартуют у версии, которая ещё ни разу не была активной.
	fmt.Printf("переобернуто TEK: %d в версию %d\n", n, active)
	fmt.Println("старую версию KEK НЕ удаляйте из iam.kek_file, пока не проверите: tapegw-keyctl -cmd forget-kek -version <N>")
}

// doForget — единственная защита от «оператор проглядел срок».
//
// Удалить ключ из файла кольцо мы не можем и не пытаемся: это делает оператор, и
// задача команды — не дать ему сделать это раньше времени. Отказываем во всех
// случаях, где срок нельзя ДОКАЗАТЬ: версия активна, про неё нет записи, или
// момент уступки неизвестен (версия досталась из дампа до миграции 0016).
func doForget(ctx context.Context, m *meta.Meta, ring *iam.KeyRing, cfg *config.Config, version int) {
	if version == 0 {
		fatal("-version обязателен")
	}
	if version == ring.Active() {
		fatal("версия %d активна: на неё заворачиваются новые обёртки", version)
	}
	row, err := m.GetKEKVersion(ctx, version)
	if errors.Is(err, meta.ErrKEKVersionUnknown) {
		fatal("про версию %d нет записи: неизвестно, когда она устарела, а значит нельзя доказать, что срок прошёл", version)
	}
	if err != nil {
		fatal("%v", err)
	}

	// Решение принимает keys.CanForget, а не эта функция: в main проверка
	// необозрима тестом, а проверяет она обещание, данное клиенту.
	check := keys.ForgetCheck{
		Version:         version,
		Active:          ring.Active(),
		SupersededAt:    row.SupersededAt,
		ExportRetention: cfg.IAM.ExportRetention,
		BackupRetention: cfg.IAM.BackupRetention,
		Now:             time.Now(),
	}
	if err := keys.CanForget(check); err != nil {
		fatal("%v", err)
	}
	required := cfg.IAM.ExportRetention
	if cfg.IAM.BackupRetention > required {
		required = cfg.IAM.BackupRetention
	}
	fmt.Printf("версию %d можно удалить из iam.kek_file: устарела %s, прошло %v, требовалось %v\n",
		version, row.SupersededAt.Format(time.RFC3339),
		time.Since(*row.SupersededAt).Round(time.Hour), required)
	fmt.Println("Убедитесь, что экспорты и дампы, свёрнутые этой версией, уничтожены — иначе они станут нечитаемыми.")
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
