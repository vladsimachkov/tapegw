package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Default() заведомо невалиден сам по себе: ключи root намеренно не задаются
// по умолчанию, иначе бинарь стартует с известными кредшнлами. Проверяем, что
// валидность достигается одной лишь их подстановкой — то есть всё остальное
// в дефолтах согласовано.
// testKEKFile — путь, который проходит Validate. Файл не читается: Validate
// проверяет наличие настройки, а не её содержимое.
const testKEKFile = "/etc/tapegw/kek.json"

func TestDefaultIsValid(t *testing.T) {
	cfg := Default()
	cfg.Root.Access, cfg.Root.Secret = "test-access", "test-secret"
	cfg.IAM.KEKFile = testKEKFile
	if err := cfg.Validate(); err != nil {
		t.Fatalf("конфиг по умолчанию невалиден: %v", err)
	}
}

// Кольцо KEK обязательно с T2: шифрование тела включено всегда (ADR-0014).
// Проверка обязана быть именно на пустом значении, а не «подставить путь по
// умолчанию в Default()» — иначе поле перестало бы быть обязательным.
func TestKEKFileRequired(t *testing.T) {
	cfg := Default()
	cfg.Root.Access, cfg.Root.Secret = "a", "b"
	cfg.IAM.KEKFile = ""
	err := cfg.Validate()
	if err == nil {
		t.Fatal("конфиг без iam.kek_file прошёл проверку")
	}
	if !strings.Contains(err.Error(), "iam.kek_file") {
		t.Errorf("в ошибке нет имени поля: %v", err)
	}
}

func TestDefaultWithoutRootIsInvalid(t *testing.T) {
	if err := Default().Validate(); err == nil {
		t.Fatal("ожидали ошибку: root не задаётся по умолчанию")
	}
}

func TestParseByteSize(t *testing.T) {
	tests := []struct {
		in      string
		want    ByteSize
		wantErr bool
	}{
		{"4096", 4096, false},
		{"1KiB", KiB, false},
		{" 5 MiB ", 5 * MiB, false},
		{"200GiB", 200 * GiB, false},
		{"2TiB", 2 * TiB, false},
		{"10G", 10 * GiB, false},
		{"", 0, true},
		{"12QB", 0, true},
		{"abc", 0, true},
	}
	for _, tt := range tests {
		got, err := ParseByteSize(tt.in)
		if (err != nil) != tt.wantErr {
			t.Errorf("ParseByteSize(%q): err=%v, wantErr=%v", tt.in, err, tt.wantErr)
			continue
		}
		if got != tt.want {
			t.Errorf("ParseByteSize(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestByteSizeString(t *testing.T) {
	for _, s := range []ByteSize{0, 512, 3 * KiB, 7 * MiB, 200 * GiB, 2 * TiB} {
		back, err := ParseByteSize(s.String())
		if err != nil {
			t.Fatalf("round-trip %v: %v", s, err)
		}
		if back != s {
			t.Errorf("round-trip %v -> %q -> %v", s, s.String(), back)
		}
	}
}

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tapegw.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadYAMLOverridesDefaults(t *testing.T) {
	path := writeConfig(t, `
s3:
  listen: [":8080", ":8081"]
  region: ru-central-1
  max_object_size: 1GiB
db:
  dsn: postgres://u@db/td
  max_conns: 7
iam:
  kek_file: /etc/tapegw/kek.json
spool:
  landing: /tmp/landing
  landing_watermark: {high: 90, low: 70}
migrator:
  batch_max_bytes: 3GiB
  batch_max_wait: 90s
phobos:
  family: dir
  max_inflight: 4
`)

	t.Setenv("TAPEGW_ROOT_ACCESS", "a")
	t.Setenv("TAPEGW_ROOT_SECRET", "b")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if got := cfg.S3.Listen; len(got) != 2 || got[0] != ":8080" || got[1] != ":8081" {
		t.Errorf("s3.listen = %v", got)
	}
	if cfg.S3.Region != "ru-central-1" {
		t.Errorf("s3.region = %q", cfg.S3.Region)
	}
	if cfg.DB.MaxConns != 7 {
		t.Errorf("db.max_conns = %d", cfg.DB.MaxConns)
	}
	// Значения, которых нет в файле, должны остаться дефолтными.
	if cfg.Spool.Cache != Default().Spool.Cache {
		t.Errorf("spool.cache затёрт: %q", cfg.Spool.Cache)
	}
	if cfg.Migrator.BatchMaxBytes != 3*GiB {
		t.Errorf("migrator.batch_max_bytes = %v", cfg.Migrator.BatchMaxBytes)
	}
	if cfg.Migrator.BatchMaxWait != 90*time.Second {
		t.Errorf("migrator.batch_max_wait = %v", cfg.Migrator.BatchMaxWait)
	}
	if cfg.Phobos.Family != "dir" || cfg.Phobos.MaxInflight != 4 {
		t.Errorf("phobos = %+v", cfg.Phobos)
	}
}

func TestLoadEnvOverridesYAML(t *testing.T) {
	path := writeConfig(t, "s3:\n  region: from-yaml\n")

	t.Setenv("TAPEGW_ROOT_ACCESS", "a")
	t.Setenv("TAPEGW_ROOT_SECRET", "b")
	t.Setenv("TAPEGW_S3_REGION", "from-env")
	t.Setenv("TAPEGW_S3_LISTEN", ":1, :2")
	t.Setenv("TAPEGW_PHOBOS_MAX_INFLIGHT", "3")
	t.Setenv("TAPEGW_MIGRATOR_BATCH_MAX_BYTES", "12GiB")
	t.Setenv("TAPEGW_S3_MAX_OBJECT_SIZE", "2GiB")
	t.Setenv("TAPEGW_DB_MIGRATE_ON_START", "false")
	t.Setenv("TAPEGW_IAM_KEK_FILE", "/tmp/kek.json")
	t.Setenv("TAPEGW_IAM_KEK_VERSION", "3")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.S3.Region != "from-env" {
		t.Errorf("env не переопределил yaml: region=%q", cfg.S3.Region)
	}
	if len(cfg.S3.Listen) != 2 || cfg.S3.Listen[1] != ":2" {
		t.Errorf("TAPEGW_S3_LISTEN = %v (ожидаем пробелы вокруг запятой сожраны)", cfg.S3.Listen)
	}
	if cfg.Phobos.MaxInflight != 3 {
		t.Errorf("max_inflight = %d", cfg.Phobos.MaxInflight)
	}
	if cfg.Migrator.BatchMaxBytes != 12*GiB {
		t.Errorf("batch_max_bytes = %v", cfg.Migrator.BatchMaxBytes)
	}
	if cfg.MigrateOnStartValue() {
		t.Error("TAPEGW_DB_MIGRATE_ON_START=false не применён")
	}
	// Кольцо KEK настраивается тем же способом, что и остальное (правило 9):
	// номер версии в проде задаётся переменной в юните, а не файлом конфига.
	if cfg.IAM.KEKFile != "/tmp/kek.json" || cfg.IAM.KEKVersion != 3 {
		t.Errorf("iam из env не применён: %+v", cfg.IAM)
	}
}

func TestLoadRequiresRoot(t *testing.T) {
	// Без ключей versitygw не стартует, поэтому это ошибка конфигурации,
	// а не опция.
	_, err := Load(writeConfig(t, "s3:\n  region: r\n"))
	if err == nil {
		t.Fatal("ожидали ошибку про root.access/root.secret")
	}
}

func TestValidateRejectsBadWatermarks(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{"low >= high", func(c *Config) { c.Spool.LandingWatermark = Watermark{High: 80, Low: 80} }},
		{"high > 100", func(c *Config) { c.Spool.LandingWatermark = Watermark{High: 101, Low: 10} }},
		{"headroom за пределами", func(c *Config) { c.Spool.SlowDownHeadroom = 50 }},
		{"bad family", func(c *Config) { c.Phobos.Family = "floppy" }},
		{"max_inflight 0", func(c *Config) { c.Phobos.MaxInflight = 0 }},
		{"пустой алиас в accept_as_glacier", func(c *Config) { c.S3.AcceptAsGlacier = []string{"STANDARD", " "} }},
		{"алиас, который и так glacier", func(c *Config) { c.S3.AcceptAsGlacier = []string{"GLACIER"} }},
		{"bad log format", func(c *Config) { c.Logging.Format = "yaml" }},
		{"нет listen", func(c *Config) { c.S3.Listen = nil }},
		{"max_object_size = 0", func(c *Config) { c.S3.MaxObjectSize = 0 }},
		{"max_object_size больше батча", func(c *Config) {
			c.S3.MaxObjectSize = 500 * GiB
			c.Migrator.BatchMaxBytes = 200 * GiB
		}},
		// Учётки: два источника сразу — это «непонятно, кто настоящий»
		// (embedgw берёт внешний IAMService и молча игнорирует iam_dir).
		{"iam.kek_file вместе с s3.iam_dir", func(c *Config) {
			c.IAM.KEKFile = "/etc/tapegw/kek.json"
			c.S3.IAMDir = "/etc/tapegw/iam"
		}},
		{"iam.kek_version без кольца", func(c *Config) {
			c.IAM.KEKFile = "/etc/tapegw/kek.json"
			c.IAM.KEKVersion = 0
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			cfg.Root.Access, cfg.Root.Secret = "a", "b"
			tt.mutate(cfg)
			if err := cfg.Validate(); err == nil {
				t.Errorf("ожидали ошибку для %q", tt.name)
			}
		})
	}
}

// batch_max_bytes = 0 означает «не ограничивать по байтам», поэтому сравнивать
// max_object_size не с чем и крупный объект законен.
func TestMaxObjectSizeLegalWhenBatchHasNoByteLimit(t *testing.T) {
	cfg := Default()
	cfg.Root.Access, cfg.Root.Secret = "a", "b"
	cfg.IAM.KEKFile = testKEKFile
	cfg.Migrator.BatchMaxBytes = 0
	cfg.S3.MaxObjectSize = 1 * TiB
	if err := cfg.Validate(); err != nil {
		t.Errorf("ошибки быть не должно, получили: %v", err)
	}
}

func TestValidateReportsAllProblemsAtOnce(t *testing.T) {
	cfg := Default()
	cfg.Root.Access = ""
	cfg.Phobos.Family = "floppy"
	cfg.Spool.MPUTTL = 0

	err := cfg.Validate()
	if err == nil {
		t.Fatal("ожидали ошибку")
	}
	// errors.Join склеивает через \n: три проблемы — три строки.
	n := 1
	for _, r := range err.Error() {
		if r == '\n' {
			n++
		}
	}
	if n < 3 {
		t.Errorf("ожидали не меньше 3 проблем, получили %d:\n%v", n, err)
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "nope.yaml")); err == nil {
		t.Fatal("ожидали ошибку для несуществующего файла")
	}
}

func TestLoadEmptyPathIsEnvOnly(t *testing.T) {
	t.Setenv("TAPEGW_ROOT_ACCESS", "acc")
	t.Setenv("TAPEGW_ROOT_SECRET", "sec")
	t.Setenv("TAPEGW_IAM_KEK_FILE", testKEKFile)

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Root.Access != "acc" || cfg.Root.Secret != "sec" {
		t.Errorf("root из окружения не подхвачен: %+v", cfg.Root)
	}
}

func TestStrictYAMLRejectsUnknownFields(t *testing.T) {
	// Опечатка в имени поля должна быть ошибкой запуска. Молча проигнорированное
	// поле — самый противный класс отказов: сервис стартует и работает «не так».
	_, err := Load(writeConfig(t, "s3:\n  regin: typo\n"))
	if err == nil {
		t.Fatal("ожидали ошибку на неизвестном поле regin")
	}
}

func TestIsGlacierClass(t *testing.T) {
	// Пустой заголовок = клиент не выразил намерения = наш класс по умолчанию.
	for _, in := range []string{"", "GLACIER", "glacier", " DEEP_ARCHIVE ", "Deep_Archive"} {
		if !IsGlacierClass(in) {
			t.Errorf("IsGlacierClass(%q) = false", in)
		}
	}
	for _, in := range []string{"STANDARD", "REDUCED_REDUNDANCY", "INTELLIGENT_TIERING", "GLACIER_IR"} {
		if IsGlacierClass(in) {
			t.Errorf("IsGlacierClass(%q) = true, а этот класс надо отвергать или объявлять алиасом", in)
		}
	}
}

// Пустой файл конфига — штатный способ сказать «всё приехало из TAPEGW_*».
// Без обработки io.EOF yaml.Decoder возвращает EOF, и tapegw -config empty.yaml
// падал на старте.
func TestLoadEmptyFileIsNotAnError(t *testing.T) {
	t.Setenv("TAPEGW_ROOT_ACCESS", "acc")
	t.Setenv("TAPEGW_ROOT_SECRET", "sec")
	t.Setenv("TAPEGW_IAM_KEK_FILE", testKEKFile)

	for name, body := range map[string]string{
		"совсем пустой":      "",
		"только комментарии": "# конфигурация из окружения\n",
	} {
		t.Run(name, func(t *testing.T) {
			cfg, err := Load(writeConfig(t, body))
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.S3.Region != Default().S3.Region {
				t.Errorf("дефолты не применились: region=%q", cfg.S3.Region)
			}
		})
	}
}

// Именованные коллекции (уточнение к правилу 9): ключ вместо индекса, потому
// что индекс привязан к порядку — переставишь элементы в YAML, и переменная
// окружения молча начнёт править другую запись.

func moversCfg() *Config {
	c := Default()
	c.Root.Access, c.Root.Secret = "a", "b"
	c.IAM.KEKFile = testKEKFile
	c.Phobos.Movers = map[string]Mover{
		"drive1": {Addr: "lib1:7443", Node: "lib1", Library: "lib-main"},
		"drive2": {Addr: "lib1:7444", Node: "lib1", Library: "lib-main"},
	}
	return c
}

func TestMoversEnvOverridesOneKey(t *testing.T) {
	t.Setenv("TAPEGW_PHOBOS_MOVERS_DRIVE1_ADDR", "lib2:7443")
	cfg := moversCfg()
	if err := applyEnv(cfg); err != nil {
		t.Fatal(err)
	}
	if got := cfg.Phobos.Movers["drive1"].Addr; got != "lib2:7443" {
		t.Errorf("drive1.addr = %q, ждали lib2:7443", got)
	}
	if got := cfg.Phobos.Movers["drive2"].Addr; got != "lib1:7444" {
		t.Errorf("drive2.addr = %q: чужой mover задет", got)
	}
}

func TestMoversEnvRejectsUnknownKey(t *testing.T) {
	// Новый mover через переменную добавить нельзя: он бы «добавился», но не
	// добавился — конфиг его не знает.
	t.Setenv("TAPEGW_PHOBOS_MOVERS_DRIVE9_ADDR", "lib9:7443")
	if err := applyEnv(moversCfg()); err == nil || !strings.Contains(err.Error(), "DRIVE9") {
		t.Fatalf("ошибка про неизвестную переменную не получена: %v", err)
	}
}

func TestMoversWholeAndFieldConflict(t *testing.T) {
	t.Setenv("TAPEGW_PHOBOS_MOVERS", `{"nvme1":{"addr":"a:1","node":"n","library":"l"}}`)
	t.Setenv("TAPEGW_PHOBOS_MOVERS_DRIVE1_ADDR", "lib1:7443")
	if err := applyEnv(moversCfg()); err == nil {
		t.Fatal("задано целиком и полем: угадывать, что главнее, нельзя")
	}
}

func TestMoversWholeCollectionEnv(t *testing.T) {
	t.Setenv("TAPEGW_PHOBOS_MOVERS", `{"nvme1":{"addr":"a:1","node":"n","library":"l"}}`)
	cfg := moversCfg()
	if err := applyEnv(cfg); err != nil {
		t.Fatal(err)
	}
	if _, ok := cfg.Phobos.Movers["nvme1"]; !ok {
		t.Fatalf("коллекция целиком не заменила YAML: %v", cfg.Phobos.Movers)
	}
	if _, ok := cfg.Phobos.Movers["drive1"]; ok {
		t.Error("drive1 остался: замена должна заменять, а не дополнять")
	}
}

func TestMoversRejectsAmbiguousKey(t *testing.T) {
	// drive-1 и drive_1 дали бы одну переменную окружения.
	cfg := moversCfg()
	cfg.Phobos.Movers["drive-1"] = Mover{Addr: "x:1", Node: "n", Library: "l"}
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "drive-1") {
		t.Fatalf("ключ вне шаблона не отклонён: %v", err)
	}
}

func TestMoversRequireFields(t *testing.T) {
	cfg := moversCfg()
	cfg.Phobos.Movers["drive3"] = Mover{}
	err := cfg.Validate()
	for _, want := range []string{"drive3.addr", "drive3.node", "drive3.library"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("про %s не сказано: %v", want, err)
		}
	}
}

// moverValidCfg — конфиг, у которого mover-секция заполнена; ValidateMover
// на нём обязан проходить: тесты ниже меняют по одному полю и ждут отказ
// именно про него.
func moverValidCfg() *Config {
	cfg := Default()
	cfg.Root.Access, cfg.Root.Secret = "a", "b"
	cfg.IAM.KEKFile = testKEKFile
	cfg.Phobos.Library = "dirlib"
	cfg.Phobos.Mover = MoverServer{
		Listen: ":7443", Drive: "drive1", Role: "migrator",
		TLS:            MoverTLS{Cert: "/etc/tapegw/mover.crt", Key: "/etc/tapegw/mover.key", CA: "/etc/tapegw/ca.crt"},
		StagingDir:     "/srv/tapegw/mover",
		BufferBytes:    8 << 30,
		ChunkSize:      4 << 20,
		Inactivity:     5 * time.Minute,
		OpTimeout:      30 * time.Minute,
		ResultTTL:      time.Hour,
		OpTTL:          24 * time.Hour,
		ExpireInterval: time.Minute,
	}
	return cfg
}

func TestValidateMoverDefaultsPass(t *testing.T) {
	if err := moverValidCfg().ValidateMover(); err != nil {
		t.Fatalf("заполненный mover невалиден: %v", err)
	}
}

func TestValidateMoverRequiredFields(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*Config)
		want string
	}{
		{"listen", func(c *Config) { c.Phobos.Mover.Listen = "" }, "listen"},
		{"drive", func(c *Config) { c.Phobos.Mover.Drive = "" }, "drive"},
		{"role", func(c *Config) { c.Phobos.Mover.Role = "watcher" }, "role"},
		{"tls", func(c *Config) { c.Phobos.Mover.TLS.CA = "" }, "tls"},
		{"staging", func(c *Config) { c.Phobos.Mover.StagingDir = "" }, "staging_dir"},
		{"buffer", func(c *Config) { c.Phobos.Mover.BufferBytes = 0 }, "buffer_bytes"},
		{"library", func(c *Config) { c.Phobos.Library = "" }, "phobos.library"},
		// chunk больше потолка кадра данных на проводе — put падал бы
		// молча на первом полном кадре, отказ обязан быть на старте.
		{"chunk больше потолка", func(c *Config) { c.Phobos.Mover.ChunkSize = 8 << 20 }, "chunk_size"},
	}
	for _, tc := range cases {
		cfg := moverValidCfg()
		tc.mut(cfg)
		err := cfg.ValidateMover()
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: отказ не про то: %v", tc.name, err)
		}
	}
}

// op_timeout обязан превышать inactivity: на время записи дедлайны продлеваются
// до op_timeout, и если он меньше ожидания кадра, длинная запись обрывалась бы
// на середине — ровно то, чего каскад touch/touchOp не должен допускать.
func TestValidateMoverOpTimeoutAboveInactivity(t *testing.T) {
	cfg := moverValidCfg()
	cfg.Phobos.Mover.OpTimeout = time.Minute
	cfg.Phobos.Mover.Inactivity = 5 * time.Minute
	err := cfg.ValidateMover()
	if err == nil || !strings.Contains(err.Error(), "op_timeout") {
		t.Fatalf("op_timeout <= inactivity не отклонён: %v", err)
	}
}

// repl_count обязан быть "1" (ADR-0022): встроенные реплики запрещены.
// Отдельный тест, а не строка в RequiredFields: отказывает не пустое значение,
// а любое другое — включая «оператор задал свою карту layout_params и потерял
// repl_count», и про каждый случай надо сказать своим текстом.
func TestValidateMoverRequiresReplCountOne(t *testing.T) {
	for _, tc := range []struct {
		name string
		rc   map[string]string
	}{
		{"нет ключа", map[string]string{"other": "x"}},
		{"пустая карта", map[string]string{}},
		{"nil", nil},
		{"две реплики", map[string]string{"repl_count": "2"}},
	} {
		cfg := moverValidCfg()
		cfg.Phobos.LayoutParams = tc.rc
		err := cfg.ValidateMover()
		if err == nil || !strings.Contains(err.Error(), "repl_count") {
			t.Errorf("%s: отказ не про repl_count: %v", tc.name, err)
		}
	}
	// И наоборот: "1" с посторонним параметром проходит — требовать «ровно
	// одну пару» значило бы мешать оператору добавлять безвредные параметры.
	cfg := moverValidCfg()
	cfg.Phobos.LayoutParams = map[string]string{"repl_count": "1", "stripe_size": "64K"}
	if err := cfg.ValidateMover(); err != nil {
		t.Errorf("repl_count=1 с посторонним параметром отклонён: %v", err)
	}
}

// Дефолты mover'а не должны требовать заполнения секции у tapegw: Validate
// (не ValidateMover) проходит и без mover-секции — иначе конфиг тех, кто mover
// ещё не поднимал, сломался бы.
func TestValidateIgnoresMoverSection(t *testing.T) {
	cfg := Default()
	cfg.Root.Access, cfg.Root.Secret = "a", "b"
	cfg.IAM.KEKFile = testKEKFile
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate не должен требовать mover-секцию: %v", err)
	}
}

// phobos.mover.* — обычная секция (не именованная коллекция): правило 9
// распространяется на неё так же, имя переменной выводится из пути в YAML,
// включая вложенности. Тест держит и скаляр, и размер с суффиксом, и duration,
// и поле во вложенной структуре tls.
func TestLoadEnvOverridesMoverSection(t *testing.T) {
	path := writeConfig(t, "phobos:\n  mover:\n    drive: from-yaml\n")

	t.Setenv("TAPEGW_ROOT_ACCESS", "a")
	t.Setenv("TAPEGW_ROOT_SECRET", "b")
	t.Setenv("TAPEGW_IAM_KEK_FILE", testKEKFile)
	t.Setenv("TAPEGW_PHOBOS_MOVER_DRIVE", "from-env")
	t.Setenv("TAPEGW_PHOBOS_MOVER_BUFFER_BYTES", "16GiB")
	t.Setenv("TAPEGW_PHOBOS_MOVER_INACTIVITY_TIMEOUT", "90s")
	t.Setenv("TAPEGW_PHOBOS_MOVER_TLS_CA", "/etc/tapegw/ca-env.crt")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Phobos.Mover.Drive != "from-env" {
		t.Errorf("drive = %q, ждали из env", cfg.Phobos.Mover.Drive)
	}
	if cfg.Phobos.Mover.BufferBytes != 16*GiB {
		t.Errorf("buffer_bytes = %v", cfg.Phobos.Mover.BufferBytes)
	}
	if cfg.Phobos.Mover.Inactivity != 90*time.Second {
		t.Errorf("inactivity_timeout = %v", cfg.Phobos.Mover.Inactivity)
	}
	if cfg.Phobos.Mover.TLS.CA != "/etc/tapegw/ca-env.crt" {
		t.Errorf("tls.ca = %q, вложенное поле из env не применено", cfg.Phobos.Mover.TLS.CA)
	}
}

// --- S2b: роль mover'а, сертификат планировщика, включение воркера ---

// setRole — роль в элементе map: присвоить полю значения из map нельзя.
func setRole(cfg *Config, key, role string) {
	m := cfg.Phobos.Movers[key]
	m.Role = role
	cfg.Phobos.Movers[key] = m
}

func TestMoverRoleValidated(t *testing.T) {
	// Пустая роль — ошибка: воркер выбирает mover'а по роли, «не указано» не
	// означает «подойдёт любой».
	cfg := moversCfg()
	err := cfg.Validate()
	for _, want := range []string{"drive1.role", "drive2.role"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("про %s не сказано: %v", want, err)
		}
	}

	cfg = moversCfg()
	setRole(cfg, "drive1", "migrator")
	setRole(cfg, "drive2", "scheduler")
	cfg.Phobos.MoverClient = MoverTLS{Cert: "c", Key: "k", CA: "ca"}
	err = cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), `drive2.role: "scheduler"`) {
		t.Errorf("неизвестная роль не отклонена: %v", err)
	}
}

func TestMoverClientRequiredForMigrator(t *testing.T) {
	cfg := moversCfg()
	setRole(cfg, "drive1", "migrator")
	setRole(cfg, "drive2", "recaller")
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "mover_client") {
		t.Errorf("mover роли migrator без сертификата планировщика не отклонён: %v", err)
	}
	cfg.Phobos.MoverClient = MoverTLS{Cert: "c", Key: "k", CA: "ca"}
	if err := cfg.Validate(); err != nil {
		t.Errorf("заполненный mover_client, а конфиг невалиден: %v", err)
	}

	// Только recaller-ы — планировщик архивации не подключается, сертификат
	// не нужен: требовать его значило бы ломать конфиг тем, кто архивацию
	// ещё не поднимал.
	cfg2 := moversCfg()
	setRole(cfg2, "drive1", "recaller")
	setRole(cfg2, "drive2", "recaller")
	if err := cfg2.Validate(); err != nil {
		t.Errorf("пул только с recaller не должен требовать mover_client: %v", err)
	}
}

func TestWorkerEnabledValue(t *testing.T) {
	// nil = включён: молчание в конфиге не должно выключать архивацию.
	var w Worker
	if !w.EnabledValue() {
		t.Error("пустой Worker обязан быть включённым по умолчанию")
	}
	off, on := false, true
	w.Enabled = &off
	if w.EnabledValue() {
		t.Error("enabled=false не учтён")
	}
	w.Enabled = &on
	if !w.EnabledValue() {
		t.Error("enabled=true не учтён")
	}
}

func TestWorkerSettingsEnvOverrides(t *testing.T) {
	// Правило 9: каждое поле переопределяется TAPEGW_* по пути в YAML,
	// включая *bool и поля вложенного mover_client.
	t.Setenv("TAPEGW_MIGRATOR_ENABLED", "false")
	t.Setenv("TAPEGW_MIGRATOR_CLAIM_TTL", "90s")
	t.Setenv("TAPEGW_MIGRATOR_SHUTDOWN_GRACE", "2m")
	t.Setenv("TAPEGW_PHOBOS_MOVERS_DRIVE1_ROLE", "migrator")
	t.Setenv("TAPEGW_PHOBOS_MOVER_CLIENT_CERT", "/etc/tapegw/sched.crt")
	cfg := moversCfg()
	if err := applyEnv(cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Migrator.EnabledValue() {
		t.Error("TAPEGW_MIGRATOR_ENABLED=false не применён")
	}
	if cfg.Migrator.ClaimTTL != 90*time.Second {
		t.Errorf("claim_ttl = %v", cfg.Migrator.ClaimTTL)
	}
	if cfg.Migrator.ShutdownGrace != 2*time.Minute {
		t.Errorf("shutdown_grace = %v", cfg.Migrator.ShutdownGrace)
	}
	if cfg.Phobos.Movers["drive1"].Role != "migrator" {
		t.Errorf("role = %q, из env не применён", cfg.Phobos.Movers["drive1"].Role)
	}
	if cfg.Phobos.MoverClient.Cert != "/etc/tapegw/sched.crt" {
		t.Errorf("mover_client.cert = %q", cfg.Phobos.MoverClient.Cert)
	}
	if cfg.Recaller.EnabledValue() != true {
		t.Error("чужой воркер задет")
	}
}

// --- S3: janitor ---

func TestJanitorEnvOverrides(t *testing.T) {
	t.Setenv("TAPEGW_JANITOR_ENABLED", "false")
	t.Setenv("TAPEGW_JANITOR_INTERVAL", "30s")
	t.Setenv("TAPEGW_JANITOR_BATCH_MAX_OBJECTS", "50")
	t.Setenv("TAPEGW_JANITOR_SHUTDOWN_GRACE", "1m")
	cfg := Default()
	cfg.Root.Access, cfg.Root.Secret = "a", "b"
	cfg.IAM.KEKFile = testKEKFile
	if err := applyEnv(cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Janitor.EnabledValue() {
		t.Error("TAPEGW_JANITOR_ENABLED=false не применён")
	}
	if cfg.Janitor.Interval != 30*time.Second {
		t.Errorf("interval = %v", cfg.Janitor.Interval)
	}
	if cfg.Janitor.BatchMaxObjects != 50 {
		t.Errorf("batch_max_objects = %d", cfg.Janitor.BatchMaxObjects)
	}
	if cfg.Janitor.ShutdownGrace != time.Minute {
		t.Errorf("shutdown_grace = %v", cfg.Janitor.ShutdownGrace)
	}
}

func TestJanitorValidate(t *testing.T) {
	for name, mut := range map[string]func(*Config){
		"interval":      func(c *Config) { c.Janitor.Interval = 0 },
		"batch":         func(c *Config) { c.Janitor.BatchMaxObjects = 0 },
		"grace":         func(c *Config) { c.Janitor.ShutdownGrace = 0 },
		"отрицательный": func(c *Config) { c.Janitor.Interval = -time.Second },
	} {
		cfg := Default()
		cfg.Root.Access, cfg.Root.Secret = "a", "b"
		cfg.IAM.KEKFile = testKEKFile
		mut(cfg)
		if err := cfg.Validate(); err == nil {
			t.Errorf("%s: ноль/минус прошёл валидацию", name)
		}
	}
}

// Поколение закрыто для записи только по явному writable: false (слой 2 Р5):
// отсутствие записи в конфиге не закрывает поколение молча.
func TestGenerationWritable(t *testing.T) {
	f := false
	tr := true
	p := Phobos{Generations: map[string]Generation{
		"LTO-7": {Writable: &f},
		"LTO-8": {Writable: &tr},
		"LTO-9": {},
	}}
	if !p.GenerationWritable("LTO-8") {
		t.Error("LTO-8 с writable: true закрыт")
	}
	if !p.GenerationWritable("LTO-9") {
		t.Error("LTO-9 без writable закрыт — закрытие должно быть явным")
	}
	if !p.GenerationWritable("LTO-10") {
		t.Error("неизвестное поколение закрыто — по умолчанию открыто")
	}
	if p.GenerationWritable("LTO-7") {
		t.Error("LTO-7 с writable: false открыт")
	}
}

// pack.threshold — настройка из правила 9: дефолт, YAML, переменная окружения
// и отказ на нуле. Ноль означал бы «каждый объект — отдельный файл на ленте»,
// то есть ровно раздутый индекс LTFS, ради которого пачки и заводили.
func TestPackThreshold(t *testing.T) {
	if Default().Pack.Threshold != 25*MiB {
		t.Errorf("по умолчанию %v, ждали 25MiB (значение из договора про RPO)", Default().Pack.Threshold)
	}

	path := writeConfig(t, "pack:\n  threshold: 1MiB\n")
	t.Setenv("TAPEGW_ROOT_ACCESS", "a")
	t.Setenv("TAPEGW_ROOT_SECRET", "b")
	t.Setenv("TAPEGW_IAM_KEK_FILE", "/tmp/kek.json")
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Pack.Threshold != 1*MiB {
		t.Errorf("yaml: threshold = %v, ждали 1MiB", cfg.Pack.Threshold)
	}

	t.Setenv("TAPEGW_PACK_THRESHOLD", "64MiB")
	cfg, err = Load(path)
	if err != nil {
		t.Fatalf("Load с env: %v", err)
	}
	if cfg.Pack.Threshold != 64*MiB {
		t.Errorf("TAPEGW_PACK_THRESHOLD не применён: %v", cfg.Pack.Threshold)
	}

	bad := Default()
	bad.Pack.Threshold = 0
	if err := bad.Validate(); err == nil || !strings.Contains(err.Error(), "pack.threshold") {
		t.Errorf("нулевой порог принят: %v", err)
	}
}

// Политика копий пула (Ф1.1): парсинг из YAML.
func TestLoadPoolsPolicy(t *testing.T) {
	t.Setenv("TAPEGW_ROOT_ACCESS", "a")
	t.Setenv("TAPEGW_ROOT_SECRET", "b")
	t.Setenv("TAPEGW_IAM_KEK_FILE", testKEKFile)
	path := writeConfig(t, `
pools:
  shared:
    copies:
      - name: left
        tags: [side:left]
      - name: right
        tags: [side:right, vault]
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	pw, ok := cfg.Pools["shared"]
	if !ok {
		t.Fatalf("pools.shared не распарсился: %+v", cfg.Pools)
	}
	if len(pw.Copies) != 2 {
		t.Fatalf("копий %d, ожидали 2: %+v", len(pw.Copies), pw.Copies)
	}
	if pw.Copies[0].Name != "left" || strings.Join(pw.Copies[0].Tags, ",") != "side:left" {
		t.Errorf("копия 1 = %+v", pw.Copies[0])
	}
	if pw.Copies[1].Name != "right" || strings.Join(pw.Copies[1].Tags, ",") != "side:right,vault" {
		t.Errorf("копия 2 = %+v", pw.Copies[1])
	}
}

// Кривая политика отсекается на старте: обнаруженная в момент записи, она
// означает уже испорченную очередь, а не ошибку конфигурации.
func TestValidatePoolsPolicy(t *testing.T) {
	tests := []struct {
		name   string
		copies []CopySpec
		want   string // подстрока ожидаемой ошибки; пусто = политика валидна
	}{
		{"пустой список", nil, "нужна хотя бы одна спецификация"},
		{"пустое имя", []CopySpec{{Name: " ", Tags: []string{"a:1"}}}, "пустое имя"},
		{
			"дубль имени",
			[]CopySpec{{Name: "x", Tags: []string{"a:1"}}, {Name: "x", Tags: []string{"a:2"}}},
			"имя \"x\" повторяется",
		},
		{
			"пустой тег",
			[]CopySpec{{Name: "x", Tags: []string{"a:1", " "}}},
			"пустой тег",
		},
		{
			"одинаковые теги",
			[]CopySpec{{Name: "left", Tags: []string{"side:left"}}, {Name: "right", Tags: []string{"side:left"}}},
			"с одинаковыми тегами",
		},
		{
			"одинаковые наборы в разном порядке",
			[]CopySpec{{Name: "a", Tags: []string{"b:2", "a:1"}}, {Name: "b", Tags: []string{"a:1", "b:2"}}},
			"с одинаковыми тегами",
		},
		{
			// Пустой набор — подмножество любого: копии «any» годится
			// любая лента пула, включая ту, куда легла первая копия.
			"пустой набор — подмножество",
			[]CopySpec{{Name: "left", Tags: []string{"side:left"}}, {Name: "any", Tags: nil}},
			"подмножество",
		},
		{
			"строгое подмножество",
			[]CopySpec{{Name: "narrow", Tags: []string{"x:1"}}, {Name: "wide", Tags: []string{"x:1", "y:2"}}},
			"подмножество",
		},
		{
			"подмножество в обратном порядке",
			[]CopySpec{{Name: "wide", Tags: []string{"x:1", "y:2"}}, {Name: "narrow", Tags: []string{"x:1"}}},
			"подмножество",
		},
		{
			"разные наборы проходят",
			[]CopySpec{{Name: "left", Tags: []string{"side:left"}}, {Name: "right", Tags: []string{"side:right"}}},
			"",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			cfg.Root.Access, cfg.Root.Secret = "a", "b"
			cfg.IAM.KEKFile = testKEKFile
			cfg.Pools = map[string]PoolWrite{"shared": {Copies: tt.copies}}
			err := cfg.Validate()
			if tt.want == "" {
				if err != nil {
					t.Fatalf("валидная политика отвергнута: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("ожидали ошибку %q, получили %v", tt.want, err)
			}
		})
	}
}

// Тег пула обязан попасть в каждую копию (ADR-0013), даже если админ уже
// написал его руками, и не продублироваться при этом.
func TestCopySpecsPrependsPoolTag(t *testing.T) {
	pw := PoolWrite{Copies: []CopySpec{
		{Name: "left", Tags: []string{"side:left"}},
		{Name: "manual", Tags: []string{"pool:shared", "side:manual"}},
		{Name: "bare", Tags: nil},
	}}
	specs, tags := pw.CopySpecs("pool:shared")
	if strings.Join(specs, ",") != "left,manual,bare" {
		t.Errorf("порядок копий сломан: %v", specs)
	}
	want := map[string]string{
		"left":   "pool:shared,side:left",
		"manual": "pool:shared,side:manual",
		"bare":   "pool:shared",
	}
	for name, wantTags := range want {
		if got := strings.Join(tags[name], ","); got != wantTags {
			t.Errorf("теги копии %s = %q, ожидали %q", name, got, wantTags)
		}
	}
}
