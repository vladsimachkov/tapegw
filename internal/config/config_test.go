package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Default() заведомо невалиден сам по себе: ключи root намеренно не задаются
// по умолчанию, иначе бинарь стартует с известными кредшнлами. Проверяем, что
// валидность достигается одной лишь их подстановкой — то есть всё остальное
// в дефолтах согласовано.
func TestDefaultIsValid(t *testing.T) {
	cfg := Default()
	cfg.Root.Access, cfg.Root.Secret = "test-access", "test-secret"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("конфиг по умолчанию невалиден: %v", err)
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
