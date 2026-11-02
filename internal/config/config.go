// Package config загружает конфигурацию tapegw из YAML и переопределяет
// отдельные поля переменными окружения TAPEGW_*.
//
// Правило 9 из AGENTS.md: настраиваемые числа (таймауты, размеры батчей,
// watermark) живут здесь, а не константами в коде.
package config

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config — корень конфигурации. Теги yaml задают имя поля в файле, теги env —
// имя переменной окружения (без префикса TAPEGW_).
type Config struct {
	S3       S3      `yaml:"s3"`
	DB       DB      `yaml:"db"`
	Spool    Spool   `yaml:"spool"`
	Phobos   Phobos  `yaml:"phobos"`
	Migrator Worker  `yaml:"migrator"`
	Recaller Worker  `yaml:"recaller"`
	Metrics  Metrics `yaml:"metrics"`
	Admin    Admin   `yaml:"admin"`
	Logging  Logging `yaml:"logging"`
	Root     Root    `yaml:"root"`
	Version  Version `yaml:"-"`
}

// Root — учётная запись администратора S3. versitygw требует непустые access/secret,
// поэтому значения обязательны.
type Root struct {
	Access string `yaml:"access" env:"ROOT_ACCESS"`
	Secret string `yaml:"secret" env:"ROOT_SECRET"`
}

type S3 struct {
	Listen              []string `yaml:"listen" env:"S3_LISTEN"`
	Region              string   `yaml:"region" env:"S3_REGION"`
	TLS                 TLS      `yaml:"tls"`
	IAMDir              string   `yaml:"iam_dir" env:"S3_IAM_DIR"`
	DefaultStorageClass string   `yaml:"default_storage_class" env:"S3_DEFAULT_STORAGE_CLASS"`
	// HealthPath — путь health-эндпоинта на S3-порту. Пусто — эндпоинт выключен.
	HealthPath string `yaml:"health_path" env:"S3_HEALTH_PATH"`
	// MaxConnections и MaxRequests прокидываются в лимитер versitygw.
	MaxConnections    int `yaml:"max_connections" env:"S3_MAX_CONNECTIONS"`
	MaxRequests       int `yaml:"max_requests" env:"S3_MAX_REQUESTS"`
	MultipartMaxParts int `yaml:"multipart_max_parts" env:"S3_MULTIPART_MAX_PARTS"`
}

type TLS struct {
	Cert string `yaml:"cert" env:"S3_TLS_CERT"`
	Key  string `yaml:"key" env:"S3_TLS_KEY"`
}

type DB struct {
	DSN            string        `yaml:"dsn" env:"DB_DSN"`
	MaxConns       int32         `yaml:"max_conns" env:"DB_MAX_CONNS"`
	ConnectTimeout time.Duration `yaml:"connect_timeout" env:"DB_CONNECT_TIMEOUT"`
	// MigrateOnStart — применять миграции при старте. По умолчанию да.
	MigrateOnStart *bool `yaml:"migrate_on_start" env:"DB_MIGRATE_ON_START"`
}

type Spool struct {
	Landing          string        `yaml:"landing" env:"SPOOL_LANDING"`
	Cache            string        `yaml:"cache" env:"SPOOL_CACHE"`
	LandingWatermark Watermark     `yaml:"landing_watermark"`
	CacheWatermark   Watermark     `yaml:"cache_watermark"`
	MPUTTL           time.Duration `yaml:"mpu_ttl" env:"SPOOL_MPU_TTL"`
	OrphanGrace      time.Duration `yaml:"orphan_grace" env:"SPOOL_ORPHAN_GRACE"`
	// SlowDownHeadroom — на сколько процентов выше high watermark ещё можно
	// принимать PUT. См. ADR про SlowDown.
	SlowDownHeadroom int `yaml:"slowdown_headroom" env:"SPOOL_SLOWDOWN_HEADROOM"`
}

// Watermark — пороги вытеснения в процентах занятого объёма тома.
type Watermark struct {
	High int `yaml:"high"`
	Low  int `yaml:"low"`
}

type Phobos struct {
	Family       string            `yaml:"family" env:"PHOBOS_FAMILY"`
	Layout       string            `yaml:"layout" env:"PHOBOS_LAYOUT"`
	LayoutParams map[string]string `yaml:"layout_params"`
	Tags         []string          `yaml:"tags"`
	// MaxInflight — размер пула горутин, имеющих право вызывать libphobos.
	// До подтверждения потокобезопасности (спайк M0-3) держим 1.
	MaxInflight int `yaml:"max_inflight" env:"PHOBOS_MAX_INFLIGHT"`
}

// Worker — настройки одного фоновыего воркера (migrator или recaller).
type Worker struct {
	BatchMaxObjects int           `yaml:"batch_max_objects"`
	BatchMaxBytes   ByteSize      `yaml:"batch_max_bytes"`
	BatchMaxWait    time.Duration `yaml:"batch_max_wait"`
	MaxAttempts     int           `yaml:"max_attempts"`
}

type Metrics struct {
	Listen string `yaml:"listen" env:"METRICS_LISTEN"`
}

type Admin struct {
	Listen     string `yaml:"listen" env:"ADMIN_LISTEN"`
	PathPrefix string `yaml:"path_prefix" env:"ADMIN_PATH_PREFIX"`
}

type Logging struct {
	Level  string `yaml:"level" env:"LOG_LEVEL"`
	Format string `yaml:"format" env:"LOG_FORMAT"`
}

// Version — заполняется сборкой через -ldflags, не из конфига.
type Version struct {
	Version string
	Build   string
}

// ByteSize — размер в байтах, принимающий суффиксы KiB/MiB/GiB/TiB.
type ByteSize int64

func (b *ByteSize) UnmarshalYAML(n *yaml.Node) error {
	var raw string
	if err := n.Decode(&raw); err != nil {
		return err
	}
	v, err := ParseByteSize(raw)
	if err != nil {
		return err
	}
	*b = v
	return nil
}

func (b ByteSize) String() string {
	switch {
	case b == 0:
		return "0"
	case b%TiB == 0:
		return fmt.Sprintf("%dTiB", b/TiB)
	case b%GiB == 0:
		return fmt.Sprintf("%dGiB", b/GiB)
	case b%MiB == 0:
		return fmt.Sprintf("%dMiB", b/MiB)
	case b%KiB == 0:
		return fmt.Sprintf("%dKiB", b/KiB)
	default:
		return strconv.FormatInt(int64(b), 10)
	}
}

const (
	KiB ByteSize = 1 << (10 * 1)
	MiB ByteSize = 1 << (10 * 2)
	GiB ByteSize = 1 << (10 * 3)
	TiB ByteSize = 1 << (10 * 4)
)

// ParseByteSize разбирает "200GiB", "500 MB", "4096".
func ParseByteSize(s string) (ByteSize, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, errors.New("empty size")
	}
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	if i == 0 {
		return 0, fmt.Errorf("size %q: expected a number with optional unit", s)
	}
	n, err := strconv.ParseInt(s[:i], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("size %q: %w", s, err)
	}
	unit := strings.TrimSpace(s[i:])
	switch strings.ToLower(unit) {
	case "":
		return ByteSize(n), nil
	case "kib", "k":
		return ByteSize(n) * KiB, nil
	case "mib", "m":
		return ByteSize(n) * MiB, nil
	case "gib", "g":
		return ByteSize(n) * GiB, nil
	case "tib", "t":
		return ByteSize(n) * TiB, nil
	default:
		return 0, fmt.Errorf("size %q: unknown unit %q (use KiB, MiB, GiB, TiB)", s, unit)
	}
}

// Default возвращает конфигурацию со стартовыми значениями из
// docs/ARCHITECTURE.md. Числа подобраны на стенде, это не требования.
func Default() *Config {
	yes := true
	return &Config{
		S3: S3{
			Listen:              []string{":7070"},
			Region:              "us-east-1",
			DefaultStorageClass: "GLACIER",
			HealthPath:          "/health",
			MaxConnections:      50000,
			MaxRequests:         50000,
			MultipartMaxParts:   10000,
		},
		DB: DB{
			DSN:            "postgres://tapegw@localhost/phobos?search_path=tapegw",
			MaxConns:       20,
			ConnectTimeout: 10 * time.Second,
			MigrateOnStart: &yes,
		},
		Spool: Spool{
			Landing:          "/srv/tapegw/landing",
			Cache:            "/srv/tapegw/cache",
			LandingWatermark: Watermark{High: 80, Low: 60},
			CacheWatermark:   Watermark{High: 85, Low: 70},
			MPUTTL:           168 * time.Hour,
			OrphanGrace:      24 * time.Hour,
			SlowDownHeadroom: 5,
		},
		Phobos: Phobos{
			Family:       "tape",
			Layout:       "raid1",
			LayoutParams: map[string]string{"repl_count": "1"},
			MaxInflight:  1,
		},
		Migrator: Worker{
			BatchMaxObjects: 1000,
			BatchMaxBytes:   200 * GiB,
			BatchMaxWait:    15 * time.Minute,
			MaxAttempts:     10,
		},
		Recaller: Worker{
			BatchMaxObjects: 500,
			BatchMaxBytes:   0,
			BatchMaxWait:    5 * time.Minute,
			MaxAttempts:     10,
		},
		Metrics: Metrics{Listen: ":9464"},
		Admin:   Admin{PathPrefix: "/admin"},
		Logging: Logging{Level: "info", Format: "json"},
	}
}

// Load читает файл конфигурации поверх значений по умолчанию, затем применяет
// переменные окружения TAPEGW_* и проверяет результат.
//
// Пустой path допустим: конфигурация целиком может приехать из окружения.
func Load(path string) (*Config, error) {
	cfg := Default()

	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read config: %w", err)
		}
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("parse config %v: %w", path, err)
		}
	}

	if err := applyEnv(cfg); err != nil {
		return nil, err
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// applyEnv переопределяет поля переменными окружения. Имя выводится из пути
// поля в YAML: s3.listen -> TAPEGW_S3_LISTEN, migrator.batch_max_bytes ->
// TAPEGW_MIGRATOR_BATCH_MAX_BYTES. Так не нужно поддерживать отдельный список
// имён, и два поля одного типа (migrator/recaller) не конфликтуют.
func applyEnv(cfg *Config) error {
	return walkEnv(reflect.ValueOf(cfg).Elem(), "", func(path string, v reflect.Value) error {
		raw, ok := os.LookupEnv("TAPEGW_" + strings.ToUpper(path))
		if !ok {
			return nil
		}
		return setField(v, raw)
	})
}

// walkEnv рекурсивно обходит конфигурацию и вызывает fn для каждого поля,
// которое можно задать строкой. Вложенные структуры участвуют в пути,
// поля с yaml:"-" пропускаются.
func walkEnv(v reflect.Value, prefix string, fn func(path string, v reflect.Value) error) error {
	t := v.Type()
	for i := range t.NumField() {
		sf := t.Field(i)
		fv := v.Field(i)
		if sf.PkgPath != "" {
			continue // неэкспортированные поля не трогаем
		}
		name := yamlName(sf)
		if name == "" {
			continue
		}
		path := name
		if prefix != "" {
			path = prefix + "_" + name
		}
		if fv.Kind() == reflect.Struct {
			if err := walkEnv(fv, path, fn); err != nil {
				return err
			}
			continue
		}
		if err := fn(path, fv); err != nil {
			return fmt.Errorf("%v: %w", path, err)
		}
	}
	return nil
}

// yamlName возвращает имя поля в YAML (без индекса и omitempty).
func yamlName(sf reflect.StructField) string {
	tag, _, _ := strings.Cut(sf.Tag.Get("yaml"), ",")
	return tag
}

// setField присваивает полю строку, распарсив её по типу поля.
func setField(v reflect.Value, raw string) error {
	if v.Kind() == reflect.Ptr && v.Type().Elem().Kind() == reflect.Bool {
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return fmt.Errorf("parse bool %q: %w", raw, err)
		}
		v.Set(reflect.ValueOf(&b))
		return nil
	}

	switch v.Kind() {
	case reflect.String:
		v.SetString(raw)
	case reflect.Int, reflect.Int32, reflect.Int64:
		switch v.Type() {
		case durationType:
			d, err := time.ParseDuration(raw)
			if err != nil {
				return fmt.Errorf("parse duration %q: %w", raw, err)
			}
			v.SetInt(int64(d))
			return nil
		case byteSizeType:
			b, err := ParseByteSize(raw)
			if err != nil {
				return err
			}
			v.SetInt(int64(b))
			return nil
		}
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return fmt.Errorf("parse int %q: %w", raw, err)
		}
		v.SetInt(n)
	case reflect.Bool:
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return fmt.Errorf("parse bool %q: %w", raw, err)
		}
		v.SetBool(b)
	case reflect.Slice:
		if v.Type().Elem().Kind() != reflect.String {
			return fmt.Errorf("env override unsupported for %v", v.Type())
		}
		parts := strings.Split(raw, ",")
		for i := range parts {
			parts[i] = strings.TrimSpace(parts[i])
		}
		v.Set(reflect.ValueOf(parts))
	default:
		return fmt.Errorf("env override unsupported for %v", v.Type())
	}
	return nil
}

var (
	durationType = reflect.TypeOf(time.Duration(0))
	byteSizeType = reflect.TypeOf(ByteSize(0))
)

// Validate проверяет конфигурацию и возвращает все найденные проблемы сразу.
func (c *Config) Validate() error {
	var errs []error
	add := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf(format, args...))
	}

	if c.Root.Access == "" || c.Root.Secret == "" {
		add("root.access и root.secret обязательны (versitygw не стартует без них)")
	}
	if len(c.S3.Listen) == 0 {
		add("s3.listen: нужен хотя бы один адрес")
	}
	switch strings.ToLower(c.S3.DefaultStorageClass) {
	case "glacier", "standard":
	default:
		add("s3.default_storage_class: %q, ожидаются GLACIER или STANDARD", c.S3.DefaultStorageClass)
	}
	if (c.S3.TLS.Cert == "") != (c.S3.TLS.Key == "") {
		add("s3.tls: cert и key задаются парой")
	}
	if c.DB.DSN == "" {
		add("db.dsn обязателен")
	}
	if c.DB.MaxConns < 1 {
		add("db.max_conns: %d, должно быть >= 1", c.DB.MaxConns)
	}

	for name, w := range map[string]Worker{"migrator": c.Migrator, "recaller": c.Recaller} {
		if w.BatchMaxObjects < 1 {
			add("%s.batch_max_objects: %d, должно быть >= 1", name, w.BatchMaxObjects)
		}
		if w.MaxAttempts < 1 {
			add("%s.max_attempts: %d, должно быть >= 1", name, w.MaxAttempts)
		}
		if w.BatchMaxWait < 0 {
			add("%s.batch_max_wait: %v, не может быть отрицательным", name, w.BatchMaxWait)
		}
	}

	for name, wm := range map[string]Watermark{
		"spool.landing_watermark": c.Spool.LandingWatermark,
		"spool.cache_watermark":   c.Spool.CacheWatermark,
	} {
		if wm.High < 1 || wm.High > 100 {
			add("%s.high: %d, ожидается 1..100", name, wm.High)
		}
		if wm.Low < 1 || wm.Low >= wm.High {
			add("%s.low: %d, должно быть 1..high-1 (high=%d)", name, wm.Low, wm.High)
		}
	}
	if c.Spool.SlowDownHeadroom < 0 || c.Spool.SlowDownHeadroom > 100-c.Spool.LandingWatermark.High {
		add("spool.slowdown_headroom: %d, ожидается 0..%d",
			c.Spool.SlowDownHeadroom, 100-c.Spool.LandingWatermark.High)
	}
	if c.Spool.Landing == "" || c.Spool.Cache == "" {
		add("spool.landing и spool.cache обязательны")
	}
	if c.Spool.MPUTTL <= 0 {
		add("spool.mpu_ttl: %v, должно быть > 0", c.Spool.MPUTTL)
	}
	if c.Spool.OrphanGrace <= 0 {
		add("spool.orphan_grace: %v, должно быть > 0", c.Spool.OrphanGrace)
	}

	if c.Phobos.MaxInflight < 1 {
		add("phobos.max_inflight: %d, должно быть >= 1", c.Phobos.MaxInflight)
	}
	switch c.Phobos.Family {
	case "tape", "dir":
	default:
		add("phobos.family: %q, ожидаются tape или dir", c.Phobos.Family)
	}

	switch strings.ToLower(c.Logging.Format) {
	case "json", "text":
	default:
		add("logging.format: %q, ожидаются json или text", c.Logging.Format)
	}

	return errors.Join(errs...)
}

// MigrateOnStartValue безопаснее, чем разыменовывать *bool напрямую.
func (c *Config) MigrateOnStartValue() bool {
	return c.DB.MigrateOnStart != nil && *c.DB.MigrateOnStart
}
