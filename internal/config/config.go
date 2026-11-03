// Package config загружает конфигурацию tapegw из YAML и переопределяет
// отдельные поля переменными окружения TAPEGW_*.
//
// Правило 9 из AGENTS.md: настраиваемые числа (таймауты, размеры батчей,
// watermark) живут здесь, а не константами в коде.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
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
	IAM      IAM     `yaml:"iam"`
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
	Access string `yaml:"access"`
	Secret string `yaml:"secret"`
}

type S3 struct {
	Listen []string `yaml:"listen"`
	Region string   `yaml:"region"`
	TLS    TLS      `yaml:"tls"`
	IAMDir string   `yaml:"iam_dir"`
	// AcceptAsGlacier — дополнительные значения x-amz-storage-class, которые
	// принимаем как GLACIER. Нужен бэкап-софту, жёстко прошитому на STANDARD.
	// Пусто = принимаем только GLACIER и DEEP_ARCHIVE. См. ADR-0003.
	AcceptAsGlacier []string `yaml:"accept_as_glacier"`
	// HealthPath — путь health-эндпоинта на S3-порту. Пусто — эндпоинт выключен.
	HealthPath string `yaml:"health_path"`
	// MaxConnections и MaxRequests прокидываются в лимитер versitygw.
	MaxConnections    int `yaml:"max_connections"`
	MaxRequests       int `yaml:"max_requests"`
	MultipartMaxParts int `yaml:"multipart_max_parts"`
	// MaxObjectSize — верхняя граница размера одного объекта. Отсекается на PUT
	// как EntityTooLarge. Обязана быть не больше migrator.batch_max_bytes:
	// батч пишется с no_split целиком на один носитель, поэтому объект крупнее
	// батча никогда не был бы заархивирован. См. ADR-0008.
	MaxObjectSize ByteSize `yaml:"max_object_size"`
}

type TLS struct {
	Cert string `yaml:"cert"`
	Key  string `yaml:"key"`
}

// IAM — учётки versitygw поверх Postgres (ADR-0009). Пустой kek_file означает
// «Postgres-IAM выключён»: остаются s3.iam_dir и режим одной учётки. Оба
// источника сразу запрещены — Validate объясняет почему.
type IAM struct {
	// KEKFile — файл с кольцом KEK (версия → base64-ключ, 0400). Вне БД
	// нарочно: KEK рядом с обёртками секретов обесценивает и то, и другое.
	KEKFile string `yaml:"kek_file"`
	// KEKVersion — какая версия кольца заворачивает новые секреты. Старые
	// версии остаются в файле, пока на них ссылается хоть одна строка accounts
	// (ADR-0014), поэтому номер — настройка, а не «максимальная в файле».
	KEKVersion int `yaml:"kek_version"`
}

type DB struct {
	DSN            string        `yaml:"dsn"`
	MaxConns       int32         `yaml:"max_conns"`
	ConnectTimeout time.Duration `yaml:"connect_timeout"`
	// MigrateOnStart — применять миграции при старте. По умолчанию да.
	MigrateOnStart *bool `yaml:"migrate_on_start"`
	// AdvisoryLockTimeout — сколько ждём pg_advisory_xact_lock на ключе перед
	// отказом (ADR-0005). Зависший PUT хуже отказа: лок может держать транзакция,
	// которая ждёт ленту.
	AdvisoryLockTimeout time.Duration `yaml:"advisory_lock_timeout"`
}

type Spool struct {
	Landing          string        `yaml:"landing"`
	Cache            string        `yaml:"cache"`
	LandingWatermark Watermark     `yaml:"landing_watermark"`
	CacheWatermark   Watermark     `yaml:"cache_watermark"`
	MPUTTL           time.Duration `yaml:"mpu_ttl"`
	OrphanGrace      time.Duration `yaml:"orphan_grace"`
	// SlowDownHeadroom — на сколько процентов выше high watermark ещё можно
	// принимать PUT. Только операции записи: DeleteObject блокировать нельзя,
	// он диск и освобождает (ADR-0002).
	SlowDownHeadroom int `yaml:"slowdown_headroom"`
	// EvictRetries — сколько раз GET перечитывает состояние, если файл спула
	// исчез между решением и open (ADR-0004).
	EvictRetries int `yaml:"evict_retries"`
	// LandingMinRetention — минимальный срок, на который дисковая копия
	// гарантированно остаётся доступной. Он же — значение online_until при PUT
	// и шаг продления при архивации (ADR-0003).
	//
	// Это не «политика кэша», а обещание клиенту: HEAD показывает
	// x-amz-restore expiry-date = online_until, и вытеснить раньше нельзя,
	// иначе обещание окажется ложным.
	LandingMinRetention time.Duration `yaml:"landing_min_retention"`
}

// Watermark — пороги вытеснения в процентах занятого объёма тома.
type Watermark struct {
	High int `yaml:"high"`
	Low  int `yaml:"low"`
}

type Phobos struct {
	Family       string            `yaml:"family"`
	Layout       string            `yaml:"layout"`
	LayoutParams map[string]string `yaml:"layout_params"`
	Tags         []string          `yaml:"tags"`
	// MaxInflight — размер пула горутин, имеющих право вызывать libphobos.
	// До подтверждения потокобезопасности (спайк M0-3) держим 1.
	MaxInflight int `yaml:"max_inflight"`
}

// Worker — настройки одного фоновыего воркера (migrator или recaller).
type Worker struct {
	BatchMaxObjects int           `yaml:"batch_max_objects"`
	BatchMaxBytes   ByteSize      `yaml:"batch_max_bytes"`
	BatchMaxWait    time.Duration `yaml:"batch_max_wait"`
	MaxAttempts     int           `yaml:"max_attempts"`
}

type Metrics struct {
	Listen string `yaml:"listen"`
}

type Admin struct {
	Listen     string `yaml:"listen"`
	PathPrefix string `yaml:"path_prefix"`
}

type Logging struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
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
			Listen:            []string{":7070"},
			Region:            "us-east-1",
			AcceptAsGlacier:   nil,
			HealthPath:        "/health",
			MaxConnections:    50000,
			MaxRequests:       50000,
			MultipartMaxParts: 10000,
			MaxObjectSize:     100 * GiB,
		},
		DB: DB{
			DSN:                 "postgres://tapegw@localhost/phobos?search_path=tapegw",
			MaxConns:            20,
			ConnectTimeout:      10 * time.Second,
			MigrateOnStart:      &yes,
			AdvisoryLockTimeout: 30 * time.Second,
		},
		IAM: IAM{
			KEKVersion: 1,
		},
		Spool: Spool{
			Landing:          "/srv/tapegw/landing",
			Cache:            "/srv/tapegw/cache",
			LandingWatermark: Watermark{High: 80, Low: 60},
			CacheWatermark:   Watermark{High: 85, Low: 70},
			MPUTTL:           168 * time.Hour,
			OrphanGrace:      24 * time.Hour,
			SlowDownHeadroom: 5,
			EvictRetries:     2,
			// 24 часа: клиент, записавший объект и ушедший домой, утром
			// застанет его читаемым без RestoreObject.
			LandingMinRetention: 24 * time.Hour,
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
		// Строгий режим: опечатка в имени поля должна быть ошибкой запуска,
		// а не молчаливым «поле не применилось».
		dec := yaml.NewDecoder(bytes.NewReader(data))
		dec.KnownFields(true)
		if err := dec.Decode(cfg); err != nil {
			// Пустой (или состоящий только из комментариев) файл — не ошибка:
			// это способ сказать «всё из окружения». Без этой ветки
			// `tapegw -config empty.yaml` падал с «parse config: EOF».
			if errors.Is(err, io.EOF) {
				err = nil
			} else {
				return nil, fmt.Errorf("parse config %v: %w", path, err)
			}
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
	for _, alias := range c.S3.AcceptAsGlacier {
		if strings.TrimSpace(alias) == "" {
			add("s3.accept_as_glacier: пустая строка в списке")
		}
		if IsGlacierClass(alias) {
			add("s3.accept_as_glacier: %q уже является glacier-классом, дублировать не нужно", alias)
		}
	}
	if (c.S3.TLS.Cert == "") != (c.S3.TLS.Key == "") {
		add("s3.tls: cert и key задаются парой")
	}
	// Размер объекта ограничен так, чтобы он гарантированно влезал в батч:
	// батч пишется с no_split целиком на один носитель (ADR-0008), и объект
	// крупнее батча не был бы заархивирован никогда.
	if c.S3.MaxObjectSize <= 0 {
		add("s3.max_object_size: %d, должно быть > 0", int64(c.S3.MaxObjectSize))
	}
	if c.Migrator.BatchMaxBytes > 0 && c.S3.MaxObjectSize > c.Migrator.BatchMaxBytes {
		add("s3.max_object_size: %d не больше migrator.batch_max_bytes: %d — "+
			"объект крупнее батча не будет заархивирован", int64(c.S3.MaxObjectSize),
			int64(c.Migrator.BatchMaxBytes))
	}
	if c.DB.DSN == "" {
		add("db.dsn обязателен")
	}
	// Два источника учёток означают «непонятно, кто настоящий»: embedgw берёт
	// внешний IAMService и молча игнорирует iam_dir (embedgw.go:178). Лучше
	// ошибка запуска, чем стенд, где учётки правятся в одном месте, а читаются
	// из другого.
	if c.IAM.KEKFile != "" && c.S3.IAMDir != "" {
		add("iam.kek_file и s3.iam_dir заданы вместе: учётки либо в Postgres, либо в каталоге")
	}
	if c.IAM.KEKFile != "" && c.IAM.KEKVersion < 1 {
		add("iam.kek_version: %d, должно быть >= 1", c.IAM.KEKVersion)
	}
	if c.DB.MaxConns < 1 {
		add("db.max_conns: %d, должно быть >= 1", c.DB.MaxConns)
	}

	// Порядок проверок фиксирован: map в Go обходится в случайном порядке,
	// а текст ошибки конфигурации должен быть предсказуемым.
	for _, w := range []struct {
		name string
		w    Worker
	}{{"migrator", c.Migrator}, {"recaller", c.Recaller}} {
		if w.w.BatchMaxObjects < 1 {
			add("%s.batch_max_objects: %d, должно быть >= 1", w.name, w.w.BatchMaxObjects)
		}
		if w.w.MaxAttempts < 1 {
			add("%s.max_attempts: %d, должно быть >= 1", w.name, w.w.MaxAttempts)
		}
		if w.w.BatchMaxWait < 0 {
			add("%s.batch_max_wait: %v, не может быть отрицательным", w.name, w.w.BatchMaxWait)
		}
	}

	for _, wm := range []struct {
		name string
		wm   Watermark
	}{{"spool.landing_watermark", c.Spool.LandingWatermark}, {"spool.cache_watermark", c.Spool.CacheWatermark}} {
		if wm.wm.High < 1 || wm.wm.High > 100 {
			add("%s.high: %d, ожидается 1..100", wm.name, wm.wm.High)
		}
		if wm.wm.Low < 1 || wm.wm.Low >= wm.wm.High {
			add("%s.low: %d, должно быть 1..high-1 (high=%d)", wm.name, wm.wm.Low, wm.wm.High)
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
	if c.Spool.EvictRetries < 0 {
		add("spool.evict_retries: %d, должно быть >= 0", c.Spool.EvictRetries)
	}
	if c.Spool.LandingMinRetention <= 0 {
		add("spool.landing_min_retention: %v, должно быть > 0 — это обещание клиенту в x-amz-restore",
			c.Spool.LandingMinRetention)
	}
	if c.DB.AdvisoryLockTimeout <= 0 {
		add("db.advisory_lock_timeout: %v, должно быть > 0", c.DB.AdvisoryLockTimeout)
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

// GlacierClass — единственный класс хранения, который tapegw реально даёт.
const GlacierClass = "GLACIER"

// DeepArchiveClass принимается как синоним GLACIER: клиент, написанный под
// Glacier Deep Archive, получает то же поведение ленты. Отказывать ему смысла
// нет — разница с Glacier у нас только в SLA, которого в MVP нет.
const DeepArchiveClass = "DEEP_ARCHIVE"

// IsGlacierClass — нужно ли архивировать объект с таким storage class.
// Пустой заголовок трактуется как GLACIER: клиент не выразил намерение,
// и мы применяем default_storage_class (ADR-0003).
func IsGlacierClass(class string) bool {
	switch strings.ToUpper(strings.TrimSpace(class)) {
	case "", GlacierClass, DeepArchiveClass:
		return true
	}
	return false
}

// StorageClassAction решает, что делать с входящим x-amz-storage-class
// (ADR-0003). Возвращает канонический класс, который запишем в метаданные,
// либо ошибку — тогда бэкенд отвечает NotImplemented.
//
// Пустой заголовок = клиент намерение не выражал = наш класс по умолчанию.
// Алиасы из s3.accept_as_glacier нужны бэкап-софту, жёстко прошитому на
// STANDARD: молча проигнорировать чужой класс хуже, чем отказаться, потому что
// клиент решит, что у него быстрый класс, а читать он будет с ленты.
func (c *Config) StorageClassAction(class string) (string, error) {
	norm := strings.ToUpper(strings.TrimSpace(class))
	if IsGlacierClass(norm) {
		return GlacierClass, nil
	}
	for _, alias := range c.S3.AcceptAsGlacier {
		if strings.EqualFold(strings.TrimSpace(alias), norm) {
			return GlacierClass, nil
		}
	}
	return "", fmt.Errorf("storage class %q не поддерживается: tapegw хранит всё на ленте", class)
}
