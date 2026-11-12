// Package config загружает конфигурацию tapegw из YAML и переопределяет
// отдельные поля переменными окружения TAPEGW_*.
//
// Правило 9 из AGENTS.md: настраиваемые числа (таймауты, размеры батчей,
// watermark) живут здесь, а не константами в коде.
package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/vladsimachkov/tapegw/internal/mover"
)

// Config — корень конфигурации. Теги yaml задают имя поля в файле, теги env —
// имя переменной окружения (без префикса TAPEGW_).
type Config struct {
	S3       S3      `yaml:"s3"`
	DB       DB      `yaml:"db"`
	IAM      IAM     `yaml:"iam"`
	Spool    Spool   `yaml:"spool"`
	Phobos   Phobos  `yaml:"phobos"`
	Pack     Pack    `yaml:"pack"`
	Migrator Worker  `yaml:"migrator"`
	Recaller Worker  `yaml:"recaller"`
	Janitor  Janitor `yaml:"janitor"`
	Deleter  Deleter `yaml:"deleter"`
	// Journal — журнал метаданных: размеры сегментов запечатывания и воркер,
	// который эти сегменты закрывает (ADR-0023).
	Journal Journal `yaml:"journal"`
	Metrics Metrics `yaml:"metrics"`
	Admin   Admin   `yaml:"admin"`
	Logging Logging `yaml:"logging"`
	Root    Root    `yaml:"root"`
	// Pools — политика записи пулов по имени (pools.name в БД). Пусто =
	// единственный пул пишет одну копию с тегом пула (историческое поведение
	// среза). См. PoolWrite.
	Pools   map[string]PoolWrite `yaml:"pools"`
	Version Version              `yaml:"-"`
}

// PoolWrite — политика записи одного пула (предложение 0013 §2): список
// спецификаций копий, число копий = длина списка. Правило «архивный = все
// копии записаны» означает, что migrator пишет каждую копию отдельной
// операцией протокола со своими тегами, а объект становится archived только
// после последней.
//
// library_write из предложения сюда пока не вошёл: библиотека в конфиге одна
// (phobos.library), выбор библиотеки записи становится по-настоящему
// попульным вместе с переездом между библиотеками (Ф2.8).
type PoolWrite struct {
	Copies []CopySpec `yaml:"copies"`
}

// CopySpec — одна копия: имя (пишется в archive_copies.spec и в атрибут
// tapegw.copy) и теги, которыми Phobos выберет ленту для этой копии.
// Тег пула добавлять не нужно — migrator дописывает его сам (ADR-0013: тег
// пула обязан быть на каждой записи).
type CopySpec struct {
	Name string   `yaml:"name"`
	Tags []string `yaml:"tags"`
}

// CopySpecs — спецификации и теги для migrator'а: порядок копий сохранён,
// тег пула добавлен к каждой копии без дублей. Теги двух копий не должны
// совпадать — иначе «две копии» могут лечь на одну ленту, и проверка
// держится конфигом, а не надеждой.
func (p PoolWrite) CopySpecs(poolTag string) (specs []string, tags map[string][]string) {
	tags = make(map[string][]string, len(p.Copies))
	for _, c := range p.Copies {
		specs = append(specs, c.Name)
		set := make([]string, 0, len(c.Tags)+1)
		set = append(set, poolTag)
		for _, t := range c.Tags {
			if t != poolTag {
				set = append(set, t)
			}
		}
		tags[c.Name] = set
	}
	return specs, tags
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
	// ExportRetention — срок, в течение которого обёрнутые TEK переживают
	// удаление тенанта (ADR-0014). Именно его надо называть в договоре как
	// «данные становятся недоступны необратимо через N».
	//
	// Обязателен: без него «мы удалили» означает только «мы стёрли строку».
	ExportRetention time.Duration `yaml:"export_retention"`
	// BackupRetention — сколько живут PITR-бэкапы Postgres и WAL-архив.
	//
	// Это не справочное поле: tenants.wrapped_key лежит в каждом дампе и в каждом
	// WAL-сегменте, поэтому реальная окончательность = max(export_retention,
	// backup_retention). Оператор обязан указать правду; ноль означает «не
	// указано», и тогда CLI отказа от старой версии KEK не даётся — см. ADR-0014.
	BackupRetention time.Duration `yaml:"backup_retention"`
	// CacheTTL — срок жизни записи кэша учёток. Инвалидация мгновенная и не по
	// TTL (LISTEN/NOTIFY, ADR-0016); TTL — страховка на правку accounts мимо
	// tapegw, поэтому он обязан быть: без него отозванная вручную учётка
	// работала бы без ограничения по времени.
	CacheTTL time.Duration `yaml:"cache_ttl"`
	// CacheMaxEntries — потолок записей в кэше: access_key выбирает клиент,
	// и без потолка перебор случайных ключей раздувал бы память.
	CacheMaxEntries int `yaml:"cache_max_entries"`
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
	// Library — имя библиотеки Phobos. API не читает профиль: без явных
	// family/library/layout вызов падает с -ENODEV, и из текста ошибки это
	// не понять (ADR-0007, спайк M0-02).
	Library string   `yaml:"library"`
	Tags    []string `yaml:"tags"`
	// Generations — сведения о поколениях лент: флаг writable (Р5 из 0014,
	// слой 2 запрета записи на устаревшее поколение). Поколение с
	// writable: false закрыто для записи — ленты такого поколения
	// регистрируются read_only, а не целью записи. Полное конфигурирование
	// поколений (совместимость чтения, проверка политик при старте) — Ф0.5.
	Generations map[string]Generation `yaml:"generations"`
	// MaxInflight — размер пула горутин, имеющих право вызывать libphobos.
	// До подтверждения потокобезопасности (спайк M0-3) держим 1.
	MaxInflight int `yaml:"max_inflight"`

	// Movers — mover'ы, к которым подключается планировщик. Именованная
	// коллекция: ключ — имя драйва, оно же слот воркера (WorkerID =
	// <node>/<ключ>) и часть SAN-URI сертификата mover'а. Одна идентичность
	// вместо трёх, которые могут разойтись.
	Movers map[string]Mover `yaml:"movers"`

	// MoverClient — сертификат tapegw как клиента mover'ов: планировщик
	// архивации (SAN-URI tapegw://scheduler, ADR-0021). Один на все mover'ы:
	// различаются mover'ы драйвом в SAN, а роль клиента у планировщика одна.
	// Читается только когда включён migrator и в библиотеке есть mover'ы.
	MoverClient MoverTLS `yaml:"mover_client"`

	// DeleterClient — сертификат tapegw как клиента mover'ов для удалителя
	// (SAN-URI tapegw://deleter, M6). Отдельный от mover_client намеренно:
	// purge удаляет целый архивный объект, и это право не должно жить на
	// сертификате планировщика архивации. Требуется, когда deleter включён и
	// в библиотеке есть mover роли migrator.
	DeleterClient MoverTLS `yaml:"deleter_client"`

	// Mover — конфигурация самого процесса tapegw-mover (ADR-0021). tapegw
	// эти поля не читает: они живут в том же файле, потому что один файл —
	// одна установка, а переменные TAPEGW_PHOBOS_MOVER_* выводятся из пути.
	Mover MoverServer `yaml:"mover"`
}

// Generation — конфигурация одного поколения лент.
type Generation struct {
	// Writable — закрыто ли поколение для записи. nil (флаг не указан) —
	// открыто: закрытие поколения всегда явное решение, отсутствие записи в
	// конфиге не закрывает поколение молча.
	Writable *bool `yaml:"writable"`
}

// GenerationWritable — открыто ли поколение для записи: по умолчанию открыто,
// закрыт только тот, у кого явно указано writable: false.
func (p *Phobos) GenerationWritable(name string) bool {
	if g, ok := p.Generations[name]; ok && g.Writable != nil {
		return *g.Writable
	}
	return true
}

// Mover — точка подключения к одному mover'у (ADR-0007, ADR-0021).
type Mover struct {
	Addr    string `yaml:"addr"`
	Node    string `yaml:"node"`
	Library string `yaml:"library"`
	// Role — роль mover'а: migrator (архивация) или recaller (восстановление).
	// Фильтр по роли, а не только по библиотеке: в одной библиотеке могут стоять
	// драйвы обоих пулов, и планировщик архивации обязан брать только свои —
	// иначе он отдаст архивацию драйву, который ждёт restorer'а, и получит
	// отказ рукопожатия (или, что хуже, подключится не туда).
	Role string `yaml:"role"`
}

// MoverServer — настройки процесса tapegw-mover: слушатель, идентичность и
// буфер (ADR-0021). Обязательное проверяет ValidateMover, а не Validate:
// tapegw эти поля не читает, и требовать их в общем валидаторе значило бы
// ломать конфиг тем, кто mover ещё не поднимал.
type MoverServer struct {
	// Listen — адрес mTLS-слушателя. Plaintext-режима нет намеренно: mover
	// возит шифротекст через сеть (ADR-0021).
	Listen string `yaml:"listen"`
	// Drive — имя драйва, к которому привязан процесс: ключ в phobos.movers,
	// слот воркера и SAN-URI сертификата (mover://<drive>). Одна идентичность
	// вместо трёх, которые могут разойтись.
	Drive string `yaml:"drive"`
	// Role — для какого пула поднят процесс: migrator или recaller (ADR-0007).
	// Роль сверяет клиент при рукопожатии, а сервер по ней решает, какие
	// роли клиентов принимать: migrator'у — планировщик архивации, recaller'у —
	// планировщик восстановления.
	Role string `yaml:"role"`
	// PhobosConfig — конфиг Phobos для libphobos и для CLI extent list. Пусто
	// — значение по умолчанию Phobos. libphobos и CLI обязаны читать один файл,
	// иначе «записали одним конфигом, ищем другим».
	PhobosConfig string   `yaml:"phobos_config"`
	TLS          MoverTLS `yaml:"tls"`
	// StagingDir — каталог буфера операций: батч сначала принят целиком и
	// fsync-нут, и только потом phobos_put читает с локального диска (ADR-0021).
	StagingDir string `yaml:"staging_dir"`
	// BufferBytes — весь буфер. Объект больше него не примут (no_buffer —
	// ошибка конфигурации, она алертится).
	BufferBytes ByteSize      `yaml:"buffer_bytes"`
	ChunkSize   ByteSize      `yaml:"chunk_size"`
	Inactivity  time.Duration `yaml:"inactivity_timeout"`
	OpTimeout   time.Duration `yaml:"op_timeout"`
	// ResultTTL — сколько держим запись и буфер завершённой операции: tapegw
	// вправе переподключиться за итогом.
	ResultTTL time.Duration `yaml:"result_ttl"`
	// OpTTL — срок незавершённой операции: брошенный буфер не должен
	// занимать место вечно.
	OpTTL          time.Duration `yaml:"op_ttl"`
	ExpireInterval time.Duration `yaml:"expire_interval"`
	PhobosVersion  string        `yaml:"phobos_version"`
}

// MoverTLS — тройка для взаимного TLS mover'а (ADR-0021): cert/key — сам
// mover (SAN-URI mover://<drive>), ca — корень, подписывающий клиентские
// сертификаты tapegw.
type MoverTLS struct {
	Cert string `yaml:"cert"`
	Key  string `yaml:"key"`
	CA   string `yaml:"ca"`
}

// Pack — упаковка объектов (M4). Порог здесь один: объекты меньше него идут
// в пачки, остальные пишутся отдельными объектами Phobos (ARCHITECTURE,
// «Упаковка мелких объектов в пачки»). target_size и max_wait появятся вместе
// с условиями закрытия пачки — сейчас пачка равна батчу.
type Pack struct {
	// Threshold — граница мелкого и крупного объекта. Сравнивается с ОТКРЫТЫМ
	// размером объекта (objects.size), а не с длиной шифротекста на диске:
	// порог — про объект, который видит клиент (и про RPO из договора, который
	// тоже считается объектами), а file_bytes добавляет манифест с футером и
	// у каждого объекта разный. Ровно порог — уже крупный: «меньше порога —
	// в пачку, остальные — отдельными».
	Threshold ByteSize `yaml:"threshold"`
}

// Worker — настройки одного фоновыего воркера (migrator или recaller).
type Worker struct {
	// Enabled — включать ли воркер. По умолчанию true, и включённый воркер
	// обязан найти себе mover'а: «включено, но ходить некому» — отказ на
	// старте с внятной ошибкой, а не молчаливый простой очереди.
	// false — воркер не стартует, и это громко пишется в лог и в /health/ready:
	// выключенная архивация, о которой никто не знает, дороже.
	// recaller.enabled читается, когда recaller появится (S5).
	Enabled *bool `yaml:"enabled"`
	// ClaimTTL — на сколько воркер запечатывает пачку за собой. Продлевается
	// во время отправки, поэтому может быть меньше самой долгой отправки; после
	// сбоя именно он определяет, как долго пачка простоит без продолжения.
	ClaimTTL time.Duration `yaml:"claim_ttl"`
	// ShutdownGrace — сколько ждём текущую отправку при остановке. Контекст
	// воркера отменяется по сигналу, но начатая пачка доходит до конца; если
	// mover висит дольше grace, процесс уходит, а пачка остаётся в sealed с
	// захватом, у которого истекает срок, — её продолжит следующий запуск.
	ShutdownGrace   time.Duration `yaml:"shutdown_grace"`
	BatchMaxObjects int           `yaml:"batch_max_objects"`
	BatchMaxBytes   ByteSize      `yaml:"batch_max_bytes"`
	BatchMaxWait    time.Duration `yaml:"batch_max_wait"`
	MaxAttempts     int           `yaml:"max_attempts"`
	// RetryBackoff — основание экспоненциальной задержки повтора (потолок
	// сутки): попытка N откладывается на RetryBackoff * 2^N.
	RetryBackoff time.Duration `yaml:"retry_backoff"`
}

// EnabledValue — включён ли воркер с учётом умолчания (nil = true).
func (w Worker) EnabledValue() bool { return w.Enabled == nil || *w.Enabled }

// Journal — журнал метаданных (ADR-0023): как режутся сегменты и как часто их
// запечатывает воркер.
type Journal struct {
	Segment JournalSegment `yaml:"segment"`
	Sealer  JournalSealer  `yaml:"sealer"`
}

// JournalSegment — условия закрытия одного сегмента: по числу событий или по
// объёму, и только. Своего таймера у сегмента нет (ARCHITECTURE, Ж2): окно
// «событие → лента» задаёт закрытие пачки журнала, второй таймер поверх первого
// сделал бы окно непонятной формы.
type JournalSegment struct {
	// MaxEvents — сколько событий максимум в одном сегменте.
	MaxEvents int `yaml:"max_events"`
	// MaxBytes — сколько максимум весит содержимое сегмента. Считается по
	// payload событий (сумма octet_length), а не по шифротексту: порог про
	// содержимое, а не про то, сколько займёт AES. Действует и на сегменты
	// опоздавших: без него такой сегмент вырос бы до всей истории тенанта.
	MaxBytes ByteSize `yaml:"max_bytes"`
}

// JournalSealer — воркер запечатывания сегментов журнала.
type JournalSealer struct {
	// Enabled — по умолчанию true. Выключённый силер означает, что события
	// журнала копятся незапечатанными: на ленту они не уедут, и восстановление
	// после аварии вернёт старое состояние блокировок. Поэтому выключение
	// пишется в лог громко, как у остальных воркеров.
	Enabled *bool `yaml:"enabled"`
	// Interval — период прохода. Запечатывание не спешит: событие уже записано
	// в БД в транзакции изменения, силер только переводит его в сегмент.
	Interval time.Duration `yaml:"interval"`
	// TenantsBatch — сколько тенантов обслуживается за один проход.
	TenantsBatch int `yaml:"tenants_batch"`
	// SweepInterval — как часто искать «опоздавших»: события, которые на момент
	// запечатывания были ещё не видны (транзакция в полёте) и потому остались ни
	// в одном сегменте. Обычный ход читает только события дальше рубежа, так что
	// без такой проверки опоздавший лежал бы незапечатанным вечно.
	SweepInterval time.Duration `yaml:"sweep_interval"`
	// SweepBatch — сколько опоздавших событий берётся за одну проверку. Это
	// верхняя граница по числу; по объёму сегмент опоздавших режется тем же
	// journal.segment.max_bytes, что и обычный.
	SweepBatch int `yaml:"sweep_batch"`
	// MaxSegmentsPerTenant — сколько сегментов одного тенанта делается за один
	// проход (и в обычном ходе, и в проверке опоздавших). Ограничение нужно,
	// чтобы один тенант с огромным backlog не отдал весь проход остальным:
	// остаток допечатается следующим тиком, он никуда не делся.
	MaxSegmentsPerTenant int `yaml:"max_segments_per_tenant"`
	// ShutdownGrace — сколько ждём текущий проход при остановке. Проход —
	// несколько запросов в Postgres, он долог только на заблокированной схеме.
	ShutdownGrace time.Duration `yaml:"shutdown_grace"`
}

// EnabledValue — включён ли силер с учётом умолчания (nil = true).
func (j JournalSealer) EnabledValue() bool { return j.Enabled == nil || *j.Enabled }

// Janitor — фоновый вытеснитель дисковой копии. S3 — только вытеснение по
// истечении online_until (ADR-0004); watermark-вытеснение и сборка сирот —
// отдельные пункты плана.
type Janitor struct {
	// Enabled — по умолчанию true. Выключённый janitor не виден ни в одном
	// S3-ответе, а landing тем временем растёт: выключенным его делает либо
	// явный false, либо ничего — как и с migrator, молчание означает «включено».
	Enabled *bool `yaml:"enabled"`
	// Interval — период выборки. Вытеснение не спешит: дата обещает файл
	// как минимум до online_until, а опоздание на интервал клиенту не видно.
	Interval time.Duration `yaml:"interval"`
	// BatchMaxObjects — сколько объектов снимается за один проход.
	BatchMaxObjects int `yaml:"batch_max_objects"`
	// ShutdownGrace — сколько ждём текущий проход при остановке. Проход — это
	// unlink по локальному ФС, он долог только при зависшем ФС; не дождались —
	// процесс уходит, неснятые объекты снимет следующий запуск.
	ShutdownGrace time.Duration `yaml:"shutdown_grace"`
}

// EnabledValue — включён ли janitor с учётом умолчания (nil = true).
func (j Janitor) EnabledValue() bool { return j.Enabled == nil || *j.Enabled }

// Deleter — фоновый удалитель (M6): снимает с диска и с ленты версии, ушедшие
// в 'deleting'. Выключенный deleter виден так же громко, как выключенная
// архивация: удалённые версии продолжали бы занимать место на ленте и на
// диске, и снаружи это не видно ничем.
type Deleter struct {
	// Enabled — по умолчанию true.
	Enabled *bool `yaml:"enabled"`
	// Interval — период выборки из 'deleting'.
	Interval time.Duration `yaml:"interval"`
	// BatchMaxObjects — сколько версий разбирается за один проход.
	BatchMaxObjects int `yaml:"batch_max_objects"`
	// ClaimTTL — на сколько воркер занимает версию (next_try_at отодвигается
	// на этот срок). Purge целого объекта — минуты; после падения захват
	// истекает сам, и версию подберёт следующий прогон.
	ClaimTTL time.Duration `yaml:"claim_ttl"`
	// RetryBackoff — основание экспоненциальной задержки повтора (потолок
	// сутки): сбой N откладывается на RetryBackoff * 2^N.
	RetryBackoff time.Duration `yaml:"retry_backoff"`
	// ShutdownGrace — сколько ждём текущий проход при остановке.
	ShutdownGrace time.Duration `yaml:"shutdown_grace"`
}

// EnabledValue — включён ли deleter с учётом умолчания (nil = true).
func (d Deleter) EnabledValue() bool { return d.Enabled == nil || *d.Enabled }

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
			KEKVersion:      1,
			ExportRetention: 30 * 24 * time.Hour,
			CacheTTL:        5 * time.Minute,
			CacheMaxEntries: 10000,
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
			Mover: MoverServer{
				// Таймауты и TTL — стартовые значения для настройки на
				// стенде, не требования (см. преамбулу к конфигурации в
				// ARCHITECTURE.md).
				ChunkSize:      4 << 20,
				Inactivity:     5 * time.Minute,
				OpTimeout:      30 * time.Minute,
				ResultTTL:      time.Hour,
				OpTTL:          24 * time.Hour,
				ExpireInterval: time.Minute,
			},
		},
		Pack: Pack{
			// 25 MiB — значение из ARCHITECTURE («Упаковка мелких объектов»);
			// RPO в договоре обещается именно этим числом, менять молча нельзя.
			Threshold: 25 * MiB,
		},
		Migrator: Worker{
			BatchMaxObjects: 1000,
			BatchMaxBytes:   200 * GiB,
			BatchMaxWait:    15 * time.Minute,
			MaxAttempts:     10,
			RetryBackoff:    30 * time.Second,
			ClaimTTL:        5 * time.Minute,
			// 30 минут: батч на 200 GiB идёт минуты, grace обязан переживать
			// штатную отправку, но не превращать остановку в вечное ожидание.
			ShutdownGrace: 30 * time.Minute,
		},
		Recaller: Worker{
			BatchMaxObjects: 500,
			BatchMaxBytes:   0,
			BatchMaxWait:    5 * time.Minute,
			MaxAttempts:     10,
			RetryBackoff:    30 * time.Second,
			// Lease захвата строки restores: чтение пачки с ленты идёт
			// минутами, слишком короткий ttl заставил бы второй воркер
			// перехватывать живую работу.
			ClaimTTL: 30 * time.Minute,
			// Начатое чтение доходит: grace переживает штатную пачку.
			ShutdownGrace: 30 * time.Minute,
		},
		Janitor: Janitor{
			Interval:        time.Minute,
			BatchMaxObjects: 1000,
			ShutdownGrace:   30 * time.Second,
		},
		Deleter: Deleter{
			Interval:        time.Minute,
			BatchMaxObjects: 200,
			// Purge пачки — несколько вызовов mover по минуте каждый; ttl с
			// запасом переживает штатный проход, а после падения истекает сам.
			ClaimTTL:      15 * time.Minute,
			RetryBackoff:  30 * time.Second,
			ShutdownGrace: 5 * time.Minute,
		},
		Journal: Journal{
			Segment: JournalSegment{
				// Событие — несколько сотен байт payload; 10000 событий это
				// единицы мегабайт, то есть сегмент режется по числу, а
				// объём — страховка от событий с большой политикой бакета.
				MaxEvents: 10000,
				MaxBytes:  16 * MiB,
			},
			Sealer: JournalSealer{
				Interval:             5 * time.Minute,
				TenantsBatch:         50,
				SweepInterval:        time.Hour,
				SweepBatch:           1000,
				MaxSegmentsPerTenant: 10,
				ShutdownGrace:        30 * time.Second,
			},
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
	cfg, err := loadRaw(path)
	if err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// LoadMover — то же чтение (тот же YAML, те же TAPEGW_*), но проверяется
// только то, что нужно процессу tapegw-mover (ValidateMover).
//
// Отдельная точка входа нужна из-за топологии двух серверов (предложение 0012):
// на сервере библиотеки у mover'а свой файл конфигурации, и требовать в нём
// root-учётку S3, DSN Postgres или KEK — значит заставлять оператора
// подставлять фиктивные значения. Механизм переопределения переменными
// (правило 9) при этом общий: путь в YAML тот же.
func LoadMover(path string) (*Config, error) {
	cfg, err := loadRaw(path)
	if err != nil {
		return nil, err
	}
	if err := cfg.ValidateMover(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// loadRaw — чтение без валидации: YAML поверх дефолтов + TAPEGW_*.
func loadRaw(path string) (*Config, error) {
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
		if isNamedCollection(fv) {
			if err := walkEnvMap(fv, path, fn); err != nil {
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

// isNamedCollection — именованная коллекция: map[string]T, где T — структура.
// Такие коллекции переопределяются по имени ключа (см. ARCHITECTURE.md,
// уточнение к правилу 9), а не индексом: перестановка элементов в YAML не должна
// менять то, какую запись правит переменная окружения.
func isNamedCollection(v reflect.Value) bool {
	if v.Kind() != reflect.Map || v.Type().Key().Kind() != reflect.String {
		return false
	}
	et := v.Type().Elem()
	for et.Kind() == reflect.Ptr {
		et = et.Elem()
	}
	return et.Kind() == reflect.Struct
}

// keyPattern — допустимые ключи именованных коллекций. Ограничение нужно,
// чтобы преобразование «ключ -> часть имени переменной» было взаимно
// однозначным: иначе drive-1 и drive_1 дали бы одну переменную.
var keyPattern = regexp.MustCompile(`^[a-z0-9_]+$`)

func walkEnvMap(fv reflect.Value, path string, fn func(string, reflect.Value) error) error {
	prefix := "TAPEGW_" + strings.ToUpper(path)

	// Вся коллекция одной переменной (контейнеры, CI).
	var replaced bool
	if raw, ok := os.LookupEnv(prefix); ok {
		// В новую карту, а не в существующую: json дополняет непустую map,
		// а переменная обязана заменять коллекцию — иначе «задал целиком»
		// оставило бы половину настроек из YAML.
		fresh := reflect.New(fv.Type())
		if err := json.Unmarshal([]byte(raw), fresh.Interface()); err != nil {
			return fmt.Errorf("%s: %w", prefix, err)
		}
		fv.Set(fresh.Elem())
		replaced = true
	}

	consulted := map[string]bool{}
	whole := prefix + "_"
	if replaced {
		// Задать целиком и править поле — не угадываем, что главнее.
		for _, kv := range os.Environ() {
			name, _, _ := strings.Cut(kv, "=")
			if strings.HasPrefix(name, whole) {
				return fmt.Errorf("%s: нельзя задать коллекцию целиком и одновременно переопределить поле %s", prefix, name)
			}
		}
		return nil
	}

	keys := fv.MapKeys()
	sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })
	for _, k := range keys {
		if !keyPattern.MatchString(k.String()) {
			return fmt.Errorf("%s: ключ %q: допускаются строчные латинские буквы, цифры и _", path, k.String())
		}
		elem := reflect.New(fv.Type().Elem()).Elem()
		elem.Set(fv.MapIndex(k))
		kpath := path + "_" + strings.ToUpper(k.String())
		if err := walkEnv(elem, kpath, func(p2 string, v2 reflect.Value) error {
			consulted["TAPEGW_"+strings.ToUpper(p2)] = true
			return fn(p2, v2)
		}); err != nil {
			return err
		}
		fv.SetMapIndex(k, elem)
	}

	// Переменная про несуществующий mover или несуществующее поле — ошибка,
	// а не молчаливое игнорирование: иначе новый mover через окружение
	// «добавился», но не добавился.
	for _, kv := range os.Environ() {
		name, _, _ := strings.Cut(kv, "=")
		if strings.HasPrefix(name, whole) && !consulted[name] {
			return fmt.Errorf("%s: неизвестная переменная %s: коллекция задаётся только в YAML, переменная правит поле существующего ключа", path, name)
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
	// выборка ограничена суммарным размером batch_max_bytes, и объект крупнее
	// этого лимита не попал бы в батч никогда — он висел бы в pending вечно.
	//
	// Раньше здесь стояла другая отсылка — «батч пишется с no_split целиком на
	// один носитель». Она перестала быть верной, когда архивация стала отправлять
	// объекты по одному (ADR-0021): требование «влезть на ленту» теперь
	// относится к каждому объекту и к пачке отдельно, а не к батчу, и
	// проверяется оно настройкой пачки, а не этим сравнением.
	if c.S3.MaxObjectSize <= 0 {
		add("s3.max_object_size: %d, должно быть > 0", int64(c.S3.MaxObjectSize))
	}
	if c.Migrator.BatchMaxBytes > 0 && c.S3.MaxObjectSize > c.Migrator.BatchMaxBytes {
		add("s3.max_object_size: %d не больше migrator.batch_max_bytes: %d — "+
			"объект крупнее батча не будет заархивирован", int64(c.S3.MaxObjectSize),
			int64(c.Migrator.BatchMaxBytes))
	}
	// Ноль означал бы «все объекты крупные»: каждый — отдельный объект Phobos,
	// индекс LTFS раздут ровно тем, ради чего пачки и заводили. Порог —
	// обязательная настройка, а не «ноль = как-нибудь».
	if c.Pack.Threshold <= 0 {
		add("pack.threshold: %d, должно быть > 0", int64(c.Pack.Threshold))
	}
	if c.DB.DSN == "" {
		add("db.dsn обязателен")
	}
	// Два источника учёток означают «непонятно, кто настоящий»: embedgw берёт
	// внешний IAMService и молча игнорирует iam_dir (embedgw.go:178). Лучше
	// ошибка запуска, чем стенд, где учётки правятся в одном месте, а читаются
	// из другого.
	// Кольцо KEK обязательно с T2: шифрование тела объекта включено всегда
	// (ADR-0014), и без KEK нечем завернуть TEK тенанта. Раньше это поле
	// включало только Postgres-IAM и могло быть пустым.
	if c.IAM.KEKFile == "" {
		add("iam.kek_file обязателен: без кольца KEK нечем завернуть ключ тенанта, " +
			"а шифрование тела объекта обязательное (ADR-0014)")
	}
	if c.IAM.KEKFile != "" && c.S3.IAMDir != "" {
		add("iam.kek_file и s3.iam_dir заданы вместе: учётки либо в Postgres, либо в каталоге")
	}
	if c.IAM.KEKFile != "" && c.IAM.KEKVersion < 1 {
		add("iam.kek_version: %d, должно быть >= 1", c.IAM.KEKVersion)
	}
	if c.IAM.ExportRetention <= 0 {
		add("iam.export_retention: %v, должно быть > 0 — без него «удалить тенанта» означает «стереть строку», а не «данные пропали»",
			c.IAM.ExportRetention)
	}
	if c.IAM.BackupRetention < 0 {
		add("iam.backup_retention: %v, не может быть отрицательным", c.IAM.BackupRetention)
	}
	// Окончательность — максимум по всем хранилищам (ADR-0014). Разрешить
	// бэкапы дольше экспорта — значит разрешить себе обещание, которое не
	// выполняется: удалённый тенант будет восстановим из дампа дольше, чем
	// мы обещали. Проще заставить согласовать сроки здесь, чем считать
	// максимум вручную при разборе инцидента.
	if c.IAM.BackupRetention > c.IAM.ExportRetention {
		add("iam.backup_retention (%v) больше iam.export_retention (%v): удалённый тенант переживёт обещанный срок на %v — поднимите export_retention или сократите срок бэкапов",
			c.IAM.BackupRetention, c.IAM.ExportRetention, c.IAM.BackupRetention-c.IAM.ExportRetention)
	}
	// Ноль здесь — не «бессрочный кэш», а дыра: правка accounts мимо tapegw
	// вообще не имела бы границы устаревания.
	if c.IAM.CacheTTL <= 0 {
		add("iam.cache_ttl: %v, должно быть больше нуля — это страховка кэша", c.IAM.CacheTTL)
	}
	if c.IAM.CacheMaxEntries < 1 {
		add("iam.cache_max_entries: %d, должно быть >= 1", c.IAM.CacheMaxEntries)
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
		if w.w.RetryBackoff <= 0 {
			add("%s.retry_backoff: %v, должно быть > 0 — ноль дал бы повтор в тот же момент",
				w.name, w.w.RetryBackoff)
		}
		if w.w.ClaimTTL <= 0 {
			add("%s.claim_ttl: %v, должно быть > 0 — просроченный захват перехватывают по нему",
				w.name, w.w.ClaimTTL)
		}
	}

	if c.Janitor.Interval <= 0 {
		add("janitor.interval: %v, должно быть > 0", c.Janitor.Interval)
	}
	if c.Janitor.BatchMaxObjects < 1 {
		add("janitor.batch_max_objects: %d, должно быть >= 1", c.Janitor.BatchMaxObjects)
	}
	if c.Janitor.ShutdownGrace <= 0 {
		add("janitor.shutdown_grace: %v, должно быть > 0", c.Janitor.ShutdownGrace)
	}

	// Размеры сегмента проверяются всегда, а не только при включённом силере:
	// сегменты однажды начнёт запечатывать кто-то другой (следующий шаг —
	// запись объекта), а ноль в пороге означал бы «сегмент пустой» — бесконечный
	// цикл воркера на пустой выборке.
	if c.Journal.Segment.MaxEvents < 1 {
		add("journal.segment.max_events: %d, должно быть >= 1", c.Journal.Segment.MaxEvents)
	}
	if c.Journal.Segment.MaxBytes < 1 {
		add("journal.segment.max_bytes: %v, должно быть >= 1", c.Journal.Segment.MaxBytes)
	}
	if c.Journal.Sealer.EnabledValue() {
		if c.Journal.Sealer.Interval <= 0 {
			add("journal.sealer.interval: %v, должно быть > 0", c.Journal.Sealer.Interval)
		}
		if c.Journal.Sealer.TenantsBatch < 1 {
			add("journal.sealer.tenants_batch: %d, должно быть >= 1", c.Journal.Sealer.TenantsBatch)
		}
		if c.Journal.Sealer.SweepInterval <= 0 {
			add("journal.sealer.sweep_interval: %v, должно быть > 0", c.Journal.Sealer.SweepInterval)
		}
		if c.Journal.Sealer.SweepBatch < 1 {
			add("journal.sealer.sweep_batch: %d, должно быть >= 1", c.Journal.Sealer.SweepBatch)
		}
		if c.Journal.Sealer.MaxSegmentsPerTenant < 1 {
			add("journal.sealer.max_segments_per_tenant: %d, должно быть >= 1",
				c.Journal.Sealer.MaxSegmentsPerTenant)
		}
		if c.Journal.Sealer.ShutdownGrace <= 0 {
			add("journal.sealer.shutdown_grace: %v, должно быть > 0", c.Journal.Sealer.ShutdownGrace)
		}
	}

	if c.Deleter.EnabledValue() {
		if c.Deleter.Interval <= 0 {
			add("deleter.interval: %v, должно быть > 0", c.Deleter.Interval)
		}
		if c.Deleter.BatchMaxObjects < 1 {
			add("deleter.batch_max_objects: %d, должно быть >= 1", c.Deleter.BatchMaxObjects)
		}
		if c.Deleter.ClaimTTL <= 0 {
			add("deleter.claim_ttl: %v, должно быть > 0", c.Deleter.ClaimTTL)
		}
		if c.Deleter.RetryBackoff <= 0 {
			add("deleter.retry_backoff: %v, должно быть > 0", c.Deleter.RetryBackoff)
		}
		if c.Deleter.ShutdownGrace <= 0 {
			add("deleter.shutdown_grace: %v, должно быть > 0", c.Deleter.ShutdownGrace)
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
	hasMigratorMover := false
	for name, mv := range c.Phobos.Movers {
		// Ключ проверяется и здесь, не только на пути к переменной окружения:
		// без ограничения два ключа (drive-1 и drive_1) дали бы одну
		// переменную, и правка ушла бы не в тот mover.
		if !keyPattern.MatchString(name) {
			add("phobos.movers: ключ %q: допускаются строчные латинские буквы, цифры и _", name)
		}
		if mv.Addr == "" {
			add("phobos.movers.%s.addr обязателен", name)
		}
		if mv.Node == "" {
			add("phobos.movers.%s.node обязателен: по нему определяется локальная передача (Ф0.4)", name)
		}
		if mv.Library == "" {
			// Идентичность mover'а — имя драйва в SAN-URI (ADR-0021), а не
			// библиотека: библиотека нужна для маршрутизации, по ней
			// планировщик выбирает mover'ы своей библиотеки.
			add("phobos.movers.%s.library обязателен: по нему планировщик выбирает mover'ы своей библиотеки", name)
		}
		switch mv.Role {
		case "migrator", "recaller":
			if mv.Role == "migrator" {
				hasMigratorMover = true
			}
		default:
			add("phobos.movers.%s.role: %q, ожидаются migrator или recaller — воркер выбирает mover'а по роли, а не только по библиотеке", name, mv.Role)
		}
	}
	if hasMigratorMover && c.Phobos.MoverClient.Cert == "" {
		add("phobos.mover_client: cert/key/ca обязательны, в phobos.movers есть mover роли migrator — " +
			"планировщику архивации нечем представиться (SAN-URI tapegw://scheduler, ADR-0021)")
	}
	if hasMigratorMover && c.Deleter.EnabledValue() && c.Phobos.DeleterClient.Cert == "" {
		add("phobos.deleter_client: cert/key/ca обязательны, deleter включён и в phobos.movers есть mover роли migrator — " +
			"удалителю нечем представиться (SAN-URI tapegw://deleter, M6); права на purge не должны жить на сертификате планировщика")
	}

	// Политика копий пула (предложение 0013 §2). Проверяется всё до старта:
	// кривая политика, обнаруженная в момент записи, — это уже испорченная
	// очередь, а не ошибка конфигурации.
	for _, name := range sortedKeys(c.Pools) {
		pw := c.Pools[name]
		if len(pw.Copies) == 0 {
			add("pools.%s.copies: пусто, нужна хотя бы одна спецификация копии", name)
		}
		seenName := map[string]bool{}
		seenTags := map[string]string{}
		for _, cp := range pw.Copies {
			if strings.TrimSpace(cp.Name) == "" {
				add("pools.%s.copies: у копии пустое имя — оно попадёт в spec копии и атрибут tapegw.copy", name)
				continue
			}
			if seenName[cp.Name] {
				add("pools.%s.copies: имя %q повторяется — спецификации копий должны различаться", name, cp.Name)
			}
			seenName[cp.Name] = true
			for _, t := range cp.Tags {
				if strings.TrimSpace(t) == "" {
					add("pools.%s.copies.%s: пустой тег в списке", name, cp.Name)
				}
			}
			// Набор тегов как множество: порядок в YAML не должен превращать
			// «одну и ту же ленту» в «две разные копии».
			key := strings.Join(sortedStrings(cp.Tags), ",")
			if other, dup := seenTags[key]; dup {
				add("pools.%s.copies: копии %q и %q с одинаковыми тегами — обе могут лечь на одну ленту, копирование теряет смысл", name, other, cp.Name)
			}
			seenTags[key] = cp.Name
		}
		// Различие наборов мало: если теги одной копии — подмножество тегов
		// другой (пустой набор — подмножество любого), любая лента, подходящая
		// «широкой» копии, годится и «узкой», и обе копии могут лечь на одну.
		// Равенство сообщено выше как дубль, здесь — собственные подмножества.
		for i := 0; i < len(pw.Copies); i++ {
			for j := i + 1; j < len(pw.Copies); j++ {
				a, b := pw.Copies[i], pw.Copies[j]
				switch {
				case properSubset(a.Tags, b.Tags):
					add("pools.%s.copies: теги копии %q — подмножество тегов %q: лента, подходящая %q, годится и для %q — обе копии могут лечь на одну", name, a.Name, b.Name, b.Name, a.Name)
				case properSubset(b.Tags, a.Tags):
					add("pools.%s.copies: теги копии %q — подмножество тегов %q: лента, подходящая %q, годится и для %q — обе копии могут лечь на одну", name, b.Name, a.Name, a.Name, b.Name)
				}
			}
		}
	}

	switch strings.ToLower(c.Logging.Format) {
	case "json", "text":
	default:
		add("logging.format: %q, ожидаются json или text", c.Logging.Format)
	}

	return errors.Join(errs...)
}

// sortedKeys — детерминированный обход map'а конфига: сообщения об ошибках
// не должны переставляться от запуска к запуску.
func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedStrings(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

// properSubset — строгое подмножество по множествам (дубли внутри списка не
// считаются): A ⊂ B, если у A меньше различных тегов, чем у B, и каждый тег A
// есть у B. Пустое A — подмножество любого непустого B.
func properSubset(a, b []string) bool {
	setA := make(map[string]bool, len(a))
	for _, t := range a {
		setA[t] = true
	}
	setB := make(map[string]bool, len(b))
	for _, t := range b {
		setB[t] = true
	}
	if len(setA) >= len(setB) {
		return false
	}
	for t := range setA {
		if !setB[t] {
			return false
		}
	}
	return true
}

// ValidateMover — обязательное для процесса tapegw-mover (S1). Вызывается из
// cmd/tapegw-mover, а не из Validate: tapegw эти поля не читает, и требовать
// их в общем валидаторе значило бы ломать конфиг тем, кто mover ещё не
// поднимал.
func (c *Config) ValidateMover() error {
	var errs []error
	add := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf(format, args...))
	}

	m := c.Phobos.Mover
	switch c.Phobos.Family {
	case "tape", "dir":
	default:
		add("phobos.family: %q, ожидаются tape или dir", c.Phobos.Family)
	}
	if m.Listen == "" {
		add("phobos.mover.listen обязателен")
	}
	if m.Drive == "" {
		add("phobos.mover.drive обязателен: имя драйва — слот воркера и SAN-URI сертификата")
	}
	switch m.Role {
	case "migrator", "recaller":
	default:
		add("phobos.mover.role: %q, ожидаются migrator или recaller", m.Role)
	}
	if m.TLS.Cert == "" || m.TLS.Key == "" || m.TLS.CA == "" {
		add("phobos.mover.tls: cert, key и ca обязательны — plaintext-режима у mover нет (ADR-0021)")
	}
	if m.StagingDir == "" {
		add("phobos.mover.staging_dir обязателен: батч сначала принимается на локальный диск")
	}
	if m.BufferBytes <= 0 {
		add("phobos.mover.buffer_bytes: %d, должно быть > 0", int64(m.BufferBytes))
	}
	if m.ChunkSize <= 0 {
		add("phobos.mover.chunk_size: %d, должно быть > 0", int64(m.ChunkSize))
	}
	if m.ChunkSize > mover.MaxDataBytes-16 {
		add("phobos.mover.chunk_size: %v, не больше %d байт — потолок кадра данных на проводе (ADR-0021)",
			int64(m.ChunkSize), mover.MaxDataBytes-16)
	}
	if m.Inactivity <= 0 {
		add("phobos.mover.inactivity_timeout: %v, должно быть > 0", m.Inactivity)
	}
	if m.OpTimeout <= m.Inactivity {
		add("phobos.mover.op_timeout: %v, должен быть больше inactivity_timeout: %v — "+
			"иначе запись дольше ожидания кадра обрывалась бы на середине", m.OpTimeout, m.Inactivity)
	}
	if m.ExpireInterval <= 0 {
		add("phobos.mover.expire_interval: %v, должно быть > 0", m.ExpireInterval)
	}
	// Библиотека обязательна: API Phobos не читает профиль, и без явной
	// library put падает с -ENODEV без внятной ошибки (ADR-0007, M0-02).
	if c.Phobos.Library == "" {
		add("phobos.library обязателен: API Phobos не читает профиль (ADR-0007)")
	}
	// Встроенные реплики Phobos запрещены (ADR-0022): копии — отдельные
	// объекты tapegw, каждая со своими тегами, а raid1 с repl_count > 1
	// пишет молча одинаковые реплики с одними тегами. Дефолт repl_count="1"
	// задан в Default(), но оператор, задающий свои layout_params, заменяет
	// карту целиком — и дефолт исчезает вместе с repl_count. Тогда Phobos
	// берёт свои две реплики: на ci-dir это уже давало ENODEV с одним
	// носителем и тихие две реплики с двумя. Отказ на старте, не в первом put.
	if rc := c.Phobos.LayoutParams["repl_count"]; rc != "1" {
		add(`phobos.layout_params.repl_count = %q, обязано быть "1": встроенные реплики Phobos запрещены, `+
			"копии — отдельные объекты tapegw (ADR-0022)", rc)
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
