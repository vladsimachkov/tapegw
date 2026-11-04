package iam

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/versity/versitygw/auth"

	"github.com/vladsimachkov/tapegw/internal/meta"
)

// Cache — кэш GetUserAccount поверх Service (ADR-0016). GetUserAccount
// вызывается versitygw на **каждом** запросе при проверке подписи, поэтому без
// кэша каждый запрос стоит один SELECT в Postgres.
//
// Инвалидация — LISTEN/NOTIFY, а не TTL. Границы честные и их надо знать:
//
//   - изменение учётки через tapegw (создание, смена секрета, удаление) —
//     отказ или новый секрет видны всем узлам сразу: pg_notify уходит в той же
//     транзакции, что и правка (meta.notifyAccountChanged), а событие
//     доставляется только после коммита;
//   - правка accounts мимо tapegw — ручной SQL или чужой инструмент — события
//     не порождает, она станет видна по истечении cache_ttl. Поэтому TTL
//     обязан быть: без него отозванная руками учётка работала бы без
//     ограничения по времени.
//
// В кэше лежит строка так, как она лежит в БД, — с обёрнутым секретом. Открытый
// секрет появляется только внутри GetUserAccount (см. Service.account), так что
// кэш не продлевает жизнь секрету в памяти процесса.
type Cache struct {
	svc *Service
	ttl time.Duration
	max int
	dsn string

	// mu закрывает entries и gen. Читают рабочие горутины шлюза, пишет
	// листенер и тот, кто кладёт свежее значение.
	//
	// Выбран RWMutex, а не sync.Map и не атомарный указатель на копию карты:
	// чтение — один поиск по map на каждый запрос S3, оно короткое и почти
	// всегда успешное, поэтому параллельные читатели не должны ждать друг
	// друга (RLock). Копией всей карты платить нечем: инвалидации редки, но
	// каждая требовала бы копии всей карты, а sync.Map берёт плату за
	// универсальность на каждом чтении и не даёт атомарно проверить поколение
	// при записи.
	mu      sync.RWMutex
	entries map[string]entry
	// gen — счётчик инвалидаций. Нужен, чтобы не записать устаревшее значение:
	// горутина могла прочитать строку до того, как пришло уведомление, и если
	// за время чтения кэш уже сбросили, её результат протух.
	gen uint64

	// Счётчики атомарные: их инкрементируют на горячем пути, и запирать их
	// тем же mutex, что и карту, означало бы сериализовать читателей ради
	// цифр для метрик.
	stats cacheStatsCounters

	conn    *pgx.Conn
	cancel  context.CancelFunc
	done    chan struct{}
	started bool
}

// entry — одна запись кэша. missing = true означает «учётки нет»: отрицательные
// ответы кэшируются нарочно, иначе перебор неизвестных ключей бил бы в БД на
// каждом запросе. Создание учётки сбрасывает такую запись тем же уведомлением.
type entry struct {
	row     meta.Account
	missing bool
	expires time.Time
}

type cacheStatsCounters struct {
	hits          atomic.Uint64
	misses        atomic.Uint64
	invalidations atomic.Uint64
	flushes       atomic.Uint64
	staleDropped  atomic.Uint64
	refused       atomic.Uint64
}

// CacheStats — снимок счётчиков для метрик (T4) и для тестов. Hit означает
// «ответ взят из кэша»: без него нельзя доказать, что кэш вообще участвует, —
// тест на мгновенный отказ прошёл бы и с полностью выключенным кэшем.
type CacheStats struct {
	Hits          uint64
	Misses        uint64
	Invalidations uint64
	Flushes       uint64
	// StaleDropped — записи, не попавшие в кэш, потому что во время чтения
	// пришла инвалидация.
	StaleDropped uint64
	// Refused — записи, не попавшие в кэш из-за cache_max_entries.
	Refused uint64
}

// CacheOptions — настройки кэша. Значения обязательны, ноль не означает
// «по умолчанию»: выключенный кэш — это отсутствие Cache как класса.
type CacheOptions struct {
	// TTL — страховка на случай правки таблицы мимо tapegw.
	TTL time.Duration
	// Max — потолок числа записей: access_key выбирает клиент, и без потолка
	// перебор случайных ключей раздувал бы память.
	Max int
	// DSN — подключение для LISTEN. Отдельное, а не из пула: соединение
	// листенера занято постоянно и в пуле оно держало бы слот из max_conns.
	DSN string
}

// NewCache оборачивает сервис кэшем. svc обязателен: кэш без источника — это
// «ответы из воздуха».
func NewCache(svc *Service, opts CacheOptions) (*Cache, error) {
	if svc == nil {
		return nil, errors.New("iam: сервис учёток обязателен для кэша")
	}
	if opts.DSN == "" {
		return nil, errors.New("iam: dsn для подписки на изменения обязателен")
	}
	if opts.TTL <= 0 {
		return nil, fmt.Errorf("iam: cache_ttl = %v, должен быть больше нуля", opts.TTL)
	}
	if opts.Max <= 0 {
		return nil, fmt.Errorf("iam: cache_max_entries = %d, должен быть больше нуля", opts.Max)
	}

	return &Cache{
		svc:     svc,
		ttl:     opts.TTL,
		max:     opts.Max,
		dsn:     opts.DSN,
		entries: make(map[string]entry),
	}, nil
}

// Start подписывается на уведомления и только после этого возвращает управление.
// Порядок принципиален: до Start кэш ничего не отдаёт, а значит не может
// запомнить значение, про изменение которого ему уже не скажут.
//
// Ошибка, если подписаться не удалось: шлюз с молча не работающей инвалидацией
// — это «отозванный ключ работает», поэтому лучше не стартовать вовсе.
func (c *Cache) Start(ctx context.Context) error {
	conn, err := c.subscribe(ctx)
	if err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(ctx)
	c.mu.Lock()
	c.conn = conn
	c.cancel = cancel
	c.done = make(chan struct{})
	c.started = true
	c.mu.Unlock()

	go c.watch(runCtx, conn)
	return nil
}

// GetUserAccount — кэшированная версия. Остальные методы auth.IAMService
// отдаются сервису напрямую: они админские и горячего пути не образуют.
func (c *Cache) GetUserAccount(access string) (auth.Account, error) {
	// Root не кэшируется: он и без кэша не читает БД, а его место в кэше
	// только добавило бы путь, где ответ мог бы устареть.
	if access == c.svc.rootAccess {
		return c.svc.GetUserAccount(access)
	}

	now := time.Now()
	c.mu.RLock()
	e, ok := c.entries[access]
	c.mu.RUnlock()
	if ok && now.Before(e.expires) {
		c.stats.hits.Add(1)
		if e.missing {
			return auth.Account{}, auth.ErrNoSuchUser
		}
		return c.svc.account(e.row)
	}

	// Поколение читается до похода в БД: если во время чтения придёт
	// инвалидация, gen изменится и кладка будет отброшена.
	gen := c.generation()
	c.stats.misses.Add(1)

	row, err := c.svc.row(context.Background(), access)
	switch {
	case err == nil:
		c.store(access, entry{row: row, expires: now.Add(c.ttl)}, gen)
		return c.svc.account(row)
	case errors.Is(err, auth.ErrNoSuchUser):
		c.store(access, entry{missing: true, expires: now.Add(c.ttl)}, gen)
		return auth.Account{}, auth.ErrNoSuchUser
	default:
		// Ошибку БД не кэшируем: повтор должен иметь шанс на успех, а
		// «отказ на cache_ttl вперёд» из-за кратковременного сбоя — это
		// испорченный доступ у исправной учётки.
		return auth.Account{}, err
	}
}

// CreateAccount — делегирование сервису. Инвалидацией занимается сам
// meta.notifyAccountChanged: событие уходит в той же транзакции, что и запись,
// поэтому кэш всех узлов, включая этот, сбрасывается надёжно и независимо от
// того, кто именно вызвал операцию.
func (c *Cache) CreateAccount(acc auth.Account) error { return c.svc.CreateAccount(acc) }

// UpdateUserAccount — делегирование, см. CreateAccount.
func (c *Cache) UpdateUserAccount(access string, props auth.MutableProps) error {
	return c.svc.UpdateUserAccount(access, props)
}

// DeleteUserAccount — делегирование, см. CreateAccount.
func (c *Cache) DeleteUserAccount(access string) error { return c.svc.DeleteUserAccount(access) }

// ListUserAccounts — делегирование, см. CreateAccount.
func (c *Cache) ListUserAccounts() ([]auth.Account, error) { return c.svc.ListUserAccounts() }

// ResolveAccounts — делегирование, см. CreateAccount.
func (c *Cache) ResolveAccounts(accessKeyIDs []string) ([]string, error) {
	return c.svc.ResolveAccounts(accessKeyIDs)
}

// Shutdown останавливает листенер и передаёт shutdown сервису.
func (c *Cache) Shutdown() error {
	c.mu.Lock()
	running, cancel, conn, done := c.started, c.cancel, c.conn, c.done
	c.started = false
	c.mu.Unlock()

	if running {
		cancel()
		<-done
		conn.Close(context.Background())
	}
	return c.svc.Shutdown()
}

// Stats — снимок счётчиков.
func (c *Cache) Stats() CacheStats {
	return CacheStats{
		Hits:          c.stats.hits.Load(),
		Misses:        c.stats.misses.Load(),
		Invalidations: c.stats.invalidations.Load(),
		Flushes:       c.stats.flushes.Load(),
		StaleDropped:  c.stats.staleDropped.Load(),
		Refused:       c.stats.refused.Load(),
	}
}

// Len — число записей в кэше. Для тестов и отладочной метрики.
func (c *Cache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}

// InvalidateByAccess — сброс одной записи. Обычный путь инвалидации —
// уведомление из БД; этот вызов нужен админ-API (T4) и тестам.
func (c *Cache) InvalidateByAccess(access string) {
	c.mu.Lock()
	c.gen++
	delete(c.entries, access)
	c.mu.Unlock()
	c.stats.invalidations.Add(1)
}

// InvalidateAll — сброс всего кэша.
func (c *Cache) InvalidateAll() {
	c.mu.Lock()
	c.gen++
	clear(c.entries)
	c.mu.Unlock()
	c.stats.flushes.Add(1)
}

func (c *Cache) generation() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.gen
}

// store кладёт запись, только если за время чтения никто ничего не сбрасывал.
// Без проверки gen гонка выглядела бы так: горутина прочитала активную учётку →
// админ её отозвал, уведомление обработано, кэш пуст → горутина кладёт
// прочитанное. Отзыв потерялся бы на весь cache_ttl.
func (c *Cache) store(access string, e entry, gen uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.gen != gen {
		c.stats.staleDropped.Add(1)
		return
	}
	if len(c.entries) >= c.max {
		// Потолок нужен потому, что ключи выбирает клиент: без него перебор
		// случайных access_key раздувал бы память без предела. LRU здесь не
		// нужен: записи и так коротко живущие, а не держать мусор дешевле,
		// чем вытеснять полезное.
		c.stats.refused.Add(1)
		return
	}
	c.entries[access] = e
}

// subscribe — подключение и LISTEN, после чего кэш сбрасывается целиком.
//
// Порядок «сначала LISTEN, потом сбросить всё» и есть закрытие дыры между
// коммитом и подпиской. Пока подписки нет, уведомления не приходят: транзакция
// могла закоммититься в тот момент, когда слушателя не было. После LISTEN
// любое изменение долетит уведомлением, а всё, что случилось до LISTEN, уже
// видно в БД и будет перечитано после сброса. Окна, в котором изменение
// теряется, не остаётся.
func (c *Cache) subscribe(ctx context.Context) (*pgx.Conn, error) {
	conn, err := pgx.Connect(ctx, c.dsn)
	if err != nil {
		return nil, fmt.Errorf("iam: подключение для уведомлений: %w", err)
	}
	if _, err := conn.Exec(ctx, `LISTEN `+meta.AccountsNotifyChannel); err != nil {
		conn.Close(context.Background())
		return nil, fmt.Errorf("iam: подписка на изменения учёток: %w", err)
	}
	c.InvalidateAll()
	return conn, nil
}

// watch читает уведомления, переподключаясь при обрыве.
func (c *Cache) watch(ctx context.Context, conn *pgx.Conn) {
	defer close(c.done)

	backoff := time.Second
	for {
		n, err := conn.WaitForNotification(ctx)
		if err == nil {
			backoff = time.Second
			if n.Payload == "" {
				c.InvalidateAll()
				continue
			}
			c.InvalidateByAccess(n.Payload)
			continue
		}
		if ctx.Err() != nil {
			return
		}

		// Соединение оборвалось: уведомления в этот момент не приходят, поэтому
		// переподписываемся — subscribe сам сбросит кэш после LISTEN.
		conn.Close(context.Background())
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff *= 2; backoff > 30*time.Second {
				backoff = 30 * time.Second
			}

			next, err := c.subscribe(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				continue
			}
			conn = next
			break
		}
	}
}

var _ auth.IAMService = (*Cache)(nil)
