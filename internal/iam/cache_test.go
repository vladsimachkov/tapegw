package iam_test

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/versity/versitygw/auth"

	"github.com/vladsimachkov/tapegw/internal/iam"
	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// Кэш GetUserAccount (ADR-0016). Главное, что здесь проверяется: отказ после
// отзыва приходит **сразу**, в том же процессе и с тем же кэшем, а не после
// истечения TTL. Поэтому во всех тестах на мгновенность TTL выставлен в час:
// если инвалидация сломается, тест не дождётся отказа и упадёт, вместо того
// чтобы молча позеленеть через час.

// cacheTTL — TTL для тестов на мгновенность. Он заведомо больше времени теста,
// так что зелёный результат может быть заслужен только уведомлением.
const cacheTTL = time.Hour

// newCache — кэш над сервисом с кольцом v1.
func newCache(t *testing.T, e *env, ttl time.Duration) *iam.Cache {
	t.Helper()
	return newCacheOver(t, e.service(t, e.ringV1), ttl, "")
}

// newCacheOver — кэш поверх готового сервиса и запуск подписки.
func newCacheOver(t *testing.T, svc *iam.Service, ttl time.Duration, appName string) *iam.Cache {
	t.Helper()

	cache, err := iam.NewCache(svc, iam.CacheOptions{TTL: ttl, Max: 1000, DSN: cacheDSN(t, appName)})
	if err != nil {
		t.Fatalf("NewCache: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cache.Shutdown()
		cancel()
	})
	if err := cache.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return cache
}

// cacheDSN — DSN схемы пакета, при желании со своим application_name: тесту на
// обрыв нужно найти соединение листенера в pg_stat_activity.
func cacheDSN(t *testing.T, appName string) string {
	t.Helper()

	raw := testdb.DSN(t)
	if appName == "" {
		return raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	q := u.Query()
	q.Set("application_name", appName)
	u.RawQuery = q.Encode()
	return u.String()
}

// waitFor — опрос с дедлайном. Возвращает, наступило ли условие, чтобы тест мог
// отличить «не дождались» от «дождались».
func waitFor(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// warmUntilCached прогревает кэш и убеждается, что значение именно отдаётся из
// кэша (вызов увеличил счётчик попаданий). Без этого тест проверял бы актуальное
// состояние БД и зелёничал бы даже с полностью сломанной инвалидацией: уведомление от
// CreateAccount может прийти уже после прогрева и сбросить прогретую запись.
func warmUntilCached(t *testing.T, c *iam.Cache, access string) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		hits := c.Stats().Hits
		if _, err := c.GetUserAccount(access); err != nil {
			t.Fatalf("прогрев %s: %v", access, err)
		}
		if c.Stats().Hits > hits {
			return
		}
	}
	t.Fatalf("кэш не отдаёт %s из себя: запись либо не кладётся, либо сбрасывается быстрее, чем прогревается", access)
}

// listenerBackendPID ищет бэкенд Postgres, в котором живёт соединение
// листенера, — по application_name из DSN кэша.
func listenerBackendPID(ctx context.Context, e *env, appName string) (int, error) {
	var pid int
	err := e.m.Pool.QueryRow(ctx,
		`SELECT pid FROM pg_stat_activity
		  WHERE application_name = $1 AND pid <> pg_backend_pid()
		  ORDER BY pid LIMIT 1`, appName).Scan(&pid)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("соединение листенера с application_name=%q не найдено", appName)
	}
	return pid, err
}

func accountName(i int) string { return fmt.Sprintf("user-%02d", i) }

// Отозванная (удалённая) через сервис учётка перестаёт входить немедленно,
// хотя кэш живёт час. Именно это требует ADR-0016: «устаревший кэш после
// отзыва ключа — это доступ».
func TestCacheInvalidatesOnDelete(t *testing.T) {
	e := setup(t)
	cache := newCache(t, e, cacheTTL)

	mustCreate(t, cache, "alice", "secret-alice", auth.RoleUser, tenantClient)
	warmUntilCached(t, cache, "alice")
	if got, err := cache.GetUserAccount("alice"); err != nil || got.Secret != "secret-alice" {
		t.Fatalf("до отзыва: %+v err=%v", got, err)
	}

	if err := cache.DeleteUserAccount("alice"); err != nil {
		t.Fatalf("удаление: %v", err)
	}

	if !waitFor(5*time.Second, func() bool {
		_, err := cache.GetUserAccount("alice")
		return errors.Is(err, auth.ErrNoSuchUser)
	}) {
		stats := cache.Stats()
		t.Fatalf("после удаления вход по-прежнему проходит; инвалидаций: %d, сбросов: %d — "+
			"уведомление не дошло, а по времени (TTL час) это не объясняется",
			stats.Invalidations, stats.Flushes)
	}
	if got := cache.Stats().Invalidations; got == 0 {
		t.Error("отказ пришёл, но счётчик инвалидаций пуст — сработало не уведомление")
	}
}

// Смена секрета через сервис видна сразу: старый ключ перестаёт подходить,
// новый подходит. Тот же механизм, что и для отзыва, но на другом пути записи.
func TestCacheInvalidatesOnSecretChange(t *testing.T) {
	e := setup(t)
	cache := newCache(t, e, cacheTTL)
	mustCreate(t, cache, "bob", "old-secret", auth.RoleUser, tenantClient)
	warmUntilCached(t, cache, "bob")

	newSecret := "new-secret"
	if err := cache.UpdateUserAccount("bob", auth.MutableProps{Secret: &newSecret}); err != nil {
		t.Fatalf("смена секрета: %v", err)
	}

	if !waitFor(5*time.Second, func() bool {
		got, err := cache.GetUserAccount("bob")
		return err == nil && got.Secret == newSecret
	}) {
		got, err := cache.GetUserAccount("bob")
		t.Fatalf("старый секрет продолжает отдаваться после смены: %q err=%v", got.Secret, err)
	}
}

// Отрицательный ответ тоже кэшируется (иначе перебор неизвестных ключей бил бы
// в БД), и создание учётки обязано этот ответ снять — иначе «ключ создали, а он
// не работает» провисел бы весь TTL.
func TestCacheInvalidatedByCreate(t *testing.T) {
	e := setup(t)
	cache := newCache(t, e, cacheTTL)

	if _, err := cache.GetUserAccount("carol"); !errors.Is(err, auth.ErrNoSuchUser) {
		t.Fatalf("учётки ещё нет: %v", err)
	}
	if _, err := cache.GetUserAccount("carol"); !errors.Is(err, auth.ErrNoSuchUser) {
		t.Fatalf("повторный запрос: %v", err)
	}
	if hits := cache.Stats().Hits; hits == 0 {
		t.Fatal("отрицательный ответ не кэшируется — перебор ключей будет бить в БД")
	}

	mustCreate(t, cache, "carol", "secret-carol", auth.RoleUser, tenantClient)

	if !waitFor(5*time.Second, func() bool {
		_, err := cache.GetUserAccount("carol")
		return err == nil
	}) {
		t.Fatal("созданная учётка не появилась: отрицательная запись кэша не сброшена")
	}
}

// Правка таблицы мимо tapegw уведомления не порождает — это граница, и её надо
// знать явно. Здесь она проверяется как TTL-граница: сразу после внешней правки
// кэш ещё отдаёт старое значение, а по истечении TTL отказ наступает сам.
//
// Это не проверка мгновенности, а честное ограничение: мгновенно — только
// изменения через tapegw.
func TestCacheTTLBoundsExternalChanges(t *testing.T) {
	e := setup(t)
	cache := newCache(t, e, 200*time.Millisecond)
	mustCreate(t, cache, "dave", "secret-dave", auth.RoleUser, tenantClient)

	// Сначала — дождаться, пока уведомление о создании обработано. Иначе оно
	// прилетело бы уже после прогрева и сбросило бы прогретую запись: тест
	// проверял бы отсутствие кэша, а не TTL-границу.
	warmUntilCached(t, cache, "dave")

	// Внешняя правка: прямой SQL, никакого pg_notify.
	if _, err := e.m.Pool.Exec(context.Background(),
		`UPDATE accounts SET status = 'revoked', secret_wrapped = NULL WHERE access_key = 'dave'`); err != nil {
		t.Fatalf("отзыв: %v", err)
	}

	// Сразу после правки кэш ещё держит старое значение — ровно то, что
	// записано в описании cache_ttl.
	if _, err := cache.GetUserAccount("dave"); err != nil {
		t.Errorf("сразу после внешней правки кэш обязан ещё отдавать старое значение: %v", err)
	}

	// Но TTL — не декорация: по истечении отказ наступает сам.
	if !waitFor(5*time.Second, func() bool {
		_, err := cache.GetUserAccount("dave")
		return errors.Is(err, auth.ErrNoSuchUser)
	}) {
		t.Fatal("TTL не работает: внешняя правка так и не стала видна — без " +
			"мгновенной инвалидации это значит «отозванный ключ работает вечно»")
	}
}

// Ротация KEK не должна ломаться из-за кэша. Моделируется штатный путь:
// учётка заведена кольцом v1, процесс перезапущен с кольцом {1,2} и активной
// версией 2 (кольцо в живом процессе не меняется — KEK читают на старте),
// кэш холодный. Проверены оба пути: промах и попадание.
func TestCacheKekRotationAcrossRestart(t *testing.T) {
	e := setup(t)
	mustCreate(t, e.serviceV1(t), "erin", "secret-erin", auth.RoleUser, tenantClient)

	ringV2, err := iam.NewKeyRing(2, map[int][]byte{1: keyV1, 2: keyV2})
	if err != nil {
		t.Fatalf("ring v2: %v", err)
	}
	svcV2, err := iam.New(e.m, ringV2, iam.Options{RootAccess: rootAccess})
	if err != nil {
		t.Fatalf("iam.New: %v", err)
	}
	cache := newCacheOver(t, svcV2, cacheTTL, "")

	// Промах по кэшу: строка читается заново и разворачивается по kek_version.
	got, err := cache.GetUserAccount("erin")
	if err != nil {
		t.Fatalf("после ротации, промах по кэшу: %v", err)
	}
	if got.Secret != "secret-erin" {
		t.Fatalf("секрет после ротации не тот: %q", got.Secret)
	}

	// Попадание в кэш: разворота «про запас» нет, но и кэш не должен
	// возвращать то, чего не развернул бы сервис.
	warmUntilCached(t, cache, "erin")
	got, err = cache.GetUserAccount("erin")
	if err != nil {
		t.Fatalf("после ротации, попадание в кэш: %v", err)
	}
	if got.Secret != "secret-erin" {
		t.Fatalf("из кэша после ротации секрет не тот: %q", got.Secret)
	}
}

// Та самая дыра «коммит случился, а слушателя не было». Проверяется честно:
// соединение листенера рвётся со стороны сервера, в этот момент учётку меняют
// через сервис (уведомление уходит в пустоту), и спастись кэш может только
// одним — полным сбросом при переподписке. TTL здесь час, надежды на него нет.
//
// Окно «слушателя нет» нельзя ни гарантировать, ни проспать: листенер
// переподписывается по бэкоффу, и если это случится между обрывом и изменением,
// уведомление дойдёт, кэш опустеет сам — и тест зелёничал бы без всякой
// инвалидации. Поэтому тест не предполагает окно, а **требует** его: после
// изменения он читает сквозь кэш и смотрит, остался ли устаревший отпечаток.
// Не остался — окно не открылось, попытка повторяется (с новым ключом: отзыв —
// надгробие, тем же access_key его не создать). Если окно не откроется ни в
// одной попытке, тест падает, а не проходит молча.
func TestCacheResubscribeFlushesAfterLostEvent(t *testing.T) {
	// Имя обязано быть уникальным на запуск. Тест ищет соединение листенера в
	// pg_stat_activity по application_name, а имя «просто константа» собирает в
	// pg_stat_activity листенеры предыдущих прогонов того же пакета
	// (go test -count=2, -race -count=3): pg_terminate_backend рвёт чужое
	// соединение, «обрыв» не наступает, и тест падает с сообщением про
	// листенера, хотя дело в порядке прогонов.
	appName := fmt.Sprintf("tapegw-iam-cache-test-%d", time.Now().UnixNano())
	// Границы нужны, чтобы тест не превращался в бесконечное ожидание на
	// медленном раннере: попыток — фиксированное число, и суммарно они уложены
	// в бюджет. За пределами бюджета тест падает с внятным сообщением, а не
	// висит до таймаута CI.
	//
	// Бюджет не может быть меньше стоимости одной попытки: листенер после обрыва
	// поднимается по бэкоффу (1с, далее удвоение), и на второй итерации цикла
	// его ещё нет в pg_stat_activity в тот момент, когда тест хочет его
	// оборвать. Требовать соединение немедленно — значит падать на второй
	// попытке на медленном раннере; ровно так CI и поймал этот тест.
	const attempts = 5
	const listenerWait = 15 * time.Second
	const budget = 120 * time.Second
	deadline := time.Now().Add(budget)

	e := setup(t)
	cache := newCacheOver(t, e.service(t, e.ringV1), cacheTTL, appName)

	opened := 0
	for i := 0; i < attempts && time.Now().Before(deadline); i++ {
		access := fmt.Sprintf("frank%d", i)
		mustCreate(t, cache, access, "secret", auth.RoleUser, tenantClient)
		warmUntilCached(t, cache, access)

		var pid int
		var lastErr error
		// Соединение листенера надо ДОЖДАТЬСЯ, а не зафиксировать его отсутствие
		// как ошибку: между предыдущим pg_terminate_backend и этой попыткой
		// прошло меньше секунды бэкоффа.
		if !waitFor(listenerWait, func() bool {
			p, err := listenerBackendPID(context.Background(), e, appName)
			if err != nil {
				lastErr = err
				return false
			}
			pid = p
			return true
		}) {
			t.Fatalf("попытка %d: найти соединение листенера за %v: %v", i, listenerWait, lastErr)
		}
		if _, err := e.m.Pool.Exec(context.Background(),
			`SELECT pg_terminate_backend($1)`, pid); err != nil {
			t.Fatalf("попытка %d: оборвать соединение: %v", i, err)
		}
		if !waitFor(5*time.Second, func() bool {
			_, err := listenerBackendPID(context.Background(), e, appName)
			return err != nil
		}) {
			t.Fatalf("попытка %d: соединение листенера не оборвалось", i)
		}

		if err := cache.DeleteUserAccount(access); err != nil {
			t.Fatalf("попытка %d: отзыв: %v", i, err)
		}

		// Открылось ли окно: кэш обязан всё ещё отдавать отозванную учётку.
		if _, err := cache.GetUserAccount(access); err == nil {
			opened++
			// Смысл теста: устаревшую запись снимает только сброс при
			// переподписке — уведомление пришло, когда слушателя не было.
			if !waitFor(15*time.Second, func() bool {
				_, err := cache.GetUserAccount(access)
				return errors.Is(err, auth.ErrNoSuchUser)
			}) {
				t.Fatalf("попытка %d: после обрыва и переподписки кэш отдаёт отозванную "+
					"учётку: переподписка не сбрасывает кэш целиком, и событие, пришедшее "+
					"в окно между обрывом и LISTEN, потеряно (сбросов: %d)",
					i, cache.Stats().Flushes)
			}
			break
		}
		// Окно закрылось до изменения — пробуем снова.
	}

	if opened == 0 {
		t.Fatalf("за %d попыток (бюджет %v) ни разу не открылось окно без слушателя: "+
			"тест не может проверить сброс при переподписке и зелёным быть не должен",
			attempts, budget)
	}
}

// Root не должен оставлять след в кэше: он и без него не читает БД.
func TestCacheRootBypassesCache(t *testing.T) {
	e := setup(t)
	cache := newCache(t, e, cacheTTL)

	if _, err := cache.GetUserAccount(rootAccess); !errors.Is(err, auth.ErrNoSuchUser) {
		t.Errorf("root: %v, хотели ErrNoSuchUser", err)
	}
	if n := cache.Len(); n != 0 {
		t.Errorf("root оставил %d записей в кэше", n)
	}
	if s := cache.Stats(); s.Hits+s.Misses != 0 {
		t.Errorf("root прошёл через кэш: %+v", s)
	}
}

// Ошибку БД кэшировать нельзя: иначе кратковременный сбой превращается в
// «доступа нет весь TTL» у исправной учётки.
func TestCacheDoesNotCacheErrors(t *testing.T) {
	e := setup(t)
	cache := newCache(t, e, cacheTTL)
	mustCreate(t, cache, "grace", "secret-grace", auth.RoleUser, tenantClient)

	// Пул сервиса закрываем: каждый последующий запрос — ошибка БД.
	e.m.Pool.Close()

	if _, err := cache.GetUserAccount("grace"); err == nil {
		t.Fatal("ожидали ошибку закрытого пула")
	}
	if n := cache.Len(); n != 0 {
		t.Errorf("ошибка закеширована: %d записей", n)
	}
	if _, err := cache.GetUserAccount("grace"); err == nil {
		t.Error("повторный запрос обязан снова попробовать БД, а не отдать закешированную ошибку")
	}
}

// Гонки: кэш читают десятки горутин, инвалидация приходит из листенера, а
// учётки в это время удаляются и создаются. go test -race обязан быть чистым.
func TestCacheConcurrentReadsAndWrites(t *testing.T) {
	e := setup(t)
	cache := newCache(t, e, 50*time.Millisecond)

	const accounts = 8
	for i := 0; i < accounts; i++ {
		mustCreate(t, cache, accountName(i), "initial", auth.RoleUser, tenantClient)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				// Ошибки ожидаемы: учётки в этот момент удаляют и создают.
				_, _ = cache.GetUserAccount(accountName(i % accounts))
			}
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			access := accountName(i % accounts)
			secret := "rotated"
			// Раньше здесь был цикл «удалить и создать заново». С отзывом-
			// надгробием (план PR#2a п. 0а) такой пары больше не существует:
			// отозванный ключ нельзя выпустить повторно, и CreateAccount
			// закономерно отвечает ErrUserExists. Гонку кэша это не ослабляет —
			// отказ по UPDATE остаётся самым частым видом инвалидации, а
			// немедленность отзыва проверяет отдельный тест.
			if err := cache.UpdateUserAccount(access, auth.MutableProps{Secret: &secret}); err != nil {
				t.Errorf("%s: ротация секрета: %v", access, err)
				return
			}
		}
	}()

	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()

	// Кэш обязан остаться связным: после остановки все учётки на месте и
	// читаются, а не отдают мусор из гонки.
	for i := 0; i < accounts; i++ {
		if _, err := cache.GetUserAccount(accountName(i)); err != nil {
			t.Errorf("%s: %v", accountName(i), err)
		}
	}
}
