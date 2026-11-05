package meta

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// Выборка батча для migrator'а (ADR-0012).
//
// Два требования, которые друг другу мешают.
//
// Первое — справедливость. FIFO по next_try_at означает, что тенант с
// многотерабайтным бэклогом занимает очередь целиком, а у остальных архивация
// уезжает за окно online_until: данные, за которые клиент получил 200, уходят на
// ленту позже обещанного. Поэтому «не больше K объектов и B байт одного
// тенанта», а не «просто ORDER BY next_try_at».
//
// Второе — тенантов тысячи и большинство с пустой очередью (ADR-0016), поэтому
// перебирать их нельзя. Round-robin строится только по тем, у кого очередь
// непустая: это row_number() PARTITION BY tenant_id по частичному индексу
// objects_tenant_queue, заложенному ровно под этот запрос.
//
// Почему это два запроса, а не один. FOR UPDATE нельзя совместить с оконной
// функцией, а без локов выборка нечестная: «взял и перевёл» должно быть атомарно
// с «кого брать». Поэтому сначала честная кандидатура без локов, затем
// блокировка уже выбранного через SKIP LOCKED. Кто-то успел перехватить
// кандидата — батч получится меньше, и это нормальный исход SKIP LOCKED, а не
// ошибка: недобор сегодня добирается следующим батчем.

// maxTenantsPerClaim — сколько тенантов осмотреть за одну выборку. Больше —
// значит гонять больше запросов ради батча, который всё равно упрётся в
// MaxObjects раньше.
const maxTenantsPerClaim = 64

// BatchLimits — ограничения одного батча migrator'а.
type BatchLimits struct {
	// MaxObjects — верх по числу объектов в батче.
	MaxObjects int
	// MaxBytes — верх по суммарному размеру батча. Ограничивает память и
	// staging, а НЕ требование «влезть на ленту»: объекты уходят по одному
	// (ADR-0021), и на ленту обязана влезать каждая единица записи отдельно.
	MaxBytes int64
	// PerTenantObjects и PerTenantBytes — доля одного тенанта в батче. Нуль
	// означает «не ограничивать», но нуль по смыслу равен «один тенант может
	// забрать всё», то есть ровно то, от чего ADR-0012 уходит, поэтому нуль
	// трактуется как «взять по умолчанию» — см. DefaultBatchLimits.
	PerTenantObjects int
	PerTenantBytes   int64
}

// DefaultBatchLimits — доли по умолчанию: ни один тенант не занимает больше
// четверти батча ни по числу, ни по байтам.
func DefaultBatchLimits(maxObjects int, maxBytes int64) BatchLimits {
	lim := BatchLimits{
		MaxObjects:       maxObjects,
		MaxBytes:         maxBytes,
		PerTenantObjects: maxObjects / 4,
		PerTenantBytes:   maxBytes / 4,
	}
	if lim.PerTenantObjects < 1 {
		lim.PerTenantObjects = 1
	}
	return lim
}

// MigrationCandidate — объект, занятый в батч архивации.
type MigrationCandidate struct {
	ID       uuid.UUID
	BucketID int64
	Bucket   string
	Key      string
	TenantID int64
	Size     int64
	// PoolID — пул, под который объект записывается: теги лент берутся отсюда
	// (ADR-0013), и батч всегда в пределах одного пула.
	PoolID int64
}

// ClaimMigrationBatch выбирает объекты одного пула и переводит их в 'archiving'
// одной транзакцией, возвращая ровно то, что было переведено.
//
// Возвращает пустой срез, если брать нечего или всё уже занято другим воркером.
//
// Транзакция закрывается здесь же: правило 13 запрещает держать открытую
// транзакцию во время вызова mover. Воркер получает список id и дальше работает
// без транзакции, а итог пишет отдельной.
func (m *Meta) ClaimMigrationBatch(ctx context.Context, poolID int64, lim BatchLimits) ([]MigrationCandidate, error) {
	if lim.MaxObjects <= 0 {
		return nil, fmt.Errorf("claim batch: MaxObjects должен быть > 0")
	}
	if lim.PerTenantObjects <= 0 {
		lim.PerTenantObjects = lim.MaxObjects
	}

	tx, err := m.Pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("claim batch: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // после commit ничего не откатывается

	// Шаг 1 — кто вообще ждёт. Перебирать тысячи тенантов нельзя (ADR-0016),
	// поэтому DISTINCT ON по частичному индексу objects_tenant_queue: он
	// останавливается, набрав нужное число различных тенантов, и до пустых
	// очередей не доходит.
	tenants, err := tx.Query(ctx, `
		SELECT DISTINCT ON (tenant_id) tenant_id, next_try_at
		  FROM objects
		 WHERE state = 'pending'
		   AND pool_id = $1
		   AND (next_try_at IS NULL OR next_try_at <= now())
		 ORDER BY tenant_id, next_try_at NULLS FIRST, id
		 LIMIT $2`, poolID, maxTenantsPerClaim)
	if err != nil {
		return nil, fmt.Errorf("claim batch: tenants: %w", err)
	}

	type tenantWait struct {
		id   int64
		wait *time.Time
	}
	var waiting []tenantWait
	for tenants.Next() {
		var tw tenantWait
		if err := tenants.Scan(&tw.id, &tw.wait); err != nil {
			tenants.Close()
			return nil, fmt.Errorf("claim batch: tenants scan: %w", err)
		}
		waiting = append(waiting, tw)
	}
	tenants.Close()
	if err := tenants.Err(); err != nil {
		return nil, fmt.Errorf("claim batch: tenants rows: %w", err)
	}
	if len(waiting) == 0 {
		return nil, nil
	}
	// Кого зовут первым — того, кто ждёт дольше всех. NULL (ещё не было сбоя)
	// считается самым старым: такой объект ждёт с момента записи.
	sort.SliceStable(waiting, func(i, j int) bool {
		a, b := waiting[i].wait, waiting[j].wait
		if a == nil {
			return b != nil
		}
		if b == nil {
			return false
		}
		return a.Before(*b)
	})

	// Шаг 2 — занять по очереди у каждого тенанта.
	//
	// Лок берётся сразу на то, что берём, а не «ранжируем без локов, потом
	// пытаемся занять». Второй вариант на параллельных воркерах даёт ровно
	// обратный эффект: все ранжируют одних и тех же кандидатов, SKIP LOCKED
	// отдаёт их первому, а остальные уходят с пустыми руками, хотя в очереди
	// полно работы. Здесь SKIP LOCKED честно разводит воркеров по разным
	// объектам.
	type picked struct {
		id     uuid.UUID
		tenant int64
		size   int64
	}
	var chosen []picked
	perTenantBytes := map[int64]int64{}
	var total int64
	for _, tw := range waiting {
		if len(chosen) >= lim.MaxObjects {
			break
		}
		rows, err := tx.Query(ctx, `
			SELECT id, size FROM objects
			 WHERE tenant_id = $1 AND state = 'pending' AND pool_id = $2
			   AND (next_try_at IS NULL OR next_try_at <= now())
			 ORDER BY next_try_at NULLS FIRST, created_at, id
			 LIMIT $3
			 FOR UPDATE SKIP LOCKED`, tw.id, poolID, lim.PerTenantObjects)
		if err != nil {
			return nil, fmt.Errorf("claim batch: lock tenant %d: %w", tw.id, err)
		}
		n := 0
		for rows.Next() {
			var id uuid.UUID
			var size int64
			if err := rows.Scan(&id, &size); err != nil {
				rows.Close()
				return nil, fmt.Errorf("claim batch: lock scan: %w", err)
			}
			if len(chosen) >= lim.MaxObjects || n >= lim.PerTenantObjects {
				continue
			}
			// Первый объект тенанта проходит поверх байтовых лимитов. Иначе
			// объект крупнее доли или крупнее всего батча не будет
			// заархивирован никогда: он не пролезет ни в один батч и будет
			// висеть в pending молча.
			if len(chosen) > 0 {
				if lim.MaxBytes > 0 && total+size > lim.MaxBytes {
					continue
				}
				if lim.PerTenantBytes > 0 && perTenantBytes[tw.id]+size > lim.PerTenantBytes {
					continue
				}
			}
			chosen = append(chosen, picked{id: id, tenant: tw.id, size: size})
			perTenantBytes[tw.id] += size
			total += size
			n++
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("claim batch: lock rows: %w", err)
		}
	}
	if len(chosen) == 0 {
		return nil, nil
	}

	ids := make([]uuid.UUID, 0, len(chosen))
	for _, c := range chosen {
		ids = append(ids, c.id)
	}

	// Шаг 3 — перевести занятое в 'archiving'. Условие state = 'pending' здесь
	// страховочное: строки уже залоканы, но переход и занятость должны быть
	// одним шагом — иначе окно «занято, но ещё не archiving» переживает падение
	// процесса как строку, которую никто не берёт и не разбирает.
	rows, err := tx.Query(ctx, `
		WITH moved AS (
			UPDATE objects o SET state = 'archiving'
			  FROM (SELECT unnest($1::uuid[]) AS id) p
			 WHERE o.id = p.id AND o.state = 'pending'
			RETURNING o.id, o.tenant_id, o.size, o.bucket_id, o.key, o.pool_id
		)
		SELECT m.id, m.tenant_id, m.size, m.bucket_id, b.name, m.key, m.pool_id
		  FROM moved m JOIN buckets b ON b.id = m.bucket_id`,
		ids)
	if err != nil {
		return nil, fmt.Errorf("claim batch: claim: %w", err)
	}

	var out []MigrationCandidate
	for rows.Next() {
		var c MigrationCandidate
		if err := rows.Scan(&c.ID, &c.TenantID, &c.Size, &c.BucketID, &c.Bucket, &c.Key, &c.PoolID); err != nil {
			rows.Close()
			return nil, fmt.Errorf("claim batch: claim scan: %w", err)
		}
		out = append(out, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("claim batch: claim rows: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("claim batch: commit: %w", err)
	}
	return out, nil
}

// RequeueArchiving возвращает в 'pending' строки, застрявшие в 'archiving', и
// возвращает число возвращённых.
//
// Вызывается после разбора итога: mover ответил ошибкой, соединение оборвалось
// до ответа, или процесс упал посреди батча.
//
// Условие state = 'archiving' — не украшение: если воркер уже разобрал итог и
// строка ушла в 'archived', повтор этого вызова не должен её откатывать.
//
// Два исхода в двух запросах, потому что они разные. Исчерпавший попытки уходит в
// 'failed' и алертится: крутить его дальше — значит молча сжигать драйвы.
// Остальные получают экспоненциальный next_try_at. attempts растёт только здесь,
// иначе «процесс упал» и «лента кончилась» различались бы по числу попыток, а
// различаться должны только причиной.
//
// base — база экспоненты, из конфига: на стенде с одной лентой её хочется
// поменьше, на боевом — побольше.
// Возвращает (возвращено в pending, ушло в failed). Оба числа нужны вызывающему:
// второе — это счётчик алерта, и молча слить его значит потерять единственный
// сигнал «архивация встала».
func (m *Meta) RequeueArchiving(ctx context.Context, ids []uuid.UUID, reason string,
	maxAttempts int, base time.Duration) (requeued int, failed int, err error) {

	if len(ids) == 0 {
		return 0, 0, nil
	}
	if base <= 0 {
		return 0, 0, fmt.Errorf("requeue: base backoff должен быть > 0")
	}
	secs := int64(base / time.Second)
	if secs < 1 {
		secs = 1
	}

	tx, err := m.Pool.Begin(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("requeue: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // после commit нечего откатывать

	// Потолок экспоненты — сутки: без него attempts=20 означает next_try_at через
	// месяц, и «объект дождётся лент» превращается в «объект забыт».
	exp := fmt.Sprintf(`LEAST(86400, %d * power(2, LEAST(10, attempts))::bigint)`, secs)

	failedTag, err := tx.Exec(ctx, `
		UPDATE objects
		   SET state = 'failed', last_error = $2, retired_at = NULL
		 WHERE id = ANY($1::uuid[])
		   AND state = 'archiving'
		   AND attempts + 1 >= $3`,
		ids, reason, maxAttempts)
	if err != nil {
		return 0, 0, fmt.Errorf("requeue: failed: %w", err)
	}

	requeueTag, err := tx.Exec(ctx, `
		UPDATE objects
		   SET state = 'pending',
		       attempts = attempts + 1,
		       last_error = $2,
		       next_try_at = now() + (make_interval(secs => `+exp+`)::interval)
		 WHERE id = ANY($1::uuid[])
		   AND state = 'archiving'`,
		ids, reason)
	if err != nil {
		return 0, 0, fmt.Errorf("requeue: pending: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, 0, fmt.Errorf("requeue: commit: %w", err)
	}
	return int(requeueTag.RowsAffected()), int(failedTag.RowsAffected()), nil
}
