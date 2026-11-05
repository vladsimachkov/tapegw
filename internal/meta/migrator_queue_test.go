package meta_test

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// Выборка батча архивации (ADR-0012).
//
// Проверяется не «запрос что-то вернул», а конкретные обещания планирования:
// один тенант не вытесняет остальных, батч не выходит за лимиты, занятые
// объекты не достаются второму воркеру, и объект крупнее лимита не зависает в
// pending навсегда.

// tenantWithObjects заводит тенанта, его бакет и n pending-объектов заданного
// размера. Возвращает id объектов в порядке вставки.
func tenantWithObjects(t *testing.T, m *meta.Meta, tenantID int64, bucket string, size int64, n int) []uuid.UUID {
	t.Helper()
	ctx := context.Background()

	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO tenants (id, name, pool_id) VALUES ($1, $2, 1)
		 ON CONFLICT (id) DO NOTHING`, tenantID, "tenant-"+bucket); err != nil {
		t.Fatalf("тенант %d: %v", tenantID, err)
	}
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO buckets (name, tenant_id, owner, acl, versioning)
		  VALUES ($1, $2, 'o', '{}'::jsonb, 'unversioned')`, bucket, tenantID); err != nil {
		t.Fatalf("бакет %s: %v", bucket, err)
	}

	var bucketID int64
	if err := m.Pool.QueryRow(ctx, `SELECT id FROM buckets WHERE name = $1`, bucket).Scan(&bucketID); err != nil {
		t.Fatalf("id бакета %s: %v", bucket, err)
	}

	var ids []uuid.UUID
	for i := 0; i < n; i++ {
		id := uuid.New()
		if _, err := m.Pool.Exec(ctx,
			`INSERT INTO objects (id, bucket_id, key, size, etag, storage_class, state,
			                      landing, online_until, tenant_id, pool_id, is_current,
			                      wrapped_dek)
			  VALUES ($1,$2,$3,$4,'etag','GLACIER','pending',true,
			          now() + interval '1 day', $5, 1, true, 'x'::bytea)`,
			id, bucketID, fmtKey(tenantID, i), size, tenantID); err != nil {
			t.Fatalf("объект %d/%d: %v", tenantID, i, err)
		}
		ids = append(ids, id)
	}
	return ids
}

func fmtKey(tenantID int64, i int) string {
	return "k-" + strconv.FormatInt(tenantID, 10) + "-" + strconv.Itoa(i)
}

func countState(t *testing.T, m *meta.Meta, state string) int {
	t.Helper()
	var n int
	if err := m.Pool.QueryRow(context.Background(),
		`SELECT count(*) FROM objects WHERE state = $1::obj_state`, state).Scan(&n); err != nil {
		t.Fatalf("счётчик %s: %v", state, err)
	}
	return n
}

// Батч набирается в пределах одного пула: батч всегда пишется на ленты одного
// пула (ADR-0013), и смешать пулы — значит выдать объекту чужой тег.
func TestClaimBatchSinglePool(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	tenantWithObjects(t, m, 2, "pool-a", 10, 5)
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO pools (id, name, phobos_tag, repl_count, kind)
		  VALUES (2, 'other', 'pool-other', 1, 'shared')`); err != nil {
		t.Fatalf("второй пул: %v", err)
	}
	// Два объекта переезжают в другой пул: они не должны попасть в батч пула 1.
	if _, err := m.Pool.Exec(ctx,
		`UPDATE objects SET pool_id = 2
		  WHERE id IN (SELECT id FROM objects ORDER BY id LIMIT 2)`); err != nil {
		t.Fatalf("перенос в пул 2: %v", err)
	}

	got, err := m.ClaimMigrationBatch(ctx, 1, meta.BatchLimits{MaxObjects: 10, PerTenantObjects: 10})
	if err != nil {
		t.Fatalf("ClaimMigrationBatch: %v", err)
	}
	if len(got) != 3 {
		t.Errorf("взято %d объектов, хотели 3 из пула 1", len(got))
	}
	for _, c := range got {
		if c.PoolID != 1 {
			t.Errorf("в батче объект пула %d", c.PoolID)
		}
	}
}

// Один тенант не вытесняет остальных: при лимите «половина батча на тенанта»
// малочисленный тенант обязан попасть в тот же батч, а не ждать, пока бэклог
// первого разгребут.
func TestClaimBatchFairness(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	tenantWithObjects(t, m, 2, "fat-tenant", 10, 20)
	small := tenantWithObjects(t, m, 3, "thin-tenant", 10, 2)

	lim := meta.BatchLimits{MaxObjects: 8, PerTenantObjects: 4}
	got, err := m.ClaimMigrationBatch(ctx, 1, lim)
	if err != nil {
		t.Fatalf("ClaimMigrationBatch: %v", err)
	}
	// 4 от жирного тенанта (его потолок) плюс все 2 тонкого: меньше лимита по
	// числу, и это правильно — справедливость важнее заполнения батча.
	if len(got) != 6 {
		t.Fatalf("взято %d, хотели 6 (4 + 2)", len(got))
	}

	byTenant := map[int64]int{}
	for _, c := range got {
		byTenant[c.TenantID]++
	}
	if byTenant[2] > lim.PerTenantObjects {
		t.Errorf("тенант 2 занял %d из %d: ограничение доли не работает", byTenant[2], len(got))
	}
	if byTenant[3] != 2 {
		t.Errorf("тенант 3 получил %d объектов, хотели все 2: его вытеснил бэклог тенанта 2", byTenant[3])
	}
	_ = small
}

// Доля тенанта ограничена и по байтам: тенант с десятью объектами по 20 GiB
// забирает батч целиком, формально не нарушив лимит по числу.
func TestClaimBatchPerTenantBytes(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	tenantWithObjects(t, m, 2, "big-objects", 1000, 10)
	tenantWithObjects(t, m, 3, "small-objects", 100, 10)

	lim := meta.BatchLimits{MaxObjects: 10, MaxBytes: 5000, PerTenantObjects: 10, PerTenantBytes: 2000}
	got, err := m.ClaimMigrationBatch(ctx, 1, lim)
	if err != nil {
		t.Fatalf("ClaimMigrationBatch: %v", err)
	}

	perTenant := map[int64]int64{}
	for _, c := range got {
		perTenant[c.TenantID] += c.Size
	}
	for tid, sum := range perTenant {
		if sum > lim.PerTenantBytes {
			t.Errorf("тенант %d занял %d байт при лимите %d", tid, sum, lim.PerTenantBytes)
		}
	}
	if len(perTenant) < 2 {
		t.Errorf("в батче только тенанты %v: лимит по байтам должен был пустить второго", perTenant)
	}
}

// Объект крупнее суммарного лимита батча обязан браться один. Иначе он не
// попадёт в батч никогда и будет висеть в pending вечно — молча.
func TestClaimBatchOversizedObjectIsStillTaken(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	tenantWithObjects(t, m, 2, "huge", 10_000, 1)

	lim := meta.BatchLimits{MaxObjects: 5, MaxBytes: 100, PerTenantObjects: 5, PerTenantBytes: 100}
	got, err := m.ClaimMigrationBatch(ctx, 1, lim)
	if err != nil {
		t.Fatalf("ClaimMigrationBatch: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("взято %d, хотели 1: объект крупнее лимита не будет заархивирован никогда", len(got))
	}
}

// Занятые объекты не достаются второму воркеру, и переход в 'archiving'
// совершён до возврата: правило 13 требует, чтобы к моменту вызова mover
// транзакции уже не было.
func TestClaimBatchTransitionsAndExcludes(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	tenantWithObjects(t, m, 2, "claim", 10, 5)
	lim := meta.BatchLimits{MaxObjects: 3, PerTenantObjects: 3}

	first, err := m.ClaimMigrationBatch(ctx, 1, lim)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 3 {
		t.Fatalf("первый батч: %d", len(first))
	}
	if n := countState(t, m, "archiving"); n != 3 {
		t.Errorf("в archiving %d строк, хотели 3: переход обязан быть закоммичен до возврата", n)
	}
	if n := countState(t, m, "pending"); n != 2 {
		t.Errorf("в pending %d, хотели 2", n)
	}

	second, err := m.ClaimMigrationBatch(ctx, 1, lim)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 2 {
		t.Errorf("второй батч взял %d, хотели 2: занятые объекты не должны повторяться", len(second))
	}

	third, err := m.ClaimMigrationBatch(ctx, 1, lim)
	if err != nil {
		t.Fatal(err)
	}
	if len(third) != 0 {
		t.Errorf("третий батч взял %d объектов, хотели ноль", len(third))
	}
}

// Параллельная выборка не должна отдавать один объект двум воркерам.
func TestClaimBatchConcurrent(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	// Четыре тенанта по десять объектов: с одним тенантом потолок батча равен
	// его собственной доле, и проверять нечего.
	for tid := int64(2); tid <= 5; tid++ {
		tenantWithObjects(t, m, tid, fmt.Sprintf("race-%d", tid), 10, 10)
	}

	const workers = 4
	lim := meta.BatchLimits{MaxObjects: 5, PerTenantObjects: 5}

	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		seen  = map[uuid.UUID]int{}
		total int
	)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := m.ClaimMigrationBatch(ctx, 1, lim)
			if err != nil {
				t.Errorf("выборка: %v", err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			total += len(got)
			for _, c := range got {
				seen[c.ID]++
			}
		}()
	}
	wg.Wait()

	for id, n := range seen {
		if n != 1 {
			t.Fatalf("объект %s выдан %d раз: SKIP LOCKED не сработал", id, n)
		}
	}
	if total != 20 {
		t.Errorf("в сумме взято %d, хотели 20 (4 воркера по 5)", total)
	}
}

// Не пришло время — не берём. next_try_at в будущем означает «рано»: экспонента
// после сбоя без этой проверки превратилась бы в spins на отказной ленте.
func TestClaimBatchRespectsNextTryAt(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	tenantWithObjects(t, m, 2, "later", 10, 3)
	if _, err := m.Pool.Exec(ctx,
		`UPDATE objects SET next_try_at = now() + interval '1 hour'`); err != nil {
		t.Fatal(err)
	}
	got, err := m.ClaimMigrationBatch(ctx, 1, meta.BatchLimits{MaxObjects: 5, PerTenantObjects: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("взято %d объектов раньше next_try_at", len(got))
	}

	if _, err := m.Pool.Exec(ctx, `UPDATE objects SET next_try_at = now() - interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	got, err = m.ClaimMigrationBatch(ctx, 1, meta.BatchLimits{MaxObjects: 5, PerTenantObjects: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Errorf("после наступления срока взято %d, хотели 3", len(got))
	}
}

// Возврат в pending: attempts растёт, next_try_at уходит в будущее, состояние
// меняется только у тех, кто ещё в archiving.
func TestRequeueArchiving(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	ids := tenantWithObjects(t, m, 2, "requeue", 10, 2)
	if _, err := m.Pool.Exec(ctx,
		`UPDATE objects SET state = 'archiving' WHERE id = ANY($1::uuid[])`, ids); err != nil {
		t.Fatal(err)
	}

	requeued, failed, err := m.RequeueArchiving(ctx, ids, "mover: no space", 5, time.Second)
	if err != nil {
		t.Fatalf("RequeueArchiving: %v", err)
	}
	if requeued != 2 || failed != 0 {
		t.Fatalf("requeued=%d failed=%d, хотели 2 и 0", requeued, failed)
	}

	var attempts int
	var future bool
	if err := m.Pool.QueryRow(ctx,
		`SELECT attempts, next_try_at > now() FROM objects WHERE id = $1`, ids[0]).
		Scan(&attempts, &future); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 {
		t.Errorf("attempts = %d, хотели 1", attempts)
	}
	if !future {
		t.Errorf("next_try_at в прошлом: экспонента не работает, и воркер закрутится")
	}

	// Повторный вызов на уже возвращённых строках не должен трогать их снова:
	// условие state = 'archiving' — единственное, что отличает «разобрали итог»
	// от «разбираем второй раз».
	requeued, _, err = m.RequeueArchiving(ctx, ids, "ещё раз", 5, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if requeued != 0 {
		t.Errorf("повторный возврат тронул %d строк, хотели 0", requeued)
	}
}

// Исчерпанный attempts — это 'failed' и алерт, а не вечный цикл. Молча
// возвращать такой объект в pending значит сжигать драйвы до бесконечности.
func TestRequeueArchivingExhaustsToFailed(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	ids := tenantWithObjects(t, m, 2, "exhaust", 10, 2)
	if _, err := m.Pool.Exec(ctx,
		`UPDATE objects SET state = 'archiving', attempts = 4 WHERE id = ANY($1::uuid[])`, ids); err != nil {
		t.Fatal(err)
	}

	requeued, failed, err := m.RequeueArchiving(ctx, ids, "нет лент", 5, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if requeued != 0 || failed != 2 {
		t.Errorf("requeued=%d failed=%d, хотели 0 и 2", requeued, failed)
	}
	var state string
	if err := m.Pool.QueryRow(ctx, `SELECT state::text FROM objects WHERE id = $1`, ids[0]).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "failed" {
		t.Errorf("состояние %q, хотели failed", state)
	}
}
