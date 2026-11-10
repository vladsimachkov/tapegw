package meta_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// Страж того, ради чего делалась миграция 0028 (ADR-0023, поправка).
//
// Пока плотный номер выдавал счётчик на строке тенанта, все журналируемые
// изменения одного тенанта сериализовались на этой строке: лок держится до
// коммита, транзакции выстраиваются в очередь и каждая делает свой fsync в
// одиночестве. Измерено: ≈1000 продлений и ≈900 удалений в секунду на тенанта,
// из них x9…x10 — именно эта строка. Для бэкап-софта это потолок: чистка
// устаревших копий — миллионы ключей, и пока она идёт, одиночное продление той
// же учётки ждёт в той же очереди.
//
// С 0028 событие получает только служебный id из sequence и строку тенанта не
// трогает, поэтому путь (a) больше не сериализован и обязан сравняться с
// безжурнальным по цене. Это и есть утверждение теста: журнал не должен
// превращаться в общую точку сериализации. Тест станет красным, если кто-то
// вернёт в путь записи `UPDATE tenants … RETURNING` или заведёт другой лок на
// тенанта — зелёные юнит-тесты журнала про такую регрессию не скажут ничего.
//
// Пути, которые сравнивает тест (один металл, один размер пула):
//
//	a) штатный PutObjectRetention — событие журнала в той же транзакции;
//	b) тот же переход метаданных без события — запрос выбран так же, как в
//	   findLockVersion (join с бакетом, фильтр по тенанту и состоянию,
//	   FOR UPDATE OF o), чтобы в цену журнала не попал лишний join;
//	c) массовое удаление (DeleteObject без versionId → маркер удаления) —
//	   именно этот путь делает потолок заметным в бою, а не продления.
//
// Утверждение одно: a к b не больше throughputMaxInsertSlower. Оба пути не
// сериализованы и кормятся групповым коммитом одинаково, поэтому отношение —
// цена самой вставки события, а не латентность fsync, и от машины оно зависит
// слабее, чем абсолютные скорости. Слабее — не «совсем»: десять прогонов этой
// машины, включая `-race`, дали x0.94…x1.48, поэтому порог стоит между этим
// шумом и регрессией в x6.5…x10.1, от которой тест и защищает. Прямая проверка
// того же свойства, без всякой скорости, — `tenants.journal_seq = 0` ниже.
// Абсолютные скорости логируются: они порядок величины и ничего про себя не
// утверждают.
//
// Измерено на dev-машине (Postgres 16 в docker, локальный диск) после 0028 —
// см. вывод теста. Ориентиры до перехода на служебный id: a ≈950–1000,
// b ≈9500, массовое удаление ≈870–930 в секунду.

const (
	throughputObjects = 1000
	throughputWorkers = 16

	// throughputMaxInsertSlower — потолок отношения «без журнала / со
	// служебным id». Это цена вставки события: оба пути не сериализованы и
	// кормятся групповым коммитом одинаково, поэтому отношение зависит от
	// машины слабее, чем абсолютные скорости, — но не равно нулю этой
	// зависимости. Замерено на этой машине десятью прогонами, включая `-race`
	// (так гоняет CI): x0.94…x1.48; один прогон вышел ниже единицы — штатный
	// путь оказался быстрее голого `UPDATE`, и это шум планировщика, а не
	// ускорение. Порог x3 стоит не «вдвое от верхней границы», а между шумом и
	// регрессией: то, от чего этот тест защищает, — сериализация на строке
	// тенанта, а она стоила x6.5…x10.1. Красный порог при зелёной проверке
	// `tenants.journal_seq = 0` — это загруженная машина, а не регрессия.
	throughputMaxInsertSlower = 3.0

	// thrTenant — числовой id тенанта testTenant: события считаются по числу,
	// а поле TenantID не экспортируется.
	thrTenant = 1
)

// retentionJob — одно продление: объект задаётся ключом и версией.
type retentionJob struct {
	key       string
	versionID string
}

// throughputPool — пул с явным числом соединений: иначе pgxpool поставил бы
// свой дефолт (max(4, NumCPU)), и измерялась бы очередь пула, а не то, что
// замеряется.
func throughputPool(t *testing.T, conns int32) *meta.Meta {
	t.Helper()
	u, err := url.Parse(os.Getenv(testdb.DSNEnv))
	if err != nil {
		t.Fatalf("разобрать DSN: %v", err)
	}
	q := u.Query()
	q.Set("search_path", testdb.PackageSchema(t))
	u.RawQuery = q.Encode()

	m, err := meta.Connect(context.Background(), meta.Options{DSN: u.String(), MaxConns: conns})
	if err != nil {
		t.Fatalf("пул на %d соединений: %v", conns, err)
	}
	t.Cleanup(m.Close)
	return m
}

// throughputBucket — бакет с включённым Object Lock и n версиями, и что у них
// id. Версии без retention: путь удаления должен быть чистым.
func throughputBucket(t *testing.T, m *meta.Meta, bucket string, n int) []retentionJob {
	t.Helper()
	ctx := context.Background()
	mustBucket(t, m, bucket)
	if _, err := m.Pool.Exec(ctx,
		`UPDATE buckets SET versioning = 'enabled', object_lock_enabled = true
		  WHERE name = $1`, bucket); err != nil {
		t.Fatalf("включить lock в %s: %v", bucket, err)
	}
	jobs := make([]retentionJob, n)
	for i := range jobs {
		key := fmt.Sprintf("k%04d", i)
		jobs[i] = retentionJob{key: key,
			versionID: insertVersion(t, m, bucket, key, versionOpts{current: true})}
	}
	return jobs
}

// lockVersionStmt — та же форма, что у findLockVersion: join с бакетом, фильтр
// по тенанту и состоянию, блокировка строки объекта. Путь (b) использует её,
// чтобы отличие от (a) сводилось к событию, а не к запросу.
const lockVersionStmt = `SELECT o.id FROM objects o
		   JOIN buckets b ON b.id = o.bucket_id
		  WHERE b.name = $1 AND b.deleted_at IS NULL AND o.key = $2
		    AND o.id = $3::uuid AND o.state NOT IN ('deleting','deleted')
		    AND b.tenant_id = $4
		      FOR UPDATE OF o`

// runJobs гоняет jobs через workers горутин и возвращает время до последнего
// завершённого перехода. Первая ошибка не прерывает замер — иначе одна отказавшая
// операция обнулила бы цифру; она возвращается и проверяется вызывающим.
func runJobs(t *testing.T, workers int, jobs []retentionJob,
	fn func(retentionJob) error) (time.Duration, error) {
	t.Helper()

	var (
		next  int64
		mu    sync.Mutex
		first error
		wg    sync.WaitGroup
	)
	start := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := atomic.AddInt64(&next, 1) - 1
				if i >= int64(len(jobs)) {
					return
				}
				if err := fn(jobs[i]); err != nil {
					mu.Lock()
					if first == nil {
						first = err
					}
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	return time.Since(start), first
}

func TestJournalRetentionThroughput(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	jobs := throughputBucket(t, m, "j-thr", throughputObjects)
	pool := throughputPool(t, throughputWorkers)
	until := time.Now().Add(24 * time.Hour)

	// События обязаны быть на месте: без них «замер» измерял бы пустой путь.
	// Отсчёт от того, что оставило создание бакета (оно тоже событие).
	eventsBefore, err := pool.JournalEventCount(ctx, thrTenant)
	if err != nil {
		t.Fatalf("число событий: %v", err)
	}

	// a) штатный путь: событие журнала в той же транзакции, номер — служебный id.
	withJournal, err := runJobs(t, throughputWorkers, jobs, func(j retentionJob) error {
		return pool.PutObjectRetention(ctx, testTenant, "j-thr", j.key,
			j.versionID, "GOVERNANCE", until)
	})
	if err != nil {
		t.Fatalf("продление с журналом: %v", err)
	}
	eventsAfter, err := pool.JournalEventCount(ctx, thrTenant)
	if err != nil {
		t.Fatalf("число событий: %v", err)
	}
	if eventsAfter-eventsBefore != throughputObjects {
		t.Fatalf("событий %d, хотели %d — измеряли не тот путь",
			eventsAfter-eventsBefore, throughputObjects)
	}
	// Строка тенанта в пути записи не участвует — то самое, что проверяет этот
	// замер, и здесь оно проверяется по факту, а не по отношению скоростей:
	// возвращённый `UPDATE tenants … RETURNING` отношение тоже портило бы, но
	// именно это утверждение объясняет почему.
	if seq := tenantJournalSeq(t, m, thrTenant); seq != 0 {
		t.Errorf("tenants.journal_seq = %d после %d продлений: путь записи снова пишет "+
			"номер на строке тенанта и все изменения тенанта встанут в одну очередь "+
			"(ADR-0023, поправка)", seq, throughputObjects)
	}

	// b) тот же переход метаданных без события.
	withoutJournal, err := runJobs(t, throughputWorkers, jobs, func(j retentionJob) error {
		tx, err := pool.Pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		var id uuid.UUID
		if err := tx.QueryRow(ctx, lockVersionStmt, "j-thr", j.key, j.versionID,
			thrTenant).Scan(&id); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE objects SET retention_mode = $2::retention_mode, retain_until = $3
			  WHERE id = $1`, id, "GOVERNANCE", until); err != nil {
			return err
		}
		return tx.Commit(ctx)
	})
	if err != nil {
		t.Fatalf("продление без журнала: %v", err)
	}

	// c) массовое удаление: чистка устаревших копий — тот путь, на котором
	// потолок виден лучше всего (миллионы ключей вместо редких продлений).
	// Бакет отдельный и без retention: GOVERNANCE на срок до завтра не пустил бы
	// удаление, и вместо замера мы получили бы отказы.
	delJobs := throughputBucket(t, m, "j-thr-del", throughputObjects)
	delBefore, err := pool.JournalEventCount(ctx, thrTenant)
	if err != nil {
		t.Fatalf("число событий до удалений: %v", err)
	}
	deletes, err := runJobs(t, throughputWorkers, delJobs, func(j retentionJob) error {
		_, err := pool.DeleteObject(ctx, testTenant, meta.DeleteObjectInput{
			Bucket: "j-thr-del", Key: j.key})
		return err
	})
	if err != nil {
		t.Fatalf("массовое удаление: %v", err)
	}
	delAfter, err := pool.JournalEventCount(ctx, thrTenant)
	if err != nil {
		t.Fatalf("число событий после удалений: %v", err)
	}
	if delAfter-delBefore != throughputObjects {
		t.Fatalf("событий про удаления %d, хотели %d — измеряли не тот путь",
			delAfter-delBefore, throughputObjects)
	}

	rate := func(d time.Duration) float64 { return float64(throughputObjects) / d.Seconds() }
	withRate, withoutRate, delRate := rate(withJournal), rate(withoutJournal), rate(deletes)
	t.Logf("продления retention: один тенант, %d объектов, %d горутин, пул %d",
		throughputObjects, throughputWorkers, throughputWorkers)
	t.Logf("  a) с журналом (служебный id):  %7.0f/сек  (%v)",
		withRate, withJournal.Round(time.Millisecond))
	t.Logf("  b) без журнала (как было до него): %7.0f/сек  (%v)",
		withoutRate, withoutJournal.Round(time.Millisecond))
	t.Logf("  c) массовое удаление (маркер на ключ): %7.0f/сек  (%v)",
		delRate, deletes.Round(time.Millisecond))
	t.Logf("  цена журнала целиком: x%.2f", withoutRate/withRate)
	t.Logf("  10 млн удалений одного тенанта: %v",
		time.Duration(10_000_000/delRate*float64(time.Second)).Round(time.Minute))

	if withoutRate/withRate > throughputMaxInsertSlower {
		t.Errorf("штатный путь медленнее безжурнального в %.1f раза (%.0f/сек до %.0f/сек) — "+
			"это больше x%.1f. Служебный id ничего не сериализует, поэтому такое отношение "+
			"значит, что в путь записи вернулся общий лок на тенанта (строка tenants, "+
			"advisory-лок тенанта) либо событие пишется отдельной транзакцией (ADR-0023)",
			withoutRate/withRate, withoutRate, withRate, throughputMaxInsertSlower)
	}
}
