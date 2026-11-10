package meta_test

import (
	"context"
	"encoding/json"
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

// Замер цены плотной нумерации (после ревью PR #136).
//
// Строка тенанта — общая точка сериализации всех журналируемых изменений:
// `journal_seq` выдаётся `UPDATE tenants … RETURNING`, и блокировка строки
// держится до коммита. Значит изменения одного тенанта, которые пишут событие,
// выполняются строго по одному, сколько бы соединений ни было. Для редких правок
// конфигурации это незаметно, но журнал заточен под бэкап-софт с Object Lock,
// где retention продлевают постоянно и пачками, — и вот тогда узким местом
// может оказаться одна строка.
//
// Три пути на одном железе и на одном размере пула:
//
//	a) штатный PutObjectRetention — событие журнала в той же транзакции;
//	b) тот же переход метаданных без события (SELECT … FOR UPDATE + UPDATE +
//	   commit) — то, что делал код до журнала;
//	c) то же событие, но номер берётся из обычной последовательности и строка
//	   тенанта не трогается — вариант, который обсуждается для Ж2.
//
// Измерено 2026-11-10 (Postgres 16 в docker на локальном диске, 16 горутин,
// пул 16; между прогонами отношение гуляет, x7…x8.4):
//
//	a)  965/сек   b) 6703/сек   c) 6012/сек
//	цена журнала x6.95, из них сама вставка события x1.12, строка тенанта x6.23.
//
// То есть дорожает не журнал как таковой — лишняя вставка стоит 12 %, — а
// именно блокировка строки тенанта. Механизм не в самом UPDATE, а в том, что
// локи держится до коммита: транзакции выстраиваются в очередь и каждая делает
// свой fsync в гордом одиночестве, группового коммита не получается.
//
// Что здесь проверяется, а что нет. Абсолютная скорость зависит от машины,
// поэтому тест про неё ничего не утверждает: утверждается только соотношение a
// к b, и с большим запасом от измеренного — чтобы ловить скачок (локи,
// удерживаемые через сеть, лишний обход таблицы на событие), а не шум раннера.
// Решение по варианту (c) принимает не этот тест: он даёт цифру, на которую
// смотрят при выборе схемы Ж2, и она записана в ADR-0023.
//
// Сравнение a к b смещено в сторону «журнал хуже»: путь (b) ещё и проще, чем
// был бы до-журнальный PutObjectRetention (версию он выбирает одним запросом, а
// не вместе с бакетом), — отношение выше, чем было бы при честной паре.

const (
	throughputObjects = 1000
	throughputWorkers = 16

	// throughputMaxSlower — потолок отношения «без журнала / с журналом».
	// Измерено x7…x8.4, здесь запас вдвое против верхней границы: тест обязан
	// падать на скачке, а не на шуме.
	throughputMaxSlower = 20.0

	// thrTenant — числовой id тенанта testTenant: счётчик журнала спрашивается
	// по числу, а поле TenantID не экспортируется.
	thrTenant = 1
)

// retentionJob — одно продление: объект задаётся ключом и версией.
type retentionJob struct {
	key       string
	versionID string
}

// throughputPool — пул с явным числом соединений: иначе pgxpool поставил бы
// свой дефолт (max(4, NumCPU)), и измерялась бы очередь пула, а не строка
// тенанта.
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

// runRetention гоняет jobs через workers горутин и возвращает время до последнего
// завершённого продления. Первая ошибка не прерывает замер — иначе одна отказавшая
// операция обнулила бы цифру; она возвращается и проверяется вызывающим.
func runRetention(t *testing.T, workers int, jobs []retentionJob,
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

	const bucket = "j-thr"
	mustBucket(t, m, bucket)
	if _, err := m.Pool.Exec(ctx,
		`UPDATE buckets SET versioning = 'enabled', object_lock_enabled = true
		  WHERE name = $1`, bucket); err != nil {
		t.Fatalf("включить lock: %v", err)
	}
	var bucketID int64
	if err := m.Pool.QueryRow(ctx, `SELECT id FROM buckets WHERE name = $1`, bucket).
		Scan(&bucketID); err != nil {
		t.Fatalf("id бакета: %v", err)
	}

	jobs := make([]retentionJob, throughputObjects)
	for i := range jobs {
		key := fmt.Sprintf("k%04d", i)
		jobs[i] = retentionJob{key: key,
			versionID: insertVersion(t, m, bucket, key, versionOpts{current: true})}
	}

	pool := throughputPool(t, throughputWorkers)
	until := time.Now().Add(24 * time.Hour)

	// События обязаны быть на месте: без них «замер» измерял бы пустой путь.
	// Отсчёт от того, что оставил mustBucket (создание бакета — тоже событие).
	seqBefore, err := pool.JournalSeq(ctx, thrTenant)
	if err != nil {
		t.Fatalf("journal_seq: %v", err)
	}

	// a) штатный путь: событие журнала в той же транзакции.
	withJournal, err := runRetention(t, throughputWorkers, jobs, func(j retentionJob) error {
		return pool.PutObjectRetention(ctx, testTenant, bucket, j.key,
			j.versionID, "GOVERNANCE", until)
	})
	if err != nil {
		t.Fatalf("продление с журналом: %v", err)
	}
	seqAfter, err := pool.JournalSeq(ctx, thrTenant)
	if err != nil {
		t.Fatalf("journal_seq: %v", err)
	}
	if seqAfter-seqBefore != throughputObjects {
		t.Fatalf("событий %d, хотели %d — измеряли не тот путь",
			seqAfter-seqBefore, throughputObjects)
	}

	// b) тот же переход метаданных без события: заблокировать версию, обновить
	// срок, коммит.
	withoutJournal, err := runRetention(t, throughputWorkers, jobs, func(j retentionJob) error {
		tx, err := pool.Pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		var id uuid.UUID
		if err := tx.QueryRow(ctx,
			`SELECT id FROM objects WHERE bucket_id = $1 AND key = $2 FOR UPDATE`,
			bucketID, j.key).Scan(&id); err != nil {
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

	// c) событие без строки тенанта — вариант, который обсуждается для Ж2:
	// служебный id из обычной последовательности (пропуски допустимы), плотный
	// journal_seq выдаёт воркер сегментов при запечатывании. Замер нужен, чтобы
	// понять, что именно дорожает: лишняя вставка или блокировка строки тенанта.
	// Последовательность заведомо выше уже выданных номеров — события (c) не
	// должны сталкиваться с событиями (a) на первичном ключе.
	if _, err := m.Pool.Exec(ctx, `CREATE SEQUENCE journal_thr_seq START 100000`); err != nil {
		t.Fatalf("последовательность для замера: %v", err)
	}
	t.Cleanup(func() {
		m.Pool.Exec(context.Background(), `DROP SEQUENCE IF EXISTS journal_thr_seq`)
	})
	withSeq, err := runRetention(t, throughputWorkers, jobs, func(j retentionJob) error {
		tx, err := pool.Pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		var id uuid.UUID
		if err := tx.QueryRow(ctx,
			`SELECT id FROM objects WHERE bucket_id = $1 AND key = $2 FOR UPDATE`,
			bucketID, j.key).Scan(&id); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE objects SET retention_mode = $2::retention_mode, retain_until = $3
			  WHERE id = $1`, id, "GOVERNANCE", until); err != nil {
			return err
		}
		var seq int64
		if err := tx.QueryRow(ctx, `SELECT nextval('journal_thr_seq')`).Scan(&seq); err != nil {
			return err
		}
		payload, err := json.Marshal(meta.JournalRetention{
			Bucket: bucket, Key: j.key, Mode: "GOVERNANCE", Until: until})
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO journal_events (tenant_id, journal_seq, kind, object_id, bucket_id, payload)
			  VALUES ($1, $2, 'retention', $3, $4, $5::jsonb)`,
			thrTenant, seq, id, bucketID, string(payload)); err != nil {
			return err
		}
		return tx.Commit(ctx)
	})
	if err != nil {
		t.Fatalf("продление с id из последовательности: %v", err)
	}
	if n := countRow(t, ctx, pool, `SELECT count(*) FROM journal_events`); n != 2*throughputObjects+1 {
		t.Fatalf("событий %d, хотели %d — вариант (c) не измерялся",
			n, 2*throughputObjects+1)
	}

	rate := func(d time.Duration) float64 { return float64(throughputObjects) / d.Seconds() }
	withRate, withoutRate, seqRate := rate(withJournal), rate(withoutJournal), rate(withSeq)
	t.Logf("продления retention: один тенант, %d объектов, %d горутин, пул %d",
		throughputObjects, throughputWorkers, throughputWorkers)
	t.Logf("  a) с журналом (счётчик на тенанте): %7.0f/сек  (%v)",
		withRate, withJournal.Round(time.Millisecond))
	t.Logf("  b) без журнала (как было до него):  %7.0f/сек  (%v)",
		withoutRate, withoutJournal.Round(time.Millisecond))
	t.Logf("  c) событие, но id из последовательности: %7.0f/сек  (%v)",
		seqRate, withSeq.Round(time.Millisecond))
	t.Logf("  цена журнала целиком:  x%.2f", withoutRate/withRate)
	t.Logf("  в том числе вставки:   x%.2f", withoutRate/seqRate)
	t.Logf("  в том числе строки тенанта: x%.2f", seqRate/withRate)

	if withRate*throughputMaxSlower < withoutRate {
		t.Errorf("журнал замедлил продление в %.1f раза (с %.0f/сек до %.0f/сек) — "+
			"это больше допустимого x%.0f: плотная нумерация стоит дороже, чем принято "+
			"решение, нужен перенос выдачи journal_seq на воркер сегментов (ADR-0023)",
			withoutRate/withRate, withoutRate, withRate, throughputMaxSlower)
	}
}
