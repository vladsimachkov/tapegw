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
// выполняются строго по одному, сколько бы соединений ни было.
//
// Пути, которые сравнивает тест (один металл, один размер пула):
//
//	a) штатный PutObjectRetention — событие журнала в той же транзакции;
//	b) тот же переход метаданных без события — запрос выбран так же, как в
//	   findLockVersion (join с бакетом, фильтр по тенанту и состоянию,
//	   FOR UPDATE OF o), чтобы в цену журнала не попал лишний join;
//	c) то же событие, но номер берётся из обычной последовательности и строка
//	   тенанта не трогается — схема, принятая для Ж2 (ADR-0023, поправка);
//	d) массовое удаление (DeleteObject без versionId → маркер удаления) —
//	   именно этот путь делает потолок заметным в бою, а не продления.
//
// Утверждения. Абсолютная скорость зависит от машины, поэтому она ничего не
// утверждает про себя; утверждаются два отношения:
//
//	б) b к c не больше throughputMaxInsertSlower — цена самой вставки события.
//	   Оба пути не сериализованы, поэтому это отношение почти не зависит от
//	   диска и годится как проверка в CI;
//	а) b к a не больше throughputMaxSlower — детектор патологий (лок через сеть,
//	   лишний обход таблицы на событие). Порог заведомо выше измеренного: путь
//	   (a) сериализован и упирается в латентность fsync, а путь (b) кормится
//	   групповым коммитом, поэтому на медленном диске отношение растёт само, без
//	   всякой регрессии. Красный порог при зелёном b/c — это про диск, а не про
//	   код; смотреть надо на b/c.
//
// Тест переходный. В PR Ж2 (воркер сегментов, миграция 0028) он переписывается:
// путь (c) тогда станет штатным путём (a), колонка `journal_seq` из таблицы
// событий уйдёт, и страж «событий ровно N» надо будет переставить на счётчик
// воркера. После Ж2 отношение a к b обязано упасть до цены самой вставки
// (x1.05–1.06), и порог a к b надо тогда ужесточить, а не оставить x40.
//
// Измерено на dev-машине (Postgres 16 в docker, локальный диск): a ≈950–1000,
// b ≈9500, c ≈9000, d ≈870–930 в секунду; вставка события x1.05–1.06, строка
// тенанта x9…x10, отношение a к b между прогонами x6.5…x10.1. Абсолютные числа —
// порядок величины, поэтому в ADR и в порогах они как ориентиры, а не как
// гарантия.

const (
	throughputObjects = 1000
	throughputWorkers = 16

	// throughputMaxInsertSlower — потолок отношения «без журнала / событие с
	// id из последовательности». Это цена вставки события, и она от диска не
	// зависит: ни тот, ни другой путь не сериализованы.
	throughputMaxInsertSlower = 2.0

	// throughputMaxSlower — потолок отношения «без журнала / штатный путь».
	// Измерено x6.5…x10.1; порог вчетверо выше верхней границы и ловит только
	// патологические случаи. См. комментарий выше про то, что краснеть он может
	// и без регрессии.
	throughputMaxSlower = 40.0

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

// throughputBucket — бакет с включённым Object Lock и n версиями, и что у них
// id. Версии без retention: путь удаления должен быть чистым.
func throughputBucket(t *testing.T, m *meta.Meta, bucket string, n int) (int64, []retentionJob) {
	t.Helper()
	ctx := context.Background()
	mustBucket(t, m, bucket)
	if _, err := m.Pool.Exec(ctx,
		`UPDATE buckets SET versioning = 'enabled', object_lock_enabled = true
		  WHERE name = $1`, bucket); err != nil {
		t.Fatalf("включить lock в %s: %v", bucket, err)
	}
	var bucketID int64
	if err := m.Pool.QueryRow(ctx, `SELECT id FROM buckets WHERE name = $1`, bucket).
		Scan(&bucketID); err != nil {
		t.Fatalf("id бакета %s: %v", bucket, err)
	}
	jobs := make([]retentionJob, n)
	for i := range jobs {
		key := fmt.Sprintf("k%04d", i)
		jobs[i] = retentionJob{key: key,
			versionID: insertVersion(t, m, bucket, key, versionOpts{current: true})}
	}
	return bucketID, jobs
}

// lockVersionStmt — та же форма, что у findLockVersion: join с бакетом, фильтр
// по тенанту и состоянию, блокировка строки объекта. Пути (b) и (c) используют
// её, чтобы отличие от (a) сводилось к событию и строке тенанта.
const lockVersionStmt = `SELECT o.id FROM objects o
		   JOIN buckets b ON b.id = o.bucket_id
		  WHERE b.name = $1 AND b.deleted_at IS NULL AND o.key = $2
		    AND o.id = $3::uuid AND o.state NOT IN ('deleting','deleted')
		    AND b.tenant_id = $4
		      FOR UPDATE OF o`

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

	bucketID, jobs := throughputBucket(t, m, "j-thr", throughputObjects)
	pool := throughputPool(t, throughputWorkers)
	until := time.Now().Add(24 * time.Hour)

	// События обязаны быть на месте: без них «замер» измерял бы пустой путь.
	// Отсчёт от того, что оставило создание бакета (оно тоже событие).
	seqBefore, err := pool.JournalSeq(ctx, thrTenant)
	if err != nil {
		t.Fatalf("journal_seq: %v", err)
	}

	// a) штатный путь: событие журнала в той же транзакции.
	withJournal, err := runRetention(t, throughputWorkers, jobs, func(j retentionJob) error {
		return pool.PutObjectRetention(ctx, testTenant, "j-thr", j.key,
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

	// b) тот же переход метаданных без события.
	withoutJournal, err := runRetention(t, throughputWorkers, jobs, func(j retentionJob) error {
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

	// c) событие без строки тенанта — схема, принятая для Ж2: служебный id из
	// обычной последовательности (пропуски допустимы), плотный journal_seq
	// выдаёт воркер сегментов при запечатывании. Нужен, чтобы разделить цену
	// вставки и цену блокировки строки тенанта.
	// IF NOT EXISTS — на случай убитого прогона: t.Cleanup не отработает, а
	// следующий спотыкался бы о «последовательность уже есть» с ошибкой не про
	// производительность.
	if _, err := m.Pool.Exec(ctx,
		`CREATE SEQUENCE IF NOT EXISTS journal_thr_seq START 100000`); err != nil {
		t.Fatalf("последовательность для замера: %v", err)
	}
	t.Cleanup(func() {
		if _, err := m.Pool.Exec(context.Background(),
			`DROP SEQUENCE IF EXISTS journal_thr_seq`); err != nil {
			t.Errorf("убрать последовательность замера: %v", err)
		}
	})
	withSeq, err := runRetention(t, throughputWorkers, jobs, func(j retentionJob) error {
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
		var seq int64
		if err := tx.QueryRow(ctx, `SELECT nextval('journal_thr_seq')`).Scan(&seq); err != nil {
			return err
		}
		payload, err := json.Marshal(meta.JournalRetention{
			Bucket: "j-thr", Key: j.key, Mode: "GOVERNANCE", Until: until})
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

	// d) массовое удаление: чистка устаревших копий — тот путь, на котором
	// потолок виден лучше всего (миллионы ключей вместо редких продлений).
	// Бакет отдельный и без retention: GOVERNANCE на срок до завтра не пустил бы
	// удаление, и вместо замера мы получили бы отказы.
	_, delJobs := throughputBucket(t, m, "j-thr-del", throughputObjects)
	seqDelBefore, err := pool.JournalSeq(ctx, thrTenant)
	if err != nil {
		t.Fatalf("journal_seq до удалений: %v", err)
	}
	deletes, err := runRetention(t, throughputWorkers, delJobs, func(j retentionJob) error {
		_, err := pool.DeleteObject(ctx, testTenant, meta.DeleteObjectInput{
			Bucket: "j-thr-del", Key: j.key})
		return err
	})
	if err != nil {
		t.Fatalf("массовое удаление: %v", err)
	}
	seqDelAfter, err := pool.JournalSeq(ctx, thrTenant)
	if err != nil {
		t.Fatalf("journal_seq после удалений: %v", err)
	}
	if seqDelAfter-seqDelBefore != throughputObjects {
		t.Fatalf("событий про удаления %d, хотели %d — измеряли не тот путь",
			seqDelAfter-seqDelBefore, throughputObjects)
	}

	rate := func(d time.Duration) float64 { return float64(throughputObjects) / d.Seconds() }
	withRate, withoutRate, seqRate, delRate := rate(withJournal), rate(withoutJournal),
		rate(withSeq), rate(deletes)
	t.Logf("продления retention: один тенант, %d объектов, %d горутин, пул %d",
		throughputObjects, throughputWorkers, throughputWorkers)
	t.Logf("  a) с журналом (счётчик на тенанте): %7.0f/сек  (%v)",
		withRate, withJournal.Round(time.Millisecond))
	t.Logf("  b) без журнала (как было до него):  %7.0f/сек  (%v)",
		withoutRate, withoutJournal.Round(time.Millisecond))
	t.Logf("  c) событие, но id из последовательности: %7.0f/сек  (%v)",
		seqRate, withSeq.Round(time.Millisecond))
	t.Logf("  d) массовое удаление (маркер на ключ): %7.0f/сек  (%v)",
		delRate, deletes.Round(time.Millisecond))
	t.Logf("  цена журнала целиком:      x%.2f", withoutRate/withRate)
	t.Logf("  в том числе вставки:       x%.2f", withoutRate/seqRate)
	t.Logf("  в том числе строки тенанта: x%.2f", seqRate/withRate)
	t.Logf("  10 млн удалений одного тенанта: %v",
		time.Duration(10_000_000/delRate*float64(time.Second)).Round(time.Minute))

	if withoutRate/seqRate > throughputMaxInsertSlower {
		t.Errorf("вставка события замедлила продление в %.1f раза (%.0f/сек до %.0f/сек) — "+
			"это больше x%.1f: дорожает не нумерация, а само событие, и перенос "+
			"номера на воркер сегментов этого не лечит (ADR-0023)",
			withoutRate/seqRate, withoutRate, seqRate, throughputMaxInsertSlower)
	}
	if withRate*throughputMaxSlower < withoutRate {
		t.Errorf("штатный путь медленнее безжурнального в %.1f раза (%.0f/сек до %.0f/сек) — "+
			"это больше порога x%.0f. Если при этом проверка на цену вставки зелёная, "+
			"дело в сериализации строки тенанта или в медленном диске (fsync на каждый "+
			"коммит), а не в регрессии кода",
			withoutRate/withRate, withoutRate, withRate, throughputMaxSlower)
	}
}
