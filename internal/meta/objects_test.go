package meta_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/testdb"
)

func mustBucket(t *testing.T, m *meta.Meta, name string) {
	t.Helper()
	if _, err := m.CreateBucket(context.Background(), name, "tester", []byte(`{}`), nil); err != nil {
		t.Fatalf("CreateBucket(%s): %v", name, err)
	}
}

func putInput(bucket, key string) meta.PutObjectInput {
	return meta.PutObjectInput{
		ID:          uuid.New(),
		Bucket:      bucket,
		Key:         key,
		Size:        11,
		ETag:        "5d41402abc4b2a76b9719d911017c592",
		UserMeta:    map[string]string{"author": "tester"},
		OnlineUntil: time.Now().Add(24 * time.Hour),
	}
}

func TestPutGetObject(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()
	mustBucket(t, m, "b1")

	in := putInput("b1", "dir/obj.txt")
	in.ContentType = ptr("text/plain")
	in.Tags = map[string]string{"team": "infra"}
	in.Checksums = map[string]string{"crc32": "AAAA"}

	got, err := m.PutObject(ctx, in)
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if got.ID != in.ID {
		t.Errorf("ID = %v, хотели %v", got.ID, in.ID)
	}
	if got.State != "pending" || !got.Landing {
		t.Errorf("свежий объект обязан быть pending и landing: %+v", got)
	}
	if got.OnlineUntil.IsZero() {
		t.Errorf("online_until не проставлен")
	}

	back, err := m.GetObject(ctx, "b1", "dir/obj.txt")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	if back.Size != in.Size || back.ETag != in.ETag {
		t.Errorf("размер/etag разошлись: %+v", back)
	}
	if back.UserMeta["author"] != "tester" || back.Tags["team"] != "infra" {
		t.Errorf("метаданные потерялись: %+v", back)
	}
	if back.Checksums["crc32"] != "AAAA" {
		t.Errorf("checksums потерялись: %v", back.Checksums)
	}
	if back.StorageClass != "GLACIER" {
		t.Errorf("storage class = %q, хотели GLACIER", back.StorageClass)
	}
}

func TestPutToMissingBucket(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)

	_, err := m.PutObject(context.Background(), putInput("nope", "k"))
	if !meta.IsNotFound(err) {
		t.Errorf("ожидали ErrNotFound, получили %v", err)
	}
}

// Перезапись: ровно одна текущая строка, и GetObject отдаёт новую версию.
// Прежняя остаётся в таблице (is_current=false) — она нужна для мягкого
// удаления и разбора сбоев.
func TestPutOverwriteKeepsOneCurrent(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()
	mustBucket(t, m, "b1")

	first := putInput("b1", "k")
	first.ETag = "first"
	if _, err := m.PutObject(ctx, first); err != nil {
		t.Fatalf("первый PUT: %v", err)
	}

	second := putInput("b1", "k")
	second.ETag = "second"
	if _, err := m.PutObject(ctx, second); err != nil {
		t.Fatalf("второй PUT: %v", err)
	}

	back, err := m.GetObject(ctx, "b1", "k")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	if back.ID != second.ID {
		t.Errorf("GetObject вернул прежнюю версию: %v", back.ID)
	}

	var n int
	if err := m.Pool.QueryRow(ctx,
		`SELECT count(*) FROM objects WHERE key='k' AND is_current`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Errorf("текущих строк %d, должна быть одна", n)
	}

	var total int
	if err := m.Pool.QueryRow(ctx, `SELECT count(*) FROM objects`).Scan(&total); err != nil {
		t.Fatalf("count: %v", err)
	}
	if total != 2 {
		t.Errorf("всего строк %d, ожидали 2: прежняя версия обязана остаться", total)
	}
}

// Две параллельные перезаписи одного ключа. Advisory-лок (ADR-0005) обязан
// сериализовать их так, чтобы в итоге осталась ровно одна текущая строка и
// ни одна из сторон не получила ошибку.
func TestConcurrentOverwriteOneCurrent(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()
	mustBucket(t, m, "b1")

	const n = 16
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := m.PutObject(ctx, putInput("b1", "hot")); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("параллельный PUT дал ошибку: %v", err)
	}

	var cur int
	if err := m.Pool.QueryRow(ctx,
		`SELECT count(*) FROM objects WHERE key='hot' AND is_current`).Scan(&cur); err != nil {
		t.Fatalf("count: %v", err)
	}
	if cur != 1 {
		t.Errorf("текущих строк %d, должна быть ровно одна", cur)
	}
}

// Разные ключи не должны блокировать друг друга дольше, чем требует дело.
// Проверяем не «вообще нет локов» (это не измерить в юните), а что параллельные
// PUT разных ключей завершаются без ошибок и дают по строке на ключ.
func TestConcurrentDistinctKeys(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()
	mustBucket(t, m, "b1")

	const n = 16
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			in := putInput("b1", uuid.NewString())
			if _, err := m.PutObject(ctx, in); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("параллельный PUT: %v", err)
	}

	var cur int
	if err := m.Pool.QueryRow(ctx,
		`SELECT count(*) FROM objects WHERE is_current`).Scan(&cur); err != nil {
		t.Fatalf("count: %v", err)
	}
	if cur != n {
		t.Errorf("текущих строк %d, хотели %d", cur, n)
	}
}

func TestGetObjectMissing(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	mustBucket(t, m, "b1")

	if _, err := m.GetObject(context.Background(), "b1", "нет"); !meta.IsNotFound(err) {
		t.Errorf("ожидали ErrNotFound, получили %v", err)
	}
}

func TestGetObjectByID(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()
	mustBucket(t, m, "b1")

	in := putInput("b1", "k")
	if _, err := m.PutObject(ctx, in); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	back, err := m.GetObjectByID(ctx, in.ID)
	if err != nil {
		t.Fatalf("GetObjectByID: %v", err)
	}
	if back.Key != "k" {
		t.Errorf("ключ разошёлся: %q", back.Key)
	}
	if _, err := m.GetObjectByID(ctx, uuid.New()); !meta.IsNotFound(err) {
		t.Errorf("для неизвестного UUID ожидали ErrNotFound, получили %v", err)
	}
}

// online_until NOT NULL — проверка на уровне БД. Если кто-то вставит строку
// в обход PutObject, база обязана отказать: NULL молча выключил бы вытеснение.
func TestOnlineUntilNotNull(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()
	mustBucket(t, m, "b1")

	var id int64
	if err := m.Pool.QueryRow(ctx, `SELECT id FROM buckets WHERE name='b1'`).Scan(&id); err != nil {
		t.Fatalf("bucket id: %v", err)
	}
	_, err := m.Pool.Exec(ctx,
		`INSERT INTO objects (id, bucket_id, key, size, etag, storage_class,
		                      state, landing, online_until)
		 VALUES ($1, $2, 'k', 1, 'e', 'GLACIER', 'pending', true, NULL)`,
		uuid.New(), id)
	if err == nil {
		t.Errorf("вставка с online_until IS NULL прошла — колонка не защищена")
	}
}

func ptr[T any](v T) *T { return &v }
