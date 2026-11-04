package meta_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// testTenant — единственный тенант тестовой базы (`default` из миграции 0005).
// Тенант — часть контракта каждой функции meta, работающей с бакетом (ADR-0017),
// поэтому его нельзя «не указать».
var testTenant = meta.NewTenant(1)

func mustBucket(t *testing.T, m *meta.Meta, name string) {
	t.Helper()
	if _, err := m.CreateBucket(context.Background(), testTenant, name, "tester", []byte(`{}`), nil); err != nil {
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
		// Тело в этих тестах не пишется — только строка. Обёртка DEK при этом
		// обязана быть: CHECK objects_dek_present из миграции 0013 не различает
		// «тест без тела» и «боевой путь, который про ключ забыл».
		WrappedDEK: []byte("wrapped-dek-для-теста"),
		// Соль обязательна там же, где обёртка DEK: с миграции 0014 у версии с
		// данными есть часть, а у части — соль.
		Salt: testSalt(),
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

	got, err := m.PutObject(ctx, testTenant, in)
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

	back, err := m.GetObject(ctx, testTenant, "b1", "dir/obj.txt")
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

	_, err := m.PutObject(context.Background(), testTenant, putInput("nope", "k"))
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
	if _, err := m.PutObject(ctx, testTenant, first); err != nil {
		t.Fatalf("первый PUT: %v", err)
	}

	second := putInput("b1", "k")
	second.ETag = "second"
	if _, err := m.PutObject(ctx, testTenant, second); err != nil {
		t.Fatalf("второй PUT: %v", err)
	}

	back, err := m.GetObject(ctx, testTenant, "b1", "k")
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

// Major ревью PR #6: прежняя версия обязана уйти в deleting в той же
// транзакции. Иначе migrator заархивирует уже заменённый объект (лишняя
// запись на ленту), а deleter его никогда не увидит — утечка места.
func TestPutOverwriteRetiresOldVersion(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()
	mustBucket(t, m, "b1")

	first := putInput("b1", "k")
	if _, err := m.PutObject(ctx, testTenant, first); err != nil {
		t.Fatalf("первый PUT: %v", err)
	}
	second := putInput("b1", "k")
	if _, err := m.PutObject(ctx, testTenant, second); err != nil {
		t.Fatalf("второй PUT: %v", err)
	}

	var state string
	var current bool
	err := m.Pool.QueryRow(ctx,
		`SELECT state, is_current FROM objects WHERE id = $1`, first.ID).Scan(&state, &current)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if current {
		t.Errorf("прежняя версия осталась текущей")
	}
	if state != "deleting" {
		t.Errorf("прежняя версия в состоянии %q, хотели deleting — иначе её "+
			"заархивирует migrator, а deleter её не увидит", state)
	}

	// В deleting строку должен подхватывать индекс objects_queue — иначе
	// deleter до неё никогда не доберётся.
	var inQueue int
	if err := m.Pool.QueryRow(ctx,
		`SELECT count(*) FROM objects
		  WHERE id = $1 AND state IN ('pending','deleting')`, first.ID).Scan(&inQueue); err != nil {
		t.Fatalf("count: %v", err)
	}
	if inQueue != 1 {
		t.Errorf("прежняя версия не попадает в очередь deleter'а")
	}

	// Новая версия, наоборот, обязана быть pending и текущей.
	var newState string
	if err := m.Pool.QueryRow(ctx,
		`SELECT state FROM objects WHERE id = $1`, second.ID).Scan(&newState); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if newState != "pending" {
		t.Errorf("новая версия в состоянии %q, хотели pending", newState)
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
			if _, err := m.PutObject(ctx, testTenant, putInput("b1", "hot")); err != nil {
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
			if _, err := m.PutObject(ctx, testTenant, in); err != nil {
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

	if _, err := m.GetObject(context.Background(), testTenant, "b1", "нет"); !meta.IsNotFound(err) {
		t.Errorf("ожидали ErrNotFound, получили %v", err)
	}
}

func TestGetObjectByID(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()
	mustBucket(t, m, "b1")

	in := putInput("b1", "k")
	if _, err := m.PutObject(ctx, testTenant, in); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	back, err := m.GetObjectByID(ctx, testTenant, in.ID)
	if err != nil {
		t.Fatalf("GetObjectByID: %v", err)
	}
	if back.Key != "k" {
		t.Errorf("ключ разошёлся: %q", back.Key)
	}
	if _, err := m.GetObjectByID(ctx, testTenant, uuid.New()); !meta.IsNotFound(err) {
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

// testSalt — соль для тестов, которые пишут только строку, без тела.
func testSalt() []byte { return make([]byte, 16) }

// --- Версионирование и null-версия (этап V, ADR-0015) ---

// setVersioning ставит состояние версионирования напрямую: PutBucketVersioning
// появляется позже по этапу V, а развилка в PutObject нужна уже сейчас.
func setVersioning(t *testing.T, m *meta.Meta, bucket, state string) {
	t.Helper()
	_, err := m.Pool.Exec(context.Background(),
		`UPDATE buckets SET versioning = $2 WHERE name = $1`, bucket, state)
	if err != nil {
		t.Fatalf("set versioning %s=%s: %v", bucket, state, err)
	}
}

// rowsByKey возвращает по ключу: число текущих, число null-версий и состояния
// всех строк.
func rowsByKey(t *testing.T, m *meta.Meta, bucket, key string) (current, nulls int, states []string) {
	t.Helper()
	ctx := context.Background()
	if err := m.Pool.QueryRow(ctx,
		`SELECT count(*) FILTER (WHERE is_current),
		        count(*) FILTER (WHERE is_null_version)
		   FROM objects WHERE bucket_id = (SELECT id FROM buckets WHERE name=$1) AND key=$2`,
		bucket, key).Scan(&current, &nulls); err != nil {
		t.Fatalf("count %s/%s: %v", bucket, key, err)
	}
	rs, err := m.Pool.Query(ctx,
		`SELECT state::text FROM objects
		  WHERE bucket_id = (SELECT id FROM buckets WHERE name=$1) AND key=$2
		  ORDER BY state`, bucket, key)
	if err != nil {
		t.Fatalf("states %s/%s: %v", bucket, key, err)
	}
	defer rs.Close()
	for rs.Next() {
		var s string
		if err := rs.Scan(&s); err != nil {
			t.Fatalf("scan state: %v", err)
		}
		states = append(states, s)
	}
	return current, nulls, states
}

// Три перезаписи одного ключа в бакете без версий. Это тот случай, из-за которого
// сброс is_null_version у прежней строки обязан быть в той же транзакции:
// частичный уникальный индекс objects_null_version пропускает только одну
// null-версию на ключ, и без сброса вторая и третья перезаписи упали бы.
func TestOverwriteUnversionedReplacesNullVersion(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()
	mustBucket(t, m, "unv")

	for i, etag := range []string{"v1", "v2", "v3"} {
		in := putInput("unv", "k")
		in.ETag = etag
		if _, err := m.PutObject(ctx, testTenant, in); err != nil {
			t.Fatalf("PUT %d (%s): %v", i+1, etag, err)
		}
		current, nulls, states := rowsByKey(t, m, "unv", "k")
		if current != 1 {
			t.Errorf("PUT %d: текущих строк %d, должна быть одна", i+1, current)
		}
		if nulls != 1 {
			t.Errorf("PUT %d: null-версий %d, обязана быть ровно одна (текущая)", i+1, nulls)
		}
		// Все прежние версии уходят в deleting: в бакете без версий они
		// заменяются, а не сохраняются.
		want := make([]string, 0, i+1)
		for j := 0; j < i; j++ {
			want = append(want, "deleting")
		}
		want = append(want, "pending")
		if len(states) != len(want) {
			t.Fatalf("PUT %d: строк %d (%v), ожидали %d", i+1, len(states), states, len(want))
		}
		for j := range want {
			if states[j] != want[j] {
				t.Errorf("PUT %d: состояния %v, ожидали %v", i+1, states, want)
				break
			}
		}
	}

	// null-версией обязана остаться именно текущая строка, а не одна из старых.
	var onCurrent bool
	if err := m.Pool.QueryRow(ctx,
		`SELECT is_null_version FROM objects
		  WHERE bucket_id = (SELECT id FROM buckets WHERE name='unv')
		    AND key='k' AND is_current`).Scan(&onCurrent); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !onCurrent {
		t.Errorf("текущая строка не помечена null-версией")
	}
}

// В бакете с включёнными версиями прежняя версия живёт дальше: is_current=false,
// но состояние не меняется. Перевод в deleting здесь означал бы уничтожение
// истории и обход Object Lock.
func TestOverwriteVersionedKeepsOldVersion(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()
	mustBucket(t, m, "ver")
	setVersioning(t, m, "ver", meta.VersioningEnabled)

	first := putInput("ver", "k")
	first.ETag = "v1"
	if _, err := m.PutObject(ctx, testTenant, first); err != nil {
		t.Fatalf("первый PUT: %v", err)
	}
	second := putInput("ver", "k")
	second.ETag = "v2"
	if _, err := m.PutObject(ctx, testTenant, second); err != nil {
		t.Fatalf("второй PUT: %v", err)
	}

	current, nulls, states := rowsByKey(t, m, "ver", "k")
	if current != 1 {
		t.Errorf("текущих строк %d, должна быть одна", current)
	}
	if nulls != 0 {
		t.Errorf("null-версий %d, в бакете с версиями их быть не должно", nulls)
	}
	// Прежняя версия обязана остаться pending: не deleting.
	if len(states) != 2 || states[0] != "pending" || states[1] != "pending" {
		t.Errorf("состояния %v, ожидали две живые pending-версии", states)
	}

	// И GetObject обязан отдавать новую.
	back, err := m.GetObject(ctx, testTenant, "ver", "k")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	if back.ID != second.ID {
		t.Errorf("GetObject вернул не текущую версию: %v", back.ID)
	}
}

// Приостановленные версии ведут себя как «версий нет»: новая запись заменяет
// предыдущую null-версию, а не добавляется к ней.
func TestOverwriteSuspendedReplacesNullVersion(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()
	mustBucket(t, m, "susp")
	setVersioning(t, m, "susp", meta.VersioningSuspended)

	for _, etag := range []string{"v1", "v2"} {
		in := putInput("susp", "k")
		in.ETag = etag
		if _, err := m.PutObject(ctx, testTenant, in); err != nil {
			t.Fatalf("PUT %s: %v", etag, err)
		}
	}

	current, nulls, states := rowsByKey(t, m, "susp", "k")
	if current != 1 || nulls != 1 {
		t.Errorf("текущих %d (хотели 1), null-версий %d (хотели 1)", current, nulls)
	}
	if len(states) != 2 || states[0] != "deleting" || states[1] != "pending" {
		t.Errorf("состояния %v, ожидали deleting + pending", states)
	}
}

// Параллельные перезаписи в бакете с версиями: код другой ветки, и она обязана
// держать то же правило «одна текущая строка».
func TestConcurrentOverwriteVersionedOneCurrent(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()
	mustBucket(t, m, "race")
	setVersioning(t, m, "race", meta.VersioningEnabled)

	const n = 12
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			in := putInput("race", "k")
			in.ETag = fmt.Sprintf("etag-%d", i)
			_, errs[i] = m.PutObject(ctx, testTenant, in)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("PUT %d: %v", i, err)
		}
	}
	current, nulls, _ := rowsByKey(t, m, "race", "k")
	if current != 1 {
		t.Errorf("текущих строк %d, должна быть одна", current)
	}
	if nulls != 0 {
		t.Errorf("null-версий %d в бакете с версиями", nulls)
	}
	var total int
	if err := m.Pool.QueryRow(ctx, `SELECT count(*) FROM objects`).Scan(&total); err != nil {
		t.Fatalf("count: %v", err)
	}
	if total != n {
		t.Errorf("всего строк %d, ожидали %d: ни одна версия не должна теряться", total, n)
	}
}

// --- suspended не имеет права трогать обычную версию (блокер ревью V PR#1) ---

// versionState возвращает (is_current, state) версии по её etag.
func versionState(t *testing.T, m *meta.Meta, bucket, etag string) (bool, string) {
	t.Helper()
	var cur bool
	var state string
	err := m.Pool.QueryRow(context.Background(),
		`SELECT is_current, state::text FROM objects
		  WHERE bucket_id = (SELECT id FROM buckets WHERE name=$1) AND etag=$2`,
		bucket, etag).Scan(&cur, &state)
	if err != nil {
		t.Fatalf("версия %s/%s не найдена: %v", bucket, etag, err)
	}
	return cur, state
}

// Включение версий, запись обычной версии, приостановка и ещё одна запись. По AWS
// приостановленный бакет заменяет null-версию, а текущую обычную версию трогать
// нельзя — клиент сохранил её именно ради истории.
//
// Прежний код (один запрос WHERE is_current) отправлял v1 в deleting: тихая
// потеря данных без единой ошибки.
func TestSuspendedDoesNotDeleteNormalVersion(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()
	mustBucket(t, m, "susp2")
	setVersioning(t, m, "susp2", meta.VersioningEnabled)

	v1 := putInput("susp2", "k")
	v1.ETag = "v1-normal"
	if _, err := m.PutObject(ctx, testTenant, v1); err != nil {
		t.Fatalf("PUT v1 при enabled: %v", err)
	}

	setVersioning(t, m, "susp2", meta.VersioningSuspended)
	v2 := putInput("susp2", "k")
	v2.ETag = "v2-null"
	if _, err := m.PutObject(ctx, testTenant, v2); err != nil {
		t.Fatalf("PUT v2 при suspended: %v", err)
	}

	cur, state := versionState(t, m, "susp2", "v1-normal")
	if state == "deleting" || state == "deleted" {
		t.Fatalf("обычная версия v1 удалена (state=%s) — потеря истории", state)
	}
	if state != "pending" {
		t.Errorf("v1 в состоянии %s, ожидали pending", state)
	}
	if cur {
		t.Errorf("v1 осталась текущей после записи null-версии")
	}

	cur2, state2 := versionState(t, m, "susp2", "v2-null")
	if !cur2 || state2 != "pending" {
		t.Errorf("v2: current=%v state=%s, ожидали current и pending", cur2, state2)
	}

	current, nulls, _ := rowsByKey(t, m, "susp2", "k")
	if current != 1 {
		t.Errorf("текущих версий %d, должна быть одна", current)
	}
	if nulls != 1 {
		t.Errorf("null-версий %d, обязана быть ровно одна (v2)", nulls)
	}
}

// Тот же ход состояний, но начинаем с null-версии: unversioned → enabled →
// suspended. Третий PUT обязан пройти.
//
// Прежний код на шаге suspended переводил в deleting текущую обычную версию, а
// INSERT с is_null_version = true упирался в прежнюю null-версию, которая
// осталась нетекущей с флагом. Транзакция откатывалась, и клиент получал ошибку
// вместо записи.
func TestSuspendedAfterEnabledAfterUnversioned(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()
	mustBucket(t, m, "cycle")

	steps := []struct {
		state string
		etag  string
	}{
		{meta.VersioningUnversioned, "n1-null"},
		{meta.VersioningEnabled, "n2-normal"},
		{meta.VersioningSuspended, "n3-null"},
	}
	for _, st := range steps {
		setVersioning(t, m, "cycle", st.state)
		in := putInput("cycle", "k")
		in.ETag = st.etag
		if _, err := m.PutObject(ctx, testTenant, in); err != nil {
			t.Fatalf("PUT %s при %s: %v", st.etag, st.state, err)
		}
	}

	// Обычная версия n2 обязана пережить переход в suspended.
	cur, state := versionState(t, m, "cycle", "n2-normal")
	if state == "deleting" || state == "deleted" {
		t.Fatalf("обычная версия n2 удалена (state=%s)", state)
	}
	if cur {
		t.Errorf("n2 осталась текущей после записи null-версии")
	}

	// Null-версия n1 заменена: живой null-версией обязана быть n3.
	cur3, state3 := versionState(t, m, "cycle", "n3-null")
	if !cur3 || state3 != "pending" {
		t.Errorf("n3: current=%v state=%s, ожидали current и pending", cur3, state3)
	}
	var nullOwner string
	if err := m.Pool.QueryRow(ctx,
		`SELECT etag FROM objects
		  WHERE bucket_id = (SELECT id FROM buckets WHERE name='cycle')
		    AND key='k' AND is_null_version`).Scan(&nullOwner); err != nil {
		t.Fatalf("null-версия не найдена: %v", err)
	}
	if nullOwner != "n3-null" {
		t.Errorf("null-версией остался %q, ожидали n3-null", nullOwner)
	}

	current, nulls, _ := rowsByKey(t, m, "cycle", "k")
	if current != 1 || nulls != 1 {
		t.Errorf("текущих %d (хотели 1), null-версий %d (хотели 1)", current, nulls)
	}
}
