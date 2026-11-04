package meta_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// versionOpts описывает вставляемую версию объекта. version_seq назначается
// последовательностью по порядку вставок, поэтому «новая» — та, что вставлена
// позже.
type versionOpts struct {
	current      bool
	deleteMarker bool
	nullVersion  bool
	size         int64
}

// insertVersion вставляет версию напрямую: PutObject умеет создавать версии
// только по состоянию версионирования бакета, а тесту нужна произвольная
// комбинация (маркер удаления, null-версия, не-текущая версия).
func insertVersion(t *testing.T, m *meta.Meta, bucket, key string, o versionOpts) string {
	t.Helper()
	if o.size == 0 && !o.deleteMarker {
		o.size = 1
	}
	var bucketID int64
	if err := m.Pool.QueryRow(context.Background(),
		`SELECT id FROM buckets WHERE name = $1`, bucket).Scan(&bucketID); err != nil {
		t.Fatalf("id бакета %s: %v", bucket, err)
	}
	id := uuid.New()
	// Маркер — не 'deleted': иначе он не пережил бы чистку терминальных строк,
	// и CHECK на схеме не позволил бы такой дрейф.
	state := "pending"
	if o.deleteMarker {
		state = "marker"
	}
	if _, err := m.Pool.Exec(context.Background(),
		`INSERT INTO objects (id, bucket_id, key, size, etag, storage_class,
		                      state, landing, online_until, tenant_id, pool_id,
		                      is_current, is_delete_marker, is_null_version,
		                      wrapped_dek)
		  VALUES ($1,$2,$3,$4,'etag','GLACIER',$8,true,
		          now() + interval '1 day', 1, 1, $5, $6, $7,
		          CASE WHEN $6 THEN NULL ELSE 'x'::bytea END)`,
		id, bucketID, key, o.size, o.current, o.deleteMarker, o.nullVersion,
		state); err != nil {
		t.Fatalf("вставка версии %s/%s: %v", bucket, key, err)
	}
	return id.String()
}

func keysOf(res meta.ListResult) []string {
	out := make([]string, 0, len(res.Entries))
	for _, e := range res.Entries {
		out = append(out, e.Key)
	}
	return out
}

func eq[T comparable](a, b []T) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Ключ, у которого текущая версия — маркер удаления, в ListObjects не виден,
// но виден в ListObjectVersions (проверено ниже).
func TestListObjectsCurrentHidesDeleteMarker(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()
	mustBucket(t, m, "lb")

	insertVersion(t, m, "lb", "a", versionOpts{current: true, size: 7})
	// Под маркером живая версия: она не удаляется, а «прячется».
	insertVersion(t, m, "lb", "b", versionOpts{size: 3})
	insertVersion(t, m, "lb", "b", versionOpts{current: true, deleteMarker: true, size: 0})
	insertVersion(t, m, "lb", "c", versionOpts{current: true, size: 5})

	res, err := m.ListObjectsCurrent(ctx, testTenant, "lb", "", "", "", 100)
	if err != nil {
		t.Fatalf("ListObjectsCurrent: %v", err)
	}
	if got := keysOf(res); !eq(got, []string{"a", "c"}) {
		t.Errorf("ключи = %v, хотели [a c] — ключ с delete marker как текущей "+
			"версией не должен попадать в выдачу", got)
	}
	if res.Truncated {
		t.Errorf("Truncated = true при полной выдаче")
	}
}

// Common prefix считается в max-keys как одна запись, и строится из части
// ключа ПОСЛЕ prefix.
func TestListObjectsDelimiter(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()
	mustBucket(t, m, "dl")

	for _, k := range []string{"a", "p/1", "p/2", "q/1"} {
		insertVersion(t, m, "dl", k, versionOpts{current: true})
	}

	res, err := m.ListObjectsCurrent(ctx, testTenant, "dl", "", "/", "", 100)
	if err != nil {
		t.Fatalf("листинг: %v", err)
	}
	if got := keysOf(res); !eq(got, []string{"a"}) {
		t.Errorf("ключи = %v, хотели [a]", got)
	}
	if !eq(res.CommonPrefixes, []string{"p/", "q/"}) {
		t.Errorf("common prefixes = %v, хотели [p/ q/]", res.CommonPrefixes)
	}

	// max-keys = 2: 'a' и 'p/' — страница заполнена, 'q/' не влезла.
	res, err = m.ListObjectsCurrent(ctx, testTenant, "dl", "", "/", "", 2)
	if err != nil {
		t.Fatalf("листинг с max-keys: %v", err)
	}
	if !res.Truncated {
		t.Errorf("Truncated = false, хотя 'q/' не выдана")
	}
	if res.NextKey != "p/" {
		t.Errorf("NextKey = %q, хотели \"p/\"", res.NextKey)
	}

	// Префикс 'p/': delimiter ищется в части ключа после prefix, поэтому
	// 'p/1' и 'p/2' дают 'p/1' и 'p/2' как есть, а не 'p/'.
	res, err = m.ListObjectsCurrent(ctx, testTenant, "dl", "p/", "/", "", 100)
	if err != nil {
		t.Fatalf("листинг с prefix: %v", err)
	}
	if got := keysOf(res); !eq(got, []string{"p/1", "p/2"}) {
		t.Errorf("под префиксом p/ получили %v, хотели [p/1 p/2]", got)
	}
}

// Проход пагинатором до конца при любом размере страницы: без потерь и дублей.
func TestListObjectsPaginationSweep(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()
	mustBucket(t, m, "pg")

	const total = 12
	want := make([]string, 0, total)
	for i := 0; i < total; i++ {
		k := fmt.Sprintf("k%02d", i)
		want = append(want, k)
		insertVersion(t, m, "pg", k, versionOpts{current: true})
	}

	for _, page := range []int{1, 2, 3, 5, total, total + 3} {
		var got []string
		marker := ""
		for i := 0; ; i++ {
			if i > total+5 {
				t.Fatalf("страница %d: листинг не заканчивается", page)
			}
			res, err := m.ListObjectsCurrent(ctx, testTenant, "pg", "", "", marker, page)
			if err != nil {
				t.Fatalf("страница %d: %v", page, err)
			}
			got = append(got, keysOf(res)...)
			if !res.Truncated {
				break
			}
			if res.NextKey == "" {
				t.Fatalf("страница %d: усечено без маркера", page)
			}
			marker = res.NextKey
		}
		if !eq(got, want) {
			t.Errorf("страница %d: получено %d ключей %v, хотели %v", page, len(got), got, want)
		}
	}
}

// Порядок версий — от новой к старой, и null-версия отдаётся как "null".
func TestListObjectVersionsOrder(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()
	mustBucket(t, m, "vv")

	// Три версии одного ключа: вставлены v1, v2, v3 — значит v3 новая.
	var ids []string
	for i := 1; i <= 3; i++ {
		ids = append(ids, insertVersion(t, m, "vv", "k", versionOpts{size: int64(i)}))
	}
	// Отдельный ключ с null-версией (бакет без версий).
	insertVersion(t, m, "vv", "n", versionOpts{current: true, nullVersion: true})

	res, err := m.ListObjectVersions(ctx, testTenant, "vv", "", "", "", "", 100)
	if err != nil {
		t.Fatalf("ListObjectVersions: %v", err)
	}

	var got []string
	for _, e := range res.Entries {
		got = append(got, e.Key+":"+e.VersionID)
	}
	want := []string{
		"k:" + ids[2], "k:" + ids[1], "k:" + ids[0], // новые сверху
		"n:null",
	}
	if !eq(got, want) {
		t.Errorf("порядок версий = %v, хотели %v", got, want)
	}
}

// Маркер в середине версий одного ключа: продолжение обязано идти с этой
// середины, иначе версии теряются или дублируются.
func TestListObjectVersionsPaginationSweep(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()
	mustBucket(t, m, "vp")

	// Два ключа по три версии + один маркер удаления как отдельная версия.
	var want []string
	for _, key := range []string{"a", "b"} {
		var ids []string
		for i := 1; i <= 3; i++ {
			ids = append(ids, insertVersion(t, m, "vp", key, versionOpts{size: int64(i)}))
		}
		// Маркер удаления — самая новая версия ключа b.
		if key == "b" {
			ids = append(ids, insertVersion(t, m, "vp", key,
				versionOpts{deleteMarker: true, size: 0}))
		}
		for i := len(ids) - 1; i >= 0; i-- {
			want = append(want, key+":"+ids[i])
		}
	}

	for _, page := range []int{1, 2, 3, 5, 7} {
		var got []string
		keyMarker, verMarker := "", ""
		for i := 0; ; i++ {
			if i > 10 {
				t.Fatalf("страница %d: листинг версий не заканчивается", page)
			}
			res, err := m.ListObjectVersions(ctx, testTenant, "vp", "", "",
				keyMarker, verMarker, page)
			if err != nil {
				t.Fatalf("страница %d: %v", page, err)
			}
			for _, e := range res.Entries {
				got = append(got, e.Key+":"+e.VersionID)
			}
			if !res.Truncated {
				break
			}
			if res.NextKey == "" || res.NextVersionID == "" {
				t.Fatalf("страница %d: усечено без маркера (key=%q ver=%q)",
					page, res.NextKey, res.NextVersionID)
			}
			keyMarker, verMarker = res.NextKey, res.NextVersionID
		}
		if !eq(got, want) {
			t.Errorf("страница %d: %v, хотели %v", page, got, want)
		}
	}
}

// Маркер удаления — отдельный список, а не версия: вызывающий (бэкенд) обязан
// разложить выдачу по Version и DeleteMarker.
func TestListObjectVersionsSeparatesDeleteMarkers(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()
	mustBucket(t, m, "dm")

	insertVersion(t, m, "dm", "k", versionOpts{size: 1})
	markerID := insertVersion(t, m, "dm", "k", versionOpts{deleteMarker: true, size: 0})

	res, err := m.ListObjectVersions(ctx, testTenant, "dm", "", "", "", "", 100)
	if err != nil {
		t.Fatalf("листинг версий: %v", err)
	}
	var versions, markers []string
	for _, e := range res.Entries {
		if e.IsDeleteMarker {
			markers = append(markers, e.Key+":"+e.VersionID)
		} else {
			versions = append(versions, e.Key+":"+e.VersionID)
		}
	}
	if !eq(markers, []string{"k:" + markerID}) {
		t.Errorf("маркеры = %v, хотели [k:%s]", markers, markerID)
	}
	if len(versions) != 1 {
		t.Errorf("версий %d, хотели 1 — версия под маркером жива", len(versions))
	}
}
