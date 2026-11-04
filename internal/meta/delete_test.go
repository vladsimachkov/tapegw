package meta_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"testing"
	"time"

	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/testdb"
)

func setBucketVersioning(t *testing.T, m *meta.Meta, bucket, state string) {
	t.Helper()
	if _, err := m.Pool.Exec(context.Background(),
		`UPDATE buckets SET versioning = $2 WHERE name = $1`, bucket, state); err != nil {
		t.Fatalf("versioning %s: %v", bucket, err)
	}
}

func currentVersionID(t *testing.T, m *meta.Meta, bucket, key string) string {
	t.Helper()
	var id string
	err := m.Pool.QueryRow(context.Background(),
		`SELECT id::text FROM objects
		  WHERE bucket_id = (SELECT id FROM buckets WHERE name = $1)
		    AND key = $2 AND is_current`, bucket, key).Scan(&id)
	if err != nil {
		return "<нет текущей>"
	}
	return id
}

// Удаление без versionId в бакете с версиями создаёт маркер удаления и НЕ
// трогает данные: прежняя версия перестаёт быть текущей, но остаётся живой.
func TestDeleteObjectCreatesMarker(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()
	mustBucket(t, m, "delmark")
	setBucketVersioning(t, m, "delmark", meta.VersioningEnabled)

	dataID := insertVersion(t, m, "delmark", "k", versionOpts{current: true, size: 42})

	res, err := m.DeleteObject(ctx, testTenant, meta.DeleteObjectInput{
		Bucket: "delmark", Key: "k",
	})
	if err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	if !res.IsDeleteMarker {
		t.Errorf("IsDeleteMarker = false, хотели true")
	}
	if res.VersionID == "" || res.VersionID == dataID {
		t.Errorf("VersionID = %q, хотели id созданного маркера", res.VersionID)
	}

	// Данные не тронуты: версия жива, просто не текущая.
	var state string
	var isCurrent bool
	if err := m.Pool.QueryRow(ctx,
		`SELECT state, is_current FROM objects WHERE id = $1`, dataID).
		Scan(&state, &isCurrent); err != nil {
		t.Fatalf("версия пропала: %v", err)
	}
	if state != "pending" {
		t.Errorf("state данных = %q, хотели pending — удаление без versionId не должно "+
			"трогать данные", state)
	}
	if isCurrent {
		t.Errorf("прежняя версия осталась текущей")
	}

	// Ключ исчез из листинга, но виден в версиях.
	live, err := m.ListObjectsCurrent(ctx, testTenant, "delmark", "", "", "", 10)
	if err != nil {
		t.Fatalf("листинг: %v", err)
	}
	if len(live.Entries) != 0 {
		t.Errorf("ListObjects показал %d после удаления", len(live.Entries))
	}
	vers, err := m.ListObjectVersions(ctx, testTenant, "delmark", "", "", "", "", 10)
	if err != nil {
		t.Fatalf("листинг версий: %v", err)
	}
	if len(vers.Entries) != 2 || len(vers.CommonPrefixes) != 0 {
		t.Errorf("версий %d, хотели 2 (маркер + данные)", len(vers.Entries))
	}
}

// Версия под legal hold или действующим retention не удаляется. GOVERNANCE
// обходится явным запросом, COMPLIANCE и legal hold — никогда.
func TestDeleteVersionUnderLock(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()
	mustBucket(t, m, "locked")
	setBucketVersioning(t, m, "locked", meta.VersioningEnabled)

	future := time.Now().Add(time.Hour)
	past := time.Now().Add(-time.Hour)

	// mode, retain, hold, bypass → ожидаем отказ?
	cases := []struct {
		name   string
		mode   string // "" — без retention
		retain *time.Time
		hold   bool
		bypass bool
		deny   bool
	}{
		{"governance действующий", "GOVERNANCE", &future, false, false, true},
		{"governance + bypass", "GOVERNANCE", &future, false, true, false},
		{"compliance + bypass", "COMPLIANCE", &future, false, true, true},
		{"legal hold + bypass", "", nil, true, true, true},
		{"retention истёк", "GOVERNANCE", &past, false, false, false},
		{"без защиты", "", nil, false, false, false},
	}

	for i, tc := range cases {
		key := "v" + string(rune('a'+i))
		var mode any
		if tc.mode != "" {
			mode = tc.mode
		}
		var retain any
		if tc.retain != nil {
			retain = *tc.retain
		}
		var bucketID int64
		if err := m.Pool.QueryRow(ctx,
			`SELECT id FROM buckets WHERE name = 'locked'`).Scan(&bucketID); err != nil {
			t.Fatalf("id бакета: %v", err)
		}
		var id string
		if err := m.Pool.QueryRow(ctx,
			`INSERT INTO objects (id, bucket_id, key, size, etag, storage_class,
			                      state, landing, online_until, tenant_id, pool_id,
			                      is_current, retention_mode, retain_until, legal_hold)
			  VALUES (gen_random_uuid(), $1, $2, 1, 'etag', 'GLACIER', 'pending', true,
			          now() + interval '1 day', 1, 1, true, $3, $4, $5)
			  RETURNING id::text`,
			bucketID, key, mode, retain, tc.hold).Scan(&id); err != nil {
			t.Fatalf("вставка %s: %v", tc.name, err)
		}

		_, err := m.DeleteObject(ctx, testTenant, meta.DeleteObjectInput{
			Bucket: "locked", Key: key, VersionID: id, BypassGovernance: tc.bypass,
		})
		locked := errors.Is(err, meta.ErrObjectLocked)
		if tc.deny && !locked {
			t.Errorf("%s: удаление прошло (err=%v), хотели ErrObjectLocked", tc.name, err)
		}
		if !tc.deny && err != nil {
			t.Errorf("%s: отказ %v, хотели успех", tc.name, err)
		}
	}
}

// После удаления текущей версии по versionId текущей становится версия с
// максимальным version_seq из оставшихся, а не произвольная.
func TestDeleteCurrentVersionPromotesMaxSeq(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()
	mustBucket(t, m, "promote")
	setBucketVersioning(t, m, "promote", meta.VersioningEnabled)

	var ids []string
	for i := 1; i <= 3; i++ {
		// Текущая — последняя вставленная: так же расставляет флаги PutObject.
		ids = append(ids, insertVersion(t, m, "promote", "k",
			versionOpts{size: int64(i), current: i == 3}))
	}
	// Вставлены по порядку, значит ids[2] — самая новая и текущая.
	if got := currentVersionID(t, m, "promote", "k"); got != ids[2] {
		t.Fatalf("до удаления текущая = %s, хотели %s", got, ids[2])
	}

	if _, err := m.DeleteObject(ctx, testTenant, meta.DeleteObjectInput{
		Bucket: "promote", Key: "k", VersionID: ids[2],
	}); err != nil {
		t.Fatalf("удаление текущей версии: %v", err)
	}

	if got := currentVersionID(t, m, "promote", "k"); got != ids[1] {
		t.Errorf("после удаления текущей текущая = %s, хотели %s (максимальный "+
			"version_seq из оставшихся)", got, ids[1])
	}

	// И листинг обязан показать именно её.
	live, err := m.ListObjectsCurrent(ctx, testTenant, "promote", "", "", "", 10)
	if err != nil {
		t.Fatalf("листинг: %v", err)
	}
	if len(live.Entries) != 1 {
		t.Fatalf("после удаления листинг дал %d записей, хотели 1", len(live.Entries))
	}
}

// Когда версий не осталось, ключа нет: ни текущей, ни в листинге.
func TestDeleteLastVersionRemovesKey(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()
	mustBucket(t, m, "lastone")
	setBucketVersioning(t, m, "lastone", meta.VersioningEnabled)

	id := insertVersion(t, m, "lastone", "k", versionOpts{current: true, size: 1})
	if _, err := m.DeleteObject(ctx, testTenant, meta.DeleteObjectInput{
		Bucket: "lastone", Key: "k", VersionID: id,
	}); err != nil {
		t.Fatalf("удаление единственной версии: %v", err)
	}

	var n int
	if err := m.Pool.QueryRow(ctx,
		`SELECT count(*) FROM objects WHERE bucket_id = (SELECT id FROM buckets WHERE name='lastone')
		  AND key = 'k' AND is_current`).Scan(&n); err != nil {
		t.Fatalf("счётчик: %v", err)
	}
	if n != 0 {
		t.Errorf("текущих версий %d, хотели 0", n)
	}
}

// Воскрешение удалённого объекта. Сценарий целиком:
//
//	PUT k → v1; DELETE k → маркер m2 (ключ «удалён»); PUT k → v3;
//	DELETE k?versionId=v3.
//
// Текущей должен стать маркер m2, и ключ остаётся удалённым. Пока маркер лежал
// в state='deleted', выбор следующей текущей версии (`NOT IN
// ('deleting','deleted')`) его не видел и текущей становилась v1 — объект,
// который клиент удалил, появлялся снова в GET и в листинге.
func TestDeleteCurrentVersionPromotesMarker(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()
	mustBucket(t, m, "res")
	setBucketVersioning(t, m, "res", meta.VersioningEnabled)

	insertVersion(t, m, "res", "k", versionOpts{size: 5, current: true})

	del, err := m.DeleteObject(ctx, testTenant, meta.DeleteObjectInput{Bucket: "res", Key: "k"})
	if err != nil {
		t.Fatalf("удаление без versionId: %v", err)
	}
	markerID := del.VersionID

	// PUT поверх маркера: PutObject сам снимает флаг с предыдущей текущей
	// версии, здесь это одна команда.
	if _, err := m.Pool.Exec(ctx,
		`UPDATE objects SET is_current = false
		  WHERE bucket_id = (SELECT id FROM buckets WHERE name = 'res') AND key = 'k'`); err != nil {
		t.Fatalf("снять флаг текущести: %v", err)
	}
	v3 := insertVersion(t, m, "res", "k", versionOpts{size: 7, current: true})

	if _, err := m.DeleteObject(ctx, testTenant, meta.DeleteObjectInput{
		Bucket: "res", Key: "k", VersionID: v3,
	}); err != nil {
		t.Fatalf("удаление v3: %v", err)
	}

	if got := currentVersionID(t, m, "res", "k"); got != markerID {
		t.Errorf("текущей стал %s, хотели маркер %s: объект воскрес", got, markerID)
	}

	// И листинг не должен показывать ключ: под маркером есть живая v1.
	live, err := m.ListObjectsCurrent(ctx, testTenant, "res", "", "", "", 10)
	if err != nil {
		t.Fatalf("листинг: %v", err)
	}
	if len(live.Entries) != 0 {
		keys := keysOf(live)
		t.Errorf("под маркером листинг дал %v, хотели пусто", keys)
	}
}

// Маркер — живущая версия, а не «мусор на удаление». CHECK запрещает и
// retired_at у маркера, и расхождение флага с состоянием: любой из двух вариантов
// означает, что janitor (M6) однажды удалит маркер и похороненная под ним
// версия воскреснет.
func TestMarkerStateConstraints(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()
	mustBucket(t, m, "chk")
	setBucketVersioning(t, m, "chk", meta.VersioningEnabled)

	var bucketID int64
	if err := m.Pool.QueryRow(ctx, `SELECT id FROM buckets WHERE name = 'chk'`).Scan(&bucketID); err != nil {
		t.Fatalf("id бакета: %v", err)
	}

	now := time.Now()
	cases := []struct {
		name, state string
		marker      bool
		retired     *time.Time // nil — retired_at нет
		wantOK      bool
	}{
		{"маркер в marker без retired_at", "marker", true, nil, true},
		{"маркер с retired_at", "marker", true, &now, false},
		{"маркер в deleted", "deleted", true, nil, false},
		{"обычная версия в marker", "marker", false, nil, false},
		{"обычная версия в pending", "pending", false, nil, true},
	}
	for i, tc := range cases {
		_, err := m.Pool.Exec(ctx,
			`INSERT INTO objects (id, bucket_id, key, size, etag, storage_class,
			                      state, landing, online_until, tenant_id, pool_id,
			                      is_current, is_delete_marker, is_null_version, retired_at)
			  VALUES ($1,$2,$3,0,'','GLACIER',$4,false,now(),1,1,false,$5,false,$6)`,
			uuid.New(), bucketID, fmt.Sprintf("k%d", i), tc.state, tc.marker, tc.retired)
		if tc.wantOK && err != nil {
			t.Errorf("%s: вставка отклонена (%v), а должна пройти", tc.name, err)
		}
		if !tc.wantOK && err == nil {
			t.Errorf("%s: вставка прошла, а схема обязана отклонить", tc.name)
		}
	}
}
