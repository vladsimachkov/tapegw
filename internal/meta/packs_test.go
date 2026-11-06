package meta_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// Учёт живого в пачках (предложение 0011).
//
// Смысл тестов не в том, что счётчик умеет уменьшаться, а в том, что он
// уменьшается ТОЙ ЖЕ транзакцией, которая убирает версию. Отдельный UPDATE
// после перевода в 'deleting' проходил бы все эти проверки на зелёном прогоне
// и расходился бы на падении между запросами — а падение между запросами и есть
// штатный случай для воркера.

// packFixture — пачка из n участников заданного размера. Возвращает id пачки и
// id участников по порядку вставки.
func packFixture(t *testing.T, m *meta.Meta, bucket string, size int64, n int) (uuid.UUID, []uuid.UUID) {
	t.Helper()
	ctx := context.Background()

	var bucketID int64
	if err := m.Pool.QueryRow(ctx, `SELECT id FROM buckets WHERE name = $1`, bucket).Scan(&bucketID); err != nil {
		t.Fatalf("id бакета %s: %v", bucket, err)
	}
	packID := uuid.New()
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO packs (id, pool_id, kind, state, total_bytes, live_bytes, members_total, members_live)
		  VALUES ($1, 1, 'data', 'archived', $2, $2, $3, $3)`,
		packID, size*int64(n), n); err != nil {
		t.Fatalf("вставка пачки: %v", err)
	}

	var ids []uuid.UUID
	for i := 0; i < n; i++ {
		id := uuid.New()
		if _, err := m.Pool.Exec(ctx,
			`INSERT INTO objects (id, bucket_id, key, size, etag, storage_class, state,
			                      landing, online_until, tenant_id, pool_id, is_current,
			                      wrapped_dek, pack_id, pack_offset, pack_length)
			  VALUES ($1,$2,$3,$4,'etag','GLACIER','archived',false,
			          now() + interval '1 day', 1, 1, $5, 'x'::bytea, $6, $7, $8)`,
			id, bucketID, keyN(i), size, i == n-1, packID, int64(i)*size, size); err != nil {
			t.Fatalf("вставка участника %d: %v", i, err)
		}
		ids = append(ids, id)
	}
	return packID, ids
}

func keyN(i int) string {
	return string(rune('a'+i)) + "-key"
}

func statsOf(t *testing.T, m *meta.Meta, packID uuid.UUID) *meta.PackStats {
	t.Helper()
	s, err := m.StatsPack(context.Background(), packID)
	if err != nil {
		t.Fatalf("StatsPack: %v", err)
	}
	return s
}

// Удаление версии по versionId снимает её байты с живого остатка пачки.
func TestPackLiveBytesOnDeleteVersion(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	mustBucket(t, m, "pack-del")
	const size = 1000
	packID, ids := packFixture(t, m, "pack-del", size, 3)

	if s := statsOf(t, m, packID); s.LiveBytes != 3*size || s.MembersLive != 3 {
		t.Fatalf("исходный учёт: live=%d members=%d, хотели %d и 3", s.LiveBytes, s.MembersLive, 3*size)
	}

	if _, err := m.DeleteObject(ctx, testTenant, meta.DeleteObjectInput{
		Bucket: "pack-del", Key: keyN(1), VersionID: ids[1].String(),
	}); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}

	s := statsOf(t, m, packID)
	if s.LiveBytes != 2*size {
		t.Errorf("live_bytes = %d, хотели %d: удаление версии не сняло её байты с пачки", s.LiveBytes, 2*size)
	}
	if s.MembersLive != 2 {
		t.Errorf("members_live = %d, хотели 2", s.MembersLive)
	}
	if s.LiveBytes != s.LiveFromMembers {
		t.Errorf("инвариант нарушен: live_bytes = %d, сумма участников = %d", s.LiveBytes, s.LiveFromMembers)
	}
	if broken, err := m.BrokenPacks(ctx); err != nil || len(broken) != 0 {
		t.Errorf("BrokenPacks нашёл расхождения: %+v (%v)", broken, err)
	}
}

// Удаление без versionId в версионированном бакете создаёт маркер: прежняя
// версия перестаёт быть текущей, но остаётся живой — её байты из пачки уходить
// не должны. Ошибка «маркер = удаление данных» сняла бы live_bytes зря, и
// уплотнение стало бы гоняться за призраками.
func TestPackLiveBytesOnDeleteMarker(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	mustBucket(t, m, "pack-marker")
	if err := m.SetBucketVersioning(ctx, testTenant, "pack-marker", meta.VersioningEnabled); err != nil {
		t.Fatalf("включить версии: %v", err)
	}
	const size = 700
	packID, _ := packFixture(t, m, "pack-marker", size, 2)

	if _, err := m.DeleteObject(ctx, testTenant, meta.DeleteObjectInput{
		Bucket: "pack-marker", Key: keyN(1),
	}); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}

	s := statsOf(t, m, packID)
	if s.LiveBytes != 2*size || s.MembersLive != 2 {
		t.Errorf("маркер удаления тронул живые байты: live=%d members=%d, хотели %d и 2",
			s.LiveBytes, s.MembersLive, 2*size)
	}
}

// Удаление самого маркера по versionId — штатный способ вернуть объект. Строка
// маркера исчезает физически, а живые байты пачки не меняются: у маркера их нет.
func TestPackLiveBytesOnMarkerRemoval(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	mustBucket(t, m, "pack-rm")
	if err := m.SetBucketVersioning(ctx, testTenant, "pack-rm", meta.VersioningEnabled); err != nil {
		t.Fatal(err)
	}
	const size = 500
	packID, _ := packFixture(t, m, "pack-rm", size, 2)

	res, err := m.DeleteObject(ctx, testTenant, meta.DeleteObjectInput{Bucket: "pack-rm", Key: keyN(1)})
	if err != nil || !res.IsDeleteMarker {
		t.Fatalf("создать маркер: %v %+v", err, res)
	}
	markerID := res.VersionID

	if _, err := m.DeleteObject(ctx, testTenant, meta.DeleteObjectInput{
		Bucket: "pack-rm", Key: keyN(1), VersionID: markerID,
	}); err != nil {
		t.Fatalf("удалить маркер: %v", err)
	}

	s := statsOf(t, m, packID)
	if s.LiveBytes != 2*size || s.MembersLive != 2 {
		t.Errorf("удаление маркера сдвинуло учёт: live=%d members=%d", s.LiveBytes, s.MembersLive)
	}
	if broken, err := m.BrokenPacks(ctx); err != nil || len(broken) != 0 {
		t.Errorf("BrokenPacks: %+v (%v)", broken, err)
	}
}

// Перезапись версии не уменьшает живое: прежняя версия перестаёт быть текущей,
// но её байты по-прежнему занимают место на ленте и могут быть прочитаны по
// versionId. «Освободить» их при перезаписи — значит занижить учёт.
func TestPackLiveBytesOnOverwrite(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	mustBucket(t, m, "pack-ow")
	if err := m.SetBucketVersioning(ctx, testTenant, "pack-ow", meta.VersioningEnabled); err != nil {
		t.Fatal(err)
	}
	const size = 400
	packID, _ := packFixture(t, m, "pack-ow", size, 2)

	in := putInput("pack-ow", keyN(1))
	in.OnlineUntil = time.Now().Add(24 * time.Hour)
	if _, err := m.PutObject(ctx, testTenant, in); err != nil {
		t.Fatalf("перезапись: %v", err)
	}

	s := statsOf(t, m, packID)
	if s.LiveBytes != 2*size || s.MembersLive != 2 {
		t.Errorf("перезапись списала живые байты: live=%d members=%d, хотели %d и 2",
			s.LiveBytes, s.MembersLive, 2*size)
	}
}

// Повторное удаление той же версии не должно списывать байты второй раз.
// Гонка «два воркера удаляют одну версию» — не гипотеза: deleter и DeleteObject
// по запросу клиента делают это независимо.
func TestPackLiveBytesNotDoubleCounted(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	mustBucket(t, m, "pack-race")
	const size = 800
	packID, ids := packFixture(t, m, "pack-race", size, 2)

	in := meta.DeleteObjectInput{Bucket: "pack-race", Key: keyN(0), VersionID: ids[0].String()}
	if _, err := m.DeleteObject(ctx, testTenant, in); err != nil {
		t.Fatalf("первое удаление: %v", err)
	}
	// Второе обращение той же версии уже ErrNotFound — строка в 'deleting'.
	if _, err := m.DeleteObject(ctx, testTenant, in); err == nil {
		t.Errorf("повторное удаление версии прошло как успех")
	}

	s := statsOf(t, m, packID)
	if s.LiveBytes != size {
		t.Errorf("live_bytes = %d, хотели %d: повтор списал байты дважды", s.LiveBytes, size)
	}
	if s.LiveBytes != s.LiveFromMembers {
		t.Errorf("инвариант нарушен: %d против %d", s.LiveBytes, s.LiveFromMembers)
	}
}

// Удаление тенанта (T4) уберёт строки объектов целиком, минуя 'deleting'.
// Пока этого пути нет, следим за инвариантом на массовом удалении: он ловит и
// «забыли пачку», и «списали не по тем строкам».
func TestBrokenPacksDetectsDrift(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	mustBucket(t, m, "pack-drift")
	const size = 300
	packID, _ := packFixture(t, m, "pack-drift", size, 2)

	if broken, err := m.BrokenPacks(ctx); err != nil || len(broken) != 0 {
		t.Fatalf("на ровном учёте BrokenPacks вернул %+v (%v)", broken, err)
	}

	// Ломаем учёт так, как сломает его забытый счётчик: строку удалили, а
	// пачку не предупредили.
	if _, err := m.Pool.Exec(ctx, `DELETE FROM objects WHERE pack_id = $1`, packID); err != nil {
		t.Fatalf("удаление участников: %v", err)
	}
	broken, err := m.BrokenPacks(ctx)
	if err != nil {
		t.Fatalf("BrokenPacks: %v", err)
	}
	if len(broken) != 1 || broken[0].ID != packID {
		t.Errorf("расхождение не обнаружено: %+v", broken)
	}
}

// Инвариант 2: перезапуск не возвращает в pending объекты запечатанной пачки.
//
// Возврат означал бы, что следующий цикл заведёт новую пачку с новым oid, а
// первая запись останется на ленте никем не учтённой.
func TestRequeueSkipsSealedObjects(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	mustBucket(t, m, "requeue-sealed")
	var bucketID int64
	if err := m.Pool.QueryRow(ctx, `SELECT id FROM buckets WHERE name = 'requeue-sealed'`).Scan(&bucketID); err != nil {
		t.Fatal(err)
	}
	var ids []uuid.UUID
	for i := 0; i < 3; i++ {
		id := uuid.New()
		if _, err := m.Pool.Exec(ctx,
			`INSERT INTO objects (id, bucket_id, key, size, etag, storage_class, state,
			                      landing, online_until, tenant_id, pool_id, is_current, wrapped_dek)
			  VALUES ($1,$2,$3,10,'etag','GLACIER','pending',true,
			          now() + interval '1 day', 1, 1, true, 'x'::bytea)`,
			id, bucketID, keyN(i)); err != nil {
			t.Fatalf("участник %d: %v", i, err)
		}
		ids = append(ids, id)
	}
	tx, packID, err := m.BeginPackArchive(ctx, 1, ids)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := m.SealPack(ctx, packID, []meta.Placement{
		{Version: ids[0], Offset: 0, Length: 10},
		{Version: ids[1], Offset: 10, Length: 10},
		{Version: ids[2], Offset: 20, Length: 10},
	}, 4096); err != nil {
		t.Fatal(err)
	}

	requeued, failed, err := m.RequeueArchiving(ctx, ids, "таймаут", 5, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if requeued != 0 || failed != 0 {
		t.Errorf("requeued=%d failed=%d, ждали 0/0: запечатанные объекты трогать нельзя", requeued, failed)
	}
	var states []string
	rows, err := m.Pool.Query(ctx, `SELECT state::text FROM objects WHERE id = ANY($1::uuid[])`, ids)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		states = append(states, s)
	}
	rows.Close()
	for _, s := range states {
		if s != "archiving" {
			t.Errorf("объект ушёл в %s — он же запечатан", s)
		}
	}

	// А определённый отказ — снимает членство целиком, иначе объект вернётся в
	// pending с висящим pack_id.
	if err := m.FailPackArchive(ctx, packID, ids, "отказ"); err != nil {
		t.Fatal(err)
	}
	var left int
	if err := m.Pool.QueryRow(ctx,
		`SELECT count(*) FROM objects WHERE id = ANY($1::uuid[]) AND pack_id IS NOT NULL`, ids).Scan(&left); err != nil {
		t.Fatal(err)
	}
	if left != 0 {
		t.Errorf("у %d объектов остался pack_id после определённого отказа", left)
	}
}
