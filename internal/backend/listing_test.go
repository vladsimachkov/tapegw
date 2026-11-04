package backend_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/s3response"

	"github.com/vladsimachkov/tapegw/internal/meta"
)

// insertVersion — версия напрямую: PutObject создаёт версии только по состоянию
// версионирования бакета, а тесту нужна произвольная комбинация (маркер
// удаления, null-версия, не-текущая версия).
func insertVersion(t *testing.T, m *meta.Meta, bucket, key string,
	current, delMarker, nullVersion bool, size int64) string {
	t.Helper()
	if size == 0 && !delMarker {
		size = 1
	}
	var bucketID int64
	if err := m.Pool.QueryRow(context.Background(),
		`SELECT id FROM buckets WHERE name = $1`, bucket).Scan(&bucketID); err != nil {
		t.Fatalf("id бакета %s: %v", bucket, err)
	}
	id := uuid.New()
	if _, err := m.Pool.Exec(context.Background(),
		`INSERT INTO objects (id, bucket_id, key, size, etag, storage_class,
		                      state, landing, online_until, tenant_id, pool_id,
		                      is_current, is_delete_marker, is_null_version)
		  VALUES ($1,$2,$3,$4,'etag','GLACIER','pending',true,
		          now() + interval '1 day', $5, 1, $6, $7, $8)`,
		id, bucketID, key, size, testTenantID, current, delMarker, nullVersion); err != nil {
		t.Fatalf("вставка версии %s/%s: %v", bucket, key, err)
	}
	return id.String()
}

// Пункт 1 ревью: ключ с delete marker как текущей версией не попадает в
// ListObjects, но виден в ListObjectVersions.
func TestListObjectsHidesDeleteMarker(t *testing.T) {
	be, m, _ := newBackend(t)
	ctx := ownerCtx("root-owner")
	const bucket = "listbox"

	if err := be.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: ptr(bucket)}, nil); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	insertVersion(t, m, bucket, "a", true, false, false, 7)
	insertVersion(t, m, bucket, "b", false, false, false, 3)
	insertVersion(t, m, bucket, "b", true, true, false, 0)
	insertVersion(t, m, bucket, "c", true, false, false, 5)

	v1, err := be.ListObjects(ctx, &s3.ListObjectsInput{Bucket: ptr(bucket)})
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if got := objectKeys(v1.Contents); len(got) != 2 || got[0] != "a" || got[1] != "c" {
		t.Errorf("ListObjects v1 = %v, хотели [a c] — ключ с маркером удаления просочился", got)
	}

	v2, err := be.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: ptr(bucket)})
	if err != nil {
		t.Fatalf("ListObjectsV2: %v", err)
	}
	if got := objectKeys(v2.Contents); len(got) != 2 {
		t.Errorf("ListObjectsV2 = %v, хотели [a c]", got)
	}
	if v2.KeyCount == nil || *v2.KeyCount != 2 {
		t.Errorf("KeyCount = %v, хотели 2", v2.KeyCount)
	}

	// Тот же ключ обязан быть виден в версиях: и маркер, и версия под ним.
	vers, err := be.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: ptr(bucket)})
	if err != nil {
		t.Fatalf("ListObjectVersions: %v", err)
	}
	if len(vers.DeleteMarkers) != 1 || *vers.DeleteMarkers[0].Key != "b" {
		t.Errorf("маркеры удаления = %d, хотели один на ключе b", len(vers.DeleteMarkers))
	}
	var keys []string
	for _, v := range vers.Versions {
		keys = append(keys, *v.Key)
	}
	if len(keys) != 3 {
		t.Errorf("версий %d (ключи %v), хотели 3 — версия под маркером обязана остаться",
			len(keys), keys)
	}
}

// Пункт 2 ревью: null-версия везде отдаётся как versionId = "null", а не UUID.
func TestListObjectVersionsNullVersion(t *testing.T) {
	be, m, _ := newBackend(t)
	ctx := ownerCtx("root-owner")
	const bucket = "nullver"

	if err := be.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: ptr(bucket)}, nil); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	insertVersion(t, m, bucket, "k", true, false, true, 4)

	vers, err := be.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: ptr(bucket)})
	if err != nil {
		t.Fatalf("ListObjectVersions: %v", err)
	}
	if len(vers.Versions) != 1 {
		t.Fatalf("версий %d, хотели 1", len(vers.Versions))
	}
	if got := *vers.Versions[0].VersionId; got != "null" {
		t.Errorf("versionId = %q, хотели \"null\" — клиент, передавший его обратно, "+
			"должен получить ту же версию", got)
	}
}

// Пункт 3 ревью: пагинация, в том числе когда маркер падает в середину версий
// одного ключа. Проход до конца при разных размерах страницы — без потерь и
// дублей.
func TestListPaginationSweep(t *testing.T) {
	be, m, _ := newBackend(t)
	ctx := ownerCtx("root-owner")
	const bucket = "pages"

	if err := be.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: ptr(bucket)}, nil); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	// Три ключа по две версии + ключ с маркером удаления сверху.
	var want []string
	for _, key := range []string{"k1", "k2", "k3"} {
		var ids []string
		for i := 1; i <= 2; i++ {
			ids = append(ids, insertVersion(t, m, bucket, key, false, false, false, int64(i)))
		}
		for i := len(ids) - 1; i >= 0; i-- {
			want = append(want, key+":"+ids[i])
		}
	}

	for _, page := range []int{1, 2, 3, 5, 6, 10} {
		var got []string
		keyMarker, verMarker := "", ""
		for i := 0; ; i++ {
			if i > 12 {
				t.Fatalf("страница %d: листинг не заканчивается", page)
			}
			in := &s3.ListObjectVersionsInput{
				Bucket: ptr(bucket), MaxKeys: ptr(int32(page)),
			}
			if keyMarker != "" {
				in.KeyMarker = ptr(keyMarker)
				in.VersionIdMarker = ptr(verMarker)
			}
			res, err := be.ListObjectVersions(ctx, in)
			if err != nil {
				t.Fatalf("страница %d: %v", page, err)
			}
			for _, v := range res.Versions {
				got = append(got, fmt.Sprintf("%s:%s", *v.Key, *v.VersionId))
			}
			if res.IsTruncated == nil || !*res.IsTruncated {
				break
			}
			keyMarker, verMarker = *res.NextKeyMarker, derefStr(res.NextVersionIdMarker)
		}
		if len(got) != len(want) {
			t.Errorf("страница %d: получено %v, хотели %v", page, got, want)
			continue
		}
		for i := range got {
			if got[i] != want[i] {
				t.Errorf("страница %d: позиция %d = %s, хотели %s", page, i, got[i], want[i])
				break
			}
		}
	}
}

// ContinuationToken V2 — непрозрачный: клиент обязан передавать его как есть.
func TestListObjectsV2ContinuationToken(t *testing.T) {
	be, m, _ := newBackend(t)
	ctx := ownerCtx("root-owner")
	const bucket = "v2tok"

	if err := be.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: ptr(bucket)}, nil); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	for i := 0; i < 5; i++ {
		insertVersion(t, m, bucket, fmt.Sprintf("k%d", i), true, false, false, 1)
	}

	var got []string
	var token *string
	for i := 0; ; i++ {
		if i > 8 {
			t.Fatal("листинг V2 не заканчивается")
		}
		in := &s3.ListObjectsV2Input{Bucket: ptr(bucket), MaxKeys: ptr(int32(2))}
		if token != nil {
			in.ContinuationToken = token
		}
		res, err := be.ListObjectsV2(ctx, in)
		if err != nil {
			t.Fatalf("страница %d: %v", i, err)
		}
		got = append(got, objectKeys(res.Contents)...)
		if res.IsTruncated == nil || !*res.IsTruncated {
			break
		}
		token = res.NextContinuationToken
	}
	if len(got) != 5 {
		t.Errorf("получено %d ключей (%v), хотели 5", len(got), got)
	}
}

// Чужой тенант получает ПУСТУЮ выдачу, а не ошибку: по ответу «пусто» нельзя
// узнать, что бакет существует.
func TestListObjectsForeignTenantSeesEmpty(t *testing.T) {
	be, m, _ := newBackend(t)
	owner := ownerCtx("root-owner")
	const bucket = "secret-list"

	if err := be.CreateBucket(owner, &s3.CreateBucketInput{Bucket: ptr(bucket)}, nil); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	insertVersion(t, m, bucket, "visible", true, false, false, 1)

	foreign := accountCtx(auth.Account{Access: "intruder", Role: auth.RoleUser, ProjectID: testProjectID + 1})
	res, err := be.ListObjects(foreign, &s3.ListObjectsInput{Bucket: ptr(bucket)})
	if err != nil {
		t.Fatalf("чужому тенанту листинг вернул ошибку %v, ожидали пустую выдачу", err)
	}
	if len(res.Contents) != 0 {
		t.Errorf("чужой тенант увидел %d объектов — страж не отфильтровал выдачу",
			len(res.Contents))
	}

	vers, err := be.ListObjectVersions(foreign, &s3.ListObjectVersionsInput{Bucket: ptr(bucket)})
	if err != nil {
		t.Fatalf("чужому тенанту листинг версий вернул ошибку %v", err)
	}
	if len(vers.Versions) != 0 || len(vers.DeleteMarkers) != 0 {
		t.Errorf("чужой тенант увидел %d версий и %d маркеров",
			len(vers.Versions), len(vers.DeleteMarkers))
	}
}

func objectKeys(objs []s3response.Object) []string {
	out := make([]string, 0, len(objs))
	for _, o := range objs {
		out = append(out, *o.Key)
	}
	return out
}

func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
