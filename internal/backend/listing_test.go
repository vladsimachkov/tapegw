package backend_test

import (
	"context"
	"fmt"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	stypes "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/versity/versitygw/auth"
	vs3err "github.com/versity/versitygw/s3err"
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

// Чужой бакет для листинга — тот же AccessDenied, что и в точечных операциях
// (ADR-0017). «Пустая выдача ничего не выдаёт» здесь не аргумент: имена бакетов
// глобальны и существование чужого бакета так или иначе выдаёт CreateBucket, а
// для обычной учётки versitygw вызывает GetBucketAcl до листинга и тот отказывает
// сам. Фильтрация только сделала бы поведение разным для разных путей.
func TestListObjectsForeignTenantDenied(t *testing.T) {
	be, m, _ := newBackend(t)
	owner := ownerCtx("root-owner")
	const bucket = "secret-list"

	if err := be.CreateBucket(owner, &s3.CreateBucketInput{Bucket: ptr(bucket)}, nil); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	insertVersion(t, m, bucket, "visible", true, false, false, 1)

	foreign := accountCtx(auth.Account{Access: "intruder", Role: auth.RoleUser, ProjectID: testProjectID + 1})
	if _, err := be.ListObjects(foreign, &s3.ListObjectsInput{Bucket: ptr(bucket)}); !isS3Code(err, vs3err.ErrAccessDenied) {
		t.Errorf("чужому тенанту ListObjects вернул %v, хотели AccessDenied", err)
	}
	if _, err := be.ListObjectsV2(foreign, &s3.ListObjectsV2Input{Bucket: ptr(bucket)}); !isS3Code(err, vs3err.ErrAccessDenied) {
		t.Errorf("чужому тенанту ListObjectsV2 вернул %v, хотели AccessDenied", err)
	}
	if _, err := be.ListObjectVersions(foreign, &s3.ListObjectVersionsInput{Bucket: ptr(bucket)}); !isS3Code(err, vs3err.ErrAccessDenied) {
		t.Errorf("чужому тенанту ListObjectVersions вернул %v, хотели AccessDenied", err)
	}
}

// Листинг несуществующего бакета обязан быть NoSuchBucket. Под администратором:
// у root/admin versitygw не вызывает GetBucketAcl перед листингом, поэтому
// проверка лежит целиком на нас, и «пусто вместо ошибки» здесь прошло бы
// незамеченным для всего остального кода.
func TestListObjectsMissingBucket(t *testing.T) {
	be, _, _ := newBackend(t)
	ctx := adminCtx()

	cases := map[string]func() error{
		"ListObjects": func() error {
			_, err := be.ListObjects(ctx, &s3.ListObjectsInput{Bucket: ptr("no-such-bucket")})
			return err
		},
		"ListObjectsV2": func() error {
			_, err := be.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: ptr("no-such-bucket")})
			return err
		},
		"ListObjectVersions": func() error {
			_, err := be.ListObjectVersions(ctx,
				&s3.ListObjectVersionsInput{Bucket: ptr("no-such-bucket")})
			return err
		},
	}
	for name, call := range cases {
		if err := call(); !isS3Code(err, vs3err.ErrNoSuchBucket) {
			t.Errorf("%s несуществующего бакета: %v, хотели NoSuchBucket — пустой "+
				"ответ клиент читает как «бакет пуст» и не замечает опечатку в имени", name, err)
		}
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

// Ключи с «+», «%», пробелом и юникодом через ListObjectVersions с
// encoding-type=url. Клиент (botocore) при этом поле декодирует ответ, и
// некодированный «a+b» вернулся бы как «a b»: имени, которое клиент получил, не
// существует, прочитать или удалить эту версию он уже не сможет.
func TestListObjectVersionsURLEncoding(t *testing.T) {
	be, m, _ := newBackend(t)
	ctx := ownerCtx("root-owner")
	const bucket = "encoded"

	if err := be.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: ptr(bucket)}, nil); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	keys := []string{"a+b", "50%", "with space", "ключ/тест", "tilde~x", "star*x"}
	for _, k := range keys {
		insertVersion(t, m, bucket, k, true, false, false, 1)
	}

	res, err := be.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: ptr(bucket), EncodingType: stypes.EncodingTypeUrl,
	})
	if err != nil {
		t.Fatalf("ListObjectVersions: %v", err)
	}
	if res.EncodingType != stypes.EncodingTypeUrl {
		t.Fatalf("EncodingType = %q, хотели url", res.EncodingType)
	}
	if len(res.Versions) != len(keys) {
		t.Fatalf("версий %d, хотели %d", len(res.Versions), len(keys))
	}

	// Декодируем ровно как клиент: обратное преобразование к percent-encoding.
	got := make([]string, 0, len(res.Versions))
	for _, v := range res.Versions {
		dec, err := url.QueryUnescape(*v.Key)
		if err != nil {
			t.Fatalf("клиент не смог декодировать %q: %v", *v.Key, err)
		}
		got = append(got, dec)
	}
	sort.Strings(got)
	want := append([]string{}, keys...)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("после декодирования %v, хотели %v", got, want)
	}

	// «+» обязан быть закодирован: иначе он приедет пробелом.
	for _, v := range res.Versions {
		if strings.Contains(*v.Key, " ") {
			t.Errorf("в закодированном ключе пробел: %q", *v.Key)
		}
	}

	// Точная форма кодирования, а не только «декодируется обратно». Разбор
	// «закодировали — декодировали» не различает рецепт versitygw и наивный
	// QueryEscape: оба обратимы, но первый оставляет «/» и «*» как есть и
	// экранирует «~», а второй закодирует всё. Кливер сверяет байты, а не
	// обратимость.
	res2, err := be.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: ptr(bucket), Prefix: ptr("star*x"), EncodingType: stypes.EncodingTypeUrl,
	})
	if err != nil {
		t.Fatalf("ListObjectVersions с prefix: %v", err)
	}
	if len(res2.Versions) != 1 {
		t.Fatalf("по prefix star*x версий %d, хотели 1", len(res2.Versions))
	}
	if got := *res2.Versions[0].Key; got != "star*x" {
		t.Errorf("ключ star*x закодирован как %q, хотели star*x — «*» у S3 не кодируется", got)
	}
	if got := *res2.Prefix; got != "star*x" {
		t.Errorf("prefix закодирован как %q, хотели star*x", got)
	}
	res3, err := be.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: ptr(bucket), Prefix: ptr("tilde~x"), EncodingType: stypes.EncodingTypeUrl,
	})
	if err != nil {
		t.Fatalf("ListObjectVersions с prefix: %v", err)
	}
	if len(res3.Versions) == 1 {
		if got := *res3.Versions[0].Key; got != "tilde%7Ex" {
			t.Errorf("ключ tilde~x закодирован как %q, хотели tilde%%7Ex — «~» у S3 экранируется", got)
		}
	}
}

// Для v1 и v2 ключи кодирует контроллер versitygw, а не бэкенд: если закодирует
// бэкенд, контроллер закодирует второй раз, и «a b» станет «a%2520b».
func TestListObjectsV1V2DoNotEncodeInBackend(t *testing.T) {
	be, m, _ := newBackend(t)
	ctx := ownerCtx("root-owner")
	const bucket = "rawkeys"

	if err := be.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: ptr(bucket)}, nil); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	insertVersion(t, m, bucket, "a+b", true, false, false, 1)

	v1, err := be.ListObjects(ctx, &s3.ListObjectsInput{
		Bucket: ptr(bucket), EncodingType: stypes.EncodingTypeUrl,
	})
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if len(v1.Contents) != 1 || *v1.Contents[0].Key != "a+b" {
		t.Errorf("ListObjects v1 закодировал ключ сам: %v — контроллер кодирует "+
			"повторно, и клиент получит двойное кодирование", objectKeys(v1.Contents))
	}

	v2, err := be.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: ptr(bucket), EncodingType: stypes.EncodingTypeUrl,
	})
	if err != nil {
		t.Fatalf("ListObjectsV2: %v", err)
	}
	if len(v2.Contents) != 1 || *v2.Contents[0].Key != "a+b" {
		t.Errorf("ListObjectsV2 закодировал ключ сам: %v", objectKeys(v2.Contents))
	}
}

// encoding-type сравнивается без учёта регистра: для v1/v2 versitygw принимает
// и «uRl», а для версий контроллер параметр не разбирает — регистр доходит до
// нас. Чувствительное сравнение вернула бы «URL» эхом без кодирования, и клиент
// стал бы декодировать то, что мы не кодировали.
func TestListObjectVersionsEncodingTypeCase(t *testing.T) {
	be, m, _ := newBackend(t)
	ctx := ownerCtx("root-owner")
	const bucket = "caseenc"

	if err := be.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: ptr(bucket)}, nil); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	insertVersion(t, m, bucket, "a+b", true, false, false, 1)

	res, err := be.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: ptr(bucket), EncodingType: stypes.EncodingType("URL"),
	})
	if err != nil {
		t.Fatalf("ListObjectVersions: %v", err)
	}
	if res.EncodingType != stypes.EncodingTypeUrl {
		t.Errorf("EncodingType = %q, хотели нормализованное url", res.EncodingType)
	}
	if len(res.Versions) != 1 {
		t.Fatalf("версий %d, хотели 1", len(res.Versions))
	}
	if got := *res.Versions[0].Key; got != "a%2Bb" {
		t.Errorf("ключ при encoding-type=URL закодирован как %q, хотели a%%2Bb", got)
	}
}
