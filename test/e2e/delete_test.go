package e2e

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5"
)

// Заголовки ответа DeleteObject: x-amz-delete-marker и x-amz-version-id
// созданного маркера. По ним клиент понимает, что произошло: объект «удалён»,
// но данные живы под маркером. Теряется заголовок — и клиент видит 200 без
// объяснения, а повторное удаление ведёт себя иначе, чем первое.

func setBucketVersioning(t *testing.T, dsn, bucket, state string) {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatalf("подключение к стенду: %v", err)
	}
	defer conn.Close(context.Background())

	// PutBucketVersioning появится следующим шагом этапа V; пока версионирование
	// включается прямой записью — иначе этот тест нельзя написать вообще.
	tag, err := conn.Exec(context.Background(),
		`UPDATE buckets SET versioning = $2 WHERE name = $1`, bucket, state)
	if err != nil {
		t.Fatalf("versioning %s: %v", bucket, err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatalf("бакет %s не найден в схеме стенда", bucket)
	}
}

func TestDeleteObjectHeaders(t *testing.T) {
	c := client(t)
	dsn := requireStand(t)
	bucket := uniqueBucket("delhdr")

	if _, err := c.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	setBucketVersioning(t, dsn, bucket, "enabled")

	put, err := c.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k"),
		Body: strings.NewReader("payload"),
	})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	dataVersion := aws.ToString(put.VersionId)
	if dataVersion == "" {
		t.Fatalf("PutObject в бакет с версиями не вернул x-amz-version-id")
	}

	out, err := c.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k"),
	})
	if err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	if out.DeleteMarker == nil || !*out.DeleteMarker {
		t.Errorf("x-amz-delete-marker не пришёл: %v", out.DeleteMarker)
	}
	marker := aws.ToString(out.VersionId)
	if marker == "" {
		t.Fatal("x-amz-version-id маркера не пришёл")
	}
	if marker == dataVersion {
		t.Errorf("x-amz-version-id = id данных (%s), а должен быть id маркера", marker)
	}

	// Ключ после удаления недоступен, а версия под маркером жива.
	if _, err := c.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k"),
	}); err == nil {
		t.Errorf("GET после удаления вернул данные")
	}

	vers, err := c.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("ListObjectVersions: %v", err)
	}
	if len(vers.DeleteMarkers) != 1 || aws.ToString(vers.DeleteMarkers[0].VersionId) != marker {
		t.Errorf("маркеров удаления %d, хотели один с id %s", len(vers.DeleteMarkers), marker)
	}
	if len(vers.Versions) != 1 || aws.ToString(vers.Versions[0].VersionId) != dataVersion {
		t.Errorf("версий %d, хотели одну живую (%s) — данные под маркером трогать нельзя",
			len(vers.Versions), dataVersion)
	}
}

// x-amz-version-id на PUT: без него версия ничем не адресуется — ни прочитать
// конкретную, ни удалить, ни поставить retention. Для бакета без версий
// заголовка нет вовсе, для приостановленного — "null".
func TestPutObjectVersionIDHeader(t *testing.T) {
	c := client(t)
	dsn := requireStand(t)
	ctx := context.Background()

	cases := []struct {
		versioning string
		want       string // "" — заголовка нет
	}{
		{"unversioned", ""},
		{"enabled", "uuid"},
		{"suspended", "null"},
	}
	for _, tc := range cases {
		bucket := uniqueBucket("putver")
		if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
			t.Fatalf("CreateBucket: %v", err)
		}
		setBucketVersioning(t, dsn, bucket, tc.versioning)

		out, err := c.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket), Key: aws.String("k"),
			Body: strings.NewReader("x"),
		})
		if err != nil {
			t.Fatalf("PutObject (%s): %v", tc.versioning, err)
		}
		got := aws.ToString(out.VersionId)
		switch tc.want {
		case "":
			if got != "" {
				t.Errorf("%s: x-amz-version-id = %q, а заголовка быть не должно", tc.versioning, got)
			}
		case "uuid":
			if len(got) != 36 {
				t.Errorf("%s: x-amz-version-id = %q, хотели UUID", tc.versioning, got)
			}
		default:
			if got != tc.want {
				t.Errorf("%s: x-amz-version-id = %q, хотели %q", tc.versioning, got, tc.want)
			}
		}
	}
}
