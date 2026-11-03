package e2e

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// M2: online-путь целиком через HTTP — PUT, HEAD, GET. Здесь важно, что путь
// проходит не только юниты: versitygw сама разбирает заголовки контрольных
// сумм и Range, и только сквозной прогон показывает, что мы ничего не потеряли
// по дороге в бэкенд и обратно.

func putObject(t *testing.T, c *s3.Client, bucket, key, body string) *s3.PutObjectOutput {
	t.Helper()
	out, err := c.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(body),
	})
	if err != nil {
		t.Fatalf("PutObject(%s): %v", key, err)
	}
	return out
}

func TestObjectPutHeadGet(t *testing.T) {
	c := client(t)
	ctx := context.Background()
	bucket := uniqueBucket("m2obj")
	if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	body := "раз, два, три — проверка сквозного пути"
	put := putObject(t, c, bucket, "dir/obj.txt", body)

	sum := md5.Sum([]byte(body))
	wantEtag := `"` + hexString(sum[:]) + `"`
	if aws.ToString(put.ETag) != wantEtag {
		t.Errorf("ETag = %s, хотели %s", aws.ToString(put.ETag), wantEtag)
	}

	head, err := c.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("dir/obj.txt"),
	})
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if aws.ToInt64(head.ContentLength) != int64(len(body)) {
		t.Errorf("ContentLength = %d", aws.ToInt64(head.ContentLength))
	}
	// ADR-0003: класс всегда GLACIER.
	if head.StorageClass != types.StorageClassGlacier {
		t.Errorf("storage class = %v, хотели GLACIER", head.StorageClass)
	}
	// Онлайновый объект без restore не должен нести x-amz-restore: фиктивная
	// дата (2050 у scoutfs) сделала бы обещание доступности ложным.
	if head.Restore != nil {
		t.Errorf("x-amz-restore у онлайнового объекта: %q", *head.Restore)
	}

	get, err := c.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("dir/obj.txt"),
	})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer get.Body.Close()
	got, err := io.ReadAll(get.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != body {
		t.Errorf("содержимое разошлось: %q", got)
	}
}

func TestObjectOverwrite(t *testing.T) {
	c := client(t)
	ctx := context.Background()
	bucket := uniqueBucket("m2ow")
	if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	putObject(t, c, bucket, "k", "первое")
	putObject(t, c, bucket, "k", "второе значение")

	get, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String("k")})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer get.Body.Close()
	got, _ := io.ReadAll(get.Body)
	if string(got) != "второе значение" {
		t.Errorf("после перезаписи получили %q", got)
	}
	if aws.ToInt64(get.ContentLength) != int64(len("второе значение")) {
		t.Errorf("ContentLength не обновился: %d", aws.ToInt64(get.ContentLength))
	}
}

func TestObjectRange(t *testing.T) {
	c := client(t)
	ctx := context.Background()
	bucket := uniqueBucket("m2range")
	if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	putObject(t, c, bucket, "k", "0123456789")

	get, err := c.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k"), Range: aws.String("bytes=3-6"),
	})
	if err != nil {
		t.Fatalf("GetObject range: %v", err)
	}
	defer get.Body.Close()
	got, _ := io.ReadAll(get.Body)
	if string(got) != "3456" {
		t.Errorf("range дал %q", got)
	}
	if get.ContentRange == nil || *get.ContentRange != "bytes 3-6/10" {
		t.Errorf("ContentRange = %v", get.ContentRange)
	}
}

func TestObjectContentMD5(t *testing.T) {
	c := client(t)
	ctx := context.Background()
	bucket := uniqueBucket("m2md5")
	if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	body := "проверяем контрольную сумму"
	sum := md5.Sum([]byte(body))

	// Верная сумма проходит.
	if _, err := c.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("good"),
		Body: strings.NewReader(body), ContentMD5: aws.String(base64.StdEncoding.EncodeToString(sum[:])),
	}); err != nil {
		t.Fatalf("корректный Content-MD5 должен проходить: %v", err)
	}

	// Неверная — отказ, и код обязан быть digest-ным, а не 500.
	wrong := md5.Sum([]byte("другие данные"))
	_, err := c.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("bad"),
		Body: strings.NewReader(body), ContentMD5: aws.String(base64.StdEncoding.EncodeToString(wrong[:])),
	})
	if err == nil {
		t.Fatalf("ожидали отказ при неверном Content-MD5")
	}
	// versitygw отдаёт BadDigest собственным кодом, которого нет в types.*
	// как класса исключений, поэтому проверяем устойчивое: это 4xx, а не 500.
	// Клиент обязан получить внятный ответ, а не «внутреннюю ошибку».
	var respErr *smithyhttp.ResponseError
	if !errors.As(err, &respErr) {
		t.Fatalf("ожидали HTTP-ошибку, получили %T: %v", err, err)
	}
	if code := respErr.Response.StatusCode; code < 400 || code >= 500 {
		t.Errorf("ожидали 4xx на неверный Content-MD5, получили %d: %v", code, err)
	}
}

func TestObjectRejectsForeignStorageClass(t *testing.T) {
	c := client(t)
	ctx := context.Background()
	bucket := uniqueBucket("m2sc")
	if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	_, err := c.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k"),
		Body: strings.NewReader("x"), StorageClass: types.StorageClassStandard,
	})
	if err == nil {
		t.Fatalf("STANDARD должны отвергаться (ADR-0003)")
	}
	// Код приходит строкой в тексте ошибки API (так же сравнивает buckets_test).
	// Важен именно NotImplemented: 500 — тоже отказ, но он про баг, а не про
	// честное «не поддерживаем».
	if !strings.Contains(err.Error(), "NotImplemented") {
		t.Errorf("ожидали NotImplemented, получили %T: %v", err, err)
	}
}

func TestObjectGetMissing(t *testing.T) {
	c := client(t)
	ctx := context.Background()
	bucket := uniqueBucket("m2miss")
	if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	_, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String("нет")})
	var nsk *types.NoSuchKey
	if !errors.As(err, &nsk) {
		t.Errorf("ожидали NoSuchKey, получили %T: %v", err, err)
	}
}

func hexString(b []byte) string {
	const hexDigits = "0123456789abcdef"
	out := make([]byte, 0, len(b)*2)
	for _, c := range b {
		out = append(out, hexDigits[c>>4], hexDigits[c&0xf])
	}
	return string(out)
}
