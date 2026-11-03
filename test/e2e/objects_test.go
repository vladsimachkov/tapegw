package e2e

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
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
	// AWS отвечает InvalidStorageClass (400), а не NotImplemented (501): сервер
	// эту операцию умеет, плохой запрос.
	if !strings.Contains(err.Error(), "InvalidStorageClass") {
		t.Errorf("ожидали InvalidStorageClass, получили %T: %v", err, err)
	}
	var respErr *smithyhttp.ResponseError
	if errors.As(err, &respErr) && respErr.Response.StatusCode != 400 {
		t.Errorf("ожидали 400, получили %d", respErr.Response.StatusCode)
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

// Trailing-суммы. Современный SDK по умолчанию считает CRC32/CRC64NVME и
// кладёт их в трейлер aws-chunked, а не в заголовок, поэтому этот путь надо
// гонять сквозняком: юнит его не видит, потому что versitygw разбирает трейлер
// сам.
//
// Два прогона: настройки по умолчанию (сумма шлётся почти всегда) и
// WhenRequired (только когда реально обязательна). Разница между ними — как раз
// тот случай, когда алгоритм объявлен, а значения в заголовке нет.
func TestObjectChecksumModes(t *testing.T) {
	for _, mode := range []struct {
		name string
		val  aws.RequestChecksumCalculation
	}{
		{"по умолчанию", aws.RequestChecksumCalculationWhenSupported},
		{"when required", aws.RequestChecksumCalculationWhenRequired},
	} {
		t.Run(mode.name, func(t *testing.T) {
			c := clientOpts(t, func(o *s3.Options) {
				o.RequestChecksumCalculation = mode.val
			})
			ctx := context.Background()
			bucket := uniqueBucket("m2ck")
			if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
				t.Fatalf("CreateBucket: %v", err)
			}

			body := "тело, для которого SDK сам считает контрольную сумму"
			key := "obj.bin"
			put, err := c.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket), Key: aws.String(key),
				Body: strings.NewReader(body),
			})
			if err != nil {
				t.Fatalf("PutObject: %v", err)
			}

			// Что бы SDK ни прислал — заголовком или трейлером — содержимое
			// обязано читаться, а сумма, если она была, обязана вернуться.
			// Конкретный алгоритм по умолчанию менялся между версиями SDK,
			// поэтому сравниваем не его, а согласованность PUT и HEAD.
			head, err := c.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(bucket), Key: aws.String(key),
			})
			if err != nil {
				t.Fatalf("HeadObject: %v", err)
			}
			putSums := putChecksums(put)
			headSums := headChecksums(head)
			if len(putSums) != len(headSums) {
				t.Errorf("набор сумм разошёлся: PUT %v, HEAD %v", putSums, headSums)
			}
			for algo, v := range putSums {
				if headSums[algo] != v {
					t.Errorf("сумма %s: PUT %q, HEAD %q", algo, v, headSums[algo])
				}
			}

			get, err := c.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucket), Key: aws.String(key),
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
		})
	}
}

// Явная проверка: если SDK прислал checksum, она обязана сохраниться и
// вернуться тем же алгоритмом — иначе «сумма не сохранилась» пройдёт незамеченной.
func TestObjectChecksumEcho(t *testing.T) {
	c := client(t)
	ctx := context.Background()
	bucket := uniqueBucket("m2ckecho")
	if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	body := "проверяем, что сумма вернулась тем же алгоритмом"
	// CRC32 считаем руками и шлём явным заголовком: так проверяется путь
	// «значение в заголовке», независимо от того, что SDK делает по умолчанию.
	crc := crc32IEEE([]byte(body))
	put, err := c.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k"),
		Body: strings.NewReader(body), ChecksumCRC32: aws.String(crc),
	})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if aws.ToString(put.ChecksumCRC32) != crc {
		t.Errorf("Put вернул ChecksumCRC32=%q, ожидали %q", aws.ToString(put.ChecksumCRC32), crc)
	}

	head, err := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("k")})
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if aws.ToString(head.ChecksumCRC32) != crc {
		t.Errorf("HEAD вернул ChecksumCRC32=%q, ожидали %q", aws.ToString(head.ChecksumCRC32), crc)
	}
}

// clientOpts — тот же клиент, но с дополнительными настройками: нужен, чтобы
// проверить RequestChecksumCalculation.
func clientOpts(t *testing.T, opts ...func(*s3.Options)) *s3.Client {
	t.Helper()

	ep := os.Getenv("TAPEGW_E2E_ENDPOINT")
	if ep == "" {
		t.Skip("TAPEGW_E2E_ENDPOINT не задан — пропускаем e2e")
	}
	access, secret := os.Getenv("TAPEGW_E2E_ACCESS"), os.Getenv("TAPEGW_E2E_SECRET")
	if access == "" || secret == "" {
		t.Fatal("TAPEGW_E2E_ACCESS / TAPEGW_E2E_SECRET обязательны, когда задан endpoint")
	}

	base := s3.Options{
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider(access, secret, ""),
		BaseEndpoint: aws.String(ep),
		UsePathStyle: true,
	}
	return s3.New(base, opts...)
}

// putChecksums / headChecksums — непустые контрольные суммы ответа, map
// алгоритм → значение. Нужны, чтобы сравнить PUT и HEAD, не привязываясь к
// конкретному алгоритму по умолчанию.
func putChecksums(o *s3.PutObjectOutput) map[string]string {
	return nonEmptySums(map[string]string{
		"CRC32": aws.ToString(o.ChecksumCRC32), "CRC32C": aws.ToString(o.ChecksumCRC32C),
		"SHA1": aws.ToString(o.ChecksumSHA1), "SHA256": aws.ToString(o.ChecksumSHA256),
		"CRC64NVME": aws.ToString(o.ChecksumCRC64NVME), "SHA512": aws.ToString(o.ChecksumSHA512),
	})
}

func headChecksums(o *s3.HeadObjectOutput) map[string]string {
	return nonEmptySums(map[string]string{
		"CRC32": aws.ToString(o.ChecksumCRC32), "CRC32C": aws.ToString(o.ChecksumCRC32C),
		"SHA1": aws.ToString(o.ChecksumSHA1), "SHA256": aws.ToString(o.ChecksumSHA256),
		"CRC64NVME": aws.ToString(o.ChecksumCRC64NVME), "SHA512": aws.ToString(o.ChecksumSHA512),
	})
}

func nonEmptySums(m map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range m {
		if v != "" {
			out[k] = v
		}
	}
	return out
}

// crc32IEEE — CRC32 (Castagnoli нет, обычный IEEE) в base64, как требует S3.
func crc32IEEE(b []byte) string {
	h := crc32.NewIEEE()
	h.Write(b)
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}
