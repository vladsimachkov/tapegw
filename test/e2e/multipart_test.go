package e2e

// Multipart против живого tapegw (M3 + T2 PR#2).
//
// Юнит-тесты backend'а проверяют шифрование и метаданные, но между клиентом и
// бэкендом стоит контроллер versitygw: он разбирает XML списка частей,
// проверяет номера частей, считает составной ETag и решает, какой код отдать.
// Только e2e и показывает, что весь путь проходит, а не только наша часть.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// mpuPartSize — размер первой части. Меньше 5 MiB нельзя (минимум S3), а брать
// ровно 5 MiB мало: граница части совпала бы с границей 64 KiB сегмента
// шифрования, и Range через неё ничего бы не проверял.
const mpuPartSize = 5 * 1024 * 1024

// mpuTailSize — хвост второй части: 1000 байт делают конец первой части
// серединой сегмента.
const mpuTailSize = 1000

// mpuData — содержимое собираемого объекта.
func mpuData() []byte {
	data := make([]byte, mpuPartSize+mpuTailSize)
	for i := range data {
		data[i] = byte(i*7 + i/1013)
	}
	return data
}

// uploadParts грузит части указанного размера и возвращает список для Complete.
func uploadParts(t *testing.T, c *s3.Client, bucket, key, uploadID string, sizes []int,
	data []byte,
) []types.CompletedPart {
	t.Helper()
	ctx := context.Background()

	var parts []types.CompletedPart
	start := 0
	for i, n := range sizes {
		body := data[start : start+n]
		out, err := c.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:        aws.String(bucket),
			Key:           aws.String(key),
			UploadId:      aws.String(uploadID),
			PartNumber:    aws.Int32(int32(i + 1)),
			Body:          bytes.NewReader(body),
			ContentLength: aws.Int64(int64(len(body))),
		})
		if err != nil {
			t.Fatalf("UploadPart(%d): %v", i+1, err)
		}
		parts = append(parts, types.CompletedPart{
			ETag: out.ETag, PartNumber: aws.Int32(int32(i + 1)),
		})
		start += n
	}
	return parts
}

// startMPU заводит загрузку и возвращает её идентификатор.
func startMPU(t *testing.T, c *s3.Client, bucket, key string) string {
	t.Helper()
	mpu, err := c.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	if mpu.UploadId == nil {
		t.Fatal("CreateMultipartUpload без UploadId")
	}
	return *mpu.UploadId
}

// Цикл целиком через контроллер: составной ETag, чтение целиком и Range через
// границу части.
func TestMultipartE2E(t *testing.T) {
	c := client(t)
	ctx := context.Background()
	bucket := uniqueBucket("m3mpu")
	if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	data := mpuData()
	uploadID := startMPU(t, c, bucket, "big")
	parts := uploadParts(t, c, bucket, "big", uploadID,
		[]int{mpuPartSize, mpuTailSize}, data)

	done, err := c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(bucket),
		Key:             aws.String("big"),
		UploadId:        aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}
	if done.ETag == nil {
		t.Fatal("Complete без ETag")
	}
	// Составной ETag: md5-N. Его считает контроллер versitygw по ETag частей из
	// нашего ответа, и это проверка, что наши ETag частей он распознал.
	if !strings.HasSuffix(strings.Trim(*done.ETag, `"`), "-2") {
		t.Errorf("ETag %q не составной", *done.ETag)
	}

	head, err := c.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("big"),
	})
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if head.ContentLength == nil || *head.ContentLength != int64(len(data)) {
		t.Errorf("ContentLength = %v, хотели %d", head.ContentLength, len(data))
	}

	get, err := c.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("big"),
	})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	got, err := io.ReadAll(get.Body)
	get.Body.Close()
	if err != nil {
		t.Fatalf("чтение тела: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("тело не совпало: %d байт против %d", len(got), len(data))
	}

	// Range через границу части: конец первой части приходится на середину
	// сегмента шифрования, и это единственный способ поймать смещение,
	// посчитанное от начала файла, а не от начала части.
	for _, spec := range []string{
		"bytes=65535-65540",     // граница сегмента внутри первой части
		"bytes=5242879-5242885", // через границу части
		"bytes=5242880-5243879", // вся вторая часть
		"bytes=5242870-5243000", // широкий диапазон через границу
	} {
		r, err := c.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket), Key: aws.String("big"), Range: aws.String(spec),
		})
		if err != nil {
			t.Fatalf("GetObject %s: %v", spec, err)
		}
		chunk, rerr := io.ReadAll(r.Body)
		r.Body.Close()
		if rerr != nil {
			t.Fatalf("чтение %s: %v", spec, rerr)
		}
		lo := rangeStart(t, spec)
		if !bytes.Equal(chunk, data[lo:lo+len(chunk)]) {
			t.Errorf("%s: байты не совпали", spec)
		}
	}
}

// Список частей и незавершённых загрузок, Abort убирает и то и другое.
func TestMultipartListE2E(t *testing.T) {
	c := client(t)
	ctx := context.Background()
	bucket := uniqueBucket("m3list")
	if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	uploadID := startMPU(t, c, bucket, "k")
	uploadParts(t, c, bucket, "k", uploadID, []int{mpuPartSize}, mpuData())

	list, err := c.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("ListMultipartUploads: %v", err)
	}
	if len(list.Uploads) != 1 || *list.Uploads[0].Key != "k" {
		t.Errorf("загрузки: %+v", list.Uploads)
	}

	parts, err := c.ListParts(ctx, &s3.ListPartsInput{
		Bucket: aws.String(bucket), Key: aws.String("k"), UploadId: aws.String(uploadID),
	})
	if err != nil {
		t.Fatalf("ListParts: %v", err)
	}
	if len(parts.Parts) != 1 || parts.Parts[0].Size == nil ||
		*parts.Parts[0].Size != int64(mpuPartSize) {
		t.Errorf("части: %+v", parts.Parts)
	}

	if _, err := c.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String("k"), UploadId: aws.String(uploadID),
	}); err != nil {
		t.Fatalf("AbortMultipartUpload: %v", err)
	}
	after, err := c.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("ListMultipartUploads после Abort: %v", err)
	}
	if len(after.Uploads) != 0 {
		t.Errorf("загрузки пережили Abort: %+v", after.Uploads)
	}
	if _, err := c.ListParts(ctx, &s3.ListPartsInput{
		Bucket: aws.String(bucket), Key: aws.String("k"), UploadId: aws.String(uploadID),
	}); err == nil {
		t.Errorf("ListParts после Abort не ошиблась")
	}
}

// Маленькая не-последняя часть: отказывает, и объект не появляется.
func TestMultipartSmallPartE2E(t *testing.T) {
	c := client(t)
	ctx := context.Background()
	bucket := uniqueBucket("m3small")
	if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	uploadID := startMPU(t, c, bucket, "k")

	// Первая часть — 1000 байт (меньше минимума), вторая — допустимая.
	data := append(make([]byte, 1000), make([]byte, mpuPartSize)...)
	parts := uploadParts(t, c, bucket, "k", uploadID, []int{1000, mpuPartSize}, data)

	_, err := c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(bucket),
		Key:             aws.String("k"),
		UploadId:        aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	})
	if err == nil {
		t.Fatal("маленькая не-последняя часть прошла")
	}
	// EntityTooSmall не входит в смоделированные ошибки CompleteMultipartUpload,
	// поэтому SDK отдаёт его как не смоделированную ошибку с кодом. Проверка по
	// коду, а не по типу: тип зависит от того, добавит ли SDK эту ошибку в модель.
	var apiErr interface{ ErrorCode() string }
	if !errors.As(err, &apiErr) || apiErr.ErrorCode() != "EntityTooSmall" {
		t.Errorf("ошибка %v, хотели EntityTooSmall", err)
	}

	// Объект не должен появиться: отказ на Complete не оставляет полусобранной
	// версии.
	if _, err := c.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k"),
	}); err == nil {
		t.Errorf("после отказа объект существует")
	}
}

// rangeStart — начальное смещение из "bytes=12-34".
func rangeStart(t *testing.T, spec string) int {
	t.Helper()
	body := strings.TrimPrefix(spec, "bytes=")
	lo, _, ok := strings.Cut(body, "-")
	if !ok {
		t.Fatalf("не Range: %s", spec)
	}
	var n int
	for _, ch := range lo {
		if ch < '0' || ch > '9' {
			t.Fatalf("не число: %s", spec)
		}
		n = n*10 + int(ch-'0')
	}
	return n
}
