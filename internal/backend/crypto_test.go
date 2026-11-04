package backend_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/versity/versitygw/s3response"

	"github.com/vladsimachkov/tapegw/internal/backend"
	"github.com/vladsimachkov/tapegw/internal/encrypt"
	"github.com/vladsimachkov/tapegw/internal/keys"
	"github.com/vladsimachkov/tapegw/internal/meta"
)

// putBytes пишет объект произвольного размера (putBody в тестах держит строку).
func putBytes(t *testing.T, be *backend.TapeGW, bucket, key string, data []byte) {
	t.Helper()
	out, err := be.PutObject(testCtx(), s3response.PutObjectInput{
		Bucket:        ptr(bucket),
		Key:           ptr(key),
		Body:          bytes.NewReader(data),
		ContentLength: ptr(int64(len(data))),
	})
	if err != nil {
		t.Fatalf("PutObject(%d байт): %v", len(data), err)
	}
	_ = out
}

func filler(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*31 + i/977)
	}
	return b
}

func currentObject(t *testing.T, m *meta.Meta, bucket, key string) *meta.Object {
	t.Helper()
	obj, err := m.GetObject(context.Background(), meta.NewTenant(testProjectID), bucket, key)
	if err != nil {
		t.Fatalf("строка объекта %s/%s: %v", bucket, key, err)
	}
	return obj
}

func readBody(t *testing.T, body io.ReadCloser) ([]byte, error) {
	t.Helper()
	defer body.Close()
	return io.ReadAll(body)
}

// Пункт приёмки 1: Range на границах сегментов. Границы — 65536 открытого и
// 65540 шифротекста: сдвиг на 16 байт тега за каждый предшествующий сегмент
// обязан считаться от номера сегмента, а не пропорцией от размера.
func TestGetRangeAcrossSegments(t *testing.T) {
	be, _, _ := newBackend(t)
	mustBucket(t, be)
	const S = encrypt.SegmentSize
	data := filler(3*S + 1000)
	putBytes(t, be, "b", "big", data)

	cases := []struct {
		rng    string
		off, n int
	}{
		{"bytes=0-0", 0, 1},
		{"bytes=65534-65535", S - 2, 2},
		{"bytes=65535-65536", S - 1, 2},  // ровно через границу
		{"bytes=65536-65537", S, 2},      // начало следующего сегмента
		{"bytes=65530-65600", S - 6, 71}, // с запасом через границу
		{fmt.Sprintf("bytes=%d-", 2*S), 2 * S, len(data) - 2*S},
		{"bytes=-1000", len(data) - 1000, 1000},
		{"bytes=0-", 0, len(data)},
		{"", 0, len(data)},
		{fmt.Sprintf("bytes=%d-%d", 3*S, 3*S+999), 3 * S, 1000}, // остаток в конце
	}
	for _, c := range cases {
		in := &s3.GetObjectInput{Bucket: ptr("b"), Key: ptr("big")}
		if c.rng != "" {
			in.Range = ptr(c.rng)
		}
		out, err := be.GetObject(testCtx(), in)
		if err != nil {
			t.Fatalf("GET range=%q: %v", c.rng, err)
		}
		got, err := readBody(t, out.Body)
		if err != nil {
			t.Fatalf("чтение range=%q: %v", c.rng, err)
		}
		if want := data[c.off : c.off+c.n]; !bytes.Equal(got, want) {
			t.Errorf("range=%q: байты не совпали (получено %d, ждали %d)", c.rng, len(got), len(want))
		}
	}
}

// Пункт приёмки 2: порча одного байта шифротекста даёт ошибку, а не мусор.
// Это главная причина потегного шифрования: лента и диск портятся молча.
func TestCorruptedCiphertextFailsRead(t *testing.T) {
	const n = 5000
	for _, pos := range []int{0, 100, n + encrypt.TagSize - 1} {
		t.Run(itoa(pos), func(t *testing.T) {
			be, m, land := newBackend(t)
			mustBucket(t, be)
			data := filler(n)
			putBytes(t, be, "b", "k", data)

			obj := currentObject(t, m, "b", "k")
			path := land.Path(obj.ID)
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("файл спула: %v", err)
			}
			if pos >= len(raw) {
				t.Fatalf("позиция %d вне файла (%d байт)", pos, len(raw))
			}
			raw[pos] ^= 0x01
			if err := os.WriteFile(path, raw, 0o600); err != nil {
				t.Fatalf("запись испорченного файла: %v", err)
			}

			out, err := be.GetObject(testCtx(), &s3.GetObjectInput{Bucket: ptr("b"), Key: ptr("k")})
			if err != nil {
				t.Fatalf("GET не должен отказать на уровне метаданных: %v", err)
			}
			got, rerr := readBody(t, out.Body)
			if rerr == nil {
				t.Fatalf("порча не обнаружена: прочитано %d байт", len(got))
			}
			if bytes.Contains(got, data[:32]) {
				t.Errorf("вместе с ошибкой вернули открытые байты")
			}
			if strings.Contains(rerr.Error(), string(data)) {
				t.Errorf("в ошибке чтения оказались данные: %v", rerr)
			}
		})
	}
}

// Пункт приёмки 3: после уничтожения ключа тенанта чтение даёт ошибку.
func TestReadAfterKeyDestroyFails(t *testing.T) {
	be, m, _ := newBackend(t)
	mustBucket(t, be)
	putBytes(t, be, "b", "k", filler(100))

	if err := m.DestroyTenantKey(context.Background(), testTenantID); err != nil {
		t.Fatalf("DestroyTenantKey: %v", err)
	}
	out, err := be.GetObject(testCtx(), &s3.GetObjectInput{Bucket: ptr("b"), Key: ptr("k")})
	if err == nil {
		_, err = readBody(t, out.Body)
	}
	if err == nil {
		t.Fatal("чтение после уничтожения ключа тенанта удалось — шифрование ничего не даёт")
	}
	if !strings.Contains(err.Error(), keys.ErrKeyGone.Error()) &&
		!strings.Contains(err.Error(), "ключ") {
		t.Errorf("ошибка %v, хотели про ключ тенанта", err)
	}
}

// Пункт приёмки 6: ключевой материал не попадает в текст ошибки.
func TestKeyNotInReadError(t *testing.T) {
	be, m, land := newBackend(t)
	mustBucket(t, be)
	putBytes(t, be, "b", "k", filler(64))

	obj := currentObject(t, m, "b", "k")
	tek, err := m.TenantKey(context.Background(), testTenantID)
	if err != nil {
		t.Fatalf("TenantKey: %v", err)
	}
	if len(tek.Wrapped) == 0 {
		t.Fatal("TEK не завернут")
	}

	path := land.Path(obj.ID)
	raw, _ := os.ReadFile(path)
	raw[0] ^= 0x01
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("порча: %v", err)
	}
	out, err := be.GetObject(testCtx(), &s3.GetObjectInput{Bucket: ptr("b"), Key: ptr("k")})
	if err != nil {
		return // отказ до чтения тоже подходит
	}
	_, rerr := readBody(t, out.Body)
	if rerr == nil {
		t.Fatal("порча не обнаружена")
	}
	for _, secret := range [][]byte{tek.Wrapped, obj.WrappedDEK} {
		if strings.Contains(rerr.Error(), string(secret)) {
			t.Errorf("в ошибке чтения оказался ключевой материал: %v", rerr)
		}
	}
}

// Шифрование включено всегда — клиент обязан это видеть (как SSE-S3).
func TestServerSideEncryptionHeader(t *testing.T) {
	be, _, _ := newBackend(t)
	mustBucket(t, be)
	putBytes(t, be, "b", "k", filler(10))

	get, err := be.GetObject(testCtx(), &s3.GetObjectInput{Bucket: ptr("b"), Key: ptr("k")})
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	get.Body.Close()
	if get.ServerSideEncryption == "" {
		t.Errorf("GET без x-amz-server-side-encryption")
	}

	head, err := be.HeadObject(testCtx(), &s3.HeadObjectInput{Bucket: ptr("b"), Key: ptr("k")})
	if err != nil {
		t.Fatalf("HEAD: %v", err)
	}
	if head.ServerSideEncryption == "" {
		t.Errorf("HEAD без x-amz-server-side-encryption")
	}
}

// ETag считается по открытым байтам: иначе клиент перестанет сверять загрузки,
// и молча — сумма шифротекста тоже сходится сама с собой.
func TestETagIsOverPlaintext(t *testing.T) {
	be, m, land := newBackend(t)
	mustBucket(t, be)
	body := []byte("проверка: ETag считается по открытым байтам")
	out, err := be.PutObject(testCtx(), s3response.PutObjectInput{
		Bucket: ptr("b"), Key: ptr("k"),
		Body: bytes.NewReader(body), ContentLength: ptr(int64(len(body))),
	})
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}

	sum := md5.Sum(body)
	want := hex.EncodeToString(sum[:])
	if got := strings.Trim(out.ETag, "\""); got != want {
		t.Errorf("ETag = %s, хотели MD5 открытых байтов %s", got, want)
	}

	raw, err := os.ReadFile(land.Path(currentObject(t, m, "b", "k").ID))
	if err != nil {
		t.Fatalf("файл спула: %v", err)
	}
	if bytes.Contains(raw, body) {
		t.Errorf("в файле спула нашёлся открытый текст")
	}
	if int64(len(raw)) != encrypt.CipherSize(int64(len(body))) {
		t.Errorf("размер файла %d, формула обещала %d", len(raw), encrypt.CipherSize(int64(len(body))))
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
