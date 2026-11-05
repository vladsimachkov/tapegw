package encrypt_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/encrypt"
)

func testKey() []byte {
	k := make([]byte, encrypt.KeySize)
	for i := range k {
		k[i] = byte(i * 7)
	}
	return k
}

func testDoc(key string) *encrypt.ManifestDoc {
	return &encrypt.ManifestDoc{
		Bucket: "photos",
		Key:    key,
		ETag:   "d41d8cd98f00b204e9800998ecf8427e",
		Size:   4096,
		Parts: []encrypt.ManifestPart{
			{Number: 1, Plain: 4096, Off: 0, Salt: bytes.Repeat([]byte{0xA1}, encrypt.SaltSize)},
		},
	}
}

func TestManifestRoundTrip(t *testing.T) {
	key := testKey()
	version := uuid.New()
	doc := testDoc("a/b/c.txt")
	doc.ContentType = "text/plain"
	doc.Meta = map[string]string{"x-amz-meta-автор": "ваня"}
	doc.Lock = &encrypt.ManifestLock{Mode: "GOVERNANCE", RetainUntil: "2027-01-01T00:00:00Z", LegalHold: true}

	ct, foot, err := encrypt.SealManifest(nil, key, version, doc)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if len(foot) != encrypt.FooterSize {
		t.Fatalf("футер %d байт, нужно %d", len(foot), encrypt.FooterSize)
	}

	got, err := encrypt.OpenManifest(nil, key, version, ct, foot)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if got.Bucket != doc.Bucket || got.Key != doc.Key || got.ETag != doc.ETag || got.Size != doc.Size {
		t.Fatalf("метаданные разъехались: %+v", got)
	}
	if got.Meta["x-amz-meta-автор"] != "ваня" {
		t.Fatalf("юникод в метаданных испорчен: %v", got.Meta)
	}
	if got.Lock == nil || got.Lock.Mode != "GOVERNANCE" || !got.Lock.LegalHold {
		t.Fatalf("блокировка потеряна: %+v", got.Lock)
	}
	if len(got.Parts) != 1 || !bytes.Equal(got.Parts[0].Salt, doc.Parts[0].Salt) {
		t.Fatalf("части разъехались: %+v", got.Parts)
	}
}

// TestManifestSaltChangesPerAttempt — каждая попытка шифрования получает свою
// соль, иначе повторный Complete дал бы два манифеста под одним DEK с одним
// nonce.
func TestManifestSaltChangesPerAttempt(t *testing.T) {
	key := testKey()
	version := uuid.New()

	_, f1, err := encrypt.SealManifest(nil, key, version, testDoc("a"))
	if err != nil {
		t.Fatal(err)
	}
	_, f2, err := encrypt.SealManifest(nil, key, version, testDoc("a"))
	if err != nil {
		t.Fatal(err)
	}
	p1, err := encrypt.ParseFooter(f1)
	if err != nil {
		t.Fatal(err)
	}
	p2, err := encrypt.ParseFooter(f2)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(p1.Salt, p2.Salt) {
		t.Fatal("соль одинакова на две попытки: nonce переиспользуется при том же DEK")
	}
}

// TestManifestNoNonceReuse — главная проверка формата. Два разных манифеста под
// одним DEK не должны давать шифротексты, чей XOR равен XOR открытых текстов:
// именно так выглядит переиспользование nonce в GCM, и из него восстанавливается
// ключ GHASH.
//
// Открытые тексты обязаны быть одной длины, иначе XOR не определён.
func TestManifestNoNonceReuse(t *testing.T) {
	key := testKey()
	version := uuid.New()

	c1, _, err := encrypt.SealManifest(nil, key, version, testDoc("key-aaaa"))
	if err != nil {
		t.Fatal(err)
	}
	c2, _, err := encrypt.SealManifest(nil, key, version, testDoc("key-bbbb"))
	if err != nil {
		t.Fatal(err)
	}
	if len(c1) != len(c2) {
		t.Fatalf("разные длины, тест теряет смысл: %d и %d", len(c1), len(c2))
	}

	// Открытые тексты получаются повторной сериализацией той же структуры:
	// encoding/json кодирует поля в порядке объявления, а ключи карт — по
	// сортировке, поэтому байты совпадают с теми, что шифрует SealManifest.
	plainA, err := json.Marshal(testDoc("key-aaaa"))
	if err != nil {
		t.Fatal(err)
	}
	plainB, err := json.Marshal(testDoc("key-bbbb"))
	if err != nil {
		t.Fatal(err)
	}
	if len(plainA) != len(plainB) {
		t.Fatalf("открытые тексты разной длины: %d и %d", len(plainA), len(plainB))
	}

	xorCT := make([]byte, len(c1))
	xorPT := make([]byte, len(plainA))
	for i := range xorPT {
		xorCT[i] = c1[i] ^ c2[i]
		xorPT[i] = plainA[i] ^ plainB[i]
	}
	// Шифротекст длиннее открытого текста на тег; сравниваем только открытую
	// часть, иначе тест падал бы по длине, а не по существу.
	if bytes.Equal(xorCT[:len(xorPT)], xorPT) {
		t.Fatal("XOR шифротекстов равен XOR открытых текстов: nonce переиспользуется при одном DEK")
	}
}

// TestManifestFooterTamperDetected — футер входит в AAD целиком. Проверяется на
// reserved-байтах: это единственное поле футера, которое ParseFooter НЕ
// проверяет, так что только AAD и может его защитить.
//
// magic и длины проверяются ParseFooter независимо, а формат — и там и там
// (см. TestManifestFormatByteRejected).
func TestManifestFooterTamperDetected(t *testing.T) {
	key := testKey()
	version := uuid.New()
	ct, foot, err := encrypt.SealManifest(nil, key, version, testDoc("a"))
	if err != nil {
		t.Fatal(err)
	}
	// reserved занимает последние 11 байт футера.
	for off := encrypt.FooterSize - 11; off < encrypt.FooterSize; off++ {
		bad := append([]byte(nil), foot...)
		bad[off] ^= 0x01
		_, err := encrypt.OpenManifest(nil, key, version, ct, bad)
		if !errors.Is(err, encrypt.ErrManifestAuth) {
			t.Fatalf("правка reserved-байта %d не поймана как ошибка аутентичности: %v", off, err)
		}
	}
}

// TestManifestFormatByteRejected — подмена байта формата отвергается. Важно,
// что отказывает ParseFooter ЕЩЁ ДО проверки тега: если бы проверка формата
// была единственной, мутация «формат не входит в AAD» осталась бы невидимой.
// Поэтому формат защищён дважды, и тест фиксирует первую линию.
func TestManifestFormatByteRejected(t *testing.T) {
	key := testKey()
	version := uuid.New()
	ct, foot, err := encrypt.SealManifest(nil, key, version, testDoc("a"))
	if err != nil {
		t.Fatal(err)
	}
	bad := append([]byte(nil), foot...)
	bad[4] = 99
	if _, err := encrypt.OpenManifest(nil, key, version, ct, bad); !errors.Is(err, encrypt.ErrBadFooter) {
		t.Fatalf("неизвестный формат не отвергнут: %v", err)
	}
}

// TestManifestWrongKey — чужой DEK не раскрывает содержимое.
func TestManifestWrongKey(t *testing.T) {
	version := uuid.New()
	ct, foot, err := encrypt.SealManifest(nil, testKey(), version, testDoc("a"))
	if err != nil {
		t.Fatal(err)
	}
	other := testKey()
	other[0] ^= 0xFF
	if _, err := encrypt.OpenManifest(nil, other, version, ct, foot); !errors.Is(err, encrypt.ErrManifestAuth) {
		t.Fatalf("чужой ключ не отвергнут: %v", err)
	}
}

// TestManifestTrailerAndFile — манифест достаётся из файла по его размеру: так
// его будет читать rebuild-meta, не имея ничего, кроме файла и ключа.
func TestManifestTrailerAndFile(t *testing.T) {
	dir := t.TempDir()
	name := filepath.Join(dir, "obj")
	body := bytes.Repeat([]byte("данные"), 100)

	key := testKey()
	version := uuid.New()
	trailer, err := encrypt.ManifestTrailer(nil, key, version, testDoc("restore/me.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if len(trailer) <= encrypt.FooterSize {
		t.Fatalf("хвост %d байт — не в него ли только футер?", len(trailer))
	}
	if err := os.WriteFile(name, append(append([]byte(nil), body...), trailer...), 0o600); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	doc, err := encrypt.OpenManifestAt(f, st.Size(), key, version)
	if err != nil {
		t.Fatalf("манифест из файла не прочитан: %v", err)
	}
	if doc.Key != "restore/me.bin" {
		t.Fatalf("прочитан не тот объект: %q", doc.Key)
	}

	// Обрезанный файл не должен «читаться».
	if _, err := encrypt.OpenManifestAt(bytes.NewReader(body), int64(len(body)), key, version); err == nil {
		t.Fatal("манифест прочитался из файла без хвоста")
	}
}

// TestManifestEmptyObject — у пустого объекта манифест есть: таблица частей у
// него одна нулевая часть, и это не особый случай в формате.
func TestManifestEmptyObject(t *testing.T) {
	key := testKey()
	version := uuid.New()
	doc := &encrypt.ManifestDoc{Bucket: "b", Key: "k", Size: 0,
		Parts: []encrypt.ManifestPart{{Number: 1, Plain: 0, Off: 0, Salt: bytes.Repeat([]byte{9}, encrypt.SaltSize)}}}
	trailer, err := encrypt.ManifestTrailer(nil, key, version, doc)
	if err != nil {
		t.Fatal(err)
	}
	r := bytes.NewReader(trailer)
	got, err := encrypt.OpenManifestAt(r, int64(len(trailer)), key, version)
	if err != nil {
		t.Fatal(err)
	}
	if got.Size != 0 || len(got.Parts) != 1 {
		t.Fatalf("пустой объект разобран неверно: %+v", got)
	}
}
