package encrypt

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"testing"

	"github.com/google/uuid"
)

func mustKey(t *testing.T) []byte {
	t.Helper()
	k := make([]byte, KeySize)
	if _, err := rand.Read(k); err != nil {
		t.Fatalf("случайный ключ: %v", err)
	}
	return k
}

// plaintext — детерминированные байты, чтобы сбой воспроизводился без случайности.
func plaintext(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7 + i/251)
	}
	return b
}

// writeRead — прогнать данные через Writer и прочитать обратно целиком.
func writeRead(t *testing.T, data []byte, version uuid.UUID, part uint32) ([]byte, []byte) {
	t.Helper()
	key := mustKey(t)
	var ct bytes.Buffer
	w, err := NewWriter(&ct, key, version, part)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if w.Wrote() != int64(len(data)) {
		t.Fatalf("Wrote = %d, хотели %d", w.Wrote(), len(data))
	}
	if int64(ct.Len()) != CipherSize(int64(len(data))) {
		t.Fatalf("размер шифротекста %d, формула обещала %d", ct.Len(), CipherSize(int64(len(data))))
	}

	r, err := NewReader(bytes.NewReader(ct.Bytes()), key, version, part, int64(len(data)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	got, err := io.ReadAll(NewSeqReader(r))
	if err != nil {
		t.Fatalf("чтение: %v", err)
	}
	return data, got
}

func TestWriterReaderRoundTrip(t *testing.T) {
	// Размеры подобраны вокруг границы сегмента: ровно сегмент, на байт меньше
	// и на байт больше, и то же через один сегмент. Именно здесь чаще всего
	// ломается арифметика смещений.
	for _, n := range []int{0, 1, 4096, SegmentSize - 1, SegmentSize, SegmentSize + 1,
		2*SegmentSize + 7, 5*SegmentSize + 123} {
		want, got := writeRead(t, plaintext(n), uuid.New(), 0)
		if !bytes.Equal(want, got) {
			t.Errorf("размер %d: прочитано %d байт, ожидалось %d", n, len(got), len(want))
		}
	}
}

func TestCipherSizeInverse(t *testing.T) {
	for _, n := range []int64{0, 1, 15, SegmentSize - 1, SegmentSize, SegmentSize + 1,
		3*SegmentSize + 999, 200 * 1024 * 1024 * 1024} {
		ct := CipherSize(n)
		if back := PlainSize(ct); back != n {
			t.Errorf("plain=%d → cipher=%d → plain=%d", n, ct, back)
		}
	}
}

// TestReaderRanges — главный тест на Range: каждый диапазон обязан сойтись
// байт в байт, включая границы сегментов.
func TestReaderRanges(t *testing.T) {
	const n = 3*SegmentSize + 1000
	data := plaintext(n)
	version := uuid.New()
	key := mustKey(t)

	var ct bytes.Buffer
	w, err := NewWriter(&ct, key, version, 0)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := NewReader(bytes.NewReader(ct.Bytes()), key, version, 0, int64(n))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	cases := []struct{ off, len int }{
		{0, n},
		{0, 1},
		{n - 1, 1},
		{SegmentSize - 1, 1},               // последний байт первого сегмента
		{SegmentSize, 1},                   // первый байт второго
		{SegmentSize - 1, 2},               // ровно через границу
		{SegmentSize - 10, 20},             // с запасом через границу
		{SegmentSize - 1, 2 * SegmentSize}, // через два сегмента
		{3 * SegmentSize, 1000},            // остаток в конце
		{3*SegmentSize + 999, 1},           // самый последний байт
		{SegmentSize + 5, 0},               // пустой диапазон
		{n, 0},                             // чтение с конца
	}
	for _, c := range cases {
		p := make([]byte, c.len)
		got, err := r.ReadAt(p, int64(c.off))
		if err != nil && !errors.Is(err, io.EOF) {
			t.Errorf("off=%d len=%d: %v", c.off, c.len, err)
			continue
		}
		want := data[c.off:]
		if int64(len(want)) > int64(c.len) {
			want = want[:c.len]
		}
		if got != len(want) {
			t.Errorf("off=%d len=%d: прочитано %d, хотели %d", c.off, c.len, got, len(want))
			continue
		}
		if !bytes.Equal(p[:got], want) {
			t.Errorf("off=%d len=%d: байты не совпали", c.off, c.len)
		}
	}
}

// TestCorruptionDetected — порча одного байта шифротекста обязана дать ошибку,
// а не мусор. Проверяем середину данных, тег, первый и последний сегменты.
func TestCorruptionDetected(t *testing.T) {
	const n = 2*SegmentSize + 500
	data := plaintext(n)
	version := uuid.New()
	key := mustKey(t)

	seal := func() []byte {
		var ct bytes.Buffer
		w, err := NewWriter(&ct, key, version, 0)
		if err != nil {
			t.Fatalf("NewWriter: %v", err)
		}
		if _, err := w.Write(data); err != nil {
			t.Fatalf("Write: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		return ct.Bytes()
	}

	cases := []struct {
		name string
		off  func(ctLen int) int
	}{
		{"первый байт", func(int) int { return 0 }},
		{"середина сегмента", func(int) int { return 1000 }},
		{"тег первого сегмента", func(int) int { return SegmentSize + 3 }},
		{"середина второго сегмента", func(int) int { return CipherSegmentSize + 40 }},
		{"последний байт", func(l int) int { return l - 1 }},
	}
	for _, c := range cases {
		ct := seal()
		pos := c.off(len(ct))
		ct[pos] ^= 0x01

		r, err := NewReader(bytes.NewReader(ct), key, version, 0, int64(n))
		if err != nil {
			t.Fatalf("%s: NewReader: %v", c.name, err)
		}
		_, err = io.ReadAll(NewSeqReader(r))
		if err == nil {
			t.Errorf("%s: порча не обнаружена — чтение вернуло данные", c.name)
			continue
		}
		if !errors.Is(err, ErrCorruptedSegment) {
			t.Errorf("%s: ошибка %v, хотели ErrCorruptedSegment", c.name, err)
		}
	}
}

func TestShortCiphertext(t *testing.T) {
	// Файл короче, чем обещает размер: обрыв записи. Ошибка, а не тихое чтение
	// того, что успело записаться.
	const n = 2 * SegmentSize
	data := plaintext(n)
	version := uuid.New()
	key := mustKey(t)

	var ct bytes.Buffer
	w, _ := NewWriter(&ct, key, version, 0)
	if _, err := w.Write(data); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	truncated := ct.Bytes()[:ct.Len()-CipherSegmentSize+7]

	r, err := NewReader(bytes.NewReader(truncated), key, version, 0, int64(n))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if _, err := io.ReadAll(NewSeqReader(r)); !errors.Is(err, ErrShortRead) {
		t.Errorf("ошибка %v, хотели ErrShortRead", err)
	}
}

// TestNonceUniquePerVersion — одинаковый открытый текст в двух версиях даёт
// разный шифротекст. Если бы nonce не зависел от версии, два объекта совпали бы
// байт в байт, и это было бы видно даже без ключа.
func TestNonceUniquePerVersion(t *testing.T) {
	data := plaintext(SegmentSize)
	key := mustKey(t)

	seal := func(v uuid.UUID, part uint32) []byte {
		var ct bytes.Buffer
		w, _ := NewWriter(&ct, key, v, part)
		if _, err := w.Write(data); err != nil {
			t.Fatalf("Write: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		return ct.Bytes()
	}

	a := seal(uuid.New(), 0)
	b := seal(uuid.New(), 0)
	if bytes.Equal(a, b) {
		t.Errorf("шифротексты двух версий совпали — nonce не зависит от версии")
	}

	// Номер части тоже обязан влиять: части multipart шифруются тем же DEK.
	if bytes.Equal(seal(uuid.MustParse("00000000-0000-0000-0000-000000000001"), 1),
		seal(uuid.MustParse("00000000-0000-0000-0000-000000000001"), 2)) {
		t.Errorf("шифротексты частей 1 и 2 одной версии совпали — nonce не зависит от номера части")
	}
}

// TestAADBindsSegment — сегмент из другой версии не принимается, даже когда DEK
// тот же. Имитация: берём шифротекст версии A и пытаемся прочитать как версию B.
func TestAADBindsSegment(t *testing.T) {
	data := plaintext(100)
	key := mustKey(t)
	vA, vB := uuid.New(), uuid.New()

	var ct bytes.Buffer
	w, _ := NewWriter(&ct, key, vA, 0)
	if _, err := w.Write(data); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := NewReader(bytes.NewReader(ct.Bytes()), key, vB, 0, int64(len(data)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if _, err := io.ReadAll(NewSeqReader(r)); !errors.Is(err, ErrCorruptedSegment) {
		t.Errorf("чтение чужой версии: %v, хотели ErrCorruptedSegment", err)
	}
}

func TestWriterRejectsBadKey(t *testing.T) {
	if _, err := NewWriter(io.Discard, make([]byte, 16), uuid.New(), 0); err == nil {
		t.Errorf("короткий ключ принят")
	}
}

func TestWriterWriteAfterClose(t *testing.T) {
	w, err := NewWriter(io.Discard, mustKey(t), uuid.New(), 0)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := w.Write([]byte("x")); err == nil {
		t.Errorf("запись после Close принята")
	}
}
