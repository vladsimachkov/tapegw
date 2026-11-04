package encrypt

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"testing"

	"github.com/google/uuid"
)

// testSalt — фиксированная соль для тестов формата. Настоящая генерируется
// случайно на каждую попытку загрузки; здесь важна длина и неизменность.
func testSalt() []byte {
	s := make([]byte, SaltSize)
	for i := range s {
		s[i] = byte(0xA0 + i)
	}
	return s
}

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
	w, err := NewWriter(&ct, key, version, part, testSalt(), int64(len(data)))
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

	r, err := NewReader(bytes.NewReader(ct.Bytes()), key, version, part, testSalt(), int64(len(data)))
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
	w, err := NewWriter(&ct, key, version, 0, testSalt(), int64(len(data)))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := NewReader(bytes.NewReader(ct.Bytes()), key, version, 0, testSalt(), int64(n))
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
		w, err := NewWriter(&ct, key, version, 0, testSalt(), int64(len(data)))
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

		r, err := NewReader(bytes.NewReader(ct), key, version, 0, testSalt(), int64(n))
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
	w, _ := NewWriter(&ct, key, version, 0, testSalt(), int64(len(data)))
	if _, err := w.Write(data); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	truncated := ct.Bytes()[:ct.Len()-CipherSegmentSize+7]

	r, err := NewReader(bytes.NewReader(truncated), key, version, 0, testSalt(), int64(n))
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
		w, _ := NewWriter(&ct, key, v, part, testSalt(), int64(len(data)))
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
	w, _ := NewWriter(&ct, key, vA, 0, testSalt(), int64(len(data)))
	if _, err := w.Write(data); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := NewReader(bytes.NewReader(ct.Bytes()), key, vB, 0, testSalt(), int64(len(data)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if _, err := io.ReadAll(NewSeqReader(r)); !errors.Is(err, ErrCorruptedSegment) {
		t.Errorf("чтение чужой версии: %v, хотели ErrCorruptedSegment", err)
	}
}

func TestWriterRejectsBadKey(t *testing.T) {
	if _, err := NewWriter(io.Discard, make([]byte, 16), uuid.New(), 0, testSalt(), 5); err == nil {
		t.Errorf("короткий ключ принят")
	}
}

func TestWriterWriteAfterClose(t *testing.T) {
	w, err := NewWriter(io.Discard, mustKey(t), uuid.New(), 0, testSalt(), 5)
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

// Размер открытого текста входит в AAD, поэтому метаданные не могут
// «договориться» с данными: объект, у которого уменьшили размер, перестаёт
// проходить проверку аутентичности вместо того, чтобы молча отдаться усечённым.
// Для rebuild-meta (ADR-0014) это единственная защита: там размер берётся с того
// же носителя, что и данные.
func TestDeclaredSizeIsBoundToSegments(t *testing.T) {
	const n = 2*SegmentSize + 500
	data := plaintext(n)
	version := uuid.New()
	key := mustKey(t)

	var ct bytes.Buffer
	// Отрицательный объявленный размер — ошибка конструктора, а не паника.
	if _, err := NewWriter(io.Discard, key, version, 0, testSalt(), -1); err == nil {
		t.Errorf("писатель принял отрицательный размер")
	}

	w, err := NewWriter(&ct, key, version, 0, testSalt(), int64(n))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Уменьшили РОВНО на целый сегмент вместе с хвостом: 2 сегмента вместо
	// трёх. Это и есть «обрезка по границе сегмента» — каждый оставшийся
	// сегмент целой длины и сам по себе подлинный, поэтому длина файла о
	// подмене не скажет ничего. Поймать это может только размер в AAD.
	//
	// Усечение в середину сегмента ловится и без размера (длина шифротекста не
	// совпадёт с тегом), и первый вариант теста давал именно этот случай —
	// мутация «AAD игнорирует размер» сквозь него проходила.
	short, err := NewReader(bytes.NewReader(ct.Bytes()), key, version, 0, testSalt(), 2*SegmentSize)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer short.Close()
	if _, err := io.ReadAll(NewSeqReader(short)); !errors.Is(err, ErrCorruptedSegment) {
		t.Errorf("уменьшенный размер: %v, хотели ErrCorruptedSegment", err)
	}

	// Целый объект читается как ни в чём не бывало.
	full, err := NewReader(bytes.NewReader(ct.Bytes()), key, version, 0, testSalt(), int64(n))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer full.Close()
	got, err := io.ReadAll(NewSeqReader(full))
	if err != nil {
		t.Errorf("полный размер: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("полный размер: байты не совпали")
	}
}

// sealParts — шифрует несколько частей в один буфер так же, как это делает
// multipart: части пишутся последовательно, каждая своим номером.
func sealParts(t *testing.T, key []byte, version uuid.UUID, sizes []int) ([]byte, [][]byte) {
	t.Helper()
	var all bytes.Buffer
	var plains [][]byte
	for i, n := range sizes {
		data := plaintext(n)
		plains = append(plains, data)
		w, err := NewWriter(&all, key, version, uint32(i+1), testSalt(), int64(n))
		if err != nil {
			t.Fatalf("часть %d: NewWriter: %v", i+1, err)
		}
		if _, err := w.Write(data); err != nil {
			t.Fatalf("часть %d: Write: %v", i+1, err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("часть %d: Close: %v", i+1, err)
		}
	}
	return all.Bytes(), plains
}

// Чтение multipart: части неровного размера, диапазоны через границы частей и
// через границы сегментов внутри части.
func TestPartsReaderRanges(t *testing.T) {
	sizes := []int{SegmentSize, 1000, 2*SegmentSize + 5, 7}
	key := mustKey(t)
	version := uuid.New()
	ct, plains := sealParts(t, key, version, sizes)

	var total int
	for _, n := range sizes {
		total += n
	}
	joined := append(append(append([]byte{}, plains[0]...), plains[1]...), plains[2]...)
	joined = append(joined, plains[3]...)

	parts := make([]Part, 0, len(sizes))
	for i, n := range sizes {
		parts = append(parts, Part{Number: uint32(i + 1), PlainSize: int64(n), Salt: testSalt()})
	}
	r, err := NewPartsReader(bytes.NewReader(ct), key, version, parts, int64(total))
	if err != nil {
		t.Fatalf("NewPartsReader: %v", err)
	}
	defer r.Close()

	// Границы частей: 65536, 66536, 132041, 132048.
	boundaries := []int{0, 1, SegmentSize - 1, SegmentSize, SegmentSize + 1,
		66536 - 1, 66536, 66536 + 1,
		132041 - 1, 132041, 132041 + 1,
		132048 - 1, 132047, total - 1}
	for _, off := range boundaries {
		for _, ln := range []int{1, 5, 70000} {
			if off+ln > total {
				ln = total - off
			}
			if ln <= 0 {
				continue
			}
			p := make([]byte, ln)
			got, err := r.ReadAt(p, int64(off))
			if err != nil && !errors.Is(err, io.EOF) {
				t.Errorf("off=%d len=%d: %v", off, ln, err)
				continue
			}
			if !bytes.Equal(p[:got], joined[off:off+got]) {
				t.Errorf("off=%d len=%d: байты не совпали", off, ln)
			}
		}
	}

	// Весь объект целиком.
	all, err := io.ReadAll(NewSeqReader(r))
	if err != nil {
		t.Fatalf("чтение целиком: %v", err)
	}
	if !bytes.Equal(all, joined) {
		t.Errorf("объект целиком не совпал: %d байт вместо %d", len(all), len(joined))
	}
}

// Размер части входит в AAD, поэтому уменьшение размера части в метаданных
// делает объект нечитаемым — а не отдаёт усечённые данные.
func TestPartsReaderRejectsWrongPartSize(t *testing.T) {
	sizes := []int{SegmentSize, 300}
	key := mustKey(t)
	version := uuid.New()
	ct, _ := sealParts(t, key, version, sizes)

	// Уменьшили вторую часть на 100 байт: суммарный размер тоже меньше, и
	// каждая оставшаяся часть целая. Ловит только привязка размера в AAD.
	parts := []Part{{Number: 1, PlainSize: SegmentSize, Salt: testSalt()},
		{Number: 2, PlainSize: 200, Salt: testSalt()}}
	r, err := NewPartsReader(bytes.NewReader(ct), key, version, parts, SegmentSize+200)
	if err != nil {
		t.Fatalf("NewPartsReader: %v", err)
	}
	defer r.Close()
	if _, err := io.ReadAll(NewSeqReader(r)); !errors.Is(err, ErrCorruptedSegment) {
		t.Errorf("уменьшенная часть: %v, хотели ErrCorruptedSegment", err)
	}
}

// Смещения шифротекста, пришедшие из базы, обязаны совпадать с формулой:
// рассинхрон object_parts и файла — это ошибка конструктора, а не мусор в ответе.
func TestPartsReaderRejectsWrongCipherOffset(t *testing.T) {
	key := mustKey(t)
	version := uuid.New()
	ct, _ := sealParts(t, key, version, []int{SegmentSize, 300})

	parts := []Part{
		{Number: 1, PlainSize: SegmentSize, CipherOffset: 0, Salt: testSalt()},
		{Number: 2, PlainSize: 300, CipherOffset: 999, Salt: testSalt()},
	}
	if _, err := NewPartsReader(bytes.NewReader(ct), key, version, parts, SegmentSize+300); err == nil {
		t.Errorf("несовпадение cipher_offset принято")
	}
}

// Повторная загрузка части не должна повторять nonce.
//
// S3 разрешает переотправить часть с тем же номером — retry после сетевой
// ошибки делают aws cli, rclone и SDK, — а DEK на multipart-загрузку один.
// Если nonce не зависит от соли, вторая попытка шифруется тем же ключом и теми
// же nonce, и тогда XOR шифротекстов равен XOR открытых текстов: данные
// утекают без ключа, а по двум сообщениям с одним nonce восстанавливается ключ
// аутентификации GHASH.
//
// Данные одинаковой длины — обязательное условие атаки, поэтому и тест берёт
// одинаковую длину: на разных длинах XOR-проверка была бы слабее.
func TestReencryptedPartDoesNotReuseNonce(t *testing.T) {
	const n = 3 * SegmentSize
	key := mustKey(t)
	version := uuid.New()
	const part = uint32(2)

	p1 := plaintext(n)
	p2 := make([]byte, n)
	for i := range p2 {
		p2[i] = p1[i] ^ byte(i%251)
	}

	seal := func(salt, plain []byte) []byte {
		var buf bytes.Buffer
		w, err := NewWriter(&buf, key, version, part, salt, int64(len(plain)))
		if err != nil {
			t.Fatalf("NewWriter: %v", err)
		}
		if _, err := w.Write(plain); err != nil {
			t.Fatalf("Write: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		return buf.Bytes()
	}

	salt1 := testSalt()
	salt2 := make([]byte, SaltSize)
	for i := range salt2 {
		salt2[i] = salt1[i] ^ 0xFF
	}

	c1 := seal(salt1, p1)
	c2 := seal(salt2, p2)

	// Прямой признак reuse: XOR шифротекстов равен XOR открытых текстов.
	xorCT := make([]byte, n)
	xorPT := make([]byte, n)
	for i := 0; i < n; i++ {
		xorCT[i] = c1[i] ^ c2[i]
		xorPT[i] = p1[i] ^ p2[i]
	}
	if bytes.Equal(xorCT[:SegmentSize], xorPT[:SegmentSize]) {
		t.Errorf("XOR шифротекстов равен XOR открытых текстов: nonce повторились")
	}
	if bytes.Equal(c1, c2) {
		t.Errorf("одинаковые шифротексты при разных данных")
	}

	// И каждая попытка читается только своей солью: соль — часть контекста,
	// а не декорация.
	r, err := NewReader(bytes.NewReader(c2), key, version, part, salt2, n)
	if err != nil {
		t.Fatalf("NewReader(salt2): %v", err)
	}
	got, err := io.ReadAll(NewSeqReader(r))
	r.Close()
	if err != nil {
		t.Fatalf("чтение второй попытки: %v", err)
	}
	if !bytes.Equal(got, p2) {
		t.Errorf("вторая попытка прочиталась неверно")
	}

	r2, err := NewReader(bytes.NewReader(c2), key, version, part, salt1, n)
	if err != nil {
		t.Fatalf("NewReader(salt1): %v", err)
	}
	defer r2.Close()
	if _, err := io.ReadAll(NewSeqReader(r2)); err == nil {
		t.Errorf("чужая соль принята: соль не влияет на nonce")
	}
}

// Писатель и читатель обязаны отказывать на соли неверного размера: молча
// дополнить нулями означало бы «уникальность nonce» только на словах.
func TestSaltSizeIsEnforced(t *testing.T) {
	key := mustKey(t)
	version := uuid.New()
	if _, err := NewWriter(io.Discard, key, version, 0, nil, 5); err == nil {
		t.Errorf("писатель принял соль нулевой длины")
	}
	if _, err := NewWriter(io.Discard, key, version, 0, make([]byte, SaltSize-1), 5); err == nil {
		t.Errorf("писатель принял короткую соль")
	}
	if _, err := NewReader(bytes.NewReader(nil), key, version, 0, make([]byte, SaltSize+1), 0); err == nil {
		t.Errorf("читатель принял длинную соль")
	}
}
