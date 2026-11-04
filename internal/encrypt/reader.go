package encrypt

import (
	"fmt"
	"io"

	"github.com/google/uuid"
)

// Part — одна часть объекта в файле шифротекста.
//
// Мultipart устроен так, что итоговый файл — конкатенация шифротекстов частей,
// а каждая часть шифровалась своим номером в nonce и AAD. Поэтому для чтения
// мало знать общий размер: нужно, где начинается шифротекст нужной части и какого
// она размера. Отсюда три поля:
//
//   - Number — входит в nonce и AAD;
//   - PlainSize — входит в AAD (усечение части по границе сегмента обязано быть
//     видно) и нужен, чтобы понять, сколько сегментов в части;
//   - CipherOffset — смещение шифротекста части в итоговом файле.
//
// Обычный PUT — это одна часть {0, размер, 0}. Второго пути чтения нет
// намеренно: то, что работает для multipart, работает и для объекта из одной
// части, а не «почти то же самое, но отдельной веткой».
type Part struct {
	Number       uint32
	PlainSize    int64
	CipherOffset int64
	// Salt — соль этой попытки загрузки части. Входит в nonce каждого её
	// сегмента: без неё переотправка части с тем же номером дала бы повтор
	// nonce при том же DEK.
	Salt []byte
}

// Reader — чтение открытых данных из файла шифротекста.
//
// Реализует io.ReaderAt: именно так читает спул при Range-запросе, и именно
// поэтому карта «открытый диапазон → шифротекстовый» обязана быть точной.
// Ошибка на единицу даёт либо чужие байты в ответе, либо выход за конец файла.
type Reader struct {
	at      io.ReaderAt
	key     []byte
	version uuid.UUID
	parts   []Part
	// ends[i] — открытое смещение, на котором заканчивается часть i. Массив
	// длиннее parts на единицу: ends[0] = 0.
	ends   []int64
	plain  int64
	segBuf []byte // буфер шифротекста одного сегмента, переиспользуется
	tmpBuf []byte // буфер открытого текста одного сегмента
}

// NewReader — читатель тела версии из одной части (обычный PUT).
func NewReader(at io.ReaderAt, key []byte, version uuid.UUID, part uint32, salt []byte, plain int64) (*Reader, error) {
	return NewPartsReader(at, key, version, []Part{{Number: part, PlainSize: plain, Salt: salt}}, plain)
}

// NewPartsReader — читатель multipart-версии.
//
// Части перечисляются в порядке следования в файле; CipherOffset подставляется
// по накопленным размерам шифротекста, поэтому вызывать его от «уже посчитанных
// в базе» смещений не требуется — но если смещения пришли из базы, они обязаны
// совпасть с этой формулой, иначе чтение даст мусор. Проверка — здесь же.
func NewPartsReader(at io.ReaderAt, key []byte, version uuid.UUID, parts []Part, plain int64) (*Reader, error) {
	if len(key) != KeySize {
		return nil, fmt.Errorf("encrypt: ключ %d байт, нужно %d", len(key), KeySize)
	}
	if plain < 0 {
		return nil, fmt.Errorf("encrypt: отрицательный размер %d", plain)
	}
	if len(parts) == 0 {
		if plain != 0 {
			return nil, fmt.Errorf("encrypt: размер %d без частей", plain)
		}
		parts = []Part{{}}
	}

	// Копия, а не исходный срез: во-первых, вызывающий не обязан ждать, что мы
	// допишем его данные; во-вторых, запись в переменную цикла ничего бы не
	// записала в parts — именно так первая версия и ломалась, молча читая все
	// части кроме первой с нулевого смещения.
	own := make([]Part, len(parts))
	copy(own, parts)

	ends := make([]int64, 0, len(own)+1)
	var acc, cipherAcc int64
	for i := range own {
		p := &own[i]
		if p.PlainSize < 0 {
			return nil, fmt.Errorf("encrypt: часть %d с отрицательным размером", p.Number)
		}
		if len(p.Salt) != SaltSize {
			return nil, fmt.Errorf("encrypt: часть %d: соль %d байт, нужно %d",
				p.Number, len(p.Salt), SaltSize)
		}
		if p.PlainSize == 0 && len(own) > 1 {
			// Пустая часть в multipart ломает поиск части по смещению: две
			// части начинаются в одном месте. S3 минимальный размер части
			// отсекает, но полагаться на вызывающего не стоит.
			return nil, fmt.Errorf("encrypt: пустая часть %d в multipart", p.Number)
		}
		if p.CipherOffset == 0 && i > 0 {
			p.CipherOffset = cipherAcc
		}
		if p.CipherOffset != cipherAcc {
			return nil, fmt.Errorf("encrypt: часть %d: смещение шифротекста %d, формула даёт %d",
				p.Number, p.CipherOffset, cipherAcc)
		}
		acc += p.PlainSize
		cipherAcc += CipherSize(p.PlainSize)
		ends = append(ends, acc)
	}
	if acc != plain {
		return nil, fmt.Errorf("encrypt: части дают %d байт, объявлено %d", acc, plain)
	}

	// Ключ копируется: Reader ленивый, он читает ключ в момент ReadAt, а
	// вызывающий, обнуливший свой массив сразу после конструктора, оставил бы
	// читатель с нулевым ключом.
	keyCopy := make([]byte, KeySize)
	copy(keyCopy, key)
	return &Reader{
		at:      at,
		key:     keyCopy,
		version: version,
		parts:   own,
		ends:    ends,
		plain:   plain,
		segBuf:  make([]byte, 0, CipherSegmentSize),
		tmpBuf:  make([]byte, 0, SegmentSize),
	}, nil
}

// Close — стирает ключ и буферы.
func (r *Reader) Close() error {
	Zero(r.key)
	Zero(r.tmpBuf)
	return nil
}

// Size — размер открытых данных.
func (r *Reader) Size() int64 { return r.plain }

// partAt — индекс части, которой принадлежит открытое смещение.
func (r *Reader) partAt(pos int64) int {
	lo, hi := 0, len(r.parts)-1
	for lo < hi {
		mid := (lo + hi) / 2
		if pos < r.ends[mid] {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}

// ReadAt читает len(p) байт, начиная с открытого смещения off. Возвращает
// io.EOF на естественном конце данных — так же, как io.ReaderAt для файла.
func (r *Reader) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("encrypt: отрицательное смещение %d", off)
	}
	if off >= r.plain {
		return 0, io.EOF
	}
	want := int64(len(p))
	if want == 0 {
		return 0, nil
	}
	if off+want > r.plain {
		want = r.plain - off
	}

	done := int64(0)
	for done < want {
		pos := off + done
		pi := r.partAt(pos)
		part := r.parts[pi]
		partStart := r.ends[pi] - part.PlainSize
		inPart := pos - partStart

		seg := uint64(inPart / SegmentSize)
		inSeg := int(inPart % SegmentSize)

		plainSeg := int64(SegmentSize)
		if rem := part.PlainSize - int64(seg)*SegmentSize; rem < plainSeg {
			plainSeg = rem
		}

		ct, err := r.readSegment(part, seg, plainSeg)
		if err != nil {
			return int(done), err
		}

		n := copy(p[done:], ct[inSeg:])
		if int64(n) > want-done {
			n = int(want - done)
		}
		done += int64(n)
	}
	if off+done < r.plain {
		return int(done), nil
	}
	return int(done), io.EOF
}

// readSegment — расшифрованный сегмент части целиком. plainSeg — размер
// открытого текста этого сегмента (для последнего в части — остаток).
func (r *Reader) readSegment(part Part, seg uint64, plainSeg int64) ([]byte, error) {
	ctLen := plainSeg + TagSize
	if int64(cap(r.segBuf)) < ctLen {
		r.segBuf = make([]byte, ctLen)
	}
	ct := r.segBuf[:ctLen]
	off := part.CipherOffset + int64(seg)*CipherSegmentSize
	n, err := r.at.ReadAt(ct, off)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("encrypt: чтение шифротекста: %w", err)
	}
	if int64(n) != ctLen {
		// Файл короче, чем требует заявленный размер: обрыв записи или
		// рассинхрон БД с диском. Молча читать то, что есть, нельзя.
		return nil, fmt.Errorf("%w: часть %d сегмент %d, прочитано %d из %d",
			ErrShortRead, part.Number, seg, n, ctLen)
	}

	need := int64(SegmentSize)
	if int64(cap(r.tmpBuf)) < need {
		r.tmpBuf = make([]byte, need)
	}
	out := r.tmpBuf[:0]
	return OpenSegment(out, r.key, r.version, part.Number, part.Salt, seg, part.PlainSize, ct)
}

// SeqReader — последовательное чтение с нуля. Нужен там, где Range нет: полный
// GET, пересылка в restore. Реализован через ReadAt, чтобы не держать вторую
// реализацию карты сегментов.
type SeqReader struct {
	r   *Reader
	pos int64
}

func NewSeqReader(r *Reader) *SeqReader { return &SeqReader{r: r} }

func (s *SeqReader) Read(p []byte) (int, error) {
	n, err := s.r.ReadAt(p, s.pos)
	s.pos += int64(n)
	if err == io.EOF && n > 0 {
		// io.Reader не имеет права вернуть EOF вместе с данными: потребитель
		// обязан сделать ещё один вызов, и так устроены все читатели в Go.
		return n, nil
	}
	return n, err
}
