package encrypt

import (
	"fmt"
	"io"

	"github.com/google/uuid"
)

// Reader — чтение открытых данных из файла шифротекста.
//
// Реализует io.ReaderAt: именно так читает спул при Range-запросе, и именно
// поэтому карта «открытый диапазон → шифротекстовый» обязана быть точной.
// Ошибка на единицу даёт либо чужие байты в ответе, либо выход за конец файла.
//
// Открытый диапазон [off, off+n) покрывает сегменты [off/SegmentSize,
// (off+n-1)/SegmentSize]. Внутри сегмента смещение открытого текста и смещение
// шифротекста различаются на 16 байт тега за каждый предшествующий сегмент —
// поэтому считать надо от номера сегмента, а не пропорцией от размера.
type Reader struct {
	at      io.ReaderAt
	key     []byte
	version uuid.UUID
	part    uint32
	plain   int64

	segBuf []byte // буфер шифротекста одного сегмента, переиспользуется
	tmpBuf []byte // буфер открытого текста одного сегмента
}

// NewReader — читатель тела версии размером plain байт.
func NewReader(at io.ReaderAt, key []byte, version uuid.UUID, part uint32, plain int64) (*Reader, error) {
	if len(key) != KeySize {
		return nil, fmt.Errorf("encrypt: ключ %d байт, нужно %d", len(key), KeySize)
	}
	if plain < 0 {
		return nil, fmt.Errorf("encrypt: отрицательный размер %d", plain)
	}
	return &Reader{
		at:      at,
		key:     key,
		version: version,
		part:    part,
		plain:   plain,
		segBuf:  make([]byte, 0, CipherSegmentSize),
		tmpBuf:  make([]byte, 0, SegmentSize),
	}, nil
}

// Size — размер открытых данных.
func (r *Reader) Size() int64 { return r.plain }

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
		pos := off + done // открытое смещение текущего байта
		seg := uint64(pos / SegmentSize)
		inSeg := int(pos % SegmentSize) // смещение внутри сегмента

		plainSeg := int64(SegmentSize)
		if rem := r.plain - int64(seg)*SegmentSize; rem < plainSeg {
			plainSeg = rem // последний сегмент — остаток
		}

		ct, err := r.readSegment(seg, plainSeg)
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

// readSegment — расшифрованный сегмент целиком. plainSeg — размер открытого
// текста этого сегмента (для последнего — остаток).
func (r *Reader) readSegment(seg uint64, plainSeg int64) ([]byte, error) {
	ctLen := plainSeg + TagSize
	if int64(cap(r.segBuf)) < ctLen {
		r.segBuf = make([]byte, ctLen)
	}
	ct := r.segBuf[:ctLen]
	n, err := r.at.ReadAt(ct, int64(seg)*CipherSegmentSize)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("encrypt: чтение шифротекста: %w", err)
	}
	if int64(n) != ctLen {
		// Файл короче, чем требует заявленный размер: обрыв записи или
		// рассинхрон БД с диском. Молча читать то, что есть, нельзя.
		return nil, fmt.Errorf("%w: сегмент %d, прочитано %d из %d", ErrShortRead, seg, n, ctLen)
	}

	need := int64(SegmentSize)
	if int64(cap(r.tmpBuf)) < need {
		r.tmpBuf = make([]byte, need)
	}
	out := r.tmpBuf[:0]
	return OpenSegment(out, r.key, r.version, r.part, seg, ct)
}

// Read — последовательное чтение с нуля. Нужен там, где Range нет: полный GET,
// пересылка в restore. Реализован через ReadAt, чтобы не держать вторую
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
