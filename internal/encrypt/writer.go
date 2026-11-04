package encrypt

import (
	"fmt"
	"io"

	"github.com/google/uuid"
)

// Writer — поток шифрования: открытые данные на входе, шифротекст в dst.
//
// Буферизуется ровно один сегмент: больше не нужно, потому что сегменты
// независимы, а выравнивание по границе сегмента делает запись предсказуемой.
// Close обязателен — последний неполный сегмент уходит из буфера именно там, и
// без Close файл окажется короче, чем обещает записанный размер.
type Writer struct {
	dst     io.Writer
	key     []byte
	version uuid.UUID
	part    uint32

	buf    []byte // недобитый хвост текущего сегмента
	seg    uint64 // номер следующего сегмента
	wrote  int64  // байт открытого текста, уже ушедших в dst
	tmp    []byte // буфер под шифротекст одного сегмента
	closed bool
}

// NewWriter — поток для записи тела версии. part = 0 для обычного PUT, номер
// части для multipart (ADR-0014: номер части входит в nonce).
//
// Ключ копируется внутрь, поэтому вызывающий волен обнулить свой массив сразу
// после записи: обёрнутый DEK он уже получил, а писателю ключ больше не нужен.
func NewWriter(dst io.Writer, key []byte, version uuid.UUID, part uint32) (*Writer, error) {
	if len(key) != KeySize {
		return nil, fmt.Errorf("encrypt: ключ %d байт, нужно %d", len(key), KeySize)
	}
	// Ключ копируется: Writer переживает вызов NewWriter, и обнуление ключа
	// вызывающим в середине записи разрушило бы шифрование так же тихо, как и
	// в Reader.
	own := make([]byte, KeySize)
	copy(own, key)
	return &Writer{
		dst:     dst,
		key:     own,
		version: version,
		part:    part,
		buf:     make([]byte, 0, SegmentSize),
		tmp:     make([]byte, 0, CipherSegmentSize),
	}, nil
}

// Wrote — сколько байт открытого текста принято. Это и есть размер объекта для
// клиента, для ETag и для колонки objects.size.
func (w *Writer) Wrote() int64 { return w.wrote }

func (w *Writer) Write(p []byte) (int, error) {
	if w.closed {
		return 0, fmt.Errorf("encrypt: запись после Close")
	}
	total := 0
	for len(p) > 0 {
		n := SegmentSize - len(w.buf)
		if n > len(p) {
			n = len(p)
		}
		w.buf = append(w.buf, p[:n]...)
		p = p[n:]
		total += n
		if len(w.buf) == SegmentSize {
			if err := w.flush(); err != nil {
				return total, err
			}
		}
	}
	return total, nil
}

// flush — шифрует и пишет накопленный сегмент.
func (w *Writer) flush() error {
	if len(w.buf) == 0 {
		return nil
	}
	ct, err := SealSegment(w.tmp, w.key, w.version, w.part, w.seg, w.buf)
	if err != nil {
		return err
	}
	if _, err := w.dst.Write(ct); err != nil {
		return fmt.Errorf("encrypt: запись шифротекста: %w", err)
	}
	w.wrote += int64(len(w.buf))
	w.seg++
	// Байты открытого текста стираем: буфер переиспользуется, а в нём только
	// что лежали открытые данные клиента.
	Zero(w.buf)
	w.buf = w.buf[:0]
	return nil
}

// Close — дописывает последний сегмент. Повторный Close безвреден.
func (w *Writer) Close() error {
	if w.closed {
		return nil
	}
	err := w.flush()
	w.closed = true
	Zero(w.key)
	Zero(w.tmp)
	return err
}
