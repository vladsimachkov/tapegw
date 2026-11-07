// Package pack — формат пачки мелких объектов (ADR-0020, раздел 2).
//
// Пачка — один объект Phobos, внутри:
//
//	[ tar(pax): member_1 … member_N ][ индекс ][ футер ]
//
// Имя участника tar — UUID версии, содержимое — файл объекта целиком:
// шифротекст + зашифрованный манифест + футер (ADR-0019). Не только
// шифротекст: соль частей, по которой строится nonce каждого сегмента (#38),
// имя бакета и ключа, ETag и метаданные живут в манифесте, а в pax их нет —
// без манифеста участник с ленты не расшифровать. pax выбран ради свойства:
// поток читается по заголовкам слева направо без индекса, а заголовок несёт
// обёртку DEK — без неё запасной путь пустой.
//
// Пакет чистый, без cgo и Phobos (правило 2): migrator собирает пачку в файл на
// staging, а mover потом кладёт байты на ленту.
package pack

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
)

// FormatVersion — версия формата пачки. Дублируется в атрибуте
// tapegw.pack.format и в футере: читатель обязан отказать неизвестной версии,
// а не читать «похожее».
const FormatVersion = 1

var magicFooter = [4]byte{'T', 'P', 'K', 'F'}

const (
	// FooterSize — размер футера пачки.
	FooterSize = 64
	// 16 (version) + 5×8 (tenant, seq, plain_len, file_off, file_len)
	// + 2 (dek_len)
	indexRecHead = 16 + 5*8 + 2
	maxDEKLen    = math.MaxUint8
)

// Ошибки формата разные, потому что реакция разная: неизвестная версия — «нужен
// новый читатель», испорченный индекс — «восстанавливать по tar», несовпадение
// суммы — «на ленте не те байты, что записывали».
var (
	ErrBadFooter     = errors.New("pack: футер не похож на пачку")
	ErrBadFormat     = errors.New("pack: неизвестная версия формата пачки")
	ErrIndexChecksum = errors.New("pack: контрольная сумма индекса не совпала")
	ErrDEKTooLong    = errors.New("pack: обёртка DEK длиннее 255 байт")
	ErrClosed        = errors.New("pack: пачка уже закрыта")
	ErrShort         = errors.New("pack: данных меньше, чем обещает заголовок")
)

// Флаги участника индекса.
const (
	FlagDeleteMarker = 1 << 0
)

// Member — участник пачки при записи.
type Member struct {
	Version    [16]byte // UUID версии объекта
	Tenant     uint64
	VersionSeq uint64
	PlainLen   uint64
	KekVersion uint64
	WrappedDEK []byte
	Flags      uint8

	// FileLen — объявленная длина файла участника: шифротекст + зашифрованный
	// манифест + футер (ADR-0019). Она известна из БД (objects.file_bytes —
	// её пишет PUT в той же транзакции, что и строку объекта), и без неё tar не
	// может выставить размер заголовка, а значит поток нельзя читать по
	// заголовкам. Сборщик сверяет фактическую длину в обе стороны: короче —
	// манифест обрезан, длиннее — следующий участник начался бы с середины
	// данных.
	FileLen uint64

	// File — файл участника целиком. Читается потоком, целиком в память не
	// кладется: пачку предполагается собирать в два прохода (ADR-0021), и
	// буферизовать по 25 МБ на участника при каждом проходе незачем.
	File io.Reader
}

// Record — запись индекса.
type Record struct {
	Version    [16]byte
	Tenant     uint64
	VersionSeq uint64
	PlainLen   uint64
	FileOff    uint64
	FileLen    uint64
	WrappedDEK []byte
	Flags      uint8
}

// Footer — то, что читатель находит в последних 64 байтах.
type Footer struct {
	Format      uint8
	IndexSHA    [32]byte
	IndexLen    uint64
	Count       uint64
	IndexOffset uint64
}

// Summary — итог записи: нужен migrator'у для строк packs и archive_copies и
// для атрибута tapegw.pack.index_off.
type Summary struct {
	TotalBytes  uint64
	IndexOffset uint64
	IndexLen    uint64
	Count       uint64
	IndexSHA    [32]byte
}

// Writer собирает пачку. Смещения считаются сами: migrator не должен их
// вычислять и не должен иметь возможности ошибиться на единицу — такое чтение
// читает чужие байты и выглядит как повреждение ключа, а не как баг сборщика.
type Writer struct {
	counted *countWriter
	tw      *tar.Writer
	count   uint64
	index   []byte
	closed  bool
}

type countWriter struct {
	w io.Writer
	n uint64
}

func (c *countWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += uint64(n)
	return n, err
}

// NewWriter начинает пачку. w не закрывается — им владеет вызывающий.
func NewWriter(w io.Writer) *Writer {
	cw := &countWriter{w: w}
	return &Writer{counted: cw, tw: tar.NewWriter(cw)}
}

// AddMember добавляет участника и возвращает смещение его файла в пачке.
func (w *Writer) AddMember(m Member) (uint64, error) {
	if w.closed {
		return 0, ErrClosed
	}
	if len(m.WrappedDEK) > maxDEKLen {
		return 0, fmt.Errorf("%w: %d байт", ErrDEKTooLong, len(m.WrappedDEK))
	}
	if m.FileLen > math.MaxInt64 {
		return 0, fmt.Errorf("pack: участник %d байт не помещается в заголовок tar", m.FileLen)
	}

	hdr := &tar.Header{
		Name:   uuidString(m.Version),
		Mode:   0400,
		Size:   int64(m.FileLen),
		Format: tar.FormatPAX,
		// POSIX требует vendor-ключей вида VENDOR.keyword. Эти четыре поля —
		// то же, что лежит в индексе, и без них запасной путь пустой: в самом
		// файле участника обёртки DEK нет (ADR-0019), а без неё манифест не
		// открыть.
		PAXRecords: map[string]string{
			"TAPEGW.tenant": fmt.Sprintf("%d", m.Tenant),
			"TAPEGW.seq":    fmt.Sprintf("%d", m.VersionSeq),
			"TAPEGW.dek":    base64.StdEncoding.EncodeToString(m.WrappedDEK),
			"TAPEGW.kek":    fmt.Sprintf("%d", m.KekVersion),
		},
	}
	if err := w.tw.WriteHeader(hdr); err != nil {
		return 0, fmt.Errorf("pack: заголовок участника: %w", err)
	}
	// Смещение данных — уже после заголовка: именно отсюда читатель будет
	// брать файл участника, не разбирая tar.
	off := w.counted.n
	n, err := io.CopyN(w.tw, m.File, int64(m.FileLen))
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, fmt.Errorf("pack: данные участника: %w", err)
	}
	if uint64(n) != m.FileLen {
		return 0, fmt.Errorf("pack: участник %s короче объявленного: %d из %d байт",
			uuidString(m.Version), n, m.FileLen)
	}
	// Лишние байты — тоже ошибка, а не «запишем сколько дали»: заголовок
	// обещает FileLen, и следующий участник начался бы с середины данных.
	if extra, err := m.File.Read([]byte{0}); err != io.EOF || extra != 0 {
		return 0, fmt.Errorf("pack: участник %s длиннее объявленного", uuidString(m.Version))
	}
	if err := w.tw.Flush(); err != nil {
		return 0, fmt.Errorf("pack: flush участника: %w", err)
	}

	w.index = append(w.index, Record{
		Version: m.Version, Tenant: m.Tenant, VersionSeq: m.VersionSeq,
		PlainLen: m.PlainLen, FileOff: off, FileLen: m.FileLen,
		WrappedDEK: m.WrappedDEK, Flags: m.Flags,
	}.encode()...)
	w.count++
	return off, nil
}

// Close дописывает индекс и футер.
func (w *Writer) Close() (Summary, error) {
	if w.closed {
		return Summary{}, ErrClosed
	}
	w.closed = true
	if err := w.tw.Close(); err != nil {
		return Summary{}, fmt.Errorf("pack: закрыть tar: %w", err)
	}

	sum := Summary{Count: w.count, IndexOffset: w.counted.n, IndexLen: uint64(len(w.index))}
	if _, err := w.counted.Write(w.index); err != nil {
		return Summary{}, fmt.Errorf("pack: индекс: %w", err)
	}
	sum.IndexSHA = sha256.Sum256(w.index)

	foot := make([]byte, 0, FooterSize)
	foot = append(foot, magicFooter[:]...)
	foot = append(foot, FormatVersion)
	foot = append(foot, sum.IndexSHA[:]...)
	foot = binary.BigEndian.AppendUint64(foot, sum.IndexLen)
	foot = binary.BigEndian.AppendUint64(foot, sum.Count)
	foot = append(foot, make([]byte, FooterSize-len(foot))...) // reserved
	if _, err := w.counted.Write(foot); err != nil {
		return Summary{}, fmt.Errorf("pack: футер: %w", err)
	}
	sum.TotalBytes = w.counted.n
	return sum, nil
}

// encode — запись индекса. Поля фиксированные, кроме обёртки DEK: её длина
// стоит впереди и по ней же читатель и идёт. Поэтому запись ~119 байт, а не
// 314 с выравниванием до 255: индекс пачки из десятков тысяч участников
// стоит дороже, чем кажется.
func (r Record) encode() []byte {
	b := make([]byte, 0, indexRecHead+len(r.WrappedDEK)+1)
	b = append(b, r.Version[:]...)
	b = binary.BigEndian.AppendUint64(b, r.Tenant)
	b = binary.BigEndian.AppendUint64(b, r.VersionSeq)
	b = binary.BigEndian.AppendUint64(b, r.PlainLen)
	b = binary.BigEndian.AppendUint64(b, r.FileOff)
	b = binary.BigEndian.AppendUint64(b, r.FileLen)
	b = binary.BigEndian.AppendUint16(b, uint16(len(r.WrappedDEK)))
	b = append(b, r.WrappedDEK...)
	b = append(b, r.Flags)
	return b
}

func uuidString(v [16]byte) string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", v[0:4], v[4:6], v[6:8], v[8:10], v[10:16])
}
