package pack

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
)

// ReadFooter читает футер из последних FooterSize байт. size — размер всего
// объекта Phobos (его сообщает stat), поэтому отдельного «где кончается tar»
// искать не нужно.
//
// Неизвестная версия — отказ, а не «попробуем прочитать как умеем»: формат
// пачки меняется только вместе с перезаписью архива, и молча прочитать версию
// N+1 средствами версии N — значит выдать за корректные данные мусор.
func ReadFooter(r io.ReaderAt, size int64) (Footer, error) {
	var f Footer
	if size < FooterSize {
		return f, fmt.Errorf("%w: %d байт, минимум %d", ErrBadFooter, size, FooterSize)
	}
	buf := make([]byte, FooterSize)
	if _, err := r.ReadAt(buf, size-FooterSize); err != nil {
		return f, fmt.Errorf("%w: %v", ErrBadFooter, err)
	}
	if [4]byte(buf[:4]) != magicFooter {
		return f, ErrBadFooter
	}
	f.Format = buf[4]
	if f.Format != FormatVersion {
		return f, fmt.Errorf("%w: %d", ErrBadFormat, f.Format)
	}
	copy(f.IndexSHA[:], buf[5:37])
	f.IndexLen = binary.BigEndian.Uint64(buf[37:45])
	f.Count = binary.BigEndian.Uint64(buf[45:53])
	if f.IndexLen > uint64(size) || f.Count > f.IndexLen {
		return f, fmt.Errorf("%w: индекс %d байт и %d записей при размере %d",
			ErrBadFooter, f.IndexLen, f.Count, size)
	}
	f.IndexOffset = uint64(size) - FooterSize - f.IndexLen
	return f, nil
}

// ReadIndex читает индекс и **обязательно** сверяет контрольную сумму.
//
// Индекс открытый и смешанный, MAC на ключе тенанта к нему не подходит
// (ADR-0020): подмена version_seq или tenant в индексе не даёт ошибки
// расшифровки, она молча переставляет версии. Поэтому несовпадение суммы —
// ошибка восстановления, а не предупреждение: дальше надо идти по tar.
func ReadIndex(r io.ReaderAt, f Footer) ([]Record, error) {
	buf := make([]byte, f.IndexLen)
	if _, err := r.ReadAt(buf, int64(f.IndexOffset)); err != nil {
		return nil, fmt.Errorf("%w: читать индекс: %v", ErrShort, err)
	}
	if sha256.Sum256(buf) != f.IndexSHA {
		return nil, ErrIndexChecksum
	}
	recs := make([]Record, 0, f.Count)
	for off := 0; uint64(len(recs)) < f.Count; {
		rec, n, err := decodeRecord(buf[off:])
		if err != nil {
			return nil, fmt.Errorf("pack: запись %d: %w", len(recs), err)
		}
		recs = append(recs, rec)
		off += n
	}
	return recs, nil
}

func decodeRecord(b []byte) (Record, int, error) {
	var r Record
	if len(b) < indexRecHead {
		return r, 0, ErrShort
	}
	copy(r.Version[:], b[:16])
	off := 16
	r.Tenant = binary.BigEndian.Uint64(b[off:])
	off += 8
	r.VersionSeq = binary.BigEndian.Uint64(b[off:])
	off += 8
	r.PlainLen = binary.BigEndian.Uint64(b[off:])
	off += 8
	r.CipherOff = binary.BigEndian.Uint64(b[off:])
	off += 8
	r.CipherLen = binary.BigEndian.Uint64(b[off:])
	off += 8
	dekLen := int(binary.BigEndian.Uint16(b[off:]))
	off += 2
	// dekLen > maxDEKLen недостижим через публичный API: наш сборщик пишет
	// только то, что принял AddMember, а тот отбраковывает длиннее 255, и
	// подделать индекс так, чтобы сумма в футере осталась верной нельзя.
	// Ветка остаётся как граница для будущего писателя формата, тестом она
	// не покрыта — покрыть её можно только испорченным индексом с верной
	// суммой, а такого не бывает.
	if dekLen > maxDEKLen || len(b) < off+dekLen+1 {
		return r, 0, ErrShort
	}
	r.WrappedDEK = append([]byte(nil), b[off:off+dekLen]...)
	off += dekLen
	r.Flags = b[off]
	off++
	return r, off, nil
}

// MemberHeader — участник, опознанный по заголовку pax: то, что удаётся
// узнать без индекса.
type MemberHeader struct {
	Version    [16]byte
	Tenant     uint64
	VersionSeq uint64
	KekVersion uint64
	WrappedDEK []byte
	PlainLen   uint64
}

// Walk обходит пачку по заголовкам tar, **не пользуясь индексом**. Это не
// второй способ чтения «на всякий случай», а единственный запасной путь:
// если index_sha256 не сошёлся, восстановить пачку можно только так, и без
// TAPEGW.* в заголовках участников этот путь был бы пустым (ADR-0020).
func Walk(r io.Reader, fn func(MemberHeader, io.Reader) error) error {
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("pack: обход tar: %w", err)
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}
		v, err := parseUUID(hdr.Name)
		if err != nil {
			return fmt.Errorf("pack: имя участника %q: %w", hdr.Name, err)
		}
		mh := MemberHeader{Version: v, PlainLen: uint64(hdr.Size)}
		pax := hdr.PAXRecords
		if mh.Tenant, err = paxUint(pax, "TAPEGW.tenant"); err != nil {
			return fmt.Errorf("pack: участник %s: %w", hdr.Name, err)
		}
		if mh.VersionSeq, err = paxUint(pax, "TAPEGW.seq"); err != nil {
			return fmt.Errorf("pack: участник %s: %w", hdr.Name, err)
		}
		if mh.KekVersion, err = paxUint(pax, "TAPEGW.kek"); err != nil {
			return fmt.Errorf("pack: участник %s: %w", hdr.Name, err)
		}
		dek, err := base64.StdEncoding.DecodeString(pax["TAPEGW.dek"])
		if err != nil {
			return fmt.Errorf("pack: участник %s: TAPEGW.dek: %w", hdr.Name, err)
		}
		mh.WrappedDEK = dek
		if err := fn(mh, tr); err != nil {
			return err
		}
	}
}

func paxUint(m map[string]string, key string) (uint64, error) {
	s, ok := m[key]
	if !ok {
		return 0, fmt.Errorf("нет заголовка %s", key)
	}
	var n uint64
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
		return 0, fmt.Errorf("%s=%q: %w", key, s, err)
	}
	return n, nil
}

func parseUUID(s string) ([16]byte, error) {
	var v [16]byte
	// 8-4-4-4-12 шестнадцатеричных цифр
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return v, fmt.Errorf("не UUID")
	}
	hex := []byte(s)
	pos := 0
	for i, part := range [][2]int{{0, 8}, {9, 13}, {14, 18}, {19, 23}, {24, 36}} {
		for j := part[0]; j < part[1]; j += 2 {
			hi, err := hexVal(hex[j])
			if err != nil {
				return v, err
			}
			lo, err := hexVal(hex[j+1])
			if err != nil {
				return v, err
			}
			v[pos] = hi<<4 | lo
			pos++
		}
		_ = i
	}
	if pos != 16 {
		return v, fmt.Errorf("не UUID: %d байт", pos)
	}
	return v, nil
}

func hexVal(c byte) (byte, error) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', nil
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, nil
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, nil
	}
	return 0, fmt.Errorf("не hex: %q", string(c))
}
