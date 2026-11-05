// Package migrator поднимает объекты с landing на ленту.
//
// Ключевое ограничение (ADR-0021): put объявляет размер и сумму ДО передачи
// первого байта, а mover сверяет сумму до phobos_put. Значит tapegw обязан
// знать сумму пачки заранее. Два варианта были: собрать пачку во временный
// файл (ещё одна запись и чтение всей пачки на landing — то, чего хотели
// избежать) и два прохода по участникам.
//
// Выбран второй, и держится он на детерминизме сборки (ADR-0020): те же
// участники в том же порядке дают побайтно одинаковую пачку. Первый проход
// считает размер и сумму в io.Discard, второй — та же сборка, но байты идут
// в соединение с mover'ом. Файлы участников после коммита неизменны, поэтому
// оба прохода дают одни и те же байты.
package migrator

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/pack"
)

// Item — участник пачки. Open вызывается по одному разу на проход: участник
// читается с самого начала, целиком в память не кладётся (пачка идёт двумя
// проходами, и буферизовать по 25 МБ на участника незачем).
type Item struct {
	Version    uuid.UUID
	Tenant     uint64
	VersionSeq uint64
	PlainLen   uint64
	KekVersion uint64
	WrappedDEK []byte
	Flags      uint8

	// CipherLen объявляется заранее: он есть в БД, и без него tar не может
	// выставить размер в заголовке, а значит поток нельзя читать по
	// заголовкам без индекса.
	CipherLen uint64

	// Open открывает шифротекст участника на чтение с начала.
	Open func() (io.ReadCloser, error)
}

func (it Item) member() (pack.Member, error) {
	if it.Open == nil {
		return pack.Member{}, fmt.Errorf("migrator: участник %s без Open", it.Version)
	}
	return pack.Member{
		Version:    it.Version,
		Tenant:     it.Tenant,
		VersionSeq: it.VersionSeq,
		PlainLen:   it.PlainLen,
		KekVersion: it.KekVersion,
		WrappedDEK: it.WrappedDEK,
		Flags:      it.Flags,
		CipherLen:  it.CipherLen,
	}, nil
}

// Plan — результат первого прохода: то, что migrator объявляет mover'у.
type Plan struct {
	Size   int64
	SHA256 string
}

// Measure — первый проход: сборка в io.Discard с подсчётом размера и суммы.
//
// Байты этого прохода никуда не пишутся специально: если записать их на
// landing, смысл двухпроходной схемы теряется.
func Measure(items []Item) (*Plan, error) {
	sum := sha256.New()
	n, err := build(io.Discard, items, sum)
	if err != nil {
		return nil, err
	}
	return &Plan{Size: n, SHA256: hex.EncodeToString(sum.Sum(nil))}, nil
}

// Build — второй проход: та же сборка, но байты идут в w.
//
// Сумма считается здесь же и сверяется с объявленной: если участник изменился
// между проходами, migrator узнаёт об этом сам, до соединения с mover'ом.
// Отказ на стороне mover'а тоже верный (он сверяет сумму перед phobos_put), но
// он приходит позже — когда байты уже переданы.
func Build(w io.Writer, items []Item, plan *Plan) error {
	sum := sha256.New()
	n, err := build(w, items, sum)
	if err != nil {
		return err
	}
	if n != plan.Size {
		return fmt.Errorf("migrator: размер пачки изменился между проходами: было %d, стало %d", plan.Size, n)
	}
	got := hex.EncodeToString(sum.Sum(nil))
	if got != plan.SHA256 {
		return fmt.Errorf("migrator: сумма пачки изменилась между проходами: было %s, стало %s", plan.SHA256, got)
	}
	return nil
}

// build собирает пачку в w и, если sum задан, считает SHA-256 записанного.
//
// Порядок участников задаёт вызывающий, и оба прохода обязаны идти в нём:
// перестановка меняет байты, а значит и сумму.
func build(w io.Writer, items []Item, sum hash.Hash) (int64, error) {
	cw := &countWriter{w: w, sum: sum}
	tw := pack.NewWriter(cw)
	for _, it := range items {
		m, err := it.member()
		if err != nil {
			return 0, err
		}
		rc, err := it.Open()
		if err != nil {
			return 0, fmt.Errorf("migrator: открыть участник %s: %w", it.Version, err)
		}
		m.Cipher = rc
		if _, err := tw.AddMember(m); err != nil {
			rc.Close()
			return 0, err
		}
		rc.Close()
	}
	if _, err := tw.Close(); err != nil {
		return 0, err
	}
	return cw.n, nil
}

// countWriter считает байты и, если задан sum, их сумму. Один проход пишет и
// считает по одному разу — иначе сумма считается от уже записанных байтов, а
// не от тех, что ушли в поток.
type countWriter struct {
	w   io.Writer
	sum hash.Hash
	n   int64
}

func (c *countWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	if c.sum != nil {
		c.sum.Write(p[:n])
	}
	return n, err
}
