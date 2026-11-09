package migrator

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"

	"github.com/vladsimachkov/tapegw/internal/archive"
)

// Крупный объект (≥ pack.threshold) уходит на ленту отдельным объектом Phobos,
// но через ту же машину состояний, что пачка: строка packs, запечатывание до
// отправки, захват с продлением, разбор неизвестного итога через stat, копии
// из одного буфера. Отличаются только байты и атрибуты:
//
//   * байты — файл объекта целиком (ADR-0020 §1), без tar, индекса и футера:
//     у единственного участника вырезать из потока нечего;
//   * oid первой копии — UUID версии (ADR-0020: «tapegw.version … он же oid
//     в Phobos»), поэтому строка packs заводится с id объекта (meta.
//     BeginObjectArchive), а копии-дубли — UUIDv5 от него (copyOp);
//   * атрибуты — шесть полей версии плюс tapegw.copy (ADR-0020 §1), их
//     считает objectAttrs, а не вызывающий: они целиком выводятся из участника.

// MeasureFile — первый проход крупного объекта: размер и сумма сырого файла.
//
// Детерминизм здесь проще пачки: байты — сам файл, и измениться между
// проходами может только он сам. Объявленная в БД длина (file_bytes)
// сверяется с фактической в обе стороны — то же правило #63, что у сборщика
// пачек: расхождение всплывает до отправки, а не как «сумма не сошлась» на
// стороне mover'а.
func MeasureFile(it Item) (*Plan, error) {
	if it.Open == nil {
		return nil, fmt.Errorf("migrator: участник %s без Open", it.Version)
	}
	rc, err := it.Open()
	if err != nil {
		return nil, fmt.Errorf("migrator: открыть %s: %w", it.Version, err)
	}
	defer rc.Close()
	sum := sha256.New()
	n, err := io.Copy(sum, rc)
	if err != nil {
		return nil, fmt.Errorf("migrator: прочитать %s: %w", it.Version, err)
	}
	if n != int64(it.FileLen) {
		return nil, fmt.Errorf("migrator: файл %s: %d байт, в БД объявлено %d", it.Version, n, it.FileLen)
	}
	return &Plan{
		Size:   n,
		SHA256: hex.EncodeToString(sum.Sum(nil)),
		// Смещение единственного участника — ноль, длина — весь файл. Именно
		// это окно потом вырежет recaller: для крупного объекта окно совпадает
		// со всем потоком, и «извлечение» превращается в отдачу целиком.
		Placements: []Placement{{Version: it.Version, Offset: 0, Length: it.FileLen}},
	}, nil
}

// BuildFile — второй проход крупного объекта: тот же файл теми же байтами.
// Размер и сумма сверяются с планом: файл на landing после коммита перехода
// неизменен, и любое расхождение — это испорченный файл, а не «другая сборка».
func BuildFile(w io.Writer, it Item, plan *Plan) ([]Placement, error) {
	rc, err := it.Open()
	if err != nil {
		return nil, fmt.Errorf("migrator: открыть %s: %w", it.Version, err)
	}
	defer rc.Close()
	sum := sha256.New()
	n, err := io.Copy(io.MultiWriter(w, sum), rc)
	if err != nil {
		return nil, fmt.Errorf("migrator: отдать %s: %w", it.Version, err)
	}
	if n != plan.Size {
		return nil, fmt.Errorf("migrator: файл %s изменился между проходами: было %d, стало %d", it.Version, plan.Size, n)
	}
	if got := hex.EncodeToString(sum.Sum(nil)); got != plan.SHA256 {
		return nil, fmt.Errorf("migrator: сумма файла %s изменилась между проходами: было %s, стало %s", it.Version, plan.SHA256, got)
	}
	return []Placement{{Version: it.Version, Offset: 0, Length: uint64(n)}}, nil
}

// objectAttrs — шесть атрибутов версии (ADR-0020 §1). Седьмой, tapegw.copy,
// добавляет attrsFor при записи конкретной копии.
//
// Атрибуты выводятся целиком из участника — и это важно для продолжения:
// ResumePacks пересобирает их из загрузчика, без внешнего вызывающего, и
// «атрибуты знает только тот, кто начал» оставило бы запечатанного крупного
// объекта без ключей для повторной отправки.
func objectAttrs(it Item) (map[string]string, error) {
	return archive.Attributes(archive.Version{
		Version:    it.Version,
		Tenant:     int64(it.Tenant),
		WrappedDEK: it.WrappedDEK,
		KEK:        strconv.FormatUint(it.KekVersion, 10),
		Format:     archive.FormatVersion,
		Seq:        it.VersionSeq,
	})
}

// unitBuilder — как собрать байты единицы архивации. Два реализма — пачка
// (tar + индекс + футер) и крупный объект (сырой файл) — и ровно один
// deliver поверх них: запас устойчивости к сбоям у путей обязан быть общим.
//
// Формат на ленту передаёт вызывающий: у пачки и файла объекта счётчики версий
// формата независимы (pack.FormatVersion и archive.FormatVersion), и какой из
// них относится к единице, знает путь архивации, а не сборщик байтов.
type unitBuilder interface {
	measure(items []Item) (*Plan, error)
	build(w io.Writer, items []Item, plan *Plan) ([]Placement, error)
}

type packUnit struct{}

func (packUnit) measure(items []Item) (*Plan, error) { return Measure(items) }
func (packUnit) build(w io.Writer, items []Item, plan *Plan) ([]Placement, error) {
	return BuildIndexed(w, items, plan)
}

type objectUnit struct{}

func (objectUnit) measure(items []Item) (*Plan, error) {
	if len(items) != 1 {
		return nil, fmt.Errorf("migrator: крупный объект — один участник, их %d", len(items))
	}
	return MeasureFile(items[0])
}
func (objectUnit) build(w io.Writer, items []Item, plan *Plan) ([]Placement, error) {
	if len(items) != 1 {
		return nil, fmt.Errorf("migrator: крупный объект — один участник, их %d", len(items))
	}
	return BuildFile(w, items[0], plan)
}

// SplitByThreshold разбивает батч по pack.threshold: меньше порога — в пачку,
// больше или ровно — крупный объект («объекты меньше порога идут в пачки,
// остальные пишутся отдельными объектами Phobos»). Ровно порог — крупный:
// граница включена в «остальные», и то же значение стоит в договоре про RPO.
//
// Сравнивается открытый размер (PlainLen = objects.size), а не длина
// шифротекста на диске: порог — про объект, который видит клиент (см.
// pack.threshold в конфиге), а file_bytes у каждого объекта свой.
func SplitByThreshold(items []Item, threshold int64) (small, large []Item) {
	for _, it := range items {
		if int64(it.PlainLen) >= threshold {
			large = append(large, it)
			continue
		}
		small = append(small, it)
	}
	return small, large
}
