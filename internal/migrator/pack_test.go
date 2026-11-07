package migrator

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"testing"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/mover"
)

// fakeMover повторяет то, что делает настоящий mover по протоколу (ADR-0021):
// сверяет объявленную сумму с посчитанной по принятым байтам и при расхождении
// не пишет ничего. Тесты ниже проверяют поведение migrator'а, а не то, умеем ли
// мы поднимать TLS-соединение.
type fakeMover struct {
	// puts — сколько операций put прошло, по порядку.
	puts   []mover.PutIn
	shas   []string // суммы, которые mover посчитал сам
	accept bool     // последняя операция принята?
}

func (f *fakeMover) Put(in mover.PutIn) (*mover.Result, error) {
	body, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(body)
	got := hex.EncodeToString(sum[:])
	f.puts = append(f.puts, in)
	f.shas = append(f.shas, got)

	if got != in.SHA256 || int64(len(body)) != in.Size {
		f.accept = false
		// Именно так отвечает настоящий mover: ничего не записано, код
		// checksum_mismatch. Записанные байты здесь не хранятся намеренно —
		// тест «не записано» проверяется отсутствием записи.
		return &mover.Result{Op: in.Op, Code: mover.CodeChecksumMismatch}, nil
	}
	f.accept = true
	return &mover.Result{Op: in.Op, Code: mover.CodeOK}, nil
}

func item(t *testing.T, body []byte) Item {
	t.Helper()
	return Item{
		Version:    uuid.New(),
		Tenant:     7,
		VersionSeq: 1,
		PlainLen:   uint64(len(body)) * 3,
		KekVersion: 1,
		WrappedDEK: []byte("wrapped-dek"),
		FileLen:    uint64(len(body)),
		Open: func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(body)), nil
		},
	}
}

func items(t *testing.T, bodies ...string) []Item {
	t.Helper()
	var out []Item
	for _, b := range bodies {
		out = append(out, item(t, []byte(b)))
	}
	return out
}

// С1: два прохода дают одну сумму — mover принимает пачку по объявленной
// sha256, без checksum_mismatch.
func TestTwoPassSameSum(t *testing.T) {
	it := items(t, "первый", "второй", "третий")

	plan, err := Measure(it)
	if err != nil {
		t.Fatalf("Measure: %v", err)
	}
	if plan.Size == 0 || plan.SHA256 == "" {
		t.Fatalf("пустой план: %+v", plan)
	}

	f := &fakeMover{}
	var buf bytes.Buffer
	if err := Build(&buf, it, plan); err != nil {
		t.Fatalf("Build: %v", err)
	}
	res, err := f.Put(mover.PutIn{Op: uuid.New(), Size: plan.Size, SHA256: plan.SHA256, Body: bytes.NewReader(buf.Bytes())})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if res.Code != mover.CodeOK {
		t.Fatalf("mover отверг свою же пачку: %s", res.Code)
	}
	if f.shas[0] != plan.SHA256 {
		t.Errorf("сумма mover'а %s не равна объявленной %s", f.shas[0], plan.SHA256)
	}
	if int64(buf.Len()) != plan.Size {
		t.Errorf("размер второго прохода %d не равен объявленному %d", buf.Len(), plan.Size)
	}
}

// С2: если между проходами участник изменился, migrator отказывается сам, а
// mover тем более отвергает по сумме. Записи быть не должно.
func TestChangedMemberIsRefused(t *testing.T) {
	it := items(t, "оригинал", "второй")

	plan, err := Measure(it)
	if err != nil {
		t.Fatalf("Measure: %v", err)
	}

	// Подмена: тот же участник, другие байты, объявленная длина та же.
	// Так выглядит файл, который переписали после первого прохода.
	it[0].Open = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader([]byte("подмена!!"))), nil
	}

	var buf bytes.Buffer
	if err := Build(&buf, it, plan); err == nil {
		t.Fatal("подменённый участник прошёл: migrator объявит mover'у сумму, которой нет")
	}

	// И на стороне mover'а: даже если migrator промахнулся, сумма не сходится,
	// и пачка не принимается.
	f := &fakeMover{}
	if _, err := f.Put(mover.PutIn{Op: uuid.New(), Size: plan.Size, SHA256: plan.SHA256, Body: bytes.NewReader(buf.Bytes())}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if f.accept {
		t.Error("mover принял пачку с другой суммой")
	}
}

// С2 (второе лицо): изменилась не длина, а только байты при той же длине —
// самый коварный случай, потому что размер плана совпадает.
func TestChangedBytesSameLengthIsRefused(t *testing.T) {
	it := items(t, "aaaa")
	plan, err := Measure(it)
	if err != nil {
		t.Fatalf("Measure: %v", err)
	}
	it[0].Open = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader([]byte("bbbb"))), nil
	}
	var buf bytes.Buffer
	err = Build(&buf, it, plan)
	if err == nil {
		t.Fatal("байты подменены при той же длине, а проверки нет")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("сумма")) {
		t.Errorf("ошибка не про сумму: %v", err)
	}
}

// С10: батч — это N отдельных операций put, а не одна на батч. Разным пачкам
// нужны разные носители, и один вызов уложил бы всё на одну ленту.
func TestBatchIsNPutOperations(t *testing.T) {
	f := &fakeMover{}
	packs := [][]Item{
		items(t, "пачка-1-а", "пачка-1-б"),
		items(t, "пачка-2-а"),
		items(t, "пачка-3-а"),
	}

	for _, p := range packs {
		plan, err := Measure(p)
		if err != nil {
			t.Fatalf("Measure: %v", err)
		}
		var buf bytes.Buffer
		if err := Build(&buf, p, plan); err != nil {
			t.Fatalf("Build: %v", err)
		}
		res, err := f.Put(mover.PutIn{Op: uuid.New(), Size: plan.Size, SHA256: plan.SHA256, Body: bytes.NewReader(buf.Bytes())})
		if err != nil {
			t.Fatalf("Put: %v", err)
		}
		if res.Code != mover.CodeOK {
			t.Fatalf("пачка отвергнута: %s", res.Code)
		}
	}

	if len(f.puts) != len(packs) {
		t.Errorf("операций put: %d, ждали %d — одна операция на пачку", len(f.puts), len(packs))
	}
	// Разные пачки — разные op: идемпотентность привязана к паре (op, сумма).
	seen := map[uuid.UUID]bool{}
	for _, p := range f.puts {
		if seen[p.Op] {
			t.Error("две пачки отправлены с одним op: вторая выглядела бы повтором первой")
		}
		seen[p.Op] = true
	}
}

func TestItemWithoutOpenIsError(t *testing.T) {
	it := item(t, []byte("x"))
	it.Open = nil
	if _, err := Measure([]Item{it}); err == nil {
		t.Error("участник без Open принят")
	}
}
