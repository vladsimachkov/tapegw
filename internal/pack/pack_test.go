package pack_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/pack"
)

func member(t testing.TB, tenant, seq uint64, body string) pack.Member {
	t.Helper()
	return pack.Member{
		Version:    uuid.New(),
		Tenant:     tenant,
		VersionSeq: seq,
		PlainLen:   uint64(len(body)) * 3,
		FileLen:    uint64(len(body)),
		KekVersion: 7,
		WrappedDEK: []byte("wrapped-dek-" + body),
		Flags:      0,
		File:       bytes.NewReader([]byte(body)),
	}
}

func build(t *testing.T, members ...pack.Member) (*bytes.Reader, pack.Summary, [][]byte) {
	t.Helper()
	var buf bytes.Buffer
	w := pack.NewWriter(&buf)
	var bodies [][]byte
	for _, m := range members {
		body, ok := m.File.(*bytes.Reader)
		if !ok {
			t.Fatal("ожидается bytes.Reader")
		}
		data, _ := io.ReadAll(body)
		bodies = append(bodies, data)
		m.File = bytes.NewReader(data)
		m.FileLen = uint64(len(data))
		if _, err := w.AddMember(m); err != nil {
			t.Fatalf("AddMember: %v", err)
		}
	}
	sum, err := w.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}
	return bytes.NewReader(buf.Bytes()), sum, bodies
}

func TestRoundTrip(t *testing.T) {
	m1 := member(t, 10, 1, "alpha")
	m2 := member(t, 10, 2, "beta")
	m3 := member(t, 11, 1, "gamma")
	r, sum, bodies := build(t, m1, m2, m3)
	size := int64(sum.TotalBytes)

	foot, err := pack.ReadFooter(r, size)
	if err != nil {
		t.Fatalf("ReadFooter: %v", err)
	}
	if foot.Count != 3 {
		t.Errorf("участников %d, ожидалось 3", foot.Count)
	}
	if foot.Format != pack.FormatVersion {
		t.Errorf("версия формата %d", foot.Format)
	}
	if foot.IndexOffset+foot.IndexLen+pack.FooterSize != uint64(size) {
		t.Errorf("индекс не в конце: off=%d len=%d size=%d", foot.IndexOffset, foot.IndexLen, size)
	}

	recs, err := pack.ReadIndex(r, foot)
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	if len(recs) != 3 {
		t.Fatalf("записей %d, ожидалось 3", len(recs))
	}
	want := []pack.Member{m1, m2, m3}
	for i, rec := range recs {
		if rec.Version != want[i].Version {
			t.Errorf("участник %d: uuid не тот", i)
		}
		if rec.Tenant != want[i].Tenant || rec.VersionSeq != want[i].VersionSeq {
			t.Errorf("участник %d: тенант/seq %d/%d, ожидалось %d/%d",
				i, rec.Tenant, rec.VersionSeq, want[i].Tenant, want[i].VersionSeq)
		}
		if !bytes.Equal(rec.WrappedDEK, want[i].WrappedDEK) {
			t.Errorf("участник %d: обёртка DEK не та", i)
		}
		if rec.PlainLen != want[i].PlainLen {
			t.Errorf("участник %d: plain_len %d, ожидалось %d", i, rec.PlainLen, want[i].PlainLen)
		}
		// Смещение обязано указывать ровно на шифротекст: читатель берёт отсюда
		// байты, не разбирая tar, и ошибка на единицу читает чужие данные.
		data := make([]byte, rec.FileLen)
		if _, err := r.ReadAt(data, int64(rec.FileOff)); err != nil {
			t.Fatalf("участник %d: читать по смещению: %v", i, err)
		}
		if !bytes.Equal(data, bodies[i]) {
			t.Errorf("участник %d: по смещению %q, ожидалось %q", i, data, bodies[i])
		}
	}
}

func TestIndexCorruptionDetected(t *testing.T) {
	r, sum, _ := build(t, member(t, 10, 1, "alpha"), member(t, 10, 2, "beta"))
	raw := make([]byte, sum.TotalBytes)
	if _, err := r.ReadAt(raw, 0); err != nil {
		t.Fatalf("читать пачку: %v", err)
	}
	// Портим один байт внутри индекса — например тот, что меняет tenant у
	// первой записи. Ошибки расшифровки это не даёт, поэтому индекс и сверяется
	// отдельно.
	raw[int(sum.IndexOffset)+20] ^= 0xff
	if _, err := pack.ReadIndex(bytes.NewReader(raw), mustFooter(t, raw)); !errors.Is(err, pack.ErrIndexChecksum) {
		t.Errorf("испорченный индекс прочитался как %v, ожидалась ошибка суммы", err)
	}
}

func TestUnknownFormatRejected(t *testing.T) {
	r, sum, _ := build(t, member(t, 10, 1, "alpha"))
	raw := make([]byte, sum.TotalBytes)
	if _, err := r.ReadAt(raw, 0); err != nil {
		t.Fatalf("читать пачку: %v", err)
	}
	// Байт версии формата — пятый с конца футера.
	raw[len(raw)-pack.FooterSize+4] = pack.FormatVersion + 1
	if _, err := pack.ReadFooter(bytes.NewReader(raw), int64(len(raw))); !errors.Is(err, pack.ErrBadFormat) {
		t.Errorf("неизвестная версия формата прочиталась как %v, ожидался отказ", err)
	}
}

func TestNotAPack(t *testing.T) {
	raw := make([]byte, pack.FooterSize+10)
	if _, err := pack.ReadFooter(bytes.NewReader(raw), int64(len(raw))); !errors.Is(err, pack.ErrBadFooter) {
		t.Errorf("мусор прочитался как %v, ожидался отказ", err)
	}
	if _, err := pack.ReadFooter(bytes.NewReader(raw[:10]), 10); !errors.Is(err, pack.ErrBadFooter) {
		t.Errorf("кусок прочитался как %v, ожидался отказ", err)
	}
}

// Walk — единственный запасной путь при испорченном индексе. Если он не
// отдаёт те же данные, что индекс, «восстановление» означает тихую потерю
// участников.
func TestWalkWithoutIndex(t *testing.T) {
	m1 := member(t, 10, 1, "alpha")
	m2 := member(t, 11, 5, "beta")
	r, sum, bodies := build(t, m1, m2)
	raw := make([]byte, sum.TotalBytes)
	if _, err := r.ReadAt(raw, 0); err != nil {
		t.Fatalf("читать пачку: %v", err)
	}

	var got []pack.MemberHeader
	var data [][]byte
	err := pack.Walk(bytes.NewReader(raw[:int(sum.IndexOffset)]), func(mh pack.MemberHeader, body io.Reader) error {
		b, err := io.ReadAll(body)
		if err != nil {
			return err
		}
		got = append(got, mh)
		data = append(data, b)
		return nil
	})
	if err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("обход нашёл %d участников, ожидалось 2", len(got))
	}
	for i, want := range []pack.Member{m1, m2} {
		if got[i].Version != want.Version {
			t.Errorf("участник %d: uuid не тот", i)
		}
		if got[i].Tenant != want.Tenant || got[i].VersionSeq != want.VersionSeq {
			t.Errorf("участник %d: тенант/seq %d/%d, ожидалось %d/%d",
				i, got[i].Tenant, got[i].VersionSeq, want.Tenant, want.VersionSeq)
		}
		if got[i].KekVersion != want.KekVersion {
			t.Errorf("участник %d: версия KEK %d, ожидалось %d", i, got[i].KekVersion, want.KekVersion)
		}
		if !bytes.Equal(got[i].WrappedDEK, want.WrappedDEK) {
			t.Errorf("участник %d: обёртка DEK из заголовка не та", i)
		}
		if !bytes.Equal(data[i], bodies[i]) {
			t.Errorf("участник %d: содержимое по обходу не то", i)
		}
	}
}

func TestDEKTooLongRejected(t *testing.T) {
	var buf bytes.Buffer
	w := pack.NewWriter(&buf)
	m := member(t, 10, 1, "alpha")
	m.WrappedDEK = make([]byte, 256)
	if _, err := w.AddMember(m); !errors.Is(err, pack.ErrDEKTooLong) {
		t.Errorf("DEK 256 байт принят как %v: в индексе поле длины 2 байта, а по ADR предел 255", err)
	}
}

func TestEmptyPack(t *testing.T) {
	r, sum, _ := build(t)
	foot, err := pack.ReadFooter(r, int64(sum.TotalBytes))
	if err != nil {
		t.Fatalf("пустая пачка: %v", err)
	}
	recs, err := pack.ReadIndex(r, foot)
	if err != nil {
		t.Fatalf("индекс пустой пачки: %v", err)
	}
	if len(recs) != 0 {
		t.Errorf("в пустой пачке %d записей", len(recs))
	}
}

func mustFooter(t *testing.T, raw []byte) pack.Footer {
	t.Helper()
	f, err := pack.ReadFooter(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		t.Fatalf("футер: %v", err)
	}
	return f
}

// Сборка обязана быть детерминированной: та же последовательность участников
// даёт побайтно одинаковую пачку. На этом держится двухпроходная отправка
// (ADR-0021): в первом проходе tapegw считает размер и сумму в io.Discard,
// во втором — те же байты идут в соединение с mover'ом, и сумма из первого
// прохода обязана совпасть со второй.
//
// Если в заголовок когда-нибудь попадёт time.Now(), тест упадёт здесь, а не
// на ленте с checksum_mismatch после минут записи.
func TestBuildIsDeterministic(t *testing.T) {
	// Участники фиксируются один раз: детерминизм про «те же участники в том
	// же порядке», а не про «сгенерировать ещё раз то же самое».
	type spec struct {
		pack.Member
		body string
	}
	var specs []spec
	for i := 0; i < 5; i++ {
		body := fmt.Sprintf("тело-%d", i)
		m := member(t, uint64(10+i), uint64(i), body)
		specs = append(specs, spec{m, body})
	}

	mk := func() []byte {
		var buf bytes.Buffer
		w := pack.NewWriter(&buf)
		for _, sp := range specs {
			m := sp.Member
			m.File = bytes.NewReader([]byte(sp.body))
			m.FileLen = uint64(len(sp.body))
			if _, err := w.AddMember(m); err != nil {
				t.Fatalf("AddMember: %v", err)
			}
		}
		if _, err := w.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		return buf.Bytes()
	}

	first, second := mk(), mk()
	if !bytes.Equal(first, second) {
		t.Errorf("две сборки одной последовательности различаются: %d и %d байт", len(first), len(second))
	}
	// Сумма индекса — та же: на неё смотрит migrator между проходами.
	f1, err := pack.ReadFooter(bytes.NewReader(first), int64(len(first)))
	if err != nil {
		t.Fatalf("футер первой: %v", err)
	}
	f2, err := pack.ReadFooter(bytes.NewReader(second), int64(len(second)))
	if err != nil {
		t.Fatalf("футер второй: %v", err)
	}
	if f1.IndexSHA != f2.IndexSHA {
		t.Error("сумма индекса различается между сборками")
	}
}

func TestMemberLengthMustMatchDeclared(t *testing.T) {
	// Короткий участник: заголовок обещает больше, чем есть.
	var buf bytes.Buffer
	w := pack.NewWriter(&buf)
	m := member(t, 10, 1, "short")
	m.FileLen = 100
	if _, err := w.AddMember(m); err == nil {
		t.Error("участник короче объявленного принят: заголовок врёт о размере")
	}

	// Длинный участник: лишние байты сдвинули бы следующий участник.
	w2 := pack.NewWriter(&buf)
	m2 := member(t, 10, 1, "short")
	m2.FileLen = 2
	if _, err := w2.AddMember(m2); err == nil {
		t.Error("участник длиннее объявленного принят: следующий начался бы с середины данных")
	}
}

// --- фаззинг читателя ---
//
// Читатель разбирает то, что пришло с ленты, а оно может быть испорчено —
// особенно на запасном пути по tar-заголовкам, где сумма индекса уже не
// сошлась. Разбор не должен паниковать и не должен выделять гигабайты из
// испорченного поля длины.
//
// Как это гоняется. Обычный `go test` в CI прогоняет затравки из f.Add — этого
// хватает на каждый PR. Длинный прогон (-fuzz) оставляем ночному заданию: найденные
// входы os.Fuzz кладёт в testdata/fuzz, и с этого момента они становятся
// постоянными тестами и переживают чистку кэша.

func FuzzReadFooter(f *testing.F) {
	var buf bytes.Buffer
	w := pack.NewWriter(&buf)
	m := member(f, 10, 1, "seed")
	m.File = bytes.NewReader([]byte("seed"))
	m.FileLen = 4
	if _, err := w.AddMember(m); err != nil {
		f.Fatal(err)
	}
	if _, err := w.Close(); err != nil {
		f.Fatal(err)
	}
	seed := buf.Bytes()
	f.Add(seed)
	f.Add(seed[:len(seed)/2])
	f.Add(make([]byte, 64))

	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) < pack.FooterSize {
			return
		}
		r := bytes.NewReader(raw)
		foot, err := pack.ReadFooter(r, int64(len(raw)))
		if err != nil {
			return
		}
		// Индекс обязан читаться в пределах размера, сколько бы ни обещал
		// испорченный index_len: иначе одно поле длины превращает разбор в
		// выделение памяти по размеру вслепую.
		if foot.IndexOffset+foot.IndexLen+pack.FooterSize > uint64(len(raw)) {
			t.Fatalf("индекс не влезает в объект: off=%d len=%d size=%d",
				foot.IndexOffset, foot.IndexLen, len(raw))
		}
		if _, err := pack.ReadIndex(r, foot); err != nil {
			return
		}
	})
}

func FuzzWalk(f *testing.F) {
	var buf bytes.Buffer
	w := pack.NewWriter(&buf)
	m := member(f, 10, 1, "seed")
	m.File = bytes.NewReader([]byte("seed"))
	m.FileLen = 4
	if _, err := w.AddMember(m); err != nil {
		f.Fatal(err)
	}
	if _, err := w.Close(); err != nil {
		f.Fatal(err)
	}
	seed := buf.Bytes()
	f.Add(seed)
	f.Add(seed[:256])
	f.Add([]byte("ustar\x0000"))

	f.Fuzz(func(t *testing.T, raw []byte) {
		err := pack.Walk(bytes.NewReader(raw), func(pack.MemberHeader, io.Reader) error {
			// Содержимое не читаем целиком: цель — разбор заголовков.
			return nil
		})
		if err != nil && errors.Is(err, io.ErrNoProgress) {
			t.Fatalf("читатель зациклился: %v", err)
		}
	})
}
