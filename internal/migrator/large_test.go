package migrator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/archive"
	"github.com/vladsimachkov/tapegw/internal/mover"
	"github.com/vladsimachkov/tapegw/internal/store"
	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// Крупный объект проходит ту же машину состояний, что пачка, но байты на ленту
// идут сырым файлом, oid первой копии — UUID версии, а атрибуты — шесть полей
// версии плюс tapegw.copy (ADR-0020 §1).

// rawItem — участник-крупный объект: файл = содержимое целиком, FileLen его
// объявляет (в продукте — objects.file_bytes).
func rawItem(id uuid.UUID, body []byte) Item {
	return Item{
		Version:    id,
		Tenant:     2,
		VersionSeq: 7,
		PlainLen:   uint64(len(body)),
		KekVersion: 1,
		WrappedDEK: []byte("wrapped-dek"),
		FileLen:    uint64(len(body)),
		Open:       func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil },
	}
}

// Главный тест пути: сырые байты, oid = UUID версии, семь атрибутов,
// запечатывание с нулевым смещением и копия с ожидаемой суммой.
func TestArchiveLargeObjectRawBytesAndAttrs(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	ids := seedObjects(t, m, "large", 1)
	body := bytes.Repeat([]byte("крупный объект."), 4096) // ~64 KiB, не карман
	item := rawItem(ids[0], body)
	a, pm := archiver(t, m, ids[0])

	plan, err := a.ArchiveLargeObject(ctx, 1, item)
	if err != nil {
		t.Fatalf("ArchiveLargeObject: %v", err)
	}
	if !pm.accept {
		t.Fatal("mover отверг крупный объект")
	}
	// oid первой копии — UUID версии (ADR-0020: «tapegw.version … он же oid
	// в Phobos»), а не свежий UUID пачки.
	if pm.lastID != ids[0] {
		t.Errorf("oid Phobos %s, ждали UUID версии %s", pm.lastID, ids[0])
	}
	// Байты — сам файл, без tar, индекса и футера: ровно FileLen байт и ровно
	// то содержимое. Тар-обёртка здесь была бы вторым форматом на ленте.
	if !bytes.Equal(pm.lastBody, body) {
		t.Errorf("на ленту ушло %d байт вместо файла целиком", len(pm.lastBody))
	}
	sum := sha256.Sum256(body)
	if plan.SHA256 != hex.EncodeToString(sum[:]) {
		t.Errorf("сумма плана %s, ждали sha256 файла %x", plan.SHA256, sum)
	}

	// Атрибуты: шесть полей версии плюс копия (ADR-0020 §1).
	want := map[string]string{
		archive.AttrVersion: ids[0].String(),
		archive.AttrTenant:  "2",
		archive.AttrDEK:     base64.StdEncoding.EncodeToString([]byte("wrapped-dek")),
		archive.AttrKEK:     "1",
		archive.AttrFormat:  "1",
		archive.AttrSeq:     "7",
		archive.AttrCopy:    "pool:archive",
	}
	if len(pm.lastAttrs) != len(want) {
		t.Fatalf("атрибутов %d (%v), ждали %d", len(pm.lastAttrs), pm.lastAttrs, len(want))
	}
	for k, v := range want {
		if pm.lastAttrs[k] != v {
			t.Errorf("атрибут %s = %q, ждали %q", k, pm.lastAttrs[k], v)
		}
	}

	// Запечатывание: ссылка на пачку с нулевым смещением и длиной = весь файл.
	var packID *uuid.UUID
	var off, length *int64
	var state string
	if err := m.Pool.QueryRow(ctx,
		`SELECT pack_id, pack_offset, pack_length, state::text FROM objects WHERE id = $1`,
		ids[0]).Scan(&packID, &off, &length, &state); err != nil {
		t.Fatal(err)
	}
	if state != "archived" {
		t.Errorf("объект в %s, ждали archived", state)
	}
	if packID == nil || *packID != ids[0] {
		t.Errorf("pack_id %v, ждали UUID объекта — строка packs заводится им", packID)
	}
	if off == nil || *off != 0 || length == nil || *length != int64(len(body)) {
		t.Errorf("размещение (%v,%v), ждали (0,%d)", off, length, len(body))
	}
	var single bool
	var packState string
	if err := m.Pool.QueryRow(ctx,
		`SELECT single_object, state::text FROM packs WHERE id = $1`, ids[0]).Scan(&single, &packState); err != nil {
		t.Fatal(err)
	}
	if !single || packState != "archived" {
		t.Errorf("пачка single_object=%v state=%s, ждали true/archived", single, packState)
	}

	// Копия: oid = UUID версии, сумма — sha256 файла (ею сверит restore).
	var copyOID string
	var copySHA []byte
	var copyState string
	if err := m.Pool.QueryRow(ctx,
		`SELECT phobos_oid, sha256, state::text FROM archive_copies WHERE pack_id = $1`,
		ids[0]).Scan(&copyOID, &copySHA, &copyState); err != nil {
		t.Fatal(err)
	}
	if copyOID != ids[0].String() {
		t.Errorf("копия под oid %s, ждали %s", copyOID, ids[0])
	}
	if !bytes.Equal(copySHA, sum[:]) {
		t.Errorf("сумма копии %x, ждали %x", copySHA, sum)
	}
	if copyState != "done" {
		t.Errorf("копия в %s, ждали done", copyState)
	}
}

// Копии крупного объекта пишутся из одного буфера первой (Ф0.4): hold на первом
// put, from_op на остальных, oid копии — UUIDv5 от UUID объекта и spec
// (ADR-0022). Буфер после всех копий освобождается.
func TestArchiveLargeObjectCopiesFromOneBuffer(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	ids := seedObjects(t, m, "large-copies", 1)
	body := bytes.Repeat([]byte("x"), 4096)
	a, pm := archiver(t, m, ids[0])
	a.Spec = "side:left"
	a.Specs = []string{"side:left", "side:right"}
	a.SpecTags = map[string][]string{"side:left": {"side:left"}, "side:right": {"side:right"}}

	if _, err := a.ArchiveLargeObject(ctx, 1, rawItem(ids[0], body)); err != nil {
		t.Fatalf("архивация: %v", err)
	}
	if pm.holds != 1 {
		t.Errorf("put с hold: %d, ждали 1", pm.holds)
	}
	wantOp := uuid.NewSHA1(ids[0], []byte("side:right"))
	if pm.lastFromOp != ids[0] {
		t.Errorf("копия писалась не из буфера объекта: from_op=%s", pm.lastFromOp)
	}
	if pm.lastID != wantOp {
		t.Errorf("oid копии %s, ждали UUIDv5(obj,spec)=%s", pm.lastID, wantOp)
	}
	if got := copySpecsDone(t, m, ctx, ids[0]); len(got) != 2 {
		t.Errorf("готовых копий %v, ждали две", got)
	}
	if st := objectState(t, m, ctx, ids[0]); st != "archived" {
		t.Errorf("объект в %s, ждали archived", st)
	}
	if pm.releases != 1 {
		t.Errorf("буфер освобождён %d раз, ждали 1", pm.releases)
	}
	// Атрибуты второй копии несут её имя копии, а не имя первой.
	if pm.lastAttrs[archive.AttrCopy] != "side:right" {
		t.Errorf("tapegw.copy копии = %q, ждали side:right", pm.lastAttrs[archive.AttrCopy])
	}
}

// Продолжение запечатанного крупного объекта: после сбоя любой процесс обязан
// переслать ровно те же байты и с теми же атрибутами. Атрибуты выводятся из
// участника, а не помнятся вызывающим, — иначе рестарт потерял бы ключи
// объекта, а признак single_object отличает пересылку сырого файла от tar.
func TestResumeLargeObjectResendsRawFile(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	ids := seedObjects(t, m, "large-resume", 1)
	body := bytes.Repeat([]byte("y"), 8192)
	a, pm := archiver(t, m, ids[0])
	pm.failPut = errors.New("mover лёг до записи") // итог неизвестен, пачка остаётся sealed
	if _, err := a.ArchiveLargeObject(ctx, 1, rawItem(ids[0], body)); err == nil {
		t.Fatal("архивация с мёртвым mover прошла")
	}
	if st := objectState(t, m, ctx, ids[0]); st != "archiving" {
		t.Fatalf("объект в %s после сбоя, ждали archiving", st)
	}

	// Тот же слот после рестарта: WorkerID слота устойчив, и свой захват
	// воркер забирает без ожидания истечения.
	a2, pm2 := archiver(t, m, ids[0])
	a2.WorkerID = a.WorkerID
	load := func(context.Context, []uuid.UUID) ([]Item, error) {
		return []Item{rawItem(ids[0], body)}, nil
	}
	n, err := a2.ResumePacks(ctx, a2.WorkerID, 30*time.Second, load, nil)
	if err != nil {
		t.Fatalf("ResumePacks: %v", err)
	}
	if n != 1 {
		t.Fatalf("продолжено %d, ждали 1", n)
	}
	if !bytes.Equal(pm2.lastBody, body) {
		t.Error("продолжение переслало не сырой файл")
	}
	if pm2.lastAttrs[archive.AttrSeq] != "7" || pm2.lastAttrs[archive.AttrCopy] != "pool:archive" {
		t.Errorf("атрибуты продолжения %v — их обязан уметь восстановить любой процесс", pm2.lastAttrs)
	}
	if st := objectState(t, m, ctx, ids[0]); st != "archived" {
		t.Errorf("объект в %s после продолжения, ждали archived", st)
	}
}

// Худший случай атрибутов крупного объекта обязан влезать в бюджет user_md
// (4079 байт, M2-01). Таблица расчёта — ADR-0020 §1; здесь она посчитана
// программно, а не переписана от руки: число в комментарии устаревает молча.
func TestLargeObjectAttrsWithinBudget(t *testing.T) {
	attrs, err := archive.Attributes(archive.Version{
		Version:    uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff"),
		Tenant:     math.MaxInt64,
		WrappedDEK: make([]byte, 60), // base64 → 80
		KEK:        "99999",
		Format:     archive.FormatVersion,
		Seq:        math.MaxInt64,
	})
	if err != nil {
		t.Fatal(err)
	}
	attrs[archive.AttrCopy] = strings.Repeat("c", 32) // длиннее не по конфигу
	if got := store.AttrSize(attrs); got > store.AttrBudgetBytes {
		t.Fatalf("атрибуты крупного объекта %d байт, бюджет %d — mover вернёт attr_too_large на живом трафике",
			got, store.AttrBudgetBytes)
	}
}

// Буфер mover'а меньше архивного юнита: no_buffer приходит до приёма байт, на
// ленту ничего не легло. Пересылка повторила бы тот же отказ, поэтому пачка
// блокируется с причиной, объект остаётся в 'archiving', а продолжение такую
// пачку не берёт. Это ответ на «что делает migrator после no_buffer»: не
// бесконечную пересылку.
func TestArchiveLargeObjectNoBufferBlocks(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	ids := seedObjects(t, m, "large-nobuf", 1)
	body := bytes.Repeat([]byte("z"), 4096)
	a, pm := archiver(t, m, ids[0])
	pm.code = mover.CodeNoBuffer
	if _, err := a.ArchiveLargeObject(ctx, 1, rawItem(ids[0], body)); err == nil {
		t.Fatal("архивация прошла несмотря на no_buffer")
	}
	if pm.puts != 1 {
		t.Errorf("put вызван %d раз, ждали 1: пачку переслали", pm.puts)
	}
	var reason, state string
	if err := m.Pool.QueryRow(ctx,
		`SELECT blocked_reason, state::text FROM packs WHERE id = $1`, ids[0]).
		Scan(&reason, &state); err != nil {
		t.Fatal(err)
	}
	if state != "sealed" || !strings.Contains(reason, "no_buffer") {
		t.Errorf("пачка %s / %q, ждали sealed с причиной no_buffer", state, reason)
	}
	if st := objectState(t, m, ctx, ids[0]); st != "archiving" {
		t.Errorf("объект в %s, ждали archiving: пачка не отменена, а не разобрана", st)
	}
	// Продолжение заблокированную пачку не берёт — пересылки нет.
	load := func(context.Context, []uuid.UUID) ([]Item, error) {
		return []Item{rawItem(ids[0], body)}, nil
	}
	n, err := a.ResumePacks(ctx, a.WorkerID, 30*time.Second, load, nil)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("продолжение взяло заблокированную пачку: %d", n)
	}
}

// MeasureFile обязан заметить расхождение объявленной и фактической длины до
// отправки — то же правило, что у сборщика пачек: «сумма не сошлась» на стороне
// mover'а или recaller'а уводит от причины на целый цикл ленты.
func TestMeasureFileRejectsLengthMismatch(t *testing.T) {
	body := []byte("файл ровно на столько-то байт")
	for _, declared := range []uint64{uint64(len(body)) + 1, uint64(len(body)) - 1} {
		it := rawItem(uuid.New(), body)
		it.FileLen = declared
		if _, err := MeasureFile(it); err == nil {
			t.Errorf("MeasureFile принял файл %d байт при объявленной длине %d",
				len(body), declared)
		}
	}
}

// Граница порога: ровно pack.threshold — уже крупный («меньше — в пачку,
// остальные — отдельными»), на байт меньше — пачка. Тест на значении по
// умолчанию из договора (25 MiB), а не на круглом тестовом: ошибка на
// единицу здесь меняет маршрут каждого объекта ровно 25 МБ.
func TestSplitByThresholdBoundary(t *testing.T) {
	const th = int64(25 * 1024 * 1024)
	at := func(n int64) Item { return Item{Version: uuid.New(), PlainLen: uint64(n)} }
	small, large := SplitByThreshold([]Item{at(th - 1), at(th), at(th + 1), at(1)}, th)
	if len(large) != 2 || large[0].PlainLen != uint64(th) || large[1].PlainLen != uint64(th+1) {
		t.Errorf("крупные %+v, ждали ровно порог и больше", large)
	}
	if len(small) != 2 || small[0].PlainLen != uint64(th-1) {
		t.Errorf("мелкие %+v, ждали порог-1 и 1 байт", small)
	}
}
