package meta_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// Запечатывание сегментов журнала (миграция 0029, ADR-0023 п. 3–5, план Ж2).
//
// Что здесь под защитой, по порядку важности:
//
//   - плотная нумерация: диапазоны сегментов одного тенанта образуют ряд 1..n
//     без дырок и без повторов — по ним rebuild-meta применяет журнал и видит
//     пропуск;
//   - «ровно в один сегмент»: признак держится схемой (UNIQUE event_id), а не
//     аккуратностью воркера;
//   - одна транзакция: нумерация, сегмент и признак появляются вместе, иначе
//     счётчик уезжал бы вперёд без сегмента;
//   - опоздавшее событие (чья транзакция шла во время запечатывания) не
//     теряется, а находится проверкой ниже рубежа;
//   - параллельные силеры не мешают друг другу и не запечатывают одно дважды.

// sealSeed — n событий тенанта напрямую, с payload указанного размера.
//
// Напрямую, а не через путь изменения: тестам важны сами id и их порядок, а
// путь изменения со своими блокировками проверяет journal_test.go. bucket_id
// без внешнего ключа сознательно (миграция 0027: событие обязано пережить
// строку, о которой оно), поэтому бакет здесь — формальность.
func sealSeed(t *testing.T, m *meta.Meta, tenant int64, pads ...int) []int64 {
	t.Helper()
	ctx := context.Background()
	ids := make([]int64, 0, len(pads))
	for i, pad := range pads {
		payload := fmt.Sprintf(`{"pad":"%s"}`, strings.Repeat("x", pad))
		var id int64
		if err := m.Pool.QueryRow(ctx,
			`INSERT INTO journal_events (tenant_id, kind, bucket_id, payload)
			 VALUES ($1, 'bucket_config', 1, $2::jsonb) RETURNING id`,
			tenant, payload).Scan(&id); err != nil {
			t.Fatalf("событие %d тенанта %d: %v", i, tenant, err)
		}
		ids = append(ids, id)
	}
	return ids
}

// sealTenantExtra — второй тенант, чтобы проверять «силер не путает тенанты».
// Миграция заводит только системный тенант 1, поэтому стенд добавляет свой сам.
func sealTenantExtra(t *testing.T, m *meta.Meta) int64 {
	t.Helper()
	const id = 777
	if _, err := m.Pool.Exec(context.Background(),
		`INSERT INTO tenants (id, name, status, pool_id)
		 VALUES ($1, 'тенант запечатывания', 'active', 1)
		 ON CONFLICT (id) DO NOTHING`, id); err != nil {
		t.Fatalf("тенант %d: %v", id, err)
	}
	return id
}

// sealAll — запечатывает тенанта до конца. 100 сегментов заведомо больше, чем
// нужно любому тесту файла: если рубеж не двигается, тест обязан упасть, а не
// висеть до глобального таймаута.
func sealAll(t *testing.T, m *meta.Meta, tenant int64, lim meta.SealLimits) []meta.SealedSegment {
	t.Helper()
	var out []meta.SealedSegment
	for i := 0; i < 100; i++ {
		seg, err := m.SealJournalTenant(context.Background(), tenant, lim)
		if err != nil {
			t.Fatalf("запечатывание тенанта %d (сегмент %d): %v", tenant, i, err)
		}
		if seg == nil {
			return out
		}
		out = append(out, *seg)
	}
	t.Fatalf("запечатывание тенанта %d не остановилось за 100 сегментов: рубеж не двигается?", tenant)
	return out
}

// sealedIDs — id событий сегмента по возрастанию.
func sealedIDs(t *testing.T, m *meta.Meta, segment int64) []int64 {
	t.Helper()
	rows, err := m.Pool.Query(context.Background(),
		`SELECT event_id FROM journal_segment_event WHERE segment_id = $1 ORDER BY event_id`,
		segment)
	if err != nil {
		t.Fatalf("события сегмента %d: %v", segment, err)
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("события сегмента %d: %v", segment, err)
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("события сегмента %d: %v", segment, err)
	}
	return out
}

func sealWatermarkOf(t *testing.T, m *meta.Meta, tenant int64) int64 {
	t.Helper()
	wm, err := m.JournalSealWatermark(context.Background(), tenant)
	if err != nil {
		t.Fatalf("рубеж тенанта %d: %v", tenant, err)
	}
	return wm
}

func sealUnsealed(t *testing.T, m *meta.Meta, tenant int64) int64 {
	t.Helper()
	n, err := m.JournalUnsealedCount(context.Background(), tenant)
	if err != nil {
		t.Fatalf("незапечатанные события тенанта %d: %v", tenant, err)
	}
	return n
}

// sealListed — перечислены ли тенанты в списке на запечатывание.
func sealListed(t *testing.T, ids []int64, tenant int64) bool {
	t.Helper()
	for _, id := range ids {
		if id == tenant {
			return true
		}
	}
	return false
}

// checkDense — диапазоны сегментов образуют непрерывный ряд от 1 и покрывают
// ровно want событий. Формулировка «каждый следующий начинается ровно там,
// где кончился предыдущий» сильнее, чем «сумма длин равна», и именно она падает
// на дырке и на повторе.
func checkDense(t *testing.T, segs []meta.SealedSegment, want int) {
	t.Helper()
	prev := int64(0)
	for _, s := range segs {
		if s.SeqFirst != prev+1 {
			t.Errorf("сегмент %d начинается с %d, а предыдущий кончился на %d: "+
				"в плотной нумерации дырка или повтор", s.ID, s.SeqFirst, prev)
		}
		if s.SeqLast-s.SeqFirst+1 != int64(s.EventCount) {
			t.Errorf("сегмент %d: диапазон %d..%d, событий %d — длина не равна числу",
				s.ID, s.SeqFirst, s.SeqLast, s.EventCount)
		}
		prev = s.SeqLast
	}
	if int(prev) != want {
		t.Errorf("плотная нумерация дошла до %d, событий %d", prev, want)
	}
}

func TestSealSegmentsAreDenseAndIdempotent(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	if wm := sealWatermarkOf(t, m, 1); wm != 0 {
		t.Fatalf("рубеж пустого тенанта = %d, хотели 0", wm)
	}
	ids := sealSeed(t, m, 1, 8, 8, 8, 8, 8)

	lim := meta.SealLimits{MaxEvents: 2, MaxBytes: 1 << 20}
	segs := sealAll(t, m, 1, lim)
	if len(segs) != 3 {
		t.Fatalf("сегментов %d, хотели 3 (5 событий по 2): %+v", len(segs), segs)
	}

	checkDense(t, segs, 5)
	for i, s := range segs {
		want := ids[i*2:]
		if len(want) > 2 {
			want = want[:2]
		}
		got := sealedIDs(t, m, s.ID)
		if len(got) != len(want) {
			t.Fatalf("сегмент %d: событий %v, хотели %v", s.ID, got, want)
		}
		for j := range got {
			if got[j] != want[j] {
				t.Errorf("сегмент %d: события %v, хотели %v — плотный номер выдаётся "+
					"не в порядке id, и rebuild-meta применит журнал не в том порядке",
					s.ID, got, want)
			}
		}
	}

	// Событие обязано остаться ровно в одном сегменте: суммарное число признаков
	// равно числу событий, а незапечатанных не осталось вовсе.
	if n := sealUnsealed(t, m, 1); n != 0 {
		t.Errorf("незапечатанных событий %d, хотели 0", n)
	}
	if n := countRow(t, ctx, m, `SELECT count(*) FROM journal_segment_event`); n != 5 {
		t.Errorf("признаков %d, событий 5", n)
	}
	// Рубеж — максимальный id сегмента, а не число событий: id не плотные.
	if wm := sealWatermarkOf(t, m, 1); wm != ids[4] {
		t.Errorf("рубеж = %d, хотели %d (max id события)", wm, ids[4])
	}
	// Плотный номер живёт на тенанте и выдаётся только здесь.
	if seq := tenantJournalSeq(t, m, 1); seq != 5 {
		t.Errorf("tenants.journal_seq = %d, хотели 5", seq)
	}
	// uuid сегмента — AAD при шифровании (ADR-0020 §3): он обязан быть у каждого
	// сегмента и не повторяться, иначе шифротекст одного сегмента подошёл бы
	// другому.
	seen := map[uuid.UUID]bool{}
	for _, s := range segs {
		if s.UUID == uuid.Nil {
			t.Errorf("сегмент %d без uuid", s.ID)
		}
		if seen[s.UUID] {
			t.Errorf("uuid %s выдан двум сегментам", s.UUID)
		}
		seen[s.UUID] = true
	}

	// Чтение цепочки — так же её пройдёт rebuild-meta: по порядку seq_first.
	again, err := m.JournalSegments(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 3 {
		t.Fatalf("JournalSegments вернул %d сегментов, хотели 3", len(again))
	}
	for i := range again {
		if again[i].SeqFirst != segs[i].SeqFirst || again[i].EventCount != segs[i].EventCount {
			t.Errorf("сегмент %d: %+v, хотели %+v", i, again[i], segs[i])
		}
	}

	// Повторный проход — норма, а не ошибка: запечатывать нечего, нумерация
	// стоит на месте.
	if more := sealAll(t, m, 1, lim); len(more) != 0 {
		t.Errorf("второй проход запечатал %d сегментов поверх запечатанного", len(more))
	}
	if seq := tenantJournalSeq(t, m, 1); seq != 5 {
		t.Errorf("после повторного прохода journal_seq = %d, хотели 5: номера выданы дважды", seq)
	}
}

func TestSealSplitsByBytes(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)

	// Сумма считается по octet_length(payload::text) — тому же числу, что и в
	// выборке. Среднее событие заведомо больше всего порога: оно обязано уехать
	// одиночным сегментом, а не зациклить воркера.
	small, big := 8, 4000
	ids := sealSeed(t, m, 1, small, big, small)
	lim := meta.SealLimits{MaxEvents: 100, MaxBytes: 2000}

	segs := sealAll(t, m, 1, lim)
	if len(segs) != 3 {
		t.Fatalf("сегментов %d, хотели 3 (середина тяжелее порога): %+v", len(segs), segs)
	}
	for i, s := range segs {
		if s.EventCount != 1 {
			t.Errorf("сегмент %d: %d событий, хотели 1", i, s.EventCount)
		}
		if got, want := sealedIDs(t, m, s.ID), ids[i:i+1]; len(got) != 1 || got[0] != want[0] {
			t.Errorf("сегмент %d: %v, хотели %v", s.ID, got, want)
		}
	}
	checkDense(t, segs, 3)

	// Контроль: то же содержимое целиком влезает в сегмент, когда порог большой.
	// Без этого теста зелёный цвет означал бы «разрезали на три», а не «разрезали
	// из-за байтов».
	other := sealTenantExtra(t, m)
	sealSeed(t, m, other, small, big, small)
	if segs := sealAll(t, m, other, meta.SealLimits{MaxEvents: 100, MaxBytes: 1 << 20}); len(segs) != 1 {
		t.Errorf("сегментов у второго тенанта %d, хотели 1 — порог по байтам не единственная причина резать",
			len(segs))
	}
}

func TestSealMarkerIsUniqueAndAppendOnly(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	ids := sealSeed(t, m, 1, 8, 8)
	segs := sealAll(t, m, 1, meta.SealLimits{MaxEvents: 1, MaxBytes: 1 << 20})
	if len(segs) != 2 {
		t.Fatalf("сегментов %d, хотели 2", len(segs))
	}

	// «Ровно в один сегмент» держится схемой: второе запечатывание того же
	// события невозможно в принципе, а не «воркер не должен».
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO journal_segment_event (segment_id, event_id) VALUES ($1, $2)`,
		segs[1].ID, ids[0]); err == nil {
		t.Error("событие записали во второй сегмент: UNIQUE (event_id) не работает, " +
			"и одно событие попало бы в журнал дважды под разными номерами")
	}
	// Признак «запечатано» тоже только на добавление: снять его — значит
	// разрешить запечатать событие второй раз под другим плотным номером.
	// UPDATE no-op: триггер обязан сработать до проверки ограничений.
	if _, err := m.Pool.Exec(ctx,
		`UPDATE journal_segment_event SET event_id = event_id`); err == nil {
		t.Error("UPDATE признака запечатывания прошёл: признак можно переписать задним числом")
	}
	if _, err := m.Pool.Exec(ctx, `DELETE FROM journal_segment_event`); err == nil {
		t.Error("DELETE признака запечатывания прошёл: признак можно снять и запечатать заново")
	}
}

func TestSealStragglerIsFoundOnlyBySweep(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	// До первого сегмента искать опоздавших негде: рубежа нет.
	if seg, err := m.SealJournalStragglers(ctx, 1, 100, 1<<20); err != nil || seg != nil {
		t.Fatalf("проверка опоздавших без сегментов: seg=%v err=%v, хотели nil/nil", seg, err)
	}

	// «Опоздавшее» событие: id уже израсходован, коммита нет. Следующее
	// закоммиченное событие получит id больше и уйдёт в сегмент первым.
	slow, err := m.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = slow.Rollback(ctx) }() //nolint:errcheck // после commit откатывать нечего
	var slowID int64
	if err := slow.QueryRow(ctx,
		`INSERT INTO journal_events (tenant_id, kind, bucket_id, payload)
		 VALUES (1, 'bucket_config', 1, '{"slow":true}') RETURNING id`).Scan(&slowID); err != nil {
		t.Fatal(err)
	}
	fast := sealSeed(t, m, 1, 8)
	if slowID >= fast[0] {
		t.Fatalf("посев неверный: опоздавшее событие %d не ниже рубежа %d", slowID, fast[0])
	}

	seg, err := m.SealJournalTenant(ctx, 1, meta.SealLimits{MaxEvents: 10, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if seg == nil || seg.EventCount != 1 || sealedIDs(t, m, seg.ID)[0] != fast[0] {
		t.Fatalf("обычный ход запечатал %+v, хотели одно событие %d", seg, fast[0])
	}

	// Пока транзакция не закоммичена, события не видно вовсе — ни обычным ходом,
	// ни проверкой опоздавших.
	if ids, err := m.JournalTenantsToSeal(ctx, 10); err != nil || sealListed(t, ids, 1) {
		t.Fatalf("тенант в списке на запечатывание (%v, %v), а запечатывать нечего", ids, err)
	}
	if ids, err := m.JournalTenantsWithStragglers(ctx, 10); err != nil || sealListed(t, ids, 1) {
		t.Fatalf("тенант в списке опоздавших (%v, %v), а опоздавших нет", ids, err)
	}

	if err := slow.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if n := sealUnsealed(t, m, 1); n != 1 {
		t.Fatalf("незапечатанных событий %d, хотели 1", n)
	}
	// Обычный ход его не найдёт: id ниже рубежа.
	if ids, err := m.JournalTenantsToSeal(ctx, 10); err != nil || sealListed(t, ids, 1) {
		t.Errorf("обычный ход позвал тенанта %v (err=%v), а дальше рубежа событий нет — "+
			"опоздавшее событие уехало бы за журнал", ids, err)
	}
	ids, err := m.JournalTenantsWithStragglers(ctx, 10)
	if err != nil || !sealListed(t, ids, 1) {
		t.Fatalf("проверка опоздавших не позвала тенанта: %v (err=%v)", ids, err)
	}

	straggler, err := m.SealJournalStragglers(ctx, 1, 100, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if straggler == nil {
		t.Fatal("проверка опоздавших не запечатала ничего")
	}
	if got := sealedIDs(t, m, straggler.ID); len(got) != 1 || got[0] != slowID {
		t.Errorf("опоздавший сегмент: %v, хотели [%d]", got, slowID)
	}
	// Номер выдаётся в продолжение: дырки в плотном ряду нет, есть сегмент,
	// который идёт «не по порядку id» — и это единственный допустимый случай.
	if straggler.SeqFirst != 2 || straggler.SeqLast != 2 {
		t.Errorf("опоздавший сегмент: диапазон %d..%d, хотели 2..2",
			straggler.SeqFirst, straggler.SeqLast)
	}
	segs, err := m.JournalSegments(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	checkDense(t, segs, 2)
	if n := sealUnsealed(t, m, 1); n != 0 {
		t.Errorf("незапечатанных событий %d, хотели 0", n)
	}
	if ids, err := m.JournalTenantsWithStragglers(ctx, 10); err != nil || sealListed(t, ids, 1) {
		t.Errorf("после запечатывания тенант всё ещё в списке опоздавших: %v (err=%v)", ids, err)
	}
}

// TestSealStragglerInvertsDenseOrder — плотный порядок НЕ обязан совпадать с
// порядком коммитов одного объекта, и тест это фиксирует, а не чинит.
//
// Интерлевация целиком из реального пути изменения: T1 не может вставить своё
// событие того же объекта, пока T2 держит его блокировку, — поэтому id(E1) >
// id(E2), а коммит E1 позже коммита E2.
//
//	T2: изменил объект U, вставил E2, держится.
//	Силер: E2 не виден, печатает F (другой объект), рубеж уходит за id(E2).
//	T2 коммитит E2 — оно опоздавшее, id ниже рубежа.
//	T1: то же изменение того же U → E1, id дальше рубежа.
//	Силер печатает E1, затем проверка опоздавших печатает E2.
//
// Итог для объекта U: плотный порядок E1, E2 при порядке коммитов E2, E1.
// Значит применять журнал надо по id события, а плотный диапазон сторожит
// полноту (ADR-0023, поправка от 2026-11-11). Если этот тест покраснеет «в лучшую
// сторону» — значит опоздавшие перестали печататься позже соседей; тогда править
// надо ADR, а не тест.
func TestSealStragglerInvertsDenseOrder(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	// Событие про конкретный объект: kind 'retention' обязан назвать версию
	// (CHECK из 0027), объект здесь и играет версию.
	seedObjectEvent := func(object uuid.UUID, what string) int64 {
		t.Helper()
		var id int64
		if err := m.Pool.QueryRow(ctx,
			`INSERT INTO journal_events (tenant_id, kind, object_id, bucket_id, payload)
			 VALUES (1, 'retention', $1, 1, $2::jsonb) RETURNING id`,
			object, fmt.Sprintf(`{"what":%q}`, what)).Scan(&id); err != nil {
			t.Fatalf("событие про объект %s: %v", object, err)
		}
		return id
	}

	objectU, objectF := uuid.New(), uuid.New()
	lim := meta.SealLimits{MaxEvents: 10, MaxBytes: 1 << 20}

	// T2: изменение объекта U, событие E2, транзакция держится.
	slow, err := m.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = slow.Rollback(ctx) }() //nolint:errcheck // после commit откатывать нечего
	var e2 int64
	if err := slow.QueryRow(ctx,
		`INSERT INTO journal_events (tenant_id, kind, object_id, bucket_id, payload)
		 VALUES (1, 'retention', $1, 1, '{"what":"E2"}') RETURNING id`, objectU).Scan(&e2); err != nil {
		t.Fatal(err)
	}
	// F — событие другого объекта, закоммичено: силер его видит, E2 — ещё нет.
	f := seedObjectEvent(objectF, "F")

	first, err := m.SealJournalTenant(ctx, 1, lim)
	if err != nil {
		t.Fatal(err)
	}
	if first == nil || sealedIDs(t, m, first.ID)[0] != f {
		t.Fatalf("первый сегмент %+v, хотели одно событие %d", first, f)
	}
	if wm := sealWatermarkOf(t, m, 1); e2 >= wm {
		t.Fatalf("посев неверный: E2=%d не ниже рубежа %d", e2, wm)
	}

	if err := slow.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	e1 := seedObjectEvent(objectU, "E1")

	second, err := m.SealJournalTenant(ctx, 1, lim)
	if err != nil {
		t.Fatal(err)
	}
	if second == nil || sealedIDs(t, m, second.ID)[0] != e1 {
		t.Fatalf("второй сегмент %+v, хотели одно событие %d", second, e1)
	}
	third, err := m.SealJournalStragglers(ctx, 1, 100, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if third == nil || sealedIDs(t, m, third.ID)[0] != e2 {
		t.Fatalf("сегмент опоздавших %+v, хотели одно событие %d", third, e2)
	}

	// Порядок id = порядок коммитов для объекта U: E2 раньше E1.
	if !(e2 < f && f < e1) {
		t.Fatalf("посев неверный: id E2=%d, F=%d, E1=%d — хотели E2 < F < E1", e2, f, e1)
	}
	// Плотный порядок для того же объекта обратен: E1 получил номер 2, E2 — 3.
	if second.SeqFirst != 2 || third.SeqFirst != 3 {
		t.Fatalf("плотные номера: E1=%d, E2=%d, хотели 2 и 3", second.SeqFirst, third.SeqFirst)
	}

	segs, err := m.JournalSegments(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	// Полнота не пострадала: ряд 1..3 без дырок, незапечатанных нет.
	checkDense(t, segs, 3)
	if n := sealUnsealed(t, m, 1); n != 0 {
		t.Errorf("незапечатанных событий %d, хотели 0", n)
	}
	// Правило «заметить перестановку по каталогу сегментов»: сегмент продолжает
	// id-порядок предыдущего, если его event_id_first больше event_id_last
	// предыдущего. Здесь так у первой пары и не так у второй — по этому признаку
	// rebuild-meta и поймёт, что применять надо по id, а не по плотному номеру.
	if !(segs[0].EventIDLast < segs[1].EventIDFirst) {
		t.Errorf("сегменты 1 и 2 идут не в id-порядке: %d..%d, затем %d..%d",
			segs[0].EventIDFirst, segs[0].EventIDLast, segs[1].EventIDFirst, segs[1].EventIDLast)
	}
	if segs[2].EventIDFirst > segs[1].EventIDLast {
		t.Error("сегмент опоздавших неотличим от обычного по границам id: " +
			"перестановку плотного порядка будет нечем заметить")
	}
}

// TestSealStragglersCutByBytes — сегмент опоздавших режется по байтам так же,
// как обычный: без реза он вырос бы до всей истории тенанта, а окно проверки —
// именно вся история.
func TestSealStragglersCutByBytes(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	// Два опоздавших: id израсходованы, коммита нет. Размер заведомо больше
	// порога реза ниже, чтобы один сегмент двух таких не вместил.
	tx1, err := m.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx1.Rollback(ctx) }() //nolint:errcheck // после commit откатывать нечего
	tx2, err := m.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx2.Rollback(ctx) }() //nolint:errcheck // после commit откатывать нечего
	big := fmt.Sprintf(`{"pad":"%s"}`, strings.Repeat("x", 4096))
	for _, tx := range []pgx.Tx{tx1, tx2} {
		var id int64
		if err := tx.QueryRow(ctx,
			`INSERT INTO journal_events (tenant_id, kind, bucket_id, payload)
			 VALUES (1, 'bucket_config', 1, $1::jsonb) RETURNING id`, big).Scan(&id); err != nil {
			t.Fatal(err)
		}
	}

	// Рубеж задаёт обычный сегмент из одного маленького события.
	sealSeed(t, m, 1, 1)
	if _, err := m.SealJournalTenant(ctx, 1, meta.SealLimits{MaxEvents: 1, MaxBytes: 1 << 20}); err != nil {
		t.Fatal(err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	// Порог меньше веса одного события: рез обязан оставить ровно одно, а не
	// всё, что влезло бы по числу (sweep_batch здесь велик).
	seg, err := m.SealJournalStragglers(ctx, 1, 100, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if seg == nil || seg.EventCount != 1 {
		t.Fatalf("сегмент опоздавших %+v, хотели ровно одно событие при малом max_bytes", seg)
	}
	second, err := m.SealJournalStragglers(ctx, 1, 100, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if second == nil || second.EventCount != 1 {
		t.Fatalf("второй сегмент опоздавших %+v, хотели второе событие", second)
	}
	if n := sealUnsealed(t, m, 1); n != 0 {
		t.Errorf("незапечатанных событий %d, хотели 0", n)
	}
}

func TestSealConcurrentSealersDoNotCollide(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	ids := sealSeed(t, m, 1, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4)
	const workers = 3
	// Один «чужой» силер с другим id проверял бы только то, что силеры разные.
	// Гонка живёт между участниками, которые делают ровно одно и то же на одном
	// тенанте одновременно, — их и запускаем.
	var wg sync.WaitGroup
	type result struct {
		segs []meta.SealedSegment
		err  error
	}
	results := make([]result, workers)
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			for {
				seg, err := m.SealJournalTenant(ctx, 1,
					meta.SealLimits{MaxEvents: 2, MaxBytes: 1 << 20})
				if err != nil {
					results[i].err = err
					return
				}
				if seg == nil {
					return
				}
				results[i].segs = append(results[i].segs, *seg)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	for i, r := range results {
		if r.err != nil {
			t.Fatalf("силер %d: %v — advisory-лок обязан сериализовать запечатывание "+
				"тенанта, а не отдавать коллизию по UNIQUE", i, r.err)
		}
	}
	var all []meta.SealedSegment
	for _, r := range results {
		all = append(all, r.segs...)
	}
	if len(all) == 0 {
		t.Fatal("ни один силер ничего не запечатал")
	}
	// Суммарно запечатано ровно то, что посеяно: ни потерянных, ни удвоенных.
	total := 0
	bySeq := map[int64]bool{}
	for _, s := range all {
		total += s.EventCount
		for seq := s.SeqFirst; seq <= s.SeqLast; seq++ {
			if bySeq[seq] {
				t.Errorf("номер %d выдан дважды (сегмент %d)", seq, s.ID)
			}
			bySeq[seq] = true
		}
	}
	if total != len(ids) {
		t.Errorf("запечатано %d событий, посеяно %d", total, len(ids))
	}
	if len(bySeq) != len(ids) {
		t.Errorf("различных плотных номеров %d, событий %d", len(bySeq), len(ids))
	}
	if n := sealUnsealed(t, m, 1); n != 0 {
		t.Errorf("незапечатанных событий %d, хотели 0", n)
	}
	segs, err := m.JournalSegments(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	checkDense(t, segs, len(ids))
}

func TestSealRollsBackWholeTransaction(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	sealSeed(t, m, 1, 8, 8, 8)
	// Сегмент-призрак: первый плотный номер занят, рубеж нулевой — силер возьмётся
	// выдавать тот же номер и обязан упасть на UNIQUE (tenant_id, seq_first).
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO journal_segments (tenant_id, seq_first, seq_last, event_count, event_id_last, event_id_first)
		 VALUES (1, 1, 1, 1, 0, 0)`); err != nil {
		t.Fatalf("сегмент-призрак: %v", err)
	}

	if _, err := m.SealJournalTenant(ctx, 1, meta.SealLimits{MaxEvents: 10, MaxBytes: 1 << 20}); err == nil {
		t.Fatal("запечатывание прошло при занятом первом номере: UNIQUE (tenant_id, seq_first) " +
			"не сработал, и два сегмента получили бы один диапазон")
	}
	// Нумерация, сегмент и признаки — одна транзакция. Если бы UPDATE tenants
	// коммитился раньше, счётчик уехал бы вперёд без сегмента, и следующий проход
	// выдал бы диапазон с дыркой — а дырка в журнале означает потерянное событие.
	if seq := tenantJournalSeq(t, m, 1); seq != 0 {
		t.Errorf("journal_seq = %d после неудачного запечатывания, хотели 0: номера выданы без сегмента", seq)
	}
	if n := countRow(t, ctx, m, `SELECT count(*) FROM journal_segments`); n != 1 {
		t.Errorf("сегментов %d, хотели 1 (только призрак)", n)
	}
	if n := countRow(t, ctx, m, `SELECT count(*) FROM journal_segment_event`); n != 0 {
		t.Errorf("признаков %d после неудачного запечатывания, хотели 0", n)
	}
	if n := sealUnsealed(t, m, 1); n != 3 {
		t.Errorf("незапечатанных событий %d, хотели 3", n)
	}
}

func TestSealTenantsAreListedOnlyWithWork(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	if _, err := m.JournalTenantsToSeal(ctx, 0); err == nil {
		t.Error("список тенантов с батчем 0 прошёл: limit обязан быть >= 1")
	}
	if _, err := m.JournalTenantsWithStragglers(ctx, 0); err == nil {
		t.Error("список опоздавших с батчем 0 прошёл: limit обязан быть >= 1")
	}
	if _, err := m.SealJournalTenant(ctx, 1, meta.SealLimits{MaxEvents: 0, MaxBytes: 10}); err == nil {
		t.Error("запечатывание с MaxEvents=0 прошло: лимит обязан быть >= 1")
	}
	if _, err := m.SealJournalStragglers(ctx, 1, 0, 1<<20); err == nil {
		t.Error("проверка опоздавших с батчем 0 прошла: limit обязан быть >= 1")
	}
	if _, err := m.SealJournalStragglers(ctx, 1, 10, 0); err == nil {
		t.Error("проверка опоздавших с max_bytes=0 прошла: сегмент опоздавших " +
			"режется по байтам так же, как обычный")
	}

	other := sealTenantExtra(t, m)
	sealSeed(t, m, other, 8)

	ids, err := m.JournalTenantsToSeal(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if !sealListed(t, ids, other) {
		t.Errorf("тенант с событиями не в списке: %v", ids)
	}
	if sealListed(t, ids, 1) {
		t.Errorf("пустой тенант в списке запечатывания: %v", ids)
	}
	if _, err := m.JournalTenantsToSeal(ctx, 1); err != nil {
		t.Errorf("батч 1: %v", err)
	}

	if segs := sealAll(t, m, other, meta.SealLimits{MaxEvents: 10, MaxBytes: 1 << 20}); len(segs) != 1 {
		t.Fatalf("сегментов %d, хотели 1", len(segs))
	}
	ids, err = m.JournalTenantsToSeal(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if sealListed(t, ids, other) {
		t.Errorf("тенант без незапечатанных событий остался в списке: %v", ids)
	}
}

// TestSealKeepsRealMutationOrder — плотные номера идут в том же порядке, в каком
// события выдал путь изменения.
//
// Тест с прямыми вставками (TestSealSegmentsAreDenseAndIdempotent) этого не
// доказывает: там порядок id задаёт сам тест. Здесь события выставляет настоящий
// путь (создание бакета, retention, маркер удаления), и сверяется порядок,
// который увидит rebuild-meta: по сегментам, а внутри сегмента — по id.
func TestSealKeepsRealMutationOrder(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	mustLockBucket(t, m, "seal-real")
	v1 := insertVersion(t, m, "seal-real", "k1", versionOpts{current: true, nullVersion: true})
	v2 := insertVersion(t, m, "seal-real", "k2", versionOpts{current: true})
	if err := m.PutObjectRetention(ctx, testTenant, "seal-real", "k1", v1,
		"GOVERNANCE", time.Now().Add(24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := m.PutObjectRetention(ctx, testTenant, "seal-real", "k2", v2,
		"COMPLIANCE", time.Now().Add(48*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := m.DeleteObject(ctx, testTenant,
		meta.DeleteObjectInput{Bucket: "seal-real", Key: "k2"}); err != nil {
		t.Fatal(err)
	}
	want := journalCount(t, m, 1)
	if want < 4 {
		t.Fatalf("событий %d, хотели не меньше 4 — посев реальных изменений не сработал", want)
	}

	segs := sealAll(t, m, 1, meta.SealLimits{MaxEvents: 3, MaxBytes: 1 << 20})
	if len(segs) < 2 {
		t.Errorf("сегментов %d, хотели не меньше 2 при MaxEvents=3", len(segs))
	}
	checkDense(t, segs, int(want))

	// Порядок «как его прочитает rebuild-meta» = порядок сегментов по seq_first,
	// внутри сегмента — по id.
	var byDense, byID string
	if err := m.Pool.QueryRow(ctx,
		`SELECT string_agg(e.kind::text, ',' ORDER BY s.seq_first, e.id)
		   FROM journal_segment_event m
		   JOIN journal_events e ON e.id = m.event_id
		   JOIN journal_segments s ON s.id = m.segment_id
		  WHERE e.tenant_id = 1`).Scan(&byDense); err != nil {
		t.Fatal(err)
	}
	if err := m.Pool.QueryRow(ctx,
		`SELECT string_agg(kind::text, ',' ORDER BY id) FROM journal_events WHERE tenant_id = 1`).
		Scan(&byID); err != nil {
		t.Fatal(err)
	}
	if byDense != byID {
		t.Errorf("журнал прочитается по плотным номерам как %q, а по порядку выдачи id был %q",
			byDense, byID)
	}
	if n := strings.Count(byDense, ","); n != int(want)-1 {
		t.Errorf("в плотном порядке %d событий, выдано %d", n+1, want)
	}
}
