package worker_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/vladsimachkov/tapegw/internal/config"
	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/testdb"
	"github.com/vladsimachkov/tapegw/internal/worker"
)

// Силер журнала (Ж2) — воркер без Phobos и без диска: ему нужна только БД,
// поэтому и стенд здесь легче, чем у migrator'а: настоящая БД, конфигурация из
// config.Default(), никаких movertest.
//
// Проверяется именно связка «конфиг → тик → meta», а не сама учётная часть:
// её проверяет internal/meta/seal_test.go. Здесь же — что воркер берёт лимиты
// из конфига, что обычный проход и проверка опоздавших действительно запускаются
// по своим таймерам, и что остановка ждёт конец прохода.

func sealerLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// seedJournal — n событий тенанта 1 напрямую; возвращает id по порядку вставки.
func seedJournal(t *testing.T, m *meta.Meta, n int) []int64 {
	t.Helper()
	ctx := context.Background()
	ids := make([]int64, 0, n)
	for i := 0; i < n; i++ {
		var id int64
		if err := m.Pool.QueryRow(ctx,
			`INSERT INTO journal_events (tenant_id, kind, bucket_id, payload)
			 VALUES (1, 'bucket_config', 1, $1::jsonb) RETURNING id`,
			fmt.Sprintf(`{"pad":"%d"}`, i)).Scan(&id); err != nil {
			t.Fatalf("событие %d: %v", i, err)
		}
		ids = append(ids, id)
	}
	return ids
}

func journalUnsealed(t *testing.T, m *meta.Meta) int64 {
	t.Helper()
	n, err := m.JournalUnsealedCount(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func journalSegmentCount(t *testing.T, m *meta.Meta) int64 {
	t.Helper()
	var n int64
	if err := m.Pool.QueryRow(context.Background(),
		`SELECT count(*) FROM journal_segments WHERE tenant_id = 1`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestSealerSealsJournalOnTick(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := config.Default()
	cfg.Journal.Sealer.Interval = 50 * time.Millisecond
	cfg.Journal.Sealer.SweepInterval = time.Hour // проверка опоздавших здесь не нужна
	cfg.Journal.Segment.MaxEvents = 2

	seedJournal(t, m, 5)

	seal := worker.StartSealer(ctx, worker.SealerDeps{Cfg: cfg, Meta: m, Log: sealerLog()})
	waitFor(t, "события запечатаны", func() bool { return journalUnsealed(t, m) == 0 })

	// 5 событий по 2 → три сегмента; заодно это проверка, что MaxEvents из конфига
	// дошёл до meta (иначе был бы один сегмент на пять событий).
	if n := journalSegmentCount(t, m); n != 3 {
		t.Errorf("сегментов %d, хотели 3 — лимит сегмента из конфига не применён", n)
	}
	segs, err := m.JournalSegments(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if segs[len(segs)-1].SeqLast != 5 {
		t.Errorf("плотная нумерация дошла до %d, хотели 5", segs[len(segs)-1].SeqLast)
	}

	// Пустой проход не должен ничего добавлять: «запечатывать нечего» — норма.
	time.Sleep(200 * time.Millisecond)
	if n := journalSegmentCount(t, m); n != 3 {
		t.Errorf("сегментов стало %d после пустых проходов, хотели 3", n)
	}

	// Новые события — новые сегменты тем же циклом.
	seedJournal(t, m, 1)
	waitFor(t, "новое событие запечатано", func() bool { return journalUnsealed(t, m) == 0 })
	if n := journalSegmentCount(t, m); n != 4 {
		t.Errorf("сегментов %d, хотели 4", n)
	}

	cancel()
	if !seal.Wait(5 * time.Second) {
		t.Error("Wait вернул false: проход не завершился после остановки")
	}
}

// TestSealerSweepSealsStraggler — проверка опоздавших запускается по своему
// таймеру. Тот же случай, что в internal/meta/seal_test.go, но собранный живым
// воркером: событие коммитится уже после того, как силер запечатал рубеж за его
// id, и обычный проход его не найдёт.
func TestSealerSweepSealsStraggler(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// id выдаётся при вставке, поэтому «опоздавшее» — то, чья транзакция открыта
	// раньше и закрыта позже запечатывания.
	slow, err := m.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = slow.Rollback(ctx) }() //nolint:errcheck // после commit нечего
	var slowID int64
	if err := slow.QueryRow(ctx,
		`INSERT INTO journal_events (tenant_id, kind, bucket_id, payload)
		 VALUES (1, 'bucket_config', 1, '{"slow":true}') RETURNING id`).Scan(&slowID); err != nil {
		t.Fatal(err)
	}
	fast := seedJournal(t, m, 1)
	if slowID >= fast[0] {
		t.Fatalf("посев неверный: опоздавшее %d не ниже будущего рубежа %d", slowID, fast[0])
	}

	cfg := config.Default()
	cfg.Journal.Sealer.Interval = time.Hour // обычный проход здесь не при чём
	cfg.Journal.Sealer.SweepInterval = 50 * time.Millisecond
	cfg.Journal.Sealer.SweepBatch = 100

	seal := worker.StartSealer(ctx, worker.SealerDeps{Cfg: cfg, Meta: m, Log: sealerLog()})
	// Первый проход — сразу при старте: он запечатывает fast и ставит рубеж.
	// Ждём по сегменту, а не по числу незапечатанных: опоздавшее событие до
	// своего коммита невидимо, и «незапечатанных 0» было бы и до прохода.
	waitFor(t, "обычный ход запечатал видимое событие",
		func() bool { return journalSegmentCount(t, m) == 1 })

	if err := slow.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	// Дальше всё делает таймер проверки опоздавших: обычный проход (interval —
	// час) этого события не увидит, у него id ниже рубежа.
	waitFor(t, "проверка опоздавших запечатала событие",
		func() bool { return journalUnsealed(t, m) == 0 })
	if n := journalSegmentCount(t, m); n != 2 {
		t.Errorf("сегментов %d, хотели 2", n)
	}
	segs, err := m.JournalSegments(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if segs[1].SeqFirst != 2 || segs[1].SeqLast != 2 {
		t.Errorf("сегмент опоздавшего: %d..%d, хотели 2..2 — номер выдан не в продолжение",
			segs[1].SeqFirst, segs[1].SeqLast)
	}
	cancel()
	if !seal.Wait(5 * time.Second) {
		t.Error("Wait вернул false: силер не завершился после остановки")
	}
}
