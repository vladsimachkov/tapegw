package meta_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// Выборка на вытеснение (ADR-0004, S3). Проверяются не «запрос что-то вернул»,
// а обещания: защита по состоянию, а не по дате; граница строго после
// online_until; два janitor не дерутся за один объект.

func setEvictState(t *testing.T, m *meta.Meta, id uuid.UUID, state string, landing bool, until string) {
	t.Helper()
	// until — SQL-выражение (интервал от now()), чтобы границу задавали те же
	// часы БД, что и сравнение в выборке.
	_, err := m.Pool.Exec(context.Background(),
		`UPDATE objects SET state = $2::obj_state, landing = $3, online_until = now() + $4::interval
		  WHERE id = $1`, id, state, landing, until)
	if err != nil {
		t.Fatal(err)
	}
}

func claimIDs(t *testing.T, m *meta.Meta, limit int) map[uuid.UUID]bool {
	t.Helper()
	got, err := m.ClaimEvictBatch(context.Background(), limit)
	if err != nil {
		t.Fatalf("ClaimEvictBatch: %v", err)
	}
	set := map[uuid.UUID]bool{}
	for _, c := range got {
		set[c.ID] = true
	}
	return set
}

func landingFlag(t *testing.T, m *meta.Meta, id uuid.UUID) bool {
	t.Helper()
	var v bool
	if err := m.Pool.QueryRow(context.Background(),
		`SELECT landing FROM objects WHERE id = $1`, id).Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

// Вытесняется только archived с истёкшим сроком; переход идемпотенен — флаг
// снят, повтор ничего не берёт.
func TestClaimEvictBatchGuards(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)

	ids := tenantWithObjects(t, m, 2, "evict", 10, 5)
	due := ids[0]       // archived, срок вышел — единственный кандидат
	pending := ids[1]   // pending со сроком в прошлом — не трогать
	archiving := ids[2] // archiving со сроком в прошлом — не трогать
	future := ids[3]    // archived, срок не наступил
	gone := ids[4]      // archived, срок вышел, но флаг уже снят

	setEvictState(t, m, due, "archived", true, "-1 hour")
	setEvictState(t, m, pending, "pending", true, "-1 hour")
	setEvictState(t, m, archiving, "archiving", true, "-1 hour")
	setEvictState(t, m, future, "archived", true, "1 hour")
	setEvictState(t, m, gone, "archived", false, "-1 hour")

	got := claimIDs(t, m, 10)
	if len(got) != 1 || !got[due] {
		t.Fatalf("выбрано %v, ожидался только %v: защита обязана быть по состоянию И по дате", got, due)
	}
	if landingFlag(t, m, due) {
		t.Error("флаг landing не снят — выборка обязана снимать его сама")
	}
	if !landingFlag(t, m, pending) || !landingFlag(t, m, archiving) || !landingFlag(t, m, future) {
		t.Error("чужой объект задет выборкой")
	}

	// Повтор: кандидат уже не кандидат (флаг снят) — ноль строк, «опередили».
	if again := claimIDs(t, m, 10); len(again) != 0 {
		t.Errorf("повторная выборка вернула %v, ждали ноль", again)
	}
}

// Граница: объект ровно в момент online_until ещё не вытесняется — раньше
// обещанного срока файл обязан жить. now() внутри транзакции постоянна,
// поэтому сравнение «с теми же часами» возможно только в ней.
func TestClaimEvictBatchBoundary(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	id := tenantWithObjects(t, m, 2, "bound", 10, 1)[0]

	tx, err := m.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // откатываем намеренно
	if _, err := tx.Exec(ctx,
		`UPDATE objects SET state = 'archived', landing = true, online_until = now() WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	got, err := m.ClaimEvictBatchTx(ctx, tx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("объект ровно в момент online_until вытеснен: %v — сравнение обязано быть строгим (<)", got)
	}
}

// Два janitor (свойство про параллельных участников): первый держит кандидата
// в своей транзакции, второй обязан пройти мимо (SKIP LOCKED), а не полезть
// в тот же файл.
func TestClaimEvictSkipLocked(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	id := tenantWithObjects(t, m, 2, "race", 10, 1)[0]
	setEvictState(t, m, id, "archived", true, "-1 hour")

	// Первый janitor держит строку.
	tx, err := m.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var held uuid.UUID
	if err := tx.QueryRow(ctx,
		`SELECT id FROM objects WHERE id = $1 FOR UPDATE`, id).Scan(&held); err != nil {
		t.Fatal(err)
	}

	// Второй janitor обязан пройти мимо, а не ждать: без SKIP LOCKED запрос
	// встал бы на чужую блокировку навсегда. Таймбокс здесь — чтобы мутация
	// «убрать SKIP LOCKED» ловилась отказом за секунды, а не зависанием
	// прогона до общего таймаута.
	type claimResult struct {
		got map[uuid.UUID]bool
	}
	ch := make(chan claimResult, 1)
	go func() { ch <- claimResult{claimIDs(t, m, 10)} }()
	select {
	case r := <-ch:
		if len(r.got) != 0 {
			tx.Rollback(ctx) //nolint:errcheck
			t.Fatalf("второй janitor забрал занятый объект: %v", r.got)
		}
	case <-time.After(3 * time.Second):
		tx.Rollback(ctx) //nolint:errcheck
		t.Fatal("второй janitor заблокировался на занятом объекте вместо SKIP LOCKED")
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	// Отпустил — взят в следующем проходе.
	if got := claimIDs(t, m, 10); !got[id] {
		t.Errorf("освобождённый кандидат не взят: %v", got)
	}
}
