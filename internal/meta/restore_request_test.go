package meta_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// Запрос восстановления (S4). Проверяются обещания из критериев ревью:
// живой копии очередь не ставят; 409 ничего не меняет; продление только
// растёт (GREATEST); переходы идемпотентны (правило 7).

func requestRestore(t *testing.T, m *meta.Meta, id uuid.UUID, days int, tier string) meta.RestoreOutcome {
	t.Helper()
	out, err := m.RequestRestore(context.Background(), meta.NewTenant(2), id, days, tier)
	if err != nil {
		t.Fatalf("RequestRestore(%d,%s): %v", days, tier, err)
	}
	return out
}

type restoreRow struct {
	state     string
	tier      string
	days      int
	priority  int
	expiresAt *time.Time
}

func fetchRestore(t *testing.T, m *meta.Meta, id uuid.UUID) (restoreRow, bool) {
	t.Helper()
	var r restoreRow
	err := m.Pool.QueryRow(context.Background(),
		`SELECT state, tier, days, priority, expires_at FROM restores WHERE object_id = $1`, id).
		Scan(&r.state, &r.tier, &r.days, &r.priority, &r.expiresAt)
	if err != nil {
		return restoreRow{}, false
	}
	return r, true
}

func onlineUntil(t *testing.T, m *meta.Meta, id uuid.UUID) time.Time {
	t.Helper()
	var v time.Time
	if err := m.Pool.QueryRow(context.Background(),
		`SELECT online_until FROM objects WHERE id = $1`, id).Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

// Архивный объект без копии: очередь создана, приоритет по tier, срок
// продлён; повтор — InProgress без побочных эффектов.
func TestRequestRestoreQueues(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)

	id := tenantWithObjects(t, m, 2, "rq", 10, 1)[0]
	setEvictState(t, m, id, "archived", false, "-1 hour")
	before := onlineUntil(t, m, id)

	if out := requestRestore(t, m, id, 7, "Expedited"); out != meta.RestoreQueued {
		t.Fatalf("исход %v, ждали Queued", out)
	}
	r, ok := fetchRestore(t, m, id)
	if !ok {
		t.Fatal("строка restores не создана")
	}
	if r.state != "requested" || r.tier != "Expedited" || r.days != 7 || r.priority != 0 {
		t.Errorf("строка %+v, ждали requested/Expedited/7/0", r)
	}
	if u := onlineUntil(t, m, id); !u.After(before.Add(6 * 24 * time.Hour)) {
		t.Errorf("online_until %v не продлён до now()+7d (был %v)", u, before)
	}

	// Повтор: 409, и ничего не изменилось — ни строка, ни срок.
	until := onlineUntil(t, m, id)
	if out := requestRestore(t, m, id, 30, "Standard"); out != meta.RestoreInProgress {
		t.Errorf("повтор вернул %v, ждали InProgress", out)
	}
	if r2, _ := fetchRestore(t, m, id); r2.days != 7 || r2.tier != "Expedited" {
		t.Errorf("отказ 409 изменил строку: %+v", r2)
	}
	if u := onlineUntil(t, m, id); !u.Equal(until) {
		t.Errorf("отказ 409 продлил срок: %v → %v", until, u)
	}
}

// Живая копия в landing: очередь НЕ создаётся, только продление (критерий
// ревью S4). GREATEST: больший существующий срок не укорачивается.
func TestRequestRestoreLiveCopyExtendsOnly(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)

	id := tenantWithObjects(t, m, 2, "rl", 10, 1)[0]
	setEvictState(t, m, id, "archived", true, "30 days")

	if out := requestRestore(t, m, id, 7, "Standard"); out != meta.RestoreExtended {
		t.Fatalf("исход %v, ждали Extended", out)
	}
	if _, ok := fetchRestore(t, m, id); ok {
		t.Error("живая копия поставлена в очередь recaller'а — так нельзя")
	}
	if u := onlineUntil(t, m, id); !u.After(time.Now().Add(29 * 24 * time.Hour)) {
		t.Errorf("срок укорочен: %v, ждали прежние ~30d", u)
	}

	// И наоборот: из прошлого продлевается, а не остаётся в прошлом.
	setEvictState(t, m, id, "archived", true, "-1 hour")
	if out := requestRestore(t, m, id, 7, "Standard"); out != meta.RestoreExtended {
		t.Fatalf("исход %v, ждали Extended", out)
	}
	if u := onlineUntil(t, m, id); !u.After(time.Now().Add(6 * 24 * time.Hour)) {
		t.Errorf("срок из прошлого не продлён: %v", u)
	}
}

// pending/archiving — не архив: отказа от restore достаточно, очередь не
// создаётся, срок не трогается.
func TestRequestRestoreNotArchived(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)

	ids := tenantWithObjects(t, m, 2, "rn", 10, 2)
	pending, archiving := ids[0], ids[1]
	setEvictState(t, m, pending, "pending", true, "1 day")
	setEvictState(t, m, archiving, "archiving", true, "1 day")

	for _, id := range []uuid.UUID{pending, archiving} {
		before := onlineUntil(t, m, id)
		if out := requestRestore(t, m, id, 7, "Standard"); out != meta.RestoreNotArchived {
			t.Errorf("%v: исход %v, ждали NotArchived", id, out)
		}
		if _, ok := fetchRestore(t, m, id); ok {
			t.Errorf("%v: очередь создана для не-архива", id)
		}
		if u := onlineUntil(t, m, id); !u.Equal(before) {
			t.Errorf("%v: срок изменён при отказе: %v → %v", id, before, u)
		}
	}
}

// Выданный и не истёкший restore: продление expires_at и online_until,
// GREATEST в обе стороны.
func TestRequestRestoreExtendsRestored(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	id := tenantWithObjects(t, m, 2, "re", 10, 1)[0]
	setEvictState(t, m, id, "archived", false, "30 days")
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO restores (object_id, tenant_id, state, tier, days, requested_at, restored_at, expires_at)
		 VALUES ($1, 2, 'restored', 'Standard', 30, now(), now(), now() + interval '30 days')`, id); err != nil {
		t.Fatal(err)
	}

	if out := requestRestore(t, m, id, 7, "Standard"); out != meta.RestoreExtended {
		t.Fatalf("исход %v, ждали Extended", out)
	}
	r, _ := fetchRestore(t, m, id)
	if r.state != "restored" {
		t.Errorf("состояние %v, ждали restored", r.state)
	}
	if r.expiresAt == nil || !r.expiresAt.After(time.Now().Add(29*24*time.Hour)) {
		t.Errorf("expires_at укорочен семидневным запросом: %v", r.expiresAt)
	}

	// Истёкший restored (janitor ещё не убрал) — новый запрос, а не продление.
	if _, err := m.Pool.Exec(ctx,
		`UPDATE restores SET expires_at = now() - interval '1 day' WHERE object_id = $1`, id); err != nil {
		t.Fatal(err)
	}
	if out := requestRestore(t, m, id, 7, "Bulk"); out != meta.RestoreQueued {
		t.Fatalf("истёкший restore: исход %v, ждали Queued", out)
	}
	if r, _ := fetchRestore(t, m, id); r.state != "requested" || r.expiresAt != nil {
		t.Errorf("перезапрос не сбросил строку в requested: %+v", r)
	}
}

// failed — путь повторного запроса после исчерпания попыток recaller'а.
func TestRequestRestoreRequeuesFailed(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	id := tenantWithObjects(t, m, 2, "rf", 10, 1)[0]
	setEvictState(t, m, id, "archived", false, "-1 hour")
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO restores (object_id, tenant_id, state, tier, days, requested_at, attempts, last_error)
		 VALUES ($1, 2, 'failed', 'Standard', 7, now(), 5, 'phobos lay down')`, id); err != nil {
		t.Fatal(err)
	}

	if out := requestRestore(t, m, id, 14, "Expedited"); out != meta.RestoreQueued {
		t.Fatalf("исход %v, ждали Queued", out)
	}
	var attempts int
	var lastErr *string
	if err := m.Pool.QueryRow(ctx,
		`SELECT attempts, last_error FROM restores WHERE object_id = $1`, id).
		Scan(&attempts, &lastErr); err != nil {
		t.Fatal(err)
	}
	if attempts != 0 || lastErr != nil {
		t.Errorf("перезапрос не обнулил ретраи: attempts=%d last_err=%v", attempts, lastErr)
	}
}

// Чужой тенант — ErrNotFound: предикат тенанта в выборке объекта обязателен,
// как и везде (ADR-0017).
func TestRequestRestoreForeignTenant(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)

	id := tenantWithObjects(t, m, 2, "rx", 10, 1)[0]
	setEvictState(t, m, id, "archived", false, "-1 hour")

	if _, err := m.RequestRestore(context.Background(), meta.NewTenant(3), id, 7, "Standard"); err != meta.ErrNotFound {
		t.Errorf("чужой тенант: %v, ждали ErrNotFound", err)
	}
	if _, ok := fetchRestore(t, m, id); ok {
		t.Error("чужой тенант создал строку restores")
	}
}

// days < 1 — внутренняя ошибка конфигурации вызывающего, не S3-код:
// бэкенд обязан отсечь раньше, но и meta не принимает.
func TestRequestRestoreRejectsBadDays(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)

	id := tenantWithObjects(t, m, 2, "rd", 10, 1)[0]
	if _, err := m.RequestRestore(context.Background(), meta.NewTenant(2), id, 0, "Standard"); err == nil {
		t.Error("days=0 принят")
	}
}
