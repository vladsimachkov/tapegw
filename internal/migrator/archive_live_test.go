package migrator

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/mover"
	"github.com/vladsimachkov/tapegw/internal/mover/movertest"
	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// Сквозной тест: migrator против настоящего mover'а (TCP, mTLS, настоящий
// протокол), а не против fake'а. Именно так ошибка `op == from_op` и осталась
// бы незамеченной: fake принимал то, что настоящий mover отвергает.
//
// Две копии → два объекта в хранилище с разными oid и разными тегами, обе
// 'done' в БД, пачка 'archived', буфер освобождён.
func TestArchiveTwoCopiesThroughRealMover(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	env := movertest.New(t, 1<<20)
	env.Store.SetExtents("TST-LIVE")
	if _, err := m.Pool.Exec(ctx, `
INSERT INTO media (label, lifecycle, pool_id, generation, medium_type, free_bytes_cached)
VALUES ('TST-LIVE', 'active', 1, 'L8', 'L8', 100000) ON CONFLICT (label) DO NOTHING`); err != nil {
		t.Fatal(err)
	}
	cli := env.Dial(t)

	ids := seedObjects(t, m, "live", 2)
	a := &Archiver{
		Meta: m, Put: cli,
		Spec: "side:left", Format: 1,
		Specs:    []string{"side:left", "side:right"},
		SpecTags: map[string][]string{"side:left": {"side:left"}, "side:right": {"side:right"}},
		WorkerID: "live-host/mover-1", ClaimTTL: 30 * time.Second,
	}

	plan, err := a.ArchivePack(ctx, 1, itemsFor(ids), nil)
	if err != nil {
		t.Fatalf("архивация: %v", err)
	}

	if env.Store.Len() != 2 {
		t.Fatalf("объектов на «ленте»: %d, ждали 2 — копия не записана отдельным объектом", env.Store.Len())
	}
	if got := env.Store.Tags(ids[0]); len(got) > 0 {
		t.Errorf("у источника теги %v — их быть не должно", got)
	}

	var packID uuid.UUID
	if err := m.Pool.QueryRow(ctx, `SELECT id FROM packs ORDER BY created_at DESC LIMIT 1`).Scan(&packID); err != nil {
		t.Fatal(err)
	}
	rows, err := m.Pool.Query(ctx,
		`SELECT spec, phobos_oid, state FROM archive_copies WHERE pack_id = $1 ORDER BY spec`, packID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	type cp struct{ spec, oid, state string }
	var got []cp
	for rows.Next() {
		var c cp
		if err := rows.Scan(&c.spec, &c.oid, &c.state); err != nil {
			t.Fatal(err)
		}
		got = append(got, c)
	}
	if len(got) != 2 {
		t.Fatalf("копий в БД: %d, ждали 2", len(got))
	}
	for _, c := range got {
		if c.state != "done" {
			t.Errorf("копия %s в %s, ждали done", c.spec, c.state)
		}
		id, err := uuid.Parse(c.oid)
		if err != nil {
			t.Fatalf("oid копии не UUID (%s): хранилище принимает только op как oid", c.oid)
		}
		if !env.Store.Has(id) {
			t.Errorf("копии %s (oid %s) на ленте нет", c.spec, c.oid)
		}
	}
	// Первая копия — сам объект пачки, её oid равен packID: повтор бьёт в тот
	// же объект. Требование «свой oid» про то, что копии различаются между
	// собой: совпадут — вторая запись даст EEXIST и «готовую» копию без записи.
	if got[0].oid == got[1].oid {
		t.Fatalf("oid копий совпали: %s", got[0].oid)
	}
	if got[1].oid == packID.String() {
		t.Errorf("вторая копия записана под oid первой: %s", got[1].oid)
	}
	if tags := env.Store.Tags(mustUUID(t, got[1].oid)); len(tags) != 1 || tags[0] != "side:right" {
		t.Errorf("теги копии: %v — копии не разведены тегами", tags)
	}

	var st string
	if err := m.Pool.QueryRow(ctx, `SELECT state::text FROM packs WHERE id = $1`, packID).Scan(&st); err != nil {
		t.Fatal(err)
	}
	if st != "archived" {
		t.Errorf("пачка в %s, ждали archived", st)
	}

	// Буфер освобождён: попытка взять ещё копию из него должна быть отказом,
	// а не успешной записью «из воздуха».
	extra := uuid.New()
	// Сумма и размер — настоящие, как у пачки. Иначе отказ придёт по чужой
	// сумме, а не по освобождённому буферу, и проверка станет пустой: ровно
	// так мутация «никогда не освобождать» и прошла первый раз.
	res, err := cli.Put(mover.PutIn{Op: extra, ID: extra, Size: plan.Size, SHA256: plan.SHA256, FromOp: packID})
	if err != nil {
		t.Fatal(err)
	}
	if res.Code == mover.CodeOK {
		t.Error("буфер не освобождён: копия после архивации записалась из висящих данных")
	}
}

func mustUUID(t *testing.T, s string) uuid.UUID {
	t.Helper()
	id, err := uuid.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
