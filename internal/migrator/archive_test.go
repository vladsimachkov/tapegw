package migrator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/mover"
	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// lockNotAvailable — SQLSTATE lock_not_available. Именно её получает
// FOR UPDATE NOWAIT, когда строку держит чужая транзакция.
const lockNotAvailable = "55P03"

// probeMover отвечает как mover по протоколу и заодно проверяет правило 13:
// в момент put он пытается взять блокировку строки объекта БЕЗ ожидания. Если
// migrator держит открытую транзакцию, NOWAIT падает сразу, а не висит до
// таймаута.
type probeMover struct {
	// target — id объекта, который проверяем в момент put.
	target uuid.UUID
	m      *meta.Meta

	// SQLState — что вернул NOWAIT: пусто, когда блокировки нет, 55P03 когда
	// есть. Тест проверяет конкретно это поле, а не «что-то упало».
	SQLState string
	Detail   string
	accept   bool
	medium   string // что mover сообщит в extents
}

func (p *probeMover) Put(in mover.PutIn) (*mover.Result, error) {
	body, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(body)
	if hex.EncodeToString(sum[:]) != in.SHA256 || int64(len(body)) != in.Size {
		p.accept = false
		return &mover.Result{Op: in.Op, Code: mover.CodeChecksumMismatch}, nil
	}

	// Проверка идёт на отдельном соединении: со своего же транзакцию не
	// заблокируешь, и тест молчал бы зелёным.
	var got uuid.UUID
	err = p.m.Pool.QueryRow(context.Background(),
		`SELECT id FROM objects WHERE id = $1 FOR UPDATE NOWAIT`, p.target).Scan(&got)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		var pe *pgconn.PgError
		if errors.As(err, &pe) {
			p.SQLState = pe.SQLState()
			p.Detail = pe.Message
		} else {
			p.SQLState = "other"
			p.Detail = err.Error()
		}
		return nil, fmt.Errorf("правило 13 нарушено: SQLSTATE %s (%s) — во время вызова mover открыта транзакция",
			p.SQLState, p.Detail)
	}

	p.accept = true
	return &mover.Result{Op: in.Op, Code: mover.CodeOK, Version: 1, Extents: []string{p.medium}}, nil
}

// seedObjects заводит тенанта, бакет и n pending-объектов.
func seedObjects(t *testing.T, m *meta.Meta, bucket string, n int) []uuid.UUID {
	t.Helper()
	ctx := context.Background()

	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO tenants (id, name, pool_id) VALUES (2, 'mig', 1) ON CONFLICT (id) DO NOTHING`); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO buckets (name, tenant_id, owner, acl, versioning)
		  VALUES ($1, 2, 'o', '{}'::jsonb, 'unversioned')`, bucket); err != nil {
		t.Fatal(err)
	}
	var bucketID int64
	if err := m.Pool.QueryRow(ctx, `SELECT id FROM buckets WHERE name = $1`, bucket).Scan(&bucketID); err != nil {
		t.Fatal(err)
	}

	var ids []uuid.UUID
	for i := 0; i < n; i++ {
		id := uuid.New()
		if _, err := m.Pool.Exec(ctx,
			`INSERT INTO objects (id, bucket_id, key, size, etag, storage_class, state,
			                      landing, online_until, tenant_id, pool_id, is_current,
			                      wrapped_dek)
			  VALUES ($1,$2,$3,10,'etag','GLACIER','pending',true,
			          now() + interval '1 day', 2, 1, true, 'x'::bytea)`,
			id, bucketID, fmt.Sprintf("k%d", i)); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	return ids
}

func itemsFor(ids []uuid.UUID) []Item {
	var out []Item
	for i, id := range ids {
		body := []byte(fmt.Sprintf("содержимое-%d", i))
		out = append(out, Item{
			Version:    id,
			Tenant:     2,
			VersionSeq: uint64(i + 1),
			PlainLen:   uint64(len(body)),
			KekVersion: 1,
			WrappedDEK: []byte("wrapped-dek"),
			CipherLen:  uint64(len(body)),
			Open:       func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil },
		})
	}
	return out
}

func archiver(t *testing.T, m *meta.Meta, target uuid.UUID) (*Archiver, *probeMover) {
	t.Helper()
	pm := &probeMover{m: m, target: target, medium: "TST-MIG"}
	if _, err := m.Pool.Exec(context.Background(), `
INSERT INTO media (label, lifecycle, pool_id, generation, medium_type, free_bytes_cached)
VALUES ('TST-MIG', 'active', 1, 'L8', 'L8', 1000) ON CONFLICT (label) DO NOTHING`); err != nil {
		t.Fatal(err)
	}
	return &Archiver{Meta: m, Put: pm, Spec: "pool:archive", Format: 1}, pm
}

// С8: во время вызова mover транзакция Postgres не открыта.
//
// Тест проверяет конкретно SQLSTATE: если бы он просто ждал ошибку, мутация
// «перенести вызов mover внутрь транзакции» упала бы где угодно — например на
// откате перехода, — и доказывала бы не то.
func TestNoTxnOpenDuringMoverCall(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	ids := seedObjects(t, m, "c8", 3)
	a, pm := archiver(t, m, ids[0])

	if _, err := a.ArchivePack(ctx, 1, itemsFor(ids), nil); err != nil {
		t.Fatalf("ArchivePack: %v", err)
	}
	// Проверяем конкретно SQLSTATE, а не «что-то упало»: иначе мутация
	// «перенести вызов mover внутрь транзакции» могла бы упасть по другой
	// причине и доказать не то.
	if pm.SQLState == lockNotAvailable {
		t.Errorf("правило 13 нарушено: NOWAIT на строку объекта вернул %s (%s) — во время put транзакция открыта",
			pm.SQLState, pm.Detail)
	}
	if pm.SQLState != "" && pm.SQLState != lockNotAvailable {
		t.Errorf("NOWAIT вернул неожиданный %s (%s)", pm.SQLState, pm.Detail)
	}
	if !pm.accept {
		t.Error("mover не принял пачку")
	}

	// И состояние подтверждено: объекты действительно прошли archiving.
	var state string
	if err := m.Pool.QueryRow(ctx, `SELECT state::text FROM objects WHERE id = $1`, ids[0]).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "archived" {
		t.Errorf("объект в состоянии %s, ждали archived", state)
	}
}

// С7: ожидаемая сумма потока сохраняется в archive_copies при первой записи.
// Без неё reconciler нечем проверить чтение с ленты, а задним числом её не
// восстановить: данные уже на ленте.
func TestExpectedSHAStoredOnFirstWrite(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	ids := seedObjects(t, m, "c7", 2)
	a, _ := archiver(t, m, ids[0])

	plan, err := a.ArchivePack(ctx, 1, itemsFor(ids), nil)
	if err != nil {
		t.Fatalf("ArchivePack: %v", err)
	}

	var got []byte
	var spec string
	var state string
	err = m.Pool.QueryRow(ctx, `
SELECT c.sha256, c.spec, c.state::text FROM archive_copies c
JOIN packs p ON p.id = c.pack_id ORDER BY c.created_at DESC LIMIT 1`).Scan(&got, &spec, &state)
	if err != nil {
		t.Fatalf("копия не записана: %v", err)
	}
	want, _ := hex.DecodeString(plan.SHA256)
	if !bytes.Equal(got, want) {
		t.Errorf("сумма копии %x, ждали %x — reconciler сверял бы не то", got, want)
	}
	if spec != "pool:archive" || state != "done" {
		t.Errorf("копия: %s/%s, ждали pool:archive/done", spec, state)
	}
	if len(got) != 32 {
		t.Errorf("длина суммы %d, ждали 32", len(got))
	}
}

// Отказ mover не оставляет объектов в archiving: иначе они зависли бы там
// навсегда, а повторный батч их не возьмёт.
func TestRefusedPackReturnsObjectsToPending(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	ids := seedObjects(t, m, "refuse", 2)
	items := itemsFor(ids)
	// Подменённый участник: сумма второго прохода не совпадёт с объявленной,
	// и migrator откажется сам, не доходя до mover.
	items[0].Open = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader([]byte("иная длина"))), nil
	}

	a, _ := archiver(t, m, ids[0])
	if _, err := a.ArchivePack(ctx, 1, items, nil); err == nil {
		t.Fatal("пачка с подменой прошла")
	}

	var state string
	if err := m.Pool.QueryRow(ctx, `SELECT state::text FROM objects WHERE id = $1`, ids[0]).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "pending" {
		t.Errorf("объект после отказа в %s, ждали pending — в archiving его не возьмёт ни один повтор", state)
	}
}
