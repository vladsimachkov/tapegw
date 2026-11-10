package meta_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// Журнал метаданных (ADR-0023).
//
// Главный тест здесь — «упасть после коммита изменения», и устроен он наоборот:
// яд ставится на саму вставку события, а проверяем мы, что **изменение** не
// состоялось. Если бы событие писалось отдельным шагом после коммита,
// отравленная вставка уронила бы только её, а блокировка осталась бы в БД без
// события — ровно та потеря, ради которой журнала и не было.

// poisonJournal запрещает вставку в journal_events и возвращает снятие яда.
//
// Яд на вставке, а не на изменении: только так проверка различает «событие в
// той же транзакции» и «событие после коммита».
func poisonJournal(t *testing.T, m *meta.Meta) func() {
	t.Helper()
	ctx := context.Background()
	if _, err := m.Pool.Exec(ctx, `
		CREATE FUNCTION tapegw_test_journal_poison() RETURNS trigger
		LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'вставка в журнал запрещена тестом'; END $$`); err != nil {
		t.Fatalf("функция яда: %v", err)
	}
	if _, err := m.Pool.Exec(ctx, `
		CREATE TRIGGER journal_events_poison BEFORE INSERT ON journal_events
		  FOR EACH ROW EXECUTE FUNCTION tapegw_test_journal_poison()`); err != nil {
		t.Fatalf("триггер яда: %v", err)
	}
	off := func() {
		t.Helper()
		if _, err := m.Pool.Exec(ctx,
			`DROP TRIGGER IF EXISTS journal_events_poison ON journal_events`); err != nil {
			t.Fatalf("снять яд: %v", err)
		}
		if _, err := m.Pool.Exec(ctx,
			`DROP FUNCTION IF EXISTS tapegw_test_journal_poison()`); err != nil {
			t.Fatalf("снять функцию яда: %v", err)
		}
	}
	t.Cleanup(func() {
		_, _ = m.Pool.Exec(context.Background(),
			`DROP TRIGGER IF EXISTS journal_events_poison ON journal_events`)
		_, _ = m.Pool.Exec(context.Background(),
			`DROP FUNCTION IF EXISTS tapegw_test_journal_poison()`)
	})
	return off
}

// journalEvents — все события тенанта по порядку номера.
func journalEvents(t *testing.T, m *meta.Meta, tenant int64) []meta.JournalEvent {
	t.Helper()
	out, err := m.JournalEventsAfter(context.Background(), tenant, 0, 1000)
	if err != nil {
		t.Fatalf("события журнала: %v", err)
	}
	return out
}

// journalSeq — счётчик на строке тенанта.
func journalSeq(t *testing.T, m *meta.Meta, tenant int64) int64 {
	t.Helper()
	seq, err := m.JournalSeq(context.Background(), tenant)
	if err != nil {
		t.Fatalf("journal_seq тенанта %d: %v", tenant, err)
	}
	return seq
}

func mustLockBucket(t *testing.T, m *meta.Meta, name string) {
	t.Helper()
	ctx := context.Background()
	mustBucket(t, m, name)
	// Напрямую: SetObjectLockConfig требует включённое версионирование, а тесту
	// нужен бакет под блокировку без проверок переходов.
	if _, err := m.Pool.Exec(ctx,
		`UPDATE buckets SET versioning = 'enabled', object_lock_enabled = true
		  WHERE name = $1`, name); err != nil {
		t.Fatalf("включить lock у %s: %v", name, err)
	}
}

// jCase — одно изменение, которое обязано оставить событие.
type jCase struct {
	name string
	kind string
	// fix готовит состояние до изменения.
	fix func(t *testing.T, m *meta.Meta)
	// act выполняет само изменение.
	act func(m *meta.Meta) error
	// unchanged проверяет, что изменение не состоялось: именно это обязано
	// верно и после отравленной вставки события.
	unchanged func(t *testing.T, m *meta.Meta)
}

func journalScalar(t *testing.T, m *meta.Meta, q string, args ...any) any {
	t.Helper()
	var v any
	if err := m.Pool.QueryRow(context.Background(), q, args...).Scan(&v); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return v
}

func TestJournalEventIsAtomicWithChange(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()

	cases := []jCase{
		{
			name: "retention", kind: meta.JournalKindRetention,
			fix: func(t *testing.T, m *meta.Meta) {
				mustLockBucket(t, m, "j-ret")
				insertVersion(t, m, "j-ret", "k", versionOpts{current: true, nullVersion: true})
			},
			act: func(m *meta.Meta) error {
				return m.PutObjectRetention(ctx, testTenant, "j-ret", "k", "null",
					"GOVERNANCE", time.Now().Add(24*time.Hour))
			},
			unchanged: func(t *testing.T, m *meta.Meta) {
				if v := journalScalar(t, m,
					`SELECT retain_until IS NULL FROM objects WHERE key = 'k'`); v != true {
					t.Error("retention применился, хотя событие не записано")
				}
			},
		},
		{
			name: "legal hold", kind: meta.JournalKindLegalHold,
			fix: func(t *testing.T, m *meta.Meta) {
				mustLockBucket(t, m, "j-hold")
				insertVersion(t, m, "j-hold", "k", versionOpts{current: true, nullVersion: true})
			},
			act: func(m *meta.Meta) error {
				return m.PutObjectLegalHold(ctx, testTenant, "j-hold", "k", "null", true)
			},
			unchanged: func(t *testing.T, m *meta.Meta) {
				if v := journalScalar(t, m,
					`SELECT legal_hold FROM objects WHERE key = 'k'`); v != false {
					t.Error("legal hold применился, хотя события нет")
				}
			},
		},
		{
			name: "маркер удаления", kind: meta.JournalKindDeleteMarker,
			fix: func(t *testing.T, m *meta.Meta) {
				mustLockBucket(t, m, "j-mark")
				insertVersion(t, m, "j-mark", "k", versionOpts{current: true})
			},
			act: func(m *meta.Meta) error {
				_, err := m.DeleteObject(ctx, testTenant,
					meta.DeleteObjectInput{Bucket: "j-mark", Key: "k"})
				return err
			},
			unchanged: func(t *testing.T, m *meta.Meta) {
				if v := journalScalar(t, m,
					`SELECT count(*) FROM objects WHERE key = 'k' AND is_delete_marker`); v != int64(0) {
					t.Error("маркер создан, хотя события нет")
				}
			},
		},
		{
			name: "удаление версии", kind: meta.JournalKindDeleteVersion,
			fix: func(t *testing.T, m *meta.Meta) {
				mustLockBucket(t, m, "j-ver")
				insertVersion(t, m, "j-ver", "k", versionOpts{current: true})
			},
			act: func(m *meta.Meta) error {
				id := currentVersionID(t, m, "j-ver", "k")
				_, err := m.DeleteObject(ctx, testTenant,
					meta.DeleteObjectInput{Bucket: "j-ver", Key: "k", VersionID: id})
				return err
			},
			unchanged: func(t *testing.T, m *meta.Meta) {
				if v := journalScalar(t, m,
					`SELECT state FROM objects WHERE key = 'k'`); v != "pending" {
					t.Errorf("версия ушла в %v, хотя события нет", v)
				}
			},
		},
		{
			name: "обход GOVERNANCE", kind: meta.JournalKindDeleteVersion,
			fix: func(t *testing.T, m *meta.Meta) {
				mustLockBucket(t, m, "j-byp")
				id := insertVersion(t, m, "j-byp", "k", versionOpts{current: true})
				if _, err := m.Pool.Exec(ctx,
					`UPDATE objects SET retention_mode = 'GOVERNANCE',
					        retain_until = now() + interval '1 day' WHERE id = $1`, id); err != nil {
					t.Fatalf("срок под обход: %v", err)
				}
			},
			act: func(m *meta.Meta) error {
				id := currentVersionID(t, m, "j-byp", "k")
				_, err := m.DeleteObject(ctx, testTenant, meta.DeleteObjectInput{
					Bucket: "j-byp", Key: "k", VersionID: id, BypassGovernance: true})
				return err
			},
			unchanged: func(t *testing.T, m *meta.Meta) {
				if v := journalScalar(t, m,
					`SELECT state FROM objects WHERE key = 'k'`); v != "pending" {
					t.Errorf("версия под обходом ушла в %v, хотя события нет", v)
				}
				if v := journalScalar(t, m,
					`SELECT retention_bypassed_at IS NULL FROM objects WHERE key = 'k'`); v != true {
					t.Error("отметка обхода поставлена, хотя события нет")
				}
			},
		},
		{
			name: "удаление без версий", kind: meta.JournalKindDeleteVersion,
			fix: func(t *testing.T, m *meta.Meta) {
				mustBucket(t, m, "j-plain")
				insertVersion(t, m, "j-plain", "k", versionOpts{current: true, nullVersion: true})
			},
			act: func(m *meta.Meta) error {
				_, err := m.DeleteObject(ctx, testTenant,
					meta.DeleteObjectInput{Bucket: "j-plain", Key: "k"})
				return err
			},
			unchanged: func(t *testing.T, m *meta.Meta) {
				if v := journalScalar(t, m,
					`SELECT state FROM objects WHERE key = 'k'`); v != "pending" {
					t.Errorf("объект ушёл в %v, хотя события нет", v)
				}
			},
		},
		{
			name: "перезапись null-версии", kind: meta.JournalKindDeleteVersion,
			fix: func(t *testing.T, m *meta.Meta) {
				mustBucket(t, m, "j-over")
				in := putInput("j-over", "k")
				if _, err := m.PutObject(ctx, testTenant, in); err != nil {
					t.Fatalf("первая версия: %v", err)
				}
			},
			act: func(m *meta.Meta) error {
				in := putInput("j-over", "k")
				_, err := m.PutObject(ctx, testTenant, in)
				return err
			},
			unchanged: func(t *testing.T, m *meta.Meta) {
				// Прежняя версия обязана остаться единственной и текущей:
				// перезапись не состоялась целиком, а не «новая вставилась, а
				// событие удаления прежней нет».
				if v := journalScalar(t, m,
					`SELECT count(*) FROM objects WHERE key = 'k'`); v != int64(1) {
					t.Errorf("версий %v после неудачной перезаписи, хотели 1", v)
				}
			},
		},
		{
			name: "создание бакета", kind: meta.JournalKindBucketConfig,
			fix: func(t *testing.T, m *meta.Meta) {},
			act: func(m *meta.Meta) error {
				_, err := m.CreateBucket(ctx, testTenant, "j-new", "tester", []byte(`{}`), nil)
				return err
			},
			unchanged: func(t *testing.T, m *meta.Meta) {
				if v := journalScalar(t, m,
					`SELECT count(*) FROM buckets WHERE name = 'j-new'`); v != int64(0) {
					t.Error("бакет создан, хотя события нет")
				}
			},
		},
		{
			name: "версионирование", kind: meta.JournalKindBucketConfig,
			fix: func(t *testing.T, m *meta.Meta) { mustBucket(t, m, "j-vers") },
			act: func(m *meta.Meta) error {
				return m.SetBucketVersioning(ctx, testTenant, "j-vers", meta.VersioningEnabled)
			},
			unchanged: func(t *testing.T, m *meta.Meta) {
				if v := journalScalar(t, m,
					`SELECT versioning FROM buckets WHERE name = 'j-vers'`); v != "unversioned" {
					t.Errorf("версионирование = %v, хотя события нет", v)
				}
			},
		},
		{
			name: "object lock", kind: meta.JournalKindBucketConfig,
			fix: func(t *testing.T, m *meta.Meta) {
				mustBucket(t, m, "j-lock")
				setBucketVersioning(t, m, "j-lock", meta.VersioningEnabled)
			},
			act: func(m *meta.Meta) error {
				return m.SetObjectLockConfig(ctx, testTenant, "j-lock", true,
					[]byte(`<ObjectLockConfiguration/>`))
			},
			unchanged: func(t *testing.T, m *meta.Meta) {
				if v := journalScalar(t, m,
					`SELECT object_lock_enabled FROM buckets WHERE name = 'j-lock'`); v != false {
					t.Error("лок включился, хотя события нет")
				}
			},
		},
		{
			name: "acl", kind: meta.JournalKindBucketConfig,
			fix: func(t *testing.T, m *meta.Meta) { mustBucket(t, m, "j-acl") },
			act: func(m *meta.Meta) error {
				return m.SetBucketACL(ctx, testTenant, "j-acl", []byte(`{"Owner":"x"}`))
			},
			unchanged: func(t *testing.T, m *meta.Meta) {
				if v := journalScalar(t, m,
					`SELECT acl::text FROM buckets WHERE name = 'j-acl'`); v != "{}" {
					t.Errorf("acl = %v, хотя события нет", v)
				}
			},
		},
		{
			name: "политика", kind: meta.JournalKindBucketConfig,
			fix: func(t *testing.T, m *meta.Meta) { mustBucket(t, m, "j-pol") },
			act: func(m *meta.Meta) error {
				return m.SetBucketPolicy(ctx, testTenant, "j-pol", []byte(`{"Version":"2012-10-17"}`))
			},
			unchanged: func(t *testing.T, m *meta.Meta) {
				if v := journalScalar(t, m,
					`SELECT policy IS NULL FROM buckets WHERE name = 'j-pol'`); v != true {
					t.Error("политика записана, хотя события нет")
				}
			},
		},
		{
			name: "удаление бакета", kind: meta.JournalKindBucketConfig,
			fix: func(t *testing.T, m *meta.Meta) { mustBucket(t, m, "j-del") },
			act: func(m *meta.Meta) error {
				return m.DeleteBucket(ctx, testTenant, "j-del")
			},
			unchanged: func(t *testing.T, m *meta.Meta) {
				if v := journalScalar(t, m,
					`SELECT deleted_at IS NULL FROM buckets WHERE name = 'j-del'`); v != true {
					t.Error("бакет помечен удалённым, хотя события нет")
				}
			},
		},
		{
			name: "смена владельца", kind: meta.JournalKindBucketConfig,
			fix: func(t *testing.T, m *meta.Meta) { mustBucket(t, m, "j-own") },
			act: func(m *meta.Meta) error {
				return m.ChangeBucketOwner(ctx, meta.AllTenantsForGuard(), "j-own", "other", 1,
					[]byte(`{"Owner":"other"}`))
			},
			unchanged: func(t *testing.T, m *meta.Meta) {
				if v := journalScalar(t, m,
					`SELECT owner FROM buckets WHERE name = 'j-own'`); v != "tester" {
					t.Errorf("владелец = %v, хотя события нет", v)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			testdb.Clean(t, m)
			tc.fix(t, m)
			before := journalSeq(t, m, 1)

			// 1. Событие записать нельзя → изменения быть не должно.
			off := poisonJournal(t, m)
			if err := tc.act(m); err == nil {
				off()
				t.Fatal("изменение прошло с отравленным журналом — событие пишется не в той же транзакции")
			}
			off()
			if got := journalSeq(t, m, 1); got != before {
				t.Errorf("journal_seq вырос до %d на откатившейся операции: номер должен откатываться", got)
			}
			if n := len(journalEvents(t, m, 1)); n != int(before) {
				t.Errorf("событий %d, ожидалось %d", n, before)
			}
			tc.unchanged(t, m)

			// 2. То же изменение без яда → событие есть, ровно одно, следом.
			if err := tc.act(m); err != nil {
				t.Fatalf("изменение без яда: %v", err)
			}
			events := journalEvents(t, m, 1)
			if len(events) != int(before)+1 {
				t.Fatalf("событий %d, ожидалось %d", len(events), before+1)
			}
			last := events[len(events)-1]
			if last.Kind != tc.kind {
				t.Errorf("kind = %q, хотели %q", last.Kind, tc.kind)
			}
			if last.Seq != before+1 {
				t.Errorf("journal_seq = %d, хотели %d", last.Seq, before+1)
			}
		})
	}
}

// Номера обязаны быть плотными при параллельных изменениях одного тенанта:
// именно это позволяет `rebuild-meta` читать пропуск как аварию. Один воркер с
// другим именем здесь ничего не проверяет — счётчик проверяют несколько
// участников одновременно.
func TestJournalSeqIsDenseUnderConcurrency(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)
	mustLockBucket(t, m, "j-conc")

	const n = 8
	// mustBucket уже оставил событие — сверяем прирост, а не абсолютное число.
	before := journalSeq(t, m, 1)
	ids := make([]string, n)
	for i := range ids {
		ids[i] = insertVersion(t, m, "j-conc", fmt.Sprintf("k%d", i),
			versionOpts{current: true})
	}

	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = m.PutObjectRetention(ctx, testTenant, "j-conc",
				fmt.Sprintf("k%d", i), ids[i], "GOVERNANCE",
				time.Now().Add(time.Duration(i+1)*time.Hour))
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("изменение %d: %v", i, err)
		}
	}

	events := journalEvents(t, m, 1)
	if len(events) != int(before)+n {
		t.Fatalf("событий %d, хотели %d", len(events), int(before)+n)
	}
	seen := map[int64]bool{}
	for _, e := range events {
		if seen[e.Seq] {
			t.Errorf("номер %d выдан дважды", e.Seq)
		}
		seen[e.Seq] = true
	}
	// Плотность проверяем по всему диапазону: пропуск мог остаться и до нас.
	for i := int64(1); i <= int64(len(events)); i++ {
		if !seen[i] {
			t.Errorf("номера %d нет — пропуск в нумерации", i)
		}
	}
	if seq := journalSeq(t, m, 1); seq != int64(len(events)) {
		t.Errorf("journal_seq = %d, хотели %d: счётчик обязан совпадать с последним событием",
			seq, len(events))
	}
}

// Смешанные изменения одного тенанта не должны ловить взаимоблокировку:
// порядок блокировок (сначала бакет и версии, строка тенанта последней) —
// договорённость, а не случайность. Проверка закрытая: любая ошибка здесь —
// провал, «похоже на гонку» не принимается.
func TestJournalMixedWritesDoNotDeadlock(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)
	mustLockBucket(t, m, "j-mix")

	const keys = 6
	before := journalSeq(t, m, 1)
	ids := make([]string, keys)
	for i := range ids {
		ids[i] = insertVersion(t, m, "j-mix", fmt.Sprintf("k%d", i), versionOpts{current: true})
	}

	var wg sync.WaitGroup
	fail := make(chan string, 16)
	push := func(fn func() error, what string, i int) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := fn(); err != nil {
				fail <- fmt.Sprintf("%s %d: %v", what, i, err)
			}
		}()
	}
	for i := 0; i < keys; i++ {
		i := i
		push(func() error {
			return m.PutObjectRetention(ctx, testTenant, "j-mix", fmt.Sprintf("k%d", i),
				ids[i], "GOVERNANCE", time.Now().Add(time.Hour))
		}, "retention", i)
	}
	for i := 0; i < 3; i++ {
		i := i
		push(func() error {
			return m.SetBucketACL(ctx, testTenant, "j-mix",
				json.RawMessage(fmt.Sprintf(`{"round":%d}`, i)))
		}, "acl", i)
	}
	for i := 0; i < 3; i++ {
		i := i
		push(func() error {
			return m.SetBucketPolicy(ctx, testTenant, "j-mix",
				[]byte(fmt.Sprintf(`{"round":%d}`, i)))
		}, "policy", i)
	}
	wg.Wait()
	close(fail)
	for s := range fail {
		t.Error(s)
	}

	events := journalEvents(t, m, 1)
	if len(events) != int(before)+keys+6 {
		t.Fatalf("событий %d, хотели %d", len(events), int(before)+keys+6)
	}
	seen := map[int64]bool{}
	for _, e := range events {
		seen[e.Seq] = true
	}
	for i := int64(1); i <= int64(len(events)); i++ {
		if !seen[i] {
			t.Errorf("пропуск в нумерации на %d", i)
		}
	}
}

// Событие обязано принадлежать тенанту строки, а не учётки запроса: под обходом
// по роли «тенант запроса» — служебное «все тенанты», и событие уехало бы в
// несуществующего тенанта.
func TestJournalEventBelongsToRowTenant(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)

	// Клиентский тенант 2: в системном тенанте 1 обход по роли неотличим от
	// обычного ключа, и проверка «событие не в тенанте запроса» была бы пустой.
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO tenants (id, name, status, pool_id) VALUES (2, 'j-client', 'active', 1)
		  ON CONFLICT (id) DO NOTHING`); err != nil {
		t.Fatalf("тенант 2: %v", err)
	}
	owner := meta.NewTenant(2)
	if _, err := m.CreateBucket(ctx, owner, "j-tenant", "u2", []byte(`{}`), nil); err != nil {
		t.Fatalf("бакет тенанта 2: %v", err)
	}
	// Тот же бакет под обходом по роли: страж отдаёт «все тенанты», а событие
	// всё равно обязано лечь в тенант бакета.
	if err := m.SetBucketACL(ctx, meta.AllTenantsForGuard(), "j-tenant", []byte(`{"x":1}`)); err != nil {
		t.Fatalf("acl под обходом: %v", err)
	}

	if n := len(journalEvents(t, m, 1)); n != 0 {
		t.Errorf("в системном тенанте %d событий, хотели 0", n)
	}
	events := journalEvents(t, m, 2)
	if len(events) != 2 {
		t.Fatalf("в тенанте 2 событий %d, хотели 2", len(events))
	}
	for _, e := range events {
		if e.TenantID != 2 {
			t.Errorf("событие %d лежит в тенанте %d", e.Seq, e.TenantID)
		}
	}
	if seq := journalSeq(t, m, 1); seq != 0 {
		t.Errorf("у системного тенанта journal_seq = %d, хотели 0", seq)
	}
}

// Снимок конфигурации обязан пережить изменение полностью: применять при
// восстановлении надо готовое состояние, а не догадываться, какие поля менялись.
func TestJournalBucketConfigSnapshot(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)
	mustBucket(t, m, "j-snap")

	if err := m.SetBucketVersioning(ctx, testTenant, "j-snap", meta.VersioningEnabled); err != nil {
		t.Fatalf("версионирование: %v", err)
	}
	if err := m.SetObjectLockConfig(ctx, testTenant, "j-snap", true,
		[]byte(`<Rule><DefaultRetention><Mode>COMPLIANCE</Mode></DefaultRetention></Rule>`)); err != nil {
		t.Fatalf("лок: %v", err)
	}
	if err := m.SetBucketPolicy(ctx, testTenant, "j-snap", []byte(`{"Version":"2012-10-17"}`)); err != nil {
		t.Fatalf("политика: %v", err)
	}

	events := journalEvents(t, m, 1)
	// Создание бакета + три изменения.
	last := events[len(events)-1]
	cfg, err := last.Config()
	if err != nil {
		t.Fatalf("разобрать снимок: %v", err)
	}
	if cfg.Bucket != "j-snap" || cfg.Versioning != meta.VersioningEnabled || !cfg.ObjectLock {
		t.Errorf("снимок не отражает состояние: %+v", cfg)
	}
	if cfg.LockConfig == "" {
		t.Error("в снимке нет lock_config — срок по умолчанию потерялся бы")
	}
	if cfg.Policy == "" || cfg.Owner != "tester" || cfg.OwnerTenant != 1 {
		t.Errorf("снимок неполный: %+v", cfg)
	}
	if cfg.Deleted {
		t.Error("снимок помечен удалённым")
	}
}

// Флаги событий удаления: обход GOVERNANCE и убранный маркер обязаны читаться,
// иначе восстановление потеряет и «удаление было досрочным», и «маркер сняли».
func TestJournalDeleteFlags(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)
	mustLockBucket(t, m, "j-flags")

	// Обход GOVERNANCE.
	gid := insertVersion(t, m, "j-flags", "g", versionOpts{current: true})
	if _, err := m.Pool.Exec(ctx,
		`UPDATE objects SET retention_mode = 'GOVERNANCE',
		        retain_until = now() + interval '1 day' WHERE id = $1`, gid); err != nil {
		t.Fatalf("срок: %v", err)
	}
	if _, err := m.DeleteObject(ctx, testTenant, meta.DeleteObjectInput{
		Bucket: "j-flags", Key: "g", VersionID: gid, BypassGovernance: true}); err != nil {
		t.Fatalf("удаление с обходом: %v", err)
	}

	// Маркер создан и затем убран.
	mustLockBucket(t, m, "j-flags2")
	insertVersion(t, m, "j-flags2", "k", versionOpts{current: true})
	res, err := m.DeleteObject(ctx, testTenant, meta.DeleteObjectInput{Bucket: "j-flags2", Key: "k"})
	if err != nil {
		t.Fatalf("маркер: %v", err)
	}
	if _, err := m.DeleteObject(ctx, testTenant, meta.DeleteObjectInput{
		Bucket: "j-flags2", Key: "k", VersionID: res.VersionID}); err != nil {
		t.Fatalf("убрать маркер: %v", err)
	}

	var bypass, created, removed bool
	for _, e := range journalEvents(t, m, 1) {
		switch e.Kind {
		case meta.JournalKindDeleteVersion:
			p, err := e.DeleteVersion()
			if err != nil {
				t.Fatalf("payload удаления: %v", err)
			}
			bypass = bypass || p.Bypass
		case meta.JournalKindDeleteMarker:
			p, err := e.DeleteMarker()
			if err != nil {
				t.Fatalf("payload маркера: %v", err)
			}
			if p.Removed {
				removed = true
			} else {
				created = true
			}
		}
	}
	if !bypass {
		t.Error("ни одно удаление не помечено обходом GOVERNANCE")
	}
	if !created || !removed {
		t.Errorf("маркеры: создано %v, убрано %v — нужны оба", created, removed)
	}
}

// Журнал только на добавление — тем же механизмом, что media_events: событие
// не должно быть поводом «исправить историю» молча.
func TestJournalIsAppendOnly(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)
	mustBucket(t, m, "j-once")

	if _, err := m.Pool.Exec(ctx,
		`UPDATE journal_events SET created_at = created_at`); err == nil {
		t.Error("UPDATE события журнала прошёл — журнал не append-only")
	} else if !strings.Contains(err.Error(), "только на добавление") {
		t.Errorf("отказ UPDATE не похож на отказ append-only: %v", err)
	}
	if _, err := m.Pool.Exec(ctx, `DELETE FROM journal_events`); err == nil {
		t.Error("DELETE событий журнала прошёл — журнал не append-only")
	}

	// Обход возможен только явно названной причиной — тот же контракт, что у
	// media_events.
	if _, err := m.Pool.Exec(ctx,
		`SET tapegw.journal_override = 'тест обхода';
		  UPDATE journal_events SET created_at = created_at`); err != nil {
		t.Errorf("явный обход не сработал: %v", err)
	}
}

// Счётчик не тратит номера на откатившиеся операции — в этом весь смысл
// счётчика на строке вместо sequence Postgres.
func TestJournalSeqSurvivesRollback(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)
	mustLockBucket(t, m, "j-rb")
	ids := []string{
		insertVersion(t, m, "j-rb", "a", versionOpts{current: true}),
		insertVersion(t, m, "j-rb", "b", versionOpts{current: true}),
	}

	off := poisonJournal(t, m)
	if err := m.PutObjectRetention(ctx, testTenant, "j-rb", "a", ids[0],
		"GOVERNANCE", time.Now().Add(time.Hour)); err == nil {
		t.Fatal("операция под ядом прошла")
	}
	off()

	if err := m.PutObjectRetention(ctx, testTenant, "j-rb", "b", ids[1],
		"GOVERNANCE", time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("второе изменение: %v", err)
	}
	events := journalEvents(t, m, 1)
	// mustBucket тоже оставил событие — важно, что после отката номер не
	// пропущен, а продолжен.
	if len(events) == 0 {
		t.Fatal("событий нет")
	}
	max := events[len(events)-1].Seq
	for i := int64(1); i <= max; i++ {
		var n int
		if err := m.Pool.QueryRow(ctx,
			`SELECT count(*) FROM journal_events WHERE tenant_id = 1 AND journal_seq = $1`, i).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Errorf("на номере %d событий %d — пропуск после откатившейся операции", i, n)
		}
	}
}

// Проверка формата payload: имена полей — часть формата на ленте, и тест
// фиксирует их явно, а не через round-trip своей же структуры.
func TestJournalPayloadFields(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)
	mustLockBucket(t, m, "j-fmt")
	id := insertVersion(t, m, "j-fmt", "k", versionOpts{current: true})

	until := time.Now().Add(72 * time.Hour).UTC().Truncate(time.Second)
	if err := m.PutObjectRetention(ctx, testTenant, "j-fmt", "k", id, "COMPLIANCE", until); err != nil {
		t.Fatalf("retention: %v", err)
	}

	events := journalEvents(t, m, 1)
	last := events[len(events)-1]
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(last.Payload, &raw); err != nil {
		t.Fatalf("payload не JSON: %v", err)
	}
	for _, key := range []string{"bucket", "key", "mode", "until"} {
		if _, ok := raw[key]; !ok {
			t.Errorf("в payload retention нет поля %q: %s", key, last.Payload)
		}
	}
	if last.ObjectID == nil || last.ObjectID.String() != id {
		t.Errorf("object_id = %v, хотели %s", last.ObjectID, id)
	}
	p, err := last.Retention()
	if err != nil {
		t.Fatalf("разобрать: %v", err)
	}
	if p.Mode != "COMPLIANCE" || !p.Until.Equal(until) {
		t.Errorf("retention распарсился как %+v", p)
	}
	if p.Bucket != "j-fmt" || p.Key != "k" {
		t.Errorf("событие не про тот объект: %+v", p)
	}
}
