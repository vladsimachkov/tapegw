package meta_test

// Тест миграции 0007 (системный тенант, ADR-0018).
//
// Правило 11 из AGENTS.md: миграция проверяется **текстом файла**, а не его
// пересказом. Up и Down вынимаются из `.sql` дословно и исполняются как есть
// (pgx без аргументов идёт простым протоколом, несколько операторов в строке он
// допускает), поэтому тест ломается ровно тогда, когда ломается сам файл —
// включая опечатку в имени ограничения, которую глазами не видно.
//
// Отдельно проверяется порядок шагов a–d. Он не «желательный», он единственный
// возможный, и тест это ловит: Up применяется к схеме, в которой уже есть
// тенант id = 1 и непустая `accounts`, поэтому и «чек до UPDATE», и
// «ADD COLUMN NOT NULL одной командой» падают здесь же, где упали бы на живой
// базе. Обе ловушки зафиксированы в разделе «Проверка» ADR-0018.

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/testdb"
)

const migration0007 = "migrations/0007_system_tenant.sql"

// SQLSTATE, по которым проверяем отказы схемы. 23514 — check_violation,
// 23503 — foreign_key_violation. Именно их путают чаще всего: «отказ есть» не
// различает чек и FK, а смысл у них разный.
const (
	stateCheckViolation = "23514"
	stateFKViolation    = "23503"
)

// section вынимает из файла миграции текст Up или Down дословно: от строки
// `-- +goose Up|Down` до следующей директивы goose или до конца файла.
// Никакой пересборки SQL здесь нет и быть не должно.
func section(t *testing.T, file, want string) string {
	t.Helper()

	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("read %s: %v", file, err)
	}

	lines := strings.Split(string(raw), "\n")
	start := -1
	for i, ln := range lines {
		if strings.TrimSpace(ln) == "-- +goose "+want {
			start = i + 1
			break
		}
	}
	if start < 0 {
		t.Fatalf("%s: секция %s не найдена", file, want)
	}

	var out []string
	for _, ln := range lines[start:] {
		if strings.HasPrefix(strings.TrimSpace(ln), "-- +goose") {
			break
		}
		out = append(out, ln)
	}
	text := strings.TrimSpace(strings.Join(out, "\n"))
	if text == "" {
		t.Fatalf("%s: секция %s пуста", file, want)
	}
	return text
}

func pgState(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}

// expectFail — отказ с ожидаемым SQLSTATE. Прошедший запрос — тоже провал:
// молча принявшаяся строка в таблице ADR-0018 и есть дыра.
func expectFail(t *testing.T, label, want string, err error) {
	t.Helper()
	if err == nil {
		t.Errorf("%s: ожидали отказ, а запрос прошёл", label)
		return
	}
	if want != "" && pgState(err) != want {
		t.Errorf("%s: SQLSTATE = %s, хотели %s; ошибка: %v", label, pgState(err), want, err)
	}
}

func hasColumn(t *testing.T, m *meta.Meta, table, col string) bool {
	t.Helper()
	var n int
	err := m.Pool.QueryRow(context.Background(),
		`SELECT count(*) FROM information_schema.columns
		  WHERE table_schema = current_schema() AND table_name = $1 AND column_name = $2`,
		table, col).Scan(&n)
	if err != nil {
		t.Fatalf("hasColumn(%s.%s): %v", table, col, err)
	}
	return n > 0
}

func hasConstraint(t *testing.T, m *meta.Meta, name string) bool {
	t.Helper()
	var n int
	err := m.Pool.QueryRow(context.Background(),
		`SELECT count(*) FROM pg_constraint c
		  JOIN pg_namespace n ON n.oid = c.connamespace
		 WHERE n.nspname = current_schema() AND c.conname = $1`, name).Scan(&n)
	if err != nil {
		t.Fatalf("hasConstraint(%s): %v", name, err)
	}
	return n > 0
}

func isNotNull(t *testing.T, m *meta.Meta, table, col string) bool {
	t.Helper()
	var n int
	err := m.Pool.QueryRow(context.Background(),
		`SELECT count(*) FROM information_schema.columns
		  WHERE table_schema = current_schema() AND table_name = $1
		    AND column_name = $2 AND is_nullable = 'NO'`, table, col).Scan(&n)
	if err != nil {
		t.Fatalf("isNotNull(%s.%s): %v", table, col, err)
	}
	return n > 0
}

// Своя схема на тест: он откатывает миграцию (Down) и применяет её заново, а в
// общей схеме пакета в это время работают остальные тесты meta. Схема
// детерминирована, поэтому старует с нуля — иначе состояние прошлого прогона
// (например, уже откатанная миграция) уезжает в следующий и падение рассказывает
// не про текущий запуск.
func mig0007DB(t *testing.T) *meta.Meta {
	t.Helper()
	schema := testdb.PackageSchema(t) + "_0007"
	testdb.DropSchema(t, schema)
	return testdb.ConnectSchema(t, schema)
}

// Down и Up текстом файла: откат обязан убрать ровно то, что добавил Up, и
// ничего лишнего, а повторный Up — пройти на схеме, где уже есть данные.
func TestMigration0007DownAndUpFromFileSync(t *testing.T) {
	m := mig0007DB(t)
	ctx := context.Background()

	// Goose применил миграции при подключении — Up из файла уже отработал.
	// Проверяем результат, а не веру в goose.
	if !hasColumn(t, m, "tenants", "system") || !hasColumn(t, m, "accounts", "tenant_system") {
		t.Fatal("после Up колонки system / tenant_system обязаны существовать")
	}
	if !isNotNull(t, m, "accounts", "tenant_system") {
		t.Error("accounts.tenant_system обязана быть NOT NULL после Up")
	}
	for _, c := range []string{"tenants_default_is_system", "accounts_tenant_fkey", "accounts_not_system_unless_admin"} {
		if !hasConstraint(t, m, c) {
			t.Errorf("после Up ограничение %s не создано", c)
		}
	}
	var sys bool
	if err := m.Pool.QueryRow(ctx, `SELECT system FROM tenants WHERE id = 1`).Scan(&sys); err != nil {
		t.Fatalf("флаг системного тенанта не читается: %v", err)
	}
	if !sys {
		t.Error("тенант id = 1 обязан быть системным после Up")
	}

	// Данные переживут откат: именно на непустой таблице и ломаются
	// «упрощённые» варианты миграции (см. ADR-0018, «Проверка»).
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO accounts (access_key, secret_wrapped, role, tenant_id, tenant_system)
		  VALUES ('op', decode('00','hex'), 'admin', 1, true)`); err != nil {
		t.Fatalf("подготовка admin-учётки: %v", err)
	}

	down := section(t, migration0007, "Down")
	if _, err := m.Pool.Exec(ctx, down); err != nil {
		t.Fatalf("Down текстом файла упал: %v", err)
	}

	for _, c := range []string{"tenants_default_is_system", "accounts_tenant_fkey", "accounts_not_system_unless_admin", "tenants_id_system_key"} {
		if hasConstraint(t, m, c) {
			t.Errorf("после Down ограничение %s осталось", c)
		}
	}
	if hasColumn(t, m, "tenants", "system") || hasColumn(t, m, "accounts", "tenant_system") {
		t.Error("после Down колонки system / tenant_system обязаны исчезнуть")
	}
	// Данные переживали откат: accounts на месте, просто без новой колонки.
	var n int
	if err := m.Pool.QueryRow(ctx, `SELECT count(*) FROM accounts WHERE access_key = 'op'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("Down не должен трогать строки accounts: n=%d err=%v", n, err)
	}

	// Повторный Up — уже текстом файла, и уже на непустой accounts. Если кто-то
	// заменит три шага (c) на `ADD COLUMN ... NOT NULL`, здесь будет
	// `contains null values`; если переставит чек (b) перед UPDATE —
	// `violated by some row`.
	if _, err := m.Pool.Exec(ctx, section(t, migration0007, "Up")); err != nil {
		t.Fatalf("Up текстом файла упал на непустой базе: %v", err)
	}
	var backfilled bool
	if err := m.Pool.QueryRow(ctx,
		`SELECT tenant_system FROM accounts WHERE access_key = 'op'`).Scan(&backfilled); err != nil {
		t.Fatalf("бэкфилл не сработал: %v", err)
	}
	if !backfilled {
		t.Error("бэкфилл tenant_system не заполнил существующую admin-учётку")
	}
}

// Таблица ADR-0018: восемь строк, которые обязан воспроизводить механизм
// «составной FK + чек». Это вторая линия защиты — от ручной правки таблицы и от
// сида стенда; код IAM (первая линия) даёт только понятный текст отказа.
func TestMigration0007SchemaRules(t *testing.T) {
	m := mig0007DB(t)
	ctx := context.Background()

	// Клиентский тенант (не системный) и тенант, который потом станет
	// системным, — для последних двух строк таблицы.
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO tenants (id, name, pool_id) VALUES (2, 'client', 1), (3, 'future', 1)
		  ON CONFLICT (id) DO NOTHING`); err != nil {
		t.Fatalf("клиентские тенанты: %v", err)
	}

	// access_key — PRIMARY KEY, поэтому имя строки таблицы уникально: тест
	// обязан оставить после себя ровно те строки, которые он проверяет.
	ins := func(key, role string, tenant int64, sys bool) error {
		_, err := m.Pool.Exec(ctx,
			`INSERT INTO accounts (access_key, secret_wrapped, role, tenant_id, tenant_system)
			  VALUES ($1, decode('00','hex'), $2, $3, $4)`,
			key, role, tenant, sys)
		return err
	}

	// 1. role = user, тенант 1, флаг true → чек учёток.
	expectFail(t, "user в системном тенанте (флаг true)", stateCheckViolation,
		ins("u1t", "user", 1, true))
	// 2. role = user, тенант 1, флаг false → FK: пары (1, false) в tenants нет.
	expectFail(t, "user в системном тенанте (флаг false)", stateFKViolation,
		ins("u1f", "user", 1, false))
	// 3. role = admin, тенант 1, флаг true — ок: оператору можно.
	if err := ins("adm1", "admin", 1, true); err != nil {
		t.Errorf("admin в системном тенанте обязан проходить: %v", err)
	}
	// 4. role = userplus, тенант 2, флаг true → чек: userplus не admin.
	expectFail(t, "userplus с флагом системности", stateCheckViolation,
		ins("up2", "userplus", 2, true))
	// 5. role = user, тенант 2, флаг false — ок, это обычный клиент.
	if err := ins("usr2", "user", 2, false); err != nil {
		t.Errorf("обычная клиентская учётка обязана проходить: %v", err)
	}

	// 6. Поднять флаг у клиентской учётки вручную → чек.
	_, err := m.Pool.Exec(ctx, `UPDATE accounts SET tenant_system = true WHERE role = 'user'`)
	expectFail(t, "UPDATE tenant_system = true у клиентской учётки", stateCheckViolation, err)

	// 7. Снять системность с id = 1 → чек на tenants. Именно эта дыра делала
	// бакеты, созданные root, чужими.
	_, err = m.Pool.Exec(ctx, `UPDATE tenants SET system = false WHERE id = 1`)
	expectFail(t, "снять system с id = 1", stateCheckViolation, err)

	// 8. Пометить системным тенант с обычными учётками → чек учёток через
	// каскад ON UPDATE CASCADE.
	_, err = m.Pool.Exec(ctx, `UPDATE tenants SET system = true WHERE id = 2`)
	expectFail(t, "system = true для тенанта с клиентскими учётками", stateCheckViolation, err)

	// И обратный случай из ADR: тенант **без** учёток пометить можно —
	// нарушать нечему. Это допустимое состояние, а не ошибка.
	if _, err := m.Pool.Exec(ctx, `UPDATE tenants SET system = true WHERE id = 3`); err != nil {
		t.Errorf("пометить системным пустой тенант можно: %v", err)
	}
	// И каскад обязан доехать до учётки, если она там появится позже — проверим
	// обратное: учётка в ставшем системным тенанте возможна только admin.
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO accounts (access_key, secret_wrapped, role, tenant_id, tenant_system)
		  VALUES ('op3', decode('00','hex'), 'admin', 3, true)`); err != nil {
		t.Errorf("admin в ставшем системным тенанте обязан проходить: %v", err)
	}
}
