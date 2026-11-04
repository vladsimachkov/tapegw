package meta_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// Clean обязан покрывать все таблицы схемы.
//
// Правило «не забыть дописать таблицу в TRUNCATE» на памяти не держится, а
// цена ошибки — тест, который проходит или падает в зависимости от предыдущего
// прогона в той же схеме (так падал второй прогон на accounts_pkey). Поэтому
// проверка не в правиле, а здесь: список CleanTables сверяется с pg_tables.
//
// Проверка статическая намеренно: если проверять только «после Clean пусто»,
// тест проходил бы ничего не делая, пока в новой таблице случайно нет строк.
func TestCleanCoversEveryTable(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()

	rows, err := m.Pool.Query(ctx,
		`SELECT tablename FROM pg_tables
		  WHERE schemaname = current_schema() AND tablename <> 'goose_db_version'
		  ORDER BY tablename`)
	if err != nil {
		t.Fatalf("список таблиц: %v", err)
	}
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			t.Fatalf("чтение имени таблицы: %v", err)
		}
		tables = append(tables, name)
	}
	rows.Close()
	if len(tables) == 0 {
		t.Fatalf("в схеме нет таблиц — тест не о чём")
	}

	cleaned := make(map[string]bool, len(testdb.CleanTables))
	for _, tbl := range testdb.CleanTables {
		cleaned[tbl] = true
	}
	for _, tbl := range tables {
		if cleaned[tbl] || testdb.IsSeedTable(tbl) {
			continue
		}
		t.Errorf("таблица %s не покрыта testdb.Clean: допишите её в testdb.CleanTables "+
			"либо, если её наполняют миграции, в testdb.SeedTables", tbl)
	}
	// Обратно: в списке не должно быть таблиц, которых в схеме нет, — иначе
	// TRUNCATE упадёт на переименованной таблице и похоронит весь набор.
	for _, tbl := range testdb.CleanTables {
		found := false
		for _, real := range tables {
			if real == tbl {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("testdb.CleanTables содержит таблицу %s, которой в схеме нет", tbl)
		}
	}
}

// Сам Clean действительно чистит то, что обещает: в таблицы вставляются строки,
// после Clean их нет, а сид миграций остаётся.
func TestCleanEmptiesDataTables(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()

	// Две таблицы с разными формами ключей: бакет и учётка (уникальный
	// access_key) — на последней и случался флейк. Имена уникальны на прогон,
	// чтобы посев не зависел от того, что осталось в схеме.
	probe := "clean-probe-" + uuid.NewString()
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO buckets (name, owner, tenant_id) VALUES ($1, 'o', 1)`,
		probe); err != nil {
		t.Fatalf("посев бакета: %v", err)
	}
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO accounts (access_key, secret_wrapped, role, tenant_id, tenant_system)
		  VALUES ($1, 'x'::bytea, 'admin', 1, true)`,
		probe); err != nil {
		t.Fatalf("посев учётки: %v", err)
	}
	// Посев обязан быть виден, иначе «после Clean пусто» ничего не проверяет.
	var nBuckets, nAccounts int
	if err := m.Pool.QueryRow(ctx,
		`SELECT count(*) FROM buckets WHERE name = $1`, probe).Scan(&nBuckets); err != nil {
		t.Fatalf("проверка посева buckets: %v", err)
	}
	if err := m.Pool.QueryRow(ctx,
		`SELECT count(*) FROM accounts WHERE access_key = $1`, probe).Scan(&nAccounts); err != nil {
		t.Fatalf("проверка посева accounts: %v", err)
	}
	if nBuckets == 0 || nAccounts == 0 {
		t.Errorf("посев не виден до Clean (buckets=%d accounts=%d) — проверка пуста",
			nBuckets, nAccounts)
	}

	testdb.Clean(t, m)

	for _, tbl := range testdb.CleanTables {
		var n int
		if err := m.Pool.QueryRow(ctx,
			`SELECT count(*) FROM `+tbl).Scan(&n); err != nil {
			t.Fatalf("спрос про %s: %v", tbl, err)
		}
		if n != 0 {
			t.Errorf("после Clean в %s осталось %d строк", tbl, n)
		}
	}

	// Строки сида обязаны пережить Clean: на них висят внешние ключи тестов.
	for _, seed := range testdb.SeedTables {
		var n int
		if err := m.Pool.QueryRow(ctx,
			`SELECT count(*) FROM `+seed.Table+` WHERE `+seed.Column+` = $1`,
			testdb.SeedID).Scan(&n); err != nil {
			t.Fatalf("спрос про сид %s: %v", seed.Table, err)
		}
		if n != 1 {
			t.Errorf("сид-строка %s (%s = %d) пропала после Clean",
				seed.Table, seed.Column, testdb.SeedID)
		}
	}
}

// Clean оставляет в таблицах сида ровно строку сида.
//
// Тесты сеют тенанты 2 и 3, их строки tenant_usage, позже появятся свои пулы.
// Если их не зачищать, они копятся между прогонами, а тесты держатся только на
// ON CONFLICT — та же форма, в которой флейк с accounts молчал до второго
// прогона.
func TestCleanKeepsOnlySeedRows(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()

	// Посев — зависимые строки в порядке внешних ключей: пул, тенант, usage.
	// Иначе вставка тенанта с id ≠ 1 упала бы на FK к pools.
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO pools (id, name, phobos_tag, repl_count, kind)
		  VALUES (91, 'probe', 'pool:probe', 1, 'dedicated')
		  ON CONFLICT (id) DO NOTHING`); err != nil {
		t.Fatalf("посев пула: %v", err)
	}
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO tenants (id, name, status, pool_id) VALUES (92, 'probe', 'active', 91)
		  ON CONFLICT (id) DO NOTHING`); err != nil {
		t.Fatalf("посев тенанта: %v", err)
	}
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO tenant_usage (tenant_id) VALUES (92)
		  ON CONFLICT (tenant_id) DO NOTHING`); err != nil {
		t.Fatalf("посев tenant_usage: %v", err)
	}

	// Посев обязан быть виден, иначе «ровно одна строка» ничего не проверяет.
	for _, seed := range testdb.SeedTables {
		var n int
		if err := m.Pool.QueryRow(ctx,
			`SELECT count(*) FROM `+seed.Table+` WHERE `+seed.Column+` <> $1`,
			testdb.SeedID).Scan(&n); err != nil {
			t.Fatalf("проверка посева %s: %v", seed.Table, err)
		}
		if n == 0 {
			t.Errorf("в %s нет ни одной не-сид строки до Clean — проверка пуста",
				seed.Table)
		}
	}

	testdb.Clean(t, m)

	for _, seed := range testdb.SeedTables {
		var total, seedRows int
		if err := m.Pool.QueryRow(ctx,
			`SELECT count(*) FROM `+seed.Table).Scan(&total); err != nil {
			t.Fatalf("подсчёт %s: %v", seed.Table, err)
		}
		if err := m.Pool.QueryRow(ctx,
			`SELECT count(*) FROM `+seed.Table+` WHERE `+seed.Column+` = $1`,
			testdb.SeedID).Scan(&seedRows); err != nil {
			t.Fatalf("подсчёт сида в %s: %v", seed.Table, err)
		}
		if total != 1 || seedRows != 1 {
			t.Errorf("после Clean в %s строк %d, из них сида %d — хотели ровно 1 и 1",
				seed.Table, total, seedRows)
		}
	}
}
