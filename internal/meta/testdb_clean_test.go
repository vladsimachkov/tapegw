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
		if cleaned[tbl] || testdb.SeedTables[tbl] != "" {
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

	for tbl, col := range testdb.SeedTables {
		var n int
		if err := m.Pool.QueryRow(ctx,
			`SELECT count(*) FROM `+tbl+` WHERE `+col+` = 1`).Scan(&n); err != nil {
			t.Fatalf("спрос про сид %s: %v", tbl, err)
		}
		if n != 1 {
			t.Errorf("сид-строка %s (%s = 1) пропала после Clean", tbl, col)
		}
	}
}
