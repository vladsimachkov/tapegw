package meta_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// Тест миграции 0023 (длина файла объекта на диске).
//
// Правило 11: Up и Down текстом файла и на таблице с данными. Колонка nullable
// (длину нельзя вывести из SQL), поэтому на наполненной таблице важно увидеть и
// что NULL проходит (строки до миграции), и что CHECK ловит ноль/минус — длина
// участника пачки не может быть нулевой, иначе сборщик обрежет манифест.

const migration0023 = "migrations/0023_object_file_bytes.sql"

func TestMigration0023ObjectFileBytes(t *testing.T) {
	m := testdb.MigrationScratch(t, testdb.AllVersions)
	ctx := context.Background()

	if n := columnIn(t, ctx, m, "objects", "file_bytes"); n != 1 {
		t.Fatal("objects.file_bytes отсутствует")
	}

	mustBucket(t, m, "fb-bucket")
	var bucketID int64
	if err := m.Pool.QueryRow(ctx,
		`SELECT id FROM buckets WHERE name = 'fb-bucket'`).Scan(&bucketID); err != nil {
		t.Fatalf("id бакета: %v", err)
	}

	seq := 0
	insert := func(id uuid.UUID, fb any) error {
		seq++
		_, err := m.Pool.Exec(ctx,
			`INSERT INTO objects (id, bucket_id, key, size, etag, storage_class, state,
			                      landing, online_until, tenant_id, pool_id, is_current,
			                      wrapped_dek, file_bytes)
			  VALUES ($1, $2, 'fb-' || $4, 100, 'etag', 'GLACIER', 'pending', true,
			          now() + interval '1 day', 1, 1, true, 'x'::bytea, $3)`,
			id, bucketID, fb, strconv.Itoa(seq))
		return err
	}

	// NULL проходит: строки, записанные до колонки, не архивируются, но и не
	// мешают миграции.
	if err := insert(uuid.New(), nil); err != nil {
		t.Fatalf("NULL file_bytes отклонён: %v", err)
	}
	// Положительная длина — норма.
	good := uuid.New()
	if err := insert(good, 4096); err != nil {
		t.Fatalf("file_bytes=4096 отклонён: %v", err)
	}
	// Ноль и минус — отказ: нулевая длина участника читает ноль байт и молча
	// теряет манифест.
	for _, bad := range []int64{0, -1} {
		if err := insert(uuid.New(), bad); err == nil {
			t.Errorf("file_bytes=%d прошёл: CHECK не работает", bad)
		}
	}

	// Down → Up текстом файла на таблице с данными: колонка уходит и возвращается,
	// строка объекта выживает.
	if _, err := m.Pool.Exec(ctx, section(t, migration0023, "Down")); err != nil {
		t.Fatalf("Down 0023 на таблице с данными: %v", err)
	}
	if n := columnIn(t, ctx, m, "objects", "file_bytes"); n != 0 {
		t.Error("после Down file_bytes осталась")
	}
	if _, err := m.Pool.Exec(ctx, section(t, migration0023, "Up")); err != nil {
		t.Fatalf("повторный Up 0023: %v", err)
	}
	if n := columnIn(t, ctx, m, "objects", "file_bytes"); n != 1 {
		t.Error("после повторного Up file_bytes нет")
	}
	var live int
	if err := m.Pool.QueryRow(ctx, `SELECT count(*) FROM objects WHERE id = $1`, good).Scan(&live); err != nil {
		t.Fatal(err)
	}
	if live != 1 {
		t.Error("Down+Up потеряли строку объекта")
	}
}
