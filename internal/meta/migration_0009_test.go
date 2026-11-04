package meta_test

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// Тест миграции 0009 (version_seq, этап V).
//
// Правило 11: миграция проверяется текстом файла. Здесь это особенно важно:
// Up состоит из шагов, порядок которых не «желательный», а единственный
// возможный (см. комментарии в самом SQL), и любой из них в одиночку ломает
// миграцию на непустой базе.

const migration0009 = "migrations/0009_version_seq.sql"

func TestMigration0009VersionSeq(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	// Колонка есть, NOT NULL, и значение берётся из последовательности.
	var nullable, def string
	if err := m.Pool.QueryRow(ctx,
		`SELECT is_nullable, COALESCE(column_default, '')
		   FROM information_schema.columns
		  WHERE table_schema = current_schema()
		    AND table_name = 'objects' AND column_name = 'version_seq'`).
		Scan(&nullable, &def); err != nil {
		t.Fatalf("колонка version_seq: %v", err)
	}
	if nullable != "NO" {
		t.Errorf("version_seq is_nullable = %q, хотели NO", nullable)
	}
	if !strings.Contains(def, "nextval") {
		t.Errorf("version_seq default = %q, хотели nextval(...) — иначе вставка без "+
			"номера получит NULL и упадёт на NOT NULL", def)
	}

	// Индекс под листинг версий.
	var idxCount int
	if err := m.Pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_indexes
		  WHERE schemaname = current_schema() AND indexname = 'objects_key_versions'`).
		Scan(&idxCount); err != nil {
		t.Fatalf("индекс: %v", err)
	}
	if idxCount != 1 {
		t.Errorf("индекс objects_key_versions отсутствует")
	}

	// Старый индекс 0005 удалён: новый начинается с тех же колонок, и оставить
	// оба — значит писать на каждый PUT в два индекса с одним префиксом.
	if n := indexCount(t, ctx, m, "objects_versions"); n != 0 {
		t.Errorf("индекс objects_versions остался (строк %d): новый его покрывает, а "+
			"четвёртый индекс на (bucket_id, key) замедляет каждый PUT", n)
	}

	// Последовательность привязана к колонке: иначе на восстановленной из
	// pg_dump базе nextval() смотрит в объект, который никому не принадлежит.
	//
	// Фильтр по схеме обязателен: в кластере живут соседние тестовые схемы
	// (tapegw_test_backend, tapegw_test_iam, scratch-схема теста 0007), и без
	// него count() считает чужие последовательности — тест падает там, где
	// миграция исправна.
	var owned int
	if err := m.Pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_class s
		   JOIN pg_depend d ON d.objid = s.oid AND d.deptype = 'a'
		   JOIN pg_class t ON d.refobjid = t.oid
		   JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = d.refobjsubid
		  WHERE s.relname = 'objects_version_seq'
		    AND s.relnamespace = current_schema()::regnamespace
		    AND t.relname = 'objects' AND a.attname = 'version_seq'`).
		Scan(&owned); err != nil {
		t.Fatalf(" OWNED BY: %v", err)
	}
	if owned != 1 {
		t.Errorf("objects_version_seq не привязана к objects.version_seq через OWNED BY")
	}

	// Номера назначаются при вставке и растут в порядке вставки — даже когда
	// created_at одинаковый. Это и есть смысл колонки: now() в Postgres — время
	// начала транзакции, и на created_at порядок держать нельзя.
	const key = "three-versions"
	mustBucket(t, m, "seq-bucket")
	var bucketID int64
	if err := m.Pool.QueryRow(ctx,
		`SELECT id FROM buckets WHERE name = 'seq-bucket'`).Scan(&bucketID); err != nil {
		t.Fatalf("id бакета: %v", err)
	}
	var seqs []int64
	for i := 0; i < 3; i++ {
		var seq int64
		// Только последняя версия текущая: частичный уникальный индекс
		// objects_current не допускает двух is_current на ключ.
		cur := i == 2
		if err := m.Pool.QueryRow(ctx,
			`INSERT INTO objects (id, bucket_id, key, size, etag, storage_class,
			                      state, landing, online_until, tenant_id, pool_id, is_current)
			  VALUES ($1, $2, $3, 1, 'etag', 'GLACIER', 'pending', true,
			          now() + interval '1 day', 1, 1, $4)
			  RETURNING version_seq`,
			uuid.New(), bucketID, key, cur).Scan(&seq); err != nil {
			t.Fatalf("вставка версии %d: %v", i, err)
		}
		seqs = append(seqs, seq)
	}
	for i := 1; i < len(seqs); i++ {
		if seqs[i] <= seqs[i-1] {
			t.Fatalf("version_seq не растёт при вставке: %v", seqs)
		}
	}

	// Инвариант листинга: «текущая версия» ключа — это максимальный version_seq.
	// На нём держится и порядок «от новой к старой», и маркер пагинации.
	var curSeq int64
	if err := m.Pool.QueryRow(ctx,
		`SELECT version_seq FROM objects
		  WHERE bucket_id = $1 AND key = $2 AND is_current`, bucketID, key).Scan(&curSeq); err != nil {
		t.Fatalf("версия текущей строки: %v", err)
	}
	if curSeq != seqs[len(seqs)-1] {
		t.Errorf("текущая версия has version_seq = %d, а максимум = %d — "+
			"листинг отдаст не ту версию как текущую", curSeq, seqs[len(seqs)-1])
	}
	var maxSeq int64
	if err := m.Pool.QueryRow(ctx,
		`SELECT max(version_seq) FROM objects WHERE bucket_id = $1 AND key = $2`,
		bucketID, key).Scan(&maxSeq); err != nil {
		t.Fatalf("максимум: %v", err)
	}
	if curSeq != maxSeq {
		t.Errorf("текущая версия = %d, максимум по ключу = %d", curSeq, maxSeq)
	}

	// Down и повторный Up — текстом файла. Без этого «миграция вперёд»
	// проверяется на одном направлении, а Down гниёт незамеченным.
	if _, err := m.Pool.Exec(ctx, section(t, migration0009, "Down")); err != nil {
		t.Fatalf("Down 0009: %v", err)
	}
	if n := columnCount(t, ctx, m, "version_seq"); n != 0 {
		t.Errorf("после Down колонка version_seq осталась")
	}
	if n := seqCount(t, ctx, m, "objects_version_seq"); n != 0 {
		t.Errorf("после Down последовательность objects_version_seq осталась")
	}
	if n := indexCount(t, ctx, m, "objects_versions"); n != 1 {
		t.Errorf("после Down индекс objects_versions не возвращён (строк %d)", n)
	}
	if _, err := m.Pool.Exec(ctx, section(t, migration0009, "Up")); err != nil {
		t.Fatalf("повторный Up 0009: %v", err)
	}
	if n := columnCount(t, ctx, m, "version_seq"); n != 1 {
		t.Errorf("после повторного Up колонка version_seq не вернулась")
	}
}

func indexCount(t *testing.T, ctx context.Context, m *meta.Meta, idx string) int {
	t.Helper()
	var n int
	if err := m.Pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_indexes
		  WHERE schemaname = current_schema() AND indexname = $1`, idx).Scan(&n); err != nil {
		t.Fatalf("счётчик индексов: %v", err)
	}
	return n
}

func columnCount(t *testing.T, ctx context.Context, m *meta.Meta, col string) int {
	t.Helper()
	var n int
	if err := m.Pool.QueryRow(ctx,
		`SELECT count(*) FROM information_schema.columns
		  WHERE table_schema = current_schema() AND table_name = 'objects'
		    AND column_name = $1`, col).Scan(&n); err != nil {
		t.Fatalf("счётчик колонок: %v", err)
	}
	return n
}

func seqCount(t *testing.T, ctx context.Context, m *meta.Meta, seq string) int {
	t.Helper()
	var n int
	if err := m.Pool.QueryRow(ctx,
		`SELECT count(*) FROM information_schema.sequences
		  WHERE sequence_schema = current_schema() AND sequence_name = $1`, seq).
		Scan(&n); err != nil {
		t.Fatalf("счётчик последовательностей: %v", err)
	}
	return n
}
