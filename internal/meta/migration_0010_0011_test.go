package meta_test

import (
	"context"
	"testing"

	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// Миграции 0010–0011: значение 'marker' и его закрепление в схеме.
//
// Проверяются три вещи, и каждая — про конкретный дефект:
//   - 'marker' есть в enum (без него вставка маркера падает);
//   - CHECK is_delete_marker = (state='marker') — флаг и состояние не
//     разойдутся, и janitor не примет маркер за терминальную строку;
//   - CHECK на retired_at — у живущей версии срока «на выброс» нет.
//
// Откат 0010 невозможен (Postgres не удаёт значения enum), поэтому Down там
// закомментирован — это проверяет тест на текст файла миграции.
func TestMigrationMarkerState(t *testing.T) {
	m := testdb.MigrationScratch(t, testdb.AllVersions)
	ctx := context.Background()

	var n int
	if err := m.Pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_enum e
		   JOIN pg_type t ON t.oid = e.enumtypid
		  WHERE t.typname = 'obj_state' AND e.enumlabel = 'marker'
		    AND t.typnamespace = current_schema()::regnamespace`).Scan(&n); err != nil {
		t.Fatalf("спрос про enum: %v", err)
	}
	if n != 1 {
		t.Errorf("значения 'marker' в enum obj_state нет — миграция 0010 не применилась")
	}

	for _, name := range []string{"objects_marker_state", "objects_marker_not_retired"} {
		if err := m.Pool.QueryRow(ctx,
			`SELECT count(*) FROM pg_constraint c
			   JOIN pg_namespace ns ON ns.oid = c.connamespace
			  WHERE c.conname = $1 AND ns.oid = current_schema()::regnamespace`,
			name).Scan(&n); err != nil {
			t.Fatalf("спрос про ограничение %s: %v", name, err)
		}
		if n != 1 {
			t.Errorf("ограничения %s нет — миграция 0011 не применилась", name)
		}
	}
}
