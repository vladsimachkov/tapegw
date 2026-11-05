package meta_test

import (
	"testing"
	"time"

	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// TestMarkKEKSupersededKeepsFirstMoment — повторная ротация не сдвигает срок.
//
// COALESCE в UPDATE выглядит придирой, пока не подумаешь, что он считает: от
// этого момента зависит, когда оператору разрешат выбросить ключ. Сдвинь его при
// повторном запуске — и «безопаснее» превратится в молча удлинённый срок
// хранения, который никто не согласовывал.
func TestMarkKEKSupersededKeepsFirstMoment(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := t.Context()

	first := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	second := first.Add(30 * 24 * time.Hour)

	for _, v := range []int{1, 2, 3} {
		if err := m.EnsureKEKVersion(ctx, v); err != nil {
			t.Fatal(err)
		}
	}
	// Версия 1 уступила версии 2.
	if err := m.MarkKEKSuperseded(ctx, 1, first); err != nil {
		t.Fatal(err)
	}
	// Повторная отметка той же версии не должна двигать срок.
	if err := m.MarkKEKSuperseded(ctx, 1, second); err != nil {
		t.Fatal(err)
	}
	// Версия 3 ещё ни разу не была активной — отмечать её не за что.
	if err := m.MarkKEKSuperseded(ctx, 3, first); err != nil {
		t.Fatal(err)
	}

	v1, err := m.GetKEKVersion(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if v1.SupersededAt == nil {
		t.Fatal("версия 1 осталась без отметки")
	}
	if !v1.SupersededAt.Equal(first) {
		t.Errorf("момент уступки сдвинулся: %s, был %s — срок «когда можно выбросить» уехал в будущее",
			v1.SupersededAt.Format(time.RFC3339), first.Format(time.RFC3339))
	}
	v3, err := m.GetKEKVersion(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}
	if v3.SupersededAt == nil {
		t.Error("версия 3 не отмечена")
	}
}

// TestGetKEKVersionUnknown — неизвестная версия отличается от «есть, но без отметки».
func TestGetKEKVersionUnknown(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)

	if _, err := m.GetKEKVersion(t.Context(), 4242); err == nil {
		t.Fatal("ошибки нет, хотя версии не существует")
	}
}
