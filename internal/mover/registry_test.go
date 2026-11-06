package mover_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/mover"
)

// Состояние операции обязано переживать перезапуск mover'а: «упал посреди
// ленты» — штатное событие (libltfs падает во время mt offline, M0-07), и
// после старта процесс обязан отличить «продолжи» от «уже записано».
func TestRegistrySurvivesRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "staging")
	op := uuid.New()

	r1, err := mover.NewRegistry(dir, 1<<20, time.Hour, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	_, created, err := r1.Begin(op, 4000, "deadbeef", map[string]string{"tapegw.version": op.String()}, nil, "scheduler")
	if err != nil || !created {
		t.Fatalf("Begin: %v created=%v", err, created)
	}
	f, err := r1.DataFile(op)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(make([]byte, 1500)); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if err := r1.Update(op, func(r *mover.OpRecord) { r.Received = 1500 }); err != nil {
		t.Fatal(err)
	}

	// «Перезапуск»: новый Registry поверх того же каталога.
	r2, err := mover.NewRegistry(dir, 1<<20, time.Hour, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := r2.Get(op)
	if !ok {
		t.Fatalf("после перезапуска операция потеряна")
	}
	if got.Received != 1500 {
		t.Errorf("Received = %d, ожидали 1500: смещение берётся из файла, а не из памяти", got.Received)
	}
	if got.Size != 4000 || got.SHA256 != "deadbeef" {
		t.Errorf("параметры операции восстановились неверно: %+v", got)
	}
}

// Привязка операции к паре (size, sha256) должна жить столько же, сколько сама
// запись, — иначе повтор с другим содержимым пройдёт как «продолжи».
func TestRegistryRejectsDifferentContent(t *testing.T) {
	r, err := mover.NewRegistry(filepath.Join(t.TempDir(), "st"), 1<<20, time.Hour, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	op := uuid.New()
	if _, _, err := r.Begin(op, 100, "aaaa", nil, nil, "scheduler"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.Begin(op, 200, "aaaa", nil, nil, "scheduler"); err != mover.ErrOpConflict {
		t.Errorf("другой размер при том же op: ожидали ErrOpConflict, получили %v", err)
	}
	if _, _, err := r.Begin(op, 100, "bbbb", nil, nil, "scheduler"); err != mover.ErrOpConflict {
		t.Errorf("другая сумма при том же op: ожидали ErrOpConflict, получили %v", err)
	}
	// Тот же op и та же пара — обычное продолжение, без конфликта.
	if _, created, err := r.Begin(op, 100, "aaaa", nil, nil, "scheduler"); err != nil || created {
		t.Errorf("повтор с той же парой обязан быть продолжением: created=%v err=%v", created, err)
	}
}

// Файл без записи об операции — мусор: запись является источником истины,
// иначе «файл есть» и «операция есть» разойдутся после обрыва.
func TestRegistryDropsOrphanData(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "st")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	orphan := uuid.New()
	if err := os.WriteFile(filepath.Join(dir, orphan.String()+".bin"), []byte("мусор"), 0o600); err != nil {
		t.Fatal(err)
	}
	r, err := mover.NewRegistry(dir, 1<<20, time.Hour, time.Hour)
	if err != nil {
		t.Fatalf("сирота не должна мешать запуску: %v", err)
	}
	if _, ok := r.Get(orphan); ok {
		t.Errorf("сирота попала в реестр")
	}
}

// Битая запись реестра обязана остановить запуск, а не быть проигнорированной:
// молча потерять известие о записанном объекте — значит записать его дважды.
func TestRegistryRefusesCorruptRecord(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "st")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, uuid.New().String()+".json"), []byte("{ не json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := mover.NewRegistry(dir, 1<<20, time.Hour, time.Hour); err == nil {
		t.Errorf("битая запись обязана прерывать запуск mover'а")
	}
}
