// Package fake — реализация store.Store для тестов и разработки без железа.
//
// Главное в ней — не «in-memory хранилище», а управляемые сбои: правило 7 из
// AGENTS.md требует для каждого воркера тест «упасть после шага N, перезапустить,
// сверить». Для этого здесь есть FailAfter и задержки.
package fake

import (
	"context"
	"io"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/store"
)

// Fault описывает впрыскиваемый сбой.
type Fault struct {
	// FailOn — номер вызова Put/Get/Delete, на котором нужно сработать (1-based).
	// 0 означает «на любом».
	FailOn int
	// Err — ошибка, которую вернуть. Если nil, считается, что процесс «упал»:
	// состояние не меняется, но и результат не возвращается.
	Err error
	// Delay задерживает вызов — проверяем таймауты и отмену ctx.
	Delay time.Duration
}

// Store — in-memory реализация store.Store.
type Store struct {
	mu      sync.Mutex
	data    map[uuid.UUID]blob
	calls   map[string]int
	faults  map[string]*Fault
	Locate_ func(ids []uuid.UUID) ([]store.Location, error)
}

type blob struct {
	data    []byte
	version int
	attrs   map[string]string
}

// New создаёт пустое хранилище.
func New() *Store {
	return &Store{
		data:   make(map[uuid.UUID]blob),
		calls:  make(map[string]int),
		faults: make(map[string]*Fault),
	}
}

// InjectFault включает сбой для операции op ("put", "get", "delete").
func (s *Store) InjectFault(op string, f *Fault) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.faults[op] = f
}

// ResetFaults снимает все сбои.
func (s *Store) ResetFaults() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.faults = make(map[string]*Fault)
}

// Calls возвращает число вызовов операции — тесты сверяют, что воркер не
// сходил в Phobos дважды там, где это не идемпотентно.
func (s *Store) Calls(op string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[op]
}

func (s *Store) tick(op string) (int, *Fault) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls[op]++
	f := s.faults[op]
	return s.calls[op], f
}

// apply проверяет, надо ли сработать сбою на этом вызове.
func apply(op string, n int, f *Fault) (store.Result, bool) {
	if f == nil {
		return store.Result{}, false
	}
	if f.FailOn != 0 && f.FailOn != n {
		return store.Result{}, false
	}
	if f.Delay > 0 {
		time.Sleep(f.Delay)
	}
	if f.Err == nil {
		// Имитация «процесс умер посреди вызова»: ничего не меняем.
		return store.Result{}, true
	}
	return store.Result{Err: f.Err}, true
}

func (s *Store) Put(ctx context.Context, items []store.PutItem, opt store.PutOptions) []store.Result {
	n, f := s.tick("put")
	out := make([]store.Result, 0, len(items))
	for _, it := range items {
		if r, bad := apply("put", n, f); bad {
			r.ID = it.ID
			out = append(out, r)
			continue
		}
		data, err := readAll(it.File)
		if err != nil {
			out = append(out, store.Result{ID: it.ID, Err: err})
			continue
		}
		s.mu.Lock()
		s.data[it.ID] = blob{data: data, version: 1, attrs: cloneAttrs(it.Attrs)}
		s.mu.Unlock()
		out = append(out, store.Result{ID: it.ID, Version: 1})
	}
	return out
}

func (s *Store) Get(ctx context.Context, items []store.GetItem) []store.Result {
	n, f := s.tick("get")
	out := make([]store.Result, 0, len(items))
	for _, it := range items {
		if r, bad := apply("get", n, f); bad {
			r.ID = it.ID
			out = append(out, r)
			continue
		}
		s.mu.Lock()
		b, ok := s.data[it.ID]
		s.mu.Unlock()
		if !ok {
			out = append(out, store.Result{ID: it.ID, Err: store.ErrNotFound})
			continue
		}
		if _, err := it.File.Write(b.data); err != nil {
			out = append(out, store.Result{ID: it.ID, Err: err})
			continue
		}
		out = append(out, store.Result{ID: it.ID, Version: b.version})
	}
	return out
}

func (s *Store) Stat(ctx context.Context, id uuid.UUID) (store.Meta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.data[id]
	if !ok {
		return store.Meta{}, store.ErrNotFound
	}
	return store.Meta{ID: id, Size: int64(len(b.data)), Version: b.version, Attrs: cloneAttrs(b.attrs)}, nil
}

func (s *Store) Delete(ctx context.Context, ids []uuid.UUID) []store.Result {
	n, f := s.tick("delete")
	out := make([]store.Result, 0, len(ids))
	for _, id := range ids {
		if r, bad := apply("delete", n, f); bad {
			r.ID = id
			out = append(out, r)
			continue
		}
		s.mu.Lock()
		delete(s.data, id)
		s.mu.Unlock()
		// Повторное удаление уже удалённого объекта — успех, а не ошибка:
		// deleter обязан быть идемпотентным.
		out = append(out, store.Result{ID: id})
	}
	return out
}

func (s *Store) Locate(ctx context.Context, ids []uuid.UUID) ([]store.Location, error) {
	if s.Locate_ == nil {
		return nil, store.ErrNotSupported
	}
	return s.Locate_(ids)
}

func (s *Store) Close() error { return nil }

// Len — сколько объектов сейчас «на ленте». Для проверок в тестах.
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.data)
}

func readAll(f *os.File) ([]byte, error) {
	if f == nil {
		return nil, nil
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	return io.ReadAll(f)
}

func cloneAttrs(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

var _ store.Store = (*Store)(nil)
