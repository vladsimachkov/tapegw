// Package fake — реализация store.Store для тестов и разработки без железа.
//
// Главное в ней — не «in-memory хранилище», а правдоподобие отказов. Правило 7
// из AGENTS.md требует для каждого воркера тест «упасть после шага N,
// перезапустить, сверить», и тесты migrator на fake не должны проходить там,
// где на настоящем Phobos всё упало. Поэтому смоделированы четыре поведения,
// которые про Phobos известны из спайков, а не выдуманы:
//
//   - батч атомарен: ошибка на батче не оставляет половины записанных объектов;
//   - после обрыва объект может оказаться в индексе с copy_status = incomplete
//     (M0-02) — «есть» и «полон» это разные условия;
//   - ENOSPC не имеет побочных эффектов: не влезло — не записано ничего;
//   - повторная запись того же oid — EEXIST, а не перезапись.
package fake

import (
	"context"
	"fmt"
	"io"
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
	// Err — ошибка, которую вернуть. Если nil, считается, что процесс «упал»
	// посреди копирования: объекты оказываются в индексе, но неполными.
	Err error
	// Delay задерживает вызов — проверяем таймауты и отмену ctx.
	Delay time.Duration
}

// Store — in-memory реализация store.Store.
type Store struct {
	mu   sync.Mutex
	data map[uuid.UUID]blob
	// capacity — сколько байт «помещается на ленту». По умолчанию безразмерна.
	capacity int64
	used     int64
	// mediaSeq нумерует носители: Extents обязан отвечать имя ленты, а не тег
	// пула. Тег — как лента помечена, носитель — где она лежит (ADR-0008).
	mediaSeq  int
	nextMedia []string
	calls     map[string]int
	faults    map[string]*Fault
	Locate_   func(ids []uuid.UUID) ([]store.Location, error)
	Extents_  func(ids []uuid.UUID) (map[uuid.UUID][]string, error)
}

type blob struct {
	data     []byte
	version  int
	complete bool
	attrs    map[string]string
	media    []string
}

// New создаёт пустое хранилище.
func New() *Store {
	return &Store{
		data:   make(map[uuid.UUID]blob),
		calls:  make(map[string]int),
		faults: make(map[string]*Fault),
	}
}

// SetCapacity ограничивает объём «ленты» в байтах. Нужно, чтобы проверить
// ветку ENOSPC: батч, который не влезает целиком, не должен записать ничего.
func (s *Store) SetCapacity(n int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.capacity = n
}

// SetNextMedia задаёт имя носителя для следующей успешной записи. Нужно,
// чтобы проверить ADR-0008 (носитель узнаётся при архивации) и repl_count = 2,
// когда Phobos кладёт две реплики и extent list отдаёт массив.
func (s *Store) SetNextMedia(media []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextMedia = append([]string(nil), media...)
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

// shouldFire проверяет, надо ли сработать сбою на этом вызове.
func shouldFire(n int, f *Fault) bool {
	if f == nil {
		return false
	}
	if f.FailOn != 0 && f.FailOn != n {
		return false
	}
	if f.Delay > 0 {
		time.Sleep(f.Delay)
	}
	return true
}

// Put пишет батч. Семантика скопирована с поведения Phobos, а не упрощена:
//
//   - атрибуты проверяются на бюджет LTFS до любой записи (ADR-0020, M2-01);
//   - если хоть один oid уже существует, отказывает весь батч (EEXIST):
//     migrator обязан исключить подтверждённые объекты перед повтором;
//   - если батч не влезает в ёмкость, не записывается ничего (ENOSPC без
//     побочных эффектов);
//   - сбой с Err == nil имитирует смерть процесса посреди копирования:
//     объекты появляются в индексе, но complete = false.
func (s *Store) Put(ctx context.Context, items []store.PutItem, opt store.PutOptions) []Result {
	n, f := s.tick("put")
	out := make([]Result, 0, len(items))

	// 1. Проверка атрибутов — до обращения к «ленте».
	payload := make([][]byte, len(items))
	var total int64
	for i, it := range items {
		if sz := store.AttrSize(it.Attrs); sz > store.AttrBudgetBytes {
			out = append(out, Result{ID: it.ID, Err: store.ErrAttrTooLarge})
			continue
		}
		data, err := readAll(it.Reader)
		if err != nil {
			out = append(out, Result{ID: it.ID, Err: err})
			continue
		}
		payload[i] = data
		total += int64(len(data))
	}
	if len(out) > 0 {
		// Часть объектов отбракована до xfer: батч не отправляется вовсе,
		// иначе «ошибка конфигурации» перепутается с отказом ленты.
		return out
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// 2. EEXIST на любом объекте отсекает весь батч.
	for _, it := range items {
		if _, ok := s.data[it.ID]; ok {
			for _, it := range items {
				out = append(out, Result{ID: it.ID, Err: store.ErrExists})
			}
			return out
		}
	}

	// 3. Ёмкость: не влезло — не записано ничего.
	if s.capacity > 0 && s.used+total > s.capacity {
		for _, it := range items {
			out = append(out, Result{ID: it.ID, Err: store.ErrNoSpace})
		}
		return out
	}

	// 4. Сбой на батче. Атомарность: при ошибке не записано ничего; при
	// «смерти процесса» объекты остаются в индексе неполными.
	if shouldFire(n, f) {
		if f.Err != nil {
			for _, it := range items {
				out = append(out, Result{ID: it.ID, Err: f.Err})
			}
			return out
		}
		for i, it := range items {
			s.data[it.ID] = blob{data: payload[i], version: 1, complete: false, attrs: cloneAttrs(it.Attrs)}
			s.used += int64(len(payload[i]))
			out = append(out, Result{ID: it.ID, Version: 1, Err: store.ErrIncomplete})
		}
		return out
	}

	media := s.nextMedia
	if len(media) == 0 {
		s.mediaSeq++
		media = []string{fmt.Sprintf("TAPE%03d", s.mediaSeq)}
	}
	s.nextMedia = nil

	for i, it := range items {
		s.data[it.ID] = blob{
			data:     payload[i],
			version:  1,
			complete: true,
			attrs:    cloneAttrs(it.Attrs),
			media:    append([]string(nil), media...),
		}
		s.used += int64(len(payload[i]))
		out = append(out, Result{ID: it.ID, Version: 1})
	}
	return out
}

// Result — итог по одному объекту батча (псевдоним, чтобы вызовы читались
// коротко: store.Result разворачивается в fake.Result).
type Result = store.Result

func (s *Store) Get(ctx context.Context, items []store.GetItem) []Result {
	n, f := s.tick("get")
	out := make([]Result, 0, len(items))
	for _, it := range items {
		if shouldFire(n, f) {
			out = append(out, Result{ID: it.ID, Err: faultErr(f)})
			continue
		}
		s.mu.Lock()
		b, ok := s.data[it.ID]
		s.mu.Unlock()
		if !ok {
			out = append(out, Result{ID: it.ID, Err: store.ErrNotFound})
			continue
		}
		if !b.complete {
			// Неполную копию читать нельзя: вернём ошибку, а не мусор.
			out = append(out, Result{ID: it.ID, Err: store.ErrIncomplete})
			continue
		}
		if _, err := it.Writer.Write(b.data); err != nil {
			out = append(out, Result{ID: it.ID, Err: err})
			continue
		}
		out = append(out, Result{ID: it.ID, Version: b.version})
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
	return store.Meta{
		ID:       id,
		Size:     int64(len(b.data)),
		Version:  b.version,
		Complete: b.complete,
		Attrs:    cloneAttrs(b.attrs),
	}, nil
}

func (s *Store) Delete(ctx context.Context, ids []uuid.UUID) []Result {
	n, f := s.tick("delete")
	out := make([]Result, 0, len(ids))
	for _, id := range ids {
		if shouldFire(n, f) {
			out = append(out, Result{ID: id, Err: faultErr(f)})
			continue
		}
		s.mu.Lock()
		if b, ok := s.data[id]; ok {
			s.used -= int64(len(b.data))
			delete(s.data, id)
		}
		s.mu.Unlock()
		// Повторное удаление уже удалённого объекта — успех, а не ошибка:
		// deleter обязан быть идемпотентным.
		out = append(out, Result{ID: id})
	}
	return out
}

func (s *Store) Extents(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID][]string, error) {
	if s.Extents_ != nil {
		return s.Extents_(ids)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[uuid.UUID][]string, len(ids))
	for _, id := range ids {
		b, ok := s.data[id]
		if !ok {
			continue
		}
		out[id] = append([]string(nil), b.media...)
	}
	return out, nil
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

// Complete — «дописан» ли объект. Тесты проверяют, что воркер не принял
// неполную копию за архивную.
func (s *Store) Complete(id uuid.UUID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.data[id]
	return ok && b.complete
}

// MarkComplete доводит неполную копию до полной — так моделируется успешный
// повтор после обрыва.
func (s *Store) MarkComplete(id uuid.UUID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if b, ok := s.data[id]; ok {
		b.complete = true
		s.data[id] = b
	}
}

// Used — сколько байт занято.
func (s *Store) Used() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.used
}

func faultErr(f *Fault) error {
	if f == nil || f.Err == nil {
		return store.ErrBusy
	}
	return f.Err
}

func readAll(r io.Reader) ([]byte, error) {
	if r == nil {
		return nil, nil
	}
	return io.ReadAll(r)
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
