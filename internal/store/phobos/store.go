//go:build phobos

// Package phobos — слой данных поверх libphobos_store (ADR-0006).
//
// Все вызовы libphobos принадлежат одной горутине-владельцу: она в начале
// захватывает поток ОС (runtime.LockOSThread) и никогда не снимает захват.
// Мьютекс дал бы последовательность, но не гарантию одного потока: горутина
// между вызовами переезжает на другой поток, а спайк M0-03 не проверял, держит
// ли библиотека thread-local состояние. См. поправку в ADR-0007.
package phobos

/*
#cgo pkg-config: phobos
#include "shim.h"
*/
import "C"

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/store"
)

// request — один вызов в C. Ответ приходит всегда, в том числе при ошибке.
type request struct {
	call func() (any, error)
	ch   chan result
}

type result struct {
	val any
	err error
}

// Store — подключение к Phobos. Один процесс = один Store: контекст библиотеки
// глобальный (спайк M0-03).
type Store struct {
	reqs  chan *request
	ready chan struct{}
	done  chan struct{}

	// cfgPath — конфиг Phobos, с которым инициализирован libphobos. CLI
	// получает его явно (cliEnv), иначе «записали одним конфигом, ищем другим».
	cfgPath string

	initErr error

	tidMu sync.Mutex
	tids  []int64 // tid каждого вызова в C; см. спайк M0-13
}

// TIDs — потоки ОС, из которых были вызовы в C. По спайку M0-13 их обязан быть
// ровно один: это проверяют фактом из C, а не уверенностью в коде Go.
func (s *Store) TIDs() []int64 {
	s.tidMu.Lock()
	defer s.tidMu.Unlock()
	return append([]int64(nil), s.tids...)
}

func (s *Store) noteTID(tid C.long) {
	s.tidMu.Lock()
	s.tids = append(s.tids, int64(tid))
	s.tidMu.Unlock()
}

// Open вызывает phobos_init внутри горутины-владельца.
func Open(ctx context.Context) (*Store, error) {
	return OpenWithConfig(ctx, os.Getenv("PHOBOS_CFG_FILE"))
}

// OpenWithConfig — то же, но конфиг задан явно: mover обязан инициализировать
// libphobos и вызывать CLI одним и тем же файлом.
func OpenWithConfig(ctx context.Context, cfgPath string) (*Store, error) {
	s := &Store{
		reqs:    make(chan *request),
		ready:   make(chan struct{}),
		done:    make(chan struct{}),
		cfgPath: cfgPath,
	}
	go s.owner()
	select {
	case <-s.ready:
		if s.initErr != nil {
			return nil, s.initErr
		}
		return s, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// owner — горутина-владельца. Захват потока не снимается никогда: вместе с
// горутиной умирает и поток.
func (s *Store) owner() {
	runtime.LockOSThread()

	rc := C.tapegw_init()
	if rc != 0 {
		s.initErr = fmt.Errorf("phobos: phobos_init: %w", syscall.Errno(-rc))
	}
	close(s.ready)
	if rc != 0 {
		<-s.done
		return
	}

	for r := range s.reqs {
		v, err := r.call()
		r.ch <- result{val: v, err: err}
	}
	C.tapegw_fini()
	close(s.done)
}

// do отправляет вызов владельцу и ждёт ответа. Прервать уже начатый вызов C
// нельзя (ADR-0021 про отсутствие cancel), поэтому контекст проверяется до.
func (s *Store) do(ctx context.Context, call func() (any, error)) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r := &request{call: call, ch: make(chan result, 1)}
	select {
	case s.reqs <- r:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case res := <-r.ch:
		return res.val, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Close завершает: phobos_fini выполняется тем же потоком-владельцем.
func (s *Store) Close() error {
	select {
	case <-s.done:
		return nil
	default:
		close(s.reqs)
	}
	<-s.done
	return nil
}

// Put записывает объекты. Перемотка файла делается здесь, внутри горутины-
// владельца, непосредственно перед вызовом C: иначе между перемоткой и вызовом
// файл формально доступен другому коду, и «позиция в начале» держится на
// дисциплине, а не на построении.
func (s *Store) Put(ctx context.Context, items []store.PutItem, opt store.PutOptions) []store.Result {
	out := make([]store.Result, 0, len(items))
	for _, it := range items {
		o := opt
		_, err := s.do(ctx, func() (any, error) { return nil, s.putOne(it, o) })
		out = append(out, store.Result{ID: it.ID, Err: err})
	}
	return out
}

// familyOf — имя семейства из конфига в enum Phobos.
func familyOf(name string) int {
	if name == "dir" {
		return C.PHO_RSC_DIR
	}
	return C.PHO_RSC_TAPE
}

func cstr(s string) *C.char {
	if s == "" {
		return nil
	}
	c := C.CString(s)
	return c
}

func (s *Store) putOne(it store.PutItem, opt store.PutOptions) error {
	if err := store.Rewind(it.File); err != nil {
		return err
	}
	id := C.CString(it.ID.String())
	defer C.free(unsafe.Pointer(id))

	keys, vals, freeAttrs := cAttrs(it.Attrs)
	defer freeAttrs()

	// Параметры layout передаются явно: без них Phobos берёт значение по
	// умолчанию, а raid1 без repl_count требует два устройства и на одном
	// даёт ENODEV без какой-либо ошибки в журнале phobosd.
	lkeys, lvals, freeLyt := cAttrs(opt.LayoutParams)
	defer freeLyt()

	runtime.KeepAlive(it.File)
	var tid C.long
	cl := cstr(opt.Layout)
	clib := cstr(opt.Library)
	defer C.free(unsafe.Pointer(cl)) // C.free(nil) безопасен
	defer C.free(unsafe.Pointer(clib))

	rc := C.tapegw_put(id, C.int(it.File.Fd()), C.long(it.Size),
		C.int(familyOf(opt.Family)), clib, cl, keys, vals,
		C.int(len(it.Attrs)), lkeys, lvals, C.int(len(opt.LayoutParams)), &tid)
	s.noteTID(tid)
	switch {
	case rc == 0:
		return nil
	case syscall.Errno(-rc) == syscall.EEXIST:
		return store.ErrExists
	case syscall.Errno(-rc) == syscall.ENOSPC:
		return store.ErrNoSpace
	default:
		return fmt.Errorf("phobos: put %s: %w", it.ID, syscall.Errno(-rc))
	}
}

// Stat читает метаданные объекта.
func (s *Store) Stat(ctx context.Context, id uuid.UUID) (store.Meta, error) {
	v, err := s.do(ctx, func() (any, error) { return s.statOne(id) })
	if err != nil {
		return store.Meta{}, err
	}
	meta := v.(store.Meta)

	// Размер берётся из CLI, а не из getmd: phobos_getmd отдаёт только
	// пользовательские атрибуты, xt_size для GETMD не заполняется (замерено на
	// ci-dir: put проходит, Stat рапортует 0). CLI — отдельный процесс, а не
	// вызов libphobos, поэтому запускается НЕ из горутины-владельца: запуск
	// Python заблокировал бы владельца на всё время старта, ничего не дав
	// взамен. ADR-0008 соблюдён: тот же источник, что и для носителя.
	ei, err := extentList(ctx, id, s.cfgPath)
	if err != nil {
		return store.Meta{}, err
	}
	if len(ei.Size) == 0 {
		// Объект есть (getmd нашёл), экстентов нет — незавершённая запись после
		// обрыва. Это не ошибка Stat: именно так migrator должен отличить
		// «долить» от «уже лежит целое».
		meta.Complete = false
		return meta, nil
	}
	// Экстенты отдаём как есть. Sumму не считаем: 94 — это и две реплики по 47,
	// и один объект на 94, а для разбора EEXIST это противоположные случаи.
	// Проверяет их тот, у кого есть план (store.CheckExtents).
	meta.Extents = make([]store.Extent, 0, len(ei.Size))
	for i, n := range ei.Size {
		e := store.Extent{Size: n, State: ei.State[i]}
		if i < len(ei.MediaName) {
			e.Media = ei.MediaName[i]
		}
		meta.Extents = append(meta.Extents, e)
	}
	meta.Complete = allSync(ei.State)
	return meta, nil
}

func allSync(states []string) bool {
	for _, st := range states {
		if st != "sync" {
			return false
		}
	}
	return true
}

// extentInfo — строка вывода `phobos extent list -f json`. Без `-o` CLI
// выдаёт один oid, поэтому поля перечислены явно (см. docs/ops/tape-library.md).
type extentInfo struct {
	Size      []int64  `json:"size"`
	MediaName []string `json:"media_name"`
	State     []string `json:"state"`
}

// cliEnv — окружение для CLI Phobos. PHOBOS_CFG_FILE передаётся явно:
// полагаться на унаследованное нельзя, потому что libphobos внутри mover
// инициализирован конкретным конфигом, а CLI по умолчанию читает свой.
// Расхождение выглядит как «объект записан, но extent list его не видит».
func cliEnv(cfgPath string) []string {
	// Наследованное значение удаляется всегда: libphobos инициализирован
	// конкретным конфигом (или значением по умолчанию), а CLI иначе прочитал бы
	// то, что лил в окружение кто-то снаружи.
	env := os.Environ()
	out := make([]string, 0, len(env)+1)
	for _, kv := range env {
		if !strings.HasPrefix(kv, "PHOBOS_CFG_FILE=") {
			out = append(out, kv)
		}
	}
	if cfgPath != "" {
		out = append(out, "PHOBOS_CFG_FILE="+cfgPath)
	}
	return out
}

func extentList(ctx context.Context, id uuid.UUID, cfgPath string) (extentInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var out, errb bytes.Buffer
	cmd := exec.CommandContext(ctx, "phobos", "extent", "list", "-f", "json",
		"-o", "oid,size,media_name,state", id.String())
	cmd.Env = cliEnv(cfgPath)
	cmd.Stdout = &out
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		return extentInfo{}, fmt.Errorf("phobos extent list %s: %w: %s",
			id, err, strings.TrimSpace(errb.String()))
	}
	trimmed := bytes.TrimSpace(out.Bytes())
	if len(trimmed) == 0 {
		// Пустой вывод — не ошибка разбора, а «экстентов нет». Именно так
		// выглядит объект, записанный с обрывом: он уже в таблице объектов
		// (повторный put даёт EEXIST), но ни одного экстента у него нет.
		return extentInfo{}, nil
	}
	var rows []extentInfo
	if err := json.Unmarshal(trimmed, &rows); err != nil {
		return extentInfo{}, fmt.Errorf("phobos extent list %s: разбор вывода: %w", id, err)
	}
	if len(rows) != 1 {
		return extentInfo{}, fmt.Errorf("phobos extent list %s: записей %d, ждём 1", id, len(rows))
	}
	return rows[0], nil
}

func (s *Store) statOne(id uuid.UUID) (store.Meta, error) {
	cid := C.CString(id.String())
	defer C.free(unsafe.Pointer(cid))

	var size C.long
	var version C.int
	var found C.int
	var tid C.long
	rc := C.tapegw_getmd(cid, &size, &version, &found, &tid)
	s.noteTID(tid)
	if rc != 0 {
		return store.Meta{}, fmt.Errorf("phobos: stat %s: %w", id, syscall.Errno(-rc))
	}
	if found == 0 {
		return store.Meta{}, store.ErrNotFound
	}
	// Size и Complete заполняет вызвавший: getmd их не отдаёт, а придумывать
	// здесь «Complete: true» означало бы считать копию целой без проверки.
	_ = size
	return store.Meta{ID: id, Version: int(version)}, nil
}

// cAttrs — массивы C-строк для pho_attr_set. Освобождение — одним вызовом,
// чтобы ни один путь выхода не забыл про него.
func cAttrs(attrs map[string]string) (**C.char, **C.char, func()) {
	n := len(attrs)
	if n == 0 {
		return nil, nil, func() {}
	}
	keys := (**C.char)(C.malloc(C.size_t(n) * C.size_t(unsafe.Sizeof((*C.char)(nil)))))
	vals := (**C.char)(C.malloc(C.size_t(n) * C.size_t(unsafe.Sizeof((*C.char)(nil)))))
	ks := unsafe.Slice(keys, n)
	vs := unsafe.Slice(vals, n)
	i := 0
	for k, v := range attrs {
		ks[i] = C.CString(k)
		vs[i] = C.CString(v)
		i++
	}
	return keys, vals, func() {
		for j := 0; j < i; j++ {
			C.free(unsafe.Pointer(ks[j]))
			C.free(unsafe.Pointer(vs[j]))
		}
		C.free(unsafe.Pointer(keys))
		C.free(unsafe.Pointer(vals))
	}
}

// Get и Delete — в 1a-2. Отвечают явно: «не реализовано» честнее, чем
// молчаливая заглушка (правило 4).
func (s *Store) Get(ctx context.Context, items []store.GetItem) []store.Result {
	res := make([]store.Result, len(items))
	for i, it := range items {
		res[i] = store.Result{ID: it.ID, Err: s.getOne(ctx, it)}
	}
	return res
}

func (s *Store) getOne(ctx context.Context, it store.GetItem) error {
	// Файл подготавливает само хранилище: Phobos пишет в fd с текущей позиции,
	// а буфер мог быть прочитан до конца (см. store.PrepareForWrite).
	if err := store.PrepareForWrite(it.File); err != nil {
		return err
	}
	_, err := s.do(ctx, func() (any, error) {
		cid := C.CString(it.ID.String())
		defer C.free(unsafe.Pointer(cid))
		runtime.KeepAlive(it.File)
		var tid C.long
		rc := C.tapegw_get(cid, C.int(it.File.Fd()), 1 /* DSS_OBJ_ALIVE */, &tid)
		s.noteTID(tid)
		if rc != 0 {
			return nil, fmt.Errorf("phobos: get %s: %w", it.ID, syscall.Errno(-rc))
		}
		return nil, nil
	})
	return err
}

func (s *Store) Delete(ctx context.Context, ids []uuid.UUID) []store.Result {
	res := make([]store.Result, len(ids))
	for i, id := range ids {
		res[i] = store.Result{ID: id, Err: s.deleteOne(ctx, id)}
	}
	return res
}

func (s *Store) deleteOne(ctx context.Context, id uuid.UUID) error {
	_, err := s.do(ctx, func() (any, error) {
		cid := C.CString(id.String())
		defer C.free(unsafe.Pointer(cid))
		var tid C.long
		rc := C.tapegw_delete(cid, &tid)
		s.noteTID(tid)
		switch {
		case rc == 0:
			return nil, nil
		// Уже удалён — не ошибка: воркер повторяет шаг после падения и должен
		// сходиться к том же результату.
		case syscall.Errno(-rc) == syscall.ENOENT:
			return nil, nil
		}
		return nil, fmt.Errorf("phobos: delete %s: %w", id, syscall.Errno(-rc))
	})
	return err
}

func (s *Store) Extents(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID][]string, error) {
	// Носитель узнаётся при архивации через CLI (ADR-0008): libpho_core не
	// линкуем. Вызов редкий и идёт вне горутины-владельца — он не вызывает
	// libphobos, а значит не обязан ждать владельца.
	out := make(map[uuid.UUID][]string, len(ids))
	for _, id := range ids {
		ei, err := extentList(ctx, id, s.cfgPath)
		if err != nil {
			return nil, err
		}
		out[id] = ei.MediaName
	}
	return out, nil
}

// Locate в протоколе TLC отсутствует (спайк M0-04): переместить ленту по
// запросу из libphobos нельзя. Возвращаем явный отказ — «молча сделать
// ничего» здесь означало бы, что воркер посчитал операцию выполненной.
func (s *Store) Locate(context.Context, []uuid.UUID) ([]store.Location, error) {
	return nil, store.ErrNotSupported
}
