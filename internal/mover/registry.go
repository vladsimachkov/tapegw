package mover

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Registry — реестр операций mover'а. Он лежит на диске не для порядка:
// «mover умер посреди ленты» — штатное событие (libltfs падает во время
// mt offline, см. M0-07), и после перезапуска процесс обязан отличить
// «продолжи передачу» от «это уже записано» от «этого я не видел».
//
// Привязка операции к паре (size, sha256) — то, что делает повтор честным:
// без неё op превратился бы в указатель на буфер, который можно переприсвоить,
// и различить «продолжи» и «забудь и запиши другое» было бы нельзя.
type Registry struct {
	dir    string
	bufMax int64
	resTTL time.Duration
	opTTL  time.Duration
	mu     sync.Mutex
	ops    map[uuid.UUID]*OpRecord
	used   int64
}

// OpRecord — то, что переживает перезапуск mover'а.
type OpRecord struct {
	Op        uuid.UUID `json:"op"`
	Size      int64     `json:"size"`
	SHA256    string    `json:"sha256"`
	Received  int64     `json:"received"`
	State     State     `json:"state"`
	Code      Code      `json:"code,omitempty"`
	Version   int       `json:"version,omitempty"`
	Extents   []string  `json:"extents,omitempty"`
	SHA256Out string    `json:"sha256_out,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`

	// Attrs и Tags нужны, чтобы дописать операцию после переподключения без
	// повторного объявления. Ключей здесь нет: Attrs содержит обёрнутый DEK,
	// который mover переносит как непрозрачные данные (ADR-0007).
	Attrs map[string]string `json:"attrs,omitempty"`
	Tags  []string          `json:"tags,omitempty"`
}

// NewRegistry открывает каталог буфера и поднимает из него незавершённые
// операции. Файлы без записи считаются мусором и удаляются: запись — источник
// истины, а не наличие файла.
func NewRegistry(dir string, bufferMax int64, resultTTL, opTTL time.Duration) (*Registry, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("mover: каталог буфера: %w", err)
	}
	r := &Registry{
		dir:    dir,
		bufMax: bufferMax,
		resTTL: resultTTL,
		opTTL:  opTTL,
		ops:    make(map[uuid.UUID]*OpRecord),
	}
	if err := r.load(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Registry) load() error {
	ents, err := os.ReadDir(r.dir)
	if err != nil {
		return fmt.Errorf("mover: чтение буфера: %w", err)
	}
	for _, e := range ents {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(r.dir, e.Name()))
		if err != nil {
			return fmt.Errorf("mover: запись операции: %w", err)
		}
		var rec OpRecord
		if err := json.Unmarshal(b, &rec); err != nil {
			// Битая запись — хуже отсутствия: молча потерять известие о
			// записанном объекте нельзя. Пусть это будет ошибка запуска.
			return fmt.Errorf("mover: битая запись %s: %w", e.Name(), err)
		}
		fi, err := os.Stat(r.dataPath(rec.Op))
		rec.Received = 0
		if err == nil {
			rec.Received = fi.Size()
		} else if rec.State == StateStaged {
			// Файла нет, а операция «в буфере» — значит её уже подчистили.
			rec.State = StateFailed
			rec.Code = CodeExpired
		}
		r.ops[rec.Op] = &rec
		r.used += rec.Received
	}
	return nil
}

func (r *Registry) dataPath(op uuid.UUID) string {
	return filepath.Join(r.dir, op.String()+".bin")
}

func (r *Registry) metaPath(op uuid.UUID) string {
	return filepath.Join(r.dir, op.String()+".json")
}

// persist пишет запись атомарно: во временный файл и rename. Прерванная запись
// реестра не должна оставлять полудокумент, который load() отвергнет и уронит
// запуск mover'а.
func (r *Registry) persist(rec *OpRecord) error {
	rec.UpdatedAt = time.Now()
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	tmp := r.metaPath(rec.Op) + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return fmt.Errorf("mover: запись реестра: %w", err)
	}
	if err := os.Rename(tmp, r.metaPath(rec.Op)); err != nil {
		return fmt.Errorf("mover: публикация реестра: %w", err)
	}
	return nil
}

// Begin регистрирует операцию или возвращает уже известную.
//
// Возвращает (rec, newlyCreated, error). Если операция известна с другой парой
// (size, sha256) — ErrOpConflict: повтор с другими значениями есть ошибка
// вызывающей стороны, а не «новое содержимое этой же операции».
func (r *Registry) Begin(op uuid.UUID, size int64, sum string, attrs map[string]string, tags []string) (*OpRecord, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if rec, ok := r.ops[op]; ok {
		if rec.Size != size || rec.SHA256 != sum {
			return rec, false, ErrOpConflict
		}
		return rec, false, nil
	}
	if r.bufMax > 0 && size > r.bufMax {
		return nil, false, ErrBufferTooSmall
	}
	rec := &OpRecord{
		Op:        op,
		Size:      size,
		SHA256:    sum,
		State:     StateStaged,
		CreatedAt: time.Now(),
		Attrs:     attrs,
		Tags:      tags,
	}
	if err := r.persist(rec); err != nil {
		return nil, false, err
	}
	r.ops[op] = rec
	return rec, true, nil
}

// Get возвращает запись по UUID операции.
func (r *Registry) Get(op uuid.UUID) (*OpRecord, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.ops[op]
	return rec, ok
}

// Update меняет запись под указанным замком и пишет её на диск.
func (r *Registry) Update(op uuid.UUID, fn func(*OpRecord)) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.ops[op]
	if !ok {
		return fmt.Errorf("mover: операция %s не известна", op)
	}
	prev := rec.Received
	fn(rec)
	r.used += rec.Received - prev
	return r.persist(rec)
}

// Free — сколько места в буфере осталось. Нужно и для handshake, и для
// кредитов: переполнение буфера есть «подожди», а не ошибка записи.
func (r *Registry) Free() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.bufMax == 0 {
		return 1 << 40 // буфер безразмерен — кредитов хватает всегда
	}
	free := r.bufMax - r.used
	if free < 0 {
		return 0
	}
	return free
}

// ReleaseData убирает файл буфера, но **оставляет запись** об операции.
// После успешной записи на ленту держать копию незачем, а запись нужна:
// именно по ней повтор после обрыва отвечает «уже записано» вместо второй
// записи на носитель.
func (r *Registry) ReleaseData(op uuid.UUID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.ops[op]
	if !ok {
		return nil
	}
	if err := os.Remove(r.dataPath(op)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("mover: удаление буфера: %w", err)
	}
	r.used -= rec.Received
	rec.Received = 0
	return r.persist(rec)
}

// Drop убирает операцию и её файл. Вызывается по истечении TTL и на
// неудачной передаче, которую повтор начнёт с нуля.
func (r *Registry) Drop(op uuid.UUID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.ops[op]
	if !ok {
		return nil
	}
	if err := os.Remove(r.dataPath(op)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("mover: удаление буфера: %w", err)
	}
	if err := os.Remove(r.metaPath(op)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("mover: удаление реестра: %w", err)
	}
	r.used -= rec.Received
	delete(r.ops, op)
	return nil
}

// Expire убирает операции, пережившие свой срок: завершённые — по resultTTL
// (дольше держать смысл есть: tapegw может переподключиться за итогом),
// незавершённые — по opTTL.
func (r *Registry) Expire(now time.Time) ([]uuid.UUID, error) {
	r.mu.Lock()
	type victim struct {
		op   uuid.UUID
		code Code
	}
	var victims []victim
	for op, rec := range r.ops {
		age := now.Sub(rec.UpdatedAt)
		switch {
		case rec.State == StateDone || rec.State == StateFailed:
			if r.resTTL > 0 && age > r.resTTL {
				victims = append(victims, victim{op, CodeExpired})
			}
		default:
			if r.opTTL > 0 && age > r.opTTL {
				victims = append(victims, victim{op, CodeExpired})
			}
		}
	}
	r.mu.Unlock()

	var out []uuid.UUID
	for _, v := range victims {
		if err := r.Drop(v.op); err != nil {
			return out, err
		}
		out = append(out, v.op)
	}
	return out, nil
}

// DataFile открывает файл буфера на дозапись: докачка идёт с того смещения,
// которое mover подтвердил в Resume.
func (r *Registry) DataFile(op uuid.UUID) (*os.File, error) {
	return os.OpenFile(r.dataPath(op), os.O_RDWR|os.O_CREATE, 0o600)
}

// Verify сверяет SHA-256 принятого потока с объявленной. До этой проверки на
// ленту не пишется ничего: промежуточное состояние буфера не может «просочиться»
// на носитель, и это единственное, что делает докачку по смещению безопасной.
func (r *Registry) Verify(op uuid.UUID) (bool, error) {
	f, err := os.Open(r.dataPath(op))
	if err != nil {
		return false, err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return false, fmt.Errorf("mover: подсчёт SHA-256: %w", err)
	}
	rec, ok := r.Get(op)
	if !ok {
		return false, fmt.Errorf("mover: операция %s не известна", op)
	}
	return hex.EncodeToString(h.Sum(nil)) == rec.SHA256, nil
}

// SumFile считает SHA-256 файла — нужно на get, где сумму считает mover и
// сверяет принимающая сторона.
func SumFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
