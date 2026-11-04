// Package spool — работа с дисковыми буферами: landing, cache, mpu.
//
// Обязательные свойства, которые реализованы в пакете:
//
//   - атомарная запись: временный файл в tmp/ рядом с назначением, fsync файла,
//     rename на место, fsync каталога. Правило 6 из AGENTS.md: файл на диске
//     раньше commit-а в Postgres, никогда наоборот;
//   - подсчёт MD5 и запрошенных x-amz-checksum-* на лету, без второго прохода,
//     и сверка с Content-MD5 / x-amz-checksum-* из запроса;
//   - watermark-вытеснение (M6): при вытеснении порядок обратный правилу 6 —
//     сначала снять флаг landing=false в транзакции, потом unlink (ADR-0004).
//
// Путь объекта вычисляется из его UUID детерминированно (Spool.Path), поэтому
// mover — отдельный процесс — собирает тот же путь по строке из БД и никто не
// передаёт пути через IPC. См. ADR-0007.
package spool

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/google/uuid"
)

// tmpSubdir — каталог временных файлов внутри корня. Он обязан быть на той же
// ФС, что и сам корень: rename на другое устройство дал бы EXDEV, и атомарность
// записи пропала бы.
const tmpSubdir = "tmp"

// shards — число каталогов-шардов: по байту префикса UUID.
const shards = 256

// ErrTooLarge — тело запроса превысило лимит. Бэкенд превращает в EntityTooLarge.
var ErrTooLarge = errors.New("spool: object exceeds size limit")

// ErrSizeMismatch — тело не совпало с объявленным размером (шифрованная запись).
var ErrSizeMismatch = errors.New("spool: размер тела не совпал с объявленным")

// Spool — один дисковый буфер (landing или cache). Методы безопасны для
// конкурентного вызова: общего изменяемого состояния у типа нет.
type Spool struct {
	root string
	tmp  string
}

// Open подготавливает буфер: создаёт каталоги и проверяет, что tmp лежит на той
// же ФС, что и корень. Ошибка здесь предпочитает молчаливое EXDEV на первом же
// PUT: конфигурация с неверным путём должна быть видна сразу на старте.
func Open(root string) (*Spool, error) {
	if root == "" {
		return nil, errors.New("spool: пустой корень буфера")
	}
	s := &Spool{root: root, tmp: filepath.Join(root, tmpSubdir)}

	for _, dir := range []string{s.root, s.tmp} {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return nil, fmt.Errorf("spool: каталог %s: %w", dir, err)
		}
	}

	devRoot, err := devOf(s.root)
	if err != nil {
		return nil, err
	}
	devTmp, err := devOf(s.tmp)
	if err != nil {
		return nil, err
	}
	if devRoot != devTmp {
		return nil, fmt.Errorf("spool: %s и %s на разных ФС (st_dev %d и %d): "+
			"rename даст EXDEV и запись перестанет быть атомарной",
			s.root, s.tmp, devRoot, devTmp)
	}

	// Все шарды создаём здесь и один раз fsync-им корень. Если создавать
	// каталог шарда лениво при записи, запись о нём в корне буфера остаётся
	// без fsync: после падения машины каталог может исчезнуть вместе с файлом,
	// который уже закоммичен в Postgres. Правило 6 тогда формально нарушено,
	// хотя ошибка проявится только на отказе питания.
	if err := s.initShards(); err != nil {
		return nil, err
	}
	return s, nil
}

// initShards — каталоги-шарды по всем возможным префиксам UUID.
func (s *Spool) initShards() error {
	for i := 0; i < shards; i++ {
		if err := os.MkdirAll(filepath.Join(s.root, fmt.Sprintf("%02x", i)), 0o750); err != nil {
			return fmt.Errorf("spool: шард %02x: %w", i, err)
		}
	}
	return fsyncDir(s.root)
}

// Root — корень буфера на диске.
func (s *Spool) Root() string { return s.root }

// Path — детерминированный путь объекта по его UUID. Разбиение по первым двум
// символам нужно, чтобы в одном каталоге не оказалось сотен тысяч файлов:
// это и время readdir у janitor, и лимиты некоторых ФС.
//
// Функция чистая: тот же путь по тому же UUID соберёт и mover в другом процессе.
func (s *Spool) Path(id uuid.UUID) string {
	str := id.String()
	return filepath.Join(s.root, str[:2], str)
}

// Exists — есть ли файл на диске. Ошибкой считается только сбой stat: прочее
// (в том числе гонка с удалением) означает «файла нет».
func (s *Spool) Exists(id uuid.UUID) bool {
	_, err := os.Stat(s.Path(id))
	return err == nil
}

// Open для чтения. Вызывающий обязан закрыть файл.
func (s *Spool) Open(id uuid.UUID) (*os.File, error) {
	f, err := os.Open(s.Path(id))
	if err != nil {
		return nil, fmt.Errorf("spool: открыть %s: %w", id, err)
	}
	return f, nil
}

// Remove удаляет файл буфера. Несуществующий файл — не ошибка: вытеснение и
// janitor вызваются повторно и должны быть идемпотентны.
func (s *Spool) Remove(id uuid.UUID) error {
	err := os.Remove(s.Path(id))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("spool: удалить %s: %w", id, err)
	}
	return nil
}

// devOf — st_dev каталога, для проверки «одна ФС».
func devOf(dir string) (uint64, error) {
	var st syscall.Stat_t
	if err := syscall.Stat(dir, &st); err != nil {
		return 0, fmt.Errorf("spool: stat %s: %w", dir, err)
	}
	return st.Dev, nil
}

// fsyncDir — fsync каталога. Без него rename может пережить падение процесса,
// но не падение машины: запись в журнал ФС без fsync каталога не гарантирована.
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("spool: открыть каталог %s: %w", dir, err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("spool: fsync каталога %s: %w", dir, err)
	}
	return nil
}
