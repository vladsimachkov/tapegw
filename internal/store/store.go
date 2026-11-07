// Package store — абстракция над хранилищем блобов (Phobos).
//
// Весь cgo изолирован за этим интерфейсом: остальной код от Phobos не зависит
// и тестируется без него (правило 2 из AGENTS.md). Реализации:
//
//	internal/store/phobos  — cgo, build tag `phobos`
//	internal/store/fake    — in-memory, с инъекцией сбоев
//
// Интерфейс намеренно не про дескрипторы файлов: mover живёт на сервере
// библиотеки, а данные приходят к нему по протоколу (ADR-0021), поэтому Put
// принимает поток, а Get — приёмник.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/google/uuid"
)

// AttrBudgetBytes — сколько байт занимают наши атрибуты объекта на ленте.
//
// Phobos сериализует все пользовательские атрибуты в один JSON в xattr
// user.user_md, а LTFS ограничивает одно значение xattr 4096 байтами. Замер
// M2-01: значение 4079 проходит, 4080 — ENOSPC; разница 17 байт — обёртка
// JSON, которую добавляет Phobos. Проверка обязана случиться ДО обращения к
// ленте: ENOSPC в середине батча неотличим от «нет ленты с тегом» (M0-08).
const AttrBudgetBytes = 4079

// AttrSize — сколько байт займут атрибуты в JSON, который пишет Phobos.
// Считается так же, как его сериализует encoding/json, потому что ровно этот
// JSON и упирается в лимит LTFS.
func AttrSize(attrs map[string]string) int {
	b, err := json.Marshal(attrs)
	if err != nil {
		// Значения атрибутов — строки, отказать может только не-UTF-8, и то
		// не здесь. Пусть это будет явная ошибка, а не «влезло».
		return 1 << 30
	}
	return len(b)
}

// PutItem — один объект в батче записи. File — открытый файл с данными,
// читаемый с самого начала; закрывает его вызывающий.
//
// Файл, а не io.Reader, потому что Phobos принимает данные дескриптором
// (xt_fd) и требует размер заранее. К моменту вызова store.Put данные уже лежат
// в файле буфера mover'а (#81, putReader), так что второй проход по диску не
// нужен. Pipe не подходит: не проверено, что phobos_put умеет в
// неперематываемый дескриптор, а размер всё равно надо знать заранее.
type PutItem struct {
	ID    uuid.UUID
	File  *os.File
	Size  int64
	Attrs map[string]string
}

// Rewind ставит позицию файла в начало. Обязателен перед передачей дескриптора
// в хранилище: Phobos читает по xt_fd от текущей позиции, а файл буфера к
// моменту записи уже прочитан до конца — SHA-256 считается именно чтением (#81).
// Передать такой дескриптор как есть значит записать ноль байт и получить
// «успех» вместо объекта.
//
// Делает это **реализация хранилища**, а не вызывающий: иначе fake, который
// читает через ReadAt, ошибку спрячет, а настоящая реализация запишет пустоту.
func Rewind(f *os.File) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("store: перемотать файл: %w", err)
	}
	return nil
}

// PrepareForWrite подготавливает файл под запись результата: позиция в начало
// и обнуление длины. Без Truncate остатки прошлых данных в файле буфера
// смешаются с объектом, и лишний хвост не виден ни по размеру ответа, ни по
// контрольной сумме, если её считают по файлу целиком.
func PrepareForWrite(f *os.File) error {
	if err := Rewind(f); err != nil {
		return err
	}
	if err := f.Truncate(0); err != nil {
		return fmt.Errorf("store: обнулить файл: %w", err)
	}
	return nil
}

// GetItem — один объект в батче чтения. File — открытый файл, куда хранилище
// запишет данные (тоже файл буфера mover'а).
type GetItem struct {
	ID   uuid.UUID
	File *os.File
}

// Result — итог по одному объекту батча. Батч может частично удалиться,
// поэтому ошибка у каждого элемента своя.
// Extent — один экстент объекта так, как его отдаёт Phobos: размер, носитель и
// состояние копии. Никаких выводов здесь не зашито.
type Extent struct {
	Size  int64
	Media string
	State string
}

type Result struct {
	ID      uuid.UUID
	Version int
	Err     error
}

// PutOptions — параметры записи, приходят из конфига (family, layout, tags).
type PutOptions struct {
	Family string
	// Library — библиотека носителей. libphobos не берёт её из профиля: без
	// явных family/library/layout PUT отвечает -19 ENODEV (спайк M0-02).
	Library      string
	Layout       string
	LayoutParams map[string]string
	Tags         []string
	// NoSplit запрещает дробить батч по носителям: Phobos обязан выбрать
	// ленту, куда влезет весь батц целиком.
	NoSplit bool
}

// Meta — метаданные объекта на стороне Phobos.
//
// Complete отдельен, а не «Size > 0»: phobos_getmd возвращает объект и при
// copy_status = incomplete (спайк M0-02). Считать такой объект
// заархивированным — значит потерять данные молча.
type Meta struct {
	ID       uuid.UUID
	Size     int64
	Version  int
	Complete bool
	Attrs    map[string]string

	// Extents — экстенты объекта как их вернул Phobos, без интерпретации.
	// Sumму не считаем: 94 байта — это и две реплики по 47, и один объект на
	// 94, а для разбора EEXIST это противоположные случаи.
	Extents []Extent
}

// Store — клиент Phobos. Все методы батчевые: один вызов = один xfer, это
// и требование производительности ленты, и способ держать каретку в покое.
//
// Вызовы блокирующие. Их разрешено делать только из процесса mover, один
// вызов за раз (ADR-0007). Отмена ctx не прерывает идущий C-вызов: воркер при
// остановке дожидается текущего батча.
type Store interface {
	Put(ctx context.Context, items []PutItem, opt PutOptions) []Result
	Get(ctx context.Context, items []GetItem) []Result
	Stat(ctx context.Context, id uuid.UUID) (Meta, error)
	Delete(ctx context.Context, ids []uuid.UUID) []Result

	// Extents — носители, на которые лёг объект. Нужны, чтобы обновить
	// objects.phobos_medium после архивации и после repack (ADR-0008).
	// Может вернуть ErrNotSupported.
	Extents(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID][]string, error)

	io.Closer
}

// Типизированные ошибки хранилища. Воркеры по ним решают, ретраить или
// сдаваться: ErrBusy и ErrNoSpace ретраятся, прочие фатальны для объекта.
var (
	ErrNotFound     = errors.New("store: object not found")
	ErrNoSpace      = errors.New("store: no space on tape")
	ErrBusy         = errors.New("store: phobos busy")
	ErrNotSupported = errors.New("store: not supported by this backend")
	// ErrIncomplete — объект есть в индексе, но копия неполная. Отдельная
	// ошибка именно потому, что «есть» и «полон» — разные условия (M0-02).
	ErrIncomplete = errors.New("store: copy is incomplete")
	// ErrExists — объект с таким oid уже записан. Повторная запись не
	// перезаписывает: oid уникален, и молча перезаписать — значит подменить
	// данные у того же version UUID.
	ErrExists = errors.New("store: object already exists")
	// ErrAttrTooLarge — атрибуты не влезут в бюджет LTFS. Ошибка
	// конфигурации вызывающей стороны, а не лента: обнаруживается до xfer.
	ErrAttrTooLarge = errors.New("store: attributes exceed LTFS budget")
)

// CheckExtents — проверка экстентов против плана. Вызывает её тот, у кого план
// есть (migrator при EEXIST), потому что «сколько должно быть» знает только он.
// Причины разделены: «не то число реплик», «копия не дописана» и «размер не тот»
// — это разные неисправности, и искать их надо по-разному.
func CheckExtents(ext []Extent, wantSize int64, wantCopies int) error {
	if len(ext) != wantCopies {
		return fmt.Errorf("экстентов %d, ожидалось %d — проверьте repl_count", len(ext), wantCopies)
	}
	for _, e := range ext {
		if e.State != "sync" {
			return fmt.Errorf("копия на %s в состоянии %q, а не sync", e.Media, e.State)
		}
	}
	var total int64
	for _, e := range ext {
		total += e.Size
	}
	if wantCopies == 1 && total != wantSize {
		return fmt.Errorf("размер %d, план %d", total, wantSize)
	}
	return nil
}
