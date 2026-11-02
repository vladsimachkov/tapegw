// Package store — абстракция над хранилищем блобов (Phobos).
//
// Весь cgo изолирован за этим интерфейсом: остальной код от Phobos не зависит
// и тестируется без него (правило 2 из AGENTS.md). Реализации:
//
//	internal/store/phobos  — cgo, build tag `phobos`
//	internal/store/fake    — in-memory, с инъекцией сбоев
package store

import (
	"context"
	"errors"
	"io"
	"os"

	"github.com/google/uuid"
)

// PutItem — один объект в батче записи. File нужно держать открытым до возврата
// из Put: Phobos читает дескриптор, а не путь.
type PutItem struct {
	ID    uuid.UUID
	File  *os.File
	Size  int64
	Attrs map[string]string
}

// GetItem — один объект в батче чтения. File — куда писать.
type GetItem struct {
	ID   uuid.UUID
	File *os.File
}

// Result — итог по одному объекту батча. Батч может частично удалиться,
// поэтому ошибка у каждого элемента своя.
type Result struct {
	ID      uuid.UUID
	Version int
	Err     error
}

// PutOptions — параметры записи, приходят из конфига (family, layout, tags).
type PutOptions struct {
	Family       string
	Layout       string
	LayoutParams map[string]string
	Tags         []string
}

// Location — где объект лежит на ленте. Нужен recaller-у, чтобы отсортировать
// батч по ленте и смещению и не мотать каретку туда-сюда.
//
// Доступность этих данных — вопрос спайка M0-4; если API Phobos их не отдаёт,
// Locate возвращает ErrNotSupported и recaller полагается на переупорядочивание
// в LRS.
type Location struct {
	TapeID    string
	Offset    int64
	DriveHint string
}

// Meta — метаданные объекта на стороне Phobos.
type Meta struct {
	ID      uuid.UUID
	Size    int64
	Version int
	Attrs   map[string]string
}

// Store — клиент Phobos. Все методы батчевые: один вызов = один xfer, это
// и требование производительности ленты, и способ держать каретку в покое.
//
// Вызовы блокирующие. Их разрешено делать только из пула размером
// phobos.max_inflight (по умолчанию 1, до подтверждения потокобезопасности
// libphobos спайком M0-3). Отмена ctx не прерывает идущий C-вызов: воркер при
// остановке дожидается текущего батча.
type Store interface {
	Put(ctx context.Context, items []PutItem, opt PutOptions) []Result
	Get(ctx context.Context, items []GetItem) []Result
	Stat(ctx context.Context, id uuid.UUID) (Meta, error)
	Delete(ctx context.Context, ids []uuid.UUID) []Result

	// Locate может вернуть ErrNotSupported.
	Locate(ctx context.Context, ids []uuid.UUID) ([]Location, error)

	io.Closer
}

// Типизированные ошибки хранилища. Воркеры по ним решают, ретраить или сдаваться:
// ErrBusy и ErrNoSpace ретраятся, прочие фатальны для конкретного объекта.
var (
	ErrNotFound     = errors.New("store: object not found")
	ErrNoSpace      = errors.New("store: no space on tape")
	ErrBusy         = errors.New("store: phobos busy")
	ErrNotSupported = errors.New("store: not supported by this backend")
)
