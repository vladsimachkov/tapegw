// Package mover — протокол между tapegw (сервер S3) и tapegw-mover (сервер
// библиотеки). Формат сообщений и правила поведения заданы в
// docs/adr/0021-mover-protocol.md, требования к ним — в
// docs/proposals/0012-split-s3-and-library.md.
//
// Границы, которые этот пакет обязан держать:
//
//   - данные идут протоколом, а не путями: в сообщениях нет ни одного пути к
//     файлу на стороне tapegw;
//   - mover не получает ключей: в типах сообщений физически нет полей для
//     развёрнутых DEK, TEK или KEK, и есть тест на набор полей
//     (TestMessageFieldsHaveNoKeys); обёрнутый DEK приходит как значение
//     атрибута — для mover'а это непрозрачные данные;
//   - отмены нет: прервать идущий вызов Phobos нельзя, поэтому итог после
//     обрыва считается неизвестным и перепроверяется через Stat.
package mover

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/google/uuid"
)

// ProtoVersion — версия протокола. Расхождение рукопожатия есть отказ
// соединения, а не попытка работать дальше: несовместимый mover, отработавший
// полчаса на ленте, стоит дороже не поднятого пула.
const ProtoVersion = 1

// Типы кадров. Один байт типа, четыре байта длина, данные.
const (
	frameJSON byte = 'J' // управляющее сообщение
	frameData byte = 'D' // кусок данных: 16 байт UUID операции + тело
)

// MaxFrameBytes — потолок управляющего кадра. Лучше честно отказать в начале,
// чем раздувать буфер под батч из тысяч объектов с атрибутами.
const MaxFrameBytes = 1 << 20 // 1 MiB

// ChunkSize — размер куска данных. Не больше kTLS-фрейма и достаточно крупно,
// чтобы не терять кадр на каждый килобайт.
const ChunkSize = 4 << 20 // 4 MiB

// Коды итога. Замкнутый набор: mover не выдаёт кодов, которых здесь нет,
// иначе воркер начнёт угадывать.
type Code string

const (
	CodeOK               Code = "ok"
	CodeNotFound         Code = "not_found"
	CodeNoSpace          Code = "no_space"
	CodeBusy             Code = "busy"
	CodeIncomplete       Code = "incomplete"
	CodeAttrTooLarge     Code = "attr_too_large"
	CodeChecksumMismatch Code = "checksum_mismatch"
	CodeNoBuffer         Code = "no_buffer"
	CodeOpConflict       Code = "op_conflict"
	// CodeExists — в Phobos уже есть объект с таким oid. Это НЕ успех: дубля
	// нет, но из exists не следует, что копия цела — incomplete после обрыва
	// посреди записи выглядит так же (спайк M0-02). Разбирается stat'ом по oid
	// (#101); до него вызывающий обязан не повторять, а поднять сигнал.
	//
	// Раньше этот случай маппился в CodeOK, то есть «записали» там, где ничего
	// не проверяли.
	CodeExists Code = "exists"
	// CodeDenied — отказ по политике доступа, а не по ошибке: например
	// локальная передача из буфера другого клиента (Ф0.4).
	// в частности mover отказывается удалять целый архивный объект.
	CodeDenied  Code = "denied"
	CodeExpired Code = "expired"
	CodeIO      Code = "io"
	CodeUnknown Code = "unknown"
)

// Состояния операции. Нужны затем, чтобы обрыв соединения не отменял запись:
// tapegw переподключается и забирает итог по UUID операции.
type State string

const (
	StateStaged  State = "staged"  // принято в буфер
	StateWriting State = "writing" // вызван Phobos
	StateDone    State = "done"    // итог известен и лежит в реестре
	StateFailed  State = "failed"  // итог известен и он плохой
)

// Role — для какого пула процессов поднят mover. Супервизор сверяет её с
// конфигом: migrator не должен случайно встать на место recaller и наоборот.
type Role string

const (
	RoleMigrator Role = "migrator"
	RoleRecaller Role = "recaller"
)

// Ошибки разбора и отказа.
var (
	ErrProtoMismatch  = errors.New("mover: несовместимая версия протокола")
	ErrFrameTooLarge  = errors.New("mover: кадр больше разрешённого")
	ErrBadFrame       = errors.New("mover: неизвестный тип кадра")
	ErrRoleMismatch   = errors.New("mover: роль mover'а не совпадает с конфигом")
	ErrNoCert         = errors.New("mover: соединение без клиентского сертификата")
	ErrOpConflict     = errors.New("mover: операция уже объявлена с другими size/sha256")
	ErrBufferTooSmall = errors.New("mover: объект больше всего буфера")
)

// --- Сообщения -------------------------------------------------------------
//
// Поля ниже — это и есть граница «без ключей». Здесь нет и не может быть
// развёрнутых DEK/TEK/KEK, имён бакетов и объектов, путей на стороне tapegw и
// доступа к БД. Проверено тестом TestMessageFieldsHaveNoKeys.

// Hello шлёт mover сразу после установки соединения.
type Hello struct {
	Type       string `json:"t"`
	Proto      int    `json:"proto"`
	Mover      string `json:"mover"`
	Phobos     string `json:"phobos"`
	Role       Role   `json:"role"`
	BufferFree int64  `json:"buffer_free"`
}

// HelloOk отвечает tapegw после проверки версии и роли.
type HelloOk struct {
	Type  string `json:"t"`
	Proto int    `json:"proto"`
}

// Put объявляет операцию записи. Attrs — атрибуты объекта Phobos (ADR-0020);
// обёрнутый DEK приходит внутри них как непрозрачные данные.
type Put struct {
	Type    string            `json:"t"`
	Op      uuid.UUID         `json:"op"`
	Size    int64             `json:"size"`
	SHA256  string            `json:"sha256"`
	Attrs   map[string]string `json:"attrs,omitempty"`
	Tags    []string          `json:"tags,omitempty"`
	NoSplit bool              `json:"no_split,omitempty"`

	// FromOp — локальная передача (Ф0.4): записать новый объект из буфера
	// уже завершённой операции этого же mover'а, без байтов по сети. Ноль —
	// обычная передача. Смысл в том, что копии пачки пишутся с одного содержимого:
	// второй драйв не читает ленту и не тянет гигабайты через S3-сервер.
	FromOp uuid.UUID `json:"from_op,omitempty"`

	// Hold — не освобождать буфер после записи: с него будут писаться копии
	// (Ф0.4, ADR-0022: буфер держится, пока не записаны ВСЕ копии). Без этого
	// mover выбрасывает данные сразу после success, и вторая копия уже не
	// имеет что читать — а сверять при копировании надо именно файл.
	Hold bool `json:"hold,omitempty"`
}

// Release говорит mover'у, что буфер, удержанный по hold, больше не нужен:
// все копии политики записаны. Без команды буфер каждой пачки висел бы до
// result_ttl — а он обязан быть длинным, чтобы tapegw успел забрать итог после
// обрыва. result_ttl остаётся страховкой на случай, когда tapegw не вернулся.
type Release struct {
	Type string    `json:"t"`
	Op   uuid.UUID `json:"op"`
}

// Get объявляет операцию чтения.
type Get struct {
	Type   string    `json:"t"`
	Op     uuid.UUID `json:"op"`
	ID     uuid.UUID `json:"id"`
	Offset int64     `json:"offset,omitempty"`
	Length int64     `json:"length,omitempty"` // 0 — до конца
}

// Stat спрашивает состояние объекта в Phobos — не операции в реестре mover'а.
// Нужен после потери реестра или при EEXIST: «объект есть» не значит «копия
// цела», а без ответа нельзя ни принять её, ни безопасно переслать.
type Stat struct {
	Type string    `json:"t"`
	ID   uuid.UUID `json:"id"`
}

// Delete удаляет объект из Phobos. Разрешён только для неполных копий:
// удалить целую — значит потерять данные ради удобства повтора.
type Delete struct {
	Type string    `json:"t"`
	ID   uuid.UUID `json:"id"`
}

// Status спрашивает состояние операции. Нужен после переподключения: обрыв
// соединения запись не отменяет.
type Status struct {
	Type string    `json:"t"`
	Op   uuid.UUID `json:"op"`
}

// Ready/Resume — ответ mover на Put: с какого смещения слать и сколько можно.
type Ready struct {
	Type    string    `json:"t"`
	Op      uuid.UUID `json:"op"`
	Offset  int64     `json:"offset"`
	Credits int64     `json:"credits"`
}

// Credit досылает разрешение на передачу. Переполнение буфера есть «подожди»,
// а не ошибка записи, поэтому отказ здесь только один — объект больше всего
// буфера (он приходит как Result).
type Credit struct {
	Type    string    `json:"t"`
	Op      uuid.UUID `json:"op"`
	Credits int64     `json:"credits"`
}

// StateMsg — переход состояния долгой операции.
type StateMsg struct {
	Type  string    `json:"t"`
	Op    uuid.UUID `json:"op"`
	State State     `json:"state"`
}

// Item — итог по одному объекту батча.
type Item struct {
	ID      uuid.UUID `json:"id"`
	Code    Code      `json:"code"`
	Version int       `json:"version,omitempty"`
}

// ExtentInfo — один экстент: размер, носитель, состояние копии.
type ExtentInfo struct {
	Size  int64  `json:"size"`
	Media string `json:"media,omitempty"`
	State string `json:"state,omitempty"`
}

// Result — итог операции. Extents — носители, на которые лёг объект
// (ADR-0008: носитель узнаётся при архивации).
type Result struct {
	Type    string    `json:"t"`
	Op      uuid.UUID `json:"op"`
	Code    Code      `json:"code"`
	State   State     `json:"state"`
	Version int       `json:"version,omitempty"`
	Extents []string  `json:"extents,omitempty"`
	// ExtentInfo — экстенты объекта так, как их видел Phobos. Заполняется в
	// ответе на stat: сверять против плана должен планировщик, у которого план
	// есть. Sumму тут не считаем: 94 — это и две реплики по 47, и один объект
	// на 94, а для разбора EEXIST это противоположные случаи.
	ExtentInfo []ExtentInfo `json:"extent_info,omitempty"`
	Size       int64        `json:"size,omitempty"`
	SHA256     string       `json:"sha256,omitempty"` // на get — сумма отданных байт
	Items      []Item       `json:"items,omitempty"`
}

// --- Кадры -----------------------------------------------------------------

// WriteFrame пишет управляющее сообщение.
func WriteJSON(w io.Writer, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(b) > MaxFrameBytes {
		return fmt.Errorf("%w: %d байт", ErrFrameTooLarge, len(b))
	}
	hdr := make([]byte, 5)
	hdr[0] = frameJSON
	binary.BigEndian.PutUint32(hdr[1:], uint32(len(b)))
	if _, err := w.Write(hdr); err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

// WriteData пишет кусок данных с префиксом операции. UUID в каждом кадре —
// не перестраховка: он делает кадр самораспознаваемым, и перепутать данные
// двух операций невозможно даже если одна из них зависла.
func WriteData(w io.Writer, op uuid.UUID, p []byte) error {
	if len(p)+16 > MaxFrameBytes {
		return fmt.Errorf("%w: %d байт данных", ErrFrameTooLarge, len(p))
	}
	hdr := make([]byte, 5+16)
	hdr[0] = frameData
	binary.BigEndian.PutUint32(hdr[1:], uint32(len(p)+16))
	copy(hdr[5:], op[:])
	if _, err := w.Write(hdr); err != nil {
		return err
	}
	_, err := w.Write(p)
	return err
}

// Frame — прочитанный кадр.
type Frame struct {
	Kind byte
	Op   uuid.UUID // только для frameData
	B    []byte    // тело: JSON для frameJSON, данные для frameData
}

// ReadFrame читает один кадр. max — потолок размера, чтобы испорченная длина
// не заставила выделить память.
func ReadFrame(r io.Reader, max int) (*Frame, error) {
	hdr := make([]byte, 5)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return nil, err
	}
	n := int(binary.BigEndian.Uint32(hdr[1:]))
	if n <= 0 || n > max {
		return nil, fmt.Errorf("%w: заявлено %d байт", ErrFrameTooLarge, n)
	}
	switch hdr[0] {
	case frameJSON:
		b := make([]byte, n)
		if _, err := io.ReadFull(r, b); err != nil {
			return nil, err
		}
		return &Frame{Kind: frameJSON, B: b}, nil
	case frameData:
		if n < 16 {
			return nil, fmt.Errorf("%w: кадр данных без UUID операции", ErrBadFrame)
		}
		b := make([]byte, n)
		if _, err := io.ReadFull(r, b); err != nil {
			return nil, err
		}
		var f Frame
		copy(f.Op[:], b[:16])
		f.Kind = frameData
		f.B = b[16:]
		return &f, nil
	default:
		return nil, fmt.Errorf("%w: 0x%02x", ErrBadFrame, hdr[0])
	}
}

// Decode разбирает управляющий кадр в конкретное сообщение.
func Decode(f *Frame, v any) error {
	return json.Unmarshal(f.B, v)
}
