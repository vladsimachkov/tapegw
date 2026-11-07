package mover

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Ожидание ответа по умолчанию. Записывать на ленту могут быть минуты, поэтому
// это не «таймаут сети», а верхняя граница одного ожидания кадра.
const defaultOpTimeout = 30 * time.Minute

// dialTimeout — только на установку TCP-соединения; сама операция идёт минуты.
const dialTimeout = 10 * time.Second

// Client — соединение tapegw с одним mover'ом. Планировщик держит по соединению
// на драйв: выбор конкретного mover'а делает планировщик, а не «любой
// свободный», иначе recaller гонял бы ленты между приводами (ADR-0008).
type Client struct {
	mu     sync.Mutex
	conn   *tls.Conn
	closed bool

	// chunk — размер кадра данных; window — сколько байт разрешено послать
	// без подтверждения кредитов.
	chunk  int
	window int
	// bufMax — весь буфер mover'а: объект больше него не примут (no_buffer).
	bufMax int64

	// opTimeout — верхняя граница ожидания одного ответа. mover пишет на
	// ленту минуты, поэтому это не сетевой таймаут, а потолок операции.
	opTimeout time.Duration
}

// Dial подключается, проверяет рукопожатие и версию протокола.
func Dial(ctx context.Context, addr string, cfg *tls.Config) (*Client, error) {
	c, _, err := DialAs(ctx, addr, cfg, "")
	return c, err
}

// DialAs — то же, но с проверкой роли из hello. Роль объявляет сам mover,
// поэтому это проверка конфигурации, а не безопасности: идентичность держится
// на SAN сертификата (см. tls.go).
func DialAs(ctx context.Context, addr string, cfg *tls.Config, want Role) (*Client, Hello, error) {
	d := tls.Dialer{NetDialer: &net.Dialer{Timeout: dialTimeout}, Config: cfg}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, Hello{}, err
	}
	tc := conn.(*tls.Conn)
	if len(tc.ConnectionState().PeerCertificates) == 0 {
		tc.Close()
		return nil, Hello{}, ErrNoCert
	}
	c := &Client{conn: tc, chunk: ChunkSize, opTimeout: defaultOpTimeout}

	// mover здоровается первым: пока не прочитан его hello, неизвестны ни
	// версия протокола, ни размер буфера.
	f, err := ReadFrame(tc, MaxFrameBytes)
	if err != nil {
		tc.Close()
		return nil, Hello{}, err
	}
	var h Hello
	if err := Decode(f, &h); err != nil || h.Type != "hello" {
		tc.Close()
		return nil, h, errors.New("mover: mover не поздоровался первым")
	}
	if h.Proto != ProtoVersion {
		tc.Close()
		return nil, h, fmt.Errorf("%w: mover на %d, tapegw на %d", ErrProtoMismatch, h.Proto, ProtoVersion)
	}
	if want != "" && h.Role != want {
		tc.Close()
		return nil, h, fmt.Errorf("%w: mover говорит %q", ErrRoleMismatch, h.Role)
	}
	c.window = c.chunk * 4
	c.bufMax = h.BufferFree
	if err := WriteJSON(tc, HelloOk{Type: "hello_ok", Proto: ProtoVersion}); err != nil {
		tc.Close()
		return nil, h, err
	}
	return c, h, nil
}

// Close закрывает соединение. Идущую операцию Phobos это не отменяет: отмены в
// протоколе нет, итог забирается через Status после переподключения.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	return c.conn.Close()
}

// PutIn — описание записываемого объекта. Body читается до конца ровно один раз:
// при resume клиент обязан уметь продолжить с ready.Offset, поэтому Body —
// именно поток, а не нарезанный заранее буфер.
type PutIn struct {
	Op     uuid.UUID
	ID     uuid.UUID
	Size   int64
	SHA256 string
	Attrs  map[string]string
	Body   io.Reader

	// Tags — теги Phobos: именно они разводят копии по носителям (ADR-0013).
	// Без них «копия left» и «копия right» лягут по умолчанию Phobos.
	Tags []string

	// FromOp — локальная передача (Ф0.4): записать объект из буфера другой
	// завершённой операции этого же mover'а. Body при этом не нужен.
	FromOp uuid.UUID

	// Hold — попросить mover оставить буфер после записи: с него пойдут копии.
	Hold bool
}

// Put отправляет объект на ленту. Возвращает итог операции; коды не-ok — тоже
// возвращаемое значение, а не error: error здесь только про транспорт.
func (c *Client) Put(in PutIn) (*Result, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if in.Size >= 0 && in.Size > c.bufMax {
		return &Result{Code: CodeNoBuffer, State: StateFailed}, nil
	}
	// Идентификатор объекта в протоколе не передаётся отдельно: oid Phobos есть
	// UUID операции (ADR-0021), иначе «продолжить» и «записать заново»
	// различались бы только по смещению.
	if in.ID != in.Op {
		return nil, fmt.Errorf("mover: oid объекта — это op, а %s != %s", in.ID, in.Op)
	}
	if err := WriteJSON(c.conn, Put{Type: "put", Op: in.Op, Size: in.Size, SHA256: in.SHA256, Attrs: in.Attrs, Tags: in.Tags, FromOp: in.FromOp, Hold: in.Hold}); err != nil {
		return nil, err
	}

	// Ждём ready / resume — либо готовый итог (op уже записан, op_conflict,
	// no_buffer: на них поток не начинается вовсе).
	var ready Ready
	for {
		res, f, err := c.readControl(c.opTimeout)
		if err != nil {
			return nil, err
		}
		if res != nil {
			return res, nil
		}
		if f.Kind != frameJSON {
			return nil, fmt.Errorf("mover: ждали управление, пришёл кадр данных")
		}
		if err := Decode(f, &ready); err != nil {
			return nil, err
		}
		if ready.Type == "ready" || ready.Type == "resume" {
			break
		}
	}
	if ready.Offset > 0 {
		rs, ok := in.Body.(io.Seeker)
		if !ok {
			return nil, fmt.Errorf("mover: просят продолжить с %d, а Body не поддерживает Seek", ready.Offset)
		}
		if _, err := rs.Seek(ready.Offset, io.SeekStart); err != nil {
			return nil, fmt.Errorf("mover: продолжить передачу: %w", err)
		}
	}

	if in.FromOp != uuid.Nil {
		// Локальная передача: байты не идут по сети, mover читает свой буфер.
		for {
			res, _, err := c.readControl(c.opTimeout)
			if err != nil {
				return nil, err
			}
			if res != nil {
				return res, nil
			}
		}
	}

	credits := ready.Credits
	sent := ready.Offset
	buf := make([]byte, c.chunk)
	for sent < in.Size {
		for credits <= 0 {
			cr, err := c.waitCredit()
			if err != nil {
				return nil, err
			}
			credits += cr
		}
		n := int64(len(buf))
		if room := in.Size - sent; room < n {
			n = room
		}
		if win := int64(credits); win < n {
			n = win
		}
		if _, err := io.ReadFull(in.Body, buf[:n]); err != nil {
			return nil, fmt.Errorf("mover: читать тело объекта: %w", err)
		}
		if err := WriteData(c.conn, in.Op, buf[:n]); err != nil {
			return nil, err
		}
		sent += n
		credits -= n
	}

	for {
		res, _, err := c.readControl(c.opTimeout)
		if err != nil {
			return nil, err
		}
		if res != nil {
			return res, nil
		}
	}
}

// Get забирает объект с ленты в w. Принимающая сторона сама выдаёт кредиты и
// сама сверяет SHA-256: байты с неверным хвостом сегмента GCM иначе всплывают
// только при чтении клиентом, а mover обязан отказать до того, как байты
// попадут в кэш.
// Release освобождает буфер, удержанный по hold: все копии записаны.
func (c *Client) Release(op uuid.UUID) (*Result, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := WriteJSON(c.conn, Release{Type: "release", Op: op}); err != nil {
		return nil, err
	}
	for {
		res, _, err := c.readControl(c.opTimeout)
		if err != nil {
			return nil, err
		}
		if res != nil {
			return res, nil
		}
	}
}

func (c *Client) Get(op, id uuid.UUID, w io.Writer) (*Result, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := WriteJSON(c.conn, Get{Type: "get", Op: op, ID: id}); err != nil {
		return nil, err
	}
	// Стартовое окно кредитов: принимающая сторона выдаёт кредиты, а не ждёт.
	if err := WriteJSON(c.conn, Credit{Type: "credit", Op: op, Credits: int64(c.window)}); err != nil {
		return nil, err
	}

	var (
		sha  hash.Hash
		seen int64
		crc  = int64(c.window)
	)
	for {
		if crc <= 0 {
			cr, err := c.waitCredit()
			if err != nil {
				return nil, err
			}
			crc += cr
		}
		f, err := ReadFrame(c.conn, MaxDataBytes)
		if err != nil {
			return nil, err
		}
		if f.Kind == frameJSON {
			var res Result
			if err := Decode(f, &res); err == nil && res.Type == "result" {
				if res.Code != CodeOK {
					return &res, nil
				}
				if sha != nil && res.SHA256 != "" && hex.EncodeToString(sha.Sum(nil)) != res.SHA256 {
					return &Result{Code: CodeChecksumMismatch, State: StateFailed},
						fmt.Errorf("mover: контрольная сумма объекта не совпала")
				}
				return &res, nil
			}
			var st StateMsg
			if err := Decode(f, &st); err == nil && st.Type == "state" {
				sha = sha256.New()
				continue
			}
			var rd Ready
			if err := Decode(f, &rd); err == nil && rd.Type == "ready" {
				crc += rd.Credits
				continue
			}
			return nil, fmt.Errorf("mover: неизвестный управляющий кадр %q", f.B[:min(40, len(f.B))])
		}
		if len(f.B) > int(crc) {
			return nil, fmt.Errorf("mover: mover нарушил кредиты: послал %d при %d", len(f.B), crc)
		}
		crc -= int64(len(f.B))
		if sha != nil {
			sha.Write(f.B)
		}
		n, err := w.Write(f.B)
		if err != nil {
			return nil, err
		}
		seen += int64(n)
		// Приняли байты — вернули кредиты: буфер освободился. Свою оценку
		// окна пополняем тем же числом: crc — это «сколько серверу ещё
		// разрешено», а не «сколько мы выдержим»; отдали кредиты — разрешение
		// вернулось. Без этого get объекта больше window (chunk*4) умирал на
		// первом же ожидании: crc обнулялся, waitCredit ждал кадр кредитов,
		// а в направлении get кредиты шлёт только клиент — приходил кадр
		// данных и «ждали кредиты, пришёл кадр данных».
		if err := WriteJSON(c.conn, Credit{Type: "credit", Op: op, Credits: int64(n)}); err != nil {
			return nil, err
		}
		crc += int64(n)
	}
}

// Status спрашивает состояние операции после обрыва.
// StatObject спрашивает состояние объекта в Phobos.
func (c *Client) StatObject(id uuid.UUID) (*Result, error) {
	return c.ask(Stat{Type: "stat", ID: id})
}

// DeleteObject удаляет неполный объект. Целый mover не отдаст: см. handleDelete.
func (c *Client) DeleteObject(id uuid.UUID) (*Result, error) {
	return c.ask(Delete{Type: "delete", ID: id})
}

func (c *Client) ask(msg interface{}) (*Result, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := WriteJSON(c.conn, msg); err != nil {
		return nil, err
	}
	for {
		res, _, err := c.readControl(c.opTimeout)
		if err != nil {
			return nil, err
		}
		if res != nil {
			return res, nil
		}
	}
}

func (c *Client) Status(op uuid.UUID) (*Result, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := WriteJSON(c.conn, Status{Type: "status", Op: op}); err != nil {
		return nil, err
	}
	for {
		res, _, err := c.readControl(c.opTimeout)
		if err != nil {
			return nil, err
		}
		if res != nil {
			return res, nil
		}
	}
}

// waitCredit ждёт кадр кредитов, не забывая про потолок ожидания.
func (c *Client) waitCredit() (int64, error) {
	for {
		f, err := c.readFrame(c.opTimeout)
		if err != nil {
			return 0, err
		}
		if f.Kind != frameJSON {
			return 0, fmt.Errorf("mover: ждали кредиты, пришёл кадр данных")
		}
		var cr Credit
		if err := Decode(f, &cr); err == nil && cr.Type == "credit" {
			return cr.Credits, nil
		}
		var res Result
		if err := Decode(f, &res); err == nil && res.Type == "result" {
			return 0, fmt.Errorf("mover: операция завершилась кодом %q", res.Code)
		}
	}
}

// readControl читает управляющие кадры до первого result. Кадры state и credit
// возвращаются вызывающему, чтобы он мог отреагировать на размер и кредиты.
func (c *Client) readControl(timeout time.Duration) (*Result, *Frame, error) {
	f, err := c.readFrame(timeout)
	if err != nil {
		return nil, nil, err
	}
	if f.Kind != frameJSON {
		return nil, f, nil
	}
	var res Result
	if err := Decode(f, &res); err == nil && res.Type == "result" {
		return &res, nil, nil
	}
	return nil, f, nil
}

// readFrame читает один кадр с потолком ожидания: полуоткрытое соединение не
// должно держать клиента вечно.
func (c *Client) readFrame(timeout time.Duration) (*Frame, error) {
	if timeout > 0 {
		if err := c.conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
			return nil, err
		}
		defer c.conn.SetReadDeadline(time.Time{})
	}
	return ReadFrame(c.conn, MaxFrameBytes)
}

// SumBytes — convenience для контрольной суммы тела при объявлении операции.
func SumBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
