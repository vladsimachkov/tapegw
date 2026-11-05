package mover

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Client — сторона tapegw. Одно соединение обслуживает одну операцию за раз:
// мультиплексирования в протоколе нет (ADR-0021), параллельность дают отдельные
// процессы mover'а.
type Client struct {
	conn  net.Conn
	role  Role
	log   *slog.Logger
	chunk int

	mu     sync.Mutex
	closed bool
}

// DialConfig — параметры подключения супервизора к mover'у.
type DialConfig struct {
	Addr    string
	TLS     *tls.Config
	Want    Role
	Chunk   int
	Timeout time.Duration
	Log     *slog.Logger
}

// Dial подключается и проверяет рукопожатие.
//
// Роль сверяется здесь, а не «когда-нибудь при разборе»: migrator, вставший на
// место recaller, молча начал бы архивировать вместо восстановления.
func Dial(ctx context.Context, cfg DialConfig) (*Client, error) {
	if cfg.TLS == nil {
		return nil, ErrNoCert
	}
	if cfg.Chunk == 0 {
		cfg.Chunk = ChunkSize
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	d := net.Dialer{Timeout: cfg.Timeout}
	raw, err := d.DialContext(ctx, "tcp", cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("mover: подключение к %s: %w", cfg.Addr, err)
	}
	tc := tls.Client(raw, cfg.TLS)
	if err := tc.HandshakeContext(ctx); err != nil {
		raw.Close()
		return nil, fmt.Errorf("mover: рукопожатие TLS: %w", err)
	}
	if len(tc.ConnectionState().PeerCertificates) == 0 {
		tc.Close()
		return nil, ErrNoCert
	}

	c := &Client{conn: tc, role: cfg.Want, log: cfg.Log, chunk: cfg.Chunk}
	f, err := ReadFrame(tc, MaxFrameBytes)
	if err != nil {
		tc.Close()
		return nil, err
	}
	var h Hello
	if err := Decode(f, &h); err != nil || h.Type != "hello" {
		tc.Close()
		return nil, errors.New("mover: mover не поздоровался первым")
	}
	if h.Proto != ProtoVersion {
		tc.Close()
		return nil, fmt.Errorf("%w: mover на %d, tapegw на %d", ErrProtoMismatch, h.Proto, ProtoVersion)
	}
	if cfg.Want != "" && h.Role != cfg.Want {
		tc.Close()
		return nil, fmt.Errorf("%w: mover говорит %q", ErrRoleMismatch, h.Role)
	}
	if err := WriteJSON(tc, HelloOk{Type: "hello_ok", Proto: ProtoVersion}); err != nil {
		tc.Close()
		return nil, err
	}
	return c, nil
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

// PutIn — параметры передачи объекта на ленту.
type PutIn struct {
	Op   uuid.UUID
	Body io.Reader
	Size int64
	// SHA256 — hex-сумма всего тела, объявляется **до** передачи. tapegw
	// собирает объект сам (пачку — тоже), поэтому сумма известна заранее;
	// mover сверяет её до phobos_put, и это единственное, что делает
	// безопасной докачку по смещению.
	SHA256  string
	Attrs   map[string]string
	Tags    []string
	NoSplit bool
}

// Put передаёт объект и возвращает итог.
//
// Повтор после обрыва — это просто Put с тем же Op: mover ответит Resume с
// смещением, и передача продолжится оттуда. Уже принятые байты источник
// пропускает сам — иначе докачка превратилась бы в порчу, а её ловит только
// итоговая SHA-256.
func (c *Client) Put(in PutIn) (*Result, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, errors.New("mover: соединение закрыто")
	}
	if in.SHA256 == "" {
		return nil, errors.New("mover: Put требует SHA-256 всего тела")
	}
	if err := WriteJSON(c.conn, Put{
		Type: "put", Op: in.Op, Size: in.Size, SHA256: in.SHA256,
		Attrs: in.Attrs, Tags: in.Tags, NoSplit: in.NoSplit,
	}); err != nil {
		return nil, err
	}

	// Ждём ready / resume — либо готовый итог (op уже записан, op_conflict,
	// no_buffer: на них поток не начинается вовсе).
	var ready Ready
	for {
		res, f, err := c.readControl()
		if err != nil {
			return nil, err
		}
		if res != nil {
			return res, nil
		}
		if f.Kind != frameJSON {
			return nil, errors.New("mover: ждали ready, получили данные")
		}
		if err := Decode(f, &ready); err != nil {
			return nil, err
		}
		if ready.Type == "ready" || ready.Type == "resume" {
			break
		}
	}

	// Пропускаем то, что mover уже принял.
	if ready.Offset > 0 {
		if n, err := io.CopyN(io.Discard, in.Body, ready.Offset); err != nil {
			return nil, fmt.Errorf("mover: пропуск %d байт при докачке: %w", ready.Offset, err)
		} else if n != ready.Offset {
			return nil, fmt.Errorf("mover: источник короче, чем уже принято (%d < %d)", n, ready.Offset)
		}
	}

	credits := ready.Credits
	buf := make([]byte, c.chunk)
	sent := ready.Offset
	for sent < in.Size {
		for credits <= 0 {
			res, f, err := c.readControl()
			if err != nil {
				return nil, err
			}
			if res != nil {
				// mover отказал до того, как мы всё послали.
				return res, nil
			}
			var cr Credit
			if err := Decode(f, &cr); err != nil || cr.Type != "credit" {
				return nil, errors.New("mover: ждали кредиты")
			}
			credits += cr.Credits
		}
		n := int64(len(buf))
		if rem := in.Size - sent; rem < n {
			n = rem
		}
		if credits < n {
			n = credits
		}
		if _, err := io.ReadFull(in.Body, buf[:n]); err != nil {
			return nil, fmt.Errorf("mover: чтение источника: %w", err)
		}
		if err := WriteData(c.conn, in.Op, buf[:n]); err != nil {
			return nil, err
		}
		sent += n
		credits -= n
	}

	for {
		res, _, err := c.readControl()
		if err != nil {
			return nil, err
		}
		if res != nil {
			return res, nil
		}
	}
}

// Get читает объект в w и сверяет SHA-256, которую назвал mover. Приёмник
// проверяет сумму сам: байты с неверным хвостом сегмента GCM иначе всплывут
// только при чтении клиентом.
func (c *Client) Get(op, id uuid.UUID, w io.Writer) (*Result, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, errors.New("mover: соединение закрыто")
	}
	if err := WriteJSON(c.conn, Get{Type: "get", Op: op, ID: id}); err != nil {
		return nil, err
	}
	// Кредиты в этом направлении выдаёт принимающая сторона; окно выдаём сразу,
	// оно дождётся mover'а в буфере сокета.
	window := int64(c.chunk) * 4
	if err := WriteJSON(c.conn, Credit{Type: "credit", Op: op, Credits: window}); err != nil {
		return nil, err
	}

	h := sha256.New()
	credits := window
	for {
		f, err := ReadFrame(c.conn, MaxFrameBytes)
		if err != nil {
			return nil, err
		}
		if f.Kind == frameData {
			if f.Op != op {
				return nil, errors.New("mover: данные чужой операции")
			}
			if _, err := w.Write(f.B); err != nil {
				return nil, err
			}
			h.Write(f.B)
			credits -= int64(len(f.B))
			if err := WriteJSON(c.conn, Credit{Type: "credit", Op: op, Credits: int64(len(f.B))}); err != nil {
				return nil, err
			}
			continue
		}
		var res Result
		if err := Decode(f, &res); err != nil {
			continue
		}
		if res.Type != "result" {
			continue // state и прочее
		}
		if res.Code != CodeOK {
			return &res, nil
		}
		if sum := hex.EncodeToString(h.Sum(nil)); sum != res.SHA256 {
			return &res, fmt.Errorf("mover: SHA-256 потока не совпала: получили %s, mover объявил %s", sum, res.SHA256)
		}
		return &res, nil
	}
}

// Status спрашивает состояние операции — так tapegw забирает итог после
// переподключения: обрыв соединения запись не отменяет.
func (c *Client) Status(op uuid.UUID) (*Result, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, errors.New("mover: соединение закрыто")
	}
	if err := WriteJSON(c.conn, Status{Type: "status", Op: op}); err != nil {
		return nil, err
	}
	for {
		res, _, err := c.readControl()
		if err != nil {
			return nil, err
		}
		if res != nil {
			return res, nil
		}
	}
}

// readControl читает один кадр: возвращает (result, nil) если это итог,
// иначе (nil, frame) для ready / resume / credit / state.
func (c *Client) readControl() (*Result, *Frame, error) {
	f, err := ReadFrame(c.conn, MaxFrameBytes)
	if err != nil {
		return nil, nil, err
	}
	if f.Kind != frameJSON {
		return nil, f, nil
	}
	var head struct {
		Type string `json:"t"`
	}
	if err := Decode(f, &head); err != nil {
		return nil, nil, err
	}
	if head.Type != "result" {
		return nil, f, nil
	}
	var res Result
	if err := Decode(f, &res); err != nil {
		return nil, nil, err
	}
	return &res, nil, nil
}

// SumBytes — hex SHA-256, та же функция, что объявляется в Put.
func SumBytes(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}
