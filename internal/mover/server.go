package mover

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/store"
)

// ServerConfig — параметры mover'а.
type ServerConfig struct {
	Addr      string
	TLS       *tls.Config
	Role      Role
	Version   string
	PhobosVer string
	// PhobosFamily/Layout/LayoutParams приходят из конфига: без явных family и
	// library вызов падает с -ENODEV, и из текста ошибки это не понять (M0-02).
	Family       string
	Layout       string
	LayoutParams map[string]string
	Chunk        int
	Log          *slog.Logger
	// Inactivity — сколько mover ждёт следующий кадр. Полуоткрытое соединение
	// (сеть пропала без RST) или клиент, который открыл соединение и молчит,
	// иначе держит горутину и операцию бесконечно; при разделённых серверах
	// полуоткрытые соединения — обычное дело.
	Inactivity time.Duration
	// OpTimeout — потолок одной операции Phobos: на время записи дедлайны
	// продлеваются до него, потому что батч на 200 GiB идёт минуты.
	OpTimeout time.Duration
}

// Server — mover: слушает, принимает операции и выполняет их через store.
//
// Один процесс = один phobos_init = одна операция Phobos за раз (ADR-0007),
// поэтому вызовы store сериализованы мьютексом. Соединений при этом может быть
// несколько: tapegw вправе держать рабочее подключение и отдельное — для
// вопросов о состоянии после переподключения.
type Server struct {
	st       store.Store
	reg      *Registry
	cfg      ServerConfig
	ln       net.Listener
	phobosMu sync.Mutex
	log      *slog.Logger
	closed   chan struct{}
	wg       sync.WaitGroup

	mu    sync.Mutex
	conns map[net.Conn]ClientID
}

// NewServer поднимает mover на st. store — это Phobos под build tag `phobos`
// и fake в тестах: сам mover про cgo ничего не знает.
func NewServer(st store.Store, reg *Registry, cfg ServerConfig) (*Server, error) {
	if cfg.TLS == nil {
		// Отдельного режима «без шифрования» нет намеренно: иначе «забыли
		// включить на боевом» остаётся возможным.
		return nil, ErrNoCert
	}
	if cfg.Chunk == 0 {
		cfg.Chunk = ChunkSize
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	if cfg.Inactivity <= 0 {
		cfg.Inactivity = 5 * time.Minute
	}
	if cfg.OpTimeout <= cfg.Inactivity {
		cfg.OpTimeout = 30 * time.Minute
	}
	ln, err := tls.Listen("tcp", cfg.Addr, cfg.TLS)
	if err != nil {
		return nil, fmt.Errorf("mover: слушать: %w", err)
	}
	return &Server{
		st:     st,
		reg:    reg,
		cfg:    cfg,
		ln:     ln,
		log:    cfg.Log,
		closed: make(chan struct{}),
		conns:  map[net.Conn]ClientID{},
	}, nil
}

// Addr — фактический адрес полезен тестам, которые слушают :0.
func (s *Server) Addr() string { return s.ln.Addr().String() }

// Serve принимает соединения до Close.
func (s *Server) Serve() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			select {
			case <-s.closed:
				return
			default:
			}
			s.log.Error("mover: приём соединения", "err", err)
			return
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handle(conn)
		}()
	}
}

// Close останавливает приём и ждёт текущих операций: прерывать Phobos на
// середине нельзя, поэтому остановка — это «перестать принимать новое».
func (s *Server) Close() error {
	close(s.closed)
	err := s.ln.Close()
	// Соединения закрываются принудительно: обработчик может ждать кадр, а
	// «дождаться ответа» — не остановка. Дедлайны вторая линия, а не замена.
	s.mu.Lock()
	for c := range s.conns {
		_ = c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
	return err
}

// touch продлевает дедлайны на время ожидания кадра.
func (s *Server) touch(c net.Conn) { _ = c.SetDeadline(time.Now().Add(s.cfg.Inactivity)) }

// touchOp продлевает дедлайны на потолок операции: пока идёт phobos_put, кадров
// нет, и обычный таймаут оборвал бы запись на середине.
func (s *Server) touchOp(c net.Conn) { _ = c.SetDeadline(time.Now().Add(s.cfg.OpTimeout)) }

func (s *Server) handle(conn net.Conn) {
	s.mu.Lock()
	s.conns[conn] = ""
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		conn.Close()
	}()
	tconn, ok := conn.(*tls.Conn)
	if !ok {
		return
	}
	s.touch(tconn)
	// Взаимный TLS: без подтверждённого клиентского сертификата соединение не
	// обслуживается. Проверка здесь, а не «где-то в разборе», потому что
	// отсутствие проверки должно быть видно в коде границы.
	if err := tconn.HandshakeContext(context.Background()); err != nil {
		s.log.Warn("mover: рукопожатие TLS", "err", err, "peer", conn.RemoteAddr().String())
		return
	}
	if len(tconn.ConnectionState().PeerCertificates) == 0 {
		s.log.Warn("mover: соединение без клиентского сертификата", "peer", conn.RemoteAddr().String())
		return
	}
	// Роль берём из SAN-URI подтверждённого сертификата, а не из hello: hello
	// пишет сам клиент, и проверять его — значит не проверять ничего.
	if cid, err := clientIDOf(tconn); err == nil {
		s.mu.Lock()
		s.conns[tconn] = cid
		s.mu.Unlock()
	}

	if err := WriteJSON(tconn, Hello{
		Type: "hello", Proto: ProtoVersion, Mover: s.cfg.Version,
		Phobos: s.cfg.PhobosVer, Role: s.cfg.Role, BufferFree: s.reg.Free(),
	}); err != nil {
		return
	}
	f, err := ReadFrame(tconn, MaxFrameBytes)
	if err != nil {
		return
	}
	var hk HelloOk
	if err := Decode(f, &hk); err != nil || hk.Type != "hello_ok" {
		return
	}
	if hk.Proto != ProtoVersion {
		// Ошибка разбора посреди передачи хуже отказа: отказ хотя бы понятен.
		_ = WriteJSON(tconn, Result{Type: "result", Code: CodeBusy, State: StateFailed})
		return
	}

	var carry *Frame
	for {
		f := carry
		carry = nil
		if f == nil {
			s.touch(tconn)
			var err error
			f, err = ReadFrame(tconn, MaxFrameBytes)
			if err != nil {
				if !errors.Is(err, io.EOF) {
					s.log.Debug("mover: соединение закрыто", "err", err)
				}
				return
			}
		}
		var head struct {
			Type string    `json:"t"`
			Op   uuid.UUID `json:"op"`
			ID   uuid.UUID `json:"id"`
		}
		if err := Decode(f, &head); err != nil {
			return
		}
		switch head.Type {
		case "put":
			var p Put
			if err := Decode(f, &p); err != nil {
				return
			}
			carry = s.handlePut(tconn, &p)
		case "get":
			var g Get
			if err := Decode(f, &g); err != nil {
				return
			}
			carry = s.handleGet(tconn, &g)
		case "status":
			s.handleStatus(tconn, head.Op)
		case "credit", "state":
			// Кадры потока управления от уже завершившейся операции: окно
			// кредитов было выдано с запасом, и получатель вправе вернуть
			// остаток когда угодно. Командами они не являются, рвать
			// соединение из-за них — значит обрывать себя на каждом get.
			continue
		default:
			return
		}
	}
}

func (s *Server) handleStatus(conn net.Conn, op uuid.UUID) {
	rec, ok := s.reg.Get(op)
	if !ok {
		_ = WriteJSON(conn, Result{Type: "result", Op: op, Code: CodeNotFound, State: StateFailed})
		return
	}
	_ = WriteJSON(conn, Result{
		Type: "result", Op: op, Code: rec.Code, State: rec.State,
		Version: rec.Version, Extents: rec.Extents, SHA256: rec.SHA256Out,
	})
}

// handlePut принимает поток в буфер, сверяет SHA-256 и только потом пишет на
// ленту. Порядок принципиален: пока сумма не совпала, на носителе ничего нет,
// и это единственное, что делает безопасной докачку по смещению.
func (s *Server) handlePut(conn net.Conn, p *Put) *Frame {
	send := func(r Result) { r.Type = "result"; r.Op = p.Op; _ = WriteJSON(conn, r) }

	if p.FromOp != uuid.Nil {
		// Локальная передача (Ф0.4): содержимое берётся из буфера другой
		// операции этого же mover'а, байты по сети не идут.
		s.handlePutFrom(conn, p)
		return nil
	}

	rec, created, err := s.reg.Begin(p.Op, p.Size, p.SHA256, p.Attrs, p.Tags, string(s.clientOf(conn)))
	switch {
	case errors.Is(err, ErrOpConflict):
		// Тот же op с другой парой (size, sha256). Итоговая сверка тут ничего
		// бы не поймала: сумма-то пришла своя, правильная.
		send(Result{Code: CodeOpConflict, State: StateFailed})
		return nil
	case errors.Is(err, ErrBufferTooSmall):
		send(Result{Code: CodeNoBuffer, State: StateFailed})
		return nil
	case err != nil:
		send(Result{Code: CodeIO, State: StateFailed})
		return nil
	}

	if !created && rec.State == StateDone {
		// Идемпотентность: повтор после обрыва не пишет второй раз.
		send(Result{Code: CodeOK, State: StateDone, Version: rec.Version, Extents: rec.Extents})
		return nil
	}
	if !created && rec.State == StateFailed {
		// Неудачу повторяют с нуля: буфер после checksum_mismatch уже выброшен.
		if err := s.reg.Update(p.Op, func(r *OpRecord) { r.Received = 0; r.State = StateStaged }); err != nil {
			send(Result{Code: CodeIO, State: StateFailed})
			return nil
		}
		rec, _ = s.reg.Get(p.Op)
	}

	f, err := s.reg.DataFile(p.Op)
	if err != nil {
		send(Result{Code: CodeIO, State: StateFailed})
		return nil
	}
	defer f.Close()

	offset := rec.Received
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		send(Result{Code: CodeIO, State: StateFailed})
		return nil
	}
	kind := "ready"
	if offset > 0 {
		kind = "resume"
	}
	if err := WriteJSON(conn, Ready{Type: kind, Op: p.Op, Offset: offset, Credits: s.credits()}); err != nil {
		return nil
	}

	received := offset
	for received < p.Size {
		s.touch(conn)
		fr, err := ReadFrame(conn, MaxFrameBytes)
		if err != nil {
			return nil
		}
		if fr.Kind == frameJSON {
			// Клиент бросил досылать байты и открыл новую операцию. Считать
			// её объявление данными нельзя: оно уходит обратно в диспетчер.
			_ = s.reg.Update(p.Op, func(r *OpRecord) { r.State, r.Code = StateFailed, CodeIO })
			return fr
		}
		if fr.Op != p.Op {
			send(Result{Code: CodeIO, State: StateFailed})
			return nil
		}
		if _, err := f.Write(fr.B); err != nil {
			send(Result{Code: CodeIO, State: StateFailed})
			return nil
		}
		received += int64(len(fr.B))
		// Смещение сохраняется на каждом кадре: если клиент оборвётся здесь,
		// Resume обязан показать ровно принятые байты, а не ноль.
		if err := s.reg.Update(p.Op, func(r *OpRecord) { r.Received = received }); err != nil {
			send(Result{Code: CodeIO, State: StateFailed})
			return nil
		}
		// Кредиты восполняются по мере записи: переполнение буфера есть
		// «подожди», а не ошибка записи.
		s.touch(conn)
		if err := WriteJSON(conn, Credit{Type: "credit", Op: p.Op, Credits: int64(len(fr.B))}); err != nil {
			return nil
		}
	}
	if err := f.Sync(); err != nil {
		send(Result{Code: CodeIO, State: StateFailed})
		return nil
	}

	match, err := s.reg.Verify(p.Op)
	if err != nil || !match {
		// На ленту не попало ничего — это главное в этой ветке.
		_ = s.reg.Update(p.Op, func(r *OpRecord) { r.State = StateFailed; r.Code = CodeChecksumMismatch })
		_ = s.reg.Drop(p.Op)
		send(Result{Code: CodeChecksumMismatch, State: StateFailed})
		return nil
	}

	_ = s.reg.Update(p.Op, func(r *OpRecord) { r.State = StateWriting })
	_ = WriteJSON(conn, StateMsg{Type: "state", Op: p.Op, State: StateWriting})

	// На время записи дедлайны продлеваются до потолка операции: батч может
	// идти минуты, и обычный таймаут ожидания кадра оборвал бы её на середине.
	s.touchOp(conn)
	res := s.write(p)
	s.touch(conn)
	code := res.Code
	version := res.Version
	extents := res.Extents

	if code == CodeOK {
		_ = s.reg.Update(p.Op, func(r *OpRecord) {
			r.State, r.Code, r.Version, r.Extents = StateDone, CodeOK, version, extents
		})
		// Буфер освобождаем, запись об операции оставляем: по ней повтор
		// после обрыва ответит «уже записано», а не перепишет объект.
		_ = s.reg.ReleaseData(p.Op)
	} else {
		_ = s.reg.Update(p.Op, func(r *OpRecord) { r.State, r.Code = StateFailed, code })
	}
	send(Result{Code: code, State: func() State {
		if code == CodeOK {
			return StateDone
		}
		return StateFailed
	}(), Version: version, Extents: extents})
	return nil
}

// write — собственно вызов Phobos. Сериализован: один процесс = один
// phobos_init = одна операция.
func (s *Server) write(p *Put) Result {
	f, err := s.reg.DataFile(p.Op)
	if err != nil {
		return Result{Code: CodeIO}
	}
	defer f.Close()
	return s.putReader(p, f)
}

// writeFrom — та же запись, но содержимое читается из буфера другой операции
// (Ф0.4). Отдельный метод, а не параметр: источник обязан быть открыт и
// проверен до вызова Phobos, и смешивать это с обычным путём нельзя.
func (s *Server) writeFrom(p *Put, src uuid.UUID) Result {
	f, err := s.reg.DataFile(src)
	if err != nil {
		return Result{Code: CodeIO}
	}
	defer f.Close()
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return Result{Code: CodeIO}
	}
	return s.putReader(p, f)
}

func (s *Server) putReader(p *Put, f io.Reader) Result {
	s.phobosMu.Lock()
	defer s.phobosMu.Unlock()

	opt := store.PutOptions{
		Family: s.cfg.Family, Layout: s.cfg.Layout,
		LayoutParams: s.cfg.LayoutParams, Tags: p.Tags, NoSplit: p.NoSplit,
	}
	items := []store.PutItem{{ID: p.Op, Reader: f, Size: p.Size, Attrs: p.Attrs}}
	res := s.st.Put(context.Background(), items, opt)
	if len(res) != 1 {
		return Result{Code: CodeUnknown}
	}
	r := res[0]
	switch {
	case r.Err == nil:
	case errors.Is(r.Err, store.ErrExists):
		// Уже записано — это успех, но только если копия полна.
		if meta, err := s.st.Stat(context.Background(), p.Op); err == nil && meta.Complete {
			return Result{Code: CodeOK, Version: meta.Version}
		}
		return Result{Code: CodeIncomplete}
	default:
		return Result{Code: codeFor(r.Err)}
	}

	var extents []string
	if ext, err := s.st.Extents(context.Background(), []uuid.UUID{p.Op}); err == nil {
		extents = ext[p.Op]
	}
	return Result{Code: CodeOK, Version: r.Version, Extents: extents}
}

// handleGet читает объект в буфер и отдаёт потоком. Кредиты в этом направлении
// выдаёт принимающая сторона — tapegw, и он же сверяет SHA-256.
func (s *Server) handleGet(conn net.Conn, g *Get) *Frame {
	send := func(r Result) { r.Type = "result"; r.Op = g.Op; _ = WriteJSON(conn, r) }

	rec, _, err := s.reg.Begin(g.Op, 0, "", nil, nil, string(s.clientOf(conn)))
	if errors.Is(err, ErrOpConflict) {
		send(Result{Code: CodeOpConflict, State: StateFailed})
		return nil
	}
	if err != nil && !errors.Is(err, ErrBufferTooSmall) {
		send(Result{Code: CodeIO, State: StateFailed})
		return nil
	}
	_ = rec

	_ = WriteJSON(conn, StateMsg{Type: "state", Op: g.Op, State: StateWriting})

	s.phobosMu.Lock()
	f, err := s.reg.DataFile(g.Op)
	if err != nil {
		s.phobosMu.Unlock()
		send(Result{Code: CodeIO, State: StateFailed})
		return nil
	}
	res := s.st.Get(context.Background(), []store.GetItem{{ID: g.ID, Writer: f}})
	code := CodeIO
	if len(res) == 1 {
		code = codeFor(res[0].Err)
	}
	if code == CodeOK {
		if err := f.Sync(); err != nil {
			code = CodeIO
		}
	}
	s.phobosMu.Unlock()
	if code != CodeOK {
		send(Result{Code: code, State: StateFailed})
		return nil
	}

	path := s.reg.dataPath(g.Op)
	sum, err := SumFile(path)
	if err != nil {
		send(Result{Code: CodeIO, State: StateFailed})
		return nil
	}
	fi, err := s.reg.DataFile(g.Op)
	if err != nil {
		send(Result{Code: CodeIO, State: StateFailed})
		return nil
	}
	defer fi.Close()
	st, err := fi.Stat()
	if err != nil {
		send(Result{Code: CodeIO, State: StateFailed})
		return nil
	}
	size := st.Size()

	if err := WriteJSON(conn, Ready{Type: "ready", Op: g.Op, Offset: 0, Credits: 0}); err != nil {
		return nil
	}
	var credits int64
	buf := make([]byte, s.cfg.Chunk)
	for off := int64(0); off < size; {
		for credits <= 0 {
			s.touch(conn)
			fr, err := ReadFrame(conn, MaxFrameBytes)
			if err != nil {
				return nil
			}
			var c Credit
			if err := Decode(fr, &c); err != nil || c.Type != "credit" {
				send(Result{Code: CodeIO, State: StateFailed})
				return nil
			}
			credits += c.Credits
		}
		n := int64(len(buf))
		if rem := size - off; rem < n {
			n = rem
		}
		if credits < n {
			n = credits
		}
		if _, err := fi.ReadAt(buf[:n], off); err != nil && !errors.Is(err, io.EOF) {
			send(Result{Code: CodeIO, State: StateFailed})
			return nil
		}
		// Запись может встать, если клиент не читает: дедлайн обязана держать
		// и сторона, которая отдаёт.
		s.touchOp(conn)
		if err := WriteData(conn, g.Op, buf[:n]); err != nil {
			return nil
		}
		off += n
		credits -= n
	}
	send(Result{Code: CodeOK, State: StateDone, SHA256: sum})
	_ = s.reg.Drop(g.Op)
	return nil
}

func (s *Server) credits() int64 {
	free := s.reg.Free()
	if w := int64(s.cfg.Chunk) * 4; free > w {
		return w
	}
	return free
}

// codeFor переводит ошибку хранилища в код протокола. Набор замкнут: новых
// кодов mover не выдаёт, иначе воркер начнёт угадывать.
func codeFor(err error) Code {
	switch {
	case err == nil:
		return CodeOK
	case errors.Is(err, store.ErrNotFound):
		return CodeNotFound
	case errors.Is(err, store.ErrNoSpace):
		return CodeNoSpace
	case errors.Is(err, store.ErrBusy):
		return CodeBusy
	case errors.Is(err, store.ErrIncomplete):
		return CodeIncomplete
	case errors.Is(err, store.ErrAttrTooLarge):
		return CodeAttrTooLarge
	case errors.Is(err, store.ErrExists):
		// Не CodeOK: «уже есть» не значит «целая копия». См. CodeExists.
		return CodeExists
	default:
		return CodeIO
	}
}

// clientOf — роль клиента по подтверждённому сертификату соединения. Пустая,
// если соединение не передало проверку (тогда и локальная передача будет
// отклонена: пустая роль не совпадёт ни с какой записью).
// clientIDOf — роль клиента из SAN-URI подтверждённого сертификата соединения.
func clientIDOf(c *tls.Conn) (ClientID, error) {
	certs := c.ConnectionState().PeerCertificates
	if len(certs) == 0 {
		return "", errors.New("mover: сертификат не предъявлен")
	}
	uris, err := peerURIs([][]byte{certs[0].Raw}, x509.ExtKeyUsageClientAuth)
	if err != nil {
		return "", err
	}
	for _, u := range uris {
		if u.Scheme == "tapegw" {
			return ClientID(u.Host), nil
		}
	}
	return "", errors.New("mover: в сертификате нет SAN-URI клиента")
}

func (s *Server) clientOf(conn net.Conn) ClientID {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conns[conn]
}

// handlePutFrom — локальная передача (Ф0.4): новый объект пишется из буфера
// уже завершённой операции этого же mover'а. Байты по сети не идут, лента
// не читается повторно.
//
// Правила и почему именно они:
//   - источник только done — буфер в staged ещё не дочитан и не сверен, failed
//     уже выброшен;
//   - ожидаемая сумма обязательна и должна совпасть с суммой источника: без
//     этого op становится указателем на любой буфер, а копия перестаёт быть
//     копией;
//   - тот же клиент, что записал источник: иначе одна сторона читает данные
//     другой;
//   - буфер источника держится счётчиком ссылок, пока передача не кончится, —
//     иначе result_ttl выбросит содержимое на середине записи.
func (s *Server) handlePutFrom(conn net.Conn, p *Put) {
	send := func(r Result) { r.Type = "result"; r.Op = p.Op; _ = WriteJSON(conn, r) }

	src, ok := s.reg.Get(p.FromOp)
	switch {
	case !ok || src.State != StateDone:
		send(Result{Code: CodeNotFound, State: StateFailed})
		return
	case p.SHA256 == "":
		send(Result{Code: CodeOpConflict, State: StateFailed})
		return
	case src.SHA256 != p.SHA256 || src.Size != p.Size:
		send(Result{Code: CodeOpConflict, State: StateFailed})
		return
	case p.FromOp == p.Op:
		send(Result{Code: CodeOpConflict, State: StateFailed})
		return
	}
	if src.Client == "" || src.Client != string(s.clientOf(conn)) {
		send(Result{Code: CodeDenied, State: StateFailed})
		return
	}

	rec, created, err := s.reg.Begin(p.Op, p.Size, p.SHA256, p.Attrs, p.Tags, string(s.clientOf(conn)))
	if err != nil {
		send(Result{Code: CodeIO, State: StateFailed})
		return
	}
	if !created && rec.State == StateDone {
		send(Result{Code: CodeOK, State: StateDone, Version: rec.Version, Extents: rec.Extents})
		return
	}

	if err := s.reg.Acquire(p.FromOp); err != nil {
		send(Result{Code: CodeNotFound, State: StateFailed})
		return
	}
	defer func() {
		if err := s.reg.Release(p.FromOp); err != nil {
			s.log.Error("mover: снять ссылку с источника", "op", p.FromOp, "err", err)
		}
	}()

	s.touchOp(conn)
	res := s.writeFrom(p, p.FromOp)
	s.touch(conn)
	if res.Code != CodeOK {
		_ = s.reg.Update(p.Op, func(r *OpRecord) { r.State, r.Code = StateFailed, res.Code })
		send(Result{Code: res.Code, State: StateFailed})
		return
	}
	_ = s.reg.Update(p.Op, func(r *OpRecord) {
		r.State, r.Code, r.Version, r.Extents = StateDone, CodeOK, res.Version, res.Extents
	})
	send(Result{Code: CodeOK, State: StateDone, Version: res.Version, Extents: res.Extents})
}
