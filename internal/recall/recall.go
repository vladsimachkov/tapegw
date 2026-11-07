// Package recall — минимальный recaller (S5): читает пачки с ленты через
// mover, извлекает участников в cache и переводит restore в 'restored'.
//
// Порядок задаёт ADR-0004 в сторону, обратную вытеснению: файл участника
// сначала целиком записан во временный файл, синхронизирован на диск и
// проверен, и только потом переход в 'restored' в транзакции. Строка,
// обещающая копию, которой на диске ещё нет, здесь недопустима.
//
// Проверка целостности — двухуровневая, и обе суммы сохранены при записи:
//
//   - поток пачки сверяется с archive_copies.sha256 — суммой, записанной
//     migrator'ом при архивации (mover при get считает сумму прочитанного
//     с ленты и отдаёт её в результате);
//   - каждый участник проверяется по-своему: размер файла, затем ПОЛНОЕ
//     расшифрование (AEAD-теги сегментов и манифеста — ADR-0019) с сверкой
//     открытого размера и, для одиночных PUT, md5 открытого содержимого с
//     ETag. Сверять шифротекст «самим с собой» бессмысленно: клиент получает
//     открытые байты, и проверять надо именно их.
//
// Флаг landing recaller не трогает: восстановленная копия живёт в cache и
// вытесняется по своему основанию — истечению online_until (janitor).
package recall

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/encrypt"
	"github.com/vladsimachkov/tapegw/internal/keys"
	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/mover"
	"github.com/vladsimachkov/tapegw/internal/spool"
)

// Getter — та часть mover.Client, что нужна recaller'у. Интерфейс, а не
// конкретный клиент: тесты подменяют ленту словарём байтов.
type Getter interface {
	Get(op, id uuid.UUID, w io.Writer) (*mover.Result, error)
}

// Recall — один проход recaller'а. Все числовые параметры приходят из конфига
// (правило 9); нули здесь — не «значения по умолчанию», а ошибка сборки.
type Recall struct {
	Meta  *meta.Meta
	Get   Getter
	Cache *spool.Spool
	Keys  *keys.Vault

	BatchMax     int
	ClaimTTL     time.Duration
	MaxAttempts  int
	RetryBackoff time.Duration

	Log *slog.Logger
}

func (r *Recall) logger() *slog.Logger {
	if r.Log == nil {
		return slog.Default()
	}
	return r.Log
}

// Once — один проход: выборка очереди, затем по пачке на одно чтение ленты.
// Возвращает число восстановленных объектов. Ошибка — только сбой выборки или
// БД; ошибки отдельных пачек уже учтены в очереди (ретрах/failed).
func (r *Recall) Once(ctx context.Context) (int, error) {
	items, err := r.Meta.ClaimRestoreBatch(ctx, r.BatchMax, r.ClaimTTL, r.MaxAttempts)
	if err != nil {
		return 0, err
	}
	if len(items) == 0 {
		return 0, nil
	}

	// Группировка по пачке: участники одной пачки читаются одним потоком с
	// ленты. Порядок пачек — по первому участнику, внутри — по смещению.
	var order []uuid.UUID
	byPack := map[uuid.UUID][]meta.RestoreItem{}
	for _, it := range items {
		if _, ok := byPack[it.PackID]; !ok {
			order = append(order, it.PackID)
		}
		byPack[it.PackID] = append(byPack[it.PackID], it)
	}

	n := 0
	for _, packID := range order {
		group := byPack[packID]
		sort.Slice(group, func(i, j int) bool { return group[i].PackOffset < group[j].PackOffset })
		n += r.recallPack(ctx, packID, group)
	}
	return n, nil
}

// recallPack — одна пачка: чтение потока с ленты, извлечение участников,
// проверка, публикация в cache, переход в 'restored'. Ошибка одной пачки не
// задевает остальные: учёт попыток идёт построчно.
func (r *Recall) recallPack(ctx context.Context, packID uuid.UUID, group []meta.RestoreItem) int {
	log := r.logger()

	cp, err := r.Meta.PackCopy(ctx, packID)
	if err != nil {
		reason := fmt.Sprintf("копия пачки %s не найдена: %v", packID, err)
		log.Error("recaller: "+reason, slog.String("pack", packID.String()))
		r.failAll(ctx, group, reason)
		return 0
	}

	// Временные файлы участников: tmp/ буфера — на той же ФС, что и cache,
	// поэтому Publish поднимет файл rename'ом без EXDEV.
	type staged struct {
		item meta.RestoreItem
		f    *os.File
	}
	stagedItems := make([]staged, 0, len(group))
	defer func() {
		// Любой незакрытый хвост (ошибка чтения, отказ проверки) убирается:
		// tmp/ не должен расти после каждой неудачной попытки.
		for _, s := range stagedItems {
			if s.f != nil {
				name := s.f.Name()
				s.f.Close()
				os.Remove(name)
			}
		}
	}()

	targets := make([]extractTarget, 0, len(group))
	for _, it := range group {
		f, err := r.Cache.Tmp()
		if err != nil {
			log.Error("recaller: временный файл cache", slog.String("err", err.Error()))
			r.failAll(ctx, group, "временный файл cache: "+err.Error())
			return 0
		}
		stagedItems = append(stagedItems, staged{item: it, f: f})
		targets = append(targets, extractTarget{
			off: it.PackOffset, end: it.PackOffset + it.FileBytes, w: f,
		})
	}

	op := uuid.New()
	res, err := r.Get.Get(op, cp.OID, &extractor{targets: targets})
	if err == nil && res != nil && res.Code != mover.CodeOK {
		err = fmt.Errorf("mover ответил %s", res.Code)
	}
	if err != nil {
		reason := fmt.Sprintf("чтение пачки %s: %v", packID, err)
		log.Error("recaller: "+reason, slog.String("pack", packID.String()))
		r.failAll(ctx, group, reason)
		return 0
	}

	// Сумма потока пачки — та, что записана при архивации. Расхождение —
	// испорченная лента или чужой объект под этим oid: тихий повтор ничего не
	// лечит, это алерт, а не штатная ошибка.
	want := hex.EncodeToString(cp.SHA256)
	if !strings.EqualFold(res.SHA256, want) {
		reason := fmt.Sprintf("сумма пачки %s не совпала с записанной: лента %q, ждали %q",
			packID, res.SHA256, want)
		log.Error("recaller: "+reason, slog.String("pack", packID.String()))
		r.failAll(ctx, group, reason)
		return 0
	}

	n := 0
	for i := range stagedItems {
		s := &stagedItems[i]
		it := s.item
		if err := r.verify(ctx, it, s.f); err != nil {
			// Громкая ошибка и в журнал, и в строку: данные с ленты пришли
			// непригодными — это не «клиенту отказать», это оператору смотреть.
			log.Error("recaller: участник не прошёл проверку",
				slog.String("key", it.Key), slog.String("id", it.ObjectID.String()),
				slog.String("pack", packID.String()), slog.String("err", err.Error()))
			r.fail(ctx, it, "проверка участника: "+err.Error())
			continue
		}
		// Publish закрывает файл и записывает его на место (fsync → rename →
		// fsync каталога). После него файл в cache есть — и только теперь
		// можно обещать его в БД (ADR-0004).
		f := s.f
		s.f = nil // defer больше не трогает
		if err := r.Cache.Publish(f, it.ObjectID); err != nil {
			log.Error("recaller: публикация в cache",
				slog.String("key", it.Key), slog.String("err", err.Error()))
			r.fail(ctx, it, "публикация в cache: "+err.Error())
			continue
		}
		if err := r.Meta.MarkRestoreRestored(ctx, it.ObjectID, it.Days); err != nil {
			if errors.Is(err, meta.ErrRestoreLost) {
				log.Info("recaller: объект удалён во время восстановления, файл останется сиротой до сборки мусора",
					slog.String("id", it.ObjectID.String()))
			} else {
				log.Error("recaller: переход в restored",
					slog.String("key", it.Key), slog.String("err", err.Error()))
			}
			continue
		}
		n++
		log.Info("recaller: объект восстановлен в cache",
			slog.String("key", it.Key), slog.String("id", it.ObjectID.String()),
			slog.Int("days", it.Days))
	}
	return n
}

// verify — проверка участника до публикации. Файл открыт на чтение; размер
// берётся из fs, содержимое — полным расшифрованием: AEAD-теги сегментов и
// манифеста (ADR-0019) и есть «сумма, сохранённая при записи» для шифра, а
// ETag сверяется уже по открытым байтам.
func (r *Recall) verify(ctx context.Context, it meta.RestoreItem, f *os.File) error {
	st, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat: %w", err)
	}
	if st.Size() != it.FileBytes {
		return fmt.Errorf("файл %d байт, в БД обещано %d", st.Size(), it.FileBytes)
	}

	dek, err := r.Keys.OpenVersionDEK(ctx, it.TenantID, it.WrappedDEK, it.ObjectID)
	if err != nil {
		return fmt.Errorf("ключ версии: %w", err)
	}
	defer encrypt.Zero(dek)

	parts, err := r.Meta.ObjectParts(ctx, it.ObjectID)
	if err != nil {
		return fmt.Errorf("таблица частей: %w", err)
	}
	if len(parts) == 0 {
		return errors.New("в object_parts нет ни одной части — БД рассинхронизирована с файлом")
	}
	enc := make([]encrypt.Part, 0, len(parts))
	for _, p := range parts {
		enc = append(enc, encrypt.Part{
			Number: uint32(p.Number), PlainSize: p.PlainSize,
			CipherOffset: p.CipherOffset, Salt: p.Salt,
		})
	}
	rd, err := encrypt.NewPartsReader(f, dek, it.ObjectID, enc, it.Size)
	if err != nil {
		return fmt.Errorf("открыть шифротекст: %w", err)
	}
	defer rd.Close()

	h := md5.New()
	n, err := io.Copy(h, encrypt.NewSeqReader(rd))
	if err != nil {
		return fmt.Errorf("расшифрование: %w", err)
	}
	if n != it.Size {
		return fmt.Errorf("открытых байт %d, в БД обещано %d", n, it.Size)
	}
	// ETag — md5 открытых байтов только у одиночных PUT; у multipart это
	// свёртка md5-частей, её из потока не восстановить (ARCHITECTURE:
	// «проверить размер и ETag (для одиночных PUT — md5)»).
	if !strings.Contains(it.ETag, "-") {
		if sum := hex.EncodeToString(h.Sum(nil)); sum != it.ETag {
			return fmt.Errorf("md5 открытого содержимого %s, в БД записан %s", sum, it.ETag)
		}
	}
	return nil
}

func (r *Recall) fail(ctx context.Context, it meta.RestoreItem, reason string) {
	if err := r.Meta.MarkRestoreFailure(ctx, it.ObjectID, r.MaxAttempts, r.RetryBackoff, reason); err != nil {
		r.logger().Error("recaller: учёт попытки",
			slog.String("id", it.ObjectID.String()), slog.String("err", err.Error()))
	}
}

func (r *Recall) failAll(ctx context.Context, group []meta.RestoreItem, reason string) {
	for _, it := range group {
		r.fail(ctx, it, reason)
	}
}

// extractTarget — окно байтов одного участника в потоке пачки.
type extractTarget struct {
	off, end int64
	w        io.Writer
}

// extractor вырезает окна участников из последовательного потока ленты:
// mover отдаёт пачку целиком, а на диске нужен только файл участника.
// Окна обязаны быть отсортированы по off и не пересекаться — участники пачки
// не перекрываются по построению (ADR-0020).
type extractor struct {
	targets []extractTarget
	pos     int64
	cur     int
}

func (e *extractor) Write(p []byte) (int, error) {
	total := len(p)
	for len(p) > 0 {
		for e.cur < len(e.targets) && e.targets[e.cur].end <= e.pos {
			e.cur++
		}
		if e.cur >= len(e.targets) {
			break // остаток потока не нужен никому
		}
		t := e.targets[e.cur]
		if e.pos < t.off {
			skip := t.off - e.pos
			if int64(len(p)) < skip {
				skip = int64(len(p))
			}
			p = p[skip:]
			e.pos += skip
			continue
		}
		n := t.end - e.pos
		if int64(len(p)) < n {
			n = int64(len(p))
		}
		if _, err := t.w.Write(p[:n]); err != nil {
			return 0, err
		}
		p = p[n:]
		e.pos += n
	}
	return total, nil
}
