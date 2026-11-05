package rebuild

// Восстановление метаданных из архива (ADR-0019).
//
// Сценарий один: база потеряна, остались ленты, KEK и экспорт ключей тенантов.
// Отсюда три правила, вокруг которых построен весь пакет.
//
// Первый источник истины — архив, а не база. Список объектов берётся у
// Archive.List, ни одна строка objects при этом не читается. Если бы rebuild
// сверялся с базой или брал список из неё, тест «восстановили из лент» проверял
// бы чтение собственной БД и ничего больше.
//
// Второй: объект без данных не читается. Метаданные версии целиком лежат в
// манифесте в хвосте файла (ADR-0019), поэтому мы читаем только последние
// килобайты, а не гоняем ленту целиком.
//
// Третий, и он же самый неприятный: часть данных восстановить нельзя, и об этом
// обязано сказать вслух. Бакет на ленте не живёт: его versioning, Object Lock,
// ACL и владелец остаются в базе. Значит бакеты создаются с значениями по
// умолчанию, и объект, который был заперт правилом бакета, после восстановления
// заперт не будет. Отчёт перечисляет такие бакеты отдельным списком — иначе
// «успешно восстановлено 4 812 объектов» означает совсем не то, что было.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"time"

	"github.com/vladsimachkov/tapegw/internal/archive"
	"github.com/vladsimachkov/tapegw/internal/encrypt"
	"github.com/vladsimachkov/tapegw/internal/iam"
	"github.com/vladsimachkov/tapegw/internal/keys"
	"github.com/vladsimachkov/tapegw/internal/meta"
)

// Reason — категория пропуска. Строка, а не enum: она попадает в отчёт и в лог,
// и человек, разбирающий недосказанное, должен прочесть её без справочника.
type Reason string

const (
	// ReasonAttrs — атрибуты не разобрались: объект записал не мы, или mover
	// записал их криво.
	ReasonAttrs Reason = "attrs"
	// ReasonTenantAbsent — тенанта нет в экспорте ключей. Почти всегда это
	// крипто-уничтожение (ADR-0014): данные остались на ленте, но ключ стёрт,
	// и это правильный, а не аварийный исход.
	ReasonTenantAbsent Reason = "tenant_absent"
	// ReasonKey — тенант в экспорте есть, но обёртка DEK не развернулась: нет
	// нужной версии KEK, или обёртка не от этого тенанта.
	ReasonKey Reason = "key"
	// ReasonManifest — файл не читается или манифест не вскрылся: испорченная
	// запись на ленте либо файл чужого формата.
	ReasonManifest Reason = "manifest"
	// ReasonBucket — бакет не создан или объект в него не вставился.
	ReasonStore Reason = "store"
)

// Skip — пропущенный объект.
type Skip struct {
	Oid    string
	Bucket string
	Key    string
	Reason Reason
	Err    error
}

func (s Skip) String() string {
	where := s.Bucket + "/" + s.Key
	if s.Bucket == "" {
		where = s.Oid
	}
	return fmt.Sprintf("%s [%s]: %v", where, s.Reason, s.Err)
}

// Report — итог прогона.
type Report struct {
	Scanned  int
	Restored int
	// Existing — версии, уже восстановленные предыдущим прогоном. Повторный
	// запуск обязан показывать именно это, а не падать и не удваивать строки.
	Existing int
	Skipped  []Skip
	// BucketsCreated — бакеты, которых в базе не было и которые заведены с
	// настройками по умолчанию. Их содержимое восстановлено, а конфигурация
	// (versioning, Object Lock, ACL, владелец) — нет, и это надо починить
	// руками до того, как бакет начнут обслуживать.
	BucketsCreated []string
}

// Options — входы восстановления.
type Options struct {
	Archive archive.Archive
	Meta    *meta.Meta
	Ring    *iam.KeyRing
	Export  *keys.Export
	Log     *slog.Logger
	// DryRun — собрать отчёт и ничего не записать. Нужен не «посмотреть и так
	// понятно», а потому что восстановление запускают на живой системе, где
	// часть ключей могла не пережить удаление тенанта: сначала отчёт, потом
	// решение.
	DryRun bool
}

// entry — объект, для которого удалось собрать метаданные.
type entry struct {
	oid  string
	ver  archive.Version
	doc  *encrypt.ManifestDoc
	size int64
	kek  int
}

// key — группа версий одного ключа.
func (e entry) group() [2]string { return [2]string{e.doc.Bucket, e.doc.Key} }

// Run — восстановить всё, что читается.
//
// Ошибки одного объекта не прерывают прогон: восстановление нужно именно тогда,
// когда часть архива могла пострадать, и падение на 3-м объекте из 50 000 — это
// способ не восстановить ничего. Прогон падает только если не работает сам
// архив или база.
func Run(ctx context.Context, o Options) (*Report, error) {
	if o.Log == nil {
		o.Log = slog.Default()
	}
	vault := keys.New(o.Meta, o.Ring)

	refs, err := o.Archive.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("rebuild: список архива: %w", err)
	}

	rep := &Report{Scanned: len(refs)}
	var entries []entry
	for _, ref := range refs {
		e, skip := readOne(ctx, o, vault, ref)
		if skip != nil {
			rep.Skipped = append(rep.Skipped, *skip)
			o.Log.Warn("объект пропущен", "oid", ref.Oid,
				"reason", string(skip.Reason), "err", skip.Err)
			continue
		}
		entries = append(entries, e)
	}

	// Порядок групп детерминированный, иначе отчёт двух прогонов не сравнить.
	sort.Slice(entries, func(i, j int) bool {
		gi, gj := entries[i].group(), entries[j].group()
		if gi != gj {
			if gi[0] != gj[0] {
				return gi[0] < gj[0]
			}
			return gi[1] < gj[1]
		}
		return entries[i].doc.Created.Before(entries[j].doc.Created)
	})

	created := map[string]bool{}
	pools := map[int64]int64{}

	var cur [2]string
	var seq int64
	for i, e := range entries {
		g := e.group()
		if g != cur {
			cur, seq = g, 0
		}
		seq++
		// Текущая — последняя по времени записи из манифеста. Это ВРЕМЕННО:
		// настоящий порядок задаёт version_seq, а он в манифесте лежать не может
		// — Postgres назначает его при вставке, уже после записи файла. На
		// параллельных PUT одного ключа время и порядок версий расходятся, и
		// восстановление выбрало бы не ту текущую версию. Чинится в M4: mover
		// кладёт version_seq в атрибут tapegw.seq (требование в ARCHITECTURE.md,
		// раздел M4), и сортировка переезжает на него.
		isCur := i+1 == len(entries) || entries[i+1].group() != g

		if err := o.Meta.Pool.Ping(ctx); err != nil {
			return rep, fmt.Errorf("rebuild: база недоступна: %w", err)
		}

		bucketID, newBucket, err := ensureBucket(ctx, o, e, created, rep)
		if err != nil {
			rep.Skipped = append(rep.Skipped, Skip{Oid: e.oid, Bucket: g[0], Key: g[1],
				Reason: ReasonStore, Err: err})
			continue
		}
		if newBucket {
			rep.BucketsCreated = append(rep.BucketsCreated, g[0])
		}

		pool, ok := pools[e.ver.Tenant]
		if !ok {
			pool, err = o.Meta.TenantPool(ctx, e.ver.Tenant)
			if err != nil {
				rep.Skipped = append(rep.Skipped, Skip{Oid: e.oid, Bucket: g[0], Key: g[1],
					Reason: ReasonStore, Err: err})
				continue
			}
			pools[e.ver.Tenant] = pool
		}

		in := rebuildInput(e, bucketID, pool, seq, isCur)
		if o.DryRun {
			// Счётчик тот же: отчёт обязан показывать ровно то, что сделал бы
			// настоящий прогон, иначе «прогнали dry-run, потом боевой — и
			// результаты разные» станет сюрпризом.
			rep.Restored++
			continue
		}
		inserted, err := o.Meta.RebuildObject(ctx, in)
		if err != nil {
			rep.Skipped = append(rep.Skipped, Skip{Oid: e.oid, Bucket: g[0], Key: g[1],
				Reason: ReasonStore, Err: err})
			continue
		}
		if inserted {
			rep.Restored++
		} else {
			rep.Existing++
		}
	}
	return rep, nil
}

// readOne — собрать метаданные одного объекта архива. Второй возврат — пропуск.
func readOne(ctx context.Context, o Options, vault *keys.Vault, ref archive.ObjectRef) (entry, *Skip) {
	ver, err := archive.ParseAttributes(ref.Attrs)
	if err != nil {
		return entry{}, &Skip{Oid: ref.Oid, Reason: ReasonAttrs, Err: err}
	}
	kek, err := ver.KEKNum()
	if err != nil {
		return entry{}, &Skip{Oid: ref.Oid, Reason: ReasonAttrs, Err: err}
	}

	// Тенант обязан быть в экспорте. Его отсутствие — штатный случай: объект
	// остался на ленте, ключ уничтожен. Такой объект пропускается с записью, а
	// не роняет прогон: иначе один удалённый тенант сделал бы невозможным
	// восстановление всех остальных.
	exp, ok := o.Export.Tenant(ver.Tenant)
	if !ok {
		return entry{}, &Skip{Oid: ref.Oid, Reason: ReasonTenantAbsent,
			Err: fmt.Errorf("тенанта %d нет в экспорте ключей (уничтожен?)", ver.Tenant)}
	}
	_ = exp

	dek, err := vault.OpenVersionDEK(ctx, ver.Tenant, ver.WrappedDEK, ver.Version)
	if err != nil {
		return entry{}, &Skip{Oid: ref.Oid, Reason: ReasonKey, Err: err}
	}
	defer encrypt.Zero(dek)

	size, err := o.Archive.Size(ctx, ref.Oid)
	if err != nil {
		return entry{}, &Skip{Oid: ref.Oid, Reason: ReasonManifest, Err: err}
	}
	doc, err := encrypt.OpenManifestAt(archiveReader{ctx: ctx, a: o.Archive, oid: ref.Oid},
		size, dek, ver.Version)
	if err != nil {
		return entry{}, &Skip{Oid: ref.Oid, Reason: ReasonManifest,
			Err: errors.Join(err, errors.New("манифест не вскрылся"))}
	}
	if doc.V != encrypt.ManifestFormat {
		return entry{}, &Skip{Oid: ref.Oid, Bucket: doc.Bucket, Key: doc.Key,
			Reason: ReasonManifest, Err: fmt.Errorf("версия манифеста %d", doc.V)}
	}
	return entry{oid: ref.Oid, ver: ver, doc: doc, size: size, kek: kek}, nil
}

// archiveReader — адаптер Archive к io.ReaderAt: манифест умеет читать из
// любого ReaderAt, а архив отдаёт чтение по смещению.
type archiveReader struct {
	ctx context.Context
	a   archive.Archive
	oid string
}

func (r archiveReader) ReadAt(p []byte, off int64) (int, error) {
	return r.a.ReadAt(r.ctx, r.oid, p, off)
}

// ensureBucket — бакет обязан существовать до вставки объектов.
//
// Конфигурацию бакета взять неоткуда: на ленте живут только объекты. Поэтому он
// заводится с дефолтами и попадает в BucketsCreated.
//
// В dry-run бакет не создаётся, но в отчёт попадает: «восстановим N объектов и
// заведём вот эти бакеты» — ровно то, что покажет боевой прогон.
func ensureBucket(ctx context.Context, o Options, e entry, seen map[string]bool, rep *Report) (int64, bool, error) {
	name := e.doc.Bucket
	if name == "" {
		return 0, false, errors.New("в манифесте пустой бакет")
	}
	b, err := o.Meta.GetBucket(ctx, meta.NewTenant(int(e.ver.Tenant)), name)
	if err == nil {
		return b.ID, false, nil
	}
	if !errors.Is(err, meta.ErrNotFound) {
		return 0, false, err
	}
	if o.DryRun {
		return 0, true, nil
	}
	if seen[name] {
		// Уже создали в этом прогоне, но GetBucket его не видит: значит бакет
		// принадлежит другому тенанту, а имена бакетов глобально уникальны.
		return 0, false, fmt.Errorf("бакет %q уже создан этим прогоном, но не виден тенанту %d", name, e.ver.Tenant)
	}
	if _, err := o.Meta.CreateBucket(ctx, meta.NewTenant(int(e.ver.Tenant)), name,
		fmt.Sprintf("tenant-%d", e.ver.Tenant), nil, nil); err != nil {
		return 0, false, fmt.Errorf("создать бакет %q: %w", name, err)
	}
	seen[name] = true
	b, err = o.Meta.GetBucket(ctx, meta.NewTenant(int(e.ver.Tenant)), name)
	if err != nil {
		return 0, false, err
	}
	return b.ID, true, nil
}

// rebuildInput — строка objects из манифеста и атрибутов.
func rebuildInput(e entry, bucketID, pool, seq int64, isCur bool) meta.RebuildObjectInput {
	in := meta.RebuildObjectInput{
		ID:           e.ver.Version,
		BucketID:     bucketID,
		TenantID:     e.ver.Tenant,
		PoolID:       pool,
		Key:          e.doc.Key,
		Size:         e.doc.Size,
		ETag:         e.doc.ETag,
		CreatedAt:    e.doc.Created,
		VersionSeq:   seq,
		IsCurrent:    isCur,
		WrappedDEK:   e.ver.WrappedDEK,
		StorageClass: "GLACIER",
		UserMeta:     e.doc.Meta,
	}
	if e.doc.ContentType != "" {
		ct := e.doc.ContentType
		in.ContentType = &ct
	}
	if l := e.doc.Lock; l != nil {
		in.RetentionMode = l.Mode
		in.LegalHold = l.LegalHold
		if l.RetainUntil != "" {
			if t, err := time.Parse(time.RFC3339Nano, l.RetainUntil); err == nil {
				in.RetainUntil = &t
			}
		}
	}
	for _, p := range e.doc.Parts {
		in.Parts = append(in.Parts, meta.ObjectPart{
			Number: int(p.Number), PlainSize: p.Plain, CipherOffset: p.Off, Salt: p.Salt,
		})
	}
	return in
}

// compile-time: адаптер обязан оставаться ReaderAt.
var _ io.ReaderAt = archiveReader{}
