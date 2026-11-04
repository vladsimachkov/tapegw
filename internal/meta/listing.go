package meta

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Листинг объектов и версий.
//
// Семантика взята из versitygw (правило 5), из backend/walk.go — там живут Walk
// и WalkVersions, на которые опирается posix-бэкенд. Три вещи, которые там
// зафиксированы и которые легко передать своими словами:
//
//   - в max-keys считаются ВМЕСТЕ и объекты, и common prefixes;
//   - common prefix строится из части ключа ПОСЛЕ prefix: delimiter ищется в
//     этой части, а не в ключе целиком;
//   - IsTruncated = true только если за последним выданным элементом реально
//     есть следующий. «Страница заполнена» — ещё не «есть продолжение».
//
// Маркер сравнивается строго: ключ должен быть БОЛЬШЕ маркера, а не не меньше,
// иначе последний элемент страницы повторится на следующей.

// ListEntry — одна строка листинга: объект для ListObjects либо версия или
// маркер удаления для ListObjectVersions.
type ListEntry struct {
	Key          string
	Size         int64
	ETag         string
	LastModified time.Time
	// StorageClass всегда GLACIER (ADR-0003), но отдаём его из строки, а не
	// константой: колонка это позволяет, а константа разошлась бы с данными.
	StorageClass string

	// VersionSeq — порядок версий внутри ключа (миграция 0009). Нужен только
	// как маркер продолжения; в листинге текущих версий не заполняется.
	VersionSeq int64
	// VersionID — "null" для версии без идентификатора (бакет без версий или
	// с приостановленными), иначе UUID строка. Пусто для ListObjects.
	VersionID string
	// IsDeleteMarker — версия-маркер удаления: в ListObjectVersions отдаётся
	// отдельным списком, в ListObjects ключ с ней не виден.
	IsDeleteMarker bool
}

// ListResult — страница листинга.
type ListResult struct {
	Entries        []ListEntry
	CommonPrefixes []string
	Truncated      bool
	// NextKey — маркер следующей страницы: ключ последнего выданного объекта
	// либо последнего common prefix.
	NextKey string
	// NextVersionID — версия последнего выданного объекта. Пусто, если последним
	// был common prefix: у префикса версии нет.
	NextVersionID string
}

// maxRunes — сколько максимальных символов U+10FFFF подвешивать к prefix, чтобы
// получить верхнюю границу диапазона ключей, начинающихся с prefix.
//
// Граница нужна, чтобы `key >= prefix AND key < upper` использовал индекс.
// Инкрементировать последний байт prefix нельзя: prefix — валидный UTF-8,
// инкремент последнего байта многобайтовой последовательности даёт невалидный
// UTF-8, а его Postgres в параметре отвергает. U+10FFFF в UTF-8 — максимальная
// байтовая последовательность (F4 8F BF BF), и четырёх штук хватает с запасом:
// любой символ после prefix меньше либо равен этому.
const maxRunes = "\U0010FFFF\U0010FFFF\U0010FFFF\U0010FFFF"

// prefixBounds — нижняя и верхняя границы ключей, начинающихся с prefix.
// Пустой prefix даёт nil: верхняя граница не нужна.
func prefixBounds(prefix string) (lo, hi any) {
	if prefix == "" {
		return "", nil
	}
	return prefix, prefix + maxRunes
}

// folder сворачивает ключи в common prefixes.
type folder struct {
	prefix string
	delim  string
	// lastCP — последний выданный префикс. Ключи идут по порядку, поэтому
	// одинаковые префиксы стоят подряд и достаточно помнить один: сравнение с
	// множеством всех виденных здесь было бы лишней памятью на большой выдаче.
	lastCP string
}

// commonPrefixOf возвращает common prefix для key либо ok = false, если ключ
// надо выдавать как есть.
func (f *folder) commonPrefixOf(key string) (string, bool) {
	if f.delim == "" {
		return "", false
	}
	rest := strings.TrimPrefix(key, f.prefix)
	i := strings.Index(rest, f.delim)
	if i < 0 {
		return "", false
	}
	return f.prefix + rest[:i] + f.delim, true
}

// listBatch — сколько строк читать за один заход. Страница может схлопнуться в
// один common prefix (тысячи ключей под «a/»), поэтому один LIMIT = max-keys
// строк не покрывает: читаем пачками и идём, пока не наберём max записей либо
// строки не кончатся.
const listBatch = 256

// listPage — общий цикл обоих листингов. fetch читает строки, строго идущие
// после (afterKey, afterSeq), где afterSeq = math.MaxInt64 означает «с начала
// ключа».
func (m *Meta) listPage(max int, markerKey string, startSeq int64, fold *folder,
	fetch func(afterKey string, afterSeq int64, limit int) ([]ListEntry, error)) (ListResult, error) {

	res := ListResult{}
	if max <= 0 {
		// S3: max-keys = 0 — пустая выдача без продолжения. Пустая — не
		// «всё до конца»: клиент по такому ответу не должен просить ещё.
		return res, nil
	}

	// startSeq = math.MaxInt64 означает «с самого начала»: для листинга текущих
	// версий порядка внутри ключа нет, и он передаётся именно таким.
	afterKey, afterSeq := markerKey, startSeq
	for {
		rows, err := fetch(afterKey, afterSeq, listBatch)
		if err != nil {
			return res, err
		}
		if len(rows) == 0 {
			return res, nil
		}

		for _, r := range rows {
			if cp, ok := fold.commonPrefixOf(r.Key); ok {
				if cp == fold.lastCP {
					continue
				}
				if len(res.Entries)+len(res.CommonPrefixes) >= max {
					// Есть что показать после последнего выданного — значит
					// страница усечена, а маркер уже указывает на него.
					res.Truncated = true
					return res, nil
				}
				fold.lastCP = cp
				res.CommonPrefixes = append(res.CommonPrefixes, cp)
				res.NextKey = cp
				res.NextVersionID = ""
				continue
			}

			if len(res.Entries)+len(res.CommonPrefixes) >= max {
				res.Truncated = true
				return res, nil
			}
			res.Entries = append(res.Entries, r)
			res.NextKey = r.Key
			res.NextVersionID = r.VersionID
		}

		last := rows[len(rows)-1]
		afterKey, afterSeq = last.Key, last.VersionSeq
	}
}

// ListObjectsCurrent — листинг текущих версий (ListObjects v1 и V2).
//
// Ключ, у которого текущая версия — маркер удаления, в выдачу НЕ попадает:
// для S3 такой ключ «удалён», хотя версия под маркером жива и видна только в
// ListObjectVersions.
func (m *Meta) ListObjectsCurrent(ctx context.Context, tenant TenantID, bucket,
	prefix, delimiter, marker string, max int) (ListResult, error) {

	fold := &folder{prefix: prefix, delim: delimiter}
	lo, hi := prefixBounds(prefix)

	return m.listPage(max, marker, int64(math.MaxInt64), fold,
		func(afterKey string, afterSeq int64, limit int) ([]ListEntry, error) {
			base := `SELECT o.key, o.size, o.etag, o.created_at, o.storage_class
			          FROM objects o
			          JOIN buckets b ON b.id = o.bucket_id
			         WHERE b.name = $1 AND b.deleted_at IS NULL
			           AND o.is_current AND NOT o.is_delete_marker
			           AND o.key > $2`
			args := []any{bucket, afterKey}
			if hi != nil {
				args = append(args, lo, hi)
				base += ` AND o.key >= $3 AND o.key < $4`
			}
			// withTenantCol дописывает условие в КОНЕЦ строки, поэтому страж
			// добавляется до ORDER BY/LIMIT: иначе вышло бы
			// `LIMIT $3 AND b.tenant_id = $4`.
			q, qargs := withTenantCol(base, "b.tenant_id", tenant, args...)
			q += fmt.Sprintf(` ORDER BY o.key COLLATE "C" LIMIT $%d`, len(qargs)+1)
			qargs = append(qargs, limit)
			rows, err := m.Pool.Query(ctx, q, qargs...)
			if err != nil {
				return nil, fmt.Errorf("list objects %s: %w", bucket, err)
			}
			defer rows.Close()

			out := make([]ListEntry, 0, limit)
			for rows.Next() {
				var e ListEntry
				if err := rows.Scan(&e.Key, &e.Size, &e.ETag, &e.LastModified,
					&e.StorageClass); err != nil {
					return nil, fmt.Errorf("scan list %s: %w", bucket, err)
				}
				out = append(out, e)
			}
			return out, rows.Err()
		})
}

// ListObjectVersions — листинг версий (ListObjectVersions).
//
// Порядок: ключ по байтовому порядку, внутри ключа версии от новой к старой —
// то есть по version_seq DESC. Маркер (keyMarker, versionIDMarker) означает
// «продолжить ПОСЛЕ этой конкретной версии», и если он попал в середину версий
// ключа, с этой середины и продолжаем: иначе страница либо теряет версии, либо
// дублирует уже выданные.
//
// Версии и маркеры удаления идут одним потоком и считаются в max-keys вместе с
// common prefixes — разложить их по отдельным спискам обязан вызывающий, а
// порядок от этого не меняется.
func (m *Meta) ListObjectVersions(ctx context.Context, tenant TenantID, bucket,
	prefix, delimiter, keyMarker, versionIDMarker string, max int) (ListResult, error) {

	fold := &folder{prefix: prefix, delim: delimiter}
	lo, hi := prefixBounds(prefix)

	// versionIdMarker -> version_seq. Маркер приходит из предыдущего ответа,
	// где мы сами отдали id версии (UUID либо "null").
	startSeq, err := m.versionSeqMarker(ctx, tenant, bucket, keyMarker, versionIDMarker)
	if err != nil {
		return ListResult{}, err
	}

	return m.listPage(max, keyMarker, startSeq, fold,
		func(afterKey string, afterSeq int64, limit int) ([]ListEntry, error) {
			base := `SELECT o.key, o.size, o.etag, o.created_at, o.storage_class,
			                o.version_seq, o.id::text, o.is_null_version,
			                o.is_delete_marker
			          FROM objects o
			          JOIN buckets b ON b.id = o.bucket_id
			         WHERE b.name = $1 AND b.deleted_at IS NULL
			           AND (o.key > $2 OR (o.key = $2 AND o.version_seq < $3))`
			args := []any{bucket, afterKey, afterSeq}
			if hi != nil {
				args = append(args, lo, hi)
				base += ` AND o.key >= $4 AND o.key < $5`
			}
			q, qargs := withTenantCol(base, "b.tenant_id", tenant, args...)
			q += fmt.Sprintf(` ORDER BY o.key COLLATE "C", o.version_seq DESC LIMIT $%d`,
				len(qargs)+1)
			qargs = append(qargs, limit)
			rows, err := m.Pool.Query(ctx, q, qargs...)
			if err != nil {
				return nil, fmt.Errorf("list versions %s: %w", bucket, err)
			}
			defer rows.Close()

			out := make([]ListEntry, 0, limit)
			for rows.Next() {
				var e ListEntry
				var id string
				var nullVersion, delMarker bool
				if err := rows.Scan(&e.Key, &e.Size, &e.ETag, &e.LastModified,
					&e.StorageClass, &e.VersionSeq, &id, &nullVersion, &delMarker); err != nil {
					return nil, fmt.Errorf("scan versions %s: %w", bucket, err)
				}
				e.IsDeleteMarker = delMarker
				// null-версия везде отдаётся как "null", а не как UUID строки:
				// клиент, который увидел versionId="null" и передал его обратно,
				// должен получить ту же версию.
				if nullVersion {
					e.VersionID = "null"
				} else {
					e.VersionID = id
				}
				out = append(out, e)
			}
			return out, rows.Err()
		})
}

// versionSeqMarker переводит пару (keyMarker, versionIdMarker) в границу
// version_seq. math.MaxInt64 означает «начать с самого начала ключа».
//
// Если версия не найдена — маркер устарел (версию удалили или это чужие
// данные). Молча начать с начала ключа нельзя: клиент получил бы дубли уже
// виденных версий. Но и ошибка здесь хуже того же: S3 на устаревший маркер не
// обещает ничего внятного, а клиенту нужен проход до конца, поэтому начинаем с
// начала ключа и это поведение описываем.
func (m *Meta) versionSeqMarker(ctx context.Context, tenant TenantID,
	bucket, keyMarker, versionIDMarker string) (int64, error) {

	if keyMarker == "" || versionIDMarker == "" {
		return int64(math.MaxInt64), nil
	}

	var cond string
	args := []any{bucket, keyMarker}
	if versionIDMarker == "null" {
		cond = `o.is_null_version`
	} else if _, err := uuid.Parse(versionIDMarker); err != nil {
		return int64(math.MaxInt64), nil
	} else {
		args = append(args, versionIDMarker)
		cond = fmt.Sprintf(`o.id = $%d::uuid`, len(args))
	}

	base := fmt.Sprintf(`SELECT o.version_seq
	                         FROM objects o
	                         JOIN buckets b ON b.id = o.bucket_id
	                        WHERE b.name = $1 AND b.deleted_at IS NULL
	                          AND o.key = $2 AND %s`, cond)
	q, qargs := withTenantCol(base, "b.tenant_id", tenant, args...)

	var seq int64
	if err := m.Pool.QueryRow(ctx, q, qargs...).Scan(&seq); err != nil {
		return int64(math.MaxInt64), nil
	}
	return seq, nil
}
