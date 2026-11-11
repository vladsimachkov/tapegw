package meta

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Запечатывание сегментов журнала (миграция 0029, ADR-0023 п. 3–5).
//
// Событие после 0028 носит только служебный id, а плотный номер, по которому
// rebuild-meta применяет журнал по порядку и видит пропуск, даёт диапазон
// запечатанного сегмента. Здесь учётная часть: выбрать не запечатанные события
// тенанта, выдать им следующий кусок плотной нумерации и оставить признак
// «запечатано» в journal_segment_event. Сам объект сегмента (шифротекст под TEK
// тенанта, очередь mover'у, пачки журнала) — следующий шаг.
//
// Как выборка устроена и почему именно так.
//
// Обычный ход берёт события **дальше рубежа** тенанта (максимальный id
// предыдущего сегмента). Это не «id ≤ watermark» из ADR-0023 п. 3, и разница
// принципиальная: там дыра возникала из того, что рубеж считался заранее, и
// событие с малым id, чья транзакция ещё шла, оставалось за сегментом навсегда.
// Здесь рубеж — это то, что уже запечатано, а всё видимое дальше него берётся
// целиком, поэтому «тихо не взято» не получается: пропущенное в окне выборки
// событие ловится проверкой sealSkippedSQL и красным, а не молчанием.
//
// Видимость отдельно не проверяется: строку, которую видел скан, нельзя
// не заметить — не коммитившая транзакция просто не отдаёт строку. Отсюда и
// «опоздавшие»: событие, чья транзакция была в полёте во время запечатывания,
// становится видно позже, и id у него **ниже** рубежа — обычный ход его не
// найдёт. Для порядка событий это безразлично (см. комментарий
// SealJournalStragglers), но событие обязано когда-нибудь запечататься, иначе
// проверка полноты при восстановлении будет падать вечно, — для этого и есть
// отдельная редкая проверка ниже рубежа.
//
// Порядок событий одного объекта при этом не страдает: каждое журналируемое
// изменение берёт блокировку строки объекта (или бакета) до вставки события,
// поэтому второй участник получит id позже и закоммитится позже — id и порядок
// коммитов для одного объекта совпадают (ADR-0023 п. 2). Разные объекты при
// восстановлении переставляться могут.

// SealedSegment — один запечатанный сегмент.
type SealedSegment struct {
	ID         int64
	UUID       uuid.UUID
	TenantID   int64
	SeqFirst   int64
	SeqLast    int64
	EventCount int
}

// SealLimits — условия закрытия сегмента (config.JournalSegment).
type SealLimits struct {
	// MaxEvents — максимум событий в сегменте.
	MaxEvents int
	// MaxBytes — максимум суммы octet_length(payload) в сегменте. Сегмент
	// режется по событиям целиком: событие всегда остаётся целым, иначе
	// воркер крутился бы на пустой выборке.
	MaxBytes int64
}

// события тенанта дальше рубежа (afterID) с их весом; только не запечатанные.
const sealNewSQL = `
	WITH cand AS (
		SELECT e.id, octet_length(e.payload::text)::bigint AS bytes
		  FROM journal_events e
		 WHERE e.tenant_id = $1
		   AND e.id > $2
		   AND NOT EXISTS (SELECT 1 FROM journal_segment_event m WHERE m.event_id = e.id)
		 ORDER BY e.id
		 LIMIT $3
	)
	SELECT id, bytes FROM cand ORDER BY id`

// те же события, но ниже рубежа — «опоздавшие» (см. шапку файла).
const sealStragglersSQL = `
	SELECT e.id, octet_length(e.payload::text)::bigint AS bytes
	  FROM journal_events e
	 WHERE e.tenant_id = $1
	   AND e.id <= $2
	   AND NOT EXISTS (SELECT 1 FROM journal_segment_event m WHERE m.event_id = e.id)
	 ORDER BY e.id
	 LIMIT $3`

// Сколько незапечатанных событий у тенанта в окне (lowID, highID]. Выборка
// обязана была взять их все; если нет — значит условие отбора потеряло строку,
// и событие уехало бы за сегмент навсегда.
const sealSkippedSQL = `
	SELECT count(*) FROM journal_events e
	 WHERE e.tenant_id = $1
	   AND e.id > $2 AND e.id <= $3
	   AND NOT EXISTS (SELECT 1 FROM journal_segment_event m WHERE m.event_id = e.id)`

type sealCandidate struct {
	id    int64
	bytes int64
}

// JournalSealWatermark — рубеж тенанта: максимальный id события, уже вошедшего в
// сегмент. 0 — сегментов ещё нет, и весь журнал впереди.
func (m *Meta) JournalSealWatermark(ctx context.Context, tenant int64) (int64, error) {
	var wm int64
	if err := m.Pool.QueryRow(ctx,
		`SELECT COALESCE(max(event_id_last), 0) FROM journal_segments WHERE tenant_id = $1`,
		tenant).Scan(&wm); err != nil {
		return 0, fmt.Errorf("meta: рубеж запечатывания тенанта %d: %w", tenant, err)
	}
	return wm, nil
}

// JournalTenantsToSeal — тенанты, у которых есть события дальше рубежа.
//
// Движок — список тенантов, а не таблица событий: для каждого тенанта проверка
// «есть ли что дальше рубежа» — это один индексный шаг по (tenant_id, id), а не
// скан журнала. Иначе выборка кандидатов стоила бы столько же, сколько весь
// журнал, на каждом тике.
func (m *Meta) JournalTenantsToSeal(ctx context.Context, limit int) ([]int64, error) {
	if limit < 1 {
		return nil, fmt.Errorf("meta: батч тенантов %d, должно быть >= 1", limit)
	}
	rows, err := m.Pool.Query(ctx, `
		SELECT t.id FROM tenants t
		 WHERE EXISTS (
		   SELECT 1 FROM journal_events e
		    WHERE e.tenant_id = t.id
		      AND e.id > COALESCE((SELECT max(s.event_id_last) FROM journal_segments s
		                             WHERE s.tenant_id = t.id), 0))
		 ORDER BY t.id
		 LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("meta: тенанты на запечатывание: %w", err)
	}
	defer rows.Close()

	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("meta: тенанты на запечатывание: %w", err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// JournalTenantsWithStragglers — тенанты, у которых есть незапечатанные события
// **ниже** рубежа (см. SealJournalStragglers).
//
// Дороже, чем JournalTenantsToSeal: чтобы увидеть отсутствие, надо просмотреть
// историю тенанта, — поэтому список и берётся редко (sweep_interval), а не на
// каждом тике.
func (m *Meta) JournalTenantsWithStragglers(ctx context.Context, limit int) ([]int64, error) {
	if limit < 1 {
		return nil, fmt.Errorf("meta: батч тенантов %d, должно быть >= 1", limit)
	}
	rows, err := m.Pool.Query(ctx, `
		SELECT t.id FROM tenants t
		 WHERE EXISTS (
		   SELECT 1 FROM journal_events e
		    WHERE e.tenant_id = t.id
		      AND e.id <= COALESCE((SELECT max(s.event_id_last) FROM journal_segments s
		                             WHERE s.tenant_id = t.id), 0)
		      AND NOT EXISTS (SELECT 1 FROM journal_segment_event m WHERE m.event_id = e.id))
		 ORDER BY t.id
		 LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("meta: тенанты с опоздавшими: %w", err)
	}
	defer rows.Close()

	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("meta: тенанты с опоздавшими: %w", err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// SealJournalTenant — запечатывает новые события одного тенанта одним сегментом.
// nil, nil означает «запечатывать нечего» — это норма, а не ошибка.
//
// Один вызов = одна транзакция = один сегмент: нумерация, сегмент и признак
// обязаны появиться вместе (иначе диапазон есть, а признака нет — событие
// запечатано дважды), а держать транзакцию открытой дольше нескольких запросов
// незачем. Большой backlog воркер разбирает несколькими вызовами.
func (m *Meta) SealJournalTenant(ctx context.Context, tenant int64, lim SealLimits) (*SealedSegment, error) {
	if lim.MaxEvents < 1 || lim.MaxBytes < 1 {
		return nil, fmt.Errorf("meta: лимиты сегмента %d/%d, должны быть >= 1", lim.MaxEvents, lim.MaxBytes)
	}
	// Изоляция — умолчальная READ COMMITTED, и это не недосмотр: рубеж читается
	// после advisory-лока, и второму силеру обязан быть виден сегмент первого.
	// При REPEATABLE READ снимок берётся на первом запросе, ещё до ожидания лока,
	// — лок перестал бы защищать, и второй силер вторично запечатал бы те же
	// события. Цена умолчания — окно между выборкой и проверкой полноты (см.
	// sealEvents), оно самозакрывающееся: красный сегмент отменяется целиком и
	// следующий проход берёт окно заново.
	tx, err := m.Pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("meta: начать запечатывание: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // после commit откатывать нечего

	if err := sealTenantLock(ctx, tx, tenant); err != nil {
		return nil, err
	}
	wm, err := sealWatermark(ctx, tx, tenant)
	if err != nil {
		return nil, err
	}
	cands, err := sealQuery(ctx, tx, sealNewSQL, tenant, wm, lim.MaxEvents)
	if err != nil {
		return nil, err
	}
	taken := cutByBytes(cands, lim.MaxBytes)
	if len(taken) == 0 {
		return nil, nil
	}
	seg, err := sealEvents(ctx, tx, tenant, wm, taken)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("meta: коммит сегмента журнала: %w", err)
	}
	return seg, nil
}

// SealJournalStragglers — запечатывает события **ниже** рубежа тенанта: те,
// чьи транзакции были в полёте, когда их диапазон уже ушёл в сегмент.
//
// Порядок при этом не страдает, и это не «авось»: каждое журналируемое изменение
// держит блокировку строки объекта (или бакета) до вставки события, поэтому
// опоздавшее событие не может относиться к тому же объекту, что и уже
// запечатанное с большим id, — иначе второй участник ждал бы эту блокировку и
// получил бы id позже. Значит переставляются только разные объекты, а их
// порядок при восстановлении не важен (ADR-0023 п. 2 и п. 6).
//
// Дороже обычного хода: окно — вся история тенанта, поэтому вызывается редко
// (journal.sealer.sweep_interval), а не на каждом тике.
func (m *Meta) SealJournalStragglers(ctx context.Context, tenant int64, batch int) (*SealedSegment, error) {
	if batch < 1 {
		return nil, fmt.Errorf("meta: батч опоздавших %d, должно быть >= 1", batch)
	}
	tx, err := m.Pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("meta: начать проверку опоздавших: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // после commit откатывать нечего

	if err := sealTenantLock(ctx, tx, tenant); err != nil {
		return nil, err
	}
	wm, err := sealWatermark(ctx, tx, tenant)
	if err != nil {
		return nil, err
	}
	if wm == 0 {
		// Сегментов нет — нечего и искать: ниже нуля id не бывает.
		return nil, nil
	}
	cands, err := sealQuery(ctx, tx, sealStragglersSQL, tenant, wm, batch)
	if err != nil {
		return nil, err
	}
	if len(cands) == 0 {
		return nil, nil
	}
	// Окно проверки — вся история до рубежа: здесь это та же цена, что у самой
	// выборки, и она сознательно принимается за редкий проход.
	seg, err := sealEvents(ctx, tx, tenant, 0, cands)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("meta: коммит сегмента опоздавших: %w", err)
	}
	return seg, nil
}

// sealTenantLock — сериализация запечатывания одного тенанта. Без неё два
// воркера выбрали бы один и тот же набор событий и второй упёрся бы в
// UNIQUE (event_id): данные не испортились бы, но красный прогон на ровном
// месте — плохая защита. Ключ advisory-лока отдельный от ключей объектов:
// запечатывание не должно мешать записи.
func sealTenantLock(ctx context.Context, tx pgx.Tx, tenant int64) error {
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
		fmt.Sprintf("journal-seal:%d", tenant)); err != nil {
		return fmt.Errorf("meta: лок запечатывания тенанта %d: %w", tenant, err)
	}
	return nil
}

func sealWatermark(ctx context.Context, tx pgx.Tx, tenant int64) (int64, error) {
	var wm int64
	if err := tx.QueryRow(ctx,
		`SELECT COALESCE(max(event_id_last), 0) FROM journal_segments WHERE tenant_id = $1`,
		tenant).Scan(&wm); err != nil {
		return 0, fmt.Errorf("meta: рубеж запечатывания тенанта %d: %w", tenant, err)
	}
	return wm, nil
}

func sealQuery(ctx context.Context, tx pgx.Tx, sql string, tenant, bound int64, limit int) ([]sealCandidate, error) {
	rows, err := tx.Query(ctx, sql, tenant, bound, limit)
	if err != nil {
		return nil, fmt.Errorf("meta: выборка событий на запечатывание: %w", err)
	}
	defer rows.Close()

	var out []sealCandidate
	for rows.Next() {
		var c sealCandidate
		if err := rows.Scan(&c.id, &c.bytes); err != nil {
			return nil, fmt.Errorf("meta: выборка событий на запечатывание: %w", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// cutByBytes — префикс списка не тяжелее maxBytes, но всегда не меньше одного
// события: событие целиком больше порога всё равно уезжает, иначе сегмент
// никогда не закрылся бы и воркер крутился бы на пустой выборке.
func cutByBytes(cands []sealCandidate, maxBytes int64) []sealCandidate {
	var sum int64
	for i, c := range cands {
		if i > 0 && sum+c.bytes > maxBytes {
			return cands[:i]
		}
		sum += c.bytes
	}
	return cands
}

// sealEvents — выдаёт плотные номера, заводит сегмент и ставит признаки.
// lowerBound — нижняя граница окна, из которого брали id (рубеж обычного хода
// или ноль для опоздавших); проверка «ничего не пропущено» берёт то же окно.
func sealEvents(ctx context.Context, tx pgx.Tx, tenant, lowerBound int64, taken []sealCandidate) (*SealedSegment, error) {
	ids := make([]int64, len(taken))
	for i, c := range taken {
		ids[i] = c.id
	}
	maxID := ids[len(ids)-1]

	// Разрыв в нумерации событий внутри окна означал бы, что отбор потерял
	// строку, — а потерянное событие не увидит уже никто.
	//
	// Проверка идёт отдельным запросом, а не в той же выборке, и между ними
	// возможно безобидное совпадение: событие получило id до выборки и
	// закоммитилось уже после неё. Тогда оно тоже посчитается, и сегмент
	// отменится. Это не потеря и не накопление ошибки: следующий проход увидит
	// его как обычное событие дальше рубежа. Молчать здесь нельзя, потому что
	// второй источник этого же сигнала — сломанный отбор, а он теряет событие
	// по-настоящему.
	var skipped int64
	if err := tx.QueryRow(ctx, sealSkippedSQL, tenant, lowerBound, maxID).Scan(&skipped); err != nil {
		return nil, fmt.Errorf("meta: проверка полноты выборки тенанта %d: %w", tenant, err)
	}
	if skipped != int64(len(ids)) {
		return nil, fmt.Errorf("meta: тенант %d: в окне (%d, %d] незапечатанных событий %d, взято %d — "+
			"отбор потерял событие либо оно закоммитилось после выборки; сегмент отменён, "+
			"следующий проход возьмёт окно заново",
			tenant, lowerBound, maxID, skipped, len(ids))
	}

	// Плотный номер выдаёт силер: одна строка tenants на сегмент, и только здесь.
	// Путь изменения её больше не трогает — ради этого вся затея и была.
	var seqLast int64
	if err := tx.QueryRow(ctx,
		`UPDATE tenants SET journal_seq = journal_seq + $2 WHERE id = $1 RETURNING journal_seq`,
		tenant, len(ids)).Scan(&seqLast); err != nil {
		return nil, fmt.Errorf("meta: выдать номера тенанту %d: %w", tenant, err)
	}
	seg := SealedSegment{
		TenantID: tenant, SeqFirst: seqLast - int64(len(ids)) + 1,
		SeqLast: seqLast, EventCount: len(ids),
	}

	if err := tx.QueryRow(ctx,
		`INSERT INTO journal_segments (tenant_id, seq_first, seq_last, event_count, event_id_last)
		 VALUES ($1, $2, $3, $4, $5) RETURNING id, uuid`,
		tenant, seg.SeqFirst, seg.SeqLast, seg.EventCount, maxID).
		Scan(&seg.ID, &seg.UUID); err != nil {
		return nil, fmt.Errorf("meta: завести сегмент тенанта %d: %w", tenant, err)
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO journal_segment_event (segment_id, event_id)
		 SELECT $1, v FROM unnest($2::bigint[]) AS v`,
		seg.ID, ids); err != nil {
		return nil, fmt.Errorf("meta: пометить события сегмента %d: %w", seg.ID, err)
	}

	// Признак обязан стоять ровно на всех взятых событиях: UNIQUE (event_id) не
	// даст записать одно дважды, но не даст и молча пропустить.
	var marked int
	if err := tx.QueryRow(ctx,
		`SELECT count(*) FROM journal_segment_event WHERE segment_id = $1`, seg.ID).
		Scan(&marked); err != nil {
		return nil, fmt.Errorf("meta: сверить признаки сегмента %d: %w", seg.ID, err)
	}
	if marked != len(ids) {
		return nil, fmt.Errorf("meta: сегмент %d: событий %d, помечено %d — признак проставлен не на всё",
			seg.ID, len(ids), marked)
	}
	return &seg, nil
}

// JournalSegments — сегменты тенанта по порядку плотных номеров: так же их
// пройдёт rebuild-meta, проверяя смежность диапазонов.
func (m *Meta) JournalSegments(ctx context.Context, tenant int64) ([]SealedSegment, error) {
	rows, err := m.Pool.Query(ctx,
		`SELECT id, uuid, tenant_id, seq_first, seq_last, event_count
		   FROM journal_segments WHERE tenant_id = $1 ORDER BY seq_first`, tenant)
	if err != nil {
		return nil, fmt.Errorf("meta: сегменты тенанта %d: %w", tenant, err)
	}
	defer rows.Close()

	var out []SealedSegment
	for rows.Next() {
		var s SealedSegment
		if err := rows.Scan(&s.ID, &s.UUID, &s.TenantID, &s.SeqFirst, &s.SeqLast, &s.EventCount); err != nil {
			return nil, fmt.Errorf("meta: сегменты тенанта %d: %w", tenant, err)
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// JournalUnsealedCount — сколько событий тенанта ещё не в сегментах. Опора
// тестов и будущей проверки полноты; cost — см. комментарий к
// JournalEventCount: это сверка по всему журналу тенанта, а не по окну.
func (m *Meta) JournalUnsealedCount(ctx context.Context, tenant int64) (int64, error) {
	var n int64
	if err := m.Pool.QueryRow(ctx,
		`SELECT count(*) FROM journal_events e
		  WHERE e.tenant_id = $1
		    AND NOT EXISTS (SELECT 1 FROM journal_segment_event m WHERE m.event_id = e.id)`,
		tenant).Scan(&n); err != nil {
		return 0, fmt.Errorf("meta: незапечатанные события тенанта %d: %w", tenant, err)
	}
	return n, nil
}
