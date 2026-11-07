package meta

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"time"
)

// archiveObjectSQL — переход участника в archived (общий для FinishPackArchive
// и ArchivePackWhenCopiesDone: один текст на два пути, чтобы правило нельзя
// было забыть в одном из них).
//
// online_until продлевается до max(online_until, now() + landing_min_retention)
// по ADR-0003: HEAD объекта в pending показывает expiry-date не раньше
// now()+landing_min_retention, и после архивации janitor не имеет права
// вытеснить копию раньше показанной даты. Без продления обещание в заголовке
// нарушалось бы ровно на разницу между датой PUT и датой архивации.
const archiveObjectSQL = `
UPDATE objects SET state = 'archived', archived_at = now(),
       online_until = GREATEST(online_until, now() + $4 * interval '1 millisecond')
WHERE id = $3 AND pack_id = $1 AND pack_offset = $2 AND state = 'archiving'`

// Placement — где участник лежит внутри пачки. Ставится в objects одной
// транзакцией с переходом в archived.
type Placement struct {
	Version uuid.UUID
	Offset  uint64
	Length  uint64
}

// PoolTag — тег Phobos пула (pools.phobos_tag): по нему пачки разводят по
// носителям (ADR-0013). Источник истины — таблица pools, а не конфиг: тег
// существует ровно там, где ему предписано быть NOT NULL UNIQUE, и второй
// копии в YAML не место — они имеют свойство расходиться.
func (m *Meta) PoolTag(ctx context.Context, poolID int64) (string, error) {
	var tag string
	if err := m.Pool.QueryRow(ctx,
		`SELECT phobos_tag FROM pools WHERE id = $1`, poolID).Scan(&tag); err != nil {
		return "", fmt.Errorf("meta: тег пула %d: %w", poolID, err)
	}
	return tag, nil
}

// BeginPackArchive заводит строку пачки и переводит участников в archiving.
//
// Возвращает открытую транзакцию: вызывающий обязан закоммитить её ДО вызова
// mover (правило 13). Батч на 200 GiB идёт минуты, и висящая в это время
// транзакция держит и строки участников, и пачку, а параллельный PUT тех же
// ключей упрётся в advisory-лок.
//
// Транзакция возвращается открытой намеренно: если бы она коммитилась здесь,
// у вызывающего не было бы способа нарушить правило 13, и тест на него
// проверял бы не то.
func (m *Meta) BeginPackArchive(ctx context.Context, poolID int64, ids []uuid.UUID) (pgx.Tx, uuid.UUID, error) {
	if len(ids) == 0 {
		return nil, uuid.Nil, fmt.Errorf("meta: пустая пачка")
	}
	tx, err := m.Pool.Begin(ctx)
	if err != nil {
		return nil, uuid.Nil, fmt.Errorf("meta: начать архивацию: %w", err)
	}

	// id генерируется здесь, а не DEFAULT-ом: у packs его нет (миграция 0017
	// уже слита, править её нельзя), а вставить пачку без id — NOT NULL.
	packID := uuid.New()
	_, err = tx.Exec(ctx, `
INSERT INTO packs (id, pool_id, kind, state)
VALUES ($1, $2, 'data', 'open')`, packID, poolID)
	if err != nil {
		tx.Rollback(ctx)
		return nil, uuid.Nil, fmt.Errorf("meta: завести пачку: %w", err)
	}

	// pack_id здесь НЕ ставится: CHECK objects_pack_member требует смещение и
	// длину вместе со ссылкой, а смещения становятся известны только после
	// второго прохода. Ссылка без адреса ничего не стоит, поэтому её ставит
	// FinishPackArchive — одной транзакцией с переходом в archived.
	//
	// Два входа в переход, и оба законны:
	//
	//   * 'pending' — вызывающий выбрал объекты сам (так устроены тесты цикла);
	//   * 'archiving' без pack_id — объекты уже заняты в батч честной выборкой
	//     ClaimMigrationBatch (ADR-0012: справедливость по тенантам живёт там,
	//     и отказываться от неё воркер не вправе). Выборка переводит в
	//     'archiving' сама, иначе SKIP LOCKED не с чем работать.
	//
	// pack_id IS NULL — граница, которая не даёт второму входу утащить участника
	// уже запечатанной пачки: SealPack ставит pack_id, и с этого момента объект
	// для BeginPackArchive недостижим.
	//
	// Переход идемпотенен (правило 7): ноль затронутых строк означает, что
	// объект уже забрал другой воркер, и брать его нельзя.
	tag, err := tx.Exec(ctx, `
UPDATE objects SET state = 'archiving'
WHERE id = ANY($1::uuid[]) AND pack_id IS NULL AND state IN ('pending', 'archiving')`, ids)
	if err != nil {
		tx.Rollback(ctx)
		return nil, uuid.Nil, fmt.Errorf("meta: перевести в archiving: %w", err)
	}
	if int(tag.RowsAffected()) != len(ids) {
		tx.Rollback(ctx)
		return nil, uuid.Nil, fmt.Errorf("meta: из %d объектов в archiving ушло %d — часть уже забрана другим воркером",
			len(ids), tag.RowsAffected())
	}
	return tx, packID, nil
}

// FinishPackArchive записывает итог: копия принята, участники архивированы.
//
// sha256 — ожидаемая сумма потока Phobos, посчитанная migrator'ом. Она
// обязательна для done-копии (CHECK в миграции 0019): без неё reconciler не
// сможет проверить чтение с ленты, а задним числом сумму не восстановить —
// данные уже на ленте.
// mediumID — id ленты из media, 0 означает «носитель ещё не известен»:
// копия пишется в state='writing', а не 'done'. CHECK
// archive_copies_done_has_medium не позволяет объявить копию готовой без
// носителя, и это верно: 'done' без ответа на вопрос «где лежит» невозможно
// ни прочитать, ни проверить.
// workerID — кто завершает. Пачку завершает только её текущий владелец:
// иначе процесс, который опоздал и уже потерял захват, переведёт объекты в
// 'archived' поверх чужой работы.
func (m *Meta) FinishPackArchive(ctx context.Context, packID uuid.UUID, placements []Placement, spec, phobosOID string, version, format int, sha256 []byte, mediumID int64, workerID string, minRetention time.Duration) error {
	if workerID == "" {
		return fmt.Errorf("meta: завершение пачки без workerID")
	}
	// > 0, а не >= 0: ноль — это «забыли пробросить настройку», и он молча
	// превращает продление в GREATEST(online_until, now()), то есть
	// архивированный объект с истёкшим сроком вытесняется на ближайшем
	// проходе janitor — ровно та ошибка, которую продление и чинит.
	// Конфиг эту настройку требует как обязательную, бэкенд не слабее.
	if minRetention <= 0 {
		return fmt.Errorf("meta: landing_min_retention %v, должно быть > 0", minRetention)
	}
	if len(sha256) != 32 {
		return fmt.Errorf("meta: сумма копии %d байт, ждали 32", len(sha256))
	}
	if len(placements) == 0 {
		return fmt.Errorf("meta: пачка без участников")
	}
	tx, err := m.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("meta: начать итог пачки: %w", err)
	}
	defer tx.Rollback(ctx)

	// Одна транзакция на пачку: копия и состояние участников должны появиться
	// вместе, иначе объект «архивирован», а копии нет, и восстановить её уже
	// нечем. doneAt — SQL-выражение: подстановка nil-интерфейса в %s дала бы
	// синтаксическую ошибку, поэтому литерал null.
	state, doneAt := "writing", "null"
	if mediumID != 0 {
		state, doneAt = "done", "now()"
	}
	_, err = tx.Exec(ctx, fmt.Sprintf(`
INSERT INTO archive_copies (pack_id, spec, phobos_oid, phobos_version, format, state, sha256, medium_id, done_at)
VALUES ($1, $2, $3, $4, $5, '%s', $6, NULLIF($7, 0), %s)`, state, doneAt),
		packID, spec, phobosOID, version, format, sha256, mediumID)
	if err != nil {
		return fmt.Errorf("meta: записать копию пачки: %w", err)
	}

	// Адреса участников уже стоят (SealPack, до отправки). Здесь только переход
	// в 'archived': данные на ленте и копия учтена.
	for _, pl := range placements {
		tag, err := tx.Exec(ctx, archiveObjectSQL, packID, pl.Offset, pl.Version,
			minRetention.Milliseconds())
		if err != nil {
			return fmt.Errorf("meta: перевести в archived: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("meta: участник %s не из этой пачки или не запечатан", pl.Version)
		}
	}
	tag, err := tx.Exec(ctx, `
-- Владение проверяется здесь: процесс, который опоздал и уже потерял захват,
-- не должен переводить объекты в archived поверх чужой работы.
UPDATE packs SET state = 'archived', archived_at = now(), claimed_by = NULL, claim_expires_at = NULL
WHERE id = $1 AND state = 'sealed' AND claimed_by = $2`, packID, workerID)
	if err != nil {
		return fmt.Errorf("meta: закрыть пачку: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("meta: пачка %s не запечатана или принадлежит другому процессу — завершать нечем", packID)
	}
	return tx.Commit(ctx)
}

// SealPack — шаг 3 цикла: пачка запечатана, адреса участников известны, отправки
// ещё не было.
//
// Смысл ровно тот же, что у CHECK objects_pack_member: ссылка без адреса ничего
// не стоит. Поэтому смещения берутся из первого прохода (сборка детерминирована,
// ADR-0020) и ставятся ДО mover'а. После этого сбой на любом шаге оставляет
// объекты в 'archiving' с pack_id, и повтор продолжает ТУ ЖЕ пачку — тот же
// UUID, тот же oid Phobos, те же участники в том же порядке.
// sizeBytes — полный размер пачки из плана, а НЕ сумма pack_length
// участников: в пачку входят заголовки tar/pax (~1 КБ на участника), индекс и
// футер. Планировщик ёмкости читает total_bytes и на сумме недооценивал бы
// занятое место, что для мелких участников заметно. Отсюда следствие, которое
// стоит помнить: у полностью живой пачки live_bytes / total_bytes чуть меньше
// единицы, и это не ошибка учёта.
// workerID и ttl — захват в той же транзакции, что и запечатывание. Без него
// только что запечатанная пачка доступна любому процессу, и второй migrator
// начнёт отправлять её параллельно с первым: это не сценарий сбоя, а обычная
// работа двух воркеров.
func (m *Meta) SealPack(ctx context.Context, packID uuid.UUID, placements []Placement, sizeBytes int64,
	workerID string, ttl time.Duration) error {
	if workerID == "" || ttl <= 0 {
		return fmt.Errorf("meta: запечатывание без владельца: пачку сразу заберёт другой процесс")
	}
	if len(placements) == 0 {
		return fmt.Errorf("meta: нечего запечатывать")
	}
	tx, err := m.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("meta: начать запечатывание: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // после commit нечего откатывать

	for _, pl := range placements {
		if pl.Length == 0 {
			return fmt.Errorf("meta: участник %s нулевой длины — CHECK objects_pack_member не примет такую ссылку", pl.Version)
		}
		tag, err := tx.Exec(ctx, `
UPDATE objects SET pack_id = $1, pack_offset = $2, pack_length = $3
WHERE id = $4 AND state = 'archiving' AND pack_id IS NULL`,
			packID, pl.Offset, pl.Length, pl.Version)
		if err != nil {
			return fmt.Errorf("meta: проставить размещение: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("meta: участник %s не из этой пачки или уже запечатан", pl.Version)
		}
	}
	// Итоги пачки считаются здесь же: они зависят только от смещений, а они
	// известны. Планировщик ёмкости читает live_bytes и не должен видеть
	// запечатанную пачку с нулём.
	tag, err := tx.Exec(ctx, `
UPDATE packs SET state = 'sealed',
       total_bytes   = $2,
       live_bytes    = (SELECT coalesce(sum(pack_length), 0) FROM objects WHERE pack_id = $1),
       members_total = (SELECT count(*) FROM objects WHERE pack_id = $1),
       members_live  = (SELECT count(*) FROM objects WHERE pack_id = $1),
       claimed_by    = $3,
       claim_expires_at = now() + $4::interval
WHERE id = $1 AND state = 'open'`, packID, sizeBytes, workerID, interval(ttl))
	if err != nil {
		return fmt.Errorf("meta: запечатать пачку: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("meta: пачка %s не в open — повторное запечатывание", packID)
	}
	return tx.Commit(ctx)
}

// ClaimSealedPack берёт на продолжение одну запечатанную пачку, которую сейчас
// никто не ведёт. Возвращает uuid.Nil, если таких нет.
//
// Это точка входа восстановления после перезапуска: объекты запечатанной пачки
// сидят в 'archiving' с pack_id, в батч не попадают и в pending не возвращаются,
// поэтому искать надо от пачки, а не от списка объектов.
//
// Захват — отметка в строке, а не локи на время работы: отправка mover'у
// занимает минуты, и держать блокировку столько значило бы нарушить правило 13.
// FOR UPDATE SKIP LOCKED держится только внутри этой короткой транзакции.
func (m *Meta) ClaimSealedPack(ctx context.Context, workerID string, ttl time.Duration) (uuid.UUID, error) {
	if workerID == "" || ttl <= 0 {
		return uuid.Nil, fmt.Errorf("meta: захват пачки: нужен workerID и ttl > 0")
	}
	var packID uuid.UUID
	err := m.Pool.QueryRow(ctx, `
UPDATE packs SET claimed_by = $1, claim_expires_at = now() + $2::interval
WHERE id = (
    SELECT id FROM packs
     WHERE state = 'sealed'
       -- Заблокированную не берём: пересылка даст тот же исход, а стоить будет
       -- всю пачку по сети.
       AND blocked_reason IS NULL
       -- Свой захват воркер забирает и до истечения срока: после рестарта тот
       -- же слот возвращается через секунду, а ttl — минуты. Идентичность
       -- обязана быть на воркера, а не на процесс: воркеров в процессе несколько
       -- (ADR-0007), и общий id превратил бы «своё» в «любое чужое».
       AND (claim_expires_at IS NULL OR claim_expires_at < now() OR claimed_by = $1)
     ORDER BY created_at
     FOR UPDATE SKIP LOCKED
     LIMIT 1
)
RETURNING id`, workerID, interval(ttl)).Scan(&packID)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, nil
	}
	if err != nil {
		return uuid.Nil, fmt.Errorf("meta: захват запечатанной пачки: %w", err)
	}
	return packID, nil
}

// BlockPack ставит пачку на блокировку: её нельзя ни считать архивированной,
// ни пересылать, пока человек (позже — stat по oid) не разберётся.
//
// Снимает захват: пачка ничья, и продолжение её не возьмёт — оно ищет пачки без
// blocked_reason. Объекты остаются в 'archiving' с pack_id: пачка не отменена,
// а не разобрана, и инвариант 2 здесь соблюдён.
func (m *Meta) BlockPack(ctx context.Context, packID uuid.UUID, reason string) error {
	if reason == "" {
		return fmt.Errorf("meta: блокировка пачки без причины")
	}
	tag, err := m.Pool.Exec(ctx, `
UPDATE packs SET blocked_reason = $2, blocked_at = now(),
       claimed_by = NULL, claim_expires_at = NULL
WHERE id = $1 AND state = 'sealed' AND blocked_reason IS NULL`, packID, reason)
	if err != nil {
		return fmt.Errorf("meta: заблокировать пачку: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("meta: пачка %s не запечатана или уже заблокирована", packID)
	}
	return nil
}

// UnblockPack снимает блокировку после разбора — вручную (tapegw admin) или
// путём продолжения, когда stat по oid даст ответ.
func (m *Meta) UnblockPack(ctx context.Context, packID uuid.UUID) error {
	tag, err := m.Pool.Exec(ctx, `
UPDATE packs SET blocked_reason = NULL, blocked_at = NULL
WHERE id = $1 AND blocked_reason IS NOT NULL`, packID)
	if err != nil {
		return fmt.Errorf("meta: разблокировать пачку: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("meta: пачка %s не заблокирована", packID)
	}
	return nil
}

// RenewPackClaim продлевает захват. Возвращает false, если захват уже потерян
// (истёк и его забрал другой, либо строку сняли) — вызывающий обязан прервать
// отправку: пачкой занимается кто-то другой, и дописывать её нельзя.
//
// Продление нужно потому, что отправка включает ожидание свободного драйва и
// монтирование — по ADR-0021 это десятки минут. Срок, выставленный один раз,
// истёк бы посреди отправки, и второй процесс отправил бы пачку повторно, пока
// первая отправка ещё идёт.
func (m *Meta) RenewPackClaim(ctx context.Context, packID uuid.UUID, workerID string, ttl time.Duration) (bool, error) {
	tag, err := m.Pool.Exec(ctx, `
UPDATE packs SET claim_expires_at = now() + $3::interval
WHERE id = $1 AND claimed_by = $2`, packID, workerID, interval(ttl))
	if err != nil {
		return false, fmt.Errorf("meta: продлить захват пачки: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// Миллисекунды, не секунды: int(d.Seconds()) режет 300ms в 0, и продление
// выставило бы срок «истёк сразу» — захват формально есть, и тут же просрочен.
func interval(d time.Duration) string { return fmt.Sprintf("%d milliseconds", d.Milliseconds()) }

// ReleasePack снимает захват — после успешного продолжения или когда процесс
// понял, что не справится, и не хочет держать пачку до истечения ttl.
func (m *Meta) ReleasePack(ctx context.Context, packID uuid.UUID) error {
	_, err := m.Pool.Exec(ctx,
		`UPDATE packs SET claimed_by = NULL, claim_expires_at = NULL WHERE id = $1`, packID)
	if err != nil {
		return fmt.Errorf("meta: снять захват пачки: %w", err)
	}
	return nil
}

// PackOfArchiving возвращает пачку, в которую эти объекты уже запечатаны.
// Нужна для продолжения после сбоя: без неё следующий цикл завёл бы новую пачку
// с новым oid, и первая запись осталась на ленте никем не учтённой.
//
// Возвращает uuid.Nil, если единой пачки нет: объекты без pack_id или из
// разных пачек. Смешанную компанию продолжат отдельными пачками — перемешивать
// участников между пачками нельзя, это меняет байты.
func (m *Meta) PackOfArchiving(ctx context.Context, ids []uuid.UUID) (uuid.UUID, error) {
	var missing, distinct int
	var packID *uuid.UUID
	err := m.Pool.QueryRow(ctx, `
SELECT count(*) FILTER (WHERE pack_id IS NULL),
       count(DISTINCT pack_id),
       -- min(uuid) в Postgres нет, а нужен «любой из, раз они все равны»:
       -- равенство проверяет distinct = 1 выше.
       (array_agg(pack_id) FILTER (WHERE pack_id IS NOT NULL))[1]
FROM objects WHERE id = ANY($1::uuid[])`, ids).Scan(&missing, &distinct, &packID)
	if err != nil {
		return uuid.Nil, fmt.Errorf("meta: найти пачку объектов: %w", err)
	}
	// Единая пачка — это когда у ВСЕХ объектов она есть и она одна. Иначе
	// продолжения нет: часть без адреса, или компания разбросана по пачкам, а
	// перемешивать участников нельзя (байты и сумма другие).
	if missing > 0 || distinct != 1 || packID == nil {
		return uuid.Nil, nil
	}
	return *packID, nil
}

// SealedMembers возвращает участников запечатанной пачки в порядке смещений —
// тот самый порядок, в котором байты уже посчитаны. Повтор обязан собрать пачку
// в нём же, иначе сумма не сойдётся.
func (m *Meta) SealedMembers(ctx context.Context, packID uuid.UUID) ([]uuid.UUID, error) {
	rows, err := m.Pool.Query(ctx, `
SELECT id FROM objects WHERE pack_id = $1 ORDER BY pack_offset`, packID)
	if err != nil {
		return nil, fmt.Errorf("meta: участники пачки: %w", err)
	}
	defer rows.Close()
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("meta: участники пачки: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// FailPackArchive возвращает участников в pending после отказа mover.
func (m *Meta) FailPackArchive(ctx context.Context, packID uuid.UUID, ids []uuid.UUID, reason string) error {
	tx, err := m.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("meta: начать откат пачки: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
UPDATE objects SET state = 'pending', pack_id = NULL, last_error = $2
WHERE id = ANY($1::uuid[]) AND state = 'archiving'`, ids, reason); err != nil {
		return fmt.Errorf("meta: вернуть в pending: %w", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM packs WHERE id = $1`, packID); err != nil {
		return fmt.Errorf("meta: убрать пустую пачку: %w", err)
	}
	return tx.Commit(ctx)
}

// MediaIDByLabel переводит имя носителя из extents mover'а в id строки media.
// Возвращает 0 без ошибки, если ленты в реестре нет: migrator тогда пишет копию
// в 'writing', а не выдумывает носитель.
func (m *Meta) MediaIDByLabel(ctx context.Context, label string) (int64, error) {
	var id int64
	err := m.Pool.QueryRow(ctx, `SELECT id FROM media WHERE label = $1`, label).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("meta: лента %s: %w", label, err)
	}
	return id, nil
}

// BindCopyMedium доводит копию из 'writing' до 'done', когда лента наконец
// зарегистрирована в реестре. Перезаписи не происходит: данные лежат на ленте
// с момента записи, не хватало только строки media.
// copyID — uuid строки archive_copies (не int64: id там uuid, и первый же
// вызов с числом упал бы «operator does not exist: uuid = bigint»).
func (m *Meta) BindCopyMedium(ctx context.Context, copyID uuid.UUID, mediaID int64) error {
	tag, err := m.Pool.Exec(ctx, `
UPDATE archive_copies SET medium_id = $2, state = 'done', done_at = now()
WHERE id = $1 AND state = 'writing'`, copyID, mediaID)
	if err != nil {
		return fmt.Errorf("meta: привязать ленту к копии: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("meta: копия %s не в writing", copyID)
	}
	return nil
}

// --- С5/С6: несколько копий пачки из одного буфера --------------------------
//
// Пачка становится 'archived' только когда записаны ВСЕ копии политики (С5).
// Сбой одной копии не откатывает остальные (С6): записанная копия остаётся
// 'done', недостающая догоняется отдельно.

// RecordPackCopy записывает одну готовую копию пачки. Отдельно от перевода
// объектов в 'archived' именно потому, что копий может быть несколько: каждая
// готова в свой момент, а 'archived' — событие про все сразу.
func (m *Meta) RecordPackCopy(ctx context.Context, packID uuid.UUID, spec, phobosOID string, version, format int, sha256 []byte, mediumID int64) (uuid.UUID, error) {
	if len(sha256) != 32 {
		return uuid.Nil, fmt.Errorf("meta: сумма копии %d байт, ждали 32", len(sha256))
	}
	if spec == "" {
		return uuid.Nil, fmt.Errorf("meta: копия без spec")
	}
	// То же правило, что в FinishPackArchive: 'done' без носителя CHECK
	// archive_copies_done_has_medium не принимает — и правильно делает.
	// Копия, легшая на ленту вне реестра, остаётся в 'writing' и доводится
	// до 'done' через BindCopyMedium. Впервые всплыло на вертикальном срезе
	// ci-dir: extent пришёл с меткой носителя, которой нет в media, и
	// незаписанная копия зациклила продолжение пачки на CHECK-ошибке.
	// doneAt — SQL-выражение, не аргумент: '%s' с nil-интерфейсом дал бы
	// синтаксическую ошибку, поэтому литерал null.
	state, doneAt := "writing", "null"
	if mediumID != 0 {
		state, doneAt = "done", "now()"
	}
	var id uuid.UUID
	err := m.Pool.QueryRow(ctx, fmt.Sprintf(`
INSERT INTO archive_copies (pack_id, spec, phobos_oid, phobos_version, format, state, sha256, medium_id, done_at)
VALUES ($1, $2, $3, $4, $5, '%s', $6, NULLIF($7, 0), %s)
RETURNING id`, state, doneAt),
		packID, spec, phobosOID, version, format, sha256, mediumID).Scan(&id)
	if err != nil {
		return uuid.Nil, fmt.Errorf("meta: записать копию пачки: %w", err)
	}
	return id, nil
}

// FailPackCopy помечает копию мёртвой, не трогая остальные (С6). Записанное до
// сбоя остаётся записанным: откатывать копию, которая лежит на ленте, — значит
// потерять данные ради формы.
func (m *Meta) FailPackCopy(ctx context.Context, packID uuid.UUID, spec, reason string) error {
	tag, err := m.Pool.Exec(ctx, `
UPDATE archive_copies SET state = 'dead', attrs = coalesce(attrs,'{}') || jsonb_build_object('reason', $3)
WHERE pack_id = $1 AND spec = $2 AND state IN ('pending','writing')`, packID, spec, reason)
	if err != nil {
		return fmt.Errorf("meta: пометить копию мёртвой: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("meta: живой копии %s у пачки %s нет", spec, packID)
	}
	return nil
}

// MissingCopySpecs — спецификации, которых пачке не хватает. Мёртвая копия
// считается недостающей: её догоняют отдельно, и точечно, а не всей пачкой.
func (m *Meta) MissingCopySpecs(ctx context.Context, packID uuid.UUID, specs []string) ([]string, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	rows, err := m.Pool.Query(ctx, `
SELECT spec FROM unnest($2::text[]) AS want(spec)
WHERE NOT EXISTS (
	SELECT 1 FROM archive_copies c
	WHERE c.pack_id = $1 AND c.spec = want.spec AND c.state = 'done')`, packID, specs)
	if err != nil {
		return nil, fmt.Errorf("meta: недостающие копии: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// ArchivePackWhenCopiesDone переводит пачку и участников в 'archived', только
// когда все нужные копии записаны. Возвращает false, если копий не хватает:
// это не ошибка, а «ещё не готово» — остальное догонит следующий проход.
func (m *Meta) ArchivePackWhenCopiesDone(ctx context.Context, packID uuid.UUID, placements []Placement, specs []string, workerID string, minRetention time.Duration) (bool, error) {
	if workerID == "" {
		return false, fmt.Errorf("meta: завершение пачки без workerID")
	}
	// См. то же правило в FinishPackArchive: ноль — забытый проброс, а не
	// «продлевать не надо».
	if minRetention <= 0 {
		return false, fmt.Errorf("meta: landing_min_retention %v, должно быть > 0", minRetention)
	}
	missing, err := m.MissingCopySpecs(ctx, packID, specs)
	if err != nil {
		return false, err
	}
	if len(missing) > 0 {
		return false, nil
	}
	tx, err := m.Pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("meta: начать итог пачки: %w", err)
	}
	defer tx.Rollback(ctx)

	for _, pl := range placements {
		tag, err := tx.Exec(ctx, archiveObjectSQL, packID, pl.Offset, pl.Version,
			minRetention.Milliseconds())
		if err != nil {
			return false, fmt.Errorf("meta: перевести в archived: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return false, fmt.Errorf("meta: участник %s не из этой пачки или не запечатан", pl.Version)
		}
	}
	tag, err := tx.Exec(ctx, `
UPDATE packs SET state = 'archived', archived_at = now(), claimed_by = NULL, claim_expires_at = NULL
WHERE id = $1 AND state = 'sealed' AND claimed_by = $2`, packID, workerID)
	if err != nil {
		return false, fmt.Errorf("meta: закрыть пачку: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return false, fmt.Errorf("meta: пачка %s не запечатана или захвачена не им", packID)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("meta: записать итог пачки: %w", err)
	}
	return true, nil
}
