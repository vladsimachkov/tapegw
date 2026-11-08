package meta

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Регистрация носителя (Ф1.5). Строка media появляется одной операцией
// вместе с регистрацией в Phobos — эту половину делает internal/admin, здесь
// только БД. Порядок «сначала Phobos, потом строка» задан вызывающим:
// полусостояние «в Phobos есть, у нас нет» переживаемо (копия подождёт в
// 'writing', команда повторится и доведёт), обратное — нет (планировщик
// обещал место, которого в Phobos нет).

// ErrMediaLabelTaken — метка уже занята другой строкой реестра. Уникальность
// метки во всей установке обеспечивает схема (media.label UNIQUE, миграция
// 0018), эта ошибка — человекочитаемая упаковка того же конфликта для
// оператора: «лента с такой меткой уже зарегистрирована с другими
// параметрами» вместо 23503.
var ErrMediaLabelTaken = errors.New("meta: метка носителя уже занята с другими параметрами")

// MediaRegistration — параметры регистрируемого носителя. Side пустой —
// сторона не задана (колонка nullable).
type MediaRegistration struct {
	Label         string
	Generation    string
	MediumType    string
	Side          string
	PoolID        int64
	CapacityBytes int64
	FreeBytes     int64
	// Lifecycle — 'active' (цель записи: вызывающий проверил в Phobos право
	// писать) или 'full' (на носителе уже есть данные, места нет — не цель).
	// Регистрация не инструмент переводов lifecycle, третьих значений здесь не
	// бывает.
	Lifecycle string
}

// RegisterMedia заводит строку media с указанным lifecycle ('active' или
// 'full'): вызывающий обязан убедиться в Phobos, что носитель отформатирован,
// — до этой проверки строку заводить нельзя. Повтор команды с теми же
// параметрами идемпотентен и возвращает существующий id; параметры другой
// строки на той же метке — ErrMediaLabelTaken, перетегировать чужой носитель
// молча нельзя.
func (m *Meta) RegisterMedia(ctx context.Context, r MediaRegistration) (int64, error) {
	if r.Lifecycle != "active" && r.Lifecycle != "full" {
		return 0, fmt.Errorf("meta: регистрация носителя %q: lifecycle %q, ожидаются active или full", r.Label, r.Lifecycle)
	}
	var id int64
	err := m.Pool.QueryRow(ctx,
		`INSERT INTO media (label, generation, medium_type, pool_id, side,
		                    lifecycle, capacity_bytes_cached, free_bytes_cached)
		 VALUES ($1, $2, $3, $4, NULLIF($5, ''), $8, $6, $7)
		 ON CONFLICT (label) DO NOTHING
		 RETURNING id`,
		r.Label, r.Generation, r.MediumType, r.PoolID, r.Side,
		r.CapacityBytes, r.FreeBytes, r.Lifecycle).Scan(&id)
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("meta: регистрация носителя %q: %w", r.Label, err)
	}

	// Метка уже в реестре (схема не пустила вторую строку). Разбираемся,
	// повтор это или конфликт.
	row, exists, err := m.MediaByLabel(ctx, r.Label)
	if err != nil {
		return 0, err
	}
	if !exists {
		// Вставка не прошла, а строки нет: её успели удалить между запросами.
		return 0, fmt.Errorf("meta: носитель %q: гонка регистрации, повторите операцию", r.Label)
	}
	sameSide := (row.Side == nil && r.Side == "") || (row.Side != nil && *row.Side == r.Side)
	samePool := row.PoolID != nil && *row.PoolID == r.PoolID
	// 'full' в списке наравне с активными состояниями: повтор команды на
	// зарегистрированной заполненной ленте обязан возвращать тот же id,
	// иначе доводку висящих копий нельзя запустить второй раз.
	if row.Generation == r.Generation && row.MediumType == r.MediumType && samePool && sameSide &&
		(row.Lifecycle == "active" || row.Lifecycle == "formatted" || row.Lifecycle == "new" || row.Lifecycle == "full") {
		return row.ID, nil
	}
	return 0, fmt.Errorf("%w: %q (в реестре %s/%s, пул %v, сторона %q, состояние %s)",
		ErrMediaLabelTaken, r.Label, row.Generation, row.MediumType, row.PoolID, derefStr(row.Side), row.Lifecycle)
}

// MediaRow — строка реестра по метке (для предварительной проверки команды
// media add: не трогать Phobos, если метка уже занята другими параметрами).
type MediaRow struct {
	ID         int64
	Generation string
	MediumType string
	PoolID     *int64
	Side       *string
	Lifecycle  string
}

// MediaByLabel — строка media по метке; false без ошибки — носителя нет.
func (m *Meta) MediaByLabel(ctx context.Context, label string) (*MediaRow, bool, error) {
	var row MediaRow
	err := m.Pool.QueryRow(ctx,
		`SELECT id, generation, medium_type, pool_id, side, lifecycle FROM media WHERE label = $1`,
		label).Scan(&row.ID, &row.Generation, &row.MediumType, &row.PoolID, &row.Side, &row.Lifecycle)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("meta: строка носителя %q: %w", label, err)
	}
	return &row, true, nil
}

func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// StuckCopy — копия, записанная на носитель вне реестра: 'writing' при
// medium_id IS NULL (migrator не нашёл метку в media, meta/archive.go).
type StuckCopy struct {
	ID        uuid.UUID
	PhobosOID string
}

// StuckCopies перечисляет копии, застрявшие в 'writing' из-за неизвестного
// носителя. Это тот самый счётчик, который обязан показывать stand-status:
// без строки media копия не выходит из 'writing' никогда, и сигнал был только
// ERROR-строкой в логе migrator'а.
func (m *Meta) StuckCopies(ctx context.Context) ([]StuckCopy, error) {
	rows, err := m.Pool.Query(ctx,
		`SELECT id, phobos_oid FROM archive_copies
		  WHERE state = 'writing' AND medium_id IS NULL
		  ORDER BY created_at`)
	if err != nil {
		return nil, fmt.Errorf("meta: копии в writing без носителя: %w", err)
	}
	defer rows.Close()
	var out []StuckCopy
	for rows.Next() {
		var c StuckCopy
		if err := rows.Scan(&c.ID, &c.PhobosOID); err != nil {
			return nil, fmt.Errorf("meta: копии в writing без носителя: %w", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
