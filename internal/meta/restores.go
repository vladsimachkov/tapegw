package meta

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Restore — строка таблицы restores. В M2 читается только для заголовка
// x-amz-restore; создаёт и двигает её recaller в M5.
type Restore struct {
	ObjectID    uuid.UUID
	State       string
	Tier        string
	Days        int
	RequestedAt time.Time
	RestoredAt  *time.Time
	ExpiresAt   *time.Time
}

// Ongoing — запрос принят, но копия ещё не готова.
func (r *Restore) Ongoing() bool {
	return r.State == "requested" || r.State == "in_progress"
}

// Active — копия выдана и срок не истёк.
func (r *Restore) Active(now time.Time) bool {
	return r.State == "restored" && r.ExpiresAt != nil && r.ExpiresAt.After(now)
}

// Формирование заголовка x-amz-restore живёт в бэкенде
// (backend.restoreHeader): по ADR-0003 показанная дата берётся из
// objects.online_until, а не из restores.expires_at, поэтому одного
// Restore для её сборки мало — нужен ещё и объект. Здесь только состояние.

// GetRestore возвращает активный запрос восстановления объекта того же
// тенанта. ErrNotFound — запроса нет, и это норма для большинства объектов.
//
// Тенант берётся из restores.tenant_id (миграция 0005) — той же денормализации,
// что у objects: сверка без JOIN, но и предикат обязан быть, иначе запрос
// чужого объекта нашёл бы чужую строку.
func (m *Meta) GetRestore(ctx context.Context, tenant TenantID, objectID uuid.UUID) (*Restore, error) {
	q, args := withTenant(
		`SELECT object_id, state, tier, days, requested_at, restored_at, expires_at
		   FROM restores WHERE object_id = $1`, tenant, objectID)
	row := m.Pool.QueryRow(ctx, q, args...)

	var r Restore
	err := row.Scan(&r.ObjectID, &r.State, &r.Tier, &r.Days,
		&r.RequestedAt, &r.RestoredAt, &r.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scan restore: %w", err)
	}
	return &r, nil
}
