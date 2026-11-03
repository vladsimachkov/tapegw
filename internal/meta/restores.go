package meta

import (
	"context"
	"errors"
	"fmt"
	"net/http"
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

// HeaderValue — содержимое заголовка x-amz-restore.
//
// Формат совпадает с scoutfs (backend/scoutfs/scoutfs_compat.go), откуда и взято
// поведение Glacier-семантики: ongoing-request="true" во время восстановления и
// ongoing-request="false", expiry-date="..." когда копия выдана.
//
// Ключевое отличие от scoutfs: дата берётся из реального срока, а не подставляется
// фиктивная 2050. Прежний механизм обещал доступность до 2050, объект архивировался,
// копия вытеснялась, и GET через час отдавал InvalidObjectState. См. ADR-0003.
//
// nil означает «заголовка не будет»: объекта без запроса восстановления это не
// касается, а выдумывать заголовок — значит врать клиенту.
func (r *Restore) HeaderValue() *string {
	if r == nil {
		return nil
	}
	if r.Ongoing() {
		v := `ongoing-request="true"`
		return &v
	}
	if r.State != "restored" || r.ExpiresAt == nil {
		return nil
	}
	v := fmt.Sprintf(`ongoing-request="false", expiry-date=%q`,
		r.ExpiresAt.UTC().Format(http.TimeFormat))
	return &v
}

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
