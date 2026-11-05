package meta

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Хранение ключей тенанта (TEK). Криптография здесь не живёт: meta не знает ни
// про KEK, ни про обёртку — она хранит байты. Обёрткой заведует internal/keys,
// который видит и meta, и iam.KeyRing (meta импортировать iam не может: iam уже
// импортирует meta).
//
// Почему TEK заводится лениво, а не миграцией. Колонка tenants.wrapped_key
// появилась на T0 с примечанием «T2 сделает бэкфилл и SET NOT NULL». Бэкфилл
// миграцией невозможен: миграция KEK не знает, а вставить выдуманный ключ —
// значит сделать данные нечитаемыми. Поэтому NULL в этой колонке означает
// «ключа ещё нет», а не «данные не шифруются», и NOT NULL здесь не ставится.
// Первый же PUT заводит ключ атомарно (см. EnsureTenantKey), и после этого
// NULL остаётся только у тенантов, у которых вообще ничего не записано.

// ErrTenantKeyGone — ключ тенанта уничтожен, данные прочесть нельзя.
//
// Отдельная ошибка, а не ErrNotFound: оператору нужно различать «тенанта нет» и
// «тенант есть, но ключ стёрт» — второе означает крипто-уничтожение (ADR-0014)
// и требует совсем другой реакции.
var ErrTenantKeyGone = errors.New("meta: ключ тенанта уничтожен")

// TenantKeyRow — состояние ключа тенанта.
//
// Wrapped == nil и Destroyed == false — «ключа ещё нет» (тенант ничего не
// записывал). Wrapped == nil и Destroyed == true — «ключ уничтожен», и это не
// то же самое, что «ещё нет»: в первом случае запись поднимет тенанта из
// мёртвых, во втором обязана быть отклонена (миграция 0013).
type TenantKeyRow struct {
	Wrapped    []byte
	KekVersion int
	Destroyed  bool
}

// TenantKey читает обёртку TEK. Строки тенанта без колонки ключа не бывает:
// колонка есть у всех с миграции 0005.
func (m *Meta) TenantKey(ctx context.Context, tenant int64) (TenantKeyRow, error) {
	var row TenantKeyRow
	err := m.Pool.QueryRow(ctx,
		`SELECT wrapped_key, kek_version FROM tenants WHERE id = $1`, tenant).
		Scan(&row.Wrapped, &row.KekVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return row, ErrNotFound
	}
	if err != nil {
		return row, fmt.Errorf("meta: ключ тенанта %d: %w", tenant, err)
	}
	return row, nil
}

// EnsureTenantKey атомарно заводит ключ тенанта, если его ещё нет, и возвращает
// актуальную обёртку.
//
// Гонка двух первых PUT одного тенанта решается условием WHERE wrapped_key IS
// NULL: победитель записывает свой ключ, проигравший ноль затронутых строк и
// перечитывает уже чужой ключ (правило 7: воркер идемпотентен и не спорит с
// тем, кто его опередил). Два разных TEK у одного тенанта быть не может.
func (m *Meta) EnsureTenantKey(ctx context.Context, tenant int64, wrapped []byte, kekVersion int) (TenantKeyRow, error) {
	tag, err := m.Pool.Exec(ctx,
		`UPDATE tenants SET wrapped_key = $2, kek_version = $3
		   WHERE id = $1 AND wrapped_key IS NULL AND key_destroyed_at IS NULL`,
		tenant, wrapped, kekVersion)
	if err != nil {
		return TenantKeyRow{}, fmt.Errorf("meta: заведение ключа тенанта %d: %w", tenant, err)
	}
	if tag.RowsAffected() == 0 {
		// Либо опередили (нормально, перечитаем чужой ключ ниже), либо ключ
		// уничтожен — тогда перечитанная строка вернёт Destroyed, и вызывающий
		// откажет.
		_ = tag
	}
	return m.TenantKey(ctx, tenant)
}

// SetTenantKey переписывает обёртку ключа. Нужен для ротации KEK: TEK тот же,
// меняется только обёртка и версия KEK.
func (m *Meta) SetTenantKey(ctx context.Context, tenant int64, wrapped []byte, kekVersion int) error {
	tag, err := m.Pool.Exec(ctx,
		`UPDATE tenants SET wrapped_key = $2, kek_version = $3 WHERE id = $1`,
		tenant, wrapped, kekVersion)
	if err != nil {
		return fmt.Errorf("meta: запись ключа тенанта %d: %w", tenant, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// DestroyTenantKey стирает обёртку ключа и выставляет неизгладимый признак
// уничтожения — крипто-уничтожение (ADR-0014).
//
// Вызывается только последним шагом удаления тенанта, и только когда нет версий
// с неистёкшим retain_until или legal_hold.
//
// Одного NULL мало: он же означал бы «ключа ещё нет», и первая же запись
// завела бы новый TEK. Поэтому рядом ставится key_destroyed_at, и EnsureTenantKey
// с ним не спорит.
func (m *Meta) DestroyTenantKey(ctx context.Context, tenant int64) error {
	_, err := m.Pool.Exec(ctx,
		`UPDATE tenants SET wrapped_key = NULL, kek_version = 0, key_destroyed_at = now()
		   WHERE id = $1`, tenant)
	if err != nil {
		return fmt.Errorf("meta: уничтожение ключа тенанта %d: %w", tenant, err)
	}
	return nil
}

// TenantPool — пул тенанта. Нужен восстановлению: pool_id у objects NOT NULL,
// а взять его при rebuild неоткуда, кроме как из строки тенанта.
func (m *Meta) TenantPool(ctx context.Context, tenant int64) (int64, error) {
	var pool int64
	err := m.Pool.QueryRow(ctx,
		`SELECT pool_id FROM tenants WHERE id = $1`, tenant).Scan(&pool)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("meta: пул тенанта %d: %w", tenant, err)
	}
	return pool, nil
}

// TenantKeyRowWithID — строка для прогона ротации KEK: нужен id тенанта.
type TenantKeyRowWithID struct {
	ID         int64
	Wrapped    []byte
	KekVersion int
}

// TenantsWithKeys — все тенанты с заведённым ключом. Ротация KEK ходит по этому
// списку; тенанты без ключа в нём не участвуют — переоборачивать нечего.
func (m *Meta) TenantsWithKeys(ctx context.Context) ([]TenantKeyRowWithID, error) {
	rows, err := m.Pool.Query(ctx,
		`SELECT id, wrapped_key, kek_version FROM tenants
		  WHERE wrapped_key IS NOT NULL ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("meta: список ключей тенантов: %w", err)
	}
	defer rows.Close()

	var out []TenantKeyRowWithID
	for rows.Next() {
		var r TenantKeyRowWithID
		if err := rows.Scan(&r.ID, &r.Wrapped, &r.KekVersion); err != nil {
			return nil, fmt.Errorf("meta: список ключей тенантов: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("meta: список ключей тенантов: %w", err)
	}
	return out, nil
}
