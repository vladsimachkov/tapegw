package meta

// Учёт версий KEK (ADR-0014).
//
// Таблица нужна ровно для одного вопроса: «можно ли уже выбросить версию N».
// Ответ на него — не «ротация прошла», а «с момента, когда версия уступила
// активной, прошло не меньше max(срок экспорта, срок бэкапов)». Момент уступки
// больше нигде не записан: файл кольца KEK хранит версию → ключ без дат, а
// память оператора не является носителем режима хранения.
//
// Отсюда странное на вид правило: superseded_at IS NULL означает и «версия ещё
// активна», и «когда устарела — неизвестно» (например, версия досталась нам из
// дампа). Для отказа от ключа оба случая недоказуемы, поэтому отказываем в обоих.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// ErrKEKVersionUnknown — про версию нет записи. Обычно означает «её создали до
// миграции 0016 и она не упоминалась в tenants/accounts».
var ErrKEKVersionUnknown = errors.New("meta: версия KEK не учтена")

// KEKVersion — то, что известно про версию KEK.
type KEKVersion struct {
	Version int
	// CreatedAt — когда версия появилась в учёте. Это момент записи строки, а не
	// момент, когда ключ напечатали; для расчёта срока он не используется.
	CreatedAt time.Time
	// SupersededAt — когда версия уступила активной. nil — неизвестно или ещё
	// активна.
	SupersededAt *time.Time
}

// EnsureKEKVersion заводит учётную запись версии, если её нет.
func (m *Meta) EnsureKEKVersion(ctx context.Context, version int) error {
	_, err := m.Pool.Exec(ctx,
		`INSERT INTO kek_versions (version) VALUES ($1) ON CONFLICT (version) DO NOTHING`,
		version)
	if err != nil {
		return fmt.Errorf("meta: учёт версии KEK %d: %w", version, err)
	}
	return nil
}

// MarkKEKSuperseded отмечает одну версию как устаревшую с указанного момента.
//
// Версия устаревает не когда вышла из конфига, а когда с неё уезжает ПОСЛЕДНЯЯ
// строка: до этого момента она ещё нужна, а после — уже нет. Помечать «все кроме
// активной» нельзя: так часы стартуют у версии, которая ещё не становилась
// активной (её заранее положили в кольцо), и оператор получил бы право выбросить
// ключ, который ещё ни разу не защищал ни одну обёртку.
//
// COALESCE здесь — не украшение: повторный запуск ротации не должен сдвигать
// уже записанный момент уступки. Сдвинул бы — и срок «когда ключ можно выбросить»
// уехал бы в будущее по чистой случайности, а такое «безопаснее» никто не
// заметил бы, пока кто-нибудь не попытался бы выбросить ключ вовремя.
func (m *Meta) MarkKEKSuperseded(ctx context.Context, version int, at time.Time) error {
	_, err := m.Pool.Exec(ctx,
		`UPDATE kek_versions SET superseded_at = COALESCE(superseded_at, $2)
		  WHERE version = $1`, version, at)
	if err != nil {
		return fmt.Errorf("meta: отметка устаревшей версии KEK %d: %w", version, err)
	}
	return nil
}

// GetKEKVersion читает учётную запись версии.
func (m *Meta) GetKEKVersion(ctx context.Context, version int) (KEKVersion, error) {
	var v KEKVersion
	err := m.Pool.QueryRow(ctx,
		`SELECT version, created_at, superseded_at FROM kek_versions WHERE version = $1`,
		version).Scan(&v.Version, &v.CreatedAt, &v.SupersededAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return v, ErrKEKVersionUnknown
	}
	if err != nil {
		return v, fmt.Errorf("meta: учёт версии KEK %d: %w", version, err)
	}
	return v, nil
}

// KEKVersions — все учтённые версии по возрастанию.
func (m *Meta) KEKVersions(ctx context.Context) ([]KEKVersion, error) {
	rows, err := m.Pool.Query(ctx,
		`SELECT version, created_at, superseded_at FROM kek_versions ORDER BY version`)
	if err != nil {
		return nil, fmt.Errorf("meta: список версий KEK: %w", err)
	}
	defer rows.Close()

	var out []KEKVersion
	for rows.Next() {
		var v KEKVersion
		if err := rows.Scan(&v.Version, &v.CreatedAt, &v.SupersededAt); err != nil {
			return nil, fmt.Errorf("meta: список версий KEK: %w", err)
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
