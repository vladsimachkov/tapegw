package keys

// Можно ли уже выбросить версию KEK (ADR-0014).
//
// Решение живёт не в CLI нарочно: в main его не проверить, а проверяет оно
// обещание клиенту. Правило одно: срок отсчитывается от момента, когда версия
// уступила активной, и равен максимуму из срока экспорта и срока PITR-бэкапов.
// Обёрнутая TEK лежит в каждом дампе и в каждом WAL-сегменте, поэтому «прошло 30
// дней с ротации» при бэкапах на 90 суток ничего не значит.
//
// Второй принцип: недоказуемое «срок прошёл» = отказ. Если момент уступки не
// записан (версия досталась из дампа до учёта версий) или версия ещё активна,
// CLI обязан отказать, а не предположить самое удобное.

import (
	"errors"
	"fmt"
	"time"
)

// ForgetCheck — вход для решения.
type ForgetCheck struct {
	Version int
	// Active — активная версия кольца.
	Active int
	// SupersededAt — когда версия уступила активной. nil — неизвестно.
	SupersededAt *time.Time
	// ExportRetention — iam.export_retention.
	ExportRetention time.Duration
	// BackupRetention — iam.backup_retention, глубина PITR.
	BackupRetention time.Duration
	// Now — текущее время; тесты подставляют своё.
	Now time.Time
}

// CanForget возвращает nil, если версию можно удалить из кольца.
func CanForget(c ForgetCheck) error {
	if c.Version == c.Active {
		return fmt.Errorf("версия %d активна: на неё заворачиваются новые обёртки", c.Version)
	}
	if c.BackupRetention <= 0 {
		return errors.New("iam.backup_retention не задан: окончательность = max(срок экспорта, срок PITR), " +
			"а глубину PITR вы не указали")
	}
	if c.ExportRetention <= 0 {
		return errors.New("iam.export_retention не задан")
	}
	if c.SupersededAt == nil {
		return fmt.Errorf("версия %d не отмечена устаревшей: она либо активна, "+
			"либо уступила до того, как версии стали учитываться, — доказать прошествие срока нельзя", c.Version)
	}

	required := c.ExportRetention
	if c.BackupRetention > required {
		required = c.BackupRetention
	}
	elapsed := c.Now.Sub(*c.SupersededAt)
	if elapsed < 0 {
		return fmt.Errorf("момент уступки версии %d лежит в будущем (%s): часы рассинхронизированы",
			c.Version, c.SupersededAt.Format(time.RFC3339))
	}
	if elapsed < required {
		return fmt.Errorf("версию %d нельзя забывать ещё %v: устарела %s, нужно %v "+
			"(max из экспорт %v и PITR %v)", c.Version, (required - elapsed).Round(time.Hour),
			c.SupersededAt.Format(time.RFC3339), required, c.ExportRetention, c.BackupRetention)
	}
	return nil
}
