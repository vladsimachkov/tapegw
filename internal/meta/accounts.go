package meta

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Учётки versitygw (ADR-0009). Таблица `accounts` — единственное место, где
// живут ключи доступа, `iam_dir` на диске больше не источник истины.
//
// Секрет здесь никогда не бывает в открытом виде: он обёрнут KEK
// (secret_wrapped) и лежит вместе с версией KEK, которой завернут. Разворачивает
// и заворачивает их слой IAM (internal/iam), здесь только байты.

// Состояния учётки. Совпадают с enum account_status из миграции 0005.
const (
	AccountActive  = "active"
	AccountRevoked = "revoked"
)

// Account — строка таблицы accounts. SecretWrapped — результат AES-GCM, а не
// секрет; открытый секрет знает только слой IAM (ADR-0009).
type Account struct {
	Access        string
	SecretWrapped []byte
	KekVersion    int
	Role          string
	TenantID      int64
	// TenantSystem — денормализация tenants.system (миграция 0007, ADR-0018).
	// Заполняет её CreateAccount/UpdateAccount, а не вызывающий: колонка обязана
	// совпадать с флагом тенанта, и «передать флаг руками» означало бы дать
	// вызывающему возможность записать пару вразнобой.
	TenantSystem bool
	Status       string
}

// AccountPatch — что меняем в учётке. nil / пустая строка = не менять.
type AccountPatch struct {
	// SecretWrapped и KekVersion задаются только вместе: новый секрет обязан
	// приехать с версией KEK, которой он завернут, иначе после ротации KEK его
	// уже не развернуть.
	SecretWrapped []byte
	KekVersion    int
	Role          string
	// TenantID — перенос учётки в другой тенант. nil = не менять.
	TenantID *int64
}

// ErrAccountExists — учётка с таким access_key уже есть. Отдельно от
// ErrNotFound по той же причине, по какой существовал ErrUserExists в файловом
// IAM: «уже есть» и «нет такого» для админской операции — разные ответы.
var ErrAccountExists = errors.New("meta: account exists")

// AccountsNotifyChannel — канал LISTEN/NOTIFY, по которому кэш IAM узнаёт об
// изменении учётки (ADR-0016). Payload — access_key изменившейся учётки.
const AccountsNotifyChannel = "tapegw_accounts_changed"

// notifyAccountChanged — событие «учётка изменилась», из-за которого кэш
// сбрасывает запись.
//
// Вызывается **в той же транзакции**, что и сама правка, и это не стиль:
// pg_notify внутри транзакции доставляется только после коммита. Значит
// листенер не может получить событие про строку, которая так и не изменилась
// (транзакцию откатили), и не может пропустить изменение, которое уже
// закоммичено: окна между «данные видны всем» и «событие ушло» нет.
//
// Границу надо знать явно: мгновенно сбрасывается кэш изменений, сделанных
// через tapegw. Правка accounts в обход этих функций — ручной SQL, другой
// инструмент — события не порождает, и там остаётся TTL-страховка кэша.
func notifyAccountChanged(ctx context.Context, tx pgx.Tx, access string) error {
	if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, AccountsNotifyChannel, access); err != nil {
		return fmt.Errorf("notify account %q: %w", access, err)
	}
	return nil
}

// accountColumns — общий список, чтобы SELECT и ListAccounts не разъезжались.
const accountColumns = `access_key, secret_wrapped, kek_version, role,
	tenant_id, tenant_system, status`

func scanAccount(row pgx.Row) (Account, error) {
	var a Account
	err := row.Scan(&a.Access, &a.SecretWrapped, &a.KekVersion, &a.Role,
		&a.TenantID, &a.TenantSystem, &a.Status)
	return a, err
}

// TenantIsSystem — системный ли тенант (ADR-0018). Отдельный запрос нужен слою
// IAM до вставки: отказ должен быть понятным (AccessDenied), а не SQLSTATE из
// схемы. Схема остаётся второй линией — между проверкой и вставкой флаг тенанта
// мог измениться, и тогда вставку отклонит FK или чек.
func (m *Meta) TenantIsSystem(ctx context.Context, tenantID int64) (bool, error) {
	var system bool
	err := m.Pool.QueryRow(ctx,
		`SELECT system FROM tenants WHERE id = $1 AND deleted_at IS NULL`, tenantID).
		Scan(&system)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, ErrNotFound
	}
	if err != nil {
		return false, fmt.Errorf("tenant %d: %w", tenantID, err)
	}
	return system, nil
}

// GetAccount возвращает строку учётки, включая отозванные: различать «нет» и
// «отозван» решает слой IAM, а не запрос. ErrNotFound — строки нет вовсе.
func (m *Meta) GetAccount(ctx context.Context, access string) (Account, error) {
	a, err := scanAccount(m.Pool.QueryRow(ctx,
		`SELECT `+accountColumns+` FROM accounts WHERE access_key = $1`, access))
	if errors.Is(err, pgx.ErrNoRows) {
		return Account{}, ErrNotFound
	}
	if err != nil {
		return Account{}, fmt.Errorf("get account: %w", err)
	}
	return a, nil
}

// CreateAccount вставляет учётку. Флаг tenant_system читается из tenants здесь
// же и в той же транзакции: составной FK (tenant_id, tenant_system) требует
// точного совпадения с tenants.system, и «посчитать флаг на стороне вызывающего»
// означало бы гонку с UPDATE tenants.
//
// Секрет сюда приходит уже обёрнутым: meta не умеет ни шифровать, ни логировать
// открытые секреты — и не должен уметь.
func (m *Meta) CreateAccount(ctx context.Context, a Account) error {
	return m.inTx(ctx, func(tx pgx.Tx) error {
		var system bool
		err := tx.QueryRow(ctx,
			`SELECT system FROM tenants WHERE id = $1 AND deleted_at IS NULL`, a.TenantID).
			Scan(&system)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("tenant %d: %w", a.TenantID, err)
		}

		_, err = tx.Exec(ctx,
			`INSERT INTO accounts (access_key, secret_wrapped, kek_version, role,
					tenant_id, tenant_system)
			  VALUES ($1, $2, $3, $4, $5, $6)`,
			a.Access, a.SecretWrapped, a.KekVersion, a.Role,
			a.TenantID, system)
		if err != nil {
			if isUniqueViolation(err) {
				return ErrAccountExists
			}
			return fmt.Errorf("insert account: %w", err)
		}
		return notifyAccountChanged(ctx, tx, a.Access)
	})
}

// UpdateAccount меняет только перечисленное в patch. Перенос в другой тенант
// заодно перечитывает tenants.system: колонка денормализована и обязана
// оставаться в согласии с тенантом (миграция 0007).
//
// Строка блокируется FOR UPDATE: без блокировки два параллельных обновления
// прочитали бы один и тот же флаг и один из них записал бы пару
// (tenant_id, tenant_system) вразнобой с реальным tenants.system.
func (m *Meta) UpdateAccount(ctx context.Context, access string, p AccountPatch) error {
	return m.inTx(ctx, func(tx pgx.Tx) error {
		var role string
		var tenantID int64
		err := tx.QueryRow(ctx,
			`SELECT role, tenant_id FROM accounts WHERE access_key = $1 FOR UPDATE`, access).
			Scan(&role, &tenantID)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("lock account: %w", err)
		}

		if p.Role != "" {
			role = p.Role
		}
		if p.TenantID != nil {
			tenantID = *p.TenantID
		}

		// tenant_system пересчитываем всегда, когда менялся тенант, — и только
		// по факту из tenants.
		var tenantSystem *bool
		if p.TenantID != nil {
			var sys bool
			err := tx.QueryRow(ctx,
				`SELECT system FROM tenants WHERE id = $1 AND deleted_at IS NULL`, tenantID).
				Scan(&sys)
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			if err != nil {
				return fmt.Errorf("tenant %d: %w", tenantID, err)
			}
			tenantSystem = &sys
		}

		// Явные приведения типов в COALESCE обязательны: при $2/$6 = NULL
		// планировщик не всегда выводит тип параметра из второй ветки, а ошибка
		// «could not determine data type of parameter» в проде выглядит как
		// поломка БД.
		tag, err := tx.Exec(ctx,
			`UPDATE accounts SET
			    secret_wrapped = COALESCE($2::bytea, secret_wrapped),
			    kek_version    = CASE WHEN $2 IS NULL THEN kek_version ELSE $3 END,
			    role           = $4,
			    tenant_id      = $5,
			    tenant_system  = COALESCE($6::boolean, tenant_system)
			  WHERE access_key = $1`,
			access, p.SecretWrapped, p.KekVersion, role, tenantID, tenantSystem)
		if err != nil {
			return fmt.Errorf("update account: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return ErrNotFound
		}
		return notifyAccountChanged(ctx, tx, access)
	})
}

// DeleteAccount удаляет строку. ErrNotFound, если учётки нет.
//
// Именно удаление, а не status = 'revoked': единственный путь отзыва в
// интерфейсе versitygw — DeleteUserAccount, а отозванная строка должна
// перестать отдавать секрет ровно так же, как удалённая (см. GetAccount и
// MissingAccounts). Разница между ними появится вместе с админ-API (T4), где
// status и нужен для аудита.
func (m *Meta) DeleteAccount(ctx context.Context, access string) error {
	return m.inTx(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `DELETE FROM accounts WHERE access_key = $1`, access)
		if err != nil {
			return fmt.Errorf("delete account: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return ErrNotFound
		}
		return notifyAccountChanged(ctx, tx, access)
	})
}

// ListAccounts — все учётки, включая отозванные: админский вывод обязан
// показывать и их, иначе «ключ отозван» и «ключа никогда не было» снаружи
// неразличимы. Секреты не возвращаются никогда — см. GetAccount.
func (m *Meta) ListAccounts(ctx context.Context) ([]Account, error) {
	rows, err := m.Pool.Query(ctx,
		`SELECT `+accountColumns+` FROM accounts ORDER BY access_key`)
	if err != nil {
		return nil, fmt.Errorf("list accounts: %w", err)
	}
	defer rows.Close()

	var out []Account
	for rows.Next() {
		a, err := scanAccount(rows)
		if err != nil {
			return nil, fmt.Errorf("list accounts: %w", err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// MissingAccounts возвращает те из accessKeyIDs, которых нет среди **активных**
// учёток. Именно этот список ждёт versitygw от ResolveAccounts (auth/iam.go:155)
// при проверке principal'ов политики и ACL.
//
// Отозванный ключ считается отсутствующим намеренно: политика, выданная
// отозванному ключу, не должна приниматься — иначе отзыв можно обойти, заранее
// вписав ключ в чужую политику.
func (m *Meta) MissingAccounts(ctx context.Context, accessKeyIDs []string) ([]string, error) {
	if len(accessKeyIDs) == 0 {
		return nil, nil
	}

	found := make(map[string]bool, len(accessKeyIDs))
	rows, err := m.Pool.Query(ctx,
		`SELECT access_key FROM accounts
		  WHERE access_key = ANY($1) AND status = 'active'`, accessKeyIDs)
	if err != nil {
		return nil, fmt.Errorf("resolve accounts: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("resolve accounts: %w", err)
		}
		found[key] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("resolve accounts: %w", err)
	}

	var missing []string
	for _, key := range accessKeyIDs {
		if !found[key] {
			missing = append(missing, key)
		}
	}
	return missing, nil
}

// isUniqueViolation — 23505 unique_violation. Нужен, чтобы превратить отказ
// PRIMARY KEY в свою ошибку, а не в «внутренняя ошибка БД».
func isUniqueViolation(err error) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && pe.Code == "23505"
}
