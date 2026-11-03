package iam

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/s3err"

	"github.com/vladsimachkov/tapegw/internal/meta"
)

// Options — настройки сервиса.
type Options struct {
	// RootAccess — access key учётки root из конфигурации (root.access). Root
	// существует вне таблицы accounts: versitygw собирает его сам
	// (s3api/middlewares/authentication.go:44) и не спрашивает IAM для
	// подписи его запросов (auth.ResolveDerivedKey). Знать ключ root нужно,
	// чтобы **не** отдавать его из accounts: строка с тем же access_key — это
	// попытка подменить учётку, у которой обход всех проверок.
	RootAccess string
}

// Service — auth.IAMService поверх Postgres.
//
// Нарочно реализует ровно семь методов интерфейса и ничего больше: у
// versitygw есть необязательные интерфейсы (PolicyEvaluator, FixedBucketOwner,
// SigningKeyProvider), и любой из них молча меняет модель доступа. Например,
// PolicyEvaluator отключил бы обход по роли userplus для CreateBucket
// (auth/access-control.go:597), а FixedBucketOwner отдал бы все бакеты одному
// владельцу. Отсутствие лишнего здесь — тоже решение, а не недоделка.
type Service struct {
	meta       *meta.Meta
	keys       *KeyRing
	rootAccess string
}

var _ auth.IAMService = (*Service)(nil)

// New собирает сервис. meta и keys обязательны: без них нечем разворачивать
// секреты, а «работает, но всегда отказывает» — худший из возможных отказов.
func New(m *meta.Meta, keys *KeyRing, opts Options) (*Service, error) {
	if m == nil {
		return nil, errors.New("iam: meta обязателен")
	}
	if keys == nil {
		return nil, errors.New("iam: кольцо ключей KEK обязательно")
	}
	if opts.RootAccess == "" {
		return nil, errors.New("iam: root.access обязателен")
	}
	return &Service{meta: m, keys: keys, rootAccess: opts.RootAccess}, nil
}

// GetUserAccount — единственный метод, который вызывается на **каждом**
// запросе (ADR-0016), поэтому он не делает ничего лишнего: один SELECT и
// расшифровка.
//
// Ошибка — auth.ErrNoSuchUser и для «строки нет», и для отозванной учётки, и
// для ключа root: versitygw превращает её в InvalidAccessKeyId
// (middlewares/authentication.go:97), и различать эти случаи наружу нельзя —
// «отозван» и «не существует» для постороннего один ответ.
func (s *Service) GetUserAccount(access string) (auth.Account, error) {
	ctx := context.Background()

	if access == s.rootAccess {
		return auth.Account{}, auth.ErrNoSuchUser
	}

	row, err := s.meta.GetAccount(ctx, access)
	if errors.Is(err, meta.ErrNotFound) {
		return auth.Account{}, auth.ErrNoSuchUser
	}
	if err != nil {
		return auth.Account{}, fmt.Errorf("iam: учётка %q: %w", access, err)
	}
	if row.Status != meta.AccountActive {
		return auth.Account{}, auth.ErrNoSuchUser
	}

	secret, err := s.keys.Open(row.SecretWrapped, row.KekVersion, []byte(access))
	if err != nil {
		// Ни байта из обёртки или секрета в тексте: ошибка уходит в лог
		// versitygw (debuglogger.InternalError) как есть.
		return auth.Account{}, fmt.Errorf("iam: учётка %q: %w", access, err)
	}

	tenant, err := projectID(row.TenantID)
	if err != nil {
		return auth.Account{}, fmt.Errorf("iam: учётка %q: %w", access, err)
	}

	return auth.Account{
		Access: access,
		Secret: string(secret),
		Role:   auth.Role(row.Role),
		// ProjectID и есть тенант (ADR-0009): бэкенд берёт из него
		// meta.NewTenant без обращения в БД.
		ProjectID: tenant,
	}, nil
}

// CreateAccount заводит учётку. Секрет приходит открытым (так его присылает
// админ-API) и здесь же заворачивается: в БД он уходит только обёрнутым.
func (s *Service) CreateAccount(acc auth.Account) error {
	ctx := context.Background()

	if acc.Access == "" {
		return errors.New("iam: access_key обязателен")
	}
	if acc.Secret == "" {
		return errors.New("iam: секрет обязателен")
	}
	if !acc.Role.IsValid() {
		return s3err.GetAPIError(s3err.ErrAdminInvalidUserRole)
	}
	// Файловый IAM versitygw тоже отвечает ErrUserExists на попытку завести
	// дубликат root (auth/iam_internal.go:69). Держим тот же ответ: учётка с
	// ключом root — это попытка получить обход проверок, не трогая конфиг.
	if acc.Access == s.rootAccess {
		return auth.ErrUserExists
	}
	// POSIX uid/gid у tapegw нет: бакеты и ленты не chown-ятся, колонок в
	// accounts тоже нет. Принять и проигнорировать — значит соврать
	// оператору, что идентификатор применился.
	if acc.UserID != 0 || acc.GroupID != 0 {
		return fmt.Errorf("iam: userID/groupID не поддерживаются: %d/%d", acc.UserID, acc.GroupID)
	}
	if acc.ProjectID <= 0 {
		return errNoTenant(acc.Access)
	}

	tenant, err := s.tenantSystem(ctx, int64(acc.ProjectID))
	if err != nil {
		return err
	}
	if err := checkRoleTenant(acc.Role, tenant); err != nil {
		return err
	}

	wrapped, kekVersion, err := s.keys.Seal([]byte(acc.Secret), []byte(acc.Access))
	if err != nil {
		return fmt.Errorf("iam: завернуть секрет %q: %w", acc.Access, err)
	}

	err = s.meta.CreateAccount(ctx, meta.Account{
		Access:        acc.Access,
		SecretWrapped: wrapped,
		KekVersion:    kekVersion,
		Role:          string(acc.Role),
		TenantID:      int64(acc.ProjectID),
	})
	if errors.Is(err, meta.ErrAccountExists) {
		return auth.ErrUserExists
	}
	if errors.Is(err, meta.ErrNotFound) {
		// Тенант исчез между проверкой и вставкой (или его мягко удалили).
		return errNoTenant(acc.Access)
	}
	if err != nil {
		return fmt.Errorf("iam: создать учётку %q: %w", acc.Access, err)
	}
	return nil
}

// UpdateUserAccount меняет только переданные поля (auth.MutableProps).
//
// Секрет при любом изменении заворачивается активной версией KEK, а не старой:
// иначе ротация KEK никогда не продвинулась бы — строки вечно ссылались бы на
// устаревшие версии.
func (s *Service) UpdateUserAccount(access string, props auth.MutableProps) error {
	ctx := context.Background()

	if access == s.rootAccess {
		return auth.ErrNoSuchUser
	}
	if err := props.Validate(); err != nil {
		return err
	}
	if props.UserID != nil || props.GroupID != nil {
		return fmt.Errorf("iam: userID/groupID не поддерживаются: %v/%v", props.UserID, props.GroupID)
	}

	row, err := s.meta.GetAccount(ctx, access)
	if errors.Is(err, meta.ErrNotFound) {
		return auth.ErrNoSuchUser
	}
	if err != nil {
		return fmt.Errorf("iam: учётка %q: %w", access, err)
	}

	patch := meta.AccountPatch{}
	if props.Secret != nil {
		if *props.Secret == "" {
			return errors.New("iam: пустой секрет")
		}
		wrapped, kekVersion, err := s.keys.Seal([]byte(*props.Secret), []byte(access))
		if err != nil {
			return fmt.Errorf("iam: завернуть секрет %q: %w", access, err)
		}
		patch.SecretWrapped = wrapped
		patch.KekVersion = kekVersion
	}

	role := row.Role
	if props.Role != "" {
		if !props.Role.IsValid() {
			return s3err.GetAPIError(s3err.ErrAdminInvalidUserRole)
		}
		role = string(props.Role)
		patch.Role = role
	}

	tenant := row.TenantID
	if props.ProjectID != nil {
		if *props.ProjectID <= 0 {
			return errNoTenant(access)
		}
		tenant = int64(*props.ProjectID)
		patch.TenantID = &tenant
	}

	// Правило «роль ↔ тенант» проверяется по **итоговой** паре: смена роли или
	// перенос в другой тенант могут нарушить его, не меняя самого способа
	// записи.
	sysErr := s.checkTenantForRole(ctx, role, tenant)
	if sysErr != nil {
		return sysErr
	}

	if err := s.meta.UpdateAccount(ctx, access, patch); err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			return auth.ErrNoSuchUser
		}
		return fmt.Errorf("iam: обновить учётку %q: %w", access, err)
	}
	return nil
}

// DeleteUserAccount удаляет учётку. Для root — ErrNoSuchUser: root не живёт в
// accounts и удалить его отсюда нельзя в принципе.
func (s *Service) DeleteUserAccount(access string) error {
	ctx := context.Background()
	if access == s.rootAccess {
		return auth.ErrNoSuchUser
	}
	if err := s.meta.DeleteAccount(ctx, access); errors.Is(err, meta.ErrNotFound) {
		return auth.ErrNoSuchUser
	} else if err != nil {
		return fmt.Errorf("iam: удалить учётку %q: %w", access, err)
	}
	return nil
}

// ListUserAccounts перечисляет учётки **без секретов**.
//
// Файловый IAM versitygw возвращает учётки с секретами, и админский list-users
// отдаёт их клиенту. Здесь секреты не отдаются: для перечисления их не нужно
// расшифровывать, а расшифровать N секретов ради ответа «какие ключи есть» —
// это лишняя поверхность утечки. Ни один путь проверки доступа не использует
// ListUserAccounts: аутентификация идёт через GetUserAccount.
func (s *Service) ListUserAccounts() ([]auth.Account, error) {
	ctx := context.Background()

	rows, err := s.meta.ListAccounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("iam: список учёток: %w", err)
	}

	out := make([]auth.Account, 0, len(rows))
	for _, row := range rows {
		tenant, err := projectID(row.TenantID)
		if err != nil {
			return nil, fmt.Errorf("iam: учётка %q: %w", row.Access, err)
		}
		out = append(out, auth.Account{
			Access:    row.Access,
			Role:      auth.Role(row.Role),
			ProjectID: tenant,
			// Secret нарочно пустой — см. шапку метода.
		})
	}
	return out, nil
}

// ResolveAccounts возвращает те ключи, которых нет среди активных учёток —
// именно этот список ждут проверки политик и ACL (auth/iam.go:121).
//
// Root исключается до запроса: его нет в accounts, но он существует как
// учётка из конфига, и владелец бакета root в ACL обязан опознаваться — иначе
// любой ACL с грантом владельцу падал бы «accounts does not exist».
func (s *Service) ResolveAccounts(accessKeyIDs []string) ([]string, error) {
	ctx := context.Background()

	check := make([]string, 0, len(accessKeyIDs))
	for _, access := range accessKeyIDs {
		if access == s.rootAccess {
			continue
		}
		check = append(check, access)
	}
	if len(check) == 0 {
		return nil, nil
	}

	missing, err := s.meta.MissingAccounts(ctx, check)
	if err != nil {
		return nil, fmt.Errorf("iam: разрешить учётки: %w", err)
	}
	return missing, nil
}

// Shutdown вызывается versitygw при остановке. Пул закрывает тот, кто его
// открыл (cmd/tapegw), здесь закрывать нечего.
func (s *Service) Shutdown() error { return nil }

// tenantSystem — признак системности тенанта с уже готовым отказом «тенанта
// нет». Отдельно от meta.TenantIsSystem только ради этого текста ошибки.
func (s *Service) tenantSystem(ctx context.Context, tenantID int64) (bool, error) {
	system, err := s.meta.TenantIsSystem(ctx, tenantID)
	if errors.Is(err, meta.ErrNotFound) {
		return false, errNoTenant(strconv.FormatInt(tenantID, 10))
	}
	if err != nil {
		return false, fmt.Errorf("iam: тенант %d: %w", tenantID, err)
	}
	return system, nil
}

// checkTenantForRole — то же правило, но по access/tenant из учётки, когда
// тенант ещё нужно прочитать.
func (s *Service) checkTenantForRole(ctx context.Context, role string, tenantID int64) error {
	system, err := s.tenantSystem(ctx, tenantID)
	if err != nil {
		return err
	}
	return checkRoleTenant(auth.Role(role), system)
}

// checkRoleTenant — правило ADR-0018 и ADR-0009 в одном месте, двумя сторонами:
//
//   - не-admin не может принадлежать системному тенанту: иначе клиент
//     получает бакеты, которые создал root (тенант 1 — они все его);
//   - admin не может принадлежать обычному тенанту: роль обходит проверки
//     политик целиком (authorizationApplies, auth/access-control.go:266),
//     поэтому выдать её внутри тенанта клиента — значит завести там
//     неуязвимого для политик пользователя.
//
// Отказ — s3err.APIError **без обёртки**: ProcessController разбирает ошибки
// контроллеров типом (err.(s3err.S3Error), s3api/controllers/base.go:314), и
// завёрнутый в fmt.Errorf "%w" APIError превратился бы в 500 вместо 403.
// Схема (миграция 0007) — вторая линия: она ловит ручную правку таблицы и сид,
// а человек заслуживает внятного AccessDenied.
func checkRoleTenant(role auth.Role, tenantSystem bool) error {
	if tenantSystem && role != auth.RoleAdmin {
		e := s3err.GetAPIError(s3err.ErrAccessDenied)
		e.Description = fmt.Sprintf(
			"role %q cannot be assigned to the system tenant: only admin accounts may belong to it",
			string(role))
		return e
	}
	if !tenantSystem && role == auth.RoleAdmin {
		e := s3err.GetAPIError(s3err.ErrAccessDenied)
		e.Description = "role admin cannot be assigned to a tenant account: it bypasses all policy checks"
		return e
	}
	return nil
}

// errNoTenant — «учётка не привязана к существующему тенанту». ADR-0009
// требует fail closed: ключ без тенанта не должен появляться вовсе, а не
// жить с ProjectID = 0, который страж трактует как «тенанта нет».
func errNoTenant(what string) error {
	e := s3err.GetAPIError(s3err.ErrAccessDenied)
	e.Description = fmt.Sprintf(
		"account %q is not bound to an existing tenant: projectID must name a tenant", what)
	return e
}

// projectID перекладывает tenant_id в Account.ProjectID. int у versitygw —
// это int, и на 32-битной сборке bigint туда влезает не всегда; молча
// обрезанное число дало бы учётку в чужом тенанте, поэтому проверка обязательна.
func projectID(tenantID int64) (int, error) {
	if tenantID < 1 || tenantID > math.MaxInt32 {
		return 0, fmt.Errorf("тенант %d не выражается через Account.ProjectID", tenantID)
	}
	return int(tenantID), nil
}
