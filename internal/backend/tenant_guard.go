package backend

import (
	"context"

	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/s3err"

	"github.com/vladsimachkov/tapegw/internal/meta"
)

// Страж тенанта (ADR-0017). Отвечает на один вопрос: какому тенанту принадлежит
// то, о чём просит запрос, — и отказывает, если ответ «не тебе».
//
// Учётка берётся из контекста. В сигнатурах backend.Backend учётки нет, но
// versitygw кладёт её в контекст запроса сам, и бэкенду отдаёт именно этот
// контекст (цепочка проверена по исходникам и приведена в ADR-0017):
//
//	middlewares/authentication.go:111  ContextKeyAccount.Set(ctx, account)
//	  → httpctx.Set → ctx.Locals("account", …) → fasthttp.SetUserValue
//	 RequestCtx.Value("account")        → UserValue("account")
//
// Ключ — строка: своего типа у versitygw для бэкенда нет, и читает её бэкенд
// так же, как posix-бэкенд.
//
// Нарочно НЕ используем "bucket-owner": этот ключ versitygw ставит ровно в
// одном месте — админский CreateBucket (s3api/controllers/admin.go:197).
// Обычный путь S3 его не заполняет, и страж на нём означал бы «доступен всем,
// кто пришёл не через админский API».
const accountKey = "account"

// tenantFromContext — единственная точка, где бэкенд узнаёт тенанта.
//
// Порядок проверок важен и он не «сначала нулевой ProjectID»:
//
//  1. учётки в контексте нет → AccessDenied. Такое возможно только если запрос
//     прошёл мимо аутентификации, а значит проверять нечего;
//  2. RoleAdmin → обход. Так root и администраторы проходят и мимо политик
//     versitygw (authorizationApplies, auth/access-control.go:266) — страж
//     обязан держаться того же признака, а не своего сравнения ключей;
//  3. ProjectID == 0 → AccessDenied. Под этот же случай попадает аноним:
//     middlewares/set-default-keys.go:33 кладёт пустой auth.Account{}, у
//     которого ProjectID именно ноль, а не «отсутствует». Отдельной ветки для
//     анонимов не нужно;
//  4. иначе — тенант из учётки.
//
// Пункт 2 обязан идти раньше пункта 3: root собирается без ProjectID
// (authentication.go:44), и проверка нулевого ProjectID до ролевой молча
// отрезала бы весь админский путь.
//
// Ошибка — AccessDenied (403), а не NoSuchBucket: так отвечает AWS на чужой
// бакет, и так же versitygw отвечает на отказ по ACL.
func tenantFromContext(ctx context.Context) (meta.TenantID, error) {
	acct, ok := ctx.Value(accountKey).(auth.Account)
	if !ok {
		return meta.TenantID{}, s3err.GetAPIError(s3err.ErrAccessDenied)
	}
	if acct.Role == auth.RoleAdmin {
		return meta.AllTenantsForGuard(), nil
	}
	if acct.ProjectID == 0 {
		return meta.TenantID{}, s3err.GetAPIError(s3err.ErrAccessDenied)
	}
	return meta.NewTenant(acct.ProjectID), nil
}
