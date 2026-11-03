package meta

import "fmt"

// TenantID — идентификатор тенанта, обязательная часть контракта любой функции
// meta, которая работает с бакетом (ADR-0017).
//
// Это структура с неэкспортируемым полем, а не int64, и это не украшение:
// значение, отключающее фильтр тенанта, опасно само по себе. Пока его нельзя
// создать иначе как через AllTenantsForGuard, подделать обход из чужого пакета
// невозможно — останется только позвать саму функцию, а её вызов проверяется
// тестом (см. internal/backend/tenant_guard.go).
//
// Нулевое значение TenantID{} не соответствует ни одному тенанту: id тенантов
// начинаются с 1 (миграция 0005), поэтому забытый тенант означает «не найдено»,
// а не «все тенанты сразу».
type TenantID struct {
	id int64
}

const (
	// allTenants — служебное значение «фильтр по тенанту не добавляется».
	// Отрицательное нарочно: id тенантов выдаёт bigserial из 1, так что 0 и
	// ниже не описывают настоящего тенанта и не могут с ним столкнуться.
	allTenants int64 = -1

	// defaultTenantID — тенант `default` из миграции 0005. Единственный в
	// однотенантовой установке.
	defaultTenantID int64 = 1
)

// DefaultPoolID — общий пул `shared` из миграции 0005. По ADR-0013 каждая
// запись обязана нести тег пула, поэтому пул указывается даже в
// однотенантовой установке, а не оставляется «без пула».
const DefaultPoolID int64 = 1

// NewTenant делает тенанта из Account.ProjectID — именно там учётку тенанта
// хранит versitygw (auth/iam.go:74, tenants.id за bigint по той же причине).
//
// Non-positive значения — не тенант: у анонимной учётки ProjectID == 0, а
// отрицательные приехали бы из испорченного IAM. Возвращаем нулевой TenantID,
// который не находит ни одной строки, а не обход фильтра: иначе
// NewTenant(-1) случайно совпал бы с allTenants и молча выключил изоляцию.
func NewTenant(projectID int) TenantID {
	if projectID <= 0 {
		return TenantID{}
	}
	return TenantID{id: int64(projectID)}
}

// AllTenantsForGuard — «не фильтровать по тенанту». Вызывать её имеет право
// только страж (internal/backend/tenant_guard.go), и это ограничение
// проверяется тестом, который читает исходники, а не устной договорённостью.
func AllTenantsForGuard() TenantID {
	return TenantID{id: allTenants}
}

// AdminTenantID — тенант, которому назначаются бакеты, созданные учёткой с
// обходом по роли. Root приходит с RoleAdmin, но ProjectID у него не заполнен
// (versitygw собирает rootAccount без него), поэтому «взять тенанта из учётки»
// для него означает «тенанта нет», а не «все тенанты»: бакет обязан принадлежать
// одному тенанту. До админского вывода тенантов это `default`.
func AdminTenantID() TenantID {
	return TenantID{id: defaultTenantID}
}

// IsBypass — фильтр по тенанту отключён.
func (t TenantID) IsBypass() bool { return t.id == allTenants }

// arg — значение для подстановки в SQL. Вызывать только когда IsBypass() ==
// false: подстановочное allTenants не нашло бы ни одной строки, а вместо
// честного отказа мы получили бы пустую выборку там, где её быть не должно.
func (t TenantID) arg() int64 { return t.id }

// withTenant дописывает к статическому запросу предикат тенанта и возвращает
// готовый список аргументов.
//
// Для обхода по роли предикат не добавляется **вовсе**. Форма вида
// `($2 = -1 OR tenant_id = $2)` выглядит эквивалентом, но лишает планировщик
// возможности использовать индекс по tenant_id — а он для учёта и очередей
// именно по этой колонке и построен.
//
// base обязан заканчиваться последним условием WHERE: предикат дописывается в
// конец. Колонка в нём без квалификатора — там, где запрос с JOIN, нужен
// withTenantCol.
func withTenant(base string, t TenantID, args ...any) (string, []any) {
	return withTenantCol(base, "tenant_id", t, args...)
}

// withTenantCol — то же, что withTenant, но с квалифицированным именем колонки
// (`b.tenant_id`), когда запрос соединяет таблицы.
func withTenantCol(base, col string, t TenantID, args ...any) (string, []any) {
	if t.IsBypass() {
		return base, args
	}
	return fmt.Sprintf("%s AND %s = $%d", base, col, len(args)+1), append(args, t.arg())
}
