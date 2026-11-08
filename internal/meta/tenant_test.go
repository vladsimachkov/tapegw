package meta

// Тесты на сам тип TenantID. Отдельный файл и белая коробка нарочно: проверять
// нужно не SQL, а то, что обход фильтра нельзя подделать и что нулевое значение
// не означает «все тенанты».

import (
	"errors"
	"testing"
)

func TestTenantIDBypassIsNotForgeable(t *testing.T) {
	// Единственный способ получить обход — явная функция стража.
	if !AllTenantsForGuard().IsBypass() {
		t.Fatal("AllTenantsForGuard обязан давать обход")
	}

	// Обычные тенанты обходом быть не могут, включая числа, которые кто-то
	// мог бы передать вместо id: 0 — аноним, -1 — то самое служебное значение,
	// и оно наружу не проходит.
	for _, projectID := range []int{0, -1, -42} {
		if got := NewTenant(projectID); got.IsBypass() {
			t.Errorf("NewTenant(%d) дал обход — изоляцию можно подделать аргументом", projectID)
		}
	}

	// Нулевое значение — «ни одного тенанта», а не «все сразу».
	if (TenantID{}).IsBypass() {
		t.Error("нулевой TenantID не обязан отключать фильтр")
	}
	if (TenantID{}).arg() == allTenants {
		t.Error("нулевой TenantID не должен совпадать со служебным значением обхода")
	}
}

// insertArg — значение для записи в tenant_id. Служебное «все тенанты» годится
// как фильтр, но не как значение: запись -1 разбила бы вставку о FK (23503), и
// клиент получил бы 500 вместо честного отказа (находка ревью PR #127).
func TestTenantIDInsertArgRefusesSentinels(t *testing.T) {
	if _, err := AllTenantsForGuard().insertArg(); !errors.Is(err, ErrTenantNotWritable) {
		t.Errorf("insertArg для обхода: %v, ждали ErrTenantNotWritable", err)
	}
	// Нулевое значение — тоже не тенант: id тенантов начинаются с 1.
	if _, err := (TenantID{}).insertArg(); !errors.Is(err, ErrTenantNotWritable) {
		t.Errorf("insertArg для нулевого значения: %v, ждали ErrTenantNotWritable", err)
	}
	if v, err := NewTenant(2).insertArg(); err != nil || v != 2 {
		t.Errorf("insertArg для тенанта 2: %d, %v — настоящий тенант отвергнут", v, err)
	}
}

func TestWithTenantAddsPredicateOnlyForRealTenant(t *testing.T) {
	q, args := withTenant(`SELECT 1 FROM buckets WHERE name = $1`, NewTenant(7), "b")
	if q != `SELECT 1 FROM buckets WHERE name = $1 AND tenant_id = $2` {
		t.Errorf("предикат собрался неверно: %s", q)
	}
	if len(args) != 2 || args[1] != int64(7) {
		t.Errorf("аргументы собрались неверно: %v", args)
	}

	// Обход: предикат не добавляется вовсе, а не превращается в «всегда истина» —
	// иначе планировщик теряет индекс по tenant_id.
	q, args = withTenant(`SELECT 1 FROM buckets WHERE name = $1`, AllTenantsForGuard(), "b")
	if q != `SELECT 1 FROM buckets WHERE name = $1` {
		t.Errorf("для обхода предикат добавлять нельзя: %s", q)
	}
	if len(args) != 1 {
		t.Errorf("для обхода лишний аргумент не подставляется: %v", args)
	}

	// Квалифицированная колонка нужна запросам с JOIN.
	q, _ = withTenantCol(`SELECT 1 FROM objects o JOIN buckets b ON b.id = o.bucket_id WHERE o.key = $1`,
		"b.tenant_id", NewTenant(2), "k")
	if q != `SELECT 1 FROM objects o JOIN buckets b ON b.id = o.bucket_id WHERE o.key = $1 AND b.tenant_id = $2` {
		t.Errorf("квалифицированный предикат собрался неверно: %s", q)
	}
}
