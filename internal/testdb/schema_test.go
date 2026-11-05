package testdb

import (
	"strings"
	"testing"
)

// Имя схемы — не только удобство: на него опирается guard в hack/mutate.sh
// (разрешены только схемы вида tapegw_test_*) и разведение прогонов по своим
// схемам. Поэтому проверяется и формат, и то, что префикс не ломается.
func TestPackageSchemaNamespace(t *testing.T) {
	t.Setenv("TAPEGW_TEST_SCHEMA_NS", "")
	base := PackageSchema(t)
	if !strings.HasPrefix(base, "tapegw_test_") {
		t.Fatalf("имя схемы %q не начинается с tapegw_test_ — guard в mutate.sh перестанет её узнавать", base)
	}

	t.Setenv("TAPEGW_TEST_SCHEMA_NS", "mut")
	got := PackageSchema(t)
	if want := "tapegw_test_mut_" + strings.TrimPrefix(base, "tapegw_test_"); got != want {
		t.Errorf("с ns=mut схема %q, ждали %q", got, want)
	}
	if got == base {
		t.Error("переменная окружения не меняет схему: прогоны по-прежнему делят одну")
	}

	// Мусор из окружения не должен уезжать в DSN и в SQL как есть.
	t.Setenv("TAPEGW_TEST_SCHEMA_NS", "mut; DROP TABLE media")
	got = PackageSchema(t)
	if strings.ContainsAny(got, "; \"' ") {
		t.Errorf("в имя схемы попала посторонняя символика: %q", got)
	}
	if !strings.HasPrefix(got, "tapegw_test_") {
		t.Errorf("после санитайза имя вышло за разрешённый формат: %q", got)
	}
}
