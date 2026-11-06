package meta_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Механизм, а не договорённость: тесты миграций обязаны брать одноразовую схему.
//
// Причина не в стиле. Каждый тест миграций снимает и применяет чужие миграции;
// в общей схеме пакета следующий тест зависит от того, что предыдущий успел
// вернуть, и каждая новая миграция добавляет дыру, которую надо помнить закрыть
// во всех предыдущих тестах. Так ломалось дважды (колонки 0021, затем 0022), и
// каждый чинили точечно — то есть правило держалось на памяти автора теста.
//
// Проверка по исходникам, а не по поведению: «быстро снять одну миграцию в
// общей схеме» выглядит невинно и всплывёт только в другом тесте.
func TestMigrationTestsUseScratchSchema(t *testing.T) {
	files, err := filepath.Glob("migration_0*_test.go")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("не найдено ни одного теста миграций — проверка стала пустой")
	}

	// Что запрещено в этих файлах: общая схема пакета и всё, что с ней связано.
	banned := map[string]string{
		"Connect":       "тесты миграций не должны брать общую схему пакета",
		"ConnectSchema": "тесты миграций не должны брать общую схему пакета",
		"DropSchema":    "схему одноразового прогона удаляет MigrationScratch",
		"Clean":         "одноразовая схема чистая по построению",
	}

	fset := token.NewFileSet()
	for _, name := range files {
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("читать %s: %v", name, err)
		}
		f, err := parser.ParseFile(fset, name, src, 0)
		if err != nil {
			t.Fatalf("разобрать %s: %v", name, err)
		}

		usesScratch := false
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			pkg, ok := sel.X.(*ast.Ident)
			if !ok || pkg.Name != "testdb" {
				return true
			}
			switch sel.Sel.Name {
			case "MigrationScratch":
				usesScratch = true
			default:
				if why, bad := banned[sel.Sel.Name]; bad {
					t.Errorf("%s: testdb.%s запрещён — %s", name, sel.Sel.Name, why)
				}
			}
			return true
		})

		if !usesScratch {
			t.Errorf("%s: ни одного testdb.MigrationScratch — тест, скорее всего, работает с общей схемой пакета", name)
		}
		if strings.Contains(string(src), "testdb.MigrationScratch(t, 0)") {
			// Пустая схема законна (миграция 0001), но упомянем явно: если
			// появится тест с нулём и без Up — он ничего не проверяет.
			t.Logf("%s: разворот с пустой схемы", name)
		}
	}
}
