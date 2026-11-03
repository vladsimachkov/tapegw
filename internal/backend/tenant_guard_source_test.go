package backend_test

// В. «Обходной» тенант не должен разъехаться по коду.
//
// meta.AllTenantsForGuard отключает фильтр тенанта целиком. Позвать её из
// обычного метода — значит выключить изоляцию без единой ошибки компиляции,
// поэтому ADR-0017 (п. 3а) требует, чтобы пользовалась ею ровно одна точка:
// файл стража.
//
// Проверка читает исходники (go/parser), а не положится на договорённость в
// комментарии: новый файл, который попробует её позвать, роняет этот тест.
//
// Границы проверки, и они записаны нарочно, чтобы не спорить потом:
//
//   - ищется **вызов**, а не любое упоминание. Определение функции обязано
//     жить в meta — там же живёт тип TenantID, и требовать «ни одного упоминания
//     за пределами стража» физически невозможно;
//   - `_test.go` не проверяются: тесты слоя meta обязаны вызывать конструктор,
//     чтобы проверить сам тип, а в продакшн они не едут.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const allTenantsFunc = "AllTenantsForGuard"

// guardFile — единственный файл, которому разрешено вызывать обход.
const guardFile = "internal/backend/tenant_guard.go"

func TestAllTenantsForGuardCalledOnlyInGuard(t *testing.T) {
	root := moduleRoot(t)

	fset := token.NewFileSet()
	var offenders []string

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "testdata", "node_modules":
				return fs.SkipDir
			}
			return nil
		}
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		if filepath.ToSlash(rel) == guardFile {
			return nil
		}

		file, parseErr := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if parseErr != nil {
			// Нераспарсившийся файл — не дело этого теста, но и молчать
			// нельзя: иначе проверка могла бы «не заметить» обход.
			t.Errorf("не распарсился %s: %v", rel, parseErr)
			return nil
		}

		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch fn := ast.Unparen(call.Fun).(type) {
			case *ast.Ident: // вызов из того же пакета meta
				if fn.Name == allTenantsFunc {
					offenders = append(offenders, rel)
				}
			case *ast.SelectorExpr: // meta.AllTenantsForGuard(...)
				if fn.Sel.Name == allTenantsFunc {
					offenders = append(offenders, rel)
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("обход репозитория: %v", err)
	}

	if len(offenders) > 0 {
		t.Errorf("%s вызывается вне %s: %v. Обход фильтра тенанта разрешён только стражу "+
			"(ADR-0017, п. 3а): любой другой вызов выключает изоляцию без единой ошибки компиляции.",
			allTenantsFunc, guardFile, offenders)
	}
}

// moduleRoot — каталог с go.mod над тестом. Тест идёт из internal/backend,
// репозиторий ищем вверх, а не считаем число «..»: путь к репозиторию у всех
// разный.
func moduleRoot(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("os.Getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod не найден выше каталога теста")
		}
		dir = parent
	}
}
