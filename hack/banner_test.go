package hack_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestStandScriptsPrintBanner проверяет, что стендовые скрипты первой строкой
// говорят, какая версия исполнилась. Стенда не касается: --banner печатает
// строку и выходит.
//
// Смысл именно в том, чтобы это был тест, а не внимательность ревьюера: правка
// по якорю, которого в файле нет, молча не применяется, и «синтаксис ок» про
// отсутствие строки ничего не говорит.
func TestStandScriptsPrintBanner(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller не вернул путь")
	}
	hack := filepath.Dir(thisFile)

	for _, name := range []string{"stand-status", "stand-up"} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(hack, name)
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("нет скрипта %s: %v", path, err)
			}
			out, err := exec.Command("bash", path, "--banner").Output()
			if err != nil {
				t.Fatalf("%s --banner завершился с ошибкой: %v", name, err)
			}
			first, _, _ := strings.Cut(string(out), "\n")
			want := "### " + name + " @ "
			if !strings.HasPrefix(first, want) {
				t.Errorf("первая строка %q, хотим начало %q — баннера нет или он не первый", first, want)
			}
		})
	}
}
