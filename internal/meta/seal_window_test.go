package meta

import (
	"fmt"
	"strings"
	"testing"
)

// Решение проверки полноты окна (sealWindowCheck) — здесь, а не в seal_test.go,
// потому что оно не про БД: сверка двух чисел и текст ошибки. Отдельная функция
// ради этого файла и заведена.
//
// Проводка — что sealEvents действительно зовёт проверку — покрыта не здесь, а
// мутацией отбора: `AND e.id > $2` → `AND e.id > $2 + 1` в sealNewSQL роняет
// TestSeal* именно по этой проверке (таблица мутаций в ADR-0023). Если убрать
// вызов из sealEvents, покраснеет та мутация, а не этот файл.

func TestSealWindowCheckAcceptsExactWindow(t *testing.T) {
	if err := sealWindowCheck(3, 3, 1, 100, 200); err != nil {
		t.Errorf("окно совпало с выборкой, а проверка покраснела: %v", err)
	}
	// Пустое окно — норма: силер мог не найти событий, и это не ошибка.
	if err := sealWindowCheck(0, 0, 1, 0, 0); err != nil {
		t.Errorf("пустое окно обязано быть годным: %v", err)
	}
}

func TestSealWindowCheckRejectsMismatch(t *testing.T) {
	for _, tc := range []struct {
		name           string
		skipped, taken int64
	}{
		// Событие в окне есть, а во взятом его нет: отбор потерял строку либо
		// она закоммитилась после выборки.
		{"в окне больше, чем взято", 4, 3},
		// Обратно: взято больше, чем окно содержит незапечатанных, — значит
		// окно считали не по тем границам.
		{"в окне меньше, чем взято", 2, 3},
	} {
		err := sealWindowCheck(tc.skipped, int(tc.taken), 7, 100, 200)
		if err == nil {
			t.Errorf("%s: проверка прошла, а обязана краснеть", tc.name)
			continue
		}
		// Сообщение читает человек в логе воркера: тенант, окно и оба числа
		// обязаны в нём быть, иначе по логу не восстановить, что случилось.
		for _, want := range []string{"7", "(100, 200]",
			fmt.Sprint(tc.skipped), fmt.Sprint(tc.taken)} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("%s: в сообщении нет %q: %v", tc.name, want, err)
			}
		}
	}
}
