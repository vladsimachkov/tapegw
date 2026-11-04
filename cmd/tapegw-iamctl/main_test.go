package main

import "testing"

func TestNormalizeSecret(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string // "" означает «ошибка»
	}{
		{"обычный", "wJalrXUtnFEMI", "wJalrXUtnFEMI"},
		{"перевод строки от echo", "wJalrXUtnFEMI\n", "wJalrXUtnFEMI"},
		{"CRLF из редактора", "wJalrXUtnFEMI\r\n", "wJalrXUtnFEMI"},
		{"пробел в начале", " wJalr", ""},
		{"пробел в конце", "wJalr ", ""},
		{"таб внутри", "wJal\tr", ""},
		{"только перевод строки", "\n", ""},
		{"пусто", "", ""},
		// Два перевода — уже подозрительно: один снимается, второй остаётся и
		// считается пробельным символом. Молча съедать оба нельзя: секрет
		// «X\n» и секрет «X» — разные байты, а у нас должен быть один смысл.
		{"два перевода", "wJalr\n\n", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := normalizeSecret(tc.in)
			if tc.want == "" {
				if err == nil {
					t.Errorf("приняли %q как %q, хотели отказ", tc.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("отказ на %q: %v", tc.in, err)
			}
			if got != tc.want {
				t.Errorf("нормализация %q = %q, хотели %q", tc.in, got, tc.want)
			}
		})
	}
}

// Флаг и stdin обязаны давать одинаковый результат: иначе клиент, который
// создал ключ одним способом и проверяет другим, получит разные байты.
func TestSecretFlagAndStdinAgree(t *testing.T) {
	const want = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	fromStdin, err := normalizeSecret(want + "\n")
	if err != nil {
		t.Fatalf("stdin: %v", err)
	}
	fromFlag, err := normalizeSecret(want)
	if err != nil {
		t.Fatalf("флаг: %v", err)
	}
	if fromStdin != fromFlag || fromStdin != want {
		t.Errorf("stdin=%q флаг=%q — пути расходятся", fromStdin, fromFlag)
	}
}
