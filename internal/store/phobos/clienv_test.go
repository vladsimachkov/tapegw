//go:build phobos

package phobos

import (
	"strings"
	"testing"
)

// TestCLIEnvPassesConfig — CLI обязан получать тот же конфиг, что и libphobos.
// Иначе «записали одним конфигом, а extent list ищет другим»: объект есть,
// а носитель по нему пустой.
func TestCLIEnvPassesConfig(t *testing.T) {
	t.Setenv("PHOBOS_CFG_FILE", "/etc/phobos.conf")

	with := cliEnv("/etc/phobos.alt.conf")
	n := 0
	for _, kv := range with {
		if strings.HasPrefix(kv, "PHOBOS_CFG_FILE=") {
			n++
			if kv != "PHOBOS_CFG_FILE=/etc/phobos.alt.conf" {
				t.Errorf("окружение CLI: %q — должен быть передан явный путь", kv)
			}
		}
	}
	if n != 1 {
		t.Errorf("PHOBOS_CFG_FILE встречается %d раз, должен ровно один (наследованный замещается, а не дублируется)", n)
	}

	plain := cliEnv("")
	for _, kv := range plain {
		if strings.HasPrefix(kv, "PHOBOS_CFG_FILE=") {
			t.Errorf("без явного конфига переменная не должна попадать в окружение: %q", kv)
		}
	}
}
