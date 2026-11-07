package hack_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestStandStatusDSSFailClosed — шаг 0 плана среза: проверки DSS обязаны быть
// закрыты при сбое. Раньше psql с ошибкой давал пустой вывод, а пустой вывод
// читался как «проблем нет» — неверный DSN делал проверку зелёной, то есть
// зелёным, который ничего не проверил.
//
// Тест не трогает стенд: конфиг Phobos подсовывается временный (PHOBOS_CONF),
// с недостижимым DSN и без него. Остальные проверки скрипта на дев-машине
// могут краснеть сами по себе — тест ищет конкретные строки про непроверенный
// DSS и ненулевой код возврата.
func TestStandStatusDSSFailClosed(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller не вернул путь")
	}
	script := filepath.Join(filepath.Dir(thisFile), "stand-status")

	cases := []struct {
		name  string
		conf  string
		wants []string
	}{
		{
			// psql не отвечает: DSS недоступен — обе проверки обязаны краснеть.
			name:  "недостижимый DSN",
			conf:  "port = 1\n[dss]\nconnect_string = host=127.0.0.1 port=1 connect_timeout=1\n",
			wants: []string{"состояние устройств не проверено", "возможность записи не проверена"},
		},
		{
			// connect_string вообще не прочитан — это тоже «не проверено»,
			// а не «проверено и пусто».
			name:  "connect_string отсутствует",
			conf:  "port = 1\n",
			wants: []string{"состояние устройств не проверено", "возможность записи не проверена"},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			conf := filepath.Join(t.TempDir(), "phobos.conf")
			if err := os.WriteFile(conf, []byte(tt.conf), 0o600); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command("bash", script)
			cmd.Env = append(os.Environ(), "PHOBOS_CONF="+conf)
			out, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatalf("скрипт завершился успешно, хотя DSS не проверен:\n%s", out)
			}
			// Строки ищем построчно: «FAIL» где-то в выводе и текст где-то
			// в выводе — не одно и то же же, что FAIL про непроверенный DSS.
			for _, want := range tt.wants {
				found := false
				for _, line := range strings.Split(string(out), "\n") {
					if strings.HasPrefix(line, "FAIL:") && strings.Contains(line, want) {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("в выводе нет FAIL-строки про %q:\n%s", want, out)
				}
			}
		})
	}
}

// TestStandStatusPSQLWarningNotData проверяет, что stderr psql не смешивается
// с результатом запроса: предупреждение при успешном запросе не должно
// превратиться в ложное «устройство в failed: WARNING ...». Фейковый psql
// пишет предупреждение в stderr, а данные — в stdout; раньше 2>&1 сливал их,
// и первая строка «результата» была ложной.
func TestStandStatusPSQLWarningNotData(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller не вернул путь")
	}
	script := filepath.Join(filepath.Dir(thisFile), "stand-status")

	bin := t.TempDir()
	fake := filepath.Join(bin, "psql")
	// Игнорируем аргументы: важен сам способ запуска. Код 0 — запрос «успешен»,
	// stdout — одна строка результата, stderr — предупреждение.
	if err := os.WriteFile(fake, []byte("#!/usr/bin/env bash\necho 'WARNING: разряд базы близко' >&2\necho 'tape 7'\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	conf := filepath.Join(t.TempDir(), "phobos.conf")
	if err := os.WriteFile(conf, []byte("port = 1\n[dss]\nconnect_string = host=ignored\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command("bash", script)
	cmd.Env = append(os.Environ(), "PHOBOS_CONF="+conf, "PATH="+bin+":"+os.Getenv("PATH"))
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("фейковый psql вернул устройство в failed — скрипт обязан краснеть:\n%s", out)
	}
	got := string(out)
	if !strings.Contains(got, "FAIL: устройство в failed: tape 7") {
		t.Errorf("результат запроса дошёл не как данные:\n%s", got)
	}
	if strings.Contains(got, "failed: WARNING") || strings.Contains(got, "не проверено: WARNING") {
		t.Errorf("stderr psql смешался с результатом или с текстом ошибки:\n%s", got)
	}
}
