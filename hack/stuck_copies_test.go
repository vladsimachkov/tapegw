package hack_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestStandStatusStuckCopies — наблюдаемость Ф1.5: stand-status обязан
// показывать число копий, застрявших в 'writing' из-за неизвестного носителя
// (без строки media копия не выходит из 'writing' никогда, а сигналом была
// только ERROR-строка в логе). Проверка закрыта при сбое: без TAPEGW_DSN или
// без ответа psql состояние копий неизвестно — это красный «не проверено»,
// а не зелёное молчание.
func TestStandStatusStuckCopies(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller не вернул путь")
	}
	script := filepath.Join(filepath.Dir(thisFile), "stand-status")
	conf := filepath.Join(t.TempDir(), "phobos.conf")
	if err := os.WriteFile(conf, []byte("port = 1\n[dss]\nconnect_string = host=ignored\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	// Фейковый psql: на запрос про копии отвечает тем, что лежит в файле
	// answer (или падает, если файла нет), остальные запросы — пусто, то есть
	// «проблем нет». Так проверка видит именно поведение секции про копии.
	fakePSQL := func(t *testing.T, body string) string {
		t.Helper()
		bin := t.TempDir()
		fake := filepath.Join(bin, "psql")
		if err := os.WriteFile(fake, []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
		return bin
	}
	silent := "#!/usr/bin/env bash\nexit 0\n"
	answers := func(n string) string {
		return "#!/usr/bin/env bash\n" +
			"sql=\"\"; prev=\"\"\n" +
			"for a in \"$@\"; do [ \"$prev\" = \"-c\" ] && sql=\"$a\"; prev=\"$a\"; done\n" +
			"case \"$sql\" in\n" +
			"  *archive_copies*) echo '" + n + "' ;;\n" +
			"esac\n"
	}

	cases := []struct {
		name   string
		dsn    string // пустой — TAPEGW_DSN не задан
		psql   string
		want   string
		wantOK bool // искать ok-строку, а не FAIL
	}{
		{
			name: "TAPEGW_DSN не задан",
			dsn:  "",
			psql: silent,
			want: "TAPEGW_DSN не задан — состояние копий не проверено",
		},
		{
			name: "база tapegw не отвечает",
			dsn:  "postgres://nope@127.0.0.1:1/nope",
			psql: "#!/usr/bin/env bash\necho 'psql: error: connection failed' >&2\nexit 2\n",
			want: "база tapegw недоступна, состояние копий не проверено",
		},
		{
			name: "копии застряли",
			dsn:  "postgres://ignored",
			psql: answers("2"),
			want: "копий в 'writing' с нераспознанным носителем: 2",
		},
		{
			name:   "копий нет",
			dsn:    "postgres://ignored",
			psql:   answers("0"),
			want:   "нет копий, застрявших в 'writing' из-за неизвестного носителя",
			wantOK: true,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			bin := fakePSQL(t, tt.psql)
			cmd := exec.Command("bash", script)
			env := []string{"PHOBOS_CONF=" + conf, "PATH=" + bin + ":" + os.Getenv("PATH")}
			if tt.dsn != "" {
				env = append(env, "TAPEGW_DSN="+tt.dsn)
			}
			cmd.Env = append(os.Environ(), env...)
			out, _ := cmd.CombinedOutput() // rc не показателен: остальные секции краснеют сами
			prefix := "FAIL:"
			if tt.wantOK {
				prefix = "ok:"
			}
			found := false
			for _, line := range strings.Split(string(out), "\n") {
				if strings.HasPrefix(line, prefix) && strings.Contains(line, tt.want) {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("в выводе нет строки %s %q:\n%s", prefix, tt.want, out)
			}
		})
	}
}
