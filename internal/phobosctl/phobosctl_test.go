package phobosctl

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Тесты гоняют Client против скрипта-обманки вместо настоящего phobos:
// проверяется обвязка (аргументы, разбор JSON, «закрыто при сомнении»), а не
// Phobos — его поведение на этих командах зафиксировано в
// docs/ops/tape-library.md и проверено на стенде ci-dir.

// fakePhobos пишет вызовы в log и отвечает тем, что лежит в файлах-ответах:
// list.out — вывод list, add.rc — код возврата add. Файлы создаёт тест.
func fakePhobos(t *testing.T) (bin, dir string) {
	t.Helper()
	dir = t.TempDir()
	bin = filepath.Join(dir, "phobos")
	script := `#!/usr/bin/env bash
echo "$@" >> "` + dir + `/calls.log"
case "$1/$2" in
  dir/list|tape/list) cat "` + dir + `/list.out" 2>/dev/null; exit ${RC_LIST:-0} ;;
  dir/add|tape/add)   exit ${RC_ADD:-0} ;;
  extent/list)        cat "` + dir + `/extents.out" 2>/dev/null; exit ${RC_EXT:-0} ;;
  *) exit 0 ;;
esac
`
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "list.out"), []byte("[]"), 0o600); err != nil {
		t.Fatal(err)
	}
	return bin, dir
}

func calls(t *testing.T, dir string) []string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "calls.log"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSpace(string(b)), "\n")
}

func write(t *testing.T, dir, name, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func client(bin, family string) *Client {
	return &Client{Family: family, Library: "lib1", Bin: bin, Timeout: 10 * time.Second}
}

const oneMedium = `[{"name": "/var/lib/phobos/dir1", "tags": ["pool:shared"], "adm_status": "unlocked",` +
	` "put_access": true, "fs.status": "used", "stats.phys_spc_free": 1000, "stats.phys_spc_used": 500}]`

func TestListParsesMedium(t *testing.T) {
	bin, dir := fakePhobos(t)
	write(t, dir, "list.out", oneMedium)

	m, found, err := client(bin, "dir").List(context.Background(), "/var/lib/phobos/dir1")
	if err != nil || !found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	if m.AdmStatus != "unlocked" || !m.PutAccess || m.FSStatus != "used" ||
		m.FreeBytes != 1000 || m.UsedBytes != 500 || len(m.Tags) != 1 || m.Tags[0] != "pool:shared" {
		t.Errorf("разобрано неверно: %+v", m)
	}
}

func TestListAbsentIsAnswer(t *testing.T) {
	bin, _ := fakePhobos(t)
	// Пустой список — Phobos ответил, носителя нет. Это не сомнение.
	m, found, err := client(bin, "dir").List(context.Background(), "/nope")
	if err != nil || found || m != nil {
		t.Errorf("found=%v err=%v, ждали «нет, без ошибки»", found, err)
	}
}

func TestListFailClosed(t *testing.T) {
	bin, dir := fakePhobos(t)
	// Ненулевой rc — ошибка, а не «носителя нет».
	t.Setenv("RC_LIST", "1")
	if _, _, err := client(bin, "dir").List(context.Background(), "x"); err == nil {
		t.Error("rc=1 обязан быть ошибкой")
	}
	t.Setenv("RC_LIST", "0")
	// Разбираемый JSON без носителя — ответ; неразбираемый — сомнение.
	write(t, dir, "list.out", "<ERROR> database is down")
	if _, _, err := client(bin, "dir").List(context.Background(), "x"); err == nil {
		t.Error("неразбираемый вывод обязан быть ошибкой, а не «носителя нет»")
	}
}

func TestAddArgsDir(t *testing.T) {
	bin, dir := fakePhobos(t)
	if err := client(bin, "dir").Add(context.Background(), "/var/lib/phobos/dir9",
		[]string{"pool:shared", "gen:L8"}, ""); err != nil {
		t.Fatal(err)
	}
	got := strings.Join(calls(t, dir), "\n")
	want := "dir add --unlock -L lib1 -T pool:shared,gen:L8 /var/lib/phobos/dir9"
	if !strings.Contains(got, want) {
		t.Errorf("вызов собрался неверно:\n%s\nждали подстроки %q", got, want)
	}
}

func TestAddArgsTape(t *testing.T) {
	bin, dir := fakePhobos(t)
	if err := client(bin, "tape").Add(context.Background(), "E01001L8",
		[]string{"pool:shared"}, "LTO8"); err != nil {
		t.Fatal(err)
	}
	got := strings.Join(calls(t, dir), "\n")
	want := "tape add --unlock -L lib1 -t LTO8 --fs LTFS -T pool:shared E01001L8"
	if !strings.Contains(got, want) {
		t.Errorf("вызов собрался неверно:\n%s\nждали подстроки %q", got, want)
	}
	if err := client(bin, "tape").Add(context.Background(), "x", nil, ""); err == nil {
		t.Error("tape без типа обязан отказывать")
	}
}

func TestAddExistingFails(t *testing.T) {
	// Повторное добавление Phobos отвергает (rc=17, «File exists») — обвязка
	// обязана это показать, а не проглотить: идемпотентность обеспечивает
	// проверка List до add.
	bin, _ := fakePhobos(t)
	t.Setenv("RC_ADD", "17")
	err := client(bin, "dir").Add(context.Background(), "x", nil, "")
	if err == nil || !strings.Contains(err.Error(), "exit status 17") {
		t.Errorf("повтор add: %v, ждали ошибку с кодом 17", err)
	}
}

func TestUpdateFormatUnlockArgs(t *testing.T) {
	bin, dir := fakePhobos(t)
	c := client(bin, "dir")
	ctx := context.Background()
	if err := c.UpdateTags(ctx, "lbl", []string{"pool:shared", "gen:L8"}); err != nil {
		t.Fatal(err)
	}
	if err := c.Format(ctx, "lbl"); err != nil {
		t.Fatal(err)
	}
	if err := c.Unlock(ctx, "lbl"); err != nil {
		t.Fatal(err)
	}
	got := strings.Join(calls(t, dir), "\n")
	for _, want := range []string{
		"dir update -L lib1 -T pool:shared,gen:L8 lbl",
		"dir format --unlock --fs POSIX lbl",
		"dir unlock lbl",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("нет вызова %q в:\n%s", want, got)
		}
	}
}

func TestExtentsMedia(t *testing.T) {
	bin, dir := fakePhobos(t)
	write(t, dir, "extents.out",
		`[{"oid": "pack/1", "media_name": ["/var/lib/phobos/dir1", "/var/lib/phobos/dir2"]}]`)
	medias, err := client(bin, "dir").ExtentsMedia(context.Background(), "pack/1")
	if err != nil {
		t.Fatal(err)
	}
	if len(medias) != 2 || medias[0] != "/var/lib/phobos/dir1" {
		t.Errorf("разобрано неверно: %v", medias)
	}
	write(t, dir, "extents.out", "not json")
	if _, err := client(bin, "dir").ExtentsMedia(context.Background(), "pack/1"); err == nil {
		t.Error("неразбираемый вывод extents обязан быть ошибкой")
	}
}

func TestValidateRejectsUnknownFamily(t *testing.T) {
	c := &Client{Family: "mtl", Library: "l"}
	if _, _, err := c.List(context.Background(), "x"); err == nil {
		t.Error("неизвестная family обязана отказывать до вызова CLI")
	}
}
