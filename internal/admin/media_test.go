package admin_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/admin"
	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/phobosctl"
	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// Тесты собирают команду целиком: БД из testdb, Phobos — скрипт-обманка.
// Проверяются договорённости Ф1.5: одна операция на два реестра, «закрыто при
// сомнении» (нет подтверждения Phobos — нет строки в реестре), идемпотентный
// повтор, уникальность метки и доводка висящих копий 'writing'.

type fake struct {
	dir string
}

// newFake собирает обманку phobos. Список носителей отдаёт по порядку:
// вызов №N печатает list.N.out, а после их конца — list.out. Так сценарий
// «носителя нет → add → вот он» и цепочка unlock→format описываются файлами
// без машины состояний. Мутации (add/update/format/unlock) пишутся в
// calls.log; add падает с rc=17, если лежит add_fail.
func newFake(t *testing.T) *fake {
	t.Helper()
	f := &fake{dir: t.TempDir()}
	script := `#!/usr/bin/env bash
echo "$@" >> "` + f.dir + `/calls.log"
case "$1/$2" in
  dir/list|tape/list)
    n=$(cat "` + f.dir + `/n" 2>/dev/null || echo 0); n=$((n+1)); echo $n > "` + f.dir + `/n"
    if [ -f "` + f.dir + `/list.$n.out" ]; then cat "` + f.dir + `/list.$n.out"; else cat "` + f.dir + `/list.out" 2>/dev/null; fi ;;
  dir/add|tape/add) if [ -f "` + f.dir + `/add_fail" ]; then exit 17; fi; exit 0 ;;
  extent/list)      cat "` + f.dir + `/extents.out" 2>/dev/null ;;
  *) exit 0 ;;
esac
`
	if err := os.WriteFile(filepath.Join(f.dir, "phobos"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	f.set("list.out", "[]")
	return f
}

func (f *fake) set(name, body string) {
	if err := os.WriteFile(filepath.Join(f.dir, name), []byte(body), 0o600); err != nil {
		panic(err)
	}
}

func (f *fake) calls() string {
	b, err := os.ReadFile(filepath.Join(f.dir, "calls.log"))
	if os.IsNotExist(err) {
		return ""
	}
	if err != nil {
		panic(err)
	}
	return string(b)
}

func (f *fake) client(family string) *phobosctl.Client {
	return &phobosctl.Client{Family: family, Library: "lib1", Bin: filepath.Join(f.dir, "phobos")}
}

func mediumJSON(name string, tags, adm, fs string, free int64) string {
	return fmt.Sprintf(`[{"name": %q, "tags": [%s], "adm_status": %q, "put_access": true,`+
		` "fs.status": %q, "stats.phys_spc_free": %d, "stats.phys_spc_used": 500}]`,
		name, tags, adm, fs, free)
}

func mediaAdd(t *testing.T, m *meta.Meta, f *fake) (admin.MediaAddResult, error) {
	t.Helper()
	return admin.MediaAdd(context.Background(),
		admin.MediaDeps{Meta: m, Phobos: f.client("dir")},
		admin.MediaAddParams{Label: "/var/lib/phobos/dir9", Generation: "L8", MediumType: "dir", PoolID: 1})
}

func mediaRow(t *testing.T, m *meta.Meta, label string) (map[string]any, bool) {
	t.Helper()
	var (
		id             int64
		gen, mtype, lc string
		poolID         *int64
		reason         *string
	)
	err := m.Pool.QueryRow(context.Background(),
		`SELECT id, generation, medium_type, pool_id, lifecycle, read_only_reason FROM media WHERE label = $1`, label).
		Scan(&id, &gen, &mtype, &poolID, &lc, &reason)
	if err != nil {
		return nil, false
	}
	rs := ""
	if reason != nil {
		rs = *reason
	}
	return map[string]any{"id": id, "gen": gen, "type": mtype, "pool": poolID, "lifecycle": lc, "reason": rs}, true
}

// Счастливый путь: носителя не было → add с тегами пула и поколения → Phobos
// подтвердил → строка в реестре с ёмкостью из Phobos.
func TestMediaAddHappy(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	f := newFake(t)
	f.set("list.2.out", mediumJSON("/var/lib/phobos/dir9", `"pool:shared","gen:L8"`, "unlocked", "used", 1000))

	res, err := mediaAdd(t, m, f)
	if err != nil {
		t.Fatalf("MediaAdd: %v", err)
	}
	if res.MediaID == 0 {
		t.Fatal("media.id не вернулся")
	}
	row, ok := mediaRow(t, m, "/var/lib/phobos/dir9")
	if !ok {
		t.Fatal("строка media не создана")
	}
	if row["gen"] != "L8" || row["type"] != "dir" || row["lifecycle"] != "active" {
		t.Errorf("строка неверна: %v", row)
	}
	var free, cap int64
	if err := m.Pool.QueryRow(context.Background(),
		`SELECT free_bytes_cached, capacity_bytes_cached FROM media WHERE id = $1`, res.MediaID).
		Scan(&free, &cap); err != nil {
		t.Fatal(err)
	}
	if free != 1000 || cap != 1500 {
		t.Errorf("ёмкость из Phobos не записана: free=%d cap=%d", free, cap)
	}
	if !strings.Contains(f.calls(), "dir add --unlock -L lib1 -T pool:shared,gen:L8 /var/lib/phobos/dir9") {
		t.Errorf("add с тегами не вызван:\n%s", f.calls())
	}
}

// «Закрыто при сомнении»: add упал — в реестр не пишем ничего.
func TestMediaAddFailClosedOnAdd(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	f := newFake(t)
	f.set("add_fail", "1")

	if _, err := mediaAdd(t, m, f); err == nil {
		t.Fatal("ошибка add в Phobos обязана приводить к ошибке команды")
	}
	if _, ok := mediaRow(t, m, "/var/lib/phobos/dir9"); ok {
		t.Error("Phobos не принял носитель, а строка в реестре появилась")
	}
}

// «Закрыто при сомнении»: add вернул ноль, но носителя в списке нет —
// подтверждения нет, в реестр не пишем.
func TestMediaAddFailClosedNoConfirm(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	f := newFake(t)
	f.set("list.2.out", "[]") // add «прошёл», но список по-прежнему пуст

	if _, err := mediaAdd(t, m, f); err == nil ||
		!strings.Contains(err.Error(), "не подтвердил") {
		t.Fatalf("ошибка %v, ждали «Phobos не подтвердил добавление»", err)
	}
	if _, ok := mediaRow(t, m, "/var/lib/phobos/dir9"); ok {
		t.Error("подтверждения не было, а строка в реестре появилась")
	}
}

// Неразбираемый ответ Phobos — сомнение: ни add, ни строки в реестре.
func TestMediaAddFailClosedGarbageList(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	f := newFake(t)
	f.set("list.out", "<ERROR> DSS is down")

	if _, err := mediaAdd(t, m, f); err == nil {
		t.Fatal("мусор вместо списка обязан быть ошибкой")
	}
	// Сам list, конечно, был — но ни одной мутации после мусорного ответа.
	calls := f.calls()
	if strings.Contains(calls, " add ") || strings.Contains(calls, " update ") {
		t.Errorf("после неразбираемого списка трогать Phobos нельзя:\n%s", calls)
	}
	if _, ok := mediaRow(t, m, "/var/lib/phobos/dir9"); ok {
		t.Error("строка в реестре при сомнении")
	}
}

// Повтор команды идемпотентен: та же строка, тот же id, второго add нет.
func TestMediaAddIdempotent(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	f := newFake(t)
	med := mediumJSON("/var/lib/phobos/dir9", `"pool:shared","gen:L8"`, "unlocked", "used", 1000)
	f.set("list.2.out", med)
	f.set("list.3.out", med) // второй заход: носитель уже в списке

	first, err := mediaAdd(t, m, f)
	if err != nil {
		t.Fatal(err)
	}
	second, err := mediaAdd(t, m, f)
	if err != nil {
		t.Fatalf("повтор: %v", err)
	}
	if first.MediaID != second.MediaID {
		t.Errorf("повтор дал другой id: %d vs %d", first.MediaID, second.MediaID)
	}
	var n int
	if err := m.Pool.QueryRow(context.Background(),
		`SELECT count(*) FROM media WHERE label = '/var/lib/phobos/dir9'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("строк media: %d, ждали 1", n)
	}
	if strings.Count(f.calls(), "dir add") != 1 {
		t.Errorf("повтор тронул Phobos повторно:\n%s", f.calls())
	}
}

// Метка занята другими параметрами — отказ до обращения к Phobos:
// перетегировать чужой носитель «заодно» нельзя.
func TestMediaAddLabelTaken(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	if _, err := m.Pool.Exec(context.Background(),
		`INSERT INTO media (label, generation, medium_type, pool_id, lifecycle)
		 VALUES ('/var/lib/phobos/dir9', 'L9', 'dir', 1, 'active')`); err != nil {
		t.Fatal(err)
	}
	f := newFake(t)

	_, err := mediaAdd(t, m, f)
	if !errors.Is(err, meta.ErrMediaLabelTaken) {
		t.Fatalf("ошибка %v, ждали ErrMediaLabelTaken", err)
	}
	if f.calls() != "" {
		t.Errorf("Phobos трогать нельзя, метка занята:\n%s", f.calls())
	}
}

// Лента помечена чужим пулом — сомнение, отказ (ADR-0013: ровно один пул).
func TestMediaAddPoolConflict(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	f := newFake(t)
	f.set("list.out", mediumJSON("/var/lib/phobos/dir9", `"pool:other"`, "unlocked", "used", 1000))

	if _, err := mediaAdd(t, m, f); err == nil ||
		!strings.Contains(err.Error(), "переназначение пула") {
		t.Fatalf("ошибка %v, ждали отказ по чужому пулу", err)
	}
	if strings.Contains(f.calls(), "update") || strings.Contains(f.calls(), "add") {
		t.Errorf("на конфликте Phobos трогать нельзя:\n%s", f.calls())
	}
}

// Носитель под замком и без формата: команда разблокирует и отформатирует,
// и только потом заведёт строку. Порядок виден по спискам: до unlock — locked,
// после unlock ещё blank, формат подтверждает только третий список.
func TestMediaAddUnlockAndFormat(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	f := newFake(t)
	f.set("list.1.out", mediumJSON("/var/lib/phobos/dir9", `"pool:shared","gen:L8"`, "locked", "blank", 1000))
	f.set("list.2.out", mediumJSON("/var/lib/phobos/dir9", `"pool:shared","gen:L8"`, "unlocked", "blank", 1000))
	f.set("list.3.out", mediumJSON("/var/lib/phobos/dir9", `"pool:shared","gen:L8"`, "unlocked", "used", 1000))

	if _, err := mediaAdd(t, m, f); err != nil {
		t.Fatalf("MediaAdd: %v", err)
	}
	calls := f.calls()
	for _, want := range []string{"dir unlock", "dir format --unlock --fs POSIX"} {
		if !strings.Contains(calls, want) {
			t.Errorf("нет вызова %q:\n%s", want, calls)
		}
	}
	if _, ok := mediaRow(t, m, "/var/lib/phobos/dir9"); !ok {
		t.Error("строка media не создана после разблокировки и форматирования")
	}
}

// Статус файловой системы не определён — поля нет в выводе или значение
// незнакомое (список полей CLI менялся вместе с обновлениями Phobos).
// Форматировать в этом случае — самая разрушительная операция системы при
// сомнении: лента может быть заполнена данными. Отказ без format.
func TestMediaAddUnknownFSStatus(t *testing.T) {
	cases := []struct{ name, body string }{
		{"поля fs.status нет",
			`[{"name": "/var/lib/phobos/dir9", "tags": ["pool:shared","gen:L8"], "adm_status": "unlocked",` +
				` "put_access": true, "stats.phys_spc_free": 1000, "stats.phys_spc_used": 500}]`},
		{"незнакомое значение",
			`[{"name": "/var/lib/phobos/dir9", "tags": ["pool:shared","gen:L8"], "adm_status": "unlocked",` +
				` "put_access": true, "fs.status": "strange", "stats.phys_spc_free": 1000, "stats.phys_spc_used": 500}]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := testdb.Connect(t)
			testdb.Clean(t, m)
			f := newFake(t)
			f.set("list.2.out", tc.body)

			if _, err := mediaAdd(t, m, f); err == nil ||
				!strings.Contains(err.Error(), "не определён") {
				t.Fatalf("ошибка %v, ждали «статус файловой системы не определён»", err)
			}
			if strings.Contains(f.calls(), " format") {
				t.Errorf("форматирование при неизвестном статусе:\n%s", f.calls())
			}
			if _, ok := mediaRow(t, m, "/var/lib/phobos/dir9"); ok {
				t.Error("строка в реестре при неизвестном статусе")
			}
		})
	}
}

// Заполненная лента — не цель записи, но зарегистрировать её можно: ровно тот
// случай, ради которого Ф1.5, — лента, записанная в обход реестра и уже
// заполнившаяся, чьи копии висят в 'writing'. Регистрация проходит с
// lifecycle='full' (put_access от такой ленты не требуются), копия доводится
// до 'done', и в media_write_targets лента не попадает.
func TestMediaAddFullRegistersAsFull(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	packID := uuid.New()
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO packs (id, pool_id, kind, state) VALUES ($1, 1, 'data', 'archived')`, packID); err != nil {
		t.Fatal(err)
	}
	copyID := uuid.New()
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO archive_copies (id, pack_id, spec, phobos_oid, format, state, sha256)
		 VALUES ($1, $2, 'main', 'pack/1', 1, 'writing', decode(repeat('AB', 32), 'hex'))`,
		copyID, packID); err != nil {
		t.Fatal(err)
	}

	f := newFake(t)
	// free=0: лента заполнилась. put не снят и замок не наведён — оператор её
	// не закрывал, это именно 'full', а не 'read_only'.
	f.set("list.2.out", `[{"name": "/var/lib/phobos/dir9", "tags": ["pool:shared","gen:L8"],`+
		` "adm_status": "unlocked", "put_access": true, "fs.status": "full",`+
		` "stats.phys_spc_free": 0, "stats.phys_spc_used": 900}]`)
	f.set("extents.out", `[{"oid": "pack/1", "media_name": ["/var/lib/phobos/dir9"]}]`)

	res, err := mediaAdd(t, m, f)
	if err != nil {
		t.Fatalf("регистрация заполненной ленты: %v", err)
	}
	if res.BoundCopies != 1 {
		t.Errorf("привязано %d копий, ждали 1; BindErrors=%v", res.BoundCopies, res.BindErrors)
	}
	row, ok := mediaRow(t, m, "/var/lib/phobos/dir9")
	if !ok {
		t.Fatal("строка media не создана")
	}
	if row["lifecycle"] != "full" {
		t.Errorf("lifecycle=%v, ждали full", row["lifecycle"])
	}
	// Определение «доступна для записи» (media_write_targets) не должен
	// пускать 'full': иначе migrator отправит put на ленту без места.
	var targets int
	if err := m.Pool.QueryRow(ctx,
		`SELECT count(*) FROM media_write_targets WHERE id = $1`, res.MediaID).Scan(&targets); err != nil {
		t.Fatal(err)
	}
	if targets != 0 {
		t.Error("лента 'full' попала в media_write_targets")
	}
	var state string
	if err := m.Pool.QueryRow(ctx,
		`SELECT state::text FROM archive_copies WHERE id = $1`, copyID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "done" {
		t.Errorf("копия: state=%s, ждали done", state)
	}
}

// Лента с данными и закрытая для записи — сознательное решение оператора:
// подозрительную ленту (ошибки чтения, TapeAlert) закрывают, чтобы на неё не
// писали, но копии её всё равно надо привязать к реестру. Регистрация проходит
// как 'read_only' с причиной, замок не трогается (повтор команды не должен
// молча открывать то, что оператор закрыл руками), копии доводятся до 'done'.
func TestMediaAddLockedDataRegistersReadOnly(t *testing.T) {
	cases := []struct{ name, adm, put, wantReason string }{
		{"под замком", "locked", "true", "заблокирована в Phobos"},
		{"put снят", "unlocked", "false", "запись запрещена в Phobos"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := testdb.Connect(t)
			testdb.Clean(t, m)
			ctx := context.Background()
			packID := uuid.New()
			if _, err := m.Pool.Exec(ctx,
				`INSERT INTO packs (id, pool_id, kind, state) VALUES ($1, 1, 'data', 'archived')`, packID); err != nil {
				t.Fatal(err)
			}
			copyID := uuid.New()
			if _, err := m.Pool.Exec(ctx,
				`INSERT INTO archive_copies (id, pack_id, spec, phobos_oid, format, state, sha256)
				 VALUES ($1, $2, 'main', 'pack/1', 1, 'writing', decode(repeat('AB', 32), 'hex'))`,
				copyID, packID); err != nil {
				t.Fatal(err)
			}
			f := newFake(t)
			f.set("list.2.out", fmt.Sprintf(`[{"name": "/var/lib/phobos/dir9", "tags": ["pool:shared","gen:L8"],`+
				` "adm_status": %q, "put_access": %s, "fs.status": "used",`+
				` "stats.phys_spc_free": 1000, "stats.phys_spc_used": 500}]`, tc.adm, tc.put))
			f.set("extents.out", `[{"oid": "pack/1", "media_name": ["/var/lib/phobos/dir9"]}]`)

			res, err := mediaAdd(t, m, f)
			if err != nil {
				t.Fatalf("регистрация закрытой ленты: %v", err)
			}
			if res.BoundCopies != 1 {
				t.Errorf("привязано %d копий, ждали 1; BindErrors=%v", res.BoundCopies, res.BindErrors)
			}
			row, ok := mediaRow(t, m, "/var/lib/phobos/dir9")
			if !ok {
				t.Fatal("строка media не создана")
			}
			if row["lifecycle"] != "read_only" || row["reason"] != tc.wantReason {
				t.Errorf("строка: %v, ждали read_only/%q", row, tc.wantReason)
			}
			if strings.Contains(f.calls(), " unlock ") {
				t.Errorf("замок трогать нельзя:\n%s", f.calls())
			}
			var state string
			if err := m.Pool.QueryRow(ctx,
				`SELECT state::text FROM archive_copies WHERE id = $1`, copyID).Scan(&state); err != nil {
				t.Fatal(err)
			}
			if state != "done" {
				t.Errorf("копия: state=%s, ждали done", state)
			}
		})
	}
}

// Поколение закрыто для записи (writable: false в конфиге — слой 2 запрета Р5
// из 0014): лента регистрируется 'read_only' с причиной, а не отказом, и замок
// не трогается — даже у свежей ленты под замком: целью записи она не станет.
func TestMediaAddGenerationReadOnlyRegistersReadOnly(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	f := newFake(t)
	f.set("list.out", `[{"name": "/var/lib/phobos/dir9", "tags": ["pool:shared","gen:L7"],`+
		` "adm_status": "locked", "put_access": true, "fs.status": "empty",`+
		` "stats.phys_spc_free": 1000, "stats.phys_spc_used": 0}]`)

	_, err := admin.MediaAdd(context.Background(),
		admin.MediaDeps{Meta: m, Phobos: f.client("dir")},
		admin.MediaAddParams{Label: "/var/lib/phobos/dir9", Generation: "L7", MediumType: "dir",
			PoolID: 1, GenerationReadOnly: true})
	if err != nil {
		t.Fatalf("регистрация ленты запрещённого поколения: %v", err)
	}
	row, ok := mediaRow(t, m, "/var/lib/phobos/dir9")
	if !ok {
		t.Fatal("строка media не создана")
	}
	if row["lifecycle"] != "read_only" || row["reason"] != "поколение закрыто для записи" {
		t.Errorf("строка: %v", row)
	}
	if strings.Contains(f.calls(), " unlock ") {
		t.Errorf("замок трогать нельзя:\n%s", f.calls())
	}
}

// Чистая лента закрытого поколения не вводится: unlock и format дали бы
// разблокированный отформатированный том запрещённого поколения, а после
// форматирования запись на него разрешена — слой 2 запрета Р5 обнулился бы.
// Отказ до действий с Phobos: ни unlock, ни format не вызваны, строки нет.
// Форматированная лента того же поколения при этом регистрируется read_only
// (TestMediaAddGenerationReadOnlyRegistersReadOnly) — отказ касается только
// чистой, вводить которую незачем.
func TestMediaAddGenerationReadOnlyRefusesBlank(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	f := newFake(t)
	f.set("list.out", mediumJSON("/var/lib/phobos/dir9", `"pool:shared","gen:L7"`, "locked", "blank", 1000))

	_, err := admin.MediaAdd(context.Background(),
		admin.MediaDeps{Meta: m, Phobos: f.client("dir")},
		admin.MediaAddParams{Label: "/var/lib/phobos/dir9", Generation: "L7", MediumType: "dir",
			PoolID: 1, GenerationReadOnly: true})
	if err == nil || !strings.Contains(err.Error(), "закрыто для записи") {
		t.Fatalf("ошибка %v, ждали отказ вводить чистую ленту закрытого поколения", err)
	}
	calls := f.calls()
	for _, forbidden := range []string{" unlock", " format"} {
		if strings.Contains(calls, forbidden) {
			t.Errorf("действие с Phobos при отказе %q:\n%s", forbidden, calls)
		}
	}
	if _, ok := mediaRow(t, m, "/var/lib/phobos/dir9"); ok {
		t.Error("строка в реестре при отказе")
	}
}

// Носитель с данными и без тега пула: присвоение пула — осознанный шаг
// оператора (-adopt), а не побочный эффект регистрации. Данные могли быть
// записаны в обход tapegw, и по какому пулу они — неизвестно.
func TestMediaAddAdoptRequired(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	f := newFake(t)
	f.set("list.out", mediumJSON("/var/lib/phobos/dir9", "", "unlocked", "used", 1000))

	if _, err := mediaAdd(t, m, f); err == nil || !strings.Contains(err.Error(), "-adopt") {
		t.Fatalf("ошибка %v, ждали отказ с указанием -adopt", err)
	}
	calls := f.calls()
	if strings.Contains(calls, " update") || strings.Contains(calls, " add ") {
		t.Errorf("без -adopt трогать Phobos нельзя:\n%s", calls)
	}
	if _, ok := mediaRow(t, m, "/var/lib/phobos/dir9"); ok {
		t.Error("строка в реестре без явного -adopt")
	}
}

// Свежий отформатированный dir без тегов: phys_spc_used у dir показывает
// занятость всей файловой системы (на стенде — сотни гигабайт), а не данные
// Phobos. Индикатор данных — fs.status, и он здесь empty: -adopt не нужен,
// регистрация проходит. Именно на этом состоянии падал e2e-tape в CI, когда
// проверка опиралась на байты.
func TestMediaAddFreshFormattedNeedsNoAdopt(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	f := newFake(t)
	f.set("list.out", `[{"name": "/var/lib/phobos/dir9", "tags": [], "adm_status": "unlocked",`+
		` "put_access": true, "fs.status": "empty", "stats.phys_spc_free": 1000,`+
		` "stats.phys_spc_used": 64513060864}]`)

	res, err := mediaAdd(t, m, f)
	if err != nil {
		t.Fatalf("регистрация свежего dir: %v", err)
	}
	if res.MediaID == 0 {
		t.Error("media.id не вернулся")
	}
	row, ok := mediaRow(t, m, "/var/lib/phobos/dir9")
	if !ok || row["lifecycle"] != "active" {
		t.Errorf("строка не заведена как active: %v", row)
	}
}

// С явным -adopt пул присваивается: теги ставятся, строка заводится.
func TestMediaAddAdoptAssignsPool(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	f := newFake(t)
	f.set("list.out", mediumJSON("/var/lib/phobos/dir9", "", "unlocked", "used", 1000))

	res, err := admin.MediaAdd(context.Background(),
		admin.MediaDeps{Meta: m, Phobos: f.client("dir")},
		admin.MediaAddParams{Label: "/var/lib/phobos/dir9", Generation: "L8", MediumType: "dir", PoolID: 1, Adopt: true})
	if err != nil {
		t.Fatalf("MediaAdd с -adopt: %v", err)
	}
	if !strings.Contains(f.calls(), "dir update") {
		t.Errorf("теги не установлены:\n%s", f.calls())
	}
	row, ok := mediaRow(t, m, "/var/lib/phobos/dir9")
	if !ok || row["lifecycle"] != "active" {
		t.Errorf("строка не заведена как active: %v", row)
	}
	if res.MediaID == 0 {
		t.Error("media.id не вернулся")
	}
}

// Ради этого всё и затевалось: лента заводилась мимо реестра, копия легла и
// висит в 'writing'. Регистрация носителя находит такую копию по extents и
// доводит её до 'done'.
func TestMediaAddBindsStuckCopies(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	packID := uuid.New()
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO packs (id, pool_id, kind, state) VALUES ($1, 1, 'data', 'archived')`, packID); err != nil {
		t.Fatal(err)
	}
	copyID := uuid.New()
	// sha256 обязателен и в 'writing' (RecordPackCopy пишет сумму всегда,
	// CHECK archive_copies_done_has_sha256 требует её при переходе в 'done').
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO archive_copies (id, pack_id, spec, phobos_oid, format, state, sha256)
		 VALUES ($1, $2, 'main', 'pack/1', 1, 'writing', decode(repeat('AB', 32), 'hex'))`,
		copyID, packID); err != nil {
		t.Fatal(err)
	}

	f := newFake(t)
	f.set("list.2.out", mediumJSON("/var/lib/phobos/dir9", `"pool:shared","gen:L8"`, "unlocked", "used", 1000))
	f.set("extents.out", `[{"oid": "pack/1", "media_name": ["/var/lib/phobos/dir9"]}]`)

	res, err := mediaAdd(t, m, f)
	if err != nil {
		t.Fatalf("MediaAdd: %v", err)
	}
	if res.BoundCopies != 1 {
		t.Errorf("привязано %d копий, ждали 1; BindErrors=%v", res.BoundCopies, res.BindErrors)
	}
	var state string
	var mediumID *int64
	if err := m.Pool.QueryRow(ctx,
		`SELECT state::text, medium_id FROM archive_copies WHERE id = $1`, copyID).
		Scan(&state, &mediumID); err != nil {
		t.Fatal(err)
	}
	if state != "done" || mediumID == nil || *mediumID != res.MediaID {
		t.Errorf("копия: state=%s medium_id=%v, ждали done/%d", state, mediumID, res.MediaID)
	}
}

// Ошибка проверки extents у одной копии не откатывает регистрацию, но команда
// обязана завершиться ошибкой — «частично проверено» не бывает зелёным.
func TestMediaAddBindErrorSurfaces(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()
	packID := uuid.New()
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO packs (id, pool_id, kind, state) VALUES ($1, 1, 'data', 'archived')`, packID); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO archive_copies (id, pack_id, spec, phobos_oid, format, state, sha256)
		 VALUES (gen_random_uuid(), $1, 'main', 'pack/1', 1, 'writing', decode(repeat('AB', 32), 'hex'))`,
		packID); err != nil {
		t.Fatal(err)
	}
	f := newFake(t)
	f.set("list.2.out", mediumJSON("/var/lib/phobos/dir9", `"pool:shared","gen:L8"`, "unlocked", "used", 1000))
	f.set("extents.out", "не json")

	res, err := mediaAdd(t, m, f)
	if err != nil {
		t.Fatalf("регистрация состоялась: %v", err)
	}
	if len(res.BindErrors) != 1 {
		t.Errorf("BindErrors=%v, ждали одну", res.BindErrors)
	}
}
