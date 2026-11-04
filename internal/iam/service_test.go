package iam_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/s3err"

	"github.com/vladsimachkov/tapegw/internal/iam"
	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// Тесты IAMService поверх Postgres. Ключевые проверки ревьюера:
//
//   - ротация KEK: вход по СТАРОМУ ключу работает, потому что kek_version
//     реально выбирает ключ при расшифровке (TestKekRotation...);
//   - секрет не попадает ни в ошибки, ни в логи (TestSecretNeverLeaks);
//   - root не задевается: его нет в accounts, сервис на него не влияет
//     (TestRootIsNotAnIamAccount);
//   - не-admin в системном тенанте — понятный AccessDenied, а не SQLSTATE
//     (TestSystemTenantRules).

const (
	rootAccess = "rootaccess"
	rootSecret = "rootsecret-do-not-put-in-accounts"

	tenantSystem = 1 // `default` из миграции 0005, system = true
	tenantClient = 2 // клиентский тенант тестовой базы
	// Второй легальный клиентский тенант: чтобы проверять надгробие на выпуске
	// того же ключа в другом тенанте, а не на правиле «роль ↔ тенант».
	tenantClient2 = 3
)

// Кольца ключей. Разные байты у v1 и v2 обязательны: если Open ошибётся и
// возьмёт активный ключ вместо версии из строки, тест обязан получить
// «не развернулось», а не молча совпадение ключей.
var (
	keyV1 = bytes.Repeat([]byte{0x11}, 32)
	keyV2 = bytes.Repeat([]byte{0x22}, 32)
)

type env struct {
	m      *meta.Meta
	ringV1 *iam.KeyRing
}

func setup(t *testing.T) *env {
	t.Helper()

	m := testdb.Connect(t)
	ctx := context.Background()

	// Своя схема на пакет, но тесты гоняются в одном процессе последовательно:
	// убираем учётки и клиентский тенант, чтобы прогон не зависел от порядка.
	if _, err := m.Pool.Exec(ctx, `TRUNCATE accounts CASCADE`); err != nil {
		t.Fatalf("truncate accounts: %v", err)
	}
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO tenants (id, name, pool_id) VALUES ($1, 'client', 1)
		  ON CONFLICT (id) DO NOTHING`, tenantClient); err != nil {
		t.Fatalf("tenant %d: %v", tenantClient, err)
	}
	// Второй клиентский тенант: нужен, чтобы проверять надгробие на переносе
	// между ДВУМЯ легальными для роли тенантами. Иначе отказ приходит по правилу
	// «роль ↔ тенант» и тест доказывал бы не то.
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO tenants (id, name, pool_id) VALUES ($1, 'client2', 1)
		  ON CONFLICT (id) DO NOTHING`, tenantClient2); err != nil {
		t.Fatalf("tenant %d: %v", tenantClient2, err)
	}

	ring, err := iam.NewKeyRing(1, map[int][]byte{1: keyV1})
	if err != nil {
		t.Fatalf("keyring: %v", err)
	}
	return &env{m: m, ringV1: ring}
}

func (e *env) service(t *testing.T, ring *iam.KeyRing) *iam.Service {
	t.Helper()
	svc, err := iam.New(e.m, ring, iam.Options{RootAccess: rootAccess})
	if err != nil {
		t.Fatalf("iam.New: %v", err)
	}
	return svc
}

func (e *env) serviceV1(t *testing.T) *iam.Service {
	t.Helper()
	return e.service(t, e.ringV1)
}

func (e *env) get(t *testing.T, access string) meta.Account {
	t.Helper()
	a, err := e.m.GetAccount(context.Background(), access)
	if err != nil {
		t.Fatalf("GetAccount(%s): %v", access, err)
	}
	return a
}

// accountCreator — то, чем тест заводит учётку: и сервис, и кэш подходят.
// Интерфейс нужен, чтобы одни и те же помощники работали с обеими поверхностями.
type accountCreator interface {
	CreateAccount(auth.Account) error
}

func mustCreate(t *testing.T, svc accountCreator, access, secret string, role auth.Role, tenant int) {
	t.Helper()
	if err := svc.CreateAccount(auth.Account{
		Access: access, Secret: secret, Role: role, ProjectID: tenant,
	}); err != nil {
		t.Fatalf("CreateAccount(%s): %v", access, err)
	}
}

// --- GetUserAccount -------------------------------------------------------

func TestGetUserAccountReturnsTenantAsProjectID(t *testing.T) {
	e := setup(t)
	svc := e.serviceV1(t)

	const secret = "s3cr3t-userplus"
	mustCreate(t, svc, "userplus", secret, auth.RoleUserPlus, tenantClient)

	got, err := svc.GetUserAccount("userplus")
	if err != nil {
		t.Fatalf("GetUserAccount: %v", err)
	}
	if got.Secret != secret {
		t.Errorf("секрет не развернулся: %q", got.Secret)
	}
	// ProjectID — единственное, чем бэкенд узнаёт тенанта (ADR-0009). Если
	// здесь 0, страж откажет до всякой политики (ADR-0017 §6).
	if got.ProjectID != tenantClient {
		t.Errorf("ProjectID = %d, хотели %d", got.ProjectID, tenantClient)
	}
	if got.Role != auth.RoleUserPlus {
		t.Errorf("role = %q, хотели %q", got.Role, auth.RoleUserPlus)
	}
}

func TestGetUserAccountUnknown(t *testing.T) {
	e := setup(t)
	svc := e.serviceV1(t)

	if _, err := svc.GetUserAccount("nope"); !errors.Is(err, auth.ErrNoSuchUser) {
		t.Errorf("неизвестная учётка: %v, хотели ErrNoSuchUser", err)
	}
}

// Отозванная учётка не должна отдавать секрет: versitygw превращает
// ErrNoSuchUser в InvalidAccessKeyId, и «отозван» снаружи неотличим от
// «не существует».
func TestGetUserAccountRevoked(t *testing.T) {
	e := setup(t)
	svc := e.serviceV1(t)
	mustCreate(t, svc, "gone", "s3cr3t", auth.RoleUser, tenantClient)

	if _, err := e.m.Pool.Exec(context.Background(),
		`UPDATE accounts SET status = 'revoked', secret_wrapped = NULL WHERE access_key = 'gone'`); err != nil {
		t.Fatalf("revoke: %v", err)
	}

	if _, err := svc.GetUserAccount("gone"); !errors.Is(err, auth.ErrNoSuchUser) {
		t.Errorf("отозванная учётка: %v, хотели ErrNoSuchUser", err)
	}
	// Тот же отзыв должен сниматься и из проверки principal'ов политик.
	missing, err := svc.ResolveAccounts([]string{"gone"})
	if err != nil {
		t.Fatalf("ResolveAccounts: %v", err)
	}
	if len(missing) != 1 || missing[0] != "gone" {
		t.Errorf("отозванный ключ должен считаться отсутствующим в политиках, получили %v", missing)
	}
}

// --- ротация KEK ----------------------------------------------------------

// Главная проверка ревьюера: после смены KEK вход по старому ключу работает.
//
// possible это только тем, что accounts.kek_version выбирает ключ при
// расшифровке. Если в Open подставить активную версию вместо версии строки,
// тест падает на первом же GetUserAccount — а не «проходит, потому что ключи
// случайно одинаковые»: v1 и v2 нарочно разные.
func TestKekRotationKeepsOldSecretWorking(t *testing.T) {
	e := setup(t)
	svcV1 := e.serviceV1(t)

	const oldSecret = "old-secret-before-rotation"
	mustCreate(t, svcV1, "old", oldSecret, auth.RoleUser, tenantClient)
	if v := e.get(t, "old").KekVersion; v != 1 {
		t.Fatalf("учётка завернута версией %d, ожидали 1", v)
	}

	// Ротация: в кольце появились обе версии, активна новая. Старую нельзя
	// выбрасывать, пока на неё ссылается хоть одна строка (ADR-0014).
	ringV2, err := iam.NewKeyRing(2, map[int][]byte{1: keyV1, 2: keyV2})
	if err != nil {
		t.Fatalf("ring v2: %v", err)
	}
	svcV2 := e.service(t, ringV2)

	got, err := svcV2.GetUserAccount("old")
	if err != nil {
		t.Fatalf("после ротации вход по старому ключу сломался: %v", err)
	}
	if got.Secret != oldSecret {
		t.Errorf("после ротации секрет развернулся неверно: %q", got.Secret)
	}
	if v := e.get(t, "old").KekVersion; v != 1 {
		t.Errorf("старая учётка переписана на версию %d — переписывание должно быть отдельной операцией", v)
	}

	// Новая учётка после ротации обязана заворачиваться активной версией,
	// иначе ротация никогда не закончится.
	mustCreate(t, svcV2, "new", "new-secret", auth.RoleUser, tenantClient)
	if v := e.get(t, "new").KekVersion; v != 2 {
		t.Errorf("новая учётка завернута версией %d, хотели 2", v)
	}
}

// Старую версию KEK выбросить можно только когда на неё никто не ссылается:
// без ключа в кольце сервис обязан отказать, а не вернуть мусор.
func TestKekRotationOldVersionDropped(t *testing.T) {
	e := setup(t)
	mustCreate(t, e.serviceV1(t), "old", "s3cr3t", auth.RoleUser, tenantClient)

	ringOnly2, err := iam.NewKeyRing(2, map[int][]byte{2: keyV2})
	if err != nil {
		t.Fatalf("ring: %v", err)
	}
	svc := e.service(t, ringOnly2)

	if _, err := svc.GetUserAccount("old"); err == nil {
		t.Fatal("без ключа старой версии вход обязан отказывать")
	} else if !errors.Is(err, iam.ErrUnknownKekVersion) {
		t.Errorf("ошибка %v, хотели ErrUnknownKekVersion", err)
	}
}

// Обёртку нельзя переставить между учётками: aad — access_key, поэтому
// «моя обёртка в твоей строке» не разворачивается.
func TestWrappedSecretCannotBeMovedBetweenAccounts(t *testing.T) {
	e := setup(t)
	svc := e.serviceV1(t)
	mustCreate(t, svc, "alice", "secret-alice", auth.RoleUser, tenantClient)
	mustCreate(t, svc, "bob", "secret-bob", auth.RoleUser, tenantClient)

	alice := e.get(t, "alice")
	if _, err := e.m.Pool.Exec(context.Background(),
		`UPDATE accounts SET secret_wrapped = $1, kek_version = $2 WHERE access_key = 'bob'`,
		alice.SecretWrapped, alice.KekVersion); err != nil {
		t.Fatalf("swap: %v", err)
	}

	if _, err := svc.GetUserAccount("bob"); err == nil {
		t.Fatal("перенесённая обёртка развернулась — aad не проверяется")
	}
}

// --- секреты не утекают ---------------------------------------------------

// Секрет не должен появляться ни в тексте ошибки, ни в обёртке, ни в выводе
// учётки. Логи проверяются отдельно и по существу: пакет вообще не логирует
// (TestPackageDoesNotLog), а versitygw пишет возвращённую ошибку как есть
// (debuglogger.InternalError), поэтому «в ошибке нет секрета» = «в логе нет
// секрета».
func TestSecretNeverLeaks(t *testing.T) {
	e := setup(t)
	svc := e.serviceV1(t)

	const secret = "super-secret-value-9e1f4c"
	mustCreate(t, svc, "leak", secret, auth.RoleUser, tenantClient)

	// 1. В открытом виде секрета нет ни в обёртке, ни в базе целиком.
	row := e.get(t, "leak")
	if bytes.Contains(row.SecretWrapped, []byte(secret)) {
		t.Error("обёртка содержит секрет открытым текстом")
	}
	var n int
	if err := e.m.Pool.QueryRow(context.Background(),
		`SELECT count(*) FROM accounts
		  WHERE position(convert_to($1, 'UTF8') in secret_wrapped) > 0`, secret).Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 0 {
		t.Errorf("в secret_wrapped %d строк встречается открытый секрет", n)
	}

	// 2. Успешно развёрнутая учётка не печатает секрет через %v/%+v/%s:
	// на Account работает String() из versitygw с REDACTED, и это единственное,
	// что стоит между «учётку залогируют» и утечкой.
	acct, err := svc.GetUserAccount("leak")
	if err != nil {
		t.Fatalf("GetUserAccount: %v", err)
	}
	for _, form := range []string{"%v", "%+v", "%s"} {
		if s := fmt.Sprintf(form, acct); strings.Contains(s, secret) {
			t.Errorf("формат %s печатает секрет: %s", form, s)
		}
	}

	// 3. Ошибочные пути: дубликат, сломанная обёртка, неверная пара роль/тенант,
	// несуществующая учётка.
	errs := map[string]error{
		"duplicate": svc.CreateAccount(auth.Account{
			Access: "leak", Secret: secret, Role: auth.RoleUser, ProjectID: tenantClient,
		}),
		"denied": svc.CreateAccount(auth.Account{
			Access: "in-system", Secret: secret, Role: auth.RoleUserPlus, ProjectID: tenantSystem,
		}),
		"unknown": func() error { _, err := svc.GetUserAccount("net-takogo"); return err }(),
	}
	if _, err := e.m.Pool.Exec(context.Background(),
		`UPDATE accounts SET secret_wrapped = decode('00','hex') WHERE access_key = 'leak'`); err != nil {
		t.Fatalf("damage: %v", err)
	}
	_, errs["broken"] = svc.GetUserAccount("leak")

	for name, err := range errs {
		if err == nil {
			t.Errorf("%s: ожидали ошибку", name)
			continue
		}
		if strings.Contains(err.Error(), secret) {
			t.Errorf("%s: текст ошибки содержит секрет: %v", name, err)
		}
		// Обёртка — тоже не «ещё один секрет», её печатать не за чем.
		if strings.Contains(err.Error(), fmt.Sprintf("%x", row.SecretWrapped)) {
			t.Errorf("%s: текст ошибки содержит обёртку целиком: %v", name, err)
		}
	}
}

// Пакет не логирует — и не должен начинать, пока в руках держит открытые
// секреты. Тест по исходникам (тот же приём, что у стража тенанта):
// логирование в этом пакете почти наверняка появится с сообщением, в которое
// «случайно» попадёт учётка, и поймать это постфактум дороже.
func TestPackageDoesNotLog(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil || len(files) == 0 {
		t.Fatalf("файлы пакета не найдены: %v", err)
	}
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		for _, banned := range []string{`"log/slog"`, `"log"`, `slog.`, `log.Printf`, `fmt.Print`} {
			if strings.Contains(string(raw), banned) {
				t.Errorf("%s использует логирование (%s): пакет отдаёт открытые секреты и логировать учётки не должен",
					name, banned)
			}
		}
	}
}

// --- root -----------------------------------------------------------------

// Root не является учёткой IAM: versitygw собирает его из конфига
// (middlewares/authentication.go:44) и не спрашивает IAM при подписи его
// запросов. Проверка «не читает accounts» делается вставкой строки с ключом
// root: если бы сервис читал таблицу, он бы её нашёл.
func TestRootIsNotAnIamAccount(t *testing.T) {
	e := setup(t)
	svc := e.serviceV1(t)

	// Строка с access_key root в tables — попытка подменить учётку с обходом
	// проверок. Её не должно быть, а если её занесли руками — сервис обязан
	// её не видеть.
	if _, err := e.m.Pool.Exec(context.Background(),
		`INSERT INTO accounts (access_key, secret_wrapped, kek_version, role, tenant_id, tenant_system)
		  VALUES ($1, decode('00','hex'), 1, 'admin', 1, true)`, rootAccess); err != nil {
		t.Fatalf("подготовка строки root: %v", err)
	}

	if _, err := svc.GetUserAccount(rootAccess); !errors.Is(err, auth.ErrNoSuchUser) {
		t.Errorf("GetUserAccount(root) = %v, хотели ErrNoSuchUser без обращения к accounts", err)
	}
	// Завести учётку с ключом root нельзя — файловый IAM отвечает то же самое.
	if err := svc.CreateAccount(auth.Account{
		Access: rootAccess, Secret: "x", Role: auth.RoleAdmin, ProjectID: tenantSystem,
	}); !errors.Is(err, auth.ErrUserExists) {
		t.Errorf("CreateAccount(root) = %v, хотели ErrUserExists", err)
	}
	// Удалить root «из IAM» нельзя: его там нет.
	if err := svc.DeleteUserAccount(rootAccess); !errors.Is(err, auth.ErrNoSuchUser) {
		t.Errorf("DeleteUserAccount(root) = %v, хотели ErrNoSuchUser", err)
	}
	// Но как principal root существует: владелец бакета root в ACL обязан
	// опознаваться, иначе любой ACL с грантом владельцу падал бы
	// «accounts does not exist».
	missing, err := svc.ResolveAccounts([]string{rootAccess})
	if err != nil {
		t.Fatalf("ResolveAccounts: %v", err)
	}
	if len(missing) != 0 {
		t.Errorf("root не должен считаться отсутствующим, получили %v", missing)
	}
	// И строка в accounts при этом осталась нетронутой — сервис её не трогал.
	if _, err := e.m.GetAccount(context.Background(), rootAccess); err != nil {
		t.Errorf("сервис задел строку root в accounts: %v", err)
	}
}

// --- правило «роль ↔ системный тенант» ------------------------------------

// sqlState — код ошибки Postgres, если он есть. Нужен, чтобы отличить отказ
// схемы от отказа кода: клиенту нужен 403 AccessDenied, а не 500 из-за
// 23514/23503.
func sqlState(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}

func expectAccessDenied(t *testing.T, label string, err error) {
	t.Helper()
	if err == nil {
		t.Errorf("%s: ожидали отказ, а прошло", label)
		return
	}
	// Разбор — типом, а не errors.As, ровно как его делает versitygw:
	// ProcessController берёт err.(s3err.S3Error) (s3api/controllers/base.go:314),
	// и завёрнутый в fmt.Errorf("%w") APIError там перестаёт быть S3Error —
	// клиент получил бы 500 вместо 403. errors.As этого не видит, поэтому
	// проверяем типом: тест обязан падать на «правильный код, но не тот тип».
	if serr, ok := err.(s3err.S3Error); !ok {
		t.Errorf("%s: отказ не является s3err.S3Error целиком, versitygw отдаст 500: %v (%T)", label, err, err)
		return
	} else if serr.BaseError().Code != "AccessDenied" {
		t.Errorf("%s: код %q, хотели AccessDenied (%v)", label, serr.BaseError().Code, err)
	}
	if pg := sqlState(err); pg != "" {
		t.Errorf("%s: отказ пришёл из схемы (SQLSTATE %s) — проверка в коде не сработала: %v", label, pg, err)
	}
}

func TestSystemTenantRules(t *testing.T) {
	e := setup(t)
	svc := e.serviceV1(t)

	// 1. Клиентская роль в системном тенанте — отказ до похода в БД.
	err := svc.CreateAccount(auth.Account{
		Access: "sneak", Secret: "s", Role: auth.RoleUserPlus, ProjectID: tenantSystem,
	})
	expectAccessDenied(t, "userplus в системный тенант", err)
	if _, getErr := e.m.GetAccount(context.Background(), "sneak"); !errors.Is(getErr, meta.ErrNotFound) {
		t.Errorf("учётка всё равно записана: %v", getErr)
	}

	// 2. Admin в системном тенанте — можно: это системный оператор.
	mustCreate(t, svc, "operator", "s", auth.RoleAdmin, tenantSystem)

	// 3. Admin в клиентском тенанте — нельзя: роль обходит все проверки
	//    политик (ADR-0009), выдать её внутри тенанта клиента — значит
	//    завести там неуязвимого для политик пользователя.
	expectAccessDenied(t, "admin в клиентский тенант", svc.CreateAccount(auth.Account{
		Access: "bad-admin", Secret: "s", Role: auth.RoleAdmin, ProjectID: tenantClient,
	}))

	// 4. Обычная учётка в клиентском тенанте — можно.
	mustCreate(t, svc, "client", "s", auth.RoleUser, tenantClient)

	// 5. Перенос учётки в другой тенант запрещён вовсе (план PR#2a п. 0б):
	// тенант неизменяем, и отказ здесь — не про права, а про то, что такой
	// операции не существует.
	if err := svc.UpdateUserAccount("client", auth.MutableProps{ProjectID: ptr(tenantSystem)}); err == nil {
		t.Errorf("перенос в системный тенант прошёл, а должен быть отказ")
	}
	if row := e.get(t, "client"); row.TenantID != tenantClient {
		t.Errorf("после отказа тенант изменился: %d, хотели %d", row.TenantID, tenantClient)
	}

	// 6. Смена роли на admin у учётки клиентского тенанта — тоже.
	err = svc.UpdateUserAccount("client", auth.MutableProps{Role: auth.RoleAdmin})
	expectAccessDenied(t, "выдать admin клиентской учётке", err)

	// 7. Ничего из вышеперечисленного не записалось.
	for _, access := range []string{"sneak", "bad-admin"} {
		if _, getErr := e.m.GetAccount(context.Background(), access); !errors.Is(getErr, meta.ErrNotFound) {
			t.Errorf("%s: строка появилась в accounts: %v", access, getErr)
		}
	}
	if a := e.get(t, "client"); a.Role != "user" || a.TenantID != tenantClient {
		t.Errorf("неудачные обновления изменили учётку: %+v", a)
	}
}

// --- остальные методы -----------------------------------------------------

func TestCreateAccountValidation(t *testing.T) {
	e := setup(t)
	svc := e.serviceV1(t)

	if err := svc.CreateAccount(auth.Account{Access: "", Secret: "s", Role: auth.RoleUser, ProjectID: tenantClient}); err == nil {
		t.Error("пустой access_key обязан отказывать")
	}
	if err := svc.CreateAccount(auth.Account{Access: "a", Secret: "", Role: auth.RoleUser, ProjectID: tenantClient}); err == nil {
		t.Error("пустой секрет обязан отказывать")
	}
	if err := svc.CreateAccount(auth.Account{Access: "a", Secret: "s", Role: auth.Role("bogus"), ProjectID: tenantClient}); err == nil {
		t.Error("неизвестная роль обязана отказывать")
	}
	// Fail closed (ADR-0009): ключ без тенанта не заводится — ProjectID = 0
	// страж читает как «тенанта нет» и отказывает до всякой политики.
	if err := svc.CreateAccount(auth.Account{Access: "a", Secret: "s", Role: auth.RoleUser, ProjectID: 0}); err == nil {
		t.Error("учётка без тенанта обязана отказывать")
	}
	if err := svc.CreateAccount(auth.Account{Access: "a", Secret: "s", Role: auth.RoleUser, ProjectID: 999999}); err == nil {
		t.Error("несуществующий тенант обязан отказывать")
	}

	mustCreate(t, svc, "dup", "s", auth.RoleUser, tenantClient)
	if err := svc.CreateAccount(auth.Account{Access: "dup", Secret: "s2", Role: auth.RoleUser, ProjectID: tenantClient}); !errors.Is(err, auth.ErrUserExists) {
		t.Errorf("дубликат: %v, хотели ErrUserExists", err)
	}
}

func TestUpdateAndDelete(t *testing.T) {
	e := setup(t)
	svc := e.serviceV1(t)
	mustCreate(t, svc, "mut", "first", auth.RoleUser, tenantClient)

	// Смена секрета — новая обёртка активной версией.
	newSecret := "second"
	if err := svc.UpdateUserAccount("mut", auth.MutableProps{Secret: &newSecret}); err != nil {
		t.Fatalf("смена секрета: %v", err)
	}
	got, err := svc.GetUserAccount("mut")
	if err != nil || got.Secret != newSecret {
		t.Fatalf("секрет не обновился: %+v err=%v", got, err)
	}

	// Переноса между тенантами не существует (план PR#2a п. 0б): тенант
	// учётки неизменяем, нужен другой тенант — новый ключ. Проверяем, что
	// попытка отклонена и ничего не записано.
	if err := svc.UpdateUserAccount("mut", auth.MutableProps{ProjectID: ptr(tenantSystem), Role: auth.RoleAdmin}); err == nil {
		t.Fatalf("перенос в системный тенант прошёл, а должен быть отказ")
	}
	row := e.get(t, "mut")
	if row.TenantID != tenantClient || row.Role == "admin" {
		t.Errorf("после отказа запись изменилась: %+v", row)
	}

	if err := svc.DeleteUserAccount("mut"); err != nil {
		t.Fatalf("удаление: %v", err)
	}
	if _, err := svc.GetUserAccount("mut"); !errors.Is(err, auth.ErrNoSuchUser) {
		t.Errorf("после удаления: %v, хотели ErrNoSuchUser", err)
	}
	if err := svc.DeleteUserAccount("mut"); !errors.Is(err, auth.ErrNoSuchUser) {
		t.Errorf("повторное удаление: %v, хотели ErrNoSuchUser", err)
	}
	if err := svc.UpdateUserAccount("mut", auth.MutableProps{Role: auth.RoleUser}); !errors.Is(err, auth.ErrNoSuchUser) {
		t.Errorf("обновление удалённой: %v, хотели ErrNoSuchUser", err)
	}
}

func TestListAndResolve(t *testing.T) {
	e := setup(t)
	svc := e.serviceV1(t)
	mustCreate(t, svc, "one", "s1", auth.RoleUser, tenantClient)
	mustCreate(t, svc, "two", "s2", auth.RoleUserPlus, tenantClient)

	list, err := svc.ListUserAccounts()
	if err != nil {
		t.Fatalf("ListUserAccounts: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("список: %d учётки, хотели 2", len(list))
	}
	for _, a := range list {
		// Секреты в списке нарочно не отдаются — см. документацию метода.
		if a.Secret != "" {
			t.Errorf("учётка %s вернула секрет в списке", a.Access)
		}
		if a.ProjectID != tenantClient {
			t.Errorf("учётка %s: ProjectID = %d", a.Access, a.ProjectID)
		}
	}

	missing, err := svc.ResolveAccounts([]string{"one", "ghost", "two", "phantom"})
	if err != nil {
		t.Fatalf("ResolveAccounts: %v", err)
	}
	if strings.Join(missing, ",") != "ghost,phantom" {
		t.Errorf("missing = %v, хотели [ghost phantom]", missing)
	}
	if none, err := svc.ResolveAccounts(nil); err != nil || none != nil {
		t.Errorf("пустой запрос: %v, %v", none, err)
	}
}

// --- кольцо ключей (без базы) ---------------------------------------------

func TestKeyRingValidation(t *testing.T) {
	if _, err := iam.NewKeyRing(0, map[int][]byte{1: keyV1}); err == nil {
		t.Error("активная версия 0 обязана отказывать")
	}
	if _, err := iam.NewKeyRing(3, map[int][]byte{1: keyV1}); err == nil {
		t.Error("активной версии нет в кольце — обязан отказ")
	}
	if _, err := iam.NewKeyRing(1, map[int][]byte{1: bytes.Repeat([]byte{1}, 16)}); err == nil {
		t.Error("ключ 16 байт для AES-256 обязан отказывать")
	}
	if _, err := iam.NewKeyRing(1, nil); err == nil {
		t.Error("пустое кольцо обязано отказывать")
	}
}

func TestLoadKeyRingFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "kek.json")

	write := func(content string, mode os.FileMode) {
		t.Helper()
		// Перед перезаписью возвращаем файлу право записи: после mode = 0400
		// os.WriteFile уже не может его усечь и даёт EACCES. Локально под root
		// этого не видно (root права на запись не спрашивает), а на CI-раннере
		// тест падал именно здесь.
		if _, statErr := os.Stat(path); statErr == nil {
			if err := os.Chmod(path, 0o600); err != nil {
				t.Fatalf("chmod перед записью: %v", err)
			}
		}
		if err := os.WriteFile(path, []byte(content), mode); err != nil {
			t.Fatalf("write: %v", err)
		}
		// WriteFile не меняет права уже существующего файла, а тест переписывает
		// его несколько раз подряд — без Chmod вторая проверка смотрела бы на
		// права первой.
		if err := os.Chmod(path, mode); err != nil {
			t.Fatalf("chmod: %v", err)
		}
	}

	body, err := json.Marshal(map[string]string{"1": encodeKey(keyV1), "2": encodeKey(keyV2)})
	if err != nil {
		t.Fatal(err)
	}

	// Права шире 0400 — ошибка запуска: KEK, который читают все, это не KEK.
	write(string(body), 0o644)
	if _, err := iam.LoadKeyRing(path, 1); err == nil {
		t.Error("файл KEK с правами 0644 обязан отказываться")
	}

	write(string(body), 0o400)
	ring, err := iam.LoadKeyRing(path, 2)
	if err != nil {
		t.Fatalf("LoadKeyRing: %v", err)
	}
	if ring.Active() != 2 {
		t.Errorf("active = %d, хотели 2", ring.Active())
	}

	write(`{"1": "!!!"}`, 0o400)
	if _, err := iam.LoadKeyRing(path, 1); err == nil {
		t.Error("не-base64 обязан отказываться")
	}
	write(`{"0": "`+encodeKey(keyV1)+`"}`, 0o400)
	if _, err := iam.LoadKeyRing(path, 0); err == nil {
		t.Error("версия 0 обязана отказываться")
	}
	if _, err := iam.LoadKeyRing(filepath.Join(dir, "net-dolzhen-byt"), 1); err == nil {
		t.Error("несуществующий файл обязан отказываться")
	}
}

// --- хелперы --------------------------------------------------------------

func encodeKey(b []byte) string { return base64.StdEncoding.EncodeToString(b) }

func ptr[T any](v T) *T { return &v }

// Отзыв — надгробие, а не удаление (план PR#2a п. 0а).
//
// Ключ доступа — принципал в ACL и в политиках бакетов. Если строку удалить,
// тот же access_key можно выпустить заново, и все старые гранты молча перейдут
// новому владельцу, возможно из другого тенанта. Поэтому проверяются четыре
// вещи: строка остаётся и теряет секрет; повторный выпуск того же ключа
// отвергается (в том числе в другом тенанте); отозванную нельзя обновить; в
// перечислении её не видно.
func TestRevokedAccountIsTombstone(t *testing.T) {
	e := setup(t)
	svc := e.serviceV1(t)
	ctx := context.Background()

	mustCreate(t, svc, "dead", "topsecret", auth.RoleUser, tenantClient)

	if err := svc.DeleteUserAccount("dead"); err != nil {
		t.Fatalf("отзыв: %v", err)
	}

	// 1. Строка жива, секрета нет.
	row := e.get(t, "dead")
	if row.Status != meta.AccountRevoked {
		t.Errorf("status = %q, хотели %q", row.Status, meta.AccountRevoked)
	}
	if row.SecretWrapped != nil {
		t.Errorf("secret_wrapped остался после отзыва: %d байт", len(row.SecretWrapped))
	}
	// Прямой запрос в БД: вдруг scanAccount что-то подставляет.
	var raw []byte
	if err := e.m.Pool.QueryRow(ctx,
		`SELECT secret_wrapped FROM accounts WHERE access_key = 'dead'`).Scan(&raw); err != nil {
		t.Fatalf("прочитать secret_wrapped: %v", err)
	}
	if raw != nil {
		t.Errorf("в БД остался зашифрованный секрет: %d байт", len(raw))
	}

	// 2. Тот же ключ нельзя выпустить заново — в том числе в ДРУГОМ легальном
	//    тенанте. Это и есть смысл надгробия: гранты принципала не должны
	//    переехать новому владельцу.
	err := svc.CreateAccount(auth.Account{
		Access: "dead", Secret: "newlife", Role: auth.RoleUser, ProjectID: tenantClient2,
	})
	if err == nil {
		t.Fatalf("повторный выпуск отозванного ключа прошёл: гранты принципала перешли бы новому владельцу")
	}
	// Разбор — типом, как его делает versitygw (см. expectAccessDenied):
	// завёрнутый в %w APIError для ProcessController уже не S3Error.
	if !errors.Is(err, auth.ErrUserExists) {
		t.Errorf("повторный выпуск: %v, хотели auth.ErrUserExists", err)
	}

	// 3. Отозванную нельзя оживить сменой секрета.
	secret := "revive"
	if err := svc.UpdateUserAccount("dead", auth.MutableProps{Secret: &secret}); !errors.Is(err, auth.ErrNoSuchUser) {
		t.Errorf("обновление отозванной: %v, хотели ErrNoSuchUser", err)
	}

	// 4. В перечислении её нет.
	list, err := svc.ListUserAccounts()
	if err != nil {
		t.Fatalf("ListUserAccounts: %v", err)
	}
	for _, a := range list {
		if a.Access == "dead" {
			t.Errorf("отозванная учётка видна в ListUserAccounts")
		}
	}
}
