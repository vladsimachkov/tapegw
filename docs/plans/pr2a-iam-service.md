# План: PR#2a — IAMService поверх Postgres, кэш, CI в тенант 2

Задача для исполнителя. Постановка самодостаточна: читать этот файл и ADR, а не
помнить переписку.

- **Этап плана:** T1 (multitenancy), продолжение ADR-0017 и ADR-0018
- **Связано:** `docs/adr/0018-system-tenant.md` (проверенный SQL и порядок шагов),
  `docs/adr/0017-tenant-guard.md` §6, `docs/adr/0009-tenant-model.md`,
  `docs/adr/0014-encryption-per-tenant.md`, `docs/ARCHITECTURE.md`
- **Задача B (продолжение):** `pr2b-change-owner.md`

## Процесс (обязателен, проверен на PR #14)

1. **Субагент реализует, агент проверяет.** Отчёт субагента — не доказательство.
   На PR #14 проверка нашла три расхождения с отчётом и одну регрессию, которую
   не увидели юнит-тесты.
2. **Коммит и пуш после каждого шага** в черновой PR: миграция → `IAMService` →
   кэш → переезд CI. Субагент PR #14 упал по таймауту в 30 минут и потерял
   незакоммиченные правки; с пошаговыми коммитами таймаут стоит только текущий шаг.
3. **Агент подтверждает проверки своими мутациями**, а не по отчёту. Мутация обязана
   падать **по существу**, а не по опечатке или по `duplicate key` — на PR #14 и в
   том, и в другом я сначала получил ложно-зелёный/ложно-красный результат.
4. **PR на ревью ревьюеру — только после проверки агентом.**

## Что сделать

### Шаг 1. Миграция 0007 — системный тенант

Порядок строгий, он проверен на живом Postgres и зафиксирован в ADR-0018:

a) `tenants`: `ADD COLUMN system boolean NOT NULL DEFAULT false`;
   `UPDATE ... SET system = true WHERE id = 1`; `ADD UNIQUE (id, system)`.
b) `tenants`: `ADD CONSTRAINT tenants_default_is_system CHECK (id <> 1 OR system)`.
   **Только после** `UPDATE` из (a) — иначе `ALTER` падает
   `check constraint ... is violated by some row`.
c) `accounts`: `ADD COLUMN tenant_system boolean` (nullable) → `UPDATE` backfill из
   `tenants` → `ALTER COLUMN ... SET NOT NULL`. Одной командой `NOT NULL` нельзя:
   на непустой таблице `column "tenant_system" ... contains null values`.
d) `accounts`: составной FK `(tenant_id, tenant_system) REFERENCES tenants(id, system)
   ON UPDATE CASCADE` + `CHECK (role = 'admin' OR NOT tenant_system)`.

Миграцию проверять **текстом файла** (правило AGENTS.md): Up/Down извлекать из
`.sql` программно и исполнять как есть, не перепечатывая. Down проверять отдельно
— на PR #11 и #12 пересказ Down уже ломался.

### Шаг 2. IAMService поверх Postgres

`GetUserAccount` / `CreateAccount` / `UpdateAccount` / `DeleteAccount` /
`ListAccounts`. `Account.ProjectID = tenant_id`. Секреты — `secret_wrapped`
(AES-GCM, KEK) и `kek_version`.

IAM отказывает учётке `role != admin` с `ProjectID` системного тенанта: понятный
`AccessDenied`, а не `SQLSTATE 23514` из схемы. Схема — вторая линия, она
защищает от ручной правки таблицы и сида.

### Шаг 3. Кэш

`GetUserAccount` вызывается на **каждом** запросе (ADR-0016). Инвалидация при
изменении учётки — `LISTEN`/`NOTIFY`, не TTL.

### Шаг 4. CI в тенант 2

`userplus` переводится из тенанта 1 (системного) в тенант 2. Иначе проверки
доступа проверяют отказ стража или доступ к бакету системного тенанта вместо
политики — тихая подмена, при которой тесты остаются зелёными.

## Чем подтвердить (критерии ревьюера)

| # | Проверка | Как подтверждать |
|---|---|---|
| 1 | Секреты в открытом виде не попадают ни в логи, ни в ошибки | grep по выводам + тест на текст ошибки |
| 2 | **Ротация KEK: после смены KEK вход по старому ключу работает** | тест обязан падать, если `kek_version` не выбирает ключ, а записан для красоты |
| 3 | **Отозванный ключ получает отказ сразу**, а не после истечения TTL | тест: отозвать → тот же процесс → отказ |
| 4 | **root не трогается `IAMService`**: его нет в `accounts`, versitygw собирает его из конфига (`authentication.go:44`) | тест: `GetUserAccount(root)` не читает `accounts`, поведение root не меняется |
| 5 | CI: проверки доступа идут между **двумя** учётками одного клиентского тенанта | e2e не скипнут и зелёный |

## Готовность

- `go build ./... && go test ./...` — чисто; `go vet`, `staticcheck`, `gofmt -l .` — пусто.
- Тесты с `TAPEGW_TEST_DSN='postgres://tapegw:tapegw@localhost:55432/phobos?search_path=tapegw_test'`
  (контейнер `tapegw-pg`) обязаны **выполняться**, а не скипаться.
- e2e зелёный и не скипнут.
- Каждый коммит собирается и проходит тесты, а не только HEAD.
