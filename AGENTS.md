# tapegw — правила для агента

Источник истины — [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md). Этот файл — краткая
выжимка плюс команды. При любом расхождении выигрывает `ARCHITECTURE.md`, а отступление
оформляется ADR в `docs/adr/`.

## Команды

```bash
go build ./...                 # сборка всего (без cgo, Phobos не нужен)
go test ./...                  # юнит-тесты (без cgo, без Postgres для чистых пакетов)
go test -race ./...            # проверка гонок, обязательно для изменений очередей
go vet ./...
staticcheck ./...              # требует go install honnef.co/go/tools/cmd/staticcheck@latest
gofmt -l .                     # должен печатать пусто

# Сборка с реальным Phobos (только стенд ci-dir / контейнер EL9)
go build -tags phobos ./...

# Миграции применяются при старте tapegw, отдельно запускать не нужно
```

Тесты, которым нужен Postgres, пропускаются сами, если `TAPEGW_TEST_DSN` не задан.
Чтобы они всё-таки прогнаться:

```bash
docker run -d --name tapegw-pg -p 55432:5432 \
  -e POSTGRES_USER=tapegw -e POSTGRES_PASSWORD=tapegw -e POSTGRES_DB=phobos \
  postgres:16-alpine

TAPEGW_TEST_DSN='postgres://tapegw:tapegw@localhost:55432/phobos?search_path=tapegw_test' \
  go test -race -count=1 ./...
```

E2E-тесты (`test/e2e`) пропускаются без `TAPEGW_E2E_ENDPOINT`. Полный прогон:

```bash
# 1. поднять tapegw (см. readme.md «Быстрый старт»)
# 2
go build -o /tmp/tapegw ./cmd/tapegw && /tmp/tapegw -config /tmp/tapegw.yaml &
# 3
TAPEGW_E2E_ENDPOINT=http://127.0.0.1:7070 \
TAPEGW_E2E_ACCESS=testaccess TAPEGW_E2E_SECRET=testsecret \
  go test -count=1 ./test/e2e -v
```

Оба набора с DSN и endpoint гоняет CI (`.github/workflows/ci.yml`) — локально
прогонять их не обязательно, но перед PR по очередям и транзакциям стоит.

## Железные правила

Нумерация совпадает один в один с разделом «Репозиторий и правила для агента» в
`docs/ARCHITECTURE.md` — на эти номера можно ссылаться в отчётах и в описаниях PR.

1. **Go — версия закреплённой версии versitygw** (сейчас `go 1.26.0`). versitygw
   фиксируется точной версией в `go.mod`, обновление — отдельный PR.
2. **`go build ./... && go test ./...` проходят на любом Linux** без Phobos и без
   cgo. Всё, что требует Phobos, — под build tag `phobos` и в стенд `ci-dir`.
3. **Исходники Phobos и versitygw не патчатся.** Ни патчей, ни форков, ни `replace`
   в `go.mod` на свою ветку. Если без патча нельзя — сначала ADR и вопрос ревьюеру.
4. **Неподдерживаемые S3-операции — `NotImplemented`** через встраивание
   `backend.BackendUnsupported`, а не «молча делаем что-то».
5. **Поведение S3 берём из кода versitygw, не выдумываем.** Для обычных операций —
   posix-бэкенд, для Glacier-семантики (`x-amz-restore`, storage class,
   RestoreObject) — `backend/scoutfs/scoutfs_compat.go`.
6. **Сначала файл на диске с `fsync`, потом commit транзакции Postgres. Никогда
   наоборот.** Любая смена состояния объекта — одна транзакция. Исключение с
   обратным порядком: вытеснение файла (снять флаг в БД → `unlink`), см. ADR про
   гонку GET и eviction.
7. **Каждый воркер идемпотентен.** Переход состояния —
   `UPDATE … WHERE state = <ожидаемое>`; ноль затронутых строк означает «опередили»,
   воркер молча пропускает. Для каждого нового шага воркера обязателен тест
   «упасть после шага N» на `store/fake`.
8. **Ошибки** через `fmt.Errorf("…: %w", err)`, **логи** через `log/slog` (JSON),
   `panic` только в `main`.
9. **Числа — в конфиг, не константами в коде**: таймауты, размеры батчей, watermark,
   `max_attempts`. Любое поле переопределяется переменной `TAPEGW_*` (имя
   выводится из пути в YAML: `migrator.batch_max_wait` → `TAPEGW_MIGRATOR_BATCH_MAX_WAIT`).
10. **Один PR — один пункт плана** из раздела «План разработки» в `ARCHITECTURE.md`.
    В описании PR: что сделано, как проверено, отступления от документа со ссылкой
    на ADR.
11. **Миграции БД — только вперёд**, одна миграция на PR, уже слитые не правим.
12. **Лицензия — Apache-2.0.** Файлы, перенесённые из Phobos (в т.ч. `.proto`),
    сохраняют LGPL-заголовок.

13. **Ни одна транзакция Postgres не открыта во время вызова Phobos.** Воркер
    коммитит переход состояния, отпускает транзакцию и advisory-лок ключа, и
    только потом вызывает `phobos_put`/`phobos_get`; результат пишется отдельной
    транзакцией. Батч на 200 GiB идёт минуты, и висящая в это время транзакция
    держит и строки, и локи ключей — параллельный PUT тех же ключей упрётся в
    `db.advisory_lock_timeout`. Таймаут лока — вторая линия защиты, а не замена
    правила.

Два правила, которые в `ARCHITECTURE.md` не пронумерованы, но обязательны:

- **Очереди — строки в Postgres, выборка через `SELECT … FOR UPDATE SKIP LOCKED`.**
  Никаких отдельных брокеров.
- **cgo изолирован** в `internal/store/phobos`. Вызовы Phobos — только в пуле
  `phobos.max_inflight`, `runtime.KeepAlive` для `os.File`, C-строки в `defer`,
  структуры через `pho_xfer_desc_clean`.

## Версии

- Go — `1.26.0`, как `go.mod` versitygw. Ставится автоматически через `GOTOOLCHAIN`.
- versitygw зафиксирован точной версией в `go.mod`; обновление — отдельный PR с прогоном e2e.
- Phobos — релизный тег, собирается в контейнере EL9.

## Чего нет в MVP

Версионирование бакетов, Object Lock, lifecycle, S3 Select, несколько узлов, упаковка
мелких объектов, освобождение места на лентах после удалений. Всё это — отдельные ADR.
