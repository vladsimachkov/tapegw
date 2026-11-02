# tapegw

S3-сервис с Glacier-семантикой поверх LTO-лент.

Данные живут на лентах под управлением [Phobos](https://github.com/cea-hpc/phobos),
S3-поверхность даёт встроенный [versitygw](https://github.com/versity/versitygw).
Клиенты пишут обычным `PUT` и получают `200` сразу, не дожидаясь ленты; диск — только
буфер. Чтение архивного объекта идёт через `RestoreObject`, как в AWS Glacier.

**Проект на стадии проектирования.** Реализации пока нет; полная архитектура, схема БД,
конечный автомат объекта, план разработки (M0–M9) и правила для агента лежат в
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Зачем

Лента дешевле дисков, но недоступна по S3 из коробки. tapegw закрывает разрыв: обычный
S3-протокол снаружи, асинхронная архивация на LTA внутри. MVP — один узел, одна
библиотека, одна копия на ленте.

## Компоненты

Два собственных бинаря, остальное — стоковые зависимости:

| Бинарь | Назначение |
| --- | --- |
| `tapegw` | S3-слой (backend для versitygw) и воркеры: migrator, recaller, janitor, deleter |
| `klipper-tlc` | Манипулятор ленточной библиотеки как TLC-демон для Phobos |

Используются как есть: versitygw, Phobos, LTFS, Postgres, Klipper/Moonraker.

## Статус этапов

| Этап | О чём | Статус |
| --- | --- | --- |
| M0 | Спайки: cgo Phobos, протокол TLC, стенд ci-dir | не начат |
| M1 | Скелет: конфиг, embedgw, миграции, бакеты | **сделан** |
| M2 | Онлайн-путь без ленты: PUT/GET/LIST/DELETE | не начат |
| M3 | Multipart | не начат |
| M4 | Архивация на ленту | не начат |
| M5 | Restore — полный Glacier-цикл | не начат |
| M6 | Удаление и сборка мусора | не начат |
| M7 | klipper-tlc на mhVTL | каркас CLI |
| M8 | Реальный манипулятор | не начат |
| M9 | Эксплуатация: метрики, DR, runbook | не начат |

M1 закрыт в объёме критерия готовности: `CreateBucket` / `HeadBucket` /
`ListBuckets` / `DeleteBucket`, ACL и tagging бакетов работают против живого
tapegw — это проверяет `test/e2e`. Джоб CI со сборкой `-tags phobos` отложен до
M0: он требует контейнер EL9 с Phobos и до M0 был бы вечно красный.

## Быстрый старт

Нужен Postgres и Go: тулчейн 1.26 подтянется сам через `GOTOOLCHAIN`.

```bash
docker run -d --name tapegw-pg -p 55432:5432 \
  -e POSTGRES_USER=tapegw -e POSTGRES_PASSWORD=tapegw -e POSTGRES_DB=phobos \
  postgres:16-alpine

cat > /tmp/tapegw.yaml <<'YAML'
s3:
  listen: [":7070"]
db:
  dsn: postgres://tapegw:tapegw@localhost:55432/phobos?search_path=tapegw
root:
  access: testaccess
  secret: testsecret
YAML

go run ./cmd/tapegw -config /tmp/tapegw.yaml
# миграции применяются на старте, схема tapegw создаётся автоматически
curl -s localhost:7070/health   # OK
```

Без подписи S3 отвечает `403 AccessDenied` — это правильно, SigV4 проверяется
штатным кодом versitygw.

## Разработка

Требования и команды — в [`AGENTS.md`](AGENTS.md). Кратко:

```bash
go build ./... && go test ./...
```

проходит на любом Linux без Phobos и без cgo. Всё, что требует Phobos, — под build tag
`phobos` и в стенд `ci-dir`.

## Лицензия

Apache-2.0 — см. [LICENSE](LICENSE). Файлы, перенесённые из Phobos, сохраняют
заголовок LGPL-2.1.
