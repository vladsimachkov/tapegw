# M0-01 — Phobos в контейнере EL9

**Статус: закрыт.** Phobos `2e3311f` (pkg 3.4.1) собирается в EL9 без патчей и делает
put/get/delete на семействе `dir`. Первая половина проверялась в контейнере Rocky
Linux 9 (Docker `--privileged`, Postgres 13.23), последняя миля — на реальной
AlmaLinux 9.8. Разница оказалась не в ОС, а в двух пропущенных командах; см. раздел
«Что не решено в контейнере».

Пошаговая воспроизводимость настройки машины — в
[docs/ops/test-server.md](../ops/test-server.md).

## Что проверяли

Собирается ли Phobos из исходников в EL9-контейнере и делает ли `phobos
put/get/delete` на семействе `dir` — то есть нужен ли нам отдельный образ с
Phobos и можно ли вообще поднимать стек без ленты.

## Что получилось

### Сборка: да, целиком

Единственные две неудачи в `make`:

```
pho_test_utils.c:35:10: fatal error: cmocka.h: No such file or directory
make[1]: *** [Makefile:602: gen-man.stamp] Error 1
```

`cmocka-devel` нет ни в baseos, ни в CRB, ни в EPEL для EL9 — это только юнит-тесты
Phobos, для нас нерелевантно. `gen-man` — генерация man-страниц. Всё функциональное
соберётся, если собирать `make -k`.

Собрано и установлено:

```
/usr/lib64/libphobos_store.so.0.0.0     <- API слоя данных
/usr/lib64/libpho_core.so.0.0.0
/usr/sbin/phobosd
/usr/sbin/phobos_tlc
/usr/bin/phobos                          <- python CLI
модули layout raid1/raid4, ldm scsi_tape/ltfs/dir/posix, io ltfs/posix
```

То есть и `libphobos_store`, и `phobos_tlc`, и LTFS-модули компилируются в EL9 без
патчей.

### Полный список зависимостей, который пришлось доставить

```
gcc make autoconf automake libtool pkgconfig which
glib2-devel jansson-devel libpq-devel openssl-devel ini-config-devel
libini_config-devel        # в CRB, не в baseos
sg3_utils-devel            # кривое имя: пакет называется sg3_utils-devel
cmocka-devel               # НЕТ в EL9/EPEL — только тесты
libtool-ltdl-devel libuuid-devel libattr-devel libaio-devel
postgresql-server postgresql-contrib   # contrib обязателен, см. ниже
python3 python3-devel python3-tabulate python3-pyyaml
python3-clustershell python3-psycopg2
e2fsprogs
```

Отдельно: `configure` падает с невнятным «Cannot find python», если нет `which`, и с
«Could not link test program to Python», если нет `openssl-devel` (реальная ошибка в
`config.log` — `/usr/bin/ld: cannot find -lcrypto`, к Python отношения не имеет).

### Схема БД: нужен `btree_gin`

Это главный практический вывод. `phobos_db setup_tables` падает с

```
psycopg2.errors.UndefinedObject: data type character varying has no default
operator class for access method "gin"
```

Причина в актуальной схеме 3.3, а не в легаси:

```sql
CREATE TABLE device(
    ...
    host            varchar(128),
    ...
);
CREATE INDEX ON device USING gin(host);
```

GIN не имеет opclass для `varchar` по умолчанию. Лечится расширением:

```sql
CREATE EXTENSION btree_gin;
```

После этого `phobos_db setup_tables -s 3.3` поднимает 10 таблиц. **Значит в нашем
образе Postgres обязан быть `postgresql-contrib`, а в инициализации БД —
`CREATE EXTENSION btree_gin`.** Это стоит зафиксировать в развёртывании, иначе
стенд не поднимется с невнятной ошибкой.

Ещё две мелочи про схему: `setup_tables` без `-s` берёт версию 1.1, у которой та же
проблема; `phobos_db setup_db` интерактивно спрашивает пароль и не работает без
TTY — в скриптах нужен `setup_db -d ... -u ... -p ... -s`.

### Демон и CLI: работают

`phobosd` требует `DAEMON_PID_FILEPATH` и каталог `/run/phobosd`, иначе стартует
молча-втихую и вешается на мёртвом сокете. После этого CLI подключается:

```
$ phobos dir add /mnt/phodir -L dirlib
Added 1 dir(s) successfully
```

Важно про сам CLI: в 3.4.1 **нет подкоманды `admin`** — набор целей это
`copy dir drive extent lib lock logs object phobosd rados_pool sched stats tape
tlc delete del locate get getmd mput put rename undelete undel`. Настройка БД
вынесена в `phobos_db`. В чек-листе M0 в `ARCHITECTURE.md` пункт написан как
`phobos put/get/delete`, но в плане обряда развёртывания я по инерции писал
`phobos admin setup` — так делать нельзя.

Ещё: `phobos put` **не использует профиль по умолчанию** так, как ожидаешь. Без `-p`
выдача одна, с `-p simple` — другая, и `phobos get` флаг `-p` не принимает вообще
(«unrecognized arguments»). Это надо учитывать в обряде развёртывания.

### Что не решено в контейнере — решено на реальной ОС

В контейнере выделение места под dir-носитель не работало: `media.fs_status`
оставался `blank`, `stats->phys_spc_free` — нул, а PUT отвечал `No space left on
device`. Я перепробовал overlayfs, отдельный loop-mount ext4 и ручную правку
строки `media` — демон пересчитывал и возвращал ноль.

Причина оказалась не в ОС и не в конфигурации, а в **двух пропущенных шагах**,
которые в контейнере я не сделал:

```bash
phobos dir format /var/lib/phobos
phobos dir unlock /var/lib/phobos
```

`dir format` пишет в каталог файл-метку `.phobos_dir_label`
(`src/ldm-modules/ldm_fs_posix.c:102`) и заполняет структуру свободного места.
Пока метки нет, `dir_present()` возвращает `-ENOENT` («dir is present but we
can't get any label»), носитель остаётся `blank` и аллокатор его не видит.

После форматирования носитель **залочен**, а запрос аллокатора требует
`adm_status = unlocked` — отсюда и был `ENOSPC`. Полное состояние после двух
шагов:

```
dir | unlocked | t | empty | dirlib | phys_spc_free = 71201673216
```

Ключ к разгадке дал не CLI, а `/tmp/phobosd.log`: демон печатает весь
DSS-запрос-критерий целиком,

```
No medium found matching query: {"$AND":[{"DSS::MDA::family":"dir"},
{"DSS::MDA::put":"t"},{"DSS::MDA::adm_status":"unlocked"},
{"$OR":[{"DSS::MDA::fs_status":"used"},{"DSS::MDA::fs_status":"empty"}]},
{"DSS::MDA::library":"dirlib"}]}
```

тогда как со стороны CLI видно только `No space left on device`. **Вывод для
диагностики: при проблемах с выделением места смотреть лог демона, а не вывод
CLI.**

Полный цикл на AlmaLinux 9.8 работает:

```
PUT  oid/big (64 MiB случайных данных)   -> succeeded
GET  oid/big                              -> md5 совпал
DELETE oid/big                            -> succeeded, GET после него отдаёт ENOENT
```

Так что **спайк 1 закрыт**: Phobos 3.4.1 собирается в EL9 без патчей и делает
put/get/delete на семействе `dir`. Пошаговая воспроизводимость — в
[docs/ops/test-server.md](../ops/test-server.md).

## Что дальше

1. Спайк 2 — минимальная cgo-программа через `libphobos_store`: библиотека уже
   собрана и установлена на тестовом сервере. Линковка с libphobos принята,
   см. [ADR-0006](../adr/0006-libphobos-linkage.md).
2. Оформить образ `ci-dir` из плана M0 по мотивам
   [docs/ops/test-server.md](../ops/test-server.md), иначе каждый будет наступать
   на `btree_gin`, `cmocka` и `dir format`.

## Побочный результат: mhVTL (спайк 7)

Хост-окружение, в котором я работал, грузит модули ядра: `target_core_mod`,
`tcm_loop`, `target_core_user`, `scsi_debug` присутствуют, `modprobe scsi_debug`
проходит и создаёт `/dev/sg0`. То есть стенд с виртуальными SCSI-устройствами тут
реален. `mhVTL` требует нетмейнлайновый `tcm_vtl`, поэтому путь, скорее всего, —
`tcm_loop` + `fileio` через LIO, а не mhVTL. Отдельный тест LTFS на таких драйвах
ещё не выполнен.
