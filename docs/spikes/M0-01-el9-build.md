# M0-01 — Phobos в контейнере EL9

**Статус: сборка и запуск подтверждены; последняя миля (выделение места под dir-носитель)
не решена.** Phobos `2e3311f` (pkg 3.4.1), Rocky Linux 9, Postgres 13.23, Docker
`--privileged`.

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

### Что не решено

Выделение места под dir-носитель не работает:

```
<ERROR> No compatible device found for write alloc: No such device (19)
```

а после того, как в профиле указана `library`, ошибка меняется на

```
<ERROR> Error while sending response to layout for encoder 0: No space left on device
```

При этом устройство в БД есть и выглядит живым:

```
| adm_status | unlocked | device | /var/lib/phobos | library | dirlib | media | /var/lib/phobos |
```

Корень в том, что `media.fs_status` остаётся `blank`, а `stats->phys_spc_free` —
нулевым. Пробовал:

- `/var/lib/phobos` на overlayfs корня контейнера — `blank`, free 0;
- отдельный loop-mount ext4 (`/mnt/phodir`, 907 MiB свободно по `df`) — тоже
  `blank`, free 0;
- вручную `update media set fs_status='empty', stats=...phys_spc_free=1e11` —
  демон пересчитывает и возвращает ноль, ошибка та же.

То есть демон не заполняет статистику свободного места dir-носителя. Чего-то в
конфиге не хватает — предположительно из `[family]` (`dir_full_threshold` по
умолчанию 1) или из связки LRS/HSM, — но за отведённое время я это не нашёл.

**Это не блокер для tapegw**, потому что dir-семейство нам нужно только как
тренировочный стенд без ленты, а не как продакшн-путь. Но спайк на этом не закрыт.

## Что дальше

1. Спросить у авторов Phobos / посмотреть их CI, чем заполняется `phys_spc_free` у
   dir-носителя. Скорее всего есть пропущенный шаг «отформатировать/смонтировать
   носитель», который в тестовом наборе Phobos делает отдельный скрипт.
2. Как только dir заработает — закрыть спайк 2 (минимальная cgo-программа через
   `libphobos_store`), потому что библиотека уже собрана и лежит в `/usr/lib64`.
3. Образ со всей этой последовательностью стоит оформить как `ci-dir` из плана M0,
   иначе каждый будет наступать на `btree_gin` и `cmocka`.

## Побочный результат: mhVTL (спайк 7)

Хост-окружение, в котором я работал, грузит модули ядра: `target_core_mod`,
`tcm_loop`, `target_core_user`, `scsi_debug` присутствуют, `modprobe scsi_debug`
проходит и создаёт `/dev/sg0`. То есть стенд с виртуальными SCSI-устройствами тут
реален. `mhVTL` требует нетмейнлайновый `tcm_vtl`, поэтому путь, скорее всего, —
`tcm_loop` + `fileio` через LIO, а не mhVTL. Отдельный тест LTFS на таких драйвах
ещё не выполнен.
