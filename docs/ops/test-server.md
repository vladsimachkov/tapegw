# Тестовый сервер: сборка и обкатка Phobos / LTFS

Машина, на которой собираются Phobos и LTFS, поднимается стенд с виртуальными
ленточными драйвами и гоняются тесты tapegw с настоящей Postgres.

Документ — воспроизводимая последовательность. Всё, что здесь описано, проверено
на этой машине, а не переписано из чужих инструкций.

Здесь — как собрать машину. Про саму ленточную библиотеку (порядок запуска,
заведение лент и драйвов, теги, копии, разбор отказов) —
[`tape-library.md`](tape-library.md); там собираются тонкости, на которых мы
реально наступали.

## Железо и ОС

| | |
| --- | --- |
| ОС | AlmaLinux 9.8 (Olive Jaguar) |
| Ядро | `5.14.0-687.5.3.el9_8.x86_64` |
| vCPU / RAM | 8 / 15 GiB |
| Диск | 70 GiB (свободно ~67 GiB) |
| SCSI | `/dev/sg0`, `/dev/sg1` |

Доступ — по ключу, пароль не нужен:

```bash
ssh tapegw-vm        # 192.168.1.144, root
```

### Почему EL9, а не что-то другое

Phobos таргетит RHEL9, и `configure` ищет именно EL9-пакеты. Отдельная причина —
будущий `tcm_vtl` из mhVTL: это нетмейнлайновый модуль ядра, и под него нужно
дерево сборки, **точно** совпадающее с запущенным ядром.

## Сетевые ограничения — прочитать до первого `go build`

Снаружи открыты не все адреса. Это не мелочь, потому что иначе сборка Go выглядит
как сломанная:

| Адрес | Статус |
| --- | --- |
| `github.com` | работает |
| `proxy.golang.org` | **таймаут** |
| `goproxy.cn`, `goproxy.io` | **таймаут** |
| `mirrors.aliyun.com/goproxy/` | работает |

Поэтому на машине настроено:

```bash
go env -w GOPROXY=https://mirrors.aliyun.com/goproxy/,direct GOSUMDB=off
```

`GOSUMDB=off` — потому что `sum.golang.org` тоже недоступен. Это осознанный размен
на сборку в изолированной сети; если машина поедет в продакшн-сеть, стоит
вернуть sumdb и поднять внутренний зеркала-прокси вместо отключения проверки.

## Репозитории

```bash
dnf config-manager --set-enabled crb
dnf -y install epel-release
```

CRB обязателен: в нём лежат `libini_config-devel` и `sg3_utils-devel`, без которых
`configure` Phobos падает.

## Пакеты

```bash
dnf -y --skip-broken install "@RPM Development Tools" "@Development Tools" \
  autoconf automake libtool pkgconfig \
  glib2-devel jansson-devel libpq-devel openssl-devel libini_config-devel \
  sg3_utils-devel libtool-ltdl-devel libuuid-devel libattr-devel libaio-devel \
  postgresql-server postgresql-contrib protobuf-c-devel protobuf-c \
  python3 python3-devel python3-tabulate python3-pyyaml python3-clustershell \
  python3-psycopg2 e2fsprogs git curl fuse-devel userspace-rcu-devel golang
```

Три ловушки, в которые я уже наступил:

- **`--skip-broken` обязателен.** Одна отсутствующая ссылка роняет всю транзакцию
  молча: `dnf` пишет `Error: Unable to find a match: <pkg>` и не ставит ничего из
  списка. Так у меня отвалилась установка из-за `liburcu-devel` (в EL9 он называется
  `userspace-rcu-devel`).
- **Имя группы не `@development-tools`.** В EL9 это `@RPM Development Tools` и
  `@Development Tools`.
- **`postgresql-contrib` — не опция.** См. ниже про `btree_gin`.

### kernel-devel: точное совпадение

```bash
K=$(uname -r)
dnf -y install kernel-devel-$K kernel-headers-$K
ls -d /usr/src/kernels/$K    # должно существовать
```

Ставить без версии — лотерея: `dnf` тянет последнюю, и после обновления ядра
дерево перестанет совпадать с запущенным. Для `tcm_vtl` это фатально.

**Правило: после `dnf update kernel` всегда переустанавливать `kernel-devel` под
новое ядро, иначе модули перестанут собираться.**

## Postgres

Стоит `postgresql-server-13.23`. Две вещи, без которых Phobos не поднимется.

### 1. Расширение `btree_gin`

Схема Phobos (актуальная, 3.3) содержит:

```sql
host varchar(128),
...
CREATE INDEX ON device USING gin(host);
```

GIN не имеет opclass для `varchar` по умолчанию, и `phobos_db setup_tables` падает с

```
data type character varying has no default operator class for access method "gin"
```

Лечится до создания схем:

```bash
PGPASSWORD=phobos psql -h 127.0.0.1 -U phobos -d phobos_test \
  -c "CREATE EXTENSION IF NOT EXISTS btree_gin"
```

### 2. `pg_hba.conf`: первый match выигрывает

Phobos подключается по TCP с паролем, а в стоковом `pg_hba.conf` для хоста стоит
`ident`, и он стоит **раньше**. Дописанные в конец строки не сработают никогда —
`pg_hba` берёт первое совпадение. Строки нужно вставить **выше** системных:

```
# tapegw spike: Phobos ходит по паролю
local all phobos md5
host all phobos 127.0.0.1/32 md5
host all phobos ::1/128 md5
```

После правки — `systemctl reload postgresql`.

### Создание схем

```bash
su postgres -c "psql -c \"CREATE ROLE phobos LOGIN PASSWORD 'phobos' SUPERUSER\""
cd /tmp && su postgres -c "createdb -O phobos phobos_test"   # не из /root!
phobos_db setup_tables -s 3.3
```

- `cd /tmp` не для красоты: из `/root` у пользователя `postgres` нет прав на cwd, и
  `createdb` ругается `could not change directory to "/root"`.
- `-s 3.3` обязательно: без `-s` `setup_tables` берёт легаси-версию 1.1.
- `phobos_db setup_db` интерактивно спрашивает пароль и без TTY падает
  (`EOFError`). В скриптах — только `setup_tables`.

### Схема для тестов tapegw

```bash
PGPASSWORD=phobos psql -h 127.0.0.1 -U phobos -d postgres \
  -c "CREATE DATABASE tapegw_test OWNER phobos"

export TAPEGW_TEST_DSN="postgres://phobos:phobos@127.0.0.1/tapegw_test?search_path=tapegw_test&sslmode=disable"
```

Без `sslmode=disable` pgx отказывается подключаться к локальному серверу.

## Phobos

```bash
cd /root && git clone https://github.com/cea-hpc/phobos.git
cd phobos && ./autogen.sh && ./configure --prefix=/usr --sysconfdir=/etc
make -k -j$(nproc) && make -k install
```

**`-k` обязателен.** Два шага падают всегда и оба нам не мешают:

```
pho_test_utils.c: fatal error: cmocka.h: No such file or directory
make[1]: *** [Makefile:602: gen-man.stamp] Error 1
```

`cmocka-devel` нет ни в baseos, ни в CRB, ни в EPEL для EL9 — это юнит-тесты
самого Phobos. `gen-man` — генерация man-страниц. С `-k` собирается всё
функциональное: `libphobos_store`, `libpho_core`, `phobosd`, `phobos_tlc`,
модули layout/ldm/io.

### Конфиг `/etc/phobos.conf`

```ini
[dss]
connect_string = dbname=phobos_test host=127.0.0.1 user=phobos password=phobos

[lrs]
server_socket = /tmp/socklrs
sync_time_ms = tape=10000,dir=10,rados_pool=10
sync_nb_req = tape=1,dir=1,rados_pool=5
sync_wsize_kb = tape=1048576,dir=1048576,rados_pool=1048576

[store]
fs_root = /var/lib/phobos
default_dir_library = dirlib

[profile "simple"]
family = dir
layout = raid1
lyt-params = repl_count=1
library = dirlib
```

`default_dir_library` и `library` в профиле обязательны, иначе PUT падает с
`No such device`.

### Запуск демона

```bash
mkdir -p /run/phobosd /var/lib/phobos
export DAEMON_PID_FILEPATH=/run/phobosd/phobosd.pid
setsid nohup phobosd > /tmp/phobosd.log 2>&1 < /dev/null &
```

`DAEMON_PID_FILEPATH` без выставленной переменной — `phobosd` стартует и молча
не работает.

> **Никогда не `pkill -f phobosd` внутри ssh-команды.** Паттерн `-f` матчится на
> всю командную строку, включая саму команду `bash -c '... phobosd ...'`, и процесс
> убивает сессию, которая его запустила. Использовать `pkill -x phobosd`.

### dir-носитель: два шага, которые легко пропустить

Это стоило мне целого захода в контейнере, где казалось, что «Phobos не умеет dir».

```bash
phobos dir add /var/lib/phobos -L dirlib
phobos dir format /var/lib/phobos
phobos dir unlock /var/lib/phobos        # <-- без этого PUT даёт ENOSPC
```

`dir format` пишет в каталог файл-метку `.phobos_dir_label` и заполняет статистику
свободного места. Без метки `dir_present()` возвращает `-ENOENT`, носитель остаётся
в `fs_status = blank` с `phys_spc_free = 0`.

А после форматирования носитель **залочен**, а аллокатор ищет `unlocked`:

```
No medium found matching query: {"$AND":[{"DSS::MDA::family":"dir"},
{"DSS::MDA::put":"t"},{"DSS::MDA::adm_status":"unlocked"},
{"$OR":[{"DSS::MDA::fs_status":"used"},{"DSS::MDA::fs_status":"empty"}]},
{"DSS::MDA::library":"dirlib"}]}
```

Эта строка в `/tmp/phobosd.log` — единственный способ понять, что не так: CLI в
ответ показывает только `No space left on device`.

Проверка состояния:

```bash
PGPASSWORD=phobos psql -h 127.0.0.1 -U phobos -d phobos_test \
  -c "select family,adm_status,put,fs_status,library,stats->>'phys_spc_free' from media"
```

Хорошее состояние: `dir | unlocked | t | empty | dirlib | 71201673216`.

### Проверка

```bash
phobos put -p simple /tmp/big.bin oid/big
phobos get oid/big /tmp/big.back
md5sum /tmp/big.bin /tmp/big.back      # совпали
phobos delete oid/big
```

Прогонялось на 64 MiB случайных данных — байт-в-байт.

Отдельно: у `phobos put` профиль задаётся флагом `-p`, а `phobos get` флаг `-p`
**не принимает**. Подкоманды `phobos admin` в 3.4.1 нет вообще.

## Что дальше

- mhVTL: собрать `tcm_vtl` под текущее ядро, поднять виртуальные драйвы, проверить
  LTFS (`mkltfs`/`ltfs`/`umount`). `libltfs` в репозиториях EL9 **нет** — только
  сборка из исходников <https://github.com/LinearTapeFileSystem/ltfs>.
- Спайк 2: минимальная cgo-программа через `libphobos_store` — библиотека уже
  собрана и установлена.
