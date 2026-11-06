#!/usr/bin/env bash
# Поднимает ci-dir с нуля: Postgres, схема Phobos, носитель dir, phobosd.
#
# Порядок и обязательные шаги — из docs/spikes/M0-01-el9-build.md:
#   * btree_gin обязателен: без него setup_tables падает на gin(host) с
#     невнятным «data type character varying has no default operator class»;
#   * phobos_db setup_db спрашивает пароль интерактивно, поэтому роль и БД
#     создаём сами;
#   * phobosd требует DAEMON_PID_FILEPATH и /run/phobosd.
#
# initdb и postmaster отказываются работать под root, поэтому всё, что
# касается Postgres, идёт под пользователем postgres.
set -euo pipefail

PGDATA=/var/lib/pgsql/data
export DAEMON_PID_FILEPATH=/run/phobosd/phobosd.pid

as_pg() { su -s /bin/bash postgres -c "$1"; }

mkdir -p "$PGDATA" /var/run/postgresql && chown postgres:postgres "$PGDATA" /var/run/postgresql && chmod 700 "$PGDATA"
as_pg "initdb -D $PGDATA" >/dev/null
as_pg "pg_ctl -D $PGDATA -l /tmp/pg.log -o '-c listen_addresses=127.0.0.1' start" >/dev/null
for _ in $(seq 1 30); do
    as_pg "pg_isready -h 127.0.0.1" >/dev/null 2>&1 && break
    sleep 1
done

as_pg "psql -v ON_ERROR_STOP=1 -c \"CREATE ROLE phobos LOGIN PASSWORD 'phobos'\""
as_pg "psql -v ON_ERROR_STOP=1 -c 'CREATE DATABASE phobos OWNER phobos'"
as_pg "psql -d phobos -v ON_ERROR_STOP=1 -c 'CREATE EXTENSION btree_gin'"

# setup_tables берёт подключение из /etc/phobos.conf ([dss] connect_string),
# ключей -d/-u/-p у него нет — они есть у setup_db.
phobos_db setup_tables -s 3.3

# Порядок: add (чистая запись в DSS, демон не нужен) → phobosd → format
# (административная операция через LRS, без демона даёт «Transport endpoint is
# not connected»). Обратный порядок — phobosd стартует без единого устройства и
# падает на инициализации планировщика с EINVAL.
# Второй носитель нужен, чтобы считать реплики: у dir носитель — обычный
# каталог, и «реплика в оба» отличается от «реплика в один» подсчётом файлов.
# Он же делает тест «параметры layout передаются» отличным от «их проигнорировали»:
# с одним носителем raid1 по умолчанию падает и с параметрами тоже не проходит.
mkdir -p /var/lib/phobos/dir1 /var/lib/phobos/dir2
phobos dir add --unlock -L dirlib /var/lib/phobos/dir1 /var/lib/phobos/dir2

phobosd
for _ in $(seq 1 30); do
    ss -xlp 2>/dev/null | grep -q socklrs && break
    sleep 1
done

phobos dir format --unlock --fs POSIX /var/lib/phobos/dir1 /var/lib/phobos/dir2

echo "ci-dir: готов"

# Держим контейнер живым: демоны форкаются, основного процесса не остаётся.
exec sleep infinity
