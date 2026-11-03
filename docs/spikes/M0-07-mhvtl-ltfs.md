# M0-07. mhVTL + LTFS на виртуальных драйвах

- **Статус:** закрыт: стек работает, цикл замерен, причина free = 0 найдена и оказалась артефактом стенда
- **Стенд:** AlmaLinux 9.8, ядро `5.14.0-687.5.3.el9_8.x86_64`, 8 vCPU / 15 GiB
- **Связано:** [ADR-0007](../adr/0007-mover-worker.md),
  [ADR-0008](../adr/0008-object-medium.md),
  [`docs/ops/test-server.md`](../ops/test-server.md)

## Цель

Проверить, что стек «ленточная библиотека → LTFS → Phobos» собирается и работает
на машине без ленточного железа, и замерить стоимость цикла
«umount LTFS → выгрузка → загрузка в другой драйв → mount LTFS». Замер нужен для
таймаута супервизора mover'а (ADR-0007) и для разумного размера батча.

Отдельная задача из ревью — два mover-процесса на двух драйвах.

## Что поднялось

**mhVTL** — модуль ядра собирается под EL9 **с первого раза**:

```
scsi host3: mhvtl: version 0.18.43 [20260830-0], opts=0x1
```

Модуль называется `mhvtl`, а не `tcm_vtl` — в исходном плане я ошибся термином,
`fujitsu/tcm_vtl` не существует. Актуальный репозиторий — `markh794/mhvtl`.
Для userspace-части не хватило `systemd-devel`, `lz4-devel`, `libicu-devel`,
`icu`.

Итог — 8 виртуальных ленточных драйвов и две библиотеки:

```
[3:0:1:0] tape     IBM ULT3580-TD8   /dev/st1  /dev/sg3
[3:0:2:0] tape     IBM ULT3580-TD8   /dev/st0  /dev/sg2
[3:0:0:0] mediumx  STK L700          /dev/sch0 /dev/sg10
```

**LTFS 2.4.9** в EL9 тоже собирается, но с оговорками:

- `genrb` нужен на сборку — он в пакете `icu`, а не в `icu-devel` (его в EL9 нет
  вовсе, есть `libicu-devel`);
- SNMP отключается явным `--disable-snmp`;
- обязательна `git submodule update --init --recursive` (uthash), иначе сборка
  падает на `uthash_submodule/src/uthash.h`.

Виртуального бэкенда в LTFS **нет** — только `sg`-драйверы поверх реальных
SCSI-лент. Поэтому mhVTL здесь не опционален, а необходим.

## LTFS на виртуальной ленте работает

```
$ mkltfs -d /dev/sg2 -n TAPEGW01
LTFS15024I Medium formatted successfully.

$ ltfs -o devname=/dev/sg2 /mnt/tape2
$ df -h /mnt/tape2
ltfs:/dev/sg2   119M   33M   86M  28%
```

Синтаксис монтирования коварный: позиционный аргумент у `ltfs` — **только точка
монтирования**, устройство задаётся через `-o devname=`. Без него LTFS берёт
устройство по умолчанию `0` и падает с невнятным `Cannot open device '0'`.

## Замер цикла

`measure-ltfs-cycle.sh`, три прогона, разброс минимальный:

| шаг | время |
|---|---|
| umount LTFS | 0.02 с |
| выгрузка в слот (MOVE MEDIUM) | 0.06 с |
| загрузка в другой драйв | 0.05 с |
| mount LTFS | 1.08 с |
| **цикл целиком** | **1.2 с** |

Целостность подтверждена (`verify-ltfs-integrity.sh`): файл случайных данных
32 MiB пережил переезд между драйвами, md5 совпал.

**Что из этого следует, а что нет.** Виртуальная лента не мотается, поэтому
1.2 с — это **нижняя граница программных накладных расходов**, а не реальное
число. Реальный цикл добавит перемотку и физический захват картриджа, десятки
секунд. Практический вывод отсюда другой и он устойчив к виртуальности:

- **`mount` даёт ~90 % времени цикла** — он читает индекс LTFS с ленты. Значит
  время жизни смонтированной ленты надо максимизировать: примонтировал — сделал
  весь батч — отмонтировал, а не монтирование на каждый объект.
- Таймаут супервизора mover'а (ADR-0007) обязан измеряться **десятками минут**, а
  не секундами: к программным 1.2 с добавятся перемотка и `umount` с записью
  индекса. Ошибка в эту сторону означает, что супервизор будет убивать
  здоровые операции.
- Граница «`batch_max_bytes` — это верхняя граница задержки restore» (ADR-0007)
  на реальном железе становится жёсткой: батч на целую ленту означает ожидание
  restore на всю запись ленты плюс цикл перестановки.

## free = 0: первый диагноз был неверен, причина в стенде

Первая версия этого отчёта утверждала, что LTFS-волсёр ограничен шестью
символами и поэтому Phobos не узнаёт носитель `E01001L8`. **Это неверно**, и
опровергается кодом Phobos.

Штатная команда форматирования (`src/ldm-modules/ldm_fs_ltfs.c` →
`/usr/sbin/pho_ldm_helper format_ltfs`) делает так:

```bash
local serial=${label:0:6}
mkltfs -f -d "$drive" -n "$label" -s "$serial"
```

То есть Phobos **сам** обрезает volser до шести символов и осознанно, а полную
метку пишет в имя тома (`-n`). Проверка метки в `ldm_fs_ltfs.c` идёт по
`user.ltfs.volumeName` — по имени тома, а не по volser:

```c
#define LTFS_VNAME_XATTR    "user.ltfs.volumeName"
...
rc = getxattr(mnt_path, LTFS_VNAME_XATTR, fs_label, llen - 1);
...
if (strcmp(vol_label, fs_label)) { ... }
```

Так что `ltfsck → Volser: E01001` при метке `E01001L8` — штатное поведение, а не
расхождение. С обычными 8-символьными LTO-баркодами в настоящих библиотеках
Phobos работает именно так. **Ограничения на именование носителей нет, в план
его заносить не нужно.**

### Настоящая причина: картриджи mhVTL сообщают LTFS нулевую ёмкость

Проверки по порядку.

**1. `cmd_format`.** В `/etc/phobos.conf` на стенде параметра нет, но дефолт
зашит в код (`PHO_LDM_HELPER" format_ltfs \"%s\" \"%s\""`), helper лежит по
`/usr/sbin/pho_ldm_helper`. Не причина.

**2. Имя тома на ленте.** См. выше — сверка по xattr, volser тут ни при чём.

**3. Ёмкость. Вот здесь корень.** Из debug-лога phobosd во время форматирования:

```
LTFS15019I Volume capacity is 0 GB.
Formatted media, available space: 0 GB
```

`mkltfs` сообщает нулевую ёмкость, Phobos читает `free = 0` и оставляет
`fs_status = empty` — носитель непригоден для размещения.

Проверено на свежем картридже, созданном с явным размером:

```bash
mktape -l 10 -m TST001 -s 5120 -t data -d LTO8
```

после `phobos tape format`:

```
LTFS15019I Volume capacity is 4 GB.
Formatted media, available space: 4 GB
```

```
   id   | fs_status |    free
--------+-----------+------------
 TST001 | empty     | 4294967296
```

Картриджи, которые `mhvtl-media.service` создал при старте без явного размера,
ёмкость LTFS не сообщают. Это **артефакт стенда, а не блокер M5**.

**4. Debug-лог** заодно показал вторую, отдельную ошибку на пути `put`:

```
No compatible device found for write alloc: No such device (19)
```

при том что `fs_status = empty` — штатное записываемое состояние
(`PHO_FS_STATUS_EMPTY` = «отформатирована, данных нет»). Причина в привязке
драйвов, см. проверку 5.

**5. Привязка драйвов.** Номера `/dev/stN` и `/dev/sgN` **меняются при каждом
перезапуске mhVTL**:

```
после 1-го запуска:  changer sg10, драйвы st1/sg3 и st0/sg2
после 2-го запуска:  changer sg11, драйвы st3/sg5 и st2/sg4
после 3-го запуска:  changer sg11, драйвы st0/sg2 и st1/sg3
```

Я зарегистрировал драйвы как `/dev/st1` и `/dev/st0`, после перезапуска эти пути
стали указывать на **другие приводы** (STK T10000B и ULT3580-TD6). Снимать
такие записи Phobos уже не даёт — `drive delete` отвечает `No such device`.
Единственный стабильный путь — `/dev/tape/by-id/`, он именован по серийнику
привода:

```
/dev/tape/by-id/scsi-00XYZZY_A1 -> ../../st0
/dev/tape/by-id/scsi-SSTK_L700_XYZZY_A-changer   # и changer тоже
```

### Порядок запуска: phobosd обязан стартовать после phobos_tlc

Это стоило больше всего времени и в первой версии отчёта выглядело как «Phobos
сломался». На самом деле всё детерминировано:

```
<ERROR> Socket connection(localhost:20123) failed: Connection refused (111)
<ERROR> Cannot contact 'TLC': will abort: Connection refused (111)
<ERROR> No functional device found: No such device or address (6)
```

Если `phobosd` стартует раньше, чем `phobos_tlc` начал слушать 20123, он
**отказывается загружать устройства и не повторяет попытку** — ни `lib refresh`,
ни `drive unlock` уже не помогают, `drive status` показывает драйвы с пустыми
`device` и `serial`. Помогает только перезапуск `phobosd`.

Второе обязательное условие: **носитель, физически лежащий в драйве, должен быть
зарегистрирован в DSS до старта `phobosd`**, иначе поток этого драйва умирает:

```
Device '/dev/sg3' (S/N '00XYZZY_A2') contains medium (family 'tape', name
'TST002', library 'vtllib'), but this medium cannot be found: No such device
or address (6)
device thread ... terminated with error
```

Для tapegw это означает конкретные требования к деплою: `phobosd.service` обязан
иметь `After=phobos_tlc.service` и `Requires=`, а порядок «зарегистрировать
носители → запустить phobosd» надо зафиксировать в инструкции развёртывания и в
DR-процедуре.

### Два mover на двух драйвах: работает

После чистой пересборки (DSS через `phobos_db drop_tables/setup_tables`,
картриджи через `mktape -s 5120`, пути только by-id, TLC раньше phobosd) обе
ленты отформатировались по 4 GiB, и полный цикл прошёл:

```
$ phobos put /tmp/t2.bin t/one -f tape -L vtllib
PUT operation for oid:'t/one' succeeded
$ phobos extent list -f json -o oid,media_name t/one
[{"oid": "t/one", "media_name": ["TST001", "TST002"]}]
$ phobos get t/one /tmp/t2back.bin
Object 't/one' successfully retrieved        # md5 совпал
```

Два параллельных put из отдельных процессов — эмуляция двух mover:

```
два параллельных put: 1.70 s
PUT operation for oid:'mv/a' succeeded
PUT operation for oid:'mv/b' succeeded
mv/a: OK
mv/b: OK
```

После этого ленты перешли в `fs_status = used`. Замечание: `media_name` у обоих
объектов — `["TST001", "TST002"]`, потому что layout `raid1` пишет две реплики.
Для проверки «каждый mover на своём драйве» нужен одно-репликовый layout, он
остраётся за рамками спайка.

### Что из этого остаётся верным

**После `phobos tape format` надо проверять `fs_status` и `phys_spc_free`, а не
возвращаемый код.** Форматирование вернуло успех при ёмкости 0 GB и при
несовпадении привязки драйва. Та же форма ошибки, что с `xt_rc` из
[M0-02](M0-02-libphobos-api.md): молчаливый успех опаснее честной ошибки.

## Ещё одна находка: libltfs падает на неверном устройстве

Если передать `ltfs` путь, который не является ленточным устройством, libltfs
**падает с NULL dereference** вместо внятной ошибки:

```
kernel: ltfs[157353]: segfault at 0 ip ... in libtape-sg.so
  #0 _raw_open (libtape-sg.so)
  #1 sg_open (libtape-sg.so)
  #2 tape_device_open (libltfs.so.0)
```

Происходит ровно тогда, когда путь уехал после перезапуска — то есть в том самом
сценарии, который выше ломал привязку драйвов. Для mover это означает: **путь
устройства надо проверять до того, как отдать его LTFS**, и не считать
невозможным падение внутри библиотеки. ADR-0007 с отдельным процессом mover'а
здесь окупается: сегфолт убьёт mover, а не процесс с клиентскими соединениями.

## Что ещё выяснилось попутно

**Соответствие DTE ↔ `/dev/sgN` не в порядке `device.conf`.** Это стоило мне
нескольких ложных «носителя нет» на живом драйве. По `lsscsi -g` и `vpd_pg83`:

```
DTE 0 = /dev/st1 = /dev/sg3 = драйв 11 (серийник XYZZY_A1)
DTE 1 = /dev/st0 = /dev/sg2 = драйв 12 (серийник XYZZY_A2)
```

Карту надо строить по серийнику привода. `klipper-tlc` обязан это делать, иначе
будет грузить картридж в драйв, который не тот. Косвенное подтверждение — адреса
элементов у Phobos: драйв 11 = `0x1f4` (500), драйв 12 = `0x1f5` (501), и
именно `501` фигурировало в ошибке mtx.

**`vtlcmd <драйв> load` молча возвращает 0 и ничего не делает**, если картриджа
нет в map. Работает только путь через SCSI-changer (`mtx load/unload`), он же
ближе к боевому — TLC дёргает ровно changer.

**FUSE отказывается монтироваться на непустой каталог** (`fuse: mountpoint is not
empty`), а запись в несмонтированную точку монтирования **молча уходит в
корневую ФС**. Я на этом дважды получил ложное «расхождение контрольных сумм».
Любой скрипт, работающий с LTFS, обязан проверять факт монтирования перед
записью — в `verify-ltfs-integrity.sh` это есть.

**`phobos put --no-split`** — существование флага «Prevent splitting object over
multiple media» прямое подтверждение того, на что указало ревью по
[ADR-0008](../adr/0008-object-medium.md): Phobos дробит объект между носителями,
и полагаться на «один батч — один носитель» нельзя.

**`media_name` в выводе `extent list` — массив из двух** при `layout raid1`:

```
[{"oid": "ltfs/obj1", "media_name": ["/var/lib/phobos2", "/var/lib/phobos"]}]
```

Подтверждает решение делать `objects.phobos_medium` как `text[]`.

**`phobos tape repack`** — «Repack a tape, by copying its alive extents to
another one». Это ответ на вопрос ревью про очистку мусора на ленте: штатный
механизм есть, и он в Phobos, а не в tapegw.

**Побочно: `dnf` поставил новое ядро** (`5.14.0-687.64.1`) как зависимость.
Модуль `mhvtl` собран под `5.14.0-687.5.3` и после перезагрузки не загрузится.
На стенде ядро надо пинить.

## Что осталось

- Одно-репликовый layout, чтобы показать «каждый mover пишет на свой драйв»
  явно, а не через raid1 на обе ленты.
- Замер на реальном железе, когда оно появится: там цикл — десятки секунд, а
  позиционирование до минуты-двух.
- Требования к деплою (порядок запуска, `mktape -s`, by-id) оформить в
  `docs/ops/` при развёртывании M5.

## Репродукция

```bash
# модуль ядра
cd /root/mhvtl/kernel && make KDIR=/usr/src/kernels/$(uname -r) install && depmod -a
modprobe mhvtl && systemctl start mhvtl.target

# LTFS
cd /root/ltfs && ./autogen.sh && ./configure --prefix=/usr --disable-snmp
make -j4 && make install

# цикл и целостность
bash /root/measure-ltfs-cycle.sh
bash /root/verify-ltfs-integrity.sh
```

Скрипты: [`measure-ltfs-cycle.sh`](measure-ltfs-cycle.sh),
[`verify-ltfs-integrity.sh`](verify-ltfs-integrity.sh).
