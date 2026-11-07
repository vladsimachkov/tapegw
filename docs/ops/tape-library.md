# Настройка ленточной библиотеки

Документ про **настройку** библиотеки: порядок запуска, как заводить ленты и
драйвы, и чем заканчивается нарушение каждого правила. Здесь же — разбор
конкретных отказов: симптом → причина → что делать.

Причина появления документа простая: в настройке библиотеки много тонкостей,
которые не видны по коду ошибок. `Format failed ... No such device` ничего не
говорит о том, что ленту надо сначала размонтировать, а том — пересоздать с
размером. Такие знания переживают и человека, и ветку, только если записаны.

**Что сюда попадает.** Всё, что проверено на стенде руками или спайком. Каждая
запись либо воспроизведена, либо помечена как непроверенная. Догадки без
подтверждения здесь не пишутся — для них есть `docs/spikes/` и `docs/proposals/`.

Родственные документы: [`test-server.md`](test-server.md) — как собран сам
стенд; [`../spikes/M0-07-mhvtl-ltfs.md`](../spikes/M0-07-mhvtl-ltfs.md) —
протокол спайка с mhVTL и LTFS.

---

## 1. Порядок запуска и почему он не переставляется

```
mhVTL (vtllibrary@N, vtltape@N)  →  phobos_tlc  →  phobosd  →  tapegw
```

* **`phobos_tlc` раньше `phobosd`.** `phobosd` при старте открывает TLC-библиотеку
  и **не повторяет попытку**, если та не отвечает: библиотека остаётся
  невидимой, при этом сам `phobosd` живой и все команды `phobos ...` отвечают
  успешно. В systemd это `After=` + `Requires=`.
* **`mhVTL` раньше `phobos_tlc`** — иначе TLC не находит робота и устройства.

### Перезапуск `phobos_tlc` быстрее минуты не получится

```
Socket binding(localhost:20123) failed: Address already in use (98)
```

Сокет висит в `TIME-WAIT`, а Phobos не ставит `SO_REUSEADDR`. Проверка —
`ss -tan | grep 20123`: слушать некому, а порт занят. Ждать ~60 с. Это
относится к любому рестарту, включая аварийный, — закладывайте в runbook.

### `pkill -f` по имени демона убивает собственную сессию

`ssh host 'pkill -f "phobosd -i"'` матчит и собственную командную строку
удалённого шелла: ssh-сессия обрывается, а команда может не дойти до цели.
Искать процесс и убивать по PID:

```bash
kill "$(pgrep -f 'sbin/phobosd' | head -1)"
```

---

## 2. mhVTL: где что настроено

Один модуль `mhvtl` = один SCSI-хост, на нём **несколько** библиотек. На стенде:

| Библиотека | ID | Робот | Драйвы | Носители |
| --- | --- | --- | --- | --- |
| A | 10 | `/dev/sch0` (STK L700) | A1, A2 — ULT3580-TD8 (LTO-8); A3, A4 — ULT3580-TD6 (LTO-6) | `E010*L8`, `TST001`, `TST002` |
| B | 30 | `/dev/sch1` (STK L80) | B1…B4 — STK T10000B | `G030*TA` |

Файлы:

* `/etc/mhvtl/device.conf` — какие есть библиотеки и драйвы: `Library ID`,
  `Slot`, `Vendor/Product identification`, `Unit serial number`. **Поколение
  драйва задаётся здесь** (`ULT3580-TD8` против `ULT3580-TD6`).
* `/etc/mhvtl/library_contents.<ID>` — содержимое ячеек: `Slot 21: TST001`.
  Штрихкод читается из имени: последние два символа — тип носителя (`L8`, `L6`,
  `L7`), префикс `CLN` — чистящая, `W` — WORM.
* `/var/lib/mhvtl/<VOLSER>/` — сам том: `data.N`, `indx.N`, `mam`.

Проверить глазами, что робот видит на самом деле:

```bash
mtx -f /dev/sch0 status        # драйвы (Data Transfer Element) и слоты
lsscsi                          # кто есть в SCSI
```

### Переезд носителя между библиотеками моделируется правкой `library_contents.*`

Убрать строку слота из `library_contents.10`, добавить в `library_contents.30`,
перезапустить `vtllibrary@` обеих библиотек. Это и есть «физический перенос
ленты из одной библиотеки в другую» для целей спайка M0-11.

---

## 3. Заведение носителя: четыре шага, и пропущенный третий всё ломает

```bash
# 1. том с ЯВНЫМ размером и плотностью
mktape -l 10 -m E01001L8 -s 4096 -t data -d LTO8

# 2. носитель в Phobos, с тегами, и сразу разблокированный
phobos tape add -L vtllib -t LTO8 --fs LTFS -T side:left --unlock E01001L8

# 3. дождаться, пока робот его увидит, и убедиться, что ёмкость не ноль
phobos tape locate E01001L8
phobos tape list -f csv -o name,tags,fs.status,stats.phys_spc_free

# 4. форматирование
phobos tape format --fs LTFS --unlock E01001L8
```

### Без `-s` том показывает нулевую ёмкость

Том, созданный без размера, существует, робот его видит, но LTFS получает
ёмкость 0. Наружу это выходит как `phobos tape format ... No such device`, а в
`stats.phys_spc_free` стоит `0`. Отличить от других причин по `phys_spc_free`.

Лечение: пересоздать том — удалить каталог и вызвать `mktape` заново:

```bash
rm -rf /var/lib/mhvtl/E01001L8
mktape -l 10 -m E01001L8 -s 4096 -t data -d LTO8
```

Перезаписывает ли `mktape` существующий том или молча пропускает его —
не проверено: в прогоне он ничего не создал на существующем каталоге, но там же
не был задан `-d`, так что причина не изолирована. Удалять каталог перед
пересозданием — безопасно независимо от ответа.

После пересоздания тома перезапустить `vtllibrary@N`/`vtltape@N` и
`phobos_tlc`: демоны держат старое состояние.

### `-d density` обязателен

Без него `mktape` печатает таблицу плотностей и ничего не создаёт. Значения:
`LTO5`…`LTO10`, `LTO8M8`, `T10KA`/`T10KB`/`T10KC` и др. (`mktape -h`).

### `phobos tape add` ставит admin lock

Под локом носитель не находится: `phobos tape locate` отвечает
`Medium ... is admin locked` / `Permission denied`. Проверить и снять:

```bash
phobos tape list -f csv -o name,adm_status
phobos tape unlock E01001L8
```

Проще сразу добавлять с `--unlock`. Влияет ли лок на `tape format` — не
проверялось отдельно: в моём прогоне лок был снят раньше, чем устранена
настоящая причина отказа (см. выше про нулевую ёмкость).

### Успешный `phobos tape format` ничего не гарантирует

Код возврата нулевой и при нулевой ёмкости. Единственная проверка — состояние
файловой системы и свободное место:

```bash
phobos tape list -f csv -o name,fs.status,stats.phys_spc_free
```

Значения `fs.status`:

| Значение | Что значит |
| --- | --- |
| `blank` | не отформатирована — обязательное состояние **до** `tape format` |
| `empty` | отформатирована, данных нет — рабоче-боевое состояние |
| `used` | есть данные |
| `full` | места нет |

---

## 4. Драйвы: кто его держит и как освободить

`phobos drive load` предупреждает «Device ... is in use» — и это не всегда
конфликт двух операций Phobos.

### Остаточный монтаж LTFS занимает драйв

Если с прошлого захода остались `ltfs -o devname=/dev/sgN /mnt/...`,
`phobos drive load` предупреждает «Device ... is in use». При этом
`mtx -f /dev/sch0 status` покажет драйв пустым — LTFS-процесс живёт, а ленты в
приводе уже нет.

```bash
mount | grep ltfs
umount /mnt/phobos-sg2 /mnt/phobos-sg3
pkill -f "ltfs -o devname"
```

Внимание к порядку: предупреждение «in use» само по себе безобидно — в прогоне
`tape format` прошёл успешно, а `phobos drive load` продолжал его печатать.
Настоящей причиной `No such device` в том прогоне была нулевая ёмкость тома
(см. раздел 3), а не занятый драйв. Не путайте эти два отказа по одному лишь
предупреждению.

### После пересоздания томов перезапускайте стек целиком

Пересозданный том демоны mhVTL и `phobosd` продолжают видеть старым. Полный
порядок: `vtllibrary@N`/`vtltape@N` → `phobos_tlc` (помня про `TIME-WAIT`)
→ `phobosd`. В прогоне форматирование заработало только после перезапуска
всего стека; какой именно из трёх перезапусков был решающим — не изолировано.

### `lock_owner` у драйва — это `phobosd`, а не чужая операция

`phobos drive list -f csv -o name,adm_status,lock_owner` всегда показывает в
`lock_owner` PID `phobosd`. Сам по себе он ничего не блокирует. Настоящая
блокировка — `adm_status = locked`.

### Устройства регистрировать только путями `/dev/tape/by-id/`

`/dev/stN` перенумеровываются между перезагрузками, и драйв начнёт смотреть не
туда. Это же правило для `lib_device` в `[tlc]`.

---

## 5. Теги и копии: что на самом деле умеет Phobos 3.4.1

Проверено на стенде (спайк M0-11), DSS — `phobos_test`.

### Тег — свойство носителя, фильтр строгий и «И»

`phobos tape add -T side:left`, отбор при записи — `-T side:left`: подойдёт
только носитель, у которого есть **все** перечисленные теги. Помеченная лента от
записи **без** тега не защищена — отсюда обязательное тегирование каждой записи
в [ADR-0013](../adr/0013-tape-pools.md).

### Копии объекта — именованные, и у каждой свой фильтр тегов

В DSS таблица `copy(object_uuid, version, copy_name, copy_status)` и
`layout(..., copy_name)` — то есть у одной версии объекта может быть несколько
именованных копий со своими разкладками и своими носителями. Конфигурация:
`[copy] default_copy_name` (по умолчанию `source`) и `get_preferred_order` —
порядок предпочтения копий при **чтении**.

Вторую копию на носители с другим тегом штатно делает **`phobos copy create`**:

```bash
phobos put  -f tape -l raid1 -P repl_count=1 -T side:left  -c left  file.bin obj
phobos copy create obj right -c left -T side:right -f tape -l raid1 -P repl_count=1
# copy=left → E01001L8 (side:left), copy=right → E01002L8 (side:right)
```

Минус: `copy create` **читает** исходную копию, то есть поднимает данные с
первой ленты, чтобы записать вторую.

### Ловушка: `put --overwrite -c <имя>` — это НЕ вторая копия

```bash
phobos put --overwrite -T side:right -c third file.bin obj   # «PUT succeeded»
```

Команда рапортует успех, в `copy` появляется строка `third` со статусом
`complete`, но это **новая версия объекта**. Прежняя версия с копиями `left` и
`right` уходит в таблицу `deprecated_object` и перестаёт быть видна:
`phobos copy list obj` покажет только `third`, а `phobos get -c left obj`
ответит `No such file or directory`. Данные на лентах остаются, но объект к ним
больше не обращается.

Тем более нельзя добавить копию обычным `put` без `--overwrite`: второй `put`
того же `oid` даёт

```
Error while saving extents for objid: '...': Invalid argument
Cannot PUT ... (File exists)
```

### В одной операции `put` все реплики пишутся с одним набором тегов

`struct pho_xfer_put_params.tags` — один список на всю операцию, и
`raid_writer_rebuilder_build_allocation_req` (`src/layout/raid_common.c`)
заполняет `media[i].tags` одними и теми же значениями для всех `i`. Протокол
разрешает теги на каждый extent, API — нет. Значит `raid1` с `repl_count=2` не
разложит реплику 1 на `side:left`, а реплику 2 на `side:right`.

**Что из этого следует для tapegw.** Политику «копия на каждой стороне» нельзя
выдать одной операцией записи с репликацией. Варианты: отдельная именованная
копия на каждую спецификацию (для второй — `copy create` с чтением первой) либо
несколько самостоятельных объектов Phobos, связанных в `archive_copies`.
Решение — за ADR по копиям, вопрос №3 спайка M0-11.

---

## 6. Разрешение на запись и чтение MAM

### Снять право записи у носителя, оставив чтение и удаление

```bash
phobos tape set-access -- -P E01001L8     # «--» обязателен, иначе -P съедает argparse
phobos tape set-access GD  E01001L8       # разрешить get и delete, запретить put
```

Флаги: `P` — put, `G` — get, `D` — delete; `+P` добавляет, `-P` снимает, список без
префикса задаёт набор целиком.

**Ловушка порядка: флаги идут ДО имени ленты.** Написанное после имени
молча не находит строку:

```bash
phobos tape set-access -L vtllib -- -P E01002L8    # верно
phobos tape set-access -- -P -L vtllib E01002L8    # «No rows were updated», rc=0
```

Второй вариант возвращает ноль, поэтому скрипт настройки не заметит, что ничего
не применилось. Проверять всегда: `phobos tape list -f csv -o name,put_access`.

**Отказ в записи приходит как `ENOSPC`, а не как «запрещено».** Phobos не говорит
«носитель закрыт», он исключает ленту из выбора, и сверху выглядит ровно как
«нет места». Проверено A/B на одной ленте: с `put_access=False` —
`Cannot PUT ... (No space left on device)`, после `+P` — успех. Поэтому по коду
ошибки различать «полка заполнена» и «полка закрыта политикой» нельзя. Это то, на чём держится третий слой запрета записи на
устаревшее поколение ([предложение 0014](../proposals/0014-media-lifecycle-migrations-plan.md),
Р5): без него запрет живёт только в тегах и конфиге tapegw, а ручной `phobos put`
мимо нас на устаревшую ленту напишет.

Что именно делает LRS с этим флагом и не сбрасывает ли его `repack` или
форматирование — проверяется в спайке M0-12.

### MAM читается, но не когда лента в приводе

```bash
sg_read_attr -e /dev/sg4     # список атрибутов
sg_read_attr   /dev/sg4      # значения
```

`sg_read_attr` входит в `sg3_utils` (на стенде 1.47) — вопреки первому впечатлению,
что READ ATTRIBUTE (0x8c) там нечем читать. mhVTL отдаёт 48 атрибутов, включая те,
которыми считаются политики здоровья: `0x0002` TapeAlert, `0x0003` счётчик
загрузок, `0x0007` счётчик инициализаций, `0x0008` идентификатор тома, блок `0x0400`
(производитель, штрихкод, серийник, ёмкости).

**Ловушка: `Device or resource busy`, пока драйв зарезервирован `phobosd`.** А при
смонтированной ленте он зарезервирован всегда, и `phobos drive scsi_release` на
залоченном приводе отвечает `File exists`. То есть MAM нельзя просто читать «при
каждой загрузке» — загрузка и есть момент, когда драйв занят. Варианты (читать в
окне до резервирования, брать `phobos tape list -o stats.*`, читать на реальном
драйве в M8) разбираются в спайке M0-12.

**Правило повтора.** Первый запрос после загрузки картриджа всегда отдаёт
`Unit attention` — это штатное «носитель сменился, повтори», а не отказ. Со
второго запроса атрибуты отдают стабильно, в том числе **при смонтированной ленте**:

```bash
sg_read_attr /dev/sg4   # 1-й: Unit attention
sg_read_attr /dev/sg4   # дальше — значения
```

`Device or resource busy` бывает, только когда Phobos прямо сейчас работает с
приводом; монопольного захвата нет — в простое `phobosd` вообще не держит
`/dev/st*` открытым. Поэтому MAM можно читать и после загрузки, а не только в
окне до резервирования.

Статистика без конфликта резервирований — из Phobos:

```bash
phobos tape list -f csv -o name,model,stats.nb_load,stats.nb_errors,\
stats.nb_obj,stats.phys_spc_used,stats.phys_spc_free
```

На mhVTL `stats.nb_load` показывает `0` при заведомо выполненных загрузках, а
`Load count` из MAM при этом растёт (6 после шести загрузок). Значит счётчик
загрузок для политик здоровья берём из MAM, а не из Phobos — и это верно и для
покупных библиотек, где Phobos своё поле может и заполнять.

### Нумерация `/dev/sgN` живёт до перезагрузки

`/dev/sg2` сегодня — это драйв A1, завтра после перезагрузки — что-то другое.
Искать устройство драйва так:

```bash
lsscsi -g | grep -E "ULT3580|tape"      # /dev/stN и /dev/sgN рядом
ls -l /dev/tape/by-id/ | grep -v nst    # стабильные имена по серийнику
```

## 7. Полезные проверки

```bash
phobos lib refresh                                  # перечитать робот
phobos tape list -f csv -o name,library,tags,fs.status,stats.phys_spc_free
phobos drive list -f csv -o name,adm_status,lock_owner
phobos copy list <oid>                              # какие копии видит Phobos
phobos getmd <oid>
phobos extent list <oid> -f json                    # где лежит (ADR-0008)
phobos logs dump                                    # журнал SCSI-действий
```

По `copy`/`layout`/`extent` напрямую из Postgres удобно смотреть то, что CLI не
показывает (версии и унаследованные копии):

```sql
SELECT c.copy_name, c.version, c.copy_status, e.medium_id, e.state
  FROM copy c
  JOIN layout l ON l.object_uuid = c.object_uuid
               AND l.version = c.version AND l.copy_name = c.copy_name
  JOIN extent e ON e.extent_uuid = l.extent_uuid
 WHERE c.object_uuid = (SELECT object_uuid FROM object WHERE oid = '<oid>');
```

---

## 8. Открытые вопросы по настройке

* **Когда читать MAM.** Сами атрибуты доступны (раздел 6), но не в тот момент,
  когда они нужны — при смонтированной ленте драйв зарезервирован. Разбирается в
  спайке M0-12.
* `phobos tape repack` с целевым тегом — поведение относительно тегов пулов не
  проверено (он же вопрос спайка M0-11 №4 и M0-10).
* Регистрация второй библиотеки в Phobos (вторая секция `[tlc]` + второй
  `phobos_tlc` на своём порту) — на стенде есть вторая библиотека mhVTL, но в
  Phobos она не зарегистрирована; сценарий переезда ленты между библиотеками
  (вопрос M0-11 №1) поэтому не проверен.

## phobosd: запуск, локи, сокет

**Правило: phobosd на стенде запускается только через systemd.** Ручной запуск
допустим только после `systemctl stop phobosd`, и после ручной работы обязательно
остановить ручной процесс и проверить, что не осталось локов. Иначе через месяц
получаете «все поля зелёные, а `put` отвечает `ENODEV`».

### `phobos dir list` показывает носитель, а не устройство

У Phobos у `dir` два разных объекта: **носитель** и **устройство**, и состояние у
них раздельное. `phobos dir list -o all` печатает строку **носителя**:
`fs.status`, `phys_spc_free`, `put_access` и `adm_status` в этом выводе — его
поля. Отказ устройства в этом списке **не виден**.

Симптом: `dir list -o all` показывает `adm_status=unlocked`, `put_access=True`,
`fs.status=empty`, место есть — а `put` отвечает `No such device`, и в журнале
только `No compatible device found for write alloc`.

Проверка (только чтение):

```bash
psql "$(sed -n 's/^ *connect_string *= *//p' /etc/phobos.conf | head -1)" \
  -c "SELECT family, id, adm_status FROM device"
```

Там `adm_status` может быть `failed` при полностью «зелёном» носителе. Снимается
штатно: `phobos dir unlock -L <library> <res>` (для драйвов — `phobos drive
unlock`), после чего состояние `unlocked` переживает рестарт phobosd.

Замечено: LRS переводит устройство в `failed` сам, когда его поток завершается с
ошибкой (например `No locks available (37)` после убийства phobosd вручную).
Проверка `adm_status = failed` входов в `hack/stand-status` — иначе «устройство
отказало час назад» снова будет выглядеть как загадка.

Симптом: `phobos put -f dir -L dirlib -l raid1 ...` → `No such device`, при этом
`phobos dir list -o all` показывает `adm_status=unlocked`, `put_access=True`,
`fs.status=empty`, `stats.phys_spc_free` с запасом.

Причина: устройство занято — в DSS записаны `lock_hostname` и `lock_owner`
(например `localhost` / `180349`). `ENODEV` от LRS означает «нет устройства,
которое может обслужить носитель», а не «нет носителя». Локи живут в базе, а не в
памяти процесса: убить phobosd недостаточно.

Что делать: `phobos lock clean` (выводит `N lock(s) cleaned`), затем убедиться,
что `lock_owner` стал `0`. В DSS руками не лезть.

### `systemctl restart phobosd` падает с `File exists (17)` и кодом 239

Симптом: `Unable to create lock file '/run/phobosd/phobosd.lock': File exists`,
`status=239/CACHE_DIRECTORY`.

Причина: живёт phobosd, запущенный вручную (`/usr/sbin/phobosd -i`). Systemd его
не порождал, поэтому `restart` его не останавливает, а новый демон не может
создать файл лока.

Что делать: найти PID (`pgrep -a phobosd`) и остановить по PID. **Не `pkill -f`
через SSH** — совпадёт командная строка собственного удалённого шелла и убьёт
сессию (см. выше). Затем `systemctl start phobosd` и проверить `pgrep -a phobosd`
— процесс обязан быть ровно один.

### `No locks available (37)` и `ENODEV` на `dir`: поток устройства умер

Симптом: `put` на `dir` отвечает `No such device`, а в журнале phobosd:

```
Error when releasing device (family 'dir', name 'localhost:/var/lib/phobos/dir1',
  library 'dirlib') with current lock (hostname localhost, owner 180349):
  No locks available (37)
device thread (...) terminated with error: No locks available (37)
No compatible device found for write alloc: No such device (19)
```

Причина: поток устройства завершился из-за `ENOLCK`, и LRS остаётся без
устройства — отсюда `ENODEV`. Локи в DSS при этом могут выглядеть чистыми
(`lock_owner = 0`), а `phobos lock clean` без ключей отвечает `0 lock(s) cleaned`:
без `--force` он не трогает локи при работающем phobosd, а тип и семейство надо
указывать явно (`-t device -f dir --force`).

Что делать: **не найдено**. `phobos lock clean -t device -f dir --force` и
`systemctl restart phobosd` положение не исправили — `put` по-прежнему `ENODEV`.
Следующая гипотеза: `ENOLCK` приходит из DSS (Postgres), и перед рестартом надо
останавливать и TLC, и phobosd полностью. TLC на этом стенде тоже запущен
вручную (`phobos_tlc -i`, юнит `inactive`) — тот же класс ошибки, что и с
phobosd.

### `Transport endpoint is not connected` при активном юните

Симптом: `systemctl is-active phobosd` = `active`, а CLI отвечает
`Cannot contact 'phobosd': Transport endpoint is not connected`. Конфиг задаёт
`server_socket = /tmp/socklrs`, файла в `/tmp` нет.

Причина: **не найдена, и сама постановка была неверна**. Сокет существует:
`ss -xlp | grep phobos` показывает `/tmp/socklrs` в состоянии LISTEN у процесса
phobosd. Ошибка «Transport endpoint is not connected» наблюдалась в окно между
смертью старого процесса и созданием сокета новым — то есть это гонка при
рестарте, а не «сокет не создаётся».

Гипотеза `PrivateTmp=yes` проверена и **отвергнута**: `PrivateTmp=no`,
`RuntimeDirectory=phobosd` уже задан, в `/proc/<pid>/root/tmp` сокета нет — и не
должно быть, сокет в общем `/tmp`.

Что делать: разбираться, а не обходить. Проверить, какой конфиг читает демон
(`PHOBOS_CFG_FILE` в юните), и нет ли второго phobosd.

### Сокет LRS стоит держать вне `/tmp`

Не потому, что подтверждён `PrivateTmp` (он отвергнут), а потому, что клиентом
сокета будет не только CLI, но и `tapegw-mover` под systemd: если у его юнита
`PrivateTmp=yes` (частая настройка по умолчанию), он получит свой `/tmp` и снова
«Transport endpoint is not connected» — уже в бою. Путь вне `/tmp` закрывает это
для всех клиентов сразу, потому что `PrivateTmp` на `/run` не влияет.

План: `server_socket = /run/phobosd/socklrs`; `RuntimeDirectory=phobosd` уже
задан — systemd создаёт каталог при старте и удаляет при остановке, что заодно
снимает проблему устаревшего `phobosd.lock`. **Права на сокет** — отдельное
требование к деплою (M9): phobosd работает от root, mover от своего
пользователя, нужна общая группа (`RuntimeDirectoryMode` либо группа на каталоге),
иначе mover получит отказ в доступе.

### `phobos dir list -o <поля>` для `dir`

Симптом: `phobos dir list -o fs_status,phys_spc_free` → `bad output attributes`.

Причина: для семейства `dir` эти имена в `-o` не принимаются (в отличие от
`tape`).

Что делать: `phobos dir list -o all` и смотреть `fs.status`,
`stats.phys_spc_free`, `lock_owner`, `adm_status`.

### Как `dir` раскладывает объект на носителе

Замерено на чистом `ci-dir`: один `put`, потом `find` по каталогам носителей.

```
/var/lib/phobos/dir1/8f/c8/probe_one.0c2a7223-a991-4b50-85c8-18e1f93832fd
/var/lib/phobos/dir2/55/39/ci_probe.a3bde3e2-cdb8-4450-98fc-764705f0f7f1
```

Имя файла реплики — `<oid, где / заменён на _>.<uuid>`, каталог — два уровня по
первым двум парам хеша; в корне каждого носителя служебный `.phobos_dir_label`.

Отсюда практическое: **число реплик считается по oid в имени файла**, а не по
размеру. Подсчёт по размеру считает и файлы других объектов того же размера, и
файлы прошлых прогонов — носители `dir` между запусками живут.

### `phobos extent list -f json` отдаёт только поля из `-o`

Без `-o` в json попадает один `oid` — выглядит так, будто экстентов нет:

```bash
phobos extent list -f json probe/one
→ [{"oid": "probe/one"}]
```

Поля надо перечислить явно, и тогда размер приходит машиночитаемо:

```bash
phobos extent list -f json -o oid,size,media_name,state probe/one
→ [{"oid": "probe/one", "size": [16391],
    "media_name": ["/var/lib/phobos/dir1"], "state": ["sync"]}]
```

Значения — массивы по числу экстентов; при `repl_count = 1` и `no_split`
элемент ровно один. Отсюда порядок для `Stat`/`resolveByStat` (ADR-0008
соблюдён, `libpho_core` линковать не нужно): одним вызовом получаем и носитель
для `archive_copies`, и измеренный размер. Разбирать строго: ровно один
экстент, иначе ошибка; `state = sync`, иначе объект не считается целым.

### Неполная копия на `dir`: экстентов нет, а данные на носителе есть

Замерено на `ci-dir` (запись 1.5 GiB, процесс убит через 300 мс; отдельно —
наблюдение раз в секунду во время записи 4 GiB без обрыва).

Во время записи:

- файл реплики на носителе **растёт** (0.98 GiB → 2.2 GiB → 4 GiB) — данные
  пишутся на диск носителя по мере отправки;
- строка объекта в `object` **уже есть**;
- строк экстента этого объекта в DSS — **ноль**. Проверено прямым запросом
  (`extent`joined на `object.object_uuid = extent.extent_uuid`), а не только
  через CLI. То есть `phobos extent list` честен: промежуточного экстента в DSS
  нет, он регистрируется по завершении записи. Состояний вроде `pending` у
  незавершённой записи на `dir` не появляется;
- фильтров по состоянию у `phobos extent list` нет: `-o` выбирает только
  колонки из фиксированного набора, поэтому «CLI что-то скрывает по умолчанию»
  исключено.

После обрыва:

```
object (строка объекта)          → есть
файл реплики на носителе         → есть, частичный (293 191 680 байт из 1.5 GiB)
phobos extent list               → пусто, rc=0
phobos getmd                     → пусто
phobos put <тот же oid>          → EEXIST
phobos delete <oid>              → succeeded
phobos put <тот же oid>          → succeeded
```

После `delete` строка объекта уходит из DSS, **а частичный файл на носителе
остаётся**. Для `dir` это значит: прерванные записи оставляют на носителе
неучтённые байты — они занимают место, но ни `extent list`, ни учёт их не видят.
Отсюда задача учёта для repack (см. `docs/proposals/0011-repack-strategy.md`):
либо Phobos чистит их своим `repack`, либо их надо считать отдельно.

Практические следствия:

- `Stat` обязан понимать пустой вывод `extent list` при `rc=0` как «экстентов
  нет», а не как ошибку разбора, и возвращать `Complete = false`;
- промежуточного состояния «экстент зарегистрирован, но не `sync`» на `dir`
  **нет** — CLI и DSS показывают экстент только когда копия цела. Проверять
  его на `ci-dir` бесполезно; на ленте оно ожидается (задача 1a-3);
- «неполная копия» для `dir` = объект есть + экстентов нет + данные на носителе
  частично. Так же устроена неполная копия в `store/fake`.
### `phobos drive unlock` отвечает «No such device or address»

**Симптом:** `stand-status` показывает `устройство в failed: tape 00XYZZY_A1`;
`phobos drive unlock -L mhvtl 00XYZZY_A1` даёт `Failed to unlock device(s) …
(No such device or address)` (ENXIO), рестарт `phobosd` не помогает.

**Причина:** `-L` — это **имя библиотеки из DSS**, а не название модуля ядра.
В DSS библиотека называется `vtllib`, а `mhvtl` — только модуль. С чужим именем
Phobos ищет устройство в несуществующей библиотеке и отвечает ENXIO, что
похоже на «устройство мертво».

**Что делать:** взять имя библиотеки из DSS и разблокировать им:

```bash
psql -h 127.0.0.1 -U phobos -d phobos_test -At -c \
  "SELECT id, adm_status, library FROM device WHERE family='tape'"
phobos drive unlock -L vtllib 00XYZZY_A1     # 1 device(s) unlocked
systemctl restart phobosd                    # LRS перечитывает состояние
```

Проверка: `stand-status` → «ни одного устройства в failed».

Диагностика, что с самим устройством всё в порядке (отказ был только в админ-
состоянии Phobos): `sg_inq /dev/sg4` отвечает на INQUIRY с `PQual=0`,
`/dev/tape/by-id/scsi-00XYZZY_A1` указывает на существующий `st0`. Юнита
`mhvtl.service` на стенде **нет** — mhVTL поднят модулем ядра и `mktape`
вручную, поэтому «перезапустить mhVTL как службу» невозможно в принципе.
### Булевы поля DSS: читать словами, а не `t`/`f`

**Симптом:** вывод `psql` по колонке `put` у таблицы `media` прочитали наоборот:
`t` приняли за «закрыто» (хотя `t` — это `true`, запись **разрешена**), и на этом
построили вывод о «заблокированном стенде». Ошибка прочтения, а не данных, и
выглядит она уверенно с любой стороны.

**Причина:** `psql` в `-At` печатает boolean как `t`/`f`. Один символ не
подсказывает, в какую сторону он читается, особенно в столбце с двусмысленным
именем (`put = t` — «пишет» или «поставлен запрет?»).

**Что делать:** переводить в слова прямо в SQL, чтобы прочесть наоборот было
нельзя:

```sql
SELECT id,
       CASE WHEN put THEN 'запись разрешена' ELSE 'запись ЗАПРЕЩЕНА' END AS put,
       CASE WHEN adm_status = 'unlocked' THEN 'разблокирована' ELSE adm_status END AS lock
  FROM media WHERE family = 'tape' ORDER BY id;
```

`hack/stand-status` проверяет это сам: если в ленточной библиотеке нет ни одной
разблокированной ленты с разрешённой записью, он печатает
`запись в библиотеку X невозможна`, а не молчит. Проверено в обе стороны:
`phobos tape set-access -L vtllib -- -P <тома>` даёт FAIL, `+P` — снова `ok`.
Снятый флаг записи снаружи неотличим от «нет места» (`put` даёт ENOSPC), поэтому
проверка и выведена словами.
>>>>>>> 46f923e (stand-status: проверка «есть ли в библиотеке куда писать» + булевы словами)
