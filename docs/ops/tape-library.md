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

## 6. Полезные проверки

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

## 7. Открытые вопросы по настройке

* **Атрибуты MAM.** У тома mhVTL есть файл `mam` (`/var/lib/mhvtl/<VOL>/mam`,
  1071 байт), но чем его читать штатно — не найдено: `mt` из AlmaLinux 9 не
  поддерживает `mtdat`, `ltmt` и `ltfsattr` в стенде нет, `sg3_utils` READ
  ATTRIBUTE (0x8c) не умеет. Вопрос «какие атрибуты MAM доступны на виртуальных
  драйвах» из спайка M0-11 **не закрыт**: без ответа непонятно, что реально
  попадёт в `media.attrs`.
* `phobos tape repack` с целевым тегом — поведение относительно тегов пулов не
  проверено (он же вопрос спайка M0-11 №4 и M0-10).
* Регистрация второй библиотеки в Phobos (вторая секция `[tlc]` + второй
  `phobos_tlc` на своём порту) — на стенде есть вторая библиотека mhVTL, но в
  Phobos она не зарегистрирована; сценарий переезда ленты между библиотеками
  (вопрос M0-11 №1) поэтому не проверен.
