# M0-11: библиотеки, поколения и копии

- **Статус:** частично закрыт — вопрос 3 (копии) закрыт полностью, он был
  блокирующим; вопросы 1, 2, 4 не проверялись, вопрос 5 проверен наполовину
- **Дата:** 2026-11-05
- **Phobos:** 3.4.1 (сборка из исходников, тег `3.4.1`), стенд `tapegw-vm`
  (AlmaLinux 9.8, mhVTL 0.18.43), DSS — `phobos_test`
- **Вопрос из плана:** пять вопросов, каждый меняет модель данных, а не только
  операцию. Главный — **можно ли вторую копию объекта записать с другими
  тегами**: от ответа зависит, описывает `archive_copies` копии Phobos или
  самостоятельные объекты Phobos.

## Краткий ответ (вопрос 3)

**Да, можно — но не встроенным механизмом репликации и не вторым `put`.**

Рабочий путь: **каждая копия — отдельный объект Phobos**, записанный отдельной
операцией `put` со своим `oid` и своим набором тегов. Встроенные именованные
копии Phobos для «копия на каждой стороне» не годятся: `phobos copy create`
поднимает данные с первой ленты, а `put --overwrite` вместо второй копии создаёт
новую версию объекта и прячет прежние копии.

Решение по этому выводу — [ADR-0022](../adr/0022-copies-as-separate-objects.md).

## Стенд, на котором проверялось

Две виртуальные библиотеки на одном SCSI-хосте mhVTL:

| Библиотека | Робот | Драйвы | Носители |
| --- | --- | --- | --- |
| A (`vtllib`) | `/dev/sch0` STK L700 | A1, A2 — ULT3580-TD8 (LTO-8); A3, A4 — ULT3580-TD6 (LTO-6) | `E010*L8`, `TST001`, `TST002` |
| B | `/dev/sch1` STK L80 | B1…B4 — STK T10000B | `G030*TA` |

В Phobos зарегистрирована только библиотека A и драйвы A1, A2. Для вопросов 1 и
2 (переезд между библиотеками, поколения) этого мало — см. «Что не закрыто».

Подготовка лент с разными тегами (подробности и грабли — в
[docs/ops/tape-library.md](../ops/tape-library.md)):

```bash
mktape -l 10 -m E01001L8 -s 4096 -t data -d LTO8
phobos tape add -L vtllib -t LTO8 --fs LTFS -T side:left  --unlock E01001L8
phobos tape format --fs LTFS --unlock E01001L8
# то же для E01002L8 с тегом side:right
```

Итог подготовки:

```
name,tags,fs.status,stats.phys_spc_free
E01001L8,['side:left'],empty,3221225472
E01002L8,['side:right'],empty,3221225472
```

## Вопрос 3 — копии с разными тегами

### Что говорит исходник

Тег — свойство **носителя** (`media_info.tags` в `pho_types.h`), а не копии
объекта. Копии задаёт layout.

Параметры записи (`pho_xfer_put_params` в `phobos_store.h`):

```c
struct string_array     tags; /**< Tags to select a media to write. */
const char      *copy_name;   /**< Name of the copy being written. */
```

`tags` — **один список на всю операцию**. `raid_writer_rebuilder_build_allocation_req`
(`src/layout/raid_common.c`) заполняет им каждый extent одними и теми же
значениями:

```c
for (i = 0; i < n_extents; ++i)
        n_tags[i] = put_params->tags.count;
...
for (i = 0; i < n_extents; ++i) {
        for (j = 0; j < put_params->tags.count; ++j)
            req->walloc->media[i]->tags[j] = xstrdup(put_params->tags.strings[j]);
}
```

Протокол разрешает теги на каждый extent (`n_tags[i]`), API — нет. Значит
`raid1` с `repl_count = 2` физически не может положить реплику 1 на `side:left`,
а реплику 2 на `side:right`.

Схема DSS (3.3) про именованные копии:

```sql
CREATE TABLE copy(
    object_uuid, version, copy_name, copy_status, ...
    PRIMARY KEY (object_uuid, version, copy_name));
CREATE TABLE layout(
    object_uuid, version, extent_uuid, layout_index, copy_name,
    PRIMARY KEY (object_uuid, version, layout_index, copy_name));
```

То есть у одной версии объекта может быть несколько именованных копий со своими
разкладками и носителями. Конфигурация: `[copy] default_copy_name` (по умолчанию
`source`) и `get_preferred_order` — порядок предпочтения копий при чтении.

### Опыт 1 — второй `put` того же объекта с другим тегом

```bash
phobos put -f tape -l raid1 -P repl_count=1 -T side:left  -c left  spike.bin spike-obj
# PUT operation for oid:'spike-obj' succeeded

phobos put -f tape -l raid1 -P repl_count=1 -T side:right -c right spike.bin spike-obj
# Error while saving extents for objid: 'spike-obj': Invalid argument
# PUT operation for oid:'spike-obj' succeeded
# Cannot PUT '/tmp/spike.bin' to objid(s) 'spike-obj' (File exists)
```

В DSS осталась одна копия:

```
 copy_name | medium_id | state
-----------+-----------+-------
 left      | E01001L8  | sync
```

**Вывод:** добавить копию обычным `put` нельзя. Обратите внимание на лживость
промежуточной строки «PUT succeeded» — ошибка в `dss_extent_insert`, а обёртка
успех не отменяет.

### Опыт 2 — `phobos copy create`

```bash
phobos copy create spike-obj right -c left -T side:right -f tape -l raid1 -P repl_count=1
# Object 'spike-obj' successfully copied as 'right'

 copy_name | medium_id
-----------+-----------
 left      | E01001L8     # тег side:left
 right     | E01002L8     # тег side:right
```

**Работает**: вторая копия легла на ленту с другим тегом.

Цена: `copy create` **читает** исходную копию, то есть поднимает данные с первой
ленты и занимает два драйва — один читает, другой пишет.

### Опыт 3 — `put --overwrite` с новым именем копии

```bash
phobos put -f tape -l raid1 -P repl_count=1 -T side:right -c third --overwrite spike.bin spike-obj
# PUT operation for oid:'spike-obj' succeeded     ← без единой ошибки

 copy_name | copy_status
-----------+-------------
 left      | complete
 right     | complete
 third     | complete
```

Выглядит как успех. Но:

```bash
phobos copy list spike-obj
# third                                ← только third, а не три копии

phobos get -c third spike-obj g.bin    # ok, 8388608 байт
phobos get -c left  spike-obj g.bin    # Cannot GET ... (No such file or directory)
phobos get -c right spike-obj g.bin    # Cannot GET ... (No such file or directory)
```

Причина — версии:

```
 copy_name | version
-----------+---------
 left      |       1
 right     |       1
 third     |       2          ← новая версия

SELECT oid, version FROM deprecated_object WHERE oid='spike-obj';
 spike-obj | 1
```

**`put --overwrite` не добавляет копию — он создаёт новую версию объекта.**
Прежняя версия с копиями `left` и `right` уходит в `deprecated_object` и
перестаёт быть видна. Данные на лентах остаются, объект к ним больше не
обращается.

Это тихое поведение: ни кода ошибки, ни предупреждения, в таблице `copy` статус
`complete`. Именно оно опасно для правила «архивный = все копии записаны»:
оператор или баг в планировщике добавит «ещё одну копию» — и объект потеряет
доступ к уже записанным копиям.

## Вывод для tapegw

1. **Каждая копия — самостоятельный объект Phobos** (`<uuid>.left`,
   `<uuid>.right`), записанный своей операцией `put` из буфера mover. Данные
   идут по сети **один раз**, лента только пишет, ничего не читая.
2. **`phobos copy create` отклонён**: лишнее чтение ленты на каждый объект и два
   драйва одновременно, что мешает и архивации, и restore.
3. **`put --overwrite` запрещён** как операция — ловушка из опыта 3.
4. Связь копий знает только tapegw: таблица `archive_copies`, логический объект →
   строки копий со своими `oid`. В атрибуты записи добавляется имя спецификации
   копии, чтобы `rebuild-meta` видел, какая это копия.

Детали — [ADR-0022](../adr/0022-copies-as-separate-objects.md).

## Что не закрыто

| № | Вопрос | Состояние |
| --- | --- | --- |
| 1 | `phobos tape rename --new-library` / `phobos drive migrate` | **не проверялось.** Нужна вторая библиотека, зарегистрированная в Phobos (вторая секция `[tlc]` + второй `phobos_tlc` на своём порту), и драйв в ней того же поколения, что и переносимая лента. Сейчас библиотека B есть в mhVTL, но в Phobos не зарегистрирована, а её драйвы T10000B не читают LTO-8. Не блокирует migrator: это процедура эксплуатации, а не формат. Смотреть перед M9 |
| 2 | Совместимость поколений | **не проверялось.** Драйвы A3/A4 (ULT3580-TD6) в Phobos не зарегистрированы. Нужно: читает ли драйв LTO-8 ленту LTO-6 и отказывает ли запись с тегом `gen:lto8` на ленту другого поколения |
| 4 | `phobos tape repack` с целевым тегом | **не проверялось.** Относится и к M0-10; перенести к ADR по repack после MVP |
| 5 | Атрибуты MAM на виртуальных драйвах | **частично.** У тома mhVTL есть файл `/var/lib/mhvtl/<VOL>/mam` (1071 байт), но разобрать его не удалось: по формату SCSI READ ATTRIBUTE в нём читается только `0x0000 Text Localization` (2048 байт, то есть за пределами самого файла — разбор требует аккуратности), штатного читателя MAM в стенде нет: `mt` из AlmaLinux 9 не знает `mtdat`, `ltmt` и `ltfsattr` не установлены, `sg3_utils` READ ATTRIBUTE (0x8c) не умеет. Попробовать `sg_read_attr` по `/dev/sgN` драйва; если mhVTL не отдаёт — проверим на реальном драйве в M8 |
