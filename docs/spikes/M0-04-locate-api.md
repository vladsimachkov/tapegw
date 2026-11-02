# M0-04 — есть ли API для ленты и смещения объекта

**Статус: закрыт по исходникам Phobos `2e3311f` (pkg 3.4.1).** Сборка не требовалась:
вопрос решается чтением протокола и заголовков.

## Что проверяли

Нужно понять, можем ли мы получить от Phobos «объект X лежит на ленте Y на
смещении Z», чтобы сортировать очередь recaller и читать несколько объектов с
одной ленты одним последовательным проходом. Если такого API нет, сортировка
restore невозможна в принципе и это меняет план M5.

## Что получилось

**Явного API для «лента + смещение» нет, и он не нужен.** Разложу по слоям.

### 1. Протокол TLC не передаёт позиционирование вообще

`src/core/serializer/pho_proto_tlc.proto` — полный набор запросов:

```proto
required uint32 id  = 1;
optional bool ping  = 2;
optional DriveLookup drive_lookup = 3;
optional Load load = 4;
optional Unload unload = 5;
optional Status status = 6;
optional bool refresh = 7;
optional Stat stat = 8;
```

Ни `Locate`, ни `Seek`, ни `Position`. `Load` принимает только
`drive_serial` и `tape_label`. По `Status` приходит состояние привода и
картриджей, но не позиция головки.

### 2. `phobos_locate()` — это не про головку

Вводит в заблуждение имя. `src/store/store.c:2941`:

```c
int phobos_locate(const char *oid, const char *uuid, int version, ...)
```

Дальше по цепочке `layout_locate()` → `raid_locate()` → `dss_medium_locate()`,
то есть это **placement**: выбрать, на какие носители и узлы кладётся объект, и
зарезервировать их. К перемещению ленты отношения не имеет.

### 3. Позиционирование спрятано внутри io-адаптера

`src/include/pho_io.h` — API адаптера это `ioa_open` / read / write /
`ioa_close`. Про позицию сказано в комментарии к `ioa_open`:

> `iod_loc` Extent location identifier. If it is missing by the caller, the call
> will set `loc->extent.address` to indicate where the extent has been located.

То есть «где лежит extent» — это **результат** открытия, а не вход для seek.
Вызывающий не ищет, а открывает.

### 4. Ленточный io-адаптер — это LTFS, то есть файловая система

`src/io-modules/io_ltfs.c` — 80 строк, обёртка над POSIX (`io_posix_common.h`)
по уже примонтированной LTFS. Единственная специфика — сброс индекса:

```c
#define LTFS_SYNC_ATTR_NAME "user.ltfs.sync"
/* flush the LTFS partition to tape */
setxattr(root_path, LTFS_SYNC_ATTR_NAME, &one, sizeof(one), 0)
```

Позиционирование, буферизация и порядок записи на ленту — внутри libltfs.

## Вывод для tapegw

**Мы никогда не выдаём Locate.** Рабочая поверхность tapegw к ленте:

1. TLC `Load` / `Unload` / `Status` / `DriveLookup` — управление картриджем;
2. обычный файловый ввод-вывод в точку монтирования LTFS;
3. `setxattr(root, "user.ltfs.sync", 1)` — сброс индекса.

Что из этого следует:

- **Сортировка очереди restore по положению на ленте — не наша задача.** Её
  делает libltfs/драйвер. План M5 не нужно усложнять сортировкой по смещению,
  и в `restores` не нужна колонка со смещением.
- **cgo-связка с libphobos не нужна вовсе** — см. ниже, это главный результат
  спайка. Если мы не вызываем `phobos_put`/`phobos_get`, то не линкуемся с
  libphobos вообще: достаточно TLC-клиента (protobuf поверх сокета) и файлового
  ввода-вывода. Это снимает и вопрос потокобезопасности (M0-03), и cgo, и
  LGPL-связывание.
- **Порядок объектов на ленте задаёт libltfs**, а не мы. Оптимизация «писать на
  ленту в порядке чтения» сводится к порядку записи файлов в LTFS, а не к seek.
- Риск переносится с «не умеем искать» на «не умеем держать LTFS-монтирование» —
  см. M0-05, там про это вывод жёстче.

## Что остаётся проверить на стенде

Что `Status` действительно достаточно для принятия решений (например, что
привод не «занят чужим процессом»). Это уже входе M0-07, на mhVTL.
