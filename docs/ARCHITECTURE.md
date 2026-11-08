# tapegw: S3 → лента на versitygw + Phobos — архитектура и план разработки

Nov 2, 2026 · @Arin

## Цели и ключевые решения

tapegw — S3-сервис с Glacier-семантикой, где данные живут на LTO-лентах под управлением Phobos, а S3-поверхность даёт встроенный versitygw. MVP — один узел, одна библиотека (самодельный манипулятор на Klipper), одна копия на ленте.

**Цели MVP**

- S3-клиенты (aws cli, rclone, SDK) пишут объекты обычным PUT и multipart и получают 200 сразу, без ожидания ленты.
- Каждый объект автоматически уходит на ленту; диск — только буфер.
- Чтение архивного объекта идёт через RestoreObject, как в AWS Glacier: HEAD показывает статус, GET работает после восстановления в кэш.
- Phobos используется без патчей; манипулятор подключается снаружи через совместимый TLC-демон.
- Метаданные можно восстановить с лент: S3-ключ и ETag пишутся в атрибуты объекта Phobos.

**Не цели MVP** (каждая — отдельное решение позже): lifecycle-политики, S3 Select, несколько узлов, освобождение места на лентах после удалений. Упаковка мелких объектов сюда больше не входит: решением владельца она вошла в M4.

**Версионирование бакетов и Object Lock из этого списка исключены решением владельца
от 2026-11-03:** они обязательны в первый год, потому что клиенты — бэкап-софт, а
неизменяемые бэкапы в AWS работают только поверх версий. См. этап V и
[предложение 0010](proposals/0010-owner-decisions.md).

**Ключевые решения**

| # | Решение | Почему |
| --- | --- | --- |
| D1 | Свой бинарь, который встраивает versitygw через `embedgw.RunVersityGW(ctx, backend, cfg)`; не форк и не Go-плагин `.so` | Плагины требуют побитово совпадающих тулчейна и модулей; форк тяжело обновлять. versitygw подключается как обычная Go-зависимость с фиксированной версией |
| D2 | Phobos — хранилище блобов; вся S3-семантика (бакеты, ключи, листинг, multipart, restore) — в tapegw и его Postgres-схеме | Листинг и HEAD никогда не трогают Phobos и ленту |
| D3 | ID объекта в Phobos — непрозрачный UUID, а не `bucket/key` | Нет проблем с кодировкой ключей, перезапись ключа — новый объект, идемпотентные повторы |
| D4 | Манипулятор подключается отдельным демоном `klipper-tlc`, который говорит по протоколу штатного `phobos_tlc` (вместо адаптера `lib_klipper` на C, обсуждавшегося ранее) | Протокол TLC маленький (Ping, DriveLookup, Load, Unload, Status, Refresh, Stat); Phobos остаётся стоковым и обновляемым |
| D5 | Очереди задач — строки в Postgres с `SELECT … FOR UPDATE SKIP LOCKED` | Транзакционность с метаданными, без отдельного брокера |
| D6 | Запись на ленту асинхронная: PUT → landing-диск → фоновая архивация батчами | Клиент не ждёт монтирования; батч = меньше монтирований |

## Компоненты

Пишем два бинаря: `tapegw` (S3-слой и воркеры) и `klipper-tlc` (манипулятор как библиотека для Phobos). versitygw, Phobos, LTFS, Postgres и Klipper используются как есть.

&#91;embedded content: компоненты tapegw и их связи\]

| Компонент | Отвечает за | Свой код |
| --- | --- | --- |
| versitygw (`embedgw`) | HTTP, SigV4, IAM, разбор S3-запросов, коды ошибок | нет, зависимость |
| backend | реализация `backend.Backend`: бакеты, объекты, multipart, листинг, RestoreObject | да |
| migrator | landing → Phobos батчами | да |
| recaller | Phobos → cache по запросам restore | да |
| janitor | вытеснение по watermark, истечение restore, брошенные multipart, orphan GC | да |
| deleter | `phobos_delete` и удаление файлов для `deleting` | да |
| Postgres | схема `tapegw` (метаданные, очереди) и DSS Phobos | только миграции `tapegw` |
| Диски (XFS) | landing и mpu на одной ФС с reflink, cache отдельно | нет |
| phobosd (LRS) | планирование драйвов, монтирование LTFS, I/O на ленту | нет, стоковый Phobos |
| klipper-tlc | протокол TLC для Phobos, inventory слотов, выгрузка драйва и сверка метки по SG\_IO | да |
| Манипулятор | Moonraker, Klipper, G-code-макросы `TAPE_*` | только макросы |

## Модель данных (схема `tapegw` в Postgres)

Все S3-метаданные и очереди живут в схеме `tapegw` того же Postgres, где Phobos держит свою DSS (в своей схеме). Один инстанс — один бэкап. Миграции только вперёд, встроены в бинарь (`embed` + goose или tern).

```sql
CREATE TYPE obj_state AS ENUM ('pending','archiving','archived','failed','deleting','deleted','marker');
CREATE TYPE restore_state AS ENUM ('requested','in_progress','restored','failed');

CREATE TABLE buckets (
  id          bigserial PRIMARY KEY,
  name        text NOT NULL UNIQUE,
  owner       text NOT NULL,
  acl         jsonb,
  policy      jsonb,
  tags        jsonb,
  created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE objects (
  id            uuid PRIMARY KEY,          -- = objid в Phobos (D3)
  bucket_id     bigint NOT NULL REFERENCES buckets(id),
  key           text NOT NULL,
  is_current    boolean NOT NULL DEFAULT true,
  size          bigint NOT NULL,
  etag          text NOT NULL,             -- md5 или md5-of-md5s-N
  checksums     jsonb,                     -- x-amz-checksum-*, если клиент прислал
  content_type  text,
  user_meta     jsonb,                     -- x-amz-meta-*
  acl           jsonb,
  tags          jsonb,
  storage_class text NOT NULL DEFAULT 'GLACIER',
  state         obj_state NOT NULL DEFAULT 'pending',
  landing       boolean NOT NULL DEFAULT true,  -- копия есть в landing
  phobos_ver    int,                       -- версия объекта в Phobos после put
  attempts      int NOT NULL DEFAULT 0,
  next_try_at   timestamptz,
  last_error    text,
  created_at    timestamptz NOT NULL DEFAULT now(),
  archived_at   timestamptz
);
-- один текущий объект на ключ; порядок листинга S3 = порядок байтов
CREATE UNIQUE INDEX objects_current ON objects (bucket_id, key COLLATE "C") WHERE is_current;
CREATE INDEX objects_queue ON objects (state, next_try_at) WHERE state IN ('pending','deleting');

CREATE TABLE restores (
  object_id    uuid PRIMARY KEY REFERENCES objects(id),
  state        restore_state NOT NULL,
  tier         text NOT NULL DEFAULT 'Standard',  -- Expedited|Standard|Bulk → приоритет
  days         int NOT NULL,
  requested_at timestamptz NOT NULL DEFAULT now(),
  restored_at  timestamptz,
  expires_at   timestamptz,
  attempts     int NOT NULL DEFAULT 0,
  next_try_at  timestamptz,
  last_error   text
);

CREATE TABLE uploads (            -- multipart
  upload_id   text PRIMARY KEY,
  bucket_id   bigint NOT NULL REFERENCES buckets(id),
  key         text NOT NULL,
  meta        jsonb NOT NULL,     -- content-type, user meta, acl, checksum algo
  initiated   timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE upload_parts (
  upload_id   text REFERENCES uploads(upload_id) ON DELETE CASCADE,
  part_number int NOT NULL,
  size        bigint NOT NULL,
  etag        text NOT NULL,
  checksums   jsonb,
  PRIMARY KEY (upload_id, part_number)
);
```

Правила:

- Перезапись ключа = новая строка `objects` с новым UUID; в той же транзакции старая получает `is_current=false, state='deleting'`. Клиент, читающий параллельно, видит либо старую, либо новую версию.
- Транзакции, меняющие текущую версию ключа, **сериализуются advisory-локом** `pg_advisory_xact_lock(hashtextextended(bucket || '/' || key, 0))`, взятым до чтения состояния (ADR-0005). Без него два параллельных PUT одного ключа сталкиваются в `unique_violation` по индексу `objects_current`, а на первой записи ключа текущей строки нет вообще и ретраить нечего. `DeleteObjects` берёт локи в детерминированном порядке, иначе два батча с пересечением ключей взаимоблокируются.
- Объект «онлайн» (GET без restore), если `landing=true` или в `restores` состояние `restored`.
- Входящий `x-amz-storage-class` принимается только как «на ленту»: пусто, `GLACIER`, `DEEP_ARCHIVE`, плюс алиасы из `s3.accept_as_glacier`. Остальное — `NotImplemented` (ADR-0003).
- Файлы на диске адресуются только по UUID: `landing/<2 hex>/<uuid>`, `cache/<2 hex>/<uuid>`, `mpu/<upload_id>/<part>`. Путь не хранится в БД, он вычисляется.
- В Phobos объект пишется с атрибутами `bucket`, `key`, `etag`, `size`, `content_type`, `created_at`, `user_meta` (JSON). Этого достаточно, чтобы восстановить схему `tapegw` с лент.

## Конечный автомат объекта и restore

У объекта два независимых автомата: основной (`objects.state`) ведёт его на ленту и к удалению, второй (`restores`) существует только для `archived` и отвечает за временную копию в кэше.

&#91;embedded content: состояния объекта и restore\]

Каждый переход выполняется одним `UPDATE … WHERE state = <ожидаемое>` в транзакции. Ноль затронутых строк означает, что состояние уже изменил кто-то другой, и воркер молча пропускает объект.

| Из | В | Кто | Условие |
| --- | --- | --- | --- |
| pending | archiving | migrator | объект взят в батч |
| archiving | archived | migrator | `getmd` + `copy_status = complete` |
| archiving | pending | migrator, старт процесса | ошибка и `attempts < max`; или при старте объект не найден в Phobos |
| archiving | failed | migrator | `attempts = max` |
| failed | pending | оператор | `tapegw admin retry` |
| pending, archiving, archived, failed | deleting | backend | DELETE или перезапись ключа; migrator, закончив put, не переведёт `deleting` в `archived` из-за условия `WHERE state='archiving'`, и объект удалит deleter |
| deleting | deleted | deleter | `phobos_delete` успешен или объекта в Phobos нет; файлы удалены |
| нет записи | requested | backend | RestoreObject для `archived` |
| requested | in\_progress | recaller | взят в батч, место в кэше есть |
| in\_progress | restored | recaller | файл в cache, размер и ETag сверены |
| in\_progress | requested | recaller | ошибка и `attempts < max` |
| in\_progress | failed | recaller | `attempts = max` |
| in\_progress | in\_progress / failed | recaller | `claim_ttl` истёк (процесс умер посреди чтения): захват перехвачен, попытка засчитана |
| restored | restored | backend | повторный RestoreObject продлевает `expires_at` |
| restored | нет записи | janitor | `online_until < now()` (обещанная дата, ADR-0003; инвариант `online_until >= expires_at`), строка удалена в транзакции, файл снят после commit |
| любое | нет записи | backend | объект ушёл в `deleting`: строка `restores` удаляется в той же транзакции, иначе recaller вечно видит restore у удалённого объекта, а метрики очередей врут |

## Потоки запросов

Каждый поток ниже — контракт для реализации и для e2e-тестов. Где поведение AWS неоднозначно, в скобках указано, что проверить в коде versitygw.

**PutObject**

1. Проверить бакет и ключ (`ValidateObjectKey`-логика как в posix-бэкенде).
2. Писать тело в `landing/tmp/<uuid>` через `O_TMPFILE` или временное имя, параллельно считая MD5 и запрошенные `x-amz-checksum-*`.
3. Сверить `Content-MD5` и checksum, если клиент их прислал; при расхождении удалить файл и вернуть `BadDigest`.
4. `fsync` файла, `rename` в `landing/<2 hex>/<uuid>`, `fsync` каталога.
5. Транзакция: вставить строку `pending`, старую текущую версию ключа перевести в `deleting`. Только после commit вернуть 200 с ETag.

Сбой между шагами 4 и 5 оставляет файл без строки; его подберёт janitor (orphan GC).

**Multipart**

- `CreateMultipartUpload` — строка в `uploads`, каталог `mpu/<upload_id>/`.
- `UploadPart` — запись части как в PutObject (fsync + rename), upsert в `upload_parts`.
- `CompleteMultipartUpload` — проверить список частей и их ETag, собрать итоговый файл в landing через reflink (`FICLONERANGE`; landing и mpu обязаны быть на одной XFS), с fallback на копирование. ETag = md5 от конкатенации бинарных md5 частей + `-N`. Дальше шаг 5 из PutObject, затем удалить каталог части.
- `AbortMultipartUpload`, `ListParts`, `ListMultipartUploads` — по таблицам. Незавершённые загрузки старше `mpu.ttl` удаляет janitor.

**Архивация (migrator)**

1. Взять батч: `SELECT … WHERE state='pending' AND next_try_at<=now() ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT n`, ограничение по суммарному размеру `batch.max_bytes`. Батч закрывается по числу, по объёму или по таймеру `batch.max_wait`.
2. Перевести строки в `archiving`, commit.
3. Каждый объект батча уходит **отдельной** операцией `put` протокола mover (ADR-0021): `op` — UUID версии, тело читается из landing-файла, атрибуты из раздела «Модель данных», family `tape`, layout и tags из конфига.
   Не один `phobos_put` на весь батч, как требовала первая редакция этого пункта. Причина — пачки (ADR-0020): с ними батч — это одна пачка мелких объектов плюс несколько крупных, а не тысяча отдельных записей, и разным пачкам нужны разные носители. Один xfer на весь батч либо уложил бы все пачки батча на одну ленту, либо потребовал бы от Phobos выбора носителя на каждый xfer внутри одного вызова. Идемпотентность при этом остаётся «одна операция — один объект»: `op` привязан к паре (размер, SHA-256), `resume` — одно смещение.

   **Носитель выбирает Phobos**, по тегам копии, которые migrator передаёт в `put`. tapegw ленту не выбирает и не должен: второй планировщик носителей поверх LRS расходится с ним при первом же repack. Цикл migrator выбирает **пул, спецификацию копии (теги) и mover**, а не ленту. Определение «доступна для записи» (`lifecycle IN ('formatted','active')` и незапрещённое поколение) нужно tapegw для другого: проверить место **до** отправки (есть ли в пуле лента нужного поколения с местом под этот размер — если нет, не начинать `put`, а сразу алерт «нужны ленты»), классифицировать `ENOSPC` после отказа («места нет» против «есть, но ленты закрыты» — см. [спайк M0-12](spikes/M0-12-write-flag-mailslots.md)) и считать свободное место пула планировщику ёмкости. Определение обязано быть одним — функцией или представлением, — иначе через месяц их будет три и свежая лента в состоянии `formatted` в одном из них потеряется.
4. Разбор итога батча. Полагаться на `xt_rc` нельзя (см. спайк M0-02): при ошибке он остаётся нулевым у всех целей. Поэтому после батча — и обязательно после любого сбоя — для каждого объекта проверяем `phobos_getmd` на наличие **и** `copy_status = complete`. Найден и полон → `archived`, `phobos_ver`, `archived_at`, плюс запись в `archive_copies`: `medium_id`, `sha256`, `format` (ADR-0022 — у каждой копии свой объект Phobos и своя строка). Носитель приходит в поле `extents` каждого `result` (ADR-0008, ADR-0021), отдельный запрос не нужен. Иначе → обратно в `pending`, `attempts+1`, экспоненциальный `next_try_at`; после `max_attempts` → `failed` и алерт. На повторе уже подтверждённые объекты исключаем, иначе батч снова упрётся в EEXIST.
5. Landing-копию не удалять сразу: её вытесняет janitor по watermark, начиная с давно архивированных.

Восстановление после падения: строки в `archiving` при старте проверяются через `phobos_getmd(uuid)`. Объект найден → `archived`, не найден → `pending`. Незавершённые копии чистит `phobos_delete_incomplete_copy`.

**HeadObject / ListObjects (v1, v2)**

- Только meta DB, лента не трогается. Листинг: префикс, delimiter, `CommonPrefixes`, `max-keys`, `continuation-token` / `marker`, порядок `COLLATE "C"`.
- Storage class: **всегда `GLACIER`**, независимо от состояния объекта (ADR-0003). Отклонение «`STANDARD` до архивации» убрано: оно заставляло клиента видеть смену класса без его действий.
- `x-amz-restore` отдаётся по колонке `objects.online_until` (ADR-0003): есть дисковая копия (`landing=true` или `restores` в `restored`) — `ongoing-request="false", expiry-date="<online_until>"`; `requested`/`in_progress` — `ongoing-request="true"`; копии нет — заголовка нет. `online_until` ставится при PUT как `now() + spool.landing_min_retention`, при переходе в `archived` продлевается до `max(online_until, archived_at + landing_min_retention)`, при RestoreObject — до `max(online_until, now() + days)`. Показанная дата никогда не бывает раньше реального удаления файла: фиктивная дата 2050 из первой редакции ADR ломала это, потому что объект архивировался и вытеснялся, а обещание оставалось.
- janitor вытесняет дисковую копию (landing или cache) только при `state = 'archived' AND online_until < now()`. При этом `pending` и `archiving` защищены **именно условием по состоянию, а не датой**: при задержке migrator дольше `landing_min_retention` дата у незаархивированного объекта уходит в прошлое, и проверка по одной дате привела бы к безвозвратной потере данных, за которые клиент уже получил 200.
- Входящий `x-amz-storage-class` принимается только как «на ленту»: пусто, `GLACIER`, `DEEP_ARCHIVE`, плюс алиасы из `s3.accept_as_glacier`. Остальное — `NotImplemented` (ADR-0003).
- Файлы на диске адресуются только по UUID: `landing/<2 hex>/<uuid>`, `cache/<2 hex>/<uuid>`, `mpu/<upload_id>/<part>`. Путь не хранится в БД, он вычисляется.
- В Phobos объект пишется с атрибутами `bucket`, `key`, `etag`, `size`, `content_type`, `created_at`, `user_meta` (JSON). Этого достаточно, чтобы восстановить схему `tapegw` с лент.

## Конечный автомат объекта и restore

У объекта два независимых автомата: основной (`objects.state`) ведёт его на ленту и к удалению, второй (`restores`) существует только для `archived` и отвечает за временную копию в кэше.

&#91;embedded content: состояния объекта и restore\]

Каждый переход выполняется одним `UPDATE … WHERE state = <ожидаемое>` в транзакции. Ноль затронутых строк означает, что состояние уже изменил кто-то другой, и воркер молча пропускает объект.

| Из | В | Кто | Условие |
| --- | --- | --- | --- |
| pending | archiving | migrator | объект взят в батч |
| archiving | archived | migrator | `getmd` + `copy_status = complete` |
| archiving | pending | migrator, старт процесса | ошибка и `attempts < max`; или при старте объект не найден в Phobos |
| archiving | failed | migrator | `attempts = max` |
| failed | pending | оператор | `tapegw admin retry` |
| pending, archiving, archived, failed | deleting | backend | DELETE или перезапись ключа; migrator, закончив put, не переведёт `deleting` в `archived` из-за условия `WHERE state='archiving'`, и объект удалит deleter |
| deleting | deleted | deleter | `phobos_delete` успешен или объекта в Phobos нет; файлы удалены |
| нет записи | requested | backend | RestoreObject для `archived` |
| requested | in\_progress | recaller | взят в батч, место в кэше есть |
| in\_progress | restored | recaller | файл в cache, размер и ETag сверены |
| in\_progress | requested | recaller | ошибка и `attempts < max` |
| in\_progress | failed | recaller | `attempts = max` |
| in\_progress | in\_progress / failed | recaller | `claim_ttl` истёк (процесс умер посреди чтения): захват перехвачен, попытка засчитана |
| restored | restored | backend | повторный RestoreObject продлевает `expires_at` |
| restored | нет записи | janitor | `online_until < now()` (обещанная дата, ADR-0003; инвариант `online_until >= expires_at`), строка удалена в транзакции, файл снят после commit |
| любое | нет записи | backend | объект ушёл в `deleting`: строка `restores` удаляется в той же транзакции, иначе recaller вечно видит restore у удалённого объекта, а метрики очередей врут |

## Потоки запросов

Каждый поток ниже — контракт для реализации и для e2e-тестов. Где поведение AWS неоднозначно, в скобках указано, что проверить в коде versitygw.

**PutObject**

1. Проверить бакет и ключ (`ValidateObjectKey`-логика как в posix-бэкенде).
2. Писать тело в `landing/tmp/<uuid>` через `O_TMPFILE` или временное имя, параллельно считая MD5 и запрошенные `x-amz-checksum-*`.
3. Сверить `Content-MD5` и checksum, если клиент их прислал; при расхождении удалить файл и вернуть `BadDigest`.
4. `fsync` файла, `rename` в `landing/<2 hex>/<uuid>`, `fsync` каталога.
5. Транзакция: вставить строку `pending`, старую текущую версию ключа перевести в `deleting`. Только после commit вернуть 200 с ETag.

Сбой между шагами 4 и 5 оставляет файл без строки; его подберёт janitor (orphan GC).

**Multipart**

- `CreateMultipartUpload` — строка в `uploads`, каталог `mpu/<upload_id>/`.
- `UploadPart` — запись части как в PutObject (fsync + rename), upsert в `upload_parts`.
- `CompleteMultipartUpload` — проверить список частей и их ETag, собрать итоговый файл в landing через reflink (`FICLONERANGE`; landing и mpu обязаны быть на одной XFS), с fallback на копирование. ETag = md5 от конкатенации бинарных md5 частей + `-N`. Дальше шаг 5 из PutObject, затем удалить каталог части.
- `AbortMultipartUpload`, `ListParts`, `ListMultipartUploads` — по таблицам. Незавершённые загрузки старше `mpu.ttl` удаляет janitor.

**Архивация (migrator)**

1. Взять батч: `SELECT … WHERE state='pending' AND next_try_at<=now() ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT n`, ограничение по суммарному размеру `batch.max_bytes`. Батч закрывается по числу, по объёму или по таймеру `batch.max_wait`.
2. Перевести строки в `archiving`, commit.
3. Каждый объект батча уходит **отдельной** операцией `put` протокола mover (ADR-0021): `op` — UUID версии, тело читается из landing-файла, атрибуты из раздела «Модель данных», family `tape`, layout и tags из конфига.
   Не один `phobos_put` на весь батч, как требовала первая редакция этого пункта. Причина — пачки (ADR-0020): с ними батч — это одна пачка мелких объектов плюс несколько крупных, а не тысяча отдельных записей, и разным пачкам нужны разные носители. Один xfer на весь батч либо уложил бы все пачки батча на одну ленту, либо потребовал бы от Phobos выбора носителя на каждый xfer внутри одного вызова. Идемпотентность при этом остаётся «одна операция — один объект»: `op` привязан к паре (размер, SHA-256), `resume` — одно смещение.

   **Носитель выбирает Phobos**, по тегам копии, которые migrator передаёт в `put`. tapegw ленту не выбирает и не должен: второй планировщик носителей поверх LRS расходится с ним при первом же repack. Цикл migrator выбирает **пул, спецификацию копии (теги) и mover**, а не ленту. Определение «доступна для записи» (`lifecycle IN ('formatted','active')` и незапрещённое поколение) нужно tapegw для другого: проверить место **до** отправки (есть ли в пуле лента нужного поколения с местом под этот размер — если нет, не начинать `put`, а сразу алерт «нужны ленты»), классифицировать `ENOSPC` после отказа («места нет» против «есть, но ленты закрыты» — см. [спайк M0-12](spikes/M0-12-write-flag-mailslots.md)) и считать свободное место пула планировщику ёмкости. Определение обязано быть одним — функцией или представлением, — иначе через месяц их будет три и свежая лента в состоянии `formatted` в одном из них потеряется.
4. Разбор итога батча. Полагаться на `xt_rc` нельзя (см. спайк M0-02): при ошибке он остаётся нулевым у всех целей. Поэтому после батча — и обязательно после любого сбоя — для каждого объекта проверяем `phobos_getmd` на наличие **и** `copy_status = complete`. Найден и полон → `archived`, `phobos_ver`, `archived_at`, плюс запись в `archive_copies`: `medium_id`, `sha256`, `format` (ADR-0022 — у каждой копии свой объект Phobos и своя строка). Носитель приходит в поле `extents` каждого `result` (ADR-0008, ADR-0021), отдельный запрос не нужен. Иначе → обратно в `pending`, `attempts+1`, экспоненциальный `next_try_at`; после `max_attempts` → `failed` и алерт. На повторе уже подтверждённые объекты исключаем, иначе батч снова упрётся в EEXIST.
5. Landing-копию не удалять сразу: её вытесняет janitor по watermark, начиная с давно архивированных.

Восстановление после падения: строки в `archiving` при старте проверяются через `phobos_getmd(uuid)`. Объект найден → `archived`, не найден → `pending`. Незавершённые копии чистит `phobos_delete_incomplete_copy`.

**HeadObject / ListObjects (v1, v2)**

- Только meta DB, лента не трогается. Листинг: префикс, delimiter, `CommonPrefixes`, `max-keys`, `continuation-token` / `marker`, порядок `COLLATE "C"`.
- Storage class: **всегда `GLACIER`**, независимо от состояния объекта (ADR-0003). Отклонение «`STANDARD` до архивации» убрано: оно заставляло клиента видеть смену класса без его действий.
- `x-amz-restore` — по колонке `objects.online_until` (см. «Модель данных» и ADR-0003): дисковая копия есть → `ongoing-request="false", expiry-date="<online_until>"`; `requested`/`in_progress` → `ongoing-request="true"`; копии нет → заголовка нет.
- `x-amz-restore` для архивных объектов: нет записи — заголовка нет; `requested`/`in_progress` — `ongoing-request="true"`; `restored` — `ongoing-request="false", expiry-date="…"`.

**RestoreObject**

- Объект не архивный → вернуть ошибку как AWS для не-Glacier объекта (`InvalidObjectState`).
- Нет записи в `restores` → вставить `requested` с `days`, `tier`; успех — 200 (ADR-0001, 202 из AWS недостижим без патча versitygw).
- Уже `restored` и срок не истёк → продлить `expires_at = max(expires_at, now() + days)`; успех — 200. Продление только растёт: повторный запрос на 7 дней не укорачивает обещанные 30.
- В `restores` стоит `requested` / `in_progress` → `RestoreAlreadyInProgress` (409), без побочных эффектов.
- У объекта живая дисковая копия (`landing=true`) → в очередь recaller'а НЕ ставится, только продление `online_until = max(online_until, now() + days)`: гонять Phobos за данными, которые и так на диске, смысла нет.
- Код успеха задать нельзя: `backend.Backend.RestoreObject` возвращает только `error`, а контроллер не заполняет `MetaOptions.Status`, поэтому успех — всегда 200. См. ADR-0001, спайк M0 по этой теме не нужен.

**Recaller**

1. Захват — **по одной пачке**, а не батчем по очереди: самая приоритетная готовая строка (приоритет tier Expedited → Standard → Bulk, затем `requested_at`), затем все готовые строки её пачки; `FOR UPDATE SKIP LOCKED`. Lease `recaller.claim_ttl` (в `next_try_at`) обязан покрывать одно чтение ленты, а не серию: при батче на весь ttl один воркер забрал бы очередь и читал пачки по очереди, второй драйв простаивал, а непрочитанные пачки после истечения lease перехватывались бы посреди чтения той же ленты. Просроченный захват перехватывается с засчитанной попыткой: процесс, умерший посреди чтения, не оставляет вечно длящий `in_progress`.
   Строки захвата закрепляются за воркером (`restores.claimed_by`, узел/слот — как `packs.claimed_by` у migrator), и захват **продлевается каждую треть срока** коротким `UPDATE … WHERE claimed_by = me` вне транзакции чтения (правило 13): чтение пачки — ожидание драйва, монтирование и передача гигабайт (ADR-0021), срок, выставленный один раз, истёк бы посреди чтения, и второй воркер крутил бы ту же ленту второй раз. Не удалось продлить (строки уже чужие) — результат не публикуется, пачкой занимается другой. `MarkRestoreRestored`/`MarkRestoreFailure`/`MarkRestoreCorrupt` проверяют, что их вызывает текущий владелец.
2. Проверить место в кэше: если `cache.used + сумма размеров > cache.high_watermark`, сначала вытеснить истёкшие restore, иначе ждать (backpressure, не ошибка). *Пункт ещё не реализован — S5 читает без проверки watermark.*
3. Если Phobos API отдаёт ленту и смещение объекта — отсортировать батч по (лента, смещение). Иначе положиться на переупорядочивание в LRS (спайк M0).
4. Участники одной пачки читаются одним потоком: `phobos_get` копии пачки (oid из `archive_copies`, сумма `sha256` той же строки сверяется с отданными байтами), участник извлекается в `cache/tmp/<uuid>`, затем проверяется по-своему: длина файла, полное расшифрование (AEAD-теги сегментов и манифеста, ADR-0019), открытый размер и ETag (md5 для одиночных PUT). Не прошёл — в `restored` не входим, ошибка в журнал. Отказ участника **при совпавшей сумме пачки — терминальный** (`failed` сразу, без ретраев): сумма доказывает, что с ленты пришли ровно те байты, что легли при архивации, — повтор прочитает то же и только потратит ленту. Ретраи остаются для ошибок чтения и несовпадения суммы пачки.
5. `fsync`, `rename` в `cache/<2 hex>/<uuid>` — и только потом строка → `restored`, `restored_at`, `expires_at = restored_at + days`, `online_until = max(online_until, restored_at + days)` (срок от завершения, ADR-0003). Порядок — ADR-0004 наоборот вытеснению: файл на диске раньше строки, которая на него ссылается. Решает `online_until` — одна колонка для заголовка, чтения из cache и вытеснения; `expires_at` справочное (из-за `GREATEST` он может быть меньше). Ошибки — ретраи как у migrator (backoff `recaller.retry_backoff`), затем `failed`.

**GetObject**

- `landing=true` → отдать из landing. Иначе `restored` и `online_until > now()` → из cache (решает одна колонка `online_until` — та же, что в заголовке и у janitor'а, ADR-0003; `expires_at` справочное). Иначе `InvalidObjectState`.
- Range-запросы и условные заголовки (`If-Match`, `If-None-Match`, `If-Modified-Since`) — поведение как в posix-бэкенде.
- Файл открывается до ответа, поэтому вытеснение **после** открытия безопасно: Linux держит inode до закрытия дескриптора.
- Промежуток «прочитали состояние → открыли файл» закрывает ADR-0004: janitor сначала ставит `landing=false` в транзакции и только потом делает `unlink`, а GET на `ENOENT` перечитывает строку и пересчитывает решение (максимум `spool.evict_retries` повторов, затем `InvalidObjectState`, а не `InternalError`).

**CopyObject**

Источник онлайн → копия как новый PUT (reflink, если источник в landing). Источник офлайн → `InvalidObjectState`, как в AWS.

**DeleteObject / DeleteObjects**

Транзакция: текущая строка → `is_current=false, state='deleting'`; ответ 204. Deleter-воркер вызывает `phobos_delete(uuid)`, удаляет файлы из landing и cache, переводит строку в `deleted`. Повтор `phobos_delete` для уже удалённого объекта считается успехом. Место на ленте не освобождается до repack; модель учёта живого и мёртвого — в M4, планировщик — после MVP ([предложение 0011](proposals/0011-repack-strategy.md)).

**Janitor** (периодически)

- Вытесняет landing-копии `archived`-объектов при превышении `landing.high_watermark` до `low_watermark`, начиная с самых старых.
- Удаляет истёкшие restore: строка из `restores`, файл из cache.
- Удаляет незавершённые multipart старше `mpu.ttl`.
- Orphan GC: файлы в landing, cache, mpu без соответствующей строки старше `orphan.grace`.
- Чистка терминальных строк: `objects` в состояниях `deleted` и `failed` помечаются `retired_at` и физически удаляются старше `meta.terminal_retention` (по умолчанию 90 дней). Маркеры удаления (`state = 'marker'`) к терминальным не относятся и `retired_at` не носят: удаление маркера по `versionId` переводит его в `deleted`, а живой маркер исчезнуть по сроку не может, иначе похороненные под ним версии воскреснут. Без этого таблица растёт всю жизнь системы: каждая перезапись ключа добавляет строку, а по `objects` построены индексы очередей и листинга. `admin verify` на ретайрных строках не ругается, но проверяет, что файлов у них уже нет.

## Интеграция с Phobos

tapegw работает как клиент `libphobos_store`: библиотека отправляет запросы локальному демону `phobosd` (LRS), а тот планирует драйвы, монтирует LTFS и ходит в TLC. Весь cgo изолирован в одном пакете за Go-интерфейсом, остальной код от cgo не зависит и тестируется без Phobos.

```go
// internal/store/store.go
type PutItem struct {
    ID    uuid.UUID
    File  *os.File        // держать открытым до конца вызова
    Size  int64
    Attrs map[string]string
}
type GetItem struct {
    ID   uuid.UUID
    File *os.File          // куда писать
}
type Result struct { ID uuid.UUID; Version int; Err error }

type Store interface {
    Put(ctx context.Context, items []PutItem, opt PutOptions) []Result
    Get(ctx context.Context, items []GetItem) []Result
    Stat(ctx context.Context, id uuid.UUID) (Meta, error)   // phobos_getmd
    Delete(ctx context.Context, ids []uuid.UUID) []Result
    Locate(ctx context.Context, ids []uuid.UUID) ([]Location, error) // если API позволяет (M0)
}
```

Реализации:

- `internal/store/phobos` — cgo, build tag `phobos`, линкуется динамически с `libphobos_store.so` (LGPL-2.1 это допускает).
- `internal/store/fake` — in-memory или каталог на диске, с инъекцией ошибок и задержек; используется в юнит-тестах и для локальной разработки.

Правила для cgo-слоя:

- `phobos_init()` один раз при старте, `phobos_fini()` при остановке.
- Вызовы `phobos_put` / `phobos_get` блокирующие и вынесены в отдельный бинарь `tapegw-mover` (ADR-0007): один процесс mover = один `phobos_init` = одна операция за раз. Пулы задаются числом процессов — `phobos.movers.migrator` и `phobos.movers.recaller`, сумма не больше числа драйвов, recaller минимум один, чтобы restore не ждал архивацию.
- **`xt_rc` не заполняется** (спайк M0-02): при ошибке батча все `xt_rc` остаются нулевыми, а возвращаемое значение несёт первую ошибку на всём батче. Результат проверяется отдельно: `phobos_getmd` на наличие **и** статус копии (`complete`, а не `incomplete`) — иначе прерванная запись будет принята за архивную. Колбэк завершения в MVP не используется (`cb=NULL`).
- Носитель копии узнаётся при архивации и кладётся в `archive_copies` (ADR-0008, [предложение 0013](proposals/0013-copies-placement-media.md)): у объекта может быть несколько копий, и каждая на своей ленте. Recaller группирует по этой таблице и в DSS не обращается. Отдельной колонкой `objects.phobos_medium` это **не** хранится: одна колонка не описывает несколько мест хранения, а после первой записи на ленту замена обошлась бы миграцией всего архива.
- Пока потокобезопасность libphobos не подтверждена спайком, пул ограничен одной горутиной. Это требование, а не оптимизация.
- `os.File` держать живым до возврата (`runtime.KeepAlive`), C-строки освобождать в `defer`, структуры — через `pho_xfer_desc_clean`.
- Отрицательный `errno` превращать в типизированные ошибки: `ErrNotFound`, `ErrNoSpace`, `ErrBusy` (ретраить), прочие считать фатальными для этого объекта.
- Отмена `ctx` не прерывает идущий C-вызов. Это фиксируется в комментарии; при остановке воркеры дожидаются текущего батча.

Конфигурация Phobos (вне кода tapegw, в `deploy/`):

- family `tape`: драйвы добавлены через `phobos drive add`, ленты — через `phobos tape add` и отформатированы в LTFS.
- family `dir`: каталоги на диске вместо лент; используется в CI и для разработки без железа. tapegw переключает family одной настройкой `phobos.family`.
- Библиотека типа SCSI, адрес TLC указывает на `klipper-tlc` (железо) или на штатный `phobos_tlc` (mhVTL).
- Layout `raid1`, `repl_count=1` в MVP. Две копии на разных лентах включаются только конфигом, без изменения кода.
- Версию Phobos зафиксировать на релизном теге и собирать RPM из исходников в контейнере EL9; в `main` сейчас 3.4.1.

## klipper-tlc: манипулятор как библиотека для Phobos

`klipper-tlc` — отдельный Go-демон, который снаружи неотличим от штатного `phobos_tlc`. Стоковый адаптер `lib_scsi` в Phobos подключается к нему по TCP, поэтому Phobos не патчится вообще.

**Протокол** (из `src/core/serializer/pho_proto_tlc.proto` в Phobos, protobuf2)

- Кадр: 4 байта длины в network byte order, затем 1 байт версии протокола (сейчас `PHO_TLC_PROTOCOL_VERSION = 2`), затем `PhoTlcRequest` или `PhoTlcResponse`.
- Запросы: `ping`, `drive_lookup{serial}`, `load{drive_serial, tape_label}`, `unload{drive_serial, tape_label?}`, `status{refresh}`, `refresh`, `stat{…}`. Ответ несёт тот же `id` в `req_id` либо `error{rc=-errno, message}`.
- `status.lib_data` — JSON-массив элементов в точности как в `scan_element()` из `src/tlc/tlc_library.c`: `type`, `address`, `full`, `volume` и остальные поля. Формат сверять по коду конкретного тега Phobos, а не по памяти; golden-тест сравнивает вывод `klipper-tlc` с выводом `phobos_tlc` на mhVTL.
- `.proto` скопировать в репозиторий с исходным заголовком лицензии (LGPL) и генерировать Go-код при сборке.

**Внутреннее устройство**

- `inventory` — состояние слотов и драйвов в локальном SQLite: адрес элемента, метка ленты, что в каком драйве, серийник драйва и его путь `/dev/sgN`. Адреса эмулируют SMC-нумерацию: манипулятор, слоты и драйвы в отдельных диапазонах, задаются конфигом.
- `robot` — интерфейс с тремя реализациями:
  - `moonraker` — реальный манипулятор: HTTP к Moonraker, G-code-макросы Klipper.
  - `smc` — делегирует перемещения настоящему SCSI-чейнджеру (MOVE MEDIUM). Нужен для mhVTL и как запасной путь для покупной библиотеки.
  - `sim` — в памяти, с настраиваемыми задержками и инъекцией сбоев, для юнит-тестов.
- `drive` — операции с драйвом через SG\_IO: TEST UNIT READY, выгрузка (LOAD/UNLOAD с load=0) и чтение MAM (barcode-атрибут) для сверки метки.

  **MAM читается только в окнах Load и Unload, когда LTFS в драйве не смонтирована.**
  Причина — та самая `Unit Attention`, которую драйв отдаёт на первый запрос после
  смены носителя: сообщить её он может **один раз на инициатора**, а все процессы
  одного хоста для драйва — один и тот же инициатор. Если наше чтение MAM
  придётся на момент, когда у драйва висит незабранный UA (сброс шины, смена
  носителя), оно «съест» сигнал, и LTFS этого события не увидит — а это ровно то
  событие, на которое LTFS обязана отреагировать ради целостности данных. В
  окнах Load и Unload ленты LTFS в драйве нет, и съесть чужое некому. На mhVTL
  это не опасно, на реальном драйве — вполне: поведение проверяется в M8
  ([спайк M0-12](spikes/M0-12-write-flag-mailslots.md) закрыл сам факт окна, но
  не проверку на настоящем железе).
- Все команды манипулятора сериализуются одним мьютексом: у робота одна каретка.

**Load(drive, label)**

1. Найти слот по метке в inventory; драйв должен быть пуст.
2. `robot.Move(slot → drive)`.
3. Ждать TEST UNIT READY драйва до `drive.ready_timeout`.
4. Если включено, прочитать метку из MAM **в этом же окне, до того как LTFS смонтировала ленту** (см. правило про окна в разделе `drive` выше), и сравнить с ожидаемой; расхождение → выгрузить ленту обратно, ответить ошибкой, пометить слот как требующий проверки.
5. Обновить inventory в одной транзакции, ответить успехом.

**Unload(drive)**

1. Выгрузить ленту из драйва (UNLOAD) и дождаться, пока картридж выйдет в позицию захвата. Кто делает eject в штатном потоке — Phobos или TLC — проверить в `tlc_library.c` (спайк M0); `klipper-tlc` должен делать это сам, если Phobos не делает.
2. `robot.Move(drive → home slot)`; home slot — исходный слот ленты из inventory.
3. Обновить inventory, вернуть метку и адрес слота.

**Контракт с Klipper**

- Макросы в `printer.cfg`: `TAPE_HOME`, `TAPE_SLOT_TO_DRIVE SLOT=<n> DRIVE=<m>`, `TAPE_DRIVE_TO_SLOT DRIVE=<m> SLOT=<n>`. Координаты слотов и драйвов, работа захвата и поворотной головы — внутри макросов, `klipper-tlc` о геометрии не знает.
- Вызов: `POST /printer/gcode/script` в Moonraker. Отдельный HTTP-таймаут на каждый тип движения. Как Moonraker сообщает об ошибке макроса и когда возвращает ответ, проверить на стенде.
- Ошибки механики (захват пуст, концевик не сработал) макрос сообщает через `RESPOND TYPE=error` или `action_raise_error`. `klipper-tlc` превращает их в `-EIO` и переводит библиотеку в состояние «требует оператора»: дальнейшие load/unload отклоняются до `klipper-tlc reset`.
- `ping` → `library_is_up` = Moonraker отвечает и Klipper в состоянии `ready`.

**CLI оператора**: `klipper-tlc inventory list`, `slot set <n> <label>`, `slot clear <n>`, `move <from> <to>`, `home`, `reset`, `verify <slot>` (загрузить, прочитать MAM, выгрузить).

## Конфигурация, развёртывание, стенды

Прод — один хост на EL9 (Rocky или Alma); все компоненты кроме Klipper работают на нём под systemd. Klipper и Moonraker живут на контроллере манипулятора.

**Юниты на хосте**: `postgresql`, `phobosd`, `klipper-tlc` (или `phobos_tlc` для mhVTL), `tapegw`. Порядок запуска задаётся через `After=`/`Requires=`. tapegw стартует только когда доступны Postgres и `phobosd`, иначе ретраит с бэкоффом и сообщает об этом в `/health`.

**Диски**: landing и mpu — одна XFS с `reflink=1` (быстрый SSD или NVMe, по скорости не ниже суммарной записи драйвов), cache — отдельный том. Лимиты и watermark заданы в процентах тома.

`cache/tmp` обязан быть на той же ФС, что и `cache`: recaller пишет во временный файл и поднимает его `rename`-ом, а `rename` через точки монтирования даёт `EXDEV`. Требование проверяется на старте сравнением `st_dev` и падает с внятной ошибкой, а не на первом restore.

### Уточнение к правилу 9: именованные коллекции

Списки в конфиге — это **словари по устойчивому ключу**, а не YAML-списки.
Индекс в имени переменной (`TAPEGW_PHOBOS_MOVERS_0_ADDR`) не используется: он
привязан к порядку, и перестановка элементов в YAML молча начинает править
настройку другого элемента — тихая ошибка того же класса, что мы уже ловили.

```yaml
phobos:
  movers:
    drive1:            # ключ = имя драйва = слот воркера = часть SAN mover://<library>/<drive>
      addr: lib1:7443
      node: lib1
      library: lib-main
```

- переменная выводится из ключа однозначно: `TAPEGW_PHOBOS_MOVERS_DRIVE1_ADDR`;
- ключ ограничен `^[a-z0-9_]+$`, иначе конфиг отклоняется при загрузке: так
  преобразование «ключ → часть имени» взаимно однозначно (никаких «`drive-1` и
  `drive_1` дают одну переменную»);
- одно имя служит ключом конфига, слотом в `WorkerID` и частью SAN-URI
  сертификата mover'а: одна идентичность вместо трёх, которые могут разойтись;
- переменная правит поле **существующего** ключа. Переменная про несуществующий
  ключ или несуществующее поле — ошибка загрузки, а не молчаливое игнорирование:
  «добавить mover переменной» нельзя так же, как нельзя опечатку в YAML (разбор
  строгий);
- целиком коллекцию можно задать одной переменной в JSON —
  `TAPEGW_PHOBOS_MOVERS='{"drive1": {...}}'` (контейнеры, CI). Она **заменяет**
  словарь целиком, а не дополняет. Заданы и коллекция целиком, и поле — отказ:
  какое значение главнее, не угадываем.

Распространяется на все будущие коллекции: пулы, библиотеки, спецификации копий.

**Конфиг tapegw** (YAML; любое поле переопределяется переменной `TAPEGW_*`):

```yaml
s3:
  listen: [":7070"]
  region: us-east-1
  tls: {cert: "", key: ""}
  # Файловые учётки уходят с T1: Postgres-IAM обязателен, а два источника
  # учёток сразу запрещены.
  # iam_dir: /etc/tapegw/iam
  # Класс хранилища не настраивается: у нас только лента, GLACIER отдаётся всегда
  # (ADR-0003). Поля вроде default_storage_class в конфиге нет намеренно.
  max_object_size: 100GiB               # <= migrator.batch_max_bytes, проверяется
                                       # при старте; см. ADR-0008 и M0-07
iam:
  kek_file: /etc/tapegw/kek.json        # кольцо KEK (версия -> base64-ключ, 0400).
                                        # Обязателен с T2: шифрование тела включено
                                        # всегда, а без KEK нечем завернуть ключ
                                        # тенанта (ADR-0014)
db:
  dsn: postgres://tapegw@localhost/phobos?search_path=tapegw
spool:
  landing: /srv/tapegw/landing          # mpu/ лежит здесь же
  cache: /srv/tapegw/cache
  landing_watermark: {high: 80, low: 60}
  cache_watermark: {high: 85, low: 70}
  mpu_ttl: 168h
  orphan_grace: 24h
phobos:
  family: tape                          # dir для CI и разработки
  layout: raid1
  layout_params: {repl_count: "1"}      # 1 реплика: mover берёт свой драйв
  tags: []
  max_inflight: 1                       # процессов mover с собственным phobos_init
  # library: tape_lib                   # появляется в M4: API не читает профиль,
  #                                     # носитель и библиотеку задаём явно (ADR-0007)
  # movers:                             # M4/M5, вместе с бинарем tapegw-mover
  #   migrator: 1                       # процессов mover на архивацию
  #   recaller: 1                       # и на restore; сумма <= числа драйвов
# pack:                                 # появляется в M4: упаковка мелких объектов
#   threshold: 25MiB                    # меньше — в пачку, больше — отдельным объектом
#   target_size: 2GiB                   # точное значение — по спайку phobos_get
#   max_wait: 8h                        # переопределяется на уровне пула;
#                                       # отсчёт от самого старого объекта в пачке
# journal:                              # журнал метаданных — отдельный поток пачек
#   pack: {max_wait: 1h}                # окно «событие → лента» не больше этого;
#                                       # отсчёт от самого старого события
migrator:
  batch_max_objects: 1000
  batch_max_bytes: 200GiB
  batch_max_wait: 15m
  max_attempts: 10
  retry_backoff: 30s
recaller:
  batch_max_objects: 500
  batch_max_wait: 5m
  max_attempts: 10
  retry_backoff: 30s
  claim_ttl: 30m                       # lease захвата строки restores: чтение пачки с ленты минутное
metrics: {listen: ":9464"}
```

Числа в примере — стартовые значения для настройки на стенде, не требования.

**Стенды**

| Стенд | Где | Что внутри | Для чего |
| --- | --- | --- | --- |
| unit | любой Linux, `go test ./...` | `store/fake`, `robot/sim`, Postgres через testcontainers | логика, state machine, листинг, ретраи |
| ci-dir | контейнер EL9 в CI | Postgres + Phobos с family `dir` + tapegw, собранный с тегом `phobos` | реальный cgo-путь без лент, e2e Glacier-цикл |
| vtl | VM EL9 (нужен модуль ядра, контейнер не подходит) | mhVTL: библиотека и LTO-драйвы, LTFS, Phobos family `tape`, сначала `phobos_tlc`, потом `klipper-tlc` с бэкендом `smc` | монтирования, LTFS, протокол TLC, сбои драйва |
| hw | прод-хост | реальные драйвы, манипулятор, `klipper-tlc` с бэкендом `moonraker` | механика, тайминги, MAM-сверка |

Поддержку LTFS на виртуальных драйвах mhVTL проверить в спайке M0. Если её нет, стенд vtl проверяет только загрузку и выгрузку лент и протокол TLC, а запись через LTFS проверяется на hw.

## Наблюдаемость и эксплуатация

Оператор должен по одной панели понимать три вещи: успевает ли архивация за записью, сколько ждут restore и не застряло ли что-то в `failed`.

**Метрики Prometheus** (`tapegw_*` и `klipper_tlc_*`)

- Очереди: число объектов и байт по `state`; возраст самого старого `pending`; число restore по `state` и `tier`.
- Архивация: байт/с и объектов/с, длительность батча, ошибки `phobos_put` по коду.
- Restore: гистограмма «запрос → restored», ошибки `phobos_get`.
- Диски: заполненность landing и cache, объём, вытесненный janitor.
- S3: запросы по операции и коду ответа. Своим экспортёром: versitygw v1.10.0 Prometheus не отдаёт вообще, у него только StatsD/DogStatsD (ADR-0002). Имя S3-действия в контексте versitygw отсутствует, метку `action` выводим сами из метода и query-подресурсов.
- klipper-tlc: число load/unload, их длительность, ошибки механики, состояние «требует оператора».

**Логи** — `log/slog` в JSON; у каждой записи воркера есть `object_id` и `batch_id`, у S3-запросов — request id из versitygw.

**Health**: `/health/live` (процесс жив) и `/health/ready` (Postgres, `phobosd` и диски доступны, landing ниже high watermark). При landing выше `high` + запас PUT получает `SlowDown` (503): лучше притормозить клиентов, чем переполнить диск. Делается это middleware-ом через `embedgw.Config.S3Options` → `s3api.WithMiddleware` — патчить versitygw не нужно. Шов монтируется **до** аутентификации и ограничивает только операции записи (`PutObject`, `UploadPart`, `CompleteMultipartUpload`): блокировать `DeleteObject` означало бы запрещать то, что диск освобождает.

**Админ-API и CLI** (`tapegw admin …`, отдельный порт или префикс `AdminPathPrefix`): список объектов по состоянию, retry для `failed`, принудительное вытеснение из cache, статистика очередей, `rebuild-meta` (M9).

**Алерты-примеры** в `deploy/alerts.yml`: есть `failed`-объекты; самый старый `pending` старше SLA; landing выше high watermark дольше 30 минут; `klipper-tlc` в состоянии «требует оператора»; ready-проба падает.

**Бэкап и DR**

- Postgres: ежедневный `pg_dump` обеих схем (`tapegw` и DSS Phobos) плюс WAL-архив. Без DSS Phobos ленты читаются только через импорт.
- Последняя линия: импорт лент средствами Phobos (`src/admin/import.c`, точную команду CLI сверить с версией) восстанавливает DSS с лент, затем `tapegw admin rebuild-meta` пересоздаёт схему `tapegw` из атрибутов объектов Phobos.

## Репозиторий и правила для агента

Один Go-модуль `tapegw` с двумя бинарями. Этот документ лежит в репозитории как `docs/ARCHITECTURE.md` (экспорт в Markdown) и считается источником истины; любое отступление фиксируется ADR в `docs/adr/`.

```
cmd/
  tapegw/            main: конфиг, embedgw.RunVersityGW, запуск воркеров, admin CLI
  klipper-tlc/       main: TLC-сервер, CLI оператора
internal/
  config/            загрузка YAML + env, валидация
  backend/           реализация backend.Backend (S3-семантика), встраивает BackendUnsupported
  meta/              доступ к Postgres, миграции (embed), запросы очередей
  spool/             landing / cache / mpu: атомарная запись, fsync, reflink, watermark
  store/             интерфейс Store
    phobos/          cgo-реализация (build tag phobos)
    fake/            тестовая реализация
  worker/            migrator, recaller, janitor, deleter
  s3err/             маппинг внутренних ошибок в ошибки S3 versitygw
  metrics/
  tlc/
    proto/           pho_proto_tlc.proto (LGPL-заголовок) + сгенерированный код
    server/          кадрирование, диспетчер запросов
    inventory/       SQLite
    robot/           moonraker, smc, sim
    drive/           SG_IO: TUR, unload, MAM
deploy/              systemd, пример phobos.conf, printer.cfg-макросы, alerts.yml, Containerfile EL9
test/e2e/            сценарии против живого tapegw (aws sdk для Go)
docs/                ARCHITECTURE.md, adr/, spikes/
AGENTS.md            краткие правила ниже + команды сборки и тестов
```

**Правила**

1. Go — версия, которую требует закреплённая версия versitygw (сейчас `go 1.26`); versitygw фиксируется точной версией в `go.mod`, обновление — отдельный PR.
2. `go build ./... && go test ./...` проходят на любом Linux без Phobos и без cgo. Всё, что требует Phobos, — под тегом `phobos` и в стенде ci-dir.
3. Исходники Phobos и versitygw не патчатся. Если без патча нельзя — сначала ADR и вопрос в ревью.
4. Неподдерживаемые S3-операции возвращают `NotImplemented` через `BackendUnsupported`, а не молча что-то делают.
5. Поведение, которое уже есть в versitygw, повторяется по его коду, а не придумывается: для обычных операций — posix-бэкенд (валидация ключей, Range, условные заголовки, checksum), для Glacier-семантики (`x-amz-restore`, storage class, `RestoreObject`) — `backend/scoutfs/scoutfs_compat.go`.
6. Любое изменение состояния объекта — одна транзакция Postgres. Сначала файл на диске с `fsync`, потом commit. Никогда наоборот.
7. Каждый воркер идемпотентен: повтор после падения в любой точке приводит к тому же результату. Для каждого воркера есть тест «упасть после шага N».
8. Ошибки оборачиваются через `fmt.Errorf("…: %w", err)`, логи — `slog`, никаких `panic` вне `main`.
9. Конфигурируемые числа (таймауты, размеры батчей, watermark) — только в конфиге, не константами в коде.
10. Один PR — один пункт плана из раздела «План разработки». В описании PR: что сделано, как проверено, отступления от документа со ссылкой на ADR.
11. Миграции БД — только вперёд, одна миграция на PR, без правки уже слитых.
    Up и Down проверяются текстом файла и прогоняются Down → Up на таблице с
    данными: на пустой схеме порядок шагов не важен и миграция, падающая на
    живой базе, остаётся зелёной.
    `Down` существует только для dev-баз и может быть невыполним на реальных
    данных (например, `Down` миграции 0007 после первого отзыва учётки —
    см. ADR-0018); в проде откат не запускается.
12. Лицензия репозитория — Apache-2.0 (как у versitygw); файлы, взятые из Phobos, сохраняют LGPL-заголовок.

13. Ни одна транзакция Postgres не открыта во время вызова Phobos. Воркер коммитит переход состояния (например `pending → archiving`), отпускает транзакцию и лок, и только потом вызывает `phobos_put`; результат пишется новой транзакцией. Иначе лента держит строки и advisory-локи ключей: `phobos_put` батчем на 200 GiB может идти минуты, а параллельный PUT тех же ключей в это время упрётся в `lock_timeout`. Таймаут лока оставляем как вторую линию защиты, а не как замену правилу.

## План разработки

Десять этапов M0–M9: M0 снимает неизвестные, M1–M6 строят tapegw снизу вверх, M7–M8 параллельно строят klipper-tlc, M9 доводит всё до эксплуатации. Этап закрыт, когда выполнены все пункты его критериев и прошло ревью.

Поверх них — этапы **T0–T5 мультитенантности** ([предложение
0009](proposals/0009-multitenancy-and-gaps.md)) и **V — версионирование и Object Lock**
([предложение 0010](proposals/0010-owner-decisions.md)). Они не идут после M-этапов, а
встраиваются в них: T0 до продолжения M2, T1 и V вместе с M2, T2 до M4, T3 вместе с M4–M5,
T4 вместе с M6 и M9. Причина перемешивания в том, что четыре вещи — пулы лент,
шифрование, принадлежность объекта тенанту и **версионирование** — определяют либо
формат данных на ленте, либо смысл перезаписи и удаления. Первые три надо решить до
первого боевого `phobos_put`, то есть до M4: после первой записи смена решения означает
перезапись всех данных. Версионирование надо решить до листинга и удаления, иначе тот же
код пишется дважды.

&#91;embedded content: этапы M0–M9, T0–T5 и зависимости\]

M5 — первый полный Glacier-цикл без железа; M8 переносит его на реальные драйв и манипулятор. Порядок: **M0 → M1 → T0 → M2 + T1 + V → M3 → T2 → M4 + T3 → M5 + T3 → M6 + T4 → M9 + T4**; параллельно M0 → M7 → M8 → M9; закрытие M7 и M8 требует готового M5.

Решения владельца по шифрованию, версионированию, Object Lock и масштабу — в
[предложении 0010](proposals/0010-owner-decisions.md); оно закрывает четыре вопроса
из «Рисков» и добавляет этап V.

### M0. Спайки и стенд ci-dir

Результат — `docs/spikes/*.md` с выводами и `deploy/Containerfile` EL9 с Phobos. Код спайков в основную ветку не идёт.

- [ ] Собрать Phobos (зафиксированный тег) и LTFS из исходников в контейнере EL9; `phobos put/get/delete` через CLI на family `dir`.
- [ ] Минимальная cgo-программа: `phobos_init`, `phobos_put` батчем из 3 объектов по fd с атрибутами, `phobos_getmd`, `phobos_get`, `phobos_delete`.
- [x] Потокобезопасность: libphobos однопоточна, один `phobos_put` пишет на один носитель — см. [M0-02](spikes/M0-02-libphobos-api.md) и [ADR-0007](adr/0007-mover-worker.md); вместо `max_inflight` два пула процессов-воркеров.
- [ ] Есть ли в API способ узнать ленту и смещение объекта (для сортировки restore).
- [ ] Кто выгружает ленту из драйва перед unload в штатном потоке TLC.
- [ ] Может ли бэкенд versitygw влиять на код ответа RestoreObject (200 / 202 / 409).
- [ ] mhVTL на VM EL9: штатный `phobos_tlc`, load/unload, работает ли LTFS на виртуальных драйвах.
- [x] Теги Phobos в записи: жёсткий фильтр или предпочтение — см.
  [M0-08](spikes/M0-08-phobos-tags.md). Жёсткий фильтр со семантикой И, но
  помеченную ленту он не защищает от записи без тега: отсюда обязательное тегирование
  каждой записи в [ADR-0013](adr/0013-tape-pools.md).
- [ ] **Спайк M0-10: поведение `phobos tape repack`**
  ([предложение 0011](proposals/0011-repack-strategy.md)), до кода mover'а.
  Главный вопрос — **соблюдает ли repack теги пулов**: данные ленты пула A не
  должны оказаться на ленте пула B. После находки M0-08 (теги защищают только в
  одну сторону) это не очевидно, а нарушение изоляции — блокер для выделенных
  пулов и пересмотр ADR-0013. Дальше: как после repack обновляются DSS и
  `phobos extent list`, жизненный цикл ленты (переформатирование и возврат в пул),
  судьба объектов, удалённых в Phobos, и время repack ленты 4 ГБ при 50 % и 90 %
  живых данных. Результат — `docs/spikes/M0-10-phobos-repack.md`.
  Формулировка предложения ставила этот спайк «до ADR-0020»; по факту от него
  зависят ADR-0013 и пригодность repack вообще, но не формат записи ADR-0020 —
  уплотнение пачек делает mover, а не Phobos, и три требования к формату из
  предложения 0011 в ADR-0020 уже внесены.

- [x] **Спайк M0-11: библиотеки, поколения и копии — блокирующий вопрос закрыт**
  ([предложение 0013](proposals/0013-copies-placement-media.md)), **до кода
  migrator'а**. Пять вопросов, каждый меняет модель данных, а не только операцию.
  Закрыт вопрос 3 (копии): вторую копию с другими тегами записать можно, но
  **отдельными объектами Phobos**, а не встроенной репликацией — отсюда
  [ADR-0022](adr/0022-copies-as-separate-objects.md). Закрыт и вопрос 5: MAM
  читается `sg_read_attr` (в `sg3_utils` 1.47 он есть), 48 атрибутов, включая
  TapeAlert, счётчик загрузок и объём за жизнь, — **но только когда драйв не
  зарезервирован `phobosd`**, а при смонтированной ленте зарезервирован. Что
  делать с этим в Ф1.6 — см. [M0-12](#m0-спайки-и-стенд-ci-dir).
  Вопросы 1, 2 и 4 не проверялись и migrator'а не блокируют (переезд между
  библиотеками — процедура эксплуатации, а не формат); сняты в Ф2.8 и Ф2.2.
  Отчёт — [`M0-11`](spikes/M0-11-libraries-generations-copies.md).

  Изначальные формулировки вопросов (для истории):
  1. `phobos tape rename --new-library` — после физического переноса ленты в другую
     библиотеку `phobos_get` обязан читать объект без переимпорта; то же для
     `drive --new-library`;
  2. совместимость поколений в конфиге Phobos (`tape_type`/`drive_type`): лента LTO-7
     читается драйвом LTO-8, а запись с тегом `gen:lto8` идёт только на LTO-8;
  3. **копии в Phobos 3: можно ли вторую копию записать с другими тегами.** Если нельзя —
     копии ведёт tapegw отдельными объектами Phobos, и это фиксируется в ADR. Вопрос
     блокирующий: от ответа зависит, будет ли `archive_copies` описывать копии Phobos
     или самостоятельные объекты;
  4. `phobos tape repack` с целевым тегом — на нём держится reconciler и миграция
     поколений;
  5. какие атрибуты MAM доступны на виртуальных драйвах — из них собирается
     `media.attrs`.
  Стенд: две виртуальные библиотеки и драйвы двух поколений на mhVTL. Результат —
  `docs/spikes/M0-11-libraries-generations-copies.md`.

- [ ] **Спайк M0-13 — привязано ли состояние libphobos к потоку ОС.** M0-03
  закрыл вопрос «можно ли вызывать параллельно» (нельзя), но не проверял
  thread-local: держит ли библиотека состояние, привязанное к потоку. Если да —
  последовательные вызовы из разных потоков ломают её так же, как параллельные,
  и мьютекс от этого не спасает. Решение принято заранее (см. поправку в
  [ADR-0007](adr/0007-mover-worker.md)): все вызовы — из одной горутины-владельца
  с `runtime.LockOSThread()`. Спайк нужен, чтобы знать, спасаем мы себя от
  реальной проблемы или от гипотетической: обёртка на C пишет `gettid()` каждого
  вызова, тест под тегом `phobos` засыпает store запросами из нескольких горутин
  и сверяет, что tid один и пересечений по времени нет.

- [x] **Спайк M0-12 — главный вопрос закрыт**
  ([предложение 0014](proposals/0014-media-lifecycle-migrations-plan.md), Ф0.6).
  **Окно для TLC есть**: сторонний процесс открывает устройство драйва при
  смонтированной ленте и во время выгрузки — за 90 замеров через 0.2 с состояния
  «недоступно» не было ни разу; `phobosd` в простое вообще не держит `/dev/st*`.
  Значит `klipper-tlc` сможет и выгружать, и сверять метку, а MAM читается и
  после загрузки (первый запрос отдаёт `Unit attention`, со второго — значения).
  **Читать его при этом надо только в окнах Load/Unload**, где LTFS в драйве нет:
  незабранный `Unit Attention` — сигнал один на инициатора, а мы с LTFS для
  драйва один инициатор.
  **Флаг `put` работает, но отказ приходит как `ENOSPC`**: различить «полка
  заполнена» и «полка закрыта политикой» по коду ошибки нельзя, поэтому слой 2
  (`read_only` в `media`) обязателен, а проверка места в пулу обязана исключать
  закрытые ленты. Почтовые слоты и пересканирование после замены магазина не
  проверялись и перенесены в M7/M8. Отчёт —
  [`M0-12`](spikes/M0-12-write-flag-mailslots.md).

  Изначальные формулировки вопросов (для истории):

  1. **флаг `put` у носителя**: `phobos tape set-access -- -P` на стенде принимается,
     но надо проверить, что LRS действительно отказывает в записи и что чтение и
     удаление остаются; как вернуть флаг; не трогает ли `repack` или форматирование;
  2. **почтовые слоты (Import/Export) и выгрузка ленты штатным `phobos_tlc`** —
     на чём держится Ф2.5 (выгрузка) и С4;
  3. **пересканирование инвентаря после замены магазина**: `phobos lib scan` /
     `refresh` — видит ли Phobos новые ленты и не дублирует ли уже известные.
  Дополнительно: **как читать MAM при занятом драйве**. `sg_read_attr` отвечает
  `Device or resource busy`, а `phobos drive scsi_release` не даёт снять резервирование
  с залоченного привода. Варианты к проверке: читать в окне между загрузкой и
  резервированием; ограничиться `phobos tape list -o stats.*` (он отдаёт `nb_load`,
  `nb_errors`, `phys_spc_free/used`, `model`, но на mhVTL `nb_load` упорно ноль);
  читать MAM на реальном драйве в M8. От ответа зависит, что реально наполняет
  `media.attrs` в Ф1.6. Результат — `docs/spikes/M0-12-write-flag-mailslots.md`.

### M1. Скелет

- [ ] Структура репозитория, `AGENTS.md`, CI: `go vet`, `staticcheck`, юнит-тесты, сборка с тегом `phobos` в контейнере.
- [ ] Конфиг, запуск `embedgw.RunVersityGW` с бэкендом-заглушкой, IAM из `iam.dir`.
- [ ] Миграции схемы `tapegw`, пакет `meta`.
- [ ] Бакеты: Create, Head, List, Delete (только пустой), ACL, tagging.

* **Готово когда**: `aws s3 mb / ls / rb` работают против tapegw.

### M2. Онлайн-путь без ленты

Разбит на три PR: (1) spuл + Put/Head/Get, (2) ListObjects v1/v2,
(3) Delete/DeleteObjects/перезапись/CopyObject.

- [x] `spool`: атомарная запись с fsync, MD5 и checksum на лету.
- [x] Первая миграция M2: `online_until NOT NULL` и сужение `objects_evictable`
  (пункты ревью PR #1).
- [x] PutObject, HeadObject, GetObject (Range, условные заголовки).
- [ ] CopyObject (только онлайн-источник).
- [ ] ListObjects v1 и v2: префикс, delimiter, пагинация, порядок байтов.
- [ ] DeleteObject, DeleteObjects, перезапись ключа.
- [ ] **Остаток M2 (листинг, удаление, CopyObject) делается вместе с T1 и этапом V**,
  а не отдельно: все три операции придётся писать сразу под стража тенанта и с
  версионированием. Отдельный «листинг параллельно всему» здесь обходится
  переписыванием через два этапа.
- [ ] **RPO и RTO — требование к деплою, а не «после MVP» (пробел Б1).** Пока
  объект не заархивирован, он существует в **единственной** копии на landing, а
  Postgres один. Потеря диска landing = потеря принятых данных клиентов. До
  закрытия M2 записать числами в `ARCHITECTURE.md`: landing на зеркале (RAID1 или
  ZFS mirror), Postgres — потоковая реплика + PITR, и сами значения RPO/RTO.

  **Зеркало под landing — обязательное требование к деплою, а не рекомендация.**
  Другого способа закрыть разрыв между «данные приняты» и «данные на ленте» нет:
  реплика Postgres чужие данные не везёт, а лента ещё не записана.

  **RPO для объектов меньше `pack.threshold` (25 МБ по умолчанию) — 8 часов**,
  ровно `pack.max_wait`: такая объект может лежать на landing до закрытия
  пачки. Это значение идёт в договор, потому что для клиента оно означает «при
  аварии безвозвратно теряются загрузки последних 8 часов», и узнавать это из
  исходников tapegw клиент не должен. Крупные объекты, идущие вне пачек, этим
  сроком не ограничены: они архивируются в ближайшем батче.

  **Изменения блокировок, legal hold и удаления версий попадают на ленту не
  позднее чем через 1 час** — `journal.pack.max_wait`, и это значение идёт в
  договор рядом с 8 часами для мелких объектов. Раздельные потоки пачек для
  данных и для журнала нужны именно затем, чтобы это обещание не зависело от
  того, насколько бойко идёт архивация данных: см. M4, пункт про журнал.

  Примечание к первому PR: чтобы PutObject вообще дошёл до бэкенда, пришлось
  реализовать `GetObjectLockConfiguration` и `GetBucketVersioning`. Versitygw
  опрашивает их на пути обычного PUT (`auth.loadObjectLockState`) и терпит
  только `ObjectLockConfigurationNotFound`; `NotImplemented` из
  `BackendUnsupported` превращался в 501 на полностью реализованную операцию.
  См. `internal/backend/capabilities.go`.

* **Готово когда**: `aws s3 cp/sync/ls/rm` и `rclone copy/check` проходят на наборе файлов от 0 байт до нескольких ГБ, ключи с юникодом и пробелами; тест порядка листинга против таблицы ожидаемых значений.

### M3. Multipart

- [ ] Create, UploadPart, Complete (reflink с fallback), Abort, ListParts, ListMultipartUploads.
- [ ] ETag вида `md5-N`; janitor удаляет загрузки старше `mpu_ttl`.

* **Готово когда**: `aws s3 cp` файла больше порога multipart и rclone с параллельными частями дают корректные ETag и содержимое.

### Жизненный цикл носителей, копии и миграции: фазы 0–2

Собрано в [предложение 0014](proposals/0014-media-lifecycle-migrations-plan.md):
девять сценариев (С1…С9) и одиннадцать решений (Р1…Р11), на которых они держатся.
Здесь — только порядок работ и то, чем фаза считается закрытой; разбор сценариев
в предложении, дублировать его сюда смысла нет.

Где 0014 уточняет [предложение 0013](proposals/0013-copies-placement-media.md),
действует 0014.

**Приёмочный сценарий — С4** (MSL4048: 4 магазина по 12 лент LTO-7, всё занято,
освободить магазин 4 и поставить магазин LTO-8). Он выбран не потому, что самый
частый, а потому что задействует почти всё сразу: запрет записи на устаревшее,
слив по месту, планирование ёмкости, чтение на старом поколении и запись на
новое, выгрузку лент и уплотнение. Если С4 проходит на стенде от начала до конца —
основа готова.

#### Фаза 0 — до кода migrator, и она не откладывается

Всё здесь фиксируется **формой таблиц, форматом на ленте и протоколом**. После
первой записи на ленту это меняется только миграцией всего архива — та же логика,
что уже дважды нас кусала (формат пачки, колонка «носитель»).

| # | Что | Куда |
| --- | --- | --- |
| Ф0.1 | ADR по копиям: копии как отдельные объекты Phobos, форма `archive_copies`, атрибут `tapegw.copy`, список запрещённых операций | [ADR-0022](adr/0022-copies-as-separate-objects.md) |
| Ф0.2 | **Версия формата у каждой копии** (`archive_copies.format`) + `write_format` в конфиге. Во время миграции поколения и формата живут копии в разных версиях, читатель выбирает декодер по версии копии, а не по «текущей версии системы» | ADR-0020, ADR-0022 |
| Ф0.3 | Реестр носителей и драйвов + **геометрия**: `media.library_cache`, `media.magazine`, `media.home_slot`; состояния `read_only`/`evacuating`/`retired`; локации `in_transit`/`offline`/`offsite` | миграция M4 |
| Ф0.4 | **Локальная передача между mover одного сервера**: буфер принадлежит серверу библиотеки, а не процессу mover; операция записи может сослаться на операцию чтения другого mover по UUID. Пути в протоколе по-прежнему не передаются | поправка к [ADR-0021](adr/0021-mover-protocol.md) |
| Ф0.5 | Конфиг поколений: совместимость чтения/записи, флаг `writable`; политика со ссылкой на запрещённое поколение отклоняется **при старте**, а не в момент записи | конфиг, ADR-0022 |
| Ф0.6 | Спайк M0-12: флаг `put` у носителя, почтовые слоты, пересканирование после замены магазина | `docs/spikes/` |

**Закрыто, когда:** ADR приняты, миграция M4 с `archive_copies`, `media`, `drives`
и журналами событий проверена на таблице с данными, поправка к ADR-0021 принята.

Проверка, которая держит всю фазу: **в схеме не должно быть колонок носителя вне
`archive_copies`** (тест на отсутствие `medium`/`phobos_medium` уже есть), и копия
без версии формата не вставляется.

#### Фаза 1 — MVP, идёт вместе с M4, M5, M7–M8

Запись копий по политике (Ф1.1), выбор копии при restore и состояние «носитель
недоступен» (Ф1.2, Ф1.3), трёхслойный запрет записи на устаревшее (Ф1.4),
добавление ленты одной операцией tapegw с проверками (Ф1.5), сбор MAM с первого
дня (Ф1.6) и сторона слота из геометрии (Ф1.7). Детали — в пунктах M4/M5/M7 ниже.

**Закрыто, когда:** С1 работает (две копии на разных сторонах, restore при `lost`
одной), и С3 работает (запись на LTO-7 невозможна ни политикой, ни ручным
`phobos put`).

#### Фаза 2 — после MVP, и порядок внутри неё не произвольный

**Ф2.1 ядро reconciler → Ф2.4 планировщик ёмкости → Ф2.3 слив по месту + Ф2.5
выгрузка лент → Ф2.2 ленивая миграция поколения → Ф2.6 контейнерная миграция →
Ф2.8 переезд библиотеки → Ф2.10 политики здоровья → Ф2.7 перекодирование →
Ф2.9 копия вне площадки.**

Планировщик ёмкости стоит вторым не по значимости, а по зависимости: без него
любой слив или миграция в почти заполненной библиотеке превращается в «непонятно,
когда закончится и хватит ли места». Он обязан ответить до начала работ: сколько
живых данных надо перенести, сколько места в целевом размещении, сколько лент
добавить, прогноз окончания по фактической скорости, и **отказ начинать слив без
места с готовым планом действий**, а не честное «попробуем».

**Закрыто, когда:** С4 пройдёт на стенде от `drain --magazine 4 --dry-run` до
уведомления «магазин можно вынимать», при этом restore клиентов работает на всех
шагах, ни одна копия не удалена до проверки новой, `tapegw admin verify` чист, а
запись на LTO-7 за всё время — ноль.

### M4. Архивация

**Порядок работ, и он начинается не с `Store`.** По ADR-0007 вызовы Phobos живут
в отдельном бинаре `tapegw-mover`, а не в основном процессе. Если сначала
встроить `Store` с cgo в tapegw, а «вынести в отдельный процесс» отложить, то
протокол между процессами придётся писать поверх уже работающего кода — и он
будет повторять вызовы Go-функций один в один, то есть окажется не протоколом,
а временной затычкой. Поэтому: 1) ADR (атрибуты, пачки, журнал, протокол mover);
2) `Store`, `store/fake` и протокол tapegw ↔ mover; 3) migrator с батчами,
`SKIP LOCKED` и разбором ENOSPC по объектам (см. «Что нужно учесть»); 4) индекс пачки и атрибуты
крупных объектов; 5) журнал метаданных; 6) janitor. Со вторым пунктом заодно
закрывается правило 2: основной бинарь собирается без cgo.

- [ ] **Два сервера: S3 и библиотека** ([предложение 0012](proposals/0012-split-s3-and-library.md),
  решение владельца). Mover живёт на сервере библиотеки, tapegw — на сервере S3,
  поэтому migrator и recaller — **клиенты протокола**, а не вызывающие локальную
  библиотеку. Следствия, которые нельзя отложить на «разнесём потом»:
  - данные идут **в протоколе**, а не путями к файлам: ни одна команда не содержит
    путь на стороне tapegw, потому что общего диска у серверов нет и «пути из
    раскладки спула» на сервере библиотеки не существуют;
  - пачку (tar pax, индекс, футер — ADR-0020) собирает **tapegw**: индекс требует
    `version_seq` и обёрнутых DEK из БД, а mover в БД не ходит и формата пачки не
    знает;
  - у mover локальный буфер (staging) на NVMe: батч сначала принят целиком и
    `fsync`-нут, и только потом `phobos_put` читает с локального диска. Иначе
    заминка сети останавливает драйв, лента начинает «чистить» и изнашивается;
  - буфер переживает перезапуск mover'а: незавершённые операции продолжаются по
    UUID, а не начинаются заново.
  Граница «у mover нет ключей» из ADR-0007 и ADR-0014 становится физической:
  компрометация сервера библиотеки не раскрывает данные клиентов.

- [ ] **Копии, размещение и реестр носителей**
  ([предложение 0013](proposals/0013-copies-placement-media.md),
  [предложение 0014](proposals/0014-media-lifecycle-migrations-plan.md),
  решение владельца; форма копий — [ADR-0022](adr/0022-copies-as-separate-objects.md)).
  Четыре сценария — две стороны библиотеки, ленивая миграция поколений, переезд
  библиотеки, копия вне площадки — это **один** механизм: у пула есть политика
  размещения (список копий, каждая своим набором тегов), а фактическое размещение
  сверяется с ней. Ленивая миграция при этом — правка конфига: заменить
  `gen:lto7` на `gen:lto8`, и всё, что лежит на LTO-7, становится «не по политике».
  Делать четыре отдельных механизма задним числом дороже, чем заложить один сейчас.

  Что входит в M4:

  - **`archive_copies`** — место хранения каждой копии объекта Phobos (пачки или
    крупного объекта): `phobos_oid` (у копии **свой** объект Phobos, см. ADR-0022),
    `spec` (имя спецификации из политики), **`format` — версия формата именно этой
    копии** (Р3 из 0014: во время миграции поколения и формата живут копии в разных
    версиях, читатель выбирает декодер по версии копии), `medium`,
    состояние `pending → writing → done` плюс `lost`/`dead`, времена, `attrs jsonb`.
    Эта таблица **заменяет колонку «носитель»**: `objects.phobos_medium` и
    `packs.medium` из предложения 0011 в схему не входят. Пока данных нет, это
    бесплатно; после первой записи на ленту замена превратилась бы в миграцию всего
    архива. Тест держит это двумя проверками: колонок носителя вне `archive_copies`
    в схеме нет, и копия без `format` не вставляется;
  - **правило поддержки формата** (Р4 из 0014): код чтения версии формата удаляется
    только когда в установке не осталось ни одной живой копии в нём. Проверка —
    запрос к `archive_copies`, а не «по ощущениям, уже давно не встречается»;
  - **`media`** — реестр носителей: метка (уникальна **во всей установке**, а не
    внутри библиотеки: библиотека — где лента лежит сейчас, а не часть её имени),
    `kind` (`data`/`cleaning`/`diagnostic`), `generation`, `worm`, `pool_id`,
    `side`, `lifecycle`, `location`, `health`, `attrs jsonb`.
    **Геометрия из 0014**: `magazine` (магазин) и `home_slot` — без них невозможен
    слив по магазину (С4) и проверка «лента не меняет сторону»; `library_cache` —
    кэш того, что про ленту знает Phobos, чтобы не дёргать DSS на каждый запрос.
    Состояния `read_only` / `evacuating` / `retired`, локации `in_transit` /
    `offline` / `offsite`. Свободное место **не** дублируется: его читает Phobos;
  - **`drives`** и журналы событий `media_events`, `drive_events` — только на
    добавление. Из них считаются `health` и политики вывода лент;
  - **разделять поля по принципу**: то, от чего зависит логика (поколение, сторона,
    пул, WORM, состояние, местоположение, здоровье) — типизированные колонки;
    всё информационное и производителезависимое (атрибуты MAM, серийник, счётчик
    загрузок, объём за жизнь, модель, Type M) — в `attrs jsonb` и собирается при
    каждой загрузке ленты. Новые атрибуты и новые политики не должны требовать
    миграции;
  - **политика копий пула в конфиге** (`library_write`, `copies: [{name, tags}]`);
  - **уникальность метки проверяется при добавлении ленты**, а не при записи.

  **Правило «архивный = все копии записаны» меняет автомат объекта.** `archived`
  наступает, только когда записаны **все** копии текущей политики пула; до этого
  landing не очищается. Иначе между записью первой и второй копии на ленте лежит
  одна копия, а на диске уже ничего нет — единственный отказ второй ленты делает
  объект потерянным. Для migrator'а это значит: батч пишет N копий отдельными
  операциями протокола с разными тегами, и состояние переходит после последней.

  **Совместимость поколений — в конфиге** (`generations: {LTO-7: {read: [...],
  write: [...]}}`) и используется для выбора mover и для алерта: если живая копия
  лежит на поколении, которое не читает ни один запланированный драйв, это
  предупреждение. LTO-9 ленты LTO-7 не читает — миграцию с LTO-7 надо закончить до
  вывода последнего драйва, способного её прочитать.

- [ ] **Запрет записи на устаревшее поколение и формат — в три слоя**
  (Р5 из [предложение 0014](proposals/0014-media-lifecycle-migrations-plan.md)).
  Один слой — политика пула (теги поколения). Второй — `writable: false` у поколения
  в конфиге tapegw: ленты получают `read_only`, политика со ссылкой на запрещённое
  поколение **отклоняется при старте**, а не в момент записи, и migrator с
  reconciler'ом не выбирают такую ленту целью. Третий — у лент запрещённого
  поколения в Phobos снимается разрешение на запись, чтение и удаление остаются:
  это `phobos tape set-access -- -P` (проверено на стенде: флаг есть, семантика
  ровно та). Третий слой нужен затем, чтобы **ручной `phobos put`** мимо tapegw не
  писал на устаревшее: без него запрет держится только на нашей вежливости.
  То же для нашего формата: одна текущая версия для записи, старые — только чтение.
  `tapegw admin verify` сверяет три слоя между собой: расхождение — авария, а не
  предупреждение.

- [x] **Добавление ленты одной операцией tapegw** (Ф1.5 из 0014): команда
  `tapegw admin media add` регистрирует носитель в Phobos и в `media` одним
  действием: сверяется со списком (уникальность метки держит схема —
  `media.label UNIQUE`), ставит теги пула/поколения/стороны, разблокирует,
  форматирует и проверяет подтверждение Phobos перед записью в реестр
  («закрыто при сомнении»), а висящие на этом носителе копии `'writing'`
  доводит до `done`. Форматирование — только по явному `fs.status=blank`
  (пустой или незнакомый статус — отказ, форматировать при неизвестном статусе
  значит стереть заполненную ленту); заполненная лента регистрируется как
  `'full'`, а не отказом (иначе её копии навсегда останутся в `'writing'`);
  пул ленте с данными без тега пула присваивается только с `-adopt`. Повтор
  команды идемпотентен и доводит прерванную регистрацию. Ручные четыре команды Phobos остаются в
  [`docs/ops/tape-library.md`](ops/tape-library.md) для особых случаев;
  число копий, застрявших в `'writing'` из-за неизвестного носителя, теперь
  показывает `stand-status` (проверка закрыта при сбое), а не только
  ERROR-строка в логе.

- [x] **ADR формата записи на ленту — принят до первой строки кода mover.** Разделён
  на два сознательно: формат после первой записи меняется только перезаписью
  ленты, а протокол между процессами — вместе с обоими бинарями и без миграции
  данных.
  - **[ADR-0020](adr/0020-tape-record-format.md)** — принято: атрибуты крупного
    объекта (семь, включая `tapegw.seq` и `tapegw.copy` из ADR-0022), пачка как
    tar/pax с бинарным индексом
    и SHA-256 индекса в футере, сегмент журнала, снимок; у каждого своя версия
    формата; посчитан худший случай `user_md` (≈315 байт из 4079).
  - **[ADR-0021](adr/0021-mover-protocol.md)** — принято по требованиям
    [предложения 0012](proposals/0012-split-s3-and-library.md): транспорт — TCP с
    **взаимным TLS** и кадры (JSON + куски данных), mover слушает, tapegw
    подключается; **данные идут в протоколе**, а не путями — общего диска у
    серверов нет; **идемпотентность по UUID операции** с докачкой по смещению и
    реестром операций на диске mover'а; **кредитный контроль** вместо ошибки
    переполнения буфера; **SHA-256 на приёме до** `phobos_put` (и на restore —
    до подъёма файла в cache); **нет команды отмены** — прервать C-вызов нельзя,
    честная семантика «отказаться и перепроверить через `stat`», поэтому итог
    после смерти mover'а считается **неизвестным**, а не неудачным; таймаут
    бездействия вместо таймаута на операцию; атрибуты проверяются на бюджет
    4079 байт **до** обращения к ленте, чтобы `attr_too_large` был отличим от
    двусмысленного `ENOSPC`.
  То, что зафиксировано в ADR-0020, обратно не переписать: на ленте не
  переиздаётся ни формат манифеста, ни набор атрибутов, ни layout пачки.
- [ ] **Поправка к ADR-0021: локальная передача между mover одного сервера**
  (Ф0.4 из [предложение 0014](proposals/0014-media-lifecycle-migrations-plan.md),
  Р7). Буфер принадлежит **серверу библиотеки**, а не процессу mover: операция
  записи может сослаться на операцию чтения другого mover по UUID — «записать то,
  что принято операцией X». Пути в протоколе по-прежнему не передаются.
  Без этого контейнерный перенос (чтение на одном драйве, запись на другой) гонял
  бы данные через сервер S3 туда и обратно, хотя оба драйва стоят рядом. Правится
  сейчас, а не при переносе: это форма протокола, а не оптимизация.
- [ ] Интерфейс `Store`, `store/fake`, `store/phobos` (cgo) — в `tapegw-mover`.
- [ ] Migrator: батчи, `SKIP LOCKED`, ретраи, `failed`, восстановление `archiving` при старте.
- [ ] **Возвращаемый код Phobos ни о чём не говорит без сверки состояния.** После
  `phobos tape format` проверять `fs_status` и `phys_spc_free`, а не rc: в
  [M0-07](spikes/M0-07-mhvtl-ltfs.md) форматирование вернуло успех при ёмкости
  0 GB. То же для итога батча — см. `xt_rc` в
  [M0-02](spikes/M0-02-libphobos-api.md).
- [ ] Запись батча идёт с `no_split`, чтобы объект целиком оставался на одном
  носителе (ADR-0008); `s3.max_object_size` отсекает слишком крупные объекты на
  PUT.
- [ ] **Сложная логика «подобрать размер батча под остаток ленты» не нужна** —
  Phobos сам выбирает носитель, куда помещается весь батч целиком (замерено в
  [M0-07](spikes/M0-07-mhvtl-ltfs.md)).
- [ ] **Но простую страховку от ENOSPC оставляем — в форме, соответствующей
  записи по одному объекту.** Случай, который Phobos не решит: новые ленты
  закончились, на каждой остался хвост меньше объекта. Суммарно свободного места
  много, а каждый `put` получает ENOSPC, и архивация встаёт.

  **Деление батча пополам из первой редакции этого пункта упразднено**: при
  записи по одному объекту делить нечего, ENOSPC приходит по конкретному
  объекту. Вместо деления:

  1. объект возвращается в `pending` с экспоненциальным `next_try_at`;
  2. **перед** повтором воркер проверяет, что в пуле вообще есть лента со
     свободным местом. Без этой проверки опечатка в теге пула закрутила бы цикл
     ретраев до бесконечности, и выглядело бы это как «нет места», а не как
     «нет лент с таким тегом» — то есть как проблема, которой нет;
  3. если свободные ленты есть, а ENOSPC повторяется на объекте, который и есть
     один объект, — это алерт «объект не влезает ни на одну ленту пула», а не
     повод крутить цикл.

  Отказ по ENOSPC откатывается чисто — ни incomplete-копий, ни записей в
  каталоге (в отличие от аварийного завершения процесса), — так что повтор
  безопасен.
- [ ] **Цепочка ограничений конфига:**
  `max(s3.max_object_size, pack.target_size) <= свободное место на одной ленте`.
  При записи по одному объекту на ленту обязана влезать каждая единица записи
  отдельно: крупный объект — по `s3.max_object_size`, пачка — по
  `pack.target_size`. `migrator.batch_max_bytes` из этой цепочки **выпадает**:
  он ограничивает размер выборки в память и на staging, а не требование к
  носителю, и путать их нельзя — иначе настройка «батч 200 GiB» тайно означает
  «каждая пачка обязана влезть на ленту целиком». Средний незаполненный хвост
  ленты — порядка половины `pack.target_size`, поэтому `pack.target_size`
  настраивается вместе с ёмкостью лент, а не независимо.
- [ ] Janitor: вытеснение landing по watermark.
- [ ] HEAD и LIST отдают storage class по состоянию.
- [ ] **Индекс пачки и атрибуты крупных объектов.** Порядок версий одного ключа в
  живой базе задаёт `version_seq`, а его Postgres назначает при вставке,
  **после** записи файла и под advisory-локом ключа (ADR-0005). В манифесте есть
  только время сборки — оно ставится во время записи тела, и на параллельных PUT
  одного ключа эти два порядка расходятся: A начался раньше, но тело у него
  большое; B начался позже, тело маленькое, лок он взял первым — значит у B
  `version_seq` меньше и текущая версия у него, а по времени «последней» выглядит
  A. Восстановление по времени вернуло бы текущей не ту версию, что была до
  аварии, и ни одной ошибки при этом не появилось бы.

  Куда это кладётся — зависит от того, идёт объект в пачку или нет (см. пункт про
  упаковку):

  - **крупный объект** (больше `pack.threshold`) — отдельный объект Phobos, и у
    него есть атрибуты: шесть полей, включая `tapegw.seq`;
  - **мелкий объект** — отдельного объекта Phobos больше нет, есть пачка с
    индексом внутри. `version_seq`, обёрнутый DEK, тенант и смещение объекта
    внутри пачки лежат в этом индексе, а атрибуты пачки описывают пачку целиком.
    Отдельных атрибутов на объект не появляется — иначе упаковка не решила бы
    задачу и индекс LTFS раздулся бы ровно так же.

  В обоих случаях `rebuild-meta` упорядочивает версии по `version_seq`, а
  `Created` из манифеста остаётся только для отображения. Номер к ключам
  отношения не имеет и на правило ADR-0014 «что попадает на ленту» не влияет:
  индекс пачки шифруется тем же путём, что и её содержимое.
  **Тест:** две параллельные записи одного ключа → после восстановления текущая та
  же, что была до аварии — и отдельно для пары мелких объектов, попавших в пачку.
- [ ] **Журнал метаданных на ленте.** Манифест описывает версию в момент её
  записи и больше не меняется, а метаданные после записи правятся. Без журнала
  восстановление вернёт состояние, которого не было:
  - `PutObjectRetention` продлевает срок, legal hold ставят и снимают. Продленный
    COMPLIANCE с 30 дней до 7 лет после восстановления снова стал бы удаляемым
    через 30 дней — для Object Lock это не «неточность», а снятие неизменяемости.
  - Маркеры удаления и удаления версий по `versionId` на ленту не пишутся вовсе:
    `state='marker'` не попадает в очередь миграции. Значит ключ, у которого
    сверху лежал маркер, после восстановления выглядит живым.
  - Версия, удалённая по `versionId`, живёт в Phobos, пока deleter не выполнил
    `phobos_delete`. Авария в это окно возвращает удалённое.
  - Конфигурация бакета — версионирование, Object Lock, срок по умолчанию,
    владелец, ACL, политика — на ленте не живёт вообще, и `BucketsCreated` в
    отчёте `rebuild-meta` честно перечисляет бакеты, где её надо вернуть руками.

  Формат — **сегменты, а не события**. Объект Phobos на каждое событие выглядит
  логично ровно до первого боевого трафика: бэкап-софт и клиенты с Object Lock
  продлевают retention постоянно, часто ежедневно и на каждую версию, и это
  миллионы крошечных файлов в индексе LTFS — худший случай для формата, где
  каждый объект отдельный файл. Риск «мелких объектов» уже записан в рисках, и
  создавать его себе руками незачем.

  - Сегмент журнала тенанта закрывается по **числу событий или объёму** — и только.
    Своего таймера у сегмента нет: окно «событие → лента» целиком задаётся
    закрытием пачки журнала (см. ниже), и второй таймер поверх первого только
    сделал бы окно непонятной формы.
  - У каждого события есть **порядковый номер внутри тенанта** (`journal_seq`).
    По нему `rebuild-meta` применяет события строго по порядку и, главное, видит
    пропуск: дырка в нумерации — это алерт, а не тихая потеря.
  - Периодически кладётся **снимок состояния блокировок и конфигурации бакетов**,
    чтобы восстановление не перечитывало журнал за годы, а начинало с последнего
    снимка и применяло только то, что после него.
  - Версии в журнале упоминаются **только по UUID**, без имён в открытом виде.
    Сегмент шифруется TEK тенанта (не DEK объекта: событие относится к тенанту и
    должно читаться без ключа конкретной версии) и после уничтожения ключа
    становится нечитаемым вместе с данными — правило ADR-0014 здесь соблюдается
    само собой.

  **Пачки журнала — отдельный поток, и это решение владельца.** Сегменты журнала
  сами по себе мелкие объекты, то есть по общему правилу поехали бы в обычные
  пачки данных. Тогда окно «событие → лента» сложилось бы из двух ожиданий: окно
  закрытия сегмента плюс до `pack.max_wait` (8 часов) в пачке данных. Для
  продления COMPLIANCE это значит: авария через 7 часов после продления — и после
  восстановления срок окажется старым. Блокировки — то, ради чего клиенты вообще
  берут Object Lock, и обещание «продление защищено на ленте не позднее чем через
  час» стоит лишних пачек журнала.

  - Пачки журнала собираются **отдельно от пачек данных**, в пределах пула.
  - `journal.pack.max_wait = 1h` по умолчанию, переопределяется на уровне пула.
    Отсчёт — от **самого старого события** в пачке.
  - Закрытие пачки журнала закрывает **все открытые сегменты**, которые в неё
    попадают. Так полное окно «событие → лента» не превышает
    `journal.pack.max_wait`, и именно это число идёт в договор.
  - Остальные условия закрытия — как у пачек данных: «попутно» с записью в тот же
    пул, высокий watermark landing, drain или остановка.

  Mover архивирует сегменты тем же путём, что и данные, `rebuild-meta` применяет
  события по порядку поверх восстановленных версий.

  **Тесты:** продление retention при отсутствии другой нагрузки попадает на ленту
  не позднее `journal.pack.max_wait`; пачка журнала не ждёт заполнения пачки
  данных; закрытие пачки журнала закрывает открытый сегмент, и события из него
  оказываются в этой пачке.

  **Правило разрешения сомнений: блокировка берётся строже** — максимум из
  известных сроков, а не последний прочитанный. Недооценить срок — это
  «неизменяемые бэкапы удалились», переоценить — «полежали дольше», и выбирать
  надо из этих двух.
  **Тесты:** продлённый COMPLIANCE → после восстановления срок продленный;
  версия, удалённая до аварии, после восстановления не появляется.
- [ ] **Упаковка мелких объектов в пачки — решение владельца, и оно в M4, а не
  «после MVP».** Каждый объект — отдельный файл в индексе LTFS, и на клиентском
  трафике бэкап-софта это раздувает индекс и делает запись медленной (риск
  «мелкие объекты» в таблице рисков). Формат пачек — часть ADR формата записи,
  потому что задним числом пачку в уже записанное не превратить.

  Параметры — в конфиг (правило 9), с переопределением переменными
  `TAPEGW_PACK_*`:

  - `pack.threshold = 25MiB` — объекты меньше порога идут в пачки, остальные
    пишутся отдельными объектами Phobos;
  - `pack.target_size = 2–4 GiB` — точное значение ставится по спайку времени
    `phobos_get`: пачка — это единица чтения при восстановлении, и слишком
    большая пачка означает, что один затребованный килобайт стоит перемотки к
    нескольким гигабайтам;
  - `pack.max_wait = 8h` по умолчанию, переопределяется на уровне пула. Отсчёт
    идёт **от записи самого старого объекта в пачке**, а не от момента создания
    пачки: иначе пачка, в которую всё время что-то приходит, не закрывалась бы
    никогда, а обещанный RPO считается именно по самому старому незаархивированному
    объекту.

  Пачка закрывается по **первому** из условий: набран `target_size`; истёк
  `max_wait`; в этот пул уже идёт запись (закрытие «попутно»); landing у high
  watermark; drain или остановка процесса.

  **Пачки собираются в пределах пула, а не тенанта.** При тысячах тенантов (решение
  T0) пачка на тенанта почти никогда не набирала бы `target_size` и закрывалась бы
  по `max_wait` полупустой — то есть ровно те мелкие объекты, ради которых всё и
  затевалось. Плюс носитель Phobos выбирает по тегу пула (ADR-0013, спайк
  M0-08), так что пачка и так естественно единица пула.

  Важно, чего упаковка НЕ меняет: крипто-уничтожение тенанта (ADR-0014) остаётся
  крипто-уничтожением. Байты удалённого тенанта лежат на ленте до repack одинаково
  что в пачке, что вне её — тенанты и в пуле делят ленту вместе. Поэтому пачка не
  требует пересматривать ADR-0014 и не делает repack частью удаления: имена
  объектов в зашифрованных манифестах, а без TEK тенанта его байты — мусор.
- [ ] **Учёт живого и мёртвого в пачках — тоже M4, а не «после MVP»**
  ([предложение 0011](proposals/0011-repack-strategy.md)). Пачка — один объект для
  Phobos, поэтому `phobos tape repack` копирует её целиком, пока жив хоть один
  участник: уплотнение пачек — наша операция, и опирается она на модель данных,
  которую потом не поправить.

  - таблица `packs`: `id uuid` (= oid в Phobos), `pool_id`, `kind` (`data` /
    `journal`), `total_bytes` / `live_bytes`, `members_total` / `members_live`,
    `medium text[]` (ADR-0008, массив под `repl_count = 2`), состояние
    `open → sealed → archived → compacting → retired`, времена;
  - ссылка объекта на место хранения **изменяемая**: `objects.pack_id` +
    `pack_offset` + `pack_length` для участника пачки; носитель — в
    `archive_copies` по oid объекта Phobos (предложение 0013), а не колонкой
    `objects.phobos_medium`; уплотнение переносит ссылку в новую пачку одной
    транзакцией вместе со сменой состояния старой;
  - удаление версии (переход в `deleting`, физическое удаление маркера, удаление
    по `versionId`) **в той же транзакции** уменьшает `live_bytes` и
    `members_live` её пачки;
  - инвариант `live_bytes` = сумма `pack_length` живых участников — тестом и
    `tapegw admin verify`;
  - **носитель обновляется после repack:** строки `archive_copies` сверяются с
    `phobos extent list -f json` (ADR-0008), иначе группировка restore по лентам
    начнёт ошибаться. До обновления устаревший носитель — не порча, а только
    неоптимальный порядок restore; это надо записать, чтобы не принимать его за
    аварию. Со сменой модели (предложение 0013) это ещё и момент, когда копия
    перестаёт быть «не по политике».

  **Тесты:** удаление участника уменьшает `live_bytes` и `members_live` в той же
  транзакции (мутация «не уменьшать» ловится); инвариант `live_bytes` держится
  после перезаписи, удаления версии, удаления маркера и удаления тенанта;
  `rebuild-meta` с двумя пачками одних и тех же участников даёт одну строку на
  версию и пишет число схлопнутых дублей в отчёт.

  **Тесты:** пачка закрывается по таймеру, если объём не набран; пачка уходит
  попутно с записью крупного объекта в тот же пул; срок считается от самого
  старого объекта, а не от создания пачки; при высоком watermark пачка
  закрывается досрочно.
- [ ] **M4 стартует только с окончательным форматом данных на ленте** — то есть
  после T2 (пулы и решение по шифрованию). Если шифрование принято, оно включается
  в спуле, и добавить его потом можно только перезаписью уже заархивированного.

* **Готово когда**: на ci-dir объект проходит `pending → archived`, вытесняется из landing, HEAD показывает `GLACIER`, GET возвращает `InvalidObjectState`; тест убийства процесса во время `phobos_put` проходит без потерь и дублей.

### M5. Restore

- [ ] RestoreObject по контракту из «Потоков», таблица `restores`.
- [ ] Recaller: приоритет по tier, батчи, сортировка по ленте (если M0 нашёл API), backpressure по кэшу.
- [ ] GET из cache, `x-amz-restore`, продление и истечение.
- [ ] Справедливость по тенантам (T3): round-robin recaller между тенантами с
  непустой очередью, `restore_inflight` на тенанта — иначе один бэклог держит всех.
- [ ] **Выбор копии при restore**
  ([предложение 0013](proposals/0013-copies-placement-media.md)): читается копия,
  чья лента уже смонтирована, иначе — с той стороны, где есть свободный драйв. При
  ошибке чтения берётся другая копия, а сбойная получает `lost` и уходит в
  reconciler на пересоздание.
- [ ] **Состояние «носитель недоступен».** `location` в `in_transit`, `offline`,
  `offsite`, `lost` либо `lifecycle = evacuating` — штатные случаи, а не авария.
  Если у объекта есть доступная копия — читается она; если нет — запрос ждёт со
  статусом и алертом оператору, **без бесконечных повторов**. Это же состояние нужно
  копии вне площадки (Б4), поэтому закладывается сейчас, а не когда площадка
  появится.

* **Готово когда**: полный Glacier-цикл на ci-dir (put → archive → evict → restore → poll HEAD → GET → expire → снова `InvalidObjectState`), а затем на vtl со штатным `phobos_tlc`.

### M6. Удаление и сборка мусора

- [ ] Deleter → `phobos_delete`, идемпотентность.
- [ ] Orphan GC для landing, cache, mpu.
- [ ] Схема учитывает удаление тенанта целиком (T4): объекты тенанта уходят в
  `deleting` пачками той же фоновой задачей, что и одиночные удаления.
- [ ] **`deleter` проверяет Object Lock через общую функцию ADR-0015**: версия под
  блокировкой не удаляется, даже если попала в `deleting`. Прямой `phobos delete`
  вне контроля tapegw — фиксируется в документации как запрещённая операция.

* **Готово когда**: после удаления и перезаписи ключей не остаётся файлов и живых объектов Phobos без строки в `tapegw`; тест сверки «БД ↔ диск ↔ Phobos».

### M7. klipper-tlc на mhVTL (параллельно M2–M6, после M0)

- [ ] Кадрирование и протокол из `.proto`, диспетчер запросов.
- [ ] Inventory на SQLite, бэкенды robot `sim` и `smc`, пакет `drive` (TUR, unload, MAM).
- [ ] **Сторона слота из геометрии и проверка «лента не меняет сторону»**
  ([предложение 0013](proposals/0013-copies-placement-media.md)): домашний слот
  ленты всегда на стороне её тега, и `klipper-tlc` проверяет при **каждой**
  выгрузке, что слот возврата на той же стороне. Иначе политика «левая и правая
  копия» распаляется молча: ленты постепенно съезжают на одну сторону, и отказ
  робота заберёт обе.
- [ ] **Сбор атрибутов MAM при каждой загрузке** в `media_events`: серийник,
  производитель, счётчик загрузок, объём записано и прочитано за жизнь. Собирается
  с самого начала, потому что политики вывода («лента после N загрузок», «рост
  ошибок чтения») пишутся поверх уже накопленной истории — собрать её задним числом
  невозможно.

- [ ] CLI оператора.
- [ ] **Пути устройств — только стабильные.** Номера `/dev/stN` и `/dev/sgN`
  меняются при каждом пересканировании шины (замерено в
  [M0-07](spikes/M0-07-mhvtl-ltfs.md)): после перезапуска один и тот же путь
  начинает указывать на другой привод, а снять такую запись из Phobos уже нельзя
  — `drive delete` отвечает `No such device`. И драйвы, и changer регистрируются
  по `/dev/tape/by-id/` (именован по серийнику: `scsi-<serial>`,
  `scsi-S<vendor>_<model>_<serial>-changer`). Соответствие DTE и устройства
  тоже строится по серийнику, а не по порядку появления узлов.

* **Готово когда**: golden-тест `status.lib_data` совпадает с `phobos_tlc` на mhVTL; стоковый Phobos через `klipper-tlc` (бэкенд `smc`) проходит e2e-сценарий M5 на стенде vtl.

### M8. Манипулятор

- [ ] Бэкенд `moonraker`, таймауты, маппинг ошибок механики, режим «требует оператора».
- [ ] Макросы Klipper по контракту в `deploy/klipper/`.
- [ ] Сверка метки через MAM при загрузке.
- [ ] **При старте сверить содержимое драйвов с inventory.** Носитель, физически
  лежащий в драйве, обязан быть зарегистрирован в DSS до старта `phobosd`, иначе
  поток драйва умирает с ENXIO (см. [M0-07](spikes/M0-07-mhvtl-ltfs.md)). После
  пропадания питания или ручных вмешательств в драйве может оказаться картридж,
  о котором DSS не знает. `klipper-tlc` обязан либо выгрузить неизвестные
  картриджи в свободный слот, либо не стартовать с внятной ошибкой — молча
  мёртвый драйв неотличим от исправного.

* **Готово когда**: на hw серия циклов load/unload по всем слотам без ошибок; e2e-сценарий M5 проходит на реальном драйве.

### M9. Эксплуатация

- [ ] **Две топологии в документации: один хост и два сервера**
  ([предложение 0012](proposals/0012-split-s3-and-library.md)). «Всё на одном
  хосте» остаётся допустимым, но как частный случай: mover подключается по
  loopback, а не по локальным путям — иначе второй топологии в коде не будет
  никогда.
- [ ] **Пропускная способность сети между серверами** не меньше суммарной скорости
  драйвов: четыре драйва по ~400 МБ/с требуют 25-гигабитного канала. Проверка
  входит в приёмку стенда, а не остаётся «на глаз».
- [ ] **Буфер на сервере библиотеки** — не меньше двух целевых батчей или пачек на
  каждый драйв; размер задаётся явно и сверяется с `migrator.batch_max_bytes` и
  `pack.target_size` при валидации конфига.
- [ ] **Выдача и ротация сертификатов mTLS** между серверами — в инструкции
  деплоя и в DR-процедуре: просроченный сертификат останавливает архивацию,
  и это должно быть предсказуемой операцией, а не аварией.
- [ ] Метрики, health, админ-API и CLI, алерты.
- [ ] systemd-юниты, инструкция развёртывания и DR в `docs/`.
- [ ] **Порядок запуска: `phobosd` после `phobos_tlc`.** Если `phobosd` стартует
  раньше, чем TLC начал слушать свой порт, он отказывается загружать устройства и
  **не повторяет попытку** — помогает только перезапуск (замерено в
  [M0-07](spikes/M0-07-mhvtl-ltfs.md)). В systemd это `After=` + `Requires=`.
  Второе условие: носитель, физически лежащий в драйве, должен быть зарегистрирован
  в DSS до старта `phobosd`, иначе поток этого драйва умирает с ENXIO.
- [ ] **Точки монтирования LTFS защищаются `chattr +i`.** Пустой каталог точки
  делается неизменяемым: запись в не смонтированную точку тогда падает с ошибкой,
  а не уходит молча в корневую ФС. Это дешевле, чем проверять факт монтирования
  в каждом скрипте (на стенде на этом получили ложное расхождение контрольных
  сумм).
- [ ] `rebuild-meta` из атрибутов Phobos, экспорта ключей тенантов и KEK — все
  три входа обязательны (ADR-0014, ADR-0019).
- [ ] **Процесс не оставляет ключи на диске.** `LimitCORE=0` во всех юнитах
  (`tapegw`, `tapegw-mover`): core dump при падении — это TEK и DEK в файле вне
  всякого учёта. Swap на хосте шифруется или отключён: страница с ключом может
  уйти туда так же молча. Проверка входит в DR-инструкцию, а не остаётся
  «очевидной» (ADR-0014).
- [ ] **Срок хранения бэкапов БД не больше срока экспорта ключей.** Реальная
  окончательность крипто-уничтожения — `max(keys.export_retention, срок
  PITR-бэкапов)`: в каждом дампе и WAL-сегменте лежит `tenants.wrapped_key`.
  Согласовать одной настройкой или проверкой при старте, и в договор вносить
  именно максимум (ADR-0014).
- [ ] **Логи не хранят имена объектов дольше N.** Либо срок хранения логов ≤ того
  же N, либо в логах нет бакета и ключа объекта — только UUID версии. Имена с
  лент мы убрали именно ради удаления, и логи не должны возвращать их обратно
  (ADR-0014).
- [ ] Нагрузочный тест: landing держит поток записи, драйв пишет без «чистки» (shoe-shining) на батчах из конфига.

* **Готово когда**: восстановление `tapegw` из пустой схемы через `rebuild-meta` на стенде совпадает с исходной; есть runbook на типовые аварии.

**После MVP** — это фаза 2 из
[предложение 0014](proposals/0014-media-lifecycle-migrations-plan.md), и порядок
внутри неё задан зависимостями, а не удобством:
**Ф2.1 ядро reconciler → Ф2.4 планировщик ёмкости → Ф2.3 слив по месту + Ф2.5
выгрузка → Ф2.2 ленивая миграция поколения → Ф2.6 контейнерная миграция → Ф2.8
переезд библиотеки → Ф2.10 политики здоровья → Ф2.7 перекодирование → Ф2.9 копия
вне площадки.** Каждое — отдельное ADR.

Ключевое про порядок: **планировщик ёмкости идёт вторым, сразу за ядром
reconciler.** Без него любой слив или миграция в почти заполненной библиотеке —
это «непонятно, когда закончится и хватит ли места». Он обязан ответить до
начала: сколько живых данных переносить, сколько места в цели, сколько лент
добавить, прогноз окончания по фактической скорости, и **отказать начинать слив
без места, выдав готовый план действий**, а не «попробуем».

Модель данных под всё это готова в M4 (`archive_copies`, `media` с геометрией,
политика копий, версия формата у копии) — после MVP остаётся планирование, а не
переделка схемы. Приёмка фазы 2 — сценарий С4 (слив магазина 4 на MSL4048).

Туда же: **планировщик** repack и уплотнения (модель данных и требования к
формату переехали в M4 — [предложение 0011](proposals/0011-repack-strategy.md)),
WORM-пулы для compliance и несколько фронтенд-узлов.
Пулы лент и «две копии на разных лентах» отсюда **убраны**: пулы
переехали в T2 (до M4), вторая копия — в Б4 (до M5). **Упаковка мелких объектов
тоже убрана** — решением владельца она вошла в M4, потому что формат пачки
нельзя добавить задним числом: он меняется только перезаписью уже заархивированного.

### T0–T5 и V. Мультитенантность, версионирование и Object Lock

Полное обоснование — [предложение 0009](proposals/0009-multitenancy-and-gaps.md); после
решения владельца разделы 1–6 разносятся в ADR-0009…0014. Здесь — только порядок
работ и критерии готовности.

Тенант — новая сущность tapegw, её нет ни в versitygw, ни в Phobos. IAM переезжает в
Postgres: `embedgw.Config.IAMService` принимает нашу реализацию `auth.IAMService`
(проверено: ровно 7 методов, поле есть, оно обходит все прочие IAM-бэкенды), а
`tenant_id` едет в `Account.ProjectID` — бэкенд узнаёт тенанта из контекста запроса
без лишнего обращения к БД.

**Ограничение, которое надо учесть в миграции:** `Account.ProjectID` в versitygw
объявлен как `int` (`auth/iam.go:74`), поэтому `tenants.id` — целочисленный
(`bigint`), а не UUID, в отличие от `buckets.id`.

Ещё одно подтверждённое свойство versitygw: для `IAMService` **встроенный кэш IAM не
применяется** (`embedgw.go`: «apply to all backends except … IAMService»), а
`GetUserAccount` вызывается на каждом запросе — значит кэш с TTL нужен наш.

#### T0. Решения и спайк — сейчас, до продолжения M2

- [x] **Четыре вопроса владельцу закрыты** решением от 2026-11-03
  ([предложение 0010](proposals/0010-owner-decisions.md)): шифрование — да, ключ на
  тенанта; клиентов тысячи и больше; в первый год Object Lock и совместимость с
  бэкап-софтом; вторая копия вне площадки и выделенные пулы — не в первый год.
- [ ] ADR-0009…0016. Из них 0014 (шифрование), 0015 (версии и Object Lock) и 0016
  (тысячи тенантов) — по предложению 0010.
- [x] **Спайк M0-08 закрыт:** тег — жёсткий фильтр, но изоляция односторонняя
  ([M0-08](spikes/M0-08-phobos-tags.md)); в ADR-0013 добавлено обязательное
  тегирование каждой записи.
- [ ] **Набор тестов совместимости с AWS** (бывший M0-09, переформулирован): эталон —
  поведение AWS S3 и тесты versitygw, отдельного продукта-эталона нет. Перенесён в
  этап V, потому что без версий и блокировки проверять нечего.
- [ ] Спайк M0-08: теги Phobos в `phobos_put` через API. Поле `put.tags`
  (`struct string_array`, «Tags to select a media to write») существует — проверить
  главное: является ли тег **жёстким фильтром** (лента без тега пула не берётся
  никогда) или предпочтением. Если предпочтением — физического разделения тенантов
  теги не дадут, и план T2 пересматривается.
- [ ] Миграция: `tenants`, `accounts`, `pools`, `tenant_usage`, `object_parts`,
  `buckets.tenant_id`, `objects.tenant_id`, `objects.pool_id`. Таблицы пока пустые,
  поэтому колонки сразу `NOT NULL` — потом это отдельная миграция с бэкфиллом.
- [ ] **Там же — колонки версионирования, блокировки и шифрования**, чтобы не гнать
  их отдельными миграциями: `objects.is_delete_marker`, `retention_mode`,
  `retain_until`, `legal_hold`, `wrapped_dek`, `tenants.wrapped_key`,
  `buckets.versioning`, `buckets.object_lock_enabled`, `buckets.default_retention`.
  Дёшево только сейчас, пока в таблицах нет строк.

* **Готово когда:** приняты ADR-0009…0016, закрыт M0-08, миграция слита.

#### T1. Тенанты и изоляция — вместе с M2, до его закрытия

- [ ] `IAMService` поверх Postgres, мастер-ключ для секретов, свой кэш; `iam_dir` из
  конфига уходит.
- [x] Bucket policy: Get/Put/Delete. **Это блокер, а не «потом»:**
  `verifyResourceAccess` вызывает `GetBucketPolicy` на каждом обращении и терпит
  ровно `ErrNoSuchBucketPolicy` (`auth/access-control.go:421`), так что раньше
  обычный пользователь получал 501 на любую операцию с бакетом. Та же ловушка
  правила 4, что и с `GetObjectLockConfiguration`.
  Сделано: `internal/backend/policy.go`. Колонка `buckets.policy` переведена в
  `text` (миграция 0006) — jsonb пересобрал бы документ, а versitygw отдаёт
  байты дословно. Тот же `GetBucketPolicy` вызывается и из проверки Object Lock
  (`auth/object_lock.go:335,349`), поэтому код обязателен и для этапа V.
- [x] `ChangeBucketOwner`, `ListBucketsAndOwners` — без них роль `user`
  неработоспособна. Это admin API (`PATCH /change-bucket-owner`,
  `PATCH /list-buckets`); `FixedBucketOwner` реализует только standalone-IAM,
  так что бэкенд обязан отвечать сам.
  **Смена владельца меняет права, а не только колонку `owner`:** versitygw
  определяет владельца по ACL из `GetBucketAcl`. Поэтому `ChangeBucketOwner`
  одним `UPDATE` ставит владельца, новый ACL (единственный грант
  `FULL_CONTROL` — новому владельцу, как в `auth.UpdateBucketACLOwner`) и
  обнуляет политику. Инвариант `buckets.owner == acl.Owner` под тестом.
- [ ] При смене владельца проверять, что **новый владелец — из того же тенанта**.
  Сейчас проверять не на чем: колонка `tenant_id` у бакета есть, но связи
  учётной записи с тенантом ещё нет. Без этой проверки `change-bucket-owner`
  позволит передать бакет чужому тенанту.
- [ ] Страж тенанта во всех методах бэкенда + **тест-перебор всех методов**
  `backend.Backend` на отказе чужому бакету (чтобы новый метод не забыли).
- [ ] Валидация ACL и политик против межтенантных грантов, включая
  `AuthenticatedUsers` и `*` — в S3 это «все аккаунты», то есть все тенанты сразу.
- [ ] Роль `admin` тенантам не выдаётся: versitygw пропускает её мимо проверок
  политик (`authorizationApplies`, `auth/access-control.go:266`). Только `userplus`
  и `user`.
- [ ] e2e: два тенанта по два ключа — свой доступ работает, чужой отказывает на
  каждой операции, листинг бакетов не показывает чужих.

* **Готово когда:** тест-перебор и e2e зелёные; обычный ключ (`userplus`) проходит
  весь сценарий M2.

#### V. Версионирование и Object Lock — вместе с T1, до закрытия M2

Принято потому, что Object Lock нужен в первый год, а в AWS блокировка работает только
при включённом версионировании. Этап дорогой, но он меняет смысл
перезаписи, удаления и листинга — то есть ровно тех операций M2, что ещё не написаны.
Делать их дважды дороже, чем сразу.

- [ ] Колонки: `objects.is_delete_marker`, `retention_mode`, `retain_until`,
      `legal_hold`; `buckets.versioning`, `object_lock_enabled`, `default_retention`.
      `versionId` — UUID строки `objects`, отдельная нумерация не нужна.
- [ ] `PutBucketVersioning` / `GetBucketVersioning` (вместо нынешнего «всегда пусто»),
      `ListObjectVersions`, GET/HEAD/DELETE по `versionId`. **Переход назад в
      `unversioned` запрещён**: только `unversioned → enabled`, `enabled → suspended`,
      `suspended → enabled`, как в AWS. Логика PutObject опирается на то, что в бакете
      без версий текущая версия всегда null-версия. См. ADR-0015.
- [ ] **ListObjects v2 пишется сразу вместе с ListObjectVersions**, а не сначала
      обычный, потом версионный.
- [ ] DeleteObject без `versionId` в версионированном бакете = delete marker; с
      `versionId` = истинное удаление с проверкой блокировки. Маркеры и блокировка —
      **одним этапом**: при включённых версиях удаление без `versionId` versitygw не
      проверяет (создаётся маркер), так что без маркеров Object Lock выглядит сломанным.
- [x] **Поправка к уже слитому PR #6:** PutObject переводит прежнюю версию в `deleting`
      безусловно. Для бакета без версий это верно, для бакета с `enabled` — уничтожение
      истории и обход блокировки. Переход стал условным от `buckets.versioning`.
- [x] **Следствие из уникального индекса null-версии (тот же PR):** при перезаписи в
      бакете `unversioned` или `suspended` старая null-версия, уходя в `deleting`,
      обязана **в той же транзакции** получить `is_null_version = false`. Иначе новая
      строка с `true` упрётся в `objects_null_version`. Тест — «три PUT одного ключа
      в бакет без версий» — проходит; без сброса флага он падает на
      `duplicate key ... objects_null_version`, что и проверялось мутацией.
- [ ] `ObjectLockEnabledForBucket` в CreateBucket: в AWS блокировку нельзя включить
      задним числом, только при создании бакета. Бэкенд её получает
      (`controllers/bucket-put.go:701`), поэтому решение нужно до первых боевых бакетов.
- [ ] `Get/PutObjectLockConfiguration`, `Get/PutObjectRetention`,
      `Get/PutObjectLegalHold`. Правила COMPLIANCE/GOVERNANCE при смене retention
      проверяет сам versitygw (`auth.IsObjectLockRetentionPutAllowed`), дублировать их
      в `meta` не нужно.
- [ ] **Ловушка этих трёх геттеров.** `auth` берёт состояние блокировки из наших
      `GetObjectLockConfiguration`/`GetObjectRetention`, и заглушка, отвечающая
      `NoSuchObjectLockConfiguration` на всё, **молча отключает неизменяемость**: запросы
      проходят, ошибок нет. Каждый код возврата из контракта `auth/object_lock.go:481–560`
      покрывается тестом, заглушек не остаётся.
- [ ] Продление retention — только метаданные, лента не нужна: клиенты продлевают
      неизменяемость регулярно, и это не должно вызывать restore.
- [ ] Квоты: некурентные версии и delete markers считаются в `storage_bytes` и
      `objects` тенанта.

* **Готово когда:** проходит применимая часть тестов versitygw на версионирование и
  Object Lock, и собран **набор тестов совместимости с AWS**: полный Glacier-цикл на
  aws-sdk-go-v2 и Object Lock в COMPLIANCE с продлением срока на архивном объекте без
  restore. Список исключений с причинами — `test/e2e/EXCLUDED.md`.

#### T2. Пулы и шифрование — до M4

Шифрование по решению владельца — три PR: (1) формат и спул, (2) multipart и
`object_parts`, (3) ключи, KEK, атрибуты Phobos и `rebuild-meta`. M4 стартует только
после T2.

- [ ] Пулы, привязка тенанта, `objects.pool_id` на PUT (дальше не меняется; смена
  пула = repack). Migrator группирует батчи по пулу и передаёт тег пула и
  `repl_count` пула.
- [ ] Шифрование в спуле: сегментированный AEAD (AES-256-GCM блоками 64 KiB, nonce из
  UUID версии, номера части multipart, **соли попытки загрузки** и номера сегмента),
  иерархия KEK → TEK → DEK (DEK свой у каждой версии), ETag и checksum по открытым
  данным, заголовок `x-amz-server-side-encryption: AES256`.
- [ ] `object_parts(version_id, part_number, plain_size, cipher_offset, salt)` — размеры
  и соли частей нужны **навсегда**, без них Range-чтение зашифрованного multipart не
  найдёт нужный сегмент, а nonce не соберётся.
- [ ] **Манифест в конце файла объекта** (ADR-0019): метаданные объекта (бакет,
  ключ, ETag, content-type, `x-amz-meta-*`, размер, блокировка) и таблица частей
  шифруются тем же DEK версии и дописываются в конец файла; поверх — открытый
  футер фиксированных 48 байт с солью попытки. Соль обязательна: повторный
  Complete после незакоммиченной транзакции — штатное поведение S3, а UUID версии
  и DEK назначаются ещё на Create. AAD манифеста — весь футер, иначе подмена
  байта формата уводит читатель в разбор по чужим правилам. В атрибуты Phobos
  таблица не влезает: лимит LTFS на `user_md` — 4096 байта, это 84 части против
  10 000 допустимых (спайк M2-01).
- [ ] В атрибуты объекта Phobos кладётся ровно пять полей: UUID версии, id
  тенанта, обёрнутый DEK (base64), версия KEK, версия формата. Имена бакетов и
  ключей — **нет**: атрибуты лежат в открытом виде в индексе LTFS и пережили бы
  крипто-уничтожение тенанта. Обёрнутая TEK — **тоже нет**: она вместе с KEK
  даёт доступ к данным удалённого тенанта, а на ленте атрибуты непереписываемы
  (ADR-0014). Набор атрибутов собирает одна чистая функция, тест
  сверяет точный список ключей. Двоичные значения — только в base64: Phobos
  молча заменяет не-UTF-8 байты на U+FFFD.
- [ ] **Экспорт ключей тенантов** — обёрнутые TEK (`tenants.wrapped_key` с
  версиями KEK), выгружаемые отдельно от лент туда же, где хранится копия KEK,
  и только на перезаписываемые носители. `rebuild-meta` требует три входа: ленты,
  KEK и последний экспорт. Срок хранения экспорта — `keys.export_retention`
  (по умолчанию 30 суток); он же попадает в договор как срок, после которого
  данные удалённого тенанта необратимо недоступны.
- [ ] Тесты: Range на границах сегментов и частей; порча байта шифротекста → ошибка,
  а не мусор; уничтожение TEK → GET, restore и `rebuild-meta` дают ошибку; смена KEK
  → всё читается.

* **Готово когда:** M4 стартует с окончательным форматом данных на ленте.

#### T3. Квоты и справедливость — вместе с M4 и M5

- [ ] `tenant_usage`, резерв атомарным `UPDATE … WHERE bytes + $n <= limit
  RETURNING` в той же транзакции, что вставка объекта (после advisory-лока
  ADR-0005); освобождение — в транзакции перевода в `deleting`; суточная сверка с
  метрикой расхождения.
- [ ] Коды есть в versitygw, свой не нужен: `ErrQuotaExceeded` (403,
  `QuotaExceeded`) и `ErrSlowDown`. Для `TooManyBuckets` кода нет — если лимит на
  число бакетов делается, код свой.
- [ ] `SlowDown` и rate limit по тенанту (middleware `s3api.WithMiddleware`).
- [ ] При переносе бакета между тенантами (`ChangeBucketOwner`) переносится и
  счётчик **числа бакетов** в `tenant_usage`. Объёмы переносить не нужно: бакет
  пуст по построению (проверка пустоты в `ChangeBucketOwner`).
- [ ] Migrator: батчи по пулу с долей тенанта; recaller: round-robin.

* **Готово когда:** тест «тенант A забивает landing и очередь restore» — тенант B
  пишет и получает restore без деградации сверх заданной доли.

#### T4. Эксплуатация тенантов — вместе с M6 и M9

- [ ] Жизненный цикл `active → suspended → deleting → deleted`; удаление тенанта =
  уничтожение TEK **последним шагом**, после удаления всех объектов; выделенные
  ленты — на переформатирование.
- [ ] **Object Lock важнее удаления тенанта:** пока у тенанта есть версии с
  неистёкшим `retain_until` или `legal_hold`, TEK не уничтожается, а удаление
  тенанта отклоняется. Проверка — через общую функцию ADR-0015, ту же, что у `deleter`.
- [ ] Учёт по дням (`tenant_usage_daily`), аудит только на добавление, админ-API на
  отдельном листенере и `tapegw admin tenant ...`. Регистрация клиента — одной
  операцией админ-API (тенант, ключи, квоты), CLI — обёртка над ним.
- [ ] Метрики — только агрегаты без метки тенанта (клиентов тысячи, метка на
  тенанта раздует кардинальность); учёт по клиенту — в `tenant_usage_daily`.

* **Готово когда:** удаление тенанта на стенде делает его объекты нечитаемыми на всех
  носителях, а тенант с активной блокировкой удалить нельзя.

#### T5. После MVP

Webhook о завершении restore (в versitygw события нет — restore завершается без
запроса, событие формирует recaller), просмотр своего учёта тенантом, выдача
выделенных лент клиенту.

#### Пробелы вне тенантов (часть Б предложения)

Не задачи «на сейчас», а решения, обязательные к указанному этапу, потому что с
чужими данными каждое становится обязательством перед клиентом.

| # | Пробел | Решить до |
| --- | --- | --- |
| Б1 | RPO/RTO: одна копия на landing, один Postgres | закрытие M2 (вписано в M2 выше) |
| Б2 | Периодическая сверка целостности лент (scrubber) | M9 |
| Б3 | TapeAlert, счётчики ошибок, чистящие картриджи | M8 |
| Б4 | Вторая копия вне площадки; при офлайне — «ждёт оператора», не ретраи | M5 |
| Б5 | ~~Object Lock / legal hold: колонки предусмотреть сразу~~ — **снято:** Object Lock и версионирование обязательны в первый год, это этап V | этап V (вместе с M2) |
| Б6 | Режим обслуживания: drain и read-only | M9 |
| Б7 | Inventory `klipper-tlc` в той же Postgres, чтобы бэкап был один | M7 |
| Б8 | Смена поколения LTO как частный случай repack | после MVP |

## Тестирование

Главный риск проекта — потерять или задвоить объект при сбое, поэтому тесты на падения важнее покрытия строк.

- **Юнит** (`go test ./...`, без cgo): state machine объекта и restore — таблица всех разрешённых и запрещённых переходов; листинг S3 на таблице ожидаемых ответов (префиксы, delimiter, пагинация, юникод); вычисление ETag; маппинг ошибок; протокол TLC (кадры, golden-ответы).
- **Падения**: у каждого воркера есть точки инъекции сбоя (`fake` Store и `sim` robot умеют падать на заданном шаге). Тест «убить после шага N, перезапустить, сверить» для всех шагов migrator, recaller, deleter и PutObject.
- **Сверка инвариантов** (`tapegw admin verify`, используется и в тестах, и в проде):
  - каждый `pending` имеет файл в landing;
  - каждый `archived` существует в Phobos с совпадающими size и etag-атрибутом;
  - каждый `restored` имеет файл в cache;
  - в Phobos нет живых объектов без строки `tapegw`, кроме `deleting`.
- **Интеграция ci-dir**: tapegw с тегом `phobos` против настоящего Phobos family `dir` в CI на каждый PR после M4.
- **S3-совместимость**: e2e на aws sdk для Go в `test/e2e`, плюс прогон `aws cli`, `rclone`, `s3cmd`. Из тестов versitygw (`tests/`) взять применимые группы; список исключений с причинами вести в `test/e2e/EXCLUDED.md`.
- **vtl** (вручную или ночной прогон): e2e Glacier-цикл со штатным `phobos_tlc`, затем с `klipper-tlc`/`smc`; сбой драйва во время записи; лента отсутствует в слоте.
- **hw**: циклы load/unload, e2e на реальной ленте, проверка, что LTFS-ленту можно смонтировать вручную и найти объект по атрибутам.

## Риски и открытые вопросы

| Риск | Чем грозит | Что делаем |
| --- | --- | --- |
| **Шифрование не принято, а тенанты на общих лентах** | Удаление тенанта физически ничего не стирает до repack, а repack общего пула — дорогая операция над чужими данными | Решить до T2/M4: либо крипто-уничтожение (ADR-0014), либо прямо записать в договор, что удаление выполняется только repack |
| **Формат данных на ленте меняется после M4 только перезаписью** | Пулы, шифрование и `tenant_id` нельзя добавить задним числом | T0/T2 встроены до M4; колонки `tenant_id`/`pool_id` добавляются, пока таблицы пустые |
| **Одна копия на landing до архивации** | Потеря диска = потеря принятых данных клиентов | Требование к деплою до закрытия M2: зеркало под landing, реплика Postgres + PITR, RPO/RTO записаны числами |
| libphobos не потокобезопасна или блокирует надолго | Restore ждёт архивацию, пропускная способность ниже драйва | Спайк M0; раздельные пулы для migrator и recaller; при необходимости вынести вызовы Phobos в отдельный процесс-воркер |
| Мелкие объекты | Каждый объект — файл на LTFS: медленно и раздувает индекс | Упаковка в пачки — в M4 (решение владельца), порог `pack.threshold` |
| Тайминги механики против таймаутов Phobos | LRS считает load проваленным, пока робот ещё едет | Найти таймауты LRS и TLC-клиента в конфиге Phobos; `klipper-tlc` отвечает только после TEST UNIT READY |
| Нет считывателя штрихкодов | Inventory расходится с реальностью | Сверка метки через MAM на каждой загрузке, `verify` в CLI |
| Изменения `backend.Backend` между версиями versitygw | Сборка ломается при обновлении | Фиксированная версия; обновление отдельным PR с прогоном e2e |
| Phobos только под EL | Сложная сборка и деплой | Контейнер сборки в M0, RPM как артефакт CI |
| Удаления не освобождают ленту | Ленты заполняются «мёртвыми» данными | Метрика доли мёртвых байт по ленте и по пулу; модель учёта — в M4, планировщик repack — после MVP ([предложение 0011](proposals/0011-repack-strategy.md)) |
| Landing медленнее драйва | Драйв пишет рывками и изнашивается | Нагрузочный тест M9; NVMe под landing; батчи достаточного объёма |
| **Живая копия на поколении, которое нечем прочитать** | Данные есть, драйв выведен — чтение невозможно, и узнаём об этом при restore | Совместимость поколений в конфиге; алерт «есть копия на поколении вне списка читаемых»; миграция LTO-7 обязана закончиться до вывода последнего подходящего драйва ([предложение 0013](proposals/0013-copies-placement-media.md)) |
| **Ленты съезжают на одну сторону** | Политика «левая и правая копия» распаляется молча, и отказ робота забирает обе | Сторона — тег ленты и свойство слота; `klipper-tlc` проверяет при каждой выгрузке, что слот возврата на стороне ленты |
| **Копии Phobos нельзя развести по тегам** | «Две копии на разных сторонах» окажется одной копией с `raid1` | Спайк M0-11 до кода migrator'а; если нельзя — копии ведёт tapegw отдельными объектами Phobos, и это ADR |

**Открытые вопросы к владельцу** (ответы влияют на конфиг и порядок работ, не на архитектуру)

- [ ] Модель и поколение драйвов, сколько их, интерфейс подключения (SAS, FC).
- [ ] Есть ли на манипуляторе считыватель штрихкодов.
- [ ] Ожидаемое распределение размеров объектов и общий объём в год.
- [x] Нужна ли вторая копия на другой ленте с первого дня — **модель заложена,
  включается конфигом**: политика копий пула описывает и одну, и две стороны,
  и копию вне площадки ([предложение 0013](proposals/0013-copies-placement-media.md)).
  Конкретный набор для боевого деплоя — решение эксплуатации, а не архитектуры.
- [ ] Какие клиенты будут писать (rclone, бэкап-софт, свой код) — от этого зависит набор S3-операций для e2e.
- [x] Нужна ли мультиарендность — **да, полноценная**; снято предложением 0009,
  вопросы по ней переехали ниже.

**Открытые вопросы по мультитенантности** — закрыты решением владельца от 2026-11-03,
[предложение 0010](proposals/0010-owner-decisions.md):

- [x] **Шифрование и крипто-уничтожение — да**, вариант C: программное шифрование,
  ключ на тенанта (ADR-0014). Регуляторных требований к криптографии нет, AES-256-GCM
  допустим.
- [x] **Выделенные пулы с первого дня — нет**: все тенанты в общих пулах, выделенный
  пул — исключение по запросу (ADR-0016).
- [x] **Число тенантов — тысячи и больше.** Отсюда: метрики без метки тенанта, кэш IAM
  с `LISTEN/NOTIFY`, round-robin по `DISTINCT ON (tenant_id)` (ADR-0016).
- [x] **Вторая копия вне площадки — не в первый год**, механизм `repl_count` в пулах
  остаётся. **Object Lock — нужен в первый год**, и он повлёк за собой версионирование
  (этап V).

Вопрос, который решение владельца **не** закрывает:

- [ ] Появится ли клиент, который читает служебные объекты **после** записи. Если да —
  понадобится класс «всегда онлайн», а это правка ADR-0003; схема под это заранее не
  закладывается, см. раздел «Открытый вопрос» в
  [ADR-0003](adr/0003-standard-storage-class.md).

## Процесс ревью

Агент работает в ветках и открывает PR на каждый пункт плана; ревью делает Claude по ссылке на PR или ветку. Если репозиторий публичный на GitHub, Claude клонирует его в песочницу, собирает и гоняет `go vet` и юнит-тесты без cgo. Если приватный — нужен доступ или выгрузка диффа.

**Что приложить к запросу на ревью**: ссылку на PR, номер пункта плана, вывод CI (особенно ci-dir), для M7–M8 — логи прогона на vtl или hw.

**Чек-лист ревьюера**

- [ ] Соответствие этому документу; отступления оформлены ADR.
- [ ] Порядок «файл + fsync → commit», транзакционность смены состояний.
- [ ] Идемпотентность воркеров и тест на падение для каждого нового шага.
- [ ] Коды ошибок и заголовки S3 совпадают с AWS и posix-бэкендом versitygw.
- [ ] cgo: освобождение памяти, `KeepAlive`, нет вызовов Phobos вне пула.
- [ ] Конкурентность: `SKIP LOCKED`, отмена `ctx`, нет гонок (`go test -race`).
- [ ] Метрики и логи для нового пути; конфиг вместо констант.
- [ ] Тесты покрывают и успех, и сбои; e2e обновлены.

**Уровни замечаний**: blocker (потеря или порча данных, нарушение контракта S3) — мерж запрещён; major — исправить в этом PR; minor — можно отдельным PR; nit — на усмотрение агента.
