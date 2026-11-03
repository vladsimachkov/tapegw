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

**Не цели MVP** (каждая — отдельное решение позже): версионирование бакетов, Object Lock, lifecycle-политики, S3 Select, несколько узлов, упаковка мелких объектов, освобождение места на лентах после удалений.

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
CREATE TYPE obj_state AS ENUM ('pending','archiving','archived','failed','deleting','deleted');
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
| restored | restored | backend | повторный RestoreObject продлевает `expires_at` |
| restored | нет записи | janitor | `expires_at < now()`, файл удалён |
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
3. Один вызов `phobos_put` на весь батч: по xfer на объект, `xt_objid=uuid`, `xt_fd` открытого landing-файла, `xt_size`, атрибуты из раздела «Модель данных», family `tape`, layout и tags из конфига.
4. Разбор итога батча. Полагаться на `xt_rc` нельзя (см. спайк M0-02): при ошибке он остаётся нулевым у всех целей. Поэтому после батча — и обязательно после любого сбоя — для каждого объекта проверяем `phobos_getmd` на наличие **и** `copy_status = complete`. Найден и полон → `archived`, `phobos_ver`, `archived_at`, плюс `phobos_medium` из одного запроса `phobos extent list` на весь батч (ADR-0008). Иначе → обратно в `pending`, `attempts+1`, экспоненциальный `next_try_at`; после `max_attempts` → `failed` и алерт. На повторе уже подтверждённые объекты исключаем, иначе батч снова упрётся в EEXIST.
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
| restored | restored | backend | повторный RestoreObject продлевает `expires_at` |
| restored | нет записи | janitor | `expires_at < now()`, файл удалён |
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
3. Один вызов `phobos_put` на весь батч: по xfer на объект, `xt_objid=uuid`, `xt_fd` открытого landing-файла, `xt_size`, атрибуты из раздела «Модель данных», family `tape`, layout и tags из конфига.
4. Разбор итога батча. Полагаться на `xt_rc` нельзя (см. спайк M0-02): при ошибке он остаётся нулевым у всех целей. Поэтому после батча — и обязательно после любого сбоя — для каждого объекта проверяем `phobos_getmd` на наличие **и** `copy_status = complete`. Найден и полон → `archived`, `phobos_ver`, `archived_at`, плюс `phobos_medium` из одного запроса `phobos extent list` на весь батч (ADR-0008). Иначе → обратно в `pending`, `attempts+1`, экспоненциальный `next_try_at`; после `max_attempts` → `failed` и алерт. На повторе уже подтверждённые объекты исключаем, иначе батч снова упрётся в EEXIST.
5. Landing-копию не удалять сразу: её вытесняет janitor по watermark, начиная с давно архивированных.

Восстановление после падения: строки в `archiving` при старте проверяются через `phobos_getmd(uuid)`. Объект найден → `archived`, не найден → `pending`. Незавершённые копии чистит `phobos_delete_incomplete_copy`.

**HeadObject / ListObjects (v1, v2)**

- Только meta DB, лента не трогается. Листинг: префикс, delimiter, `CommonPrefixes`, `max-keys`, `continuation-token` / `marker`, порядок `COLLATE "C"`.
- Storage class: **всегда `GLACIER`**, независимо от состояния объекта (ADR-0003). Отклонение «`STANDARD` до архивации» убрано: оно заставляло клиента видеть смену класса без его действий.
- `x-amz-restore` — по колонке `objects.online_until` (см. «Модель данных» и ADR-0003): дисковая копия есть → `ongoing-request="false", expiry-date="<online_until>"`; `requested`/`in_progress` → `ongoing-request="true"`; копии нет → заголовка нет.
- `x-amz-restore` для архивных объектов: нет записи — заголовка нет; `requested`/`in_progress` — `ongoing-request="true"`; `restored` — `ongoing-request="false", expiry-date="…"`.

**RestoreObject**

- Объект не архивный → вернуть ошибку как AWS для не-Glacier объекта (`InvalidObjectState`).
- Нет записи в `restores` → вставить `requested` с `days`, `tier`; ответ 202.
- Уже `restored` → продлить `expires_at = now() + days`; ответ 200.
- `requested` / `in_progress` → `RestoreAlreadyInProgress` (409).
- Код успеха задать нельзя: `backend.Backend.RestoreObject` возвращает только `error`, а контроллер не заполняет `MetaOptions.Status`, поэтому успех — всегда 200. См. ADR-0001, спайк M0 по этой теме не нужен.

**Recaller**

1. Взять батч `requested` с `FOR UPDATE SKIP LOCKED`, порядок: `tier` (Expedited → Standard → Bulk), затем `requested_at`.
2. Проверить место в кэше: если `cache.used + сумма размеров > cache.high_watermark`, сначала вытеснить истёкшие restore, иначе ждать (backpressure, не ошибка).
3. Если Phobos API отдаёт ленту и смещение объекта — отсортировать батч по (лента, смещение). Иначе положиться на переупорядочивание в LRS (спайк M0).
4. `phobos_get` батчем в файлы `cache/tmp/<uuid>`, проверить размер и ETag (для одиночных PUT — md5), `fsync`, `rename` в `cache/<2 hex>/<uuid>`.
5. Строка → `restored`, `restored_at`, `expires_at = restored_at + days`. Ошибки — ретраи как у migrator, затем `failed`.

**GetObject**

- `landing=true` → отдать из landing. `restored` и не истёк → из cache. Иначе `InvalidObjectState`.
- Range-запросы и условные заголовки (`If-Match`, `If-None-Match`, `If-Modified-Since`) — поведение как в posix-бэкенде.
- Файл открывается до ответа, поэтому вытеснение **после** открытия безопасно: Linux держит inode до закрытия дескриптора.
- Промежуток «прочитали состояние → открыли файл» закрывает ADR-0004: janitor сначала ставит `landing=false` в транзакции и только потом делает `unlink`, а GET на `ENOENT` перечитывает строку и пересчитывает решение (максимум `spool.evict_retries` повторов, затем `InvalidObjectState`, а не `InternalError`).

**CopyObject**

Источник онлайн → копия как новый PUT (reflink, если источник в landing). Источник офлайн → `InvalidObjectState`, как в AWS.

**DeleteObject / DeleteObjects**

Транзакция: текущая строка → `is_current=false, state='deleting'`; ответ 204. Deleter-воркер вызывает `phobos_delete(uuid)`, удаляет файлы из landing и cache, переводит строку в `deleted`. Повтор `phobos_delete` для уже удалённого объекта считается успехом. Место на ленте не освобождается до repack (вне MVP).

**Janitor** (периодически)

- Вытесняет landing-копии `archived`-объектов при превышении `landing.high_watermark` до `low_watermark`, начиная с самых старых.
- Удаляет истёкшие restore: строка из `restores`, файл из cache.
- Удаляет незавершённые multipart старше `mpu.ttl`.
- Orphan GC: файлы в landing, cache, mpu без соответствующей строки старше `orphan.grace`.
- Чистка терминальных строк: `objects` в состояниях `deleted` и `failed` помечаются `retired_at` и физически удаляются старше `meta.terminal_retention` (по умолчанию 90 дней). Без этого таблица растёт всю жизнь системы: каждая перезапись ключа добавляет строку, а по `objects` построены индексы очередей и листинга. `admin verify` на ретайрных строках не ругается, но проверяет, что файлов у них уже нет.

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
- Носитель объекта узнаётся один раз при архивации и кладётся в `objects.phobos_medium` (ADR-0008); recaller группирует по этой колонке и в DSS не обращается.
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
- Все команды манипулятора сериализуются одним мьютексом: у робота одна каретка.

**Load(drive, label)**

1. Найти слот по метке в inventory; драйв должен быть пуст.
2. `robot.Move(slot → drive)`.
3. Ждать TEST UNIT READY драйва до `drive.ready_timeout`.
4. Если включено, прочитать метку из MAM и сравнить с ожидаемой; расхождение → выгрузить ленту обратно, ответить ошибкой, пометить слот как требующий проверки.
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

**Конфиг tapegw** (YAML; любое поле переопределяется переменной `TAPEGW_*`):

```yaml
s3:
  listen: [":7070"]
  region: us-east-1
  tls: {cert: "", key: ""}
  iam: {dir: /etc/tapegw/iam}          # прокидывается в embedgw.Config
  default_storage_class: GLACIER
  max_object_size: 100GiB               # <= migrator.batch_max_bytes, проверяется
                                       # при старте; см. ADR-0008 и M0-07
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
  library: tape_lib                       # API не читает профиль, задаём явно (ADR-0007)
  layout: raid1
  layout_params: {repl_count: "1"}      # 1 реплика: mover берёт свой драйв
  tags: []
  movers:
    migrator: 1                         # процессов mover на архивацию
    recaller: 1                         # и на restore; сумма <= числа драйвов
migrator:
  batch_max_objects: 1000
  batch_max_bytes: 200GiB
  batch_max_wait: 15m
  max_attempts: 10
recaller:
  batch_max_objects: 500
  batch_max_wait: 5m
  max_attempts: 10
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
12. Лицензия репозитория — Apache-2.0 (как у versitygw); файлы, взятые из Phobos, сохраняют LGPL-заголовок.

13. Ни одна транзакция Postgres не открыта во время вызова Phobos. Воркер коммитит переход состояния (например `pending → archiving`), отпускает транзакцию и лок, и только потом вызывает `phobos_put`; результат пишется новой транзакцией. Иначе лента держит строки и advisory-локи ключей: `phobos_put` батчем на 200 GiB может идти минуты, а параллельный PUT тех же ключей в это время упрётся в `lock_timeout`. Таймаут лока оставляем как вторую линию защиты, а не как замену правилу.

## План разработки

Десять этапов: M0 снимает неизвестные, M1–M6 строят tapegw снизу вверх, M7–M8 параллельно строят klipper-tlc, M9 доводит всё до эксплуатации. Этап закрыт, когда выполнены все пункты его критериев и прошло ревью.

&#91;embedded content: этапы M0–M9 и зависимости\]

M5 — первый полный Glacier-цикл без железа; M8 переносит его на реальные драйв и манипулятор. Порядок: M0 → M1 → M2 → M3 → M4 → M5 → M6 → M9; параллельно M0 → M7 → M8 → M9; закрытие M7 и M8 требует готового M5.

### M0. Спайки и стенд ci-dir

Результат — `docs/spikes/*.md` с выводами и `deploy/Containerfile` EL9 с Phobos. Код спайков в основную ветку не идёт.

- [ ] Собрать Phobos (зафиксированный тег) и LTFS из исходников в контейнере EL9; `phobos put/get/delete` через CLI на family `dir`.
- [ ] Минимальная cgo-программа: `phobos_init`, `phobos_put` батчем из 3 объектов по fd с атрибутами, `phobos_getmd`, `phobos_get`, `phobos_delete`.
- [x] Потокобезопасность: libphobos однопоточна, один `phobos_put` пишет на один носитель — см. [M0-02](spikes/M0-02-libphobos-api.md) и [ADR-0007](adr/0007-mover-worker.md); вместо `max_inflight` два пула процессов-воркеров.
- [ ] Есть ли в API способ узнать ленту и смещение объекта (для сортировки restore).
- [ ] Кто выгружает ленту из драйва перед unload в штатном потоке TLC.
- [ ] Может ли бэкенд versitygw влиять на код ответа RestoreObject (200 / 202 / 409).
- [ ] mhVTL на VM EL9: штатный `phobos_tlc`, load/unload, работает ли LTFS на виртуальных драйвах.

### M1. Скелет

- [ ] Структура репозитория, `AGENTS.md`, CI: `go vet`, `staticcheck`, юнит-тесты, сборка с тегом `phobos` в контейнере.
- [ ] Конфиг, запуск `embedgw.RunVersityGW` с бэкендом-заглушкой, IAM из `iam.dir`.
- [ ] Миграции схемы `tapegw`, пакет `meta`.
- [ ] Бакеты: Create, Head, List, Delete (только пустой), ACL, tagging.

* **Готово когда**: `aws s3 mb / ls / rb` работают против tapegw.

### M2. Онлайн-путь без ленты

- [ ] `spool`: атомарная запись с fsync, MD5 и checksum на лету.
- [ ] PutObject, HeadObject, GetObject (Range, условные заголовки), CopyObject (только онлайн-источник).
- [ ] ListObjects v1 и v2: префикс, delimiter, пагинация, порядок байтов.
- [ ] DeleteObject, DeleteObjects, перезапись ключа.

* **Готово когда**: `aws s3 cp/sync/ls/rm` и `rclone copy/check` проходят на наборе файлов от 0 байт до нескольких ГБ, ключи с юникодом и пробелами; тест порядка листинга против таблицы ожидаемых значений.

### M3. Multipart

- [ ] Create, UploadPart, Complete (reflink с fallback), Abort, ListParts, ListMultipartUploads.
- [ ] ETag вида `md5-N`; janitor удаляет загрузки старше `mpu_ttl`.

* **Готово когда**: `aws s3 cp` файла больше порога multipart и rclone с параллельными частями дают корректные ETag и содержимое.

### M4. Архивация

- [ ] Интерфейс `Store`, `store/fake`, `store/phobos` (cgo).
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
- [ ] **Но простую страховку от ENOSPC оставляем.** Случай, который Phobos не
  решит: новые ленты закончились, на каждой остался хвост меньше батча. Суммарно
  свободного места много, а каждый батч получает ENOSPC, и архивация встаёт.
  Поэтому при ENOSPC батч **делится пополам и повторяется**, вплоть до одного
  объекта. Отказ по ENOSPC откатывается чисто — ни incomplete-копий, ни записей в
  каталоге (в отличие от аварийного завершения процесса), — так что повтор
  безопасен. ENOSPC на батче из одного объекта — это уже алерт «нужны новые
  ленты», а не повод крутить цикл. Реализация — около десятка строк, зато хвосты
  лент используются, а не теряются.
- [ ] **Цепочка ограничений конфига:**
  `s3.max_object_size <= migrator.batch_max_bytes <= свободное место на одной
  ленте`. Средний незаполненный хвост ленты — порядка половины `batch_max_bytes`
  (на LTO-8 при батче 200 GiB это ~1,5 % ёмкости), поэтому `batch_max_bytes`
  настраивается вместе с ёмкостью лент, а не независимо.
- [ ] Janitor: вытеснение landing по watermark.
- [ ] HEAD и LIST отдают storage class по состоянию.

* **Готово когда**: на ci-dir объект проходит `pending → archived`, вытесняется из landing, HEAD показывает `GLACIER`, GET возвращает `InvalidObjectState`; тест убийства процесса во время `phobos_put` проходит без потерь и дублей.

### M5. Restore

- [ ] RestoreObject по контракту из «Потоков», таблица `restores`.
- [ ] Recaller: приоритет по tier, батчи, сортировка по ленте (если M0 нашёл API), backpressure по кэшу.
- [ ] GET из cache, `x-amz-restore`, продление и истечение.

* **Готово когда**: полный Glacier-цикл на ci-dir (put → archive → evict → restore → poll HEAD → GET → expire → снова `InvalidObjectState`), а затем на vtl со штатным `phobos_tlc`.

### M6. Удаление и сборка мусора

- [ ] Deleter → `phobos_delete`, идемпотентность.
- [ ] Orphan GC для landing, cache, mpu.

* **Готово когда**: после удаления и перезаписи ключей не остаётся файлов и живых объектов Phobos без строки в `tapegw`; тест сверки «БД ↔ диск ↔ Phobos».

### M7. klipper-tlc на mhVTL (параллельно M2–M6, после M0)

- [ ] Кадрирование и протокол из `.proto`, диспетчер запросов.
- [ ] Inventory на SQLite, бэкенды robot `sim` и `smc`, пакет `drive` (TUR, unload, MAM).
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
- [ ] `rebuild-meta` из атрибутов Phobos.
- [ ] Нагрузочный тест: landing держит поток записи, драйв пишет без «чистки» (shoe-shining) на батчах из конфига.

* **Готово когда**: восстановление `tapegw` из пустой схемы через `rebuild-meta` на стенде совпадает с исходной; есть runbook на типовые аварии.

**После MVP** (каждое — отдельное ADR): упаковка мелких объектов в пачки, две копии на разных лентах, привязка бакетов к пулам лент через теги Phobos, repack и освобождение места, несколько фронтенд-узлов.

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
| libphobos не потокобезопасна или блокирует надолго | Restore ждёт архивацию, пропускная способность ниже драйва | Спайк M0; раздельные пулы для migrator и recaller; при необходимости вынести вызовы Phobos в отдельный процесс-воркер |
| Мелкие объекты | Каждый объект — файл на LTFS: медленно и раздувает индекс | В MVP задокументировать рекомендацию по размеру; упаковка — первое ADR после MVP |
| Тайминги механики против таймаутов Phobos | LRS считает load проваленным, пока робот ещё едет | Найти таймауты LRS и TLC-клиента в конфиге Phobos; `klipper-tlc` отвечает только после TEST UNIT READY |
| Нет считывателя штрихкодов | Inventory расходится с реальностью | Сверка метки через MAM на каждой загрузке, `verify` в CLI |
| Изменения `backend.Backend` между версиями versitygw | Сборка ломается при обновлении | Фиксированная версия; обновление отдельным PR с прогоном e2e |
| Phobos только под EL | Сложная сборка и деплой | Контейнер сборки в M0, RPM как артефакт CI |
| Удаления не освобождают ленту | Ленты заполняются «мёртвыми» данными | Метрика доли удалённых байт на ленте; repack — после MVP |
| Landing медленнее драйва | Драйв пишет рывками и изнашивается | Нагрузочный тест M9; NVMe под landing; батчи достаточного объёма |

**Открытые вопросы к владельцу** (ответы влияют на конфиг и порядок работ, не на архитектуру)

- [ ] Модель и поколение драйвов, сколько их, интерфейс подключения (SAS, FC).
- [ ] Есть ли на манипуляторе считыватель штрихкодов.
- [ ] Ожидаемое распределение размеров объектов и общий объём в год.
- [ ] Нужна ли вторая копия на другой ленте с первого дня.
- [ ] Какие клиенты будут писать (rclone, бэкап-софт, свой код) — от этого зависит набор S3-операций для e2e.
- [ ] Нужна ли мультиарендность (разные пользователи и квоты) или достаточно одного аккаунта.

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
