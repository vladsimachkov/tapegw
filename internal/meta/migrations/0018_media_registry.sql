-- +goose Up
-- Реестр копий, носителей и драйвов (предложение 0013, Ф0.1–Ф0.3 из 0014).
--
-- Форма таблиц сверена с 0013 раздел 4 и с 0014, а не с удобством реализации:
-- после первой записи на ленту всё это меняется только перезаписью архива.
--
--   1) **носитель — у копии, и один**: `medium_id`, а не колонка у объекта и не
--      массив. У объекта несколько копий на разных лентах (ADR-0022), и каждая
--      из них — отдельный объект Phobos на одной ленте (`no_split`), поэтому
--      «одна копия — один носитель» и есть модель ADR-0022. Массив был бы
--      отголоском встроенных реплик `repl_count = 2`, от которых ADR-0022
--      отказался.
--   2) **у копии обязана быть версия формата** (ADR-0020): при двух копиях
--      разные версии легально сосуществуют, читатель выбирает декодер по ней.
--   3) **жизненный цикл и местоположение — два поля**, как в 0013. Одно поле
--      их не вмещает: «лента сливается и прямо сейчас в драйве» — нормальное
--      состояние при сливе, а «носитель недоступен» для recaller'а (Ф1.3)
--      вообще про другое.
--   4) **журналы только на добавление** — см. триггер внизу.

-- Жизненный цикл носителя (0013): new → formatted → active → full →
-- read_only → evacuating → retired. 'read_only' — это слой 2 запрета записи из
-- Р5: лента закрыта политикой поколения, и именно это поле отличает «полка
-- закрыта» от «полка полна», потому что Phobos на отказ по записи отвечает
-- ENOSPC (спайк M0-12).
CREATE TYPE media_lifecycle AS ENUM (
  'new', 'formatted', 'active', 'full', 'read_only', 'evacuating', 'retired'
);

-- Где носитель физически (0013). Независимо от жизненного цикла: уходящая с
-- полок носитель бывает и в драйве, и в пути.
CREATE TYPE media_location AS ENUM (
  'in_slot', 'in_drive', 'in_transit', 'offline', 'offsite', 'lost'
);

CREATE TYPE media_kind AS ENUM ('data', 'cleaning', 'diagnostic');
CREATE TYPE media_health AS ENUM ('ok', 'degraded', 'failing');
CREATE TYPE drive_state AS ENUM ('online', 'offline', 'needs_cleaning', 'failed');
CREATE TYPE copy_state AS ENUM ('pending', 'writing', 'done', 'lost', 'dead');

CREATE TABLE drives (
  -- Имя, которым драйв знает Phobos (и которым оперирует протокол mover,
  -- ADR-0021), — ключ. Серийник при этом уникален: это то же самое «железо»,
  -- опознанное паспортом, и два имени на один серийник — два mover'а на один
  -- привод.
  name        text PRIMARY KEY,
  serial      text UNIQUE,
  library     text NOT NULL,
  -- Поколение драйва нужно и для «читать LTO-7 на драйвах LTO-7, писать на
  -- LTO-8» (С2, С4), и для выбора mover по совместимости. Без него выбор
  -- mover'а превращается в гадание.
  generation  text NOT NULL,
  model       text,
  firmware    text,
  state       drive_state NOT NULL DEFAULT 'offline',
  -- Что в драйве прямо сейчас. NULL, когда пусто или состояние неизвестно.
  media_id    bigint,
  -- Путь из /dev/tape/by-id — единственный способ однозначно опознать драйв
  -- после перезагрузки: имена /dev/stN плывут.
  scsi_id     text,
  attrs       jsonb NOT NULL DEFAULT '{}',
  updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE media (
  id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  -- Метка уникальна на уровне схемы, а не «договорились не повторять»: две
  -- строки на одну метку означают, что планировщик выберет ленту, которой уже
  -- пользуется другая копия, и запишет поверх живой данных.
  label       text NOT NULL UNIQUE,
  barcode     text,
  -- КЭШ: библиотека читается у Phobos (`phobos tape list`), это не источник
  -- истины (0013). NULL — ещё не опознана. Названа *_cached вместе с ёмкостью:
  -- планировщик, который доверится кэшу как истине, выберет ленту по устаревшим
  -- данным и упрётся в отказ Phobos вместо того, чтобы пересканировать.
  library_cached text,
  kind        media_kind NOT NULL DEFAULT 'data',
  generation  text NOT NULL,
  medium_type text NOT NULL,
  worm        boolean NOT NULL DEFAULT false,
  -- Пул ленты. Может быть не назначен: лента добавлена в реестр раньше, чем
  -- её разместили в пул (Ф1.5 — добавление одной операцией с проверками).
  pool_id     bigint REFERENCES pools (id),
  -- Сторона (для библиотек с двумя сторонами/плечами). Без неё не проверить
  -- «лента не меняет сторону» (Ф1.7) и не разложить копии по сторонам (С1).
  side        text,

  lifecycle   media_lifecycle NOT NULL DEFAULT 'new',
  location    media_location  NOT NULL DEFAULT 'in_slot',
  health      media_health    NOT NULL DEFAULT 'ok',
  last_verified_at timestamptz,

  -- Размещение на полках. magazine нужен сливу по месту (Ф2.3);
  -- in_library_cache — уточнение к location='in_slot': лента в окне загрузки,
  -- а не в магазине, и манипулятор это различает.
  magazine   text,
  home_slot  text,
  in_library_cache boolean NOT NULL DEFAULT false,
  current_drive text REFERENCES drives (name),
  -- location='in_drive' обязана быть в драйве: иначе «в драйве» без указания
  -- в каком — это состояние, которое нельзя ни проверить, ни починить.
  CONSTRAINT media_in_drive_has_drive CHECK (
    location <> 'in_drive' OR current_drive IS NOT NULL
  ),

  -- Причина закрытия для записи. Держим её отдельно и проверяем согласованность
  -- с lifecycle: закрытая лента без причины неотличима от закрытой по ошибке,
  -- а «закрыта политикой» без lifecycle обманет планировщик.
  read_only_reason text,
  CONSTRAINT media_read_only_consistent CHECK (
    (lifecycle = 'read_only') = (read_only_reason IS NOT NULL)
  ),

  -- КЭШ данных Phobos, а не источник истины (0013): ёмкость и свободное место
  -- читаются у Phobos (`phobos tape list`, extent list) и переписываются при
  -- каждой сверке и после каждой записи. Названы *_cached именно поэтому:
  -- планировщик, который доверится им как истине, будет выбирать ленту по
  -- устаревшим байтам.
  capacity_bytes_cached bigint CHECK (capacity_bytes_cached IS NULL OR capacity_bytes_cached >= 0),
  free_bytes_cached     bigint CHECK (free_bytes_cached IS NULL OR free_bytes_cached >= 0),
  format_version smallint,

  attrs       jsonb NOT NULL DEFAULT '{}',
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now(),
  -- Свободных байт не больше ёмкости: перевёрнутая пара выглядит как
  -- «места много» и уводит планировщик на ленту, которая не примет пачку.
  CONSTRAINT media_geometry_sane CHECK (
    capacity_bytes_cached IS NULL OR free_bytes_cached IS NULL
      OR free_bytes_cached <= capacity_bytes_cached
  )
);

ALTER TABLE drives
  ADD CONSTRAINT drives_media_fk FOREIGN KEY (media_id) REFERENCES media (id);

CREATE INDEX media_by_lifecycle ON media (library_cached, lifecycle);
-- Планировщик ищет ленту для записи в конкретном поколении; запрос на каждый
-- батч, и без индекса он смотрит всю таблицу. 'active' — единственное
-- состояние, в которое разрешена запись: 'full' уже не примет, 'read_only'
-- закрыта политикой, 'evacuating' уходит с полок.
CREATE INDEX media_writable ON media (library_cached, generation) WHERE lifecycle = 'active';

-- Копии (ADR-0022): каждая копия — самостоятельный объект Phobos на одной
-- ленте.
CREATE TABLE archive_copies (
  -- Id генерится схемой: копия создаётся в той же транзакции, что и запись о
  -- переходе состояния, и требовать от каждого воркера свою uuid — способ
  -- получить дубль на повторе вместо идемпотентного пропуска.
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  -- Ровно один владелец: копия либо пачки, либо одиночного объекта. Оба —
  -- ошибка маршрутизации, ни одного — висящая копия, которую никто не удалит.
  pack_id         uuid REFERENCES packs (id),
  object_id       uuid REFERENCES objects (id),
  CONSTRAINT archive_copies_one_owner CHECK (
    (pack_id IS NULL) <> (object_id IS NULL)
  ),
  -- Спецификация размещения: 'side:left', 'pool:default' … Именно по ней
  -- reconciler понимает, какая копия есть и какой не хватает.
  spec            text NOT NULL,
  phobos_oid      text NOT NULL,
  phobos_version  bigint NOT NULL DEFAULT 1,
  -- Версия формата обязательна (см. шапку): при двух копиях разные версии
  -- легально сосуществуют, и декодер выбирается по ней.
  format          smallint NOT NULL CHECK (format >= 1),
  -- Один носитель на копию и связан с реестром: метка в массиве не связана
  -- ни с чем, и опечатка в ней или удалённая лента прошли бы незамеченными.
  medium_id       bigint REFERENCES media (id),
  state           copy_state NOT NULL DEFAULT 'pending',
  attrs           jsonb NOT NULL DEFAULT '{}',
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now(),
  done_at         timestamptz,
  -- Готовая копия обязана знать, где лежит: 'done' без носителя — это объект,
  -- который нельзя прочитать и нельзя удалить, только гадать.
  CONSTRAINT archive_copies_done_has_medium CHECK (
    state <> 'done' OR medium_id IS NOT NULL
  ),
  -- Идемпотентность перехода состояния (правило 7) опирается на то, что
  -- повторная запись той же копии — это та же строка, а не вторая.
  CONSTRAINT archive_copies_copy_unique UNIQUE (phobos_oid, phobos_version, spec)
);

CREATE INDEX archive_copies_by_pack ON archive_copies (pack_id) WHERE pack_id IS NOT NULL;
CREATE INDEX archive_copies_by_object ON archive_copies (object_id) WHERE object_id IS NOT NULL;
-- Reconciler ищет копии не по состоянию «вообще», а по «не хватает такой-то».
CREATE INDEX archive_copies_incomplete ON archive_copies (state) WHERE state IN ('pending', 'writing', 'lost');

-- Одна живая копия на спецификацию. UNIQUE (phobos_oid, phobos_version, spec)
-- от этого не защищает: две копии 'left' одной пачки с разными oid проходят
-- обе и обе могут быть 'done', а reconciler не поймёт, какая настоящая.
-- Владелец — пачка или объект, поэтому coalesce: у одной копии он ровно один
-- (см. archive_copies_one_owner).
CREATE UNIQUE INDEX archive_copies_live_spec
  ON archive_copies (coalesce(pack_id, object_id), spec)
  WHERE state IN ('pending', 'writing', 'done');

-- Журналы. Только на добавление: по ним строятся политики здоровья (Р9), и
-- правка записи журнала — это не «починили данные», а «подделали историю
-- больной ленты под здоровую».
--
-- ON DELETE RESTRICT, а не CASCADE: ленты по модели не удаляются, а выводятся
-- (retired), и история должна жить дольше строки ленты, иначе вывод поколения
-- стирает доказательства, по которым его и решали выводить.
CREATE TABLE media_events (
  id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  media_id    bigint NOT NULL REFERENCES media (id) ON DELETE RESTRICT,
  event       text NOT NULL,
  detail      jsonb NOT NULL DEFAULT '{}',
  occurred_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX media_events_recent ON media_events (media_id, occurred_at DESC);

CREATE TABLE drive_events (
  id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  drive_name  text NOT NULL REFERENCES drives (name) ON DELETE RESTRICT,
  event       text NOT NULL,
  detail      jsonb NOT NULL DEFAULT '{}',
  occurred_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX drive_events_recent ON drive_events (drive_name, occurred_at DESC);

-- «Только на добавление» закрепляется в схеме, а не в коде. Привилегиями
-- нельзя: приложение и миграции работают под одной ролью (общий db.dsn), а
-- владелец таблицы свои grants обходит молча. Триггер обходится так же, зато
-- обходится **явно**: сессия обязана назвать причину, и это остаётся в
-- логе и в коде, а не в «мы же договорились».
CREATE FUNCTION tapegw_journal_append_only() RETURNS trigger
-- +goose StatementBegin
LANGUAGE plpgsql AS $$
BEGIN
  IF coalesce(current_setting('tapegw.journal_override', true), '') = '' THEN
    RAISE EXCEPTION 'таблица %.% только на добавление (см. миграцию 0018)',
      TG_TABLE_SCHEMA, TG_TABLE_NAME
      USING HINT = 'SET tapegw.journal_override = ''<причина>'' для исправления записи';
  END IF;
  -- RETURN COALESCE(NEW, OLD), а не NULL. Для строчного BEFORE-триггера NULL
  -- означает «пропустить операцию»: с разрешённым обходом UPDATE и DELETE
  -- молча затрагивали бы ноль строк — защита работала бы, а заявленный обход
  -- нет, и проверить это можно было только по числу изменённых строк.
  RETURN COALESCE(NEW, OLD);
END $$;
-- +goose StatementEnd

CREATE TRIGGER media_events_append_only
  BEFORE UPDATE OR DELETE ON media_events
  FOR EACH ROW EXECUTE FUNCTION tapegw_journal_append_only();

CREATE TRIGGER drive_events_append_only
  BEFORE UPDATE OR DELETE ON drive_events
  FOR EACH ROW EXECUTE FUNCTION tapegw_journal_append_only();

-- +goose Down
DROP TRIGGER drive_events_append_only ON drive_events;
DROP TRIGGER media_events_append_only ON media_events;
DROP FUNCTION tapegw_journal_append_only();
DROP TABLE drive_events;
DROP TABLE media_events;
DROP INDEX archive_copies_live_spec;
DROP TABLE archive_copies;
ALTER TABLE drives DROP CONSTRAINT drives_media_fk;
DROP TABLE media;
DROP TABLE drives;
DROP TYPE copy_state;
DROP TYPE drive_state;
DROP TYPE media_health;
DROP TYPE media_location;
DROP TYPE media_lifecycle;
DROP TYPE media_kind;
