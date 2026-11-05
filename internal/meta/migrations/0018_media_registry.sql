-- +goose Up
-- Реестр копий, носителей и драйвов (предложение 0013, Ф0.1–Ф0.3 из 0014).
--
-- Всё, что здесь закреплено, после первой записи на ленту меняется только
-- перезаписью архива, поэтому форма важнее удобства:
--
--   1) **носитель живёт в archive_copies, а не колонкой у объекта**. У объекта
--      может быть несколько копий на разных лентах (ADR-0022), и одна колонка
--      их не описывает. Тест migration_0018 проверяет, что колонок носителя
--      нет больше нигде в схеме.
--   2) **у копии обязана быть версия формата**. При двух копиях, записанных в
--      разное время, в одном архиве легально сосуществуют разные версии
--      формата (ADR-0020). Читатель выбирает декодер по версии копии, а не по
--      «текущей версии системы», и узнать её позже негде.
--   3) **журналы только на добавление** — см. триггер внизу: политики здоровья
--      строятся по истории, и правка истории выглядит как здоровая лента.

-- Носитель: от «чистая, ещё не форматирована» до «списана». 'foreign' — лента
-- с известной меткой в чужом пулу: её видно, но трогать её нельзя.
CREATE TYPE media_state AS ENUM (
  'blank', 'registered', 'in_drive', 'evacuating', 'retired', 'lost', 'foreign'
);

CREATE TYPE copy_state AS ENUM ('pending', 'writing', 'done', 'lost', 'dead');

-- Драйвы раньше media: media.current_drive ссылается на них.
CREATE TABLE drives (
  name        text PRIMARY KEY,
  library     text NOT NULL,
  state       text NOT NULL DEFAULT 'unknown'
              CHECK (state IN ('unknown', 'empty', 'loaded', 'broken')),
  -- Что в драйве прямо сейчас. NULL, когда пусто или состояние неизвестно.
  media_id    bigint,
  -- Путь устройства из /dev/tape/by-id — единственный способ однозначно
  -- опознать драйв после перезагрузки (имена /dev/stN плывут).
  scsi_id     text,
  attrs       jsonb NOT NULL DEFAULT '{}',
  updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE media (
  id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  library       text NOT NULL,
  -- Метка уникальна на уровне схемы, а не «договорились не повторять»:
  -- две строки на одну метку означают, что планировщик выберет ленту,
  -- которой уже пользуется другая копия, и запишет поверх живой данных.
  label         text NOT NULL UNIQUE,
  barcode       text,
  generation    text NOT NULL,
  medium_type   text NOT NULL,
  state         media_state NOT NULL DEFAULT 'registered',

  -- Размещение. home_slot — куда лента должна вернуться; magazine — магазин,
  -- он нужен сливу по месту (Ф2.3); in_library_cache — в окне загрузки, а не
  -- в магазине, и манипулятор это различает.
  magazine      text,
  home_slot     text,
  in_library_cache boolean NOT NULL DEFAULT false,
  current_drive text REFERENCES drives (name),

  -- Слой 2 трёхслойного запрета записи (Р5). Обязателен, потому что слой 3
  -- (put_access в Phobos) отказывает как ENOSPC и неотличим от «места нет»
  -- (спайк M0-12): различие «полка закрыта» / «полка полна» живёт здесь.
  writable      boolean NOT NULL DEFAULT true,
  read_only_reason text,
  -- Закрытая для записи лента не может быть «доступна для записи» — иначе
  -- два слоя запрета начнут спорить, и победит тот, что громче.
  CONSTRAINT media_writable_state CHECK (
    NOT (writable AND state IN ('retired', 'lost', 'foreign', 'blank'))
  ),

  -- Геометрия: без неё планировщик не знает, влезет ли пачка, а «сколько
  -- места» без носителя не узнать.
  capacity_bytes bigint CHECK (capacity_bytes IS NULL OR capacity_bytes >= 0),
  free_bytes     bigint CHECK (free_bytes IS NULL OR free_bytes >= 0),
  format_version smallint,

  attrs         jsonb NOT NULL DEFAULT '{}',
  created_at    timestamptz NOT NULL DEFAULT now(),
  updated_at    timestamptz NOT NULL DEFAULT now(),
  -- Свободных байт не больше ёмкости: перевёрнутая пара выглядит как
  -- «места много» и уводит планировщик на ленту, которая не примет пачку.
  CONSTRAINT media_geometry_sane CHECK (
    capacity_bytes IS NULL OR free_bytes IS NULL OR free_bytes <= capacity_bytes
  )
);

ALTER TABLE drives
  ADD CONSTRAINT drives_media_fk FOREIGN KEY (media_id) REFERENCES media (id);

CREATE INDEX media_by_state ON media (library, state);
-- Планировщик ищет ленту для записи в конкретном пуле-поколении; этот запрос
-- на каждом бате, и без индекса он смотрит всю таблицу.
CREATE INDEX media_writable ON media (library, generation) WHERE writable;

-- Копии (ADR-0022): каждая копия — самостоятельный объект Phobos.
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
  -- Носитель копии. Массив, потому что repl_count = 2 кладёт реплики на
  -- разные ленты, и для Phobos это одна копия.
  medium          text[] NOT NULL DEFAULT '{}',
  state           copy_state NOT NULL DEFAULT 'pending',
  attrs           jsonb NOT NULL DEFAULT '{}',
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now(),
  done_at         timestamptz,
  -- Готовая копия обязана знать, где лежит: 'done' без носителя — это объект,
  -- который нельзя прочитать и нельзя удалить, только гадать.
  CONSTRAINT archive_copies_done_has_medium CHECK (
    state <> 'done' OR cardinality(medium) > 0
  ),
  -- Идемпотентность перехода состояния (правило 7) опирается на то, что
  -- повторная запись той же копии — это та же строка, а не вторая.
  CONSTRAINT archive_copies_copy_unique UNIQUE (phobos_oid, phobos_version, spec)
);

CREATE INDEX archive_copies_by_pack ON archive_copies (pack_id) WHERE pack_id IS NOT NULL;
CREATE INDEX archive_copies_by_object ON archive_copies (object_id) WHERE object_id IS NOT NULL;
-- Reconciler ищет копии не по состоянию «вообще», а по «не хватает такой-то».
CREATE INDEX archive_copies_incomplete ON archive_copies (state) WHERE state IN ('pending', 'writing', 'lost');

-- Журналы. Только на добавление: по ним строятся политики здоровья (Р9), и
-- правка записи журнала — это не «починили данные», а «подделали историю
-- больной ленты под здоровую».
CREATE TABLE media_events (
  id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  media_id    bigint NOT NULL REFERENCES media (id) ON DELETE CASCADE,
  event       text NOT NULL,
  detail      jsonb NOT NULL DEFAULT '{}',
  occurred_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX media_events_recent ON media_events (media_id, occurred_at DESC);

CREATE TABLE drive_events (
  id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  drive_name  text NOT NULL REFERENCES drives (name) ON DELETE CASCADE,
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
  RETURN NULL;
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
DROP TABLE archive_copies;
ALTER TABLE drives DROP CONSTRAINT drives_media_fk;
DROP TABLE media;
DROP TABLE drives;
DROP TYPE copy_state;
DROP TYPE media_state;
