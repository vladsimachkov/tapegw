-- +goose Up
-- T0: тенанты, пулы, версионирование, Object Lock и шифрование — каркас.
--
-- См. docs/proposals/0009-multitenancy-and-gaps.md и
-- docs/proposals/0010-owner-decisions.md.
--
-- Почему всё в одной миграции и почему колонки NOT NULL. Таблицы `objects` и
-- `buckets` в бою ещё пустые, поэтому колонку можно сделать NOT NULL сразу и без
-- бэкфилла. Откладывать это на T1/V/T2 означало бы писать отдельную миграцию с
-- UPDATE по полной таблице и с ALTER, который берёт блокировку на всё время
-- переписывания, — то есть на живой системе.
--
-- Где NOT NULL поставлен сразу, а где осознанно оставлен NULL — помечено в
-- комментариях. Правило такое: NULL остаётся только там, где значение физически
-- некому выдать до этапа, который его вводит (шифрование — T2). Для таких колонок
-- этап T2 обязан выпустить миграцию с бэкфиллом и SET NOT NULL, и ниже это
-- написано прямо в тексте, чтобы колонка не осталась «вечно nullable».

-- ---------------------------------------------------------------------------
-- Типы
-- ---------------------------------------------------------------------------

-- AWS знает ровно два режима блокировки; третьего не будет, поэтому enum, а не text.
CREATE TYPE retention_mode AS ENUM ('GOVERNANCE','COMPLIANCE');

-- 'unversioned' — бакет никогда не имел версий; 'suspended' — имел, но версии
-- выключены. Различие важно: в AWS у suspended бакета уже созданные версии
-- остаются, и удалить бакет можно только вычистив их.
CREATE TYPE bucket_versioning AS ENUM ('unversioned','enabled','suspended');

CREATE TYPE tenant_status AS ENUM ('active','suspended','deleting','deleted');

CREATE TYPE pool_kind AS ENUM ('shared','dedicated');

-- ---------------------------------------------------------------------------
-- Пулы лент
-- ---------------------------------------------------------------------------
-- Пул — набор лент Phobos с общим тегом. См. спайк M0-08: тег в put — жёсткий
-- фильтр со семантикой И, но помеченную ленту он НЕ защищает от записи без тега.
-- Отсюда два следствия, зашитых в схему:
--   1) phobos_tag NOT NULL и UNIQUE — лента без тега или с чужим тегом это ошибка
--      конфигурации, а не допустимое состояние;
--   2) каждая запись обязана нести тег пула, включая общий пул (у него свой тег),
--      поэтому objects.pool_id тоже NOT NULL.

CREATE TABLE pools (
  id          bigserial PRIMARY KEY,
  name        text NOT NULL UNIQUE,
  -- тег Phobos; префикс 'pool:' по соглашению, чтобы не столкнуться с тегами,
  -- которые оператор вешает руками
  phobos_tag  text NOT NULL UNIQUE CHECK (phobos_tag <> ''),
  repl_count  int  NOT NULL DEFAULT 1 CHECK (repl_count BETWEEN 1 AND 2),
  kind        pool_kind NOT NULL DEFAULT 'shared'
);

-- ---------------------------------------------------------------------------
-- Тенанты
-- ---------------------------------------------------------------------------
-- tenants.id — bigint, а не uuid, в отличие от buckets.id. Причина внешняя:
-- versitygw передаёт тенанта через Account.ProjectID, а он объявлен как int
-- (auth/iam.go:74). UUID туда не положить без словаря соответствия.

CREATE TABLE tenants (
  id          bigserial PRIMARY KEY,
  name        text NOT NULL UNIQUE,
  status      tenant_status NOT NULL DEFAULT 'active',
  pool_id     bigint NOT NULL REFERENCES pools(id),
  -- переопределение лимитов из ADR-0011; отсутствующий ключ = значение из конфига
  limits      jsonb NOT NULL DEFAULT '{}',
  -- Ключ тенанта (TEK), обёрнутый KEK. NULL до этапа T2: шифрования ещё нет и
  -- выдавать ключ некому. T2 обязан сделать бэкфилл и SET NOT NULL —
  -- см. примечание в начале файла.
  wrapped_key bytea,
  -- версия KEK, которой завернут wrapped_key; нужна, чтобы перешифрование KEK
  -- было идемпотентным и чтобы старую версию KEK можно было не хранить зря
  kek_version int NOT NULL DEFAULT 1,
  created_at  timestamptz NOT NULL DEFAULT now(),
  -- строка не удаляется: она нужна для аудита и для учёта уже удалённого клиента
  deleted_at  timestamptz
);

-- ---------------------------------------------------------------------------
-- Базовые строки
-- ---------------------------------------------------------------------------
-- Однотенантовая установка (и весь текущий код) работает без явных тенантов,
-- поэтому общий пул и тенант по умолчанию создаются здесь же. id = 1 зафиксирован
-- явно, а не через bigserial, чтобы DEFAULT ниже ссылался на него без магии;
-- setval нужен, чтобы следующий созданный пул или тенант не получил id = 1.

INSERT INTO pools (id, name, phobos_tag, repl_count, kind)
VALUES (1, 'shared', 'pool:shared', 1, 'shared');

INSERT INTO tenants (id, name, status, pool_id)
VALUES (1, 'default', 'active', 1);

SELECT setval(pg_get_serial_sequence('pools','id'),   1);
SELECT setval(pg_get_serial_sequence('tenants','id'), 1);

-- Учёт (ADR-0011): одна строка на тенанта, без глобальных горячих строк —
-- клиентов тысячи, и общий счётчик стал бы точкой спора всех транзакций.
CREATE TABLE tenant_usage (
  tenant_id            bigint PRIMARY KEY REFERENCES tenants(id) ON DELETE CASCADE,
  storage_bytes        bigint NOT NULL DEFAULT 0 CHECK (storage_bytes >= 0),
  objects              bigint NOT NULL DEFAULT 0 CHECK (objects >= 0),
  landing_bytes        bigint NOT NULL DEFAULT 0 CHECK (landing_bytes >= 0),
  restore_queue_bytes  bigint NOT NULL DEFAULT 0 CHECK (restore_queue_bytes >= 0),
  restore_inflight     int    NOT NULL DEFAULT 0 CHECK (restore_inflight >= 0),
  updated_at           timestamptz NOT NULL DEFAULT now()
);

INSERT INTO tenant_usage (tenant_id) VALUES (1);

-- Учётки versitygw поверх Postgres вместо iam_dir (ADR-0009).
CREATE TABLE accounts (
  access_key     text PRIMARY KEY,
  -- секрет нужен в открытом виде для проверки SigV4, поэтому он не хеш, а
  -- зашифрованное значение (AES-GCM, ключ вне БД). См. ADR-0009.
  secret_wrapped bytea NOT NULL,
  -- 'admin' обходит проверки политик в versitygw (authorizationApplies),
  -- поэтому тенантам он не выдаётся никогда — только системным операторам.
  role           text NOT NULL CHECK (role IN ('user','userplus','admin')),
  tenant_id      bigint NOT NULL REFERENCES tenants(id),
  status         tenant_status NOT NULL DEFAULT 'active',
  created_at     timestamptz NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Бакеты: принадлежность тенанту, версии и Object Lock
-- ---------------------------------------------------------------------------

ALTER TABLE buckets
  ADD COLUMN tenant_id bigint NOT NULL DEFAULT 1 REFERENCES tenants(id);

ALTER TABLE buckets
  ADD COLUMN versioning bucket_versioning NOT NULL DEFAULT 'unversioned';

ALTER TABLE buckets
  ADD COLUMN object_lock_enabled boolean NOT NULL DEFAULT false;

-- retention по умолчанию на бакет (GOVERNANCE/COMPLIANCE + срок).
ALTER TABLE buckets
  ADD COLUMN default_retention jsonb;

-- В AWS Object Lock нельзя включить у бакета без версий, и версии при включённом
-- lock нельзя выключить. Правило дешёвое, а нарушается легко — держим в БД.
ALTER TABLE buckets ADD CONSTRAINT buckets_lock_needs_versioning
  CHECK (NOT object_lock_enabled OR versioning = 'enabled');

-- ---------------------------------------------------------------------------
-- Объекты: тенант, пул, версии, блокировка, ключ
-- ---------------------------------------------------------------------------

ALTER TABLE objects
  ADD COLUMN tenant_id bigint NOT NULL DEFAULT 1 REFERENCES tenants(id);

ALTER TABLE objects
  ADD COLUMN pool_id bigint NOT NULL REFERENCES pools(id) DEFAULT 1;

-- Delete marker — «версия-маркер удаления»: текущая версия без тела, из-за которой
-- GET отдаёт 404, а сама версия под ней остаётся живой.
ALTER TABLE objects
  ADD COLUMN is_delete_marker boolean NOT NULL DEFAULT false;

ALTER TABLE objects ADD CONSTRAINT objects_delete_marker_empty
  CHECK (NOT is_delete_marker OR size = 0);

ALTER TABLE objects
  ADD COLUMN retention_mode retention_mode,
  ADD COLUMN retain_until    timestamptz,
  ADD COLUMN legal_hold      boolean NOT NULL DEFAULT false;

-- Ключ объекта (DEK), обёрнутый TEK тенанта. Свой на каждую версию — иначе nonce
-- в AES-GCM повторялся бы при переиспользовании ключа. NULL до T2 (см. выше).
ALTER TABLE objects
  ADD COLUMN wrapped_dek bytea;

-- Индекс листинга сужается: ListObjects обязан НЕ показывать delete markers, а
-- прежний индекс тянул их в выборку. Заодно точно совпадает с запросом листинга.
DROP INDEX IF EXISTS objects_listing;
CREATE INDEX objects_listing ON objects (bucket_id, key COLLATE "C")
  WHERE is_current AND NOT is_delete_marker;

-- ListObjectVersions показывает все версии и маркеры, включая некурентные, —
-- ему нужен неполный индекс по всем строкам.
CREATE INDEX objects_versions ON objects (bucket_id, key COLLATE "C");

-- Планировщики (ADR-0016): round-robin берёт по чуть-чуть от каждого тенанта с
-- непустой очередью, а не перебирает всех. Порядок колонок именно такой, чтобы
-- DISTINCT ON (tenant_id) и «не больше K объектов одного тенанта» шли по индексу.
CREATE INDEX objects_tenant_queue ON objects (tenant_id, next_try_at)
  WHERE state = 'pending';

-- Суточная сверка счётчиков учёта с фактом (ADR-0011): сумма по всем строкам
-- тенанта, включая некурентные версии и маркеры.
CREATE INDEX objects_tenant_size ON objects (tenant_id, size);

-- ---------------------------------------------------------------------------
-- Restore: тенант для честной очереди
-- ---------------------------------------------------------------------------
-- Denormalizация по той же причине, что у objects.tenant_id: round-robin recaller
-- не должен ради выбора следующего запроса джойнить objects.

ALTER TABLE restores
  ADD COLUMN tenant_id bigint NOT NULL DEFAULT 1 REFERENCES tenants(id);

CREATE INDEX restores_tenant_queue ON restores (tenant_id, tier, requested_at)
  WHERE state = 'requested';

-- ---------------------------------------------------------------------------
-- Части multipart: границы для Range-чтения шифротекста
-- ---------------------------------------------------------------------------
-- Нужны только шифрованию (ADR-0014): после AES-GCM смещение байта открытого
-- текста в шифротексте неизвестно, поэтому без таблицы частей Range не построить.
-- Хранить придётся навсегда — объект читается всё время жизни версии.
--
-- plain_size NOT NULL: M3 (multipart) раньше T2, и он эти размеры заполняет.
-- cipher_offset остаётся NULL до T2 — см. примечание в начале файла.

CREATE TABLE object_parts (
  version_id    uuid NOT NULL REFERENCES objects(id) ON DELETE CASCADE,
  part_number   int  NOT NULL CHECK (part_number >= 1),
  plain_size    bigint NOT NULL CHECK (plain_size >= 0),
  cipher_offset bigint,
  PRIMARY KEY (version_id, part_number)
);

-- +goose Down
DROP TABLE IF EXISTS object_parts;
DROP INDEX IF EXISTS restores_tenant_queue;
ALTER TABLE restores DROP COLUMN IF EXISTS tenant_id;
DROP INDEX IF EXISTS objects_tenant_size;
DROP INDEX IF EXISTS objects_tenant_queue;
DROP INDEX IF EXISTS objects_versions;
DROP INDEX IF EXISTS objects_listing;
CREATE INDEX objects_listing ON objects (bucket_id, key COLLATE "C") WHERE is_current;
ALTER TABLE objects DROP COLUMN IF EXISTS wrapped_dek;
ALTER TABLE objects DROP COLUMN IF EXISTS legal_hold;
ALTER TABLE objects DROP COLUMN IF EXISTS retain_until;
ALTER TABLE objects DROP COLUMN IF EXISTS retention_mode;
ALTER TABLE objects DROP COLUMN IF EXISTS is_delete_marker;
ALTER TABLE objects DROP COLUMN IF EXISTS pool_id;
ALTER TABLE objects DROP COLUMN IF EXISTS tenant_id;
ALTER TABLE buckets DROP CONSTRAINT IF EXISTS buckets_lock_needs_versioning;
ALTER TABLE buckets DROP COLUMN IF EXISTS default_retention;
ALTER TABLE buckets DROP COLUMN IF EXISTS object_lock_enabled;
ALTER TABLE buckets DROP COLUMN IF EXISTS versioning;
ALTER TABLE buckets DROP COLUMN IF EXISTS tenant_id;
DROP TABLE IF EXISTS accounts;
DROP TABLE IF EXISTS tenant_usage;
DROP TABLE IF EXISTS tenants;
DROP TABLE IF EXISTS pools;
DROP TYPE IF EXISTS pool_kind;
DROP TYPE IF EXISTS tenant_status;
DROP TYPE IF EXISTS bucket_versioning;
DROP TYPE IF EXISTS retention_mode;
