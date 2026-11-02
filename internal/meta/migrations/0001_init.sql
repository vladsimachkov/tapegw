-- +goose Up

-- Конечный автомат объекта и состояние restore.
-- См. docs/ARCHITECTURE.md, раздел «Конечный автомат объекта и restore».
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
  id            uuid PRIMARY KEY,          -- = objid в Phobos (решение D3)
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
  archived_at   timestamptz,
  -- Терминальные строки (deleted/failed) помечаются для выгрузки; см. ревью B2.
  retired_at    timestamptz
);

-- один текущий объект на ключ; порядок листинга S3 = порядок байтов
CREATE UNIQUE INDEX objects_current ON objects (bucket_id, key COLLATE "C") WHERE is_current;

-- выборка в очередь migrator/deleter
CREATE INDEX objects_queue ON objects (state, next_try_at) WHERE state IN ('pending','deleting');

-- листинг бакета: префикс + delimiter идут по этому порядку
CREATE INDEX objects_listing ON objects (bucket_id, key COLLATE "C") WHERE is_current;

-- janitor: чистка терминальных строк (ревью B2)
CREATE INDEX objects_retired ON objects (retired_at) WHERE retired_at IS NOT NULL;

CREATE TABLE restores (
  object_id    uuid PRIMARY KEY REFERENCES objects(id) ON DELETE CASCADE,
  state        restore_state NOT NULL,
  tier         text NOT NULL DEFAULT 'Standard',  -- Expedited|Standard|Bulk -> приоритет
  days         int NOT NULL,
  requested_at timestamptz NOT NULL DEFAULT now(),
  restored_at  timestamptz,
  expires_at   timestamptz,
  attempts     int NOT NULL DEFAULT 0,
  next_try_at  timestamptz,
  last_error   text
);

-- janitor: истёкшие restore (ревью M5)
CREATE INDEX restores_expired ON restores (expires_at) WHERE expires_at IS NOT NULL;

-- recaller: выборка requested по приоритету tier
CREATE INDEX restores_queue ON restores (tier, requested_at) WHERE state = 'requested';

CREATE TABLE uploads (            -- multipart
  upload_id   text PRIMARY KEY,
  bucket_id   bigint NOT NULL REFERENCES buckets(id),
  key         text NOT NULL,
  meta        jsonb NOT NULL,     -- content-type, user meta, acl, checksum algo
  initiated   timestamptz NOT NULL DEFAULT now()
);

-- janitor: незавершённые загрузки старше mpu_ttl (ревью M5)
CREATE INDEX uploads_initiated ON uploads (initiated);

CREATE TABLE upload_parts (
  upload_id   text REFERENCES uploads(upload_id) ON DELETE CASCADE,
  part_number int NOT NULL,
  size        bigint NOT NULL,
  etag        text NOT NULL,
  checksums   jsonb,
  PRIMARY KEY (upload_id, part_number)
);

-- +goose Down
DROP TABLE IF EXISTS upload_parts;
DROP TABLE IF EXISTS uploads;
DROP INDEX IF EXISTS restores_queue;
DROP INDEX IF EXISTS restores_expired;
DROP TABLE IF EXISTS restores;
DROP INDEX IF EXISTS objects_retired;
DROP INDEX IF EXISTS objects_listing;
DROP INDEX IF EXISTS objects_queue;
DROP INDEX IF EXISTS objects_current;
DROP TABLE IF EXISTS objects;
DROP TABLE IF EXISTS buckets;
DROP TYPE IF EXISTS restore_state;
DROP TYPE IF EXISTS obj_state;
