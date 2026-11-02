-- +goose Up
-- Исправления по ревью M1 (docs/reviews/2026-11-02-architecture-review.md
-- и замечания ревьюера к коду M1).

-- 1. Приоритет restore. В индексе (tier, requested_at) tier сортировался как
--    текст: Bulk < Expedited < Standard, то есть Expedited ехал бы вторым,
--    а не первым. Приоритет вынесен в числовую колонку, порядок задаёт код.
--    Значения: 0 = Expedited, 1 = Standard, 2 = Bulk.
ALTER TABLE restores ADD COLUMN priority smallint NOT NULL DEFAULT 1;

UPDATE restores
   SET priority = CASE tier
                    WHEN 'Expedited' THEN 0
                    WHEN 'Standard'  THEN 1
                    WHEN 'Bulk'      THEN 2
                    ELSE 1
                  END;

DROP INDEX IF EXISTS restores_queue;
CREATE INDEX restores_queue ON restores (priority, requested_at)
  WHERE state = 'requested';

-- 2. objects_listing полностью дублировал objects_current: те же колонки и
--    тот же предикат. Лишняя запись на каждый PUT.
DROP INDEX IF EXISTS objects_listing;

-- 3. Мягкое удаление бакетов.
--
--    Проверка «бакет пуст» смотрит только на is_current, но строки objects в
--    состояниях deleting/deleted остаются и ссылаются на бакет по FK. Жёсткое
--    удаление бакета давало FK violation и 500 на первом же бакете, в котором
--    вообще были объекты.
--
--    Бакет помечается deleted_at: имя освобождается (частичный уникальный
--    индекс), а строка живёт, пока на неё ссылаются исторические объекты.
--    Переиспользование имени после rb -> mb работает.
ALTER TABLE buckets ADD COLUMN deleted_at timestamptz;
ALTER TABLE buckets DROP CONSTRAINT buckets_name_key;
CREATE UNIQUE INDEX buckets_name_active ON buckets (name) WHERE deleted_at IS NULL;
CREATE INDEX buckets_retired ON buckets (deleted_at) WHERE deleted_at IS NOT NULL;

-- +goose Down
DROP INDEX IF EXISTS buckets_retired;
ALTER TABLE buckets DROP COLUMN deleted_at;
DROP INDEX IF EXISTS buckets_name_active;
ALTER TABLE buckets ADD CONSTRAINT buckets_name_key UNIQUE (name);
DROP INDEX IF EXISTS restores_queue;
CREATE INDEX restores_queue ON restores (tier, requested_at) WHERE state = 'requested';
ALTER TABLE restores DROP COLUMN priority;
CREATE INDEX objects_listing ON objects (bucket_id, key COLLATE "C") WHERE is_current;
