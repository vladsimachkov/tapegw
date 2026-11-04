-- +goose Up
-- Конфигурация Object Lock бакета: JSON, который отдаёт versitygw
-- (auth.BucketLockConfig: Enabled + DefaultRetention). Бэкенд хранит его как
-- есть и отдаёт обратно как есть — контроллер сам превращает в XML.
--
-- text, а не jsonb: тот же урок, что и с buckets.policy. jsonb переставляет
-- ключи и приводит числа, а конфигурация должна возвращаться байт в байт —
-- клиенты сверяют её с тем, что записывали.
ALTER TABLE buckets ADD COLUMN lock_config text;

-- Конфигурация без включённого лока невозможна: lock_config — это и есть её
-- правило, а включённый лок без правила — нормальное состояние (колонка NULL).
-- Обратное направление (лок выключить нельзя) выражается только в коде: схема не
-- видит разницы между «не меняли» и «выключили».
ALTER TABLE buckets
  ADD CONSTRAINT buckets_lock_config_needs_lock
  CHECK (lock_config IS NULL OR object_lock_enabled);

-- +goose Down
ALTER TABLE buckets DROP CONSTRAINT IF EXISTS buckets_lock_config_needs_lock;
ALTER TABLE buckets DROP COLUMN IF EXISTS lock_config;
