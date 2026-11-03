-- +goose Up
-- M2, пункты ревью PR #1.
--
-- 1. online_until становится NOT NULL.
--
--    NULL в SQL коварен именно здесь: условие вытеснения
--        state = 'archived' AND online_until < now()
--    при NULL даёт NULL, то есть «не вытеснять», и молча отключает освобождение
--    места на диске. Ошибка конфигурации выглядела бы как штатная работа.
--    Колонка обязана быть NOT NULL, чтобы нельзя было создать объект без срока.
--
--    Backfill нужен, чтобы ALTER не упал на непустой таблице. На момент M2
--    объектов ещё нет (PUT появился только в M2), но миграция обязана быть
--    безопасной и на заполненной базе. Значение — created_at плюс срок
--    удержания по умолчанию: тот же смысл, что у spool.landing_min_retention.
--    В миграции он захардкожен, потому что миграция конфиг не читает; для
--    пустой таблицы это не имеет значения, а для непустой даёт консервативную
--    нижнюю границу вместо выдумывания прошлого.

UPDATE objects
   SET online_until = created_at + interval '24 hours'
 WHERE online_until IS NULL;

ALTER TABLE objects ALTER COLUMN online_until SET NOT NULL;

-- 2. objects_evictable сужается.
--
--    Прежний вариант (WHERE state = 'archived') зря включал архивные объекты
--    без дисковой копии: вытеснять у них нечего, а таких в работающей системе
--    подавляющее большинство. Индекс разрастался до размера таблицы и
--    использовался бы хуже.
--
--    Условие точно совпадает с запросом janitor: «архивен, копия на диске
--    и срок истёк».

DROP INDEX IF EXISTS objects_evictable;
CREATE INDEX objects_evictable ON objects (online_until)
  WHERE state = 'archived' AND landing;

-- +goose Down
DROP INDEX IF EXISTS objects_evictable;
CREATE INDEX objects_evictable ON objects (online_until)
  WHERE state = 'archived';
ALTER TABLE objects ALTER COLUMN online_until DROP NOT NULL;
