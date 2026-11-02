-- +goose Up
-- ADR-0003 в редакции после ревью PR #1.
--
-- Одна колонка вместо двух механизмов: «до какой даты объект точно можно
-- прочитать с диска». Она же — expiry-date в x-amz-restore, она же — нижняя
-- граница удержания копии для janitor.
--
-- Прежний план (фиктивная дата 2050 для pending/archiving) нарушал собственное
-- обещание: HEAD показывал доступность до 2050, объект спокойно архивировался,
-- janitor вытеснял копию, и GET через час отдавал InvalidObjectState — при том
-- что клиент даты не трогал.

ALTER TABLE objects ADD COLUMN online_until timestamptz;

-- janitor: «архивен и срок онлайн-доступа истёк» — единственный условие,
-- при котором копия вытесняется.
CREATE INDEX objects_evictable ON objects (online_until)
  WHERE state = 'archived';

-- +goose Down
DROP INDEX IF EXISTS objects_evictable;
ALTER TABLE objects DROP COLUMN online_until;
