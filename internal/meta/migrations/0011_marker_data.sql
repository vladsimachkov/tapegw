-- +goose Up
-- Применяет значение 'marker' (добавлено миграцией 0010) к данным и закрепляет
-- связь флага и состояния.

-- Маркеры, записанные прежним кодом, лежали в 'deleted' с retired_at. Для них
-- это та же строка, что и для janitor: пролежав 90 дней, маркер был бы удалён,
-- а версия под ним воскресла. Переводим в 'marker' и снимаем retired_at — у
-- живущей версии срока «на выброс» быть не должно.
UPDATE objects
   SET state = 'marker', retired_at = NULL
 WHERE is_delete_marker;

-- Флаг и состояние больше не могут разойтись: is_delete_marker без state='marker'
-- (то есть маркер, который janitor посчитает мусором) и state='marker' без флага
-- (маркер, который листинг покажет как обычный объект) запрещены на уровне схемы.
ALTER TABLE objects
  ADD CONSTRAINT objects_marker_state
  CHECK (is_delete_marker = (state = 'marker'));

-- retired_at — отметка «когда удалить». У маркера её нет: он не удаляется по
-- сроку. Явное ограничение нужно ещё и потому, что перевод в 'deleted' при
-- удалении самого маркера по versionId retired_at выставляет, и ошибка «забыли
-- обнулить» должна быть видна сразу.
ALTER TABLE objects
  ADD CONSTRAINT objects_marker_not_retired
  CHECK (NOT is_delete_marker OR retired_at IS NULL);

-- Down проверен на таблице С ДАННЫМИ (правило 11): два маркера в 'deleted' с
-- retired_at после Up лежат в 'marker' без retired_at, после Down возвращаются
-- в 'deleted' с восстановленным retired_at. На пустой таблице проверка ничего
-- не сказала бы.
--
-- +goose Down
ALTER TABLE objects DROP CONSTRAINT IF EXISTS objects_marker_not_retired;
ALTER TABLE objects DROP CONSTRAINT IF EXISTS objects_marker_state;

UPDATE objects
   SET state = 'deleted', retired_at = now()
 WHERE state = 'marker';
