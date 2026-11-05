-- +goose Up
-- Единое определение «доступна для записи» (задача #100).

-- До сих пор предикат жил в трёх местах по отдельности: индекс media_writable
-- брал только lifecycle='active' и терял свежие отформатированные ленты,
-- migrator собирал свой WHERE, планировщик ёмкости — третий. Через месяц их
-- стало бы три расходящихся, и потерялась бы именно свежая лента: пул отказался
-- бы писать на пустую ленту, пока кто-нибудь не переведёт её в active.
--
-- Поколение в определение НЕ входит: оно зависит от политики пула и приходит
-- параметром из конфига, а не из строки media. Запрет по поколению — отдельная
-- причина, и migrator различает «нет места» и «ленты закрыты» (спайк M0-12).
--
-- Запрет записи в условие НЕ входит, хотя выглядит, что должен: CHECK
-- media_read_only_consistent из миграции 0018 гарантирует
-- (lifecycle = 'read_only') = (read_only_reason IS NOT NULL), то есть лента со
-- снятым запретом уже не 'formatted' и не 'active'. Условие было бы мёртвым и
-- вводило бы в заблуждение, будто состояние «active, но закрыта» бывает.
-- Тест TestReadOnlyConsistencyCheckIsEnforced следит, чтобы CHECK не исчез:
-- вместе с ним мёртвое условие станет нужным.

-- +goose StatementBegin
CREATE VIEW media_write_targets AS
    SELECT id, label, pool_id, generation, medium_type,
           COALESCE(free_bytes_cached, 0) AS free_bytes
    FROM media
    WHERE lifecycle IN ('formatted', 'active');
-- +goose StatementEnd

COMMENT ON VIEW media_write_targets IS
    'Ленты, на которые вообще можно писать: отформатирована или активна. '
    'Запрет записи сюда не входит его гарантирует CHECK media_read_only_consistent. '
    'Поколение и наличие места — фильтры сверху, они зависят от запроса. '
    'Индекс media_writable обязан иметь ровно тот же предикат (тест сверяет).';

-- Индекс с тем же предикатом, что у представления. Частичный индекс не может
-- сослаться на представление, поэтому предикат повторяется текстом — и тест
-- migration_0020 сверяет его с текстом представления, а не надеется на память.
DROP INDEX media_writable;
CREATE INDEX media_writable ON media (pool_id, generation)
    WHERE lifecycle IN ('formatted', 'active');

-- +goose Down
DROP VIEW media_write_targets;

-- Индекс пересоздаётся, а не правится: Up его уронил и создал с новым
-- предикатом, поэтому Down обязан сначала убрать текущий. Без этого Down
-- падает на «relation already exists» — и падает только на живой базе, на
-- пустой он бы тоже упал, но проверить это можно только прогоном текста файла.
DROP INDEX media_writable;
CREATE INDEX media_writable ON media (pool_id, generation) WHERE lifecycle = 'active';
