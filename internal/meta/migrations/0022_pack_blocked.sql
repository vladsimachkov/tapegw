-- +goose Up
-- Пачка, застрявшая до разбора человеком (#101, срочная часть).

-- Исход «Phobos ответил exists»: дубля нет, но цела ли копия — из exists не
-- следует (incomplete после обрыва выглядит так же, спайк M0-02). Такую пачку
-- нельзя ни считать архивированной, ни пересылать: повтор даёт тот же exists, а
-- пересылка — это гигабайты по сети впустую, раз в срок захвата и бесконечно.

-- Обычные колонки, без нового значения enum: 'stuck' в pack_state завести
-- нельзя в одной миграции с CHECK или частичным индексом, которые его
-- употребляют (55P04 «unsafe use of new value»), а обход через
-- выполнив ни одного шага (проверено: is_applied = t, колонок нет).
-- Проверенный путь для enum — две миграции (см. 0010/0011); здесь он не нужен:
-- «заблокирована» — это флаг поверх состояния, а не новое состояние. Пачка
-- остаётся 'sealed', участники — в 'archiving' с pack_id (инвариант 2).
ALTER TABLE packs ADD COLUMN blocked_reason text;
ALTER TABLE packs ADD COLUMN blocked_at timestamptz;

-- Причина обязательна: блок без объяснения — строка, по которой непонятно, что
-- смотреть и когда разблокировать.
ALTER TABLE packs ADD CONSTRAINT packs_blocked_pair CHECK (
  (blocked_reason IS NULL) = (blocked_at IS NULL)
);

-- «Что стоит и с какого времени» — по этому же индексу читается очередь разбора.
CREATE INDEX packs_blocked ON packs (blocked_at) WHERE blocked_reason IS NOT NULL;

-- +goose Down
DROP INDEX IF EXISTS packs_blocked;
ALTER TABLE packs DROP CONSTRAINT IF EXISTS packs_blocked_pair;
ALTER TABLE packs DROP COLUMN IF EXISTS blocked_at;
ALTER TABLE packs DROP COLUMN IF EXISTS blocked_reason;
