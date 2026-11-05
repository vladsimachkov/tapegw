-- +goose Up
-- Пачки и изменяемая ссылка объекта на место хранения (предложение 0011).
--
-- Пачка — один объект для Phobos, поэтому `phobos tape repack` переносит её
-- целиком, пока жив хоть один участник: уплотнение пачек остаётся нашей
-- операцией, а она опирается на модель данных, которую потом не поправить.
-- Отсюда два решения, которые нельзя добавить задним числом:
--
--   1) ссылка на место хранения **изменяемая**: участник хранит не «свой oid»,
--      а (pack_id, pack_offset, pack_length), и перенос в новую пачку — это
--      UPDATE одной транзакцией вместе со сменой состояния старой;
--   2) живые байты пачки считаются сразу: без live_bytes планировщик уплотнения
--      не знает, какие ленты пустеют, и вынужден читать содержимое лент.
--
-- Носителей у пачки здесь нет: при repl_count = 2 и при двух копиях объект
-- лежит на нескольких лентах, и описывает это таблица archive_copies
-- (предложение 0013), где каждая копия — отдельная строка.

CREATE TYPE pack_kind AS ENUM ('data', 'journal');
CREATE TYPE pack_state AS ENUM ('open', 'sealed', 'archived', 'compacting', 'retired');

CREATE TABLE packs (
  id            uuid PRIMARY KEY,
  pool_id       bigint NOT NULL REFERENCES pools (id),
  kind          pack_kind  NOT NULL,
  state         pack_state NOT NULL DEFAULT 'open',
  -- total_bytes — сколько в пачке записано, live_bytes — из этого живо.
  -- Разница и есть смысл уплотнения: repack ленты смотрит на неё, а не на
  -- содержимое носителя.
  total_bytes   bigint NOT NULL DEFAULT 0 CHECK (total_bytes >= 0),
  live_bytes    bigint NOT NULL DEFAULT 0 CHECK (live_bytes >= 0 AND live_bytes <= total_bytes),
  members_total int    NOT NULL DEFAULT 0 CHECK (members_total >= 0),
  members_live  int    NOT NULL DEFAULT 0 CHECK (members_live >= 0 AND members_live <= members_total),
  -- Носителя здесь нет намеренно: место хранения каждой копии описывает
  -- таблица archive_copies (предложение 0013), а не колонка. У объекта может
  -- быть несколько копий на разных лентах, и одна колонка их не описывает, а
  -- после первой записи на ленту замена колонки таблицей стала бы миграцией
  -- всего архива. Пока данных нет — бесплатно.
  created_at    timestamptz NOT NULL DEFAULT now(),
  sealed_at     timestamptz,
  archived_at   timestamptz,
  retired_at    timestamptz
);

-- Открытые пачки ищутся по пулу и назначению на каждый записываемый объект:
-- «есть ли уже открытая пачка этого пула под данные» — первый вопрос маршрутизации.
CREATE INDEX packs_open ON packs (pool_id, kind, created_at) WHERE state = 'open';

-- Ссылка участника пачки. В момент 'pending' пустая: пачку назначит
-- планировщик. Носитель — не здесь и не колонкой: у объекта их может быть
-- несколько, и описывает их archive_copies (предложение 0013).
ALTER TABLE objects
  ADD COLUMN pack_id      uuid REFERENCES packs (id),
  ADD COLUMN pack_offset  bigint,
  ADD COLUMN pack_length  bigint;

-- Участник пачки обязан знать, где в ней лежит: смещение и длину. Отдельная
-- проверка, а не «разберёмся при чтении», — иначе потерянный offset читает
-- чужие байты молча и шифротекст выглядит порчей.
--
-- Добавляется как NOT VALID и валидируется отдельно: ADD COLUMN ... CHECK под
-- ACCESS EXCLUSIVE пересчитал бы всю таблицу objects, а на ней миллионы версий
-- и блокировка на время сканирования остановила бы PUT. NOT VALID ставит
-- ограничение сразу, а VALIDATE не мешает записи.
ALTER TABLE objects
  ADD CONSTRAINT objects_pack_member CHECK (
    pack_id IS NULL OR (pack_offset IS NOT NULL AND pack_length IS NOT NULL)
  ) NOT VALID;
ALTER TABLE objects VALIDATE CONSTRAINT objects_pack_member;

-- «Кто лежит в этой пачке» — запрос планировщика уплотнения и проверки
-- инварианта live_bytes. Частичный индекс: участников у крупных объектов нет.
CREATE INDEX objects_pack ON objects (pack_id) WHERE pack_id IS NOT NULL;

-- +goose Down
DROP INDEX IF EXISTS objects_pack;
ALTER TABLE objects
  DROP CONSTRAINT IF EXISTS objects_pack_member,
  DROP COLUMN IF EXISTS pack_length,
  DROP COLUMN IF EXISTS pack_offset,
  DROP COLUMN IF EXISTS pack_id;
DROP TABLE IF EXISTS packs;
DROP TYPE IF EXISTS pack_state;
DROP TYPE IF EXISTS pack_kind;
