-- +goose Up
-- Служебный id события журнала вместо плотного номера (ADR-0023, поправка).
--
-- Пока номер выдавал счётчик на строке тенанта, все журналируемые изменения
-- одного тенанта сериализовались на этой строке: лок держится до коммита,
-- транзакции выстраиваются в очередь и каждая делает свой fsync в одиночестве.
-- Измерено (internal/meta/journal_throughput_test.go): ≈1000 продлений и ≈900
-- удалений в секунду на тенанта, из них x9…x10 — именно эта строка. Для
-- бэкап-софта это потолок: чистка устаревших копий — миллионы ключей, и пока
-- она идёт, одиночное продление той же учётки ждёт в той же очереди.
--
-- Здесь событие получает только служебный id, выданный sequence, и строку
-- тенанта больше не трогает. Пропуски в этих id — норма (откатившаяся
-- транзакция оставляет дырку), и это НЕ потеря события: плотность определяется
-- не таблицей событий, а диапазоном запечатанного сегмента, плотный
-- journal_seq выдаёт воркер сегментов при запечатывании (Ж2, следующий шаг).
--
-- Порядок событий одного объекта при этом сохраняется, и держится он не на
-- номере: изменения одного объекта сериализованы блокировкой его строки, а id
-- присваивается на вставке — то есть после того, как блокировка взята. Порядок
-- событий разных объектов при восстановлении не важен.
--
-- Пра́вило «событие в той же транзакции, что и изменение» (ADR-0023 п. 1) это
-- не касается: вставка остаётся в транзакции изменения, откат по-прежнему
-- откатывает событие.

-- Служебный id. ALWAYS, а не BY DEFAULT: подменить его снаружи нельзя, и
-- «у события есть id, выданный схемой» перестаёт быть договорённостью.
ALTER TABLE journal_events DROP CONSTRAINT journal_events_pkey;

ALTER TABLE journal_events ADD COLUMN id bigint
  GENERATED ALWAYS AS IDENTITY;

ALTER TABLE journal_events ADD PRIMARY KEY (id);

-- Чтение «события тенанта по порядку выдачи» — по нему воркер сегментов
-- запечатывает диапазон и по нему же читает себя rebuild-meta.
CREATE INDEX journal_events_tenant_id ON journal_events (tenant_id, id);

-- Плотный номер с этого момента живёт на сегменте, а не на событии.
ALTER TABLE journal_events DROP COLUMN journal_seq;

COMMENT ON COLUMN tenants.journal_seq IS
    'последний выданный тенанту плотный номер журнала; пишет воркер сегментов при запечатывании (ADR-0023, поправка)';

-- +goose Down
-- Возврат плотной нумерации на таблице с данными требует перенумерации, а
-- UPDATE по journal_events закрыт триггером «только на добавление». Обход
-- через tapegw.journal_override здесь не подходит: он сеансовый и пережил бы
-- коммит миграции, а пережитый обход — это выключенная защита на всём
-- соединении. Поэтому триггер на время перенумерации снимается и ставится
-- обратно: состояние после Down ровно то, что оставил Up миграции 0027.
DROP TRIGGER journal_events_append_only ON journal_events;

ALTER TABLE journal_events ADD COLUMN journal_seq bigint CHECK (journal_seq > 0);

-- Номера плотные по построению, поэтому восстанавливаются порядком id внутри
-- тенанта: смысл плотной нумерации — «соседние», а не «совпали с прежними».
WITH ranked AS (
    SELECT ctid, row_number() OVER (PARTITION BY tenant_id ORDER BY id) AS rn
      FROM journal_events
)
UPDATE journal_events AS je
   SET journal_seq = ranked.rn
  FROM ranked
 WHERE je.ctid = ranked.ctid;

ALTER TABLE journal_events ALTER COLUMN journal_seq SET NOT NULL;
ALTER TABLE journal_events DROP CONSTRAINT journal_events_pkey;
ALTER TABLE journal_events ADD PRIMARY KEY (tenant_id, journal_seq);
DROP INDEX journal_events_tenant_id;
ALTER TABLE journal_events DROP COLUMN id;

CREATE TRIGGER journal_events_append_only
  BEFORE UPDATE OR DELETE ON journal_events
  FOR EACH ROW EXECUTE FUNCTION tapegw_journal_append_only();

COMMENT ON COLUMN tenants.journal_seq IS
    'номер последнего события журнала тенанта; выдаётся под блокировкой строки в транзакции изменения (ADR-0023)';
