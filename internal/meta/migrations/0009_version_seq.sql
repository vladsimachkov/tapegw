-- +goose Up
-- Порядок версий одного ключа (этап V: ListObjectVersions).
--
-- Листингу версий нужен строгий порядок «от новой к старой» и пагинация по
-- маркеру. Существующие колонки для этого не годятся:
--
--   * id — случайный UUID, порядок не несёт;
--   * created_at — now() в Postgres это время НАЧАЛА транзакции, поэтому два
--     параллельных PUT одного ключа получают одинаковое время. Порядок по
--     (created_at, id) тогда произволен внутри совпадения, а пагинация на
--     произвольном порядке теряет или дублирует версии.
--
-- version_seq — монотонный номер версии, назначаемый при вставке. Он задаёт
-- порядок, на нём строится маркер пагинации, и «текущая версия» ключа совпадает
-- с максимальным version_seq этого ключа.
--
-- Последовательность одна на всю таблицу, а не на ключ: номера сравниваются
-- только внутри ключа, а общая последовательность не требует отдельной блокировки
-- ключа при вставке.
CREATE SEQUENCE objects_version_seq;

ALTER TABLE objects
  ADD COLUMN version_seq bigint;

-- Последовательность привязана к колонке: она удаляется вместе с ней и
-- правильно переносится pg_dump/restore (иначе на восстановленной базе
-- nextval() смотрит в последовательность, которая никому не принадлежит).
ALTER SEQUENCE objects_version_seq OWNED BY objects.version_seq;

-- Старый индекс 0005 на (bucket_id, key COLLATE "C") по всем строкам. Новый
-- objects_key_versions начинается с тех же колонок и покрывает все запросы
-- старого, а четвёртый индекс на те же колонки — это четвёртая запись на каждый
-- PUT. В Down он возвращается дословно так, как был создан в 0005.
DROP INDEX objects_versions;

-- Порядок существующих строк задаём по (created_at, id): лучше, чем ничего, и
-- единственный доступный. На живой базе это компромисс, а не истина — но к
-- моменту включения версионирования строк здесь быть не должно, а тест ниже
-- проверяет именно назначенный порядок, а не угаданный.
WITH ranked AS (
  SELECT ctid, row_number() OVER (ORDER BY created_at, id) AS rn
  FROM objects
)
UPDATE objects
   SET version_seq = ranked.rn
  FROM ranked
 WHERE objects.ctid = ranked.ctid;

-- Следующий номер — на единицу больше максимального использованного. setval с
-- is_called = false: следующий nextval вернёт ровно это значение.
SELECT setval('objects_version_seq',
              (SELECT COALESCE(max(version_seq), 0) + 1 FROM objects), false);

ALTER TABLE objects
  ALTER COLUMN version_seq SET DEFAULT nextval('objects_version_seq');

-- NOT NULL отдельным шагом: на колонке с NULL-ами ALTER SET NOT NULL упал бы, а
-- default сам по себе старые строки не заполняет.
ALTER TABLE objects
  ALTER COLUMN version_seq SET NOT NULL;

-- Листинг версий: ключ + порядок версий внутри него. DESC — потому что S3
-- отдаёт версии от новой к старой, и обход по индексу должен идти в том же
-- направлении, иначе сортировка ляжет на каждую страницу.
CREATE INDEX objects_key_versions
  ON objects (bucket_id, key COLLATE "C", version_seq DESC);

-- +goose Down
DROP INDEX IF EXISTS objects_key_versions;
ALTER TABLE objects DROP COLUMN IF EXISTS version_seq;
DROP SEQUENCE IF EXISTS objects_version_seq;

-- Возврат индекса из 0005 — тем же определением, каким он был создан.
CREATE INDEX objects_versions ON objects (bucket_id, key COLLATE "C");
