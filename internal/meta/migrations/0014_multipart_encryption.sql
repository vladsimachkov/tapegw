-- +goose Up
-- T2 PR#2: multipart с шифрованием частей.
--
--   (a) uploads.wrapped_dek — DEK загрузки: части шифруются одним ключом, а
--       номер части входит в nonce и AAD;
--   (b) upload_parts.file_id — файл части в спуле;
--   (c) object_parts.cipher_offset становится NOT NULL.
--
-- Все три колонки добавляются как NOT NULL без DEFAULT, и это работает только на
-- пустых таблицах. Пусты они потому, что multipart раньше не был реализован
-- вовсе: методы приходили из BackendUnsupported, и вставить строку было некому.
-- Если в какой-то базе они всё-таки непустые, значит там уже есть multipart —
-- и тогда старые строки не расшифровать (DEK не заводился), поэтому их удаляет
-- человек, а не миграция. Проверка на данных, а не «на пустой схеме» (правило 11).

-- (a) DEK загрузки.
-- +goose StatementBegin
DO $$
DECLARE n bigint;
BEGIN
  SELECT count(*) INTO n FROM uploads;
  IF n > 0 THEN
    RAISE EXCEPTION
      'в uploads % строк незавершённых multipart-загрузок: DEK для них не '
      'заводился, после T2 они не читаются — удаляет их человек, а не миграция', n;
  END IF;
END $$;
-- +goose StatementEnd

ALTER TABLE uploads ADD COLUMN wrapped_dek bytea NOT NULL;

-- (a2) UUID будущей версии.
--
-- nonce и AAD каждого сегмента строятся от UUID версии, а части загружаются до
-- того, как версия появится. Значит UUID обязан быть известен на
-- CreateMultipartUpload и пережить её до Complete, где он станет objects.id.
-- Иначе части были бы зашифрованы одним идентификатором, а читались под другим.
ALTER TABLE uploads ADD COLUMN object_id uuid NOT NULL;

-- (b) Файл части в спуле. Части лежат отдельными файлами до Complete, где
-- склеиваются в файл объекта.
ALTER TABLE upload_parts ADD COLUMN file_id uuid NOT NULL;

-- (c) Смещение шифротекста части в итоговом файле. NULL означал бы «не знаю,
-- где читать», а это не состояние, а ошибка.
-- +goose StatementBegin
DO $$
DECLARE n bigint;
BEGIN
  SELECT count(*) INTO n FROM object_parts WHERE cipher_offset IS NULL;
  IF n > 0 THEN
    RAISE EXCEPTION
      'в object_parts % строк без cipher_offset: без него Range по '
      'зашифрованному multipart не вычислит нужный сегмент', n;
  END IF;
END $$;
-- +goose StatementEnd

ALTER TABLE object_parts ALTER COLUMN cipher_offset SET NOT NULL;

-- Загрузки одного бакета и одного ключа выбираются часто (ListMultipartUploads,
-- janitor по mpu_ttl).
CREATE INDEX uploads_bucket_key ON uploads (bucket_id, key);

-- +goose Down
DROP INDEX IF EXISTS uploads_bucket_key;
ALTER TABLE object_parts ALTER COLUMN cipher_offset DROP NOT NULL;
ALTER TABLE upload_parts DROP COLUMN IF EXISTS file_id;
ALTER TABLE uploads DROP COLUMN IF EXISTS object_id;
ALTER TABLE uploads DROP COLUMN IF EXISTS wrapped_dek;
