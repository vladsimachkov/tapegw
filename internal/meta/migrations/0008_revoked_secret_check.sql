-- +goose Up
-- M1 (PR#2b follow-up): связка «статус ↔ секрет» в схеме.
--
-- После миграции 0007 колонка secret_wrapped стала nullable — она означает
-- «строка отозвана, секрета больше нет». Но nullable сама по себе не мешает
-- активной учётке остаться без секрета (тогда вход по ней падает с
-- неразборчивой ошибкой расшифровки) или отозванной — остаться с секретом
-- (материал для офлайн-подбора лежит в базе без надобности).
--
-- Ограничение декларативное и двустороннее: revoked без секрета, активный —
-- с секретом. Триггеров нет, правило видно в \d accounts.
ALTER TABLE accounts
  ADD CONSTRAINT accounts_revoked_has_no_secret
  CHECK ((status = 'revoked') = (secret_wrapped IS NULL));

-- +goose Down
ALTER TABLE accounts DROP CONSTRAINT IF EXISTS accounts_revoked_has_no_secret;
