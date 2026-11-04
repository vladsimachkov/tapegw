-- +goose Up
-- T2 PR#1: шифрование данных (ADR-0014).
--
-- Два изменения, оба про ключи:
--
--   (a) tenants.key_destroyed_at — признак «ключ уничтожен навсегда»;
--   (b) objects.wrapped_dek обязана быть у каждой версии с данными.
--
-- Про (a). До этого «ключа нет» выражалось одним способом — NULL в
-- tenants.wrapped_key, и этим же способом было записано «ключ уничтожен».
-- Разницы для кода не получалось: EnsureTenantKey при NULL заводит новый ключ,
-- и первая же запись после крипто-уничтожения молча поднимала тенанта из
-- мёртвых с новым TEK. Данные, которые считали уничтоженными, оставались бы
-- читаемыми, то есть шифрование не давало ничего. Поэтому уничтожение теперь
-- отдельное состояние, а не отсутствие значения.
--
-- NOT NULL на wrapped_key здесь не ставится: миграция KEK не знает и обёртку
-- построить не может, а ленивое заведение (первый PUT) оставляет NULL
-- нормальным состоянием для тенантов, у которых ещё ничего не записано.

-- (a) Явный признак уничтожения ключа.
ALTER TABLE tenants ADD COLUMN key_destroyed_at timestamptz;

-- Уничтоженный ключ не может одновременно лежать в строке: иначе «уничтожение»
-- оставило бы обёртку, которую кто-нибудь когда-нибудь развернёт.
ALTER TABLE tenants ADD CONSTRAINT tenants_destroyed_key_empty
  CHECK (key_destroyed_at IS NULL OR wrapped_key IS NULL);

-- (b) Проверка идёт по данным, а не «на пустой схеме»: на пустой порядок шагов
-- не имеет значения и миграция выглядит надёжнее, чем она есть (правило 11).
-- +goose StatementBegin
DO $$
DECLARE n bigint;
BEGIN
  SELECT count(*) INTO n FROM objects
    WHERE wrapped_dek IS NULL AND NOT is_delete_marker;
  IF n > 0 THEN
    RAISE EXCEPTION
      'в objects есть % незашифрованных версий: после T2 они не читаются в '
      'принципе, поэтому их удаляет человек, а не миграция', n;
  END IF;
END $$;
-- +goose StatementEnd

-- Маркер удаления тела не имеет (size = 0 по CHECK из 0005), ключ ему не нужен.
ALTER TABLE objects ADD CONSTRAINT objects_dek_present
  CHECK (is_delete_marker OR wrapped_dek IS NOT NULL);

-- +goose Down
ALTER TABLE objects DROP CONSTRAINT IF EXISTS objects_dek_present;
ALTER TABLE tenants DROP CONSTRAINT IF EXISTS tenants_destroyed_key_empty;
ALTER TABLE tenants DROP COLUMN IF EXISTS key_destroyed_at;
