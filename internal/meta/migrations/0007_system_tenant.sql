-- +goose Up
-- T1 (PR#2a): системный тенант — id = 1 закрыт для клиентов (ADR-0018).
--
-- Порядок шагов a → b → c → d не «желательный», он единственно возможный, и
-- ниже он зафиксирован прямо в тексте:
--
--   * чек (b) обязан идти ПОСЛЕ UPDATE из (a): на строке с id = 1 и
--     system = false он не проходит валидации, и ALTER падает с
--     `check constraint "tenants_default_is_system" ... is violated by some row`
--     (проверено на живом Postgres, см. раздел «Проверка» в ADR-0018);
--   * колонку (c) нельзя добавлять сразу NOT NULL: на непустой `accounts` она
--     была бы заполнена NULL, и ALTER упал бы ещё до бэкфилла —
--     `column "tenant_system" of relation "accounts" contains null values`.
--     Отсюда три шага: nullable → бэкфилл → SET NOT NULL.
--
-- Всё в одной транзакции (goose оборачивает Up целиком): составной FK из (d)
-- не выполнится, если уже лежащие строки accounts получат флаг, не совпадающий
-- с флагом тенанта.

-- (a) Признак системности — данные, а не константа в коде. Ровно один тенант
-- с system = true по факту ниже: чек (b) закрепляет это для id = 1.
ALTER TABLE tenants ADD COLUMN system boolean NOT NULL DEFAULT false;

UPDATE tenants SET system = true WHERE id = 1;

-- Пара (id, system) — цель составного FK из (d). Отдельного индекса не
-- требуется: UNIQUE его и создаёт.
ALTER TABLE tenants ADD UNIQUE (id, system);

-- (b) Снять флаг с id = 1 нельзя: иначе каскад ON UPDATE CASCADE честно
-- обновит tenant_system у admin-учёток, в тенант 1 заведётся клиент, и все
-- бакеты, созданные root, станут его. Это ровно тот сценарий, ради которого
-- ADR и пишется.
ALTER TABLE tenants ADD CONSTRAINT tenants_default_is_system
  CHECK (id <> 1 OR system);

-- (c) Денормализация флага в accounts: нужна, чтобы правило «не-admin не может
-- принадлежать системному тенанту» выразилось декларативно, без триггера и без
-- межтабличного запроса. Бэкфилл читаем из tenants — другого источника нет.
ALTER TABLE accounts ADD COLUMN tenant_system boolean;

UPDATE accounts a SET tenant_system = t.system
  FROM tenants t WHERE t.id = a.tenant_id;

ALTER TABLE accounts ALTER COLUMN tenant_system SET NOT NULL;

-- (d) Две проверки, обе видны прямо в \d accounts:
--
--   * составной FK попадает в UNIQUE (id, system) только при ТОЧНОМ совпадении
--     флага с тенантом, так что записать учётку с неверным флагом нельзя ни в
--     какую сторону;
--   * чек запрещает не-admin учётку в системном тенанте. Роль admin тенантам
--     не выдаётся никогда (ADR-0009), поэтому «admin» здесь — про системных
--     операторов, а не про лазейку.
--
-- ON UPDATE CASCADE держит денормализацию в согласии при смене system у
-- тенанта. Для системного тенанта это событие исключительное, и админ-API
-- смены флага обязан отказывать, если у тенанта есть бакеты (ADR-0018).
ALTER TABLE accounts
  ADD CONSTRAINT accounts_tenant_fkey
    FOREIGN KEY (tenant_id, tenant_system) REFERENCES tenants (id, system)
    ON UPDATE CASCADE,
  ADD CONSTRAINT accounts_not_system_unless_admin
    CHECK (role = 'admin' OR NOT tenant_system);

-- +goose Down
-- Откат — строгий обратный порядок: сначала ограничения accounts, затем
-- колонка accounts, затем чеки и колонка tenants. Иначе DROP COLUMN оставил бы
-- висящиеся ограничения, а DROP tenants.system сломал бы FK из (d).
ALTER TABLE accounts DROP CONSTRAINT IF EXISTS accounts_not_system_unless_admin;
ALTER TABLE accounts DROP CONSTRAINT IF EXISTS accounts_tenant_fkey;
ALTER TABLE accounts DROP COLUMN IF EXISTS tenant_system;

ALTER TABLE tenants DROP CONSTRAINT IF EXISTS tenants_default_is_system;
ALTER TABLE tenants DROP CONSTRAINT IF EXISTS tenants_id_system_key;
ALTER TABLE tenants DROP COLUMN IF EXISTS system;
