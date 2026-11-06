-- +goose Up
-- Продолжение запечатанной пачки после перезапуска.

-- Пачка в 'sealed' — это «адреса известны, отправка не завершена». После
-- перезапуска процесса migrator обязан сам найти такие пачки: объекты в
-- 'archiving' с pack_id в батч не попадут (ClaimMigrationBatch берёт только
-- 'pending'), а RequeueArchiving их намеренно пропускает.
--
-- Захват — отметка в строке пачки, а не блокировка на время отправки: запись на
-- ленту идёт минуты, и держать локи столько означало бы нарушить правило 13.
-- Поэтому «взято процессом до такого-то времени», а блокировка (FOR UPDATE SKIP
-- LOCKED) живёт только внутри короткой транзакции захвата.
ALTER TABLE packs ADD COLUMN claimed_by text;
ALTER TABLE packs ADD COLUMN claim_expires_at timestamptz;

-- Истёкший захват равен «никто не держит»: упавший процесс не должен навсегда
-- спрятать пачку от остальных.
ALTER TABLE packs ADD CONSTRAINT packs_claim_pair CHECK (
  (claimed_by IS NULL) = (claim_expires_at IS NULL)
);

CREATE INDEX packs_sealed ON packs (created_at) WHERE state = 'sealed';

-- +goose Down
DROP INDEX IF EXISTS packs_sealed;
ALTER TABLE packs DROP CONSTRAINT IF EXISTS packs_claim_pair;
ALTER TABLE packs DROP COLUMN IF EXISTS claim_expires_at;
ALTER TABLE packs DROP COLUMN IF EXISTS claimed_by;
