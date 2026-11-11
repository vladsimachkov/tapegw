-- +goose Up
-- Сегменты журнала метаданных (ADR-0023 п. 3–5, ADR-0020 §3) — Ж2 шаг 2.
--
-- Событие после 0028 носит только служебный id с дырками от откатов. Плотный
-- номер, по которому rebuild-meta применяет журнал по порядку и видит пропуск,
-- даёт **диапазон запечатанного сегмента**: воркер запечатывания берёт
-- не запечатанные события тенанта, выдаёт им следующий кусок плотной нумерации
-- и оставляет признак «уже запечатано» — отдельной таблицей, потому что
-- помечать UPDATE-ом само событие нельзя (ADR-0023 п. 4: UPDATE противоречит
-- триггеру «только на добавление» и обоснованию миграции 0027).
--
-- Здесь только учётная часть: нумерация, признак и проверка полноты. Сам
-- объект сегмента (шифротекст под TEK тенанта, AAD из uuid, очередь mover'у и
-- пачки журнала) — следующий шаг, и колонки состояния («не записан / в очереди /
-- на ленте») появятся там же: заводить их сейчас означало бы хранить состояние,
-- которое никто не читает и потому не проверяет.

CREATE TABLE journal_segments (
  id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  -- UUID сегмента — AAD при шифровании (ADR-0020 §3): сегмент нельзя вставить
  -- в другой тенант. Выдаёт схема, а не код, — та же договорённость, что с
  -- id события после 0028: «у сегмента есть uuid» перестаёт быть уговором.
  uuid          uuid NOT NULL UNIQUE DEFAULT gen_random_uuid(),
  tenant_id     bigint NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
  -- Плотный диапазон этого сегмента. Длина обязана равняться числу событий:
  -- это и есть контроль пропуска, по нему rebuild-meta сверяет смежность
  -- диапазонов одного тенанта и на дырке останавливается, а не применяет
  -- неполный журнал молча.
  seq_first     bigint NOT NULL CHECK (seq_first > 0),
  seq_last      bigint NOT NULL CHECK (seq_last >= seq_first),
  event_count   int    NOT NULL CHECK (event_count > 0),
  CHECK (seq_last - seq_first + 1 = event_count),
  -- Самый большой id события в сегменте. По нему следующий цикл понимает,
  -- откуда читать («события дальше рубежа»), и по нему же ищет опоздавших.
  event_id_last bigint NOT NULL,
  created_at    timestamptz NOT NULL DEFAULT now(),
  -- Два сегмента одного тенанта не могут начинаться с одного номера: это
  -- означало бы, что плотный номер выдали дважды. Ключ держит это схемой, а не
  -- воркером — как UNIQUE (event_id) ниже.
  UNIQUE (tenant_id, seq_first)
);

-- Чтение цепочки сегментов тенанта по порядку («следующий после рубежа») и
-- рубежа по max(event_id_last). DESC — потому что рубеж ищут с конца.
CREATE INDEX journal_segments_tenant_event ON journal_segments (tenant_id, event_id_last DESC);

COMMENT ON COLUMN journal_segments.seq_first IS
    'первый плотный номер диапазона сегмента; выдаёт воркер запечатывания (ADR-0023, поправка)';
COMMENT ON COLUMN journal_segments.event_id_last IS
    'максимальный journal_events.id в сегменте; рубеж следующей выборки и поиска опоздавших';

CREATE TABLE journal_segment_event (
  segment_id bigint NOT NULL REFERENCES journal_segments (id),
  event_id   bigint NOT NULL REFERENCES journal_events (id),
  -- «Каждое событие ровно в одном сегменте» (ADR-0023 п. 5) держится схемой:
  -- повторное запечатывание того же события невозможно в принципе, а не
  -- «воркер не должен». Именно это и делает признак признаком.
  UNIQUE (event_id),
  PRIMARY KEY (segment_id, event_id)
);

-- Признак «запечатано» тоже только на добавление: снять его — значит разрешить
-- запечатать событие второй раз под другим плотным номером.
CREATE TRIGGER journal_segment_event_append_only
  BEFORE UPDATE OR DELETE ON journal_segment_event
  FOR EACH ROW EXECUTE FUNCTION tapegw_journal_append_only();

-- +goose Down
-- Порядок обратный Up: сначала признак, потом сегменты (на них FK событий).
DROP TRIGGER journal_segment_event_append_only ON journal_segment_event;
DROP TABLE journal_segment_event;
DROP TABLE journal_segments;
