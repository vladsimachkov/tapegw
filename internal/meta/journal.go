package meta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Журнал метаданных (ADR-0023).
//
// Манифест описывает версию в момент записи и больше не меняется, а метаданные
// после записи правятся. Всё, что правится, обязано оставить событие — иначе
// `rebuild-meta` после аварии вернёт состояние, которого не было: продленный
// COMPLIANCE снова станет удаляемым по старому сроку, а ключ из-под маркера —
// живым.
//
// Два правила, на которых держится весь пакет, заданы не удобством:
//
//  1. Событие пишется **в той же транзакции, что и изменение**. Отдельный шаг
//     после коммита оставил бы окно «изменение есть, события нет», и падение в
//     нём потеряло бы блокировку молча. Поэтому appendEvent принимает tx
//     вызывающего и не умеет открывать свою.
//  2. Порядок внутри тенанта даёт счётчик на строке тенанта, а не sequence
//     Postgres: последовательность не откатывается, и откатившаяся операция
//     оставила бы дырку, а дырка в журнале — это «потерянное событие».
//
// Номера плотные по построению, и `rebuild-meta` вправе читать любой пропуск
// как аварию, а не как потерянное событие.
//
// Про правило 2 есть поправка: оно описывает сегодняшнее состояние кода, а на Ж2
// способ выдачи номера меняется (служебный id при записи, плотный номер — на
// запечатывании сегмента). Основание, условия и цену перехода читать в
// docs/adr/0023-journal-events-same-tx.md, раздел «Измерение цены» — там же
// перечислено, что обязано измениться вместе с этим.

// Виды событий — значения enum journal_event_kind (миграция 0027).
const (
	// JournalKindRetention — изменён retention версии.
	JournalKindRetention = "retention"
	// JournalKindLegalHold — legal hold включён или снят.
	JournalKindLegalHold = "legal_hold"
	// JournalKindDeleteMarker — маркер удаления создан или убран.
	JournalKindDeleteMarker = "delete_marker"
	// JournalKindDeleteVersion — версия ушла в 'deleting'.
	JournalKindDeleteVersion = "delete_version"
	// JournalKindBucketConfig — снимок конфигурации бакета после изменения.
	JournalKindBucketConfig = "bucket_config"
)

// Payload'ы событий. Имена полей — часть формата: события пойдут на ленту
// внутри зашифрованного сегмента и будут читаться при восстановлении, поэтому
// переименование поля — это изменение формата, а не рефакторинг.
//
// Имя бакета и ключ здесь намеренно: после восстановления бакет получает другой
// id, и сопоставить событие бакету можно только по имени. Открытым это не
// делает ничего — в Postgres эти имена и так лежат в buckets, а на ленту
// пойдёт шифртекст (ADR-0020 §3).

// JournalRetention — PutObjectRetention. Until обязан быть в моменте, а не в
// «осталось столько-то»: событие перечитывают через годы после записи.
type JournalRetention struct {
	Bucket string    `json:"bucket"`
	Key    string    `json:"key"`
	Mode   string    `json:"mode"`
	Until  time.Time `json:"until"`
}

// JournalLegalHold — PutObjectLegalHold.
type JournalLegalHold struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
	On     bool   `json:"on"`
}

// JournalDeleteMarker — маркер удаления. Object_id — id самого маркера.
type JournalDeleteMarker struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
	// NullVersion — маркер с versionId "null" (бакет suspended).
	NullVersion bool `json:"null_version,omitempty"`
	// Removed — маркер убран (возврат удалённого объекта), а не создан. Без
	// этого флага применение журнала оставило бы фантомный маркер там, где
	// клиент уже вернул объект.
	Removed bool `json:"removed,omitempty"`
}

// JournalDeleteVersion — версия ушла в 'deleting'. Object_id — сама версия.
type JournalDeleteVersion struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
	// Bypass — удаление принято с обходом GOVERNANCE (миграция 0026). На
	// ленте это единственное место, где видно, что удаление было досрочным:
	// отметка на строке до восстановления не доживёт.
	Bypass bool `json:"bypass,omitempty"`
}

// JournalBucketConfig — снимок конфигурации бакета целиком, а не дифф:
// применять диффы при восстановлении значит хранить порядок и историю полей,
// а снимок применяется идемпотентно повтором.
//
// Состав — ровно то, без чего бакет после восстановления был бы непригоден:
// версионирование, Object Lock с правилом по умолчанию, владелец, ACL,
// политика, признак удаления. Теги в снимок не входят сознательно: на
// сохранность и доступ они не влияют, а их потеря — косметика, а не потеря
// данных (ADR-0023 п. 5).
type JournalBucketConfig struct {
	Bucket      string `json:"bucket"`
	Versioning  string `json:"versioning"`
	ObjectLock  bool   `json:"object_lock"`
	LockConfig  string `json:"lock_config,omitempty"`
	Owner       string `json:"owner"`
	OwnerTenant int64  `json:"owner_tenant"`
	// ACL отдаётся как json.RawMessage: колонка jsonb, и пересобирать её
	// структурой значило бы потерять гранты, которые мы не знаем.
	ACL json.RawMessage `json:"acl,omitempty"`
	// Policy — текст колонки policy: документ обязан быть JSON, но отдаём мы
	// его дословно (миграция 0006), поэтому и в снимке он строка.
	Policy string `json:"policy,omitempty"`
	// Deleted — бакет удалён (DeleteBucket мягко).
	Deleted bool `json:"deleted,omitempty"`
}

// JournalEvent — строка journal_events.
type JournalEvent struct {
	TenantID  int64
	Seq       int64
	Kind      string
	ObjectID  *uuid.UUID
	BucketID  int64
	Payload   json.RawMessage
	CreatedAt time.Time
}

// Retention расшифровывает payload события retention.
func (e JournalEvent) Retention() (JournalRetention, error) {
	var p JournalRetention
	err := json.Unmarshal(e.Payload, &p)
	return p, err
}

// LegalHold расшифровывает payload события legal hold.
func (e JournalEvent) LegalHold() (JournalLegalHold, error) {
	var p JournalLegalHold
	err := json.Unmarshal(e.Payload, &p)
	return p, err
}

// DeleteMarker расшифровывает payload события маркера.
func (e JournalEvent) DeleteMarker() (JournalDeleteMarker, error) {
	var p JournalDeleteMarker
	err := json.Unmarshal(e.Payload, &p)
	return p, err
}

// DeleteVersion расшифровывает payload события удаления версии.
func (e JournalEvent) DeleteVersion() (JournalDeleteVersion, error) {
	var p JournalDeleteVersion
	err := json.Unmarshal(e.Payload, &p)
	return p, err
}

// Config расшифровывает payload снимка конфигурации бакета.
func (e JournalEvent) Config() (JournalBucketConfig, error) {
	var p JournalBucketConfig
	err := json.Unmarshal(e.Payload, &p)
	return p, err
}

// uuidPtr — указатель на uuid для колонки, где версия может отсутствовать.
func uuidPtr(id uuid.UUID) *uuid.UUID { return &id }

// appendEvent вставляет событие журнала в транзакцию изменения.
//
// Номер выдаётся здесь же и обязательно под блокировкой строки тенанта:
//
//	UPDATE tenants SET journal_seq = journal_seq + 1 ... RETURNING
//
// Это не «обновить счётчик», а способ взять строку на запись: два параллельных
// изменения одного тенанта сериализуются на этой строке и получают соседние
// номера, а откатившееся изменение откатывает и номер — дырки не возникает.
// Sequence Postgres на этом месте выглядит эквивалентом и не является им:
// nextval не откатывается, и откатившаяся операция оставила бы пропуск, а
// пропуск в журнале rebuild-meta обязан читать как потерю события.
//
// tenant обязан браться из строки (бакета или версии), а не из учётки запроса:
// под обходом по роли страж отдаёт «все тенанты», и событие уехало бы в
// тенанта, которого нет (та же форма ошибки, что с DEK на PR #127).
//
// Порядок блокировок: строка тенанта всегда берётся последней — после
// advisory-лока ключа, бакета и строк objects. Все места журнала следуют ему,
// иначе два изменения одного ключа обменяются локами и встанут в тупик.
func appendEvent(ctx context.Context, tx pgx.Tx, tenant int64, kind string,
	objectID *uuid.UUID, bucketID int64, payload any) error {

	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("journal payload %s: %w", kind, err)
	}

	var seq int64
	err = tx.QueryRow(ctx,
		`UPDATE tenants SET journal_seq = journal_seq + 1
		  WHERE id = $1 RETURNING journal_seq`, tenant).Scan(&seq)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("journal seq: тенант %d не найден", tenant)
	}
	if err != nil {
		return fmt.Errorf("journal seq tenant %d: %w", tenant, err)
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO journal_events (tenant_id, journal_seq, kind, object_id, bucket_id, payload)
		 VALUES ($1, $2, $3::journal_event_kind, $4, $5, $6::jsonb)`,
		tenant, seq, kind, objectID, bucketID, string(raw)); err != nil {
		return fmt.Errorf("journal event %s: %w", kind, err)
	}
	return nil
}

// bucketConfigSnapshot читает конфигурацию бакета для снимка. Запрос обязан
// идти внутри той же транзакции, что изменила бакет: снимок «как было до»
// бесполезен, а прочитанный позже — враньё при параллельном изменении.
func bucketConfigSnapshot(ctx context.Context, tx pgx.Tx, bucketID int64) (JournalBucketConfig, error) {
	var (
		cfg             JournalBucketConfig
		lockConfig, acl *string
		policy          *string
	)
	err := tx.QueryRow(ctx,
		`SELECT name, versioning::text, object_lock_enabled, lock_config, owner,
		        tenant_id, acl::text, policy, deleted_at IS NOT NULL
		   FROM buckets WHERE id = $1`, bucketID).
		Scan(&cfg.Bucket, &cfg.Versioning, &cfg.ObjectLock, &lockConfig, &cfg.Owner,
			&cfg.OwnerTenant, &acl, &policy, &cfg.Deleted)
	if errors.Is(err, pgx.ErrNoRows) {
		return cfg, ErrNotFound
	}
	if err != nil {
		return cfg, fmt.Errorf("snapshot bucket %d: %w", bucketID, err)
	}
	if lockConfig != nil {
		cfg.LockConfig = *lockConfig
	}
	if acl != nil {
		cfg.ACL = json.RawMessage(*acl)
	}
	if policy != nil {
		cfg.Policy = *policy
	}
	return cfg, nil
}

// appendBucketConfig вставляет снимок конфигурации бакета. Тенант берётся из
// самой строки бакета — вызывающий может быть с обходом по роли.
func appendBucketConfig(ctx context.Context, tx pgx.Tx, bucketID int64) error {
	cfg, err := bucketConfigSnapshot(ctx, tx, bucketID)
	if err != nil {
		return err
	}
	return appendEvent(ctx, tx, cfg.OwnerTenant, JournalKindBucketConfig, nil, bucketID, cfg)
}

// JournalEventsAfter читает события тенанта по порядку после afterSeq.
//
// Этим же читает себя будет `rebuild-meta`, и лимт здесь не украшение: журнал
// за годы — это миллионы событий, и применять их надо порциями.
func (m *Meta) JournalEventsAfter(ctx context.Context, tenant int64,
	afterSeq int64, limit int) ([]JournalEvent, error) {

	if limit <= 0 {
		limit = 1000
	}
	rows, err := m.Pool.Query(ctx,
		`SELECT tenant_id, journal_seq, kind::text, object_id, bucket_id, payload, created_at
		   FROM journal_events
		  WHERE tenant_id = $1 AND journal_seq > $2
		  ORDER BY journal_seq
		  LIMIT $3`, tenant, afterSeq, limit)
	if err != nil {
		return nil, fmt.Errorf("journal events tenant %d: %w", tenant, err)
	}
	defer rows.Close()

	var out []JournalEvent
	for rows.Next() {
		var e JournalEvent
		if err := rows.Scan(&e.TenantID, &e.Seq, &e.Kind, &e.ObjectID,
			&e.BucketID, &e.Payload, &e.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan journal event: %w", err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate journal events: %w", err)
	}
	return out, nil
}

// JournalSeq — текущий номер последнего события тенанта. Нужен тестам и
// воркеру сегментов: закрытый сегмент обязан захватывать ровно диапазон до
// этого номера включительно.
func (m *Meta) JournalSeq(ctx context.Context, tenant int64) (int64, error) {
	var seq int64
	err := m.Pool.QueryRow(ctx, `SELECT journal_seq FROM tenants WHERE id = $1`, tenant).Scan(&seq)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("journal seq tenant %d: %w", tenant, err)
	}
	return seq, nil
}
