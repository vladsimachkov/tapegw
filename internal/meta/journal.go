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
// Три правила, на которых держится весь пакет, заданы не удобством:
//
//  1. Событие пишется **в той же транзакции, что и изменение**. Отдельный шаг
//     после коммита оставил бы окно «изменение есть, события нет», и падение в
//     нём потеряло бы блокировку молча. Поэтому appendEvent принимает tx
//     вызывающего и не умеет открывать свою.
//  2. У события есть только **служебный id** из sequence; строку тенанта
//     запись не трогает. Счётчик на строке тенанта сериализовал бы все
//     журналируемые изменения тенанта (≈1000 в секунду и очередь за массовой
//     операцией — см. ADR-0023, «Измерение цены»), а плотность всё равно даёт не
//     таблица событий: плотный journal_seq выдаёт воркер сегментов при
//     запечатывании, и пропуски служебных id от откатов — норма.
//  3. Служебный id присваивается на вставке, то есть **после** того, как
//     вызывающий взял блокировку строки объекта. Только поэтому порядок событий
//     одного объекта совпадает с порядком коммита: изменения одного объекта
//     сериализованы его строкой. Взять id до блокировки — значит дать
//     восстановлению применить retention не в том порядке.
//
// Порядок событий разных объектов при восстановлении не важен.

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
	// ID — служебный id: порядок выдачи, а не плотный номер. Дырки в нём
	// нормальны (откатившаяся транзакция), плотный journal_seq даёт сегмент.
	ID        int64
	TenantID  int64
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
// id выдаёт схема (`GENERATED ALWAYS AS IDENTITY`) на самой вставке, и это не
// мелочь, а условие корректности: вставка всегда идёт после того, как
// вызывающий заблокировал строку объекта, поэтому для одного объекта порядок id
// совпадает с порядком коммитов. Вызвать nextval раньше блокировки — значит
// получить события одного объекта в порядке, отличном от порядка изменений, и
// восстановление применит retention не в том порядке.
//
// Строку тенанта запись больше не трогает: плотный номер даёт воркер сегментов
// при запечатывании, а не таблица событий. Пропуски в служебных id от
// откатившихся транзакций — норма, потерянным событием они не считаются.
//
// tenant обязан браться из строки (бакета или версии), а не из учётки запроса:
// под обходом по роли страж отдаёт «все тенанты», и событие уехало бы в
// тенанта, которого нет (та же форма ошибки, что с DEK на PR #127).
// Несуществующий тенант здесь отклоняет внешний ключ journal_events.tenant_id.
func appendEvent(ctx context.Context, tx pgx.Tx, tenant int64, kind string,
	objectID *uuid.UUID, bucketID int64, payload any) error {

	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("journal payload %s: %w", kind, err)
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO journal_events (tenant_id, kind, object_id, bucket_id, payload)
		 VALUES ($1, $2::journal_event_kind, $3, $4, $5::jsonb)`,
		tenant, kind, objectID, bucketID, string(raw)); err != nil {
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

// JournalEventsAfter читает события тенанта по порядку выдачи id, после
// afterID (0 — с самого начала).
//
// Этим же читает себя будет `rebuild-meta`, и лимт здесь не украшение: журнал
// за годы — это миллионы событий, и применять их надо порциями.
//
// Порядок — по id, а не по времени: время на разных соединениях расходится, а id
// для одного объекта совпадает с порядком коммитов (см. appendEvent).
func (m *Meta) JournalEventsAfter(ctx context.Context, tenant int64,
	afterID int64, limit int) ([]JournalEvent, error) {

	if limit <= 0 {
		// Правило 9: когда читатель начнёт употребляться в проде, размер страницы
		// уезжает в конфиг (`journal.read_batch` с TAPEGW_JOURNAL_READ_BATCH).
		// Сейчас JournalEventsAfter вызывают только тесты — настраивать нечего,
		// и заводить поле конфига без потребителя значит плодить неиспроверяемую
		// настройку.
		limit = 1000
	}
	rows, err := m.Pool.Query(ctx,
		`SELECT id, tenant_id, kind::text, object_id, bucket_id, payload, created_at
		   FROM journal_events
		  WHERE tenant_id = $1 AND id > $2
		  ORDER BY id
		  LIMIT $3`, tenant, afterID, limit)
	if err != nil {
		return nil, fmt.Errorf("journal events tenant %d: %w", tenant, err)
	}
	defer rows.Close()

	var out []JournalEvent
	for rows.Next() {
		var e JournalEvent
		if err := rows.Scan(&e.ID, &e.TenantID, &e.Kind, &e.ObjectID,
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

// JournalEventCount — число событий тенанта в таблице.
//
// Нужен не для отчёта, а как опора проверки полноты: воркер сегментов обязан
// показать, что ни одно видимое закоммиченное событие не осталось за
// сегментами, а смежность диапазонов сама по себе ничего не доказывает —
// номера раздаёт тот, кто сам отобрал события (ADR-0023, поправка, п. 5).
//
// Но не в этом виде: count(*) считает все события тенанта, а журнал до Ж2 ничем
// не очищается, — на тенанте с миллионами событий это полный скан индекса на
// каждый цикл запечатывания. Воркеру сверка нужна не по всей таблице, а по
// диапазону-кандидату: max(id) против рубежа запечатанного и число событий
// внутри этого диапазона. Сейчас функцию зовут только тесты; брать её за основу
// проверки воркеру нельзя.
func (m *Meta) JournalEventCount(ctx context.Context, tenant int64) (int64, error) {
	var n int64
	if err := m.Pool.QueryRow(ctx,
		`SELECT count(*) FROM journal_events WHERE tenant_id = $1`, tenant).Scan(&n); err != nil {
		return 0, fmt.Errorf("journal event count tenant %d: %w", tenant, err)
	}
	return n, nil
}
