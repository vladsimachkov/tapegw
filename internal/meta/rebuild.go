package meta

// Вставка объектов при восстановлении из архива (rebuild-meta, ADR-0019).
//
// Это не PutObject и не пытается им быть. PutObject обслуживает живой запрос:
// он берёт advisory-лок ключа, проверяет условия If-Match, считает
// online_until из конфига, потому что обещает клиенту дисковую копию. У
// восстановления ничего этого нет: данных на диске нет в помине, объект уже на
// ленте, и очередь миграции его не должна трогать — state сразу 'archived',
// landing=false.
//
// Главное отличие — идемпотентность по чужому признаку. Повторный запуск
// восстановления обязан не плодить строки, и «уже есть» здесь означает не только
// «та же строка по id», но и «текущей строкой ключа уже считается другая
// версия». Поэтому перед вставкой текущей версии мы снимаем признак с
// соседей, а не надеемся на уникальность: иначе второй прогон споткнулся бы о
// objects_current там, где первый прогон оставил устаревшую текущей.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// RebuildObjectInput — одна восстановленная версия.
type RebuildObjectInput struct {
	ID          uuid.UUID
	BucketID    int64
	TenantID    int64
	PoolID      int64
	Key         string
	Size        int64
	ETag        string
	ContentType *string
	UserMeta    map[string]string
	// CreatedAt — время записи версии из манифеста. По нему rebuild считает
	// version_seq и решает, какая версия текущая.
	CreatedAt time.Time
	// VersionSeq — порядок версии среди версий того же ключа, 1..N.
	VersionSeq int64
	IsCurrent  bool
	// IsNullVersion — версия с id "null": бакет был без версий или
	// приостановлен. У такой версии особый путь перезаписи (миграция 0005).
	IsNullVersion  bool
	IsDeleteMarker bool
	WrappedDEK     []byte
	RetentionMode  string
	RetainUntil    *time.Time
	LegalHold      bool
	// Parts — таблица частей multipart-версии. Пусто — объект записан одним PUT.
	Parts []ObjectPart
	// StorageClass — из манифеста; за пределами GLACIER у нас ничего нет, но
	// переносить надо то, что было, а не то, что удобно.
	StorageClass string
}

// RebuildObject вставляет восстановленную версию.
//
// Возвращает inserted=false, если такая версия уже есть: повторный прогон по
// уже восстановленной схеме — нормальная операция, а не авария.
func (m *Meta) RebuildObject(ctx context.Context, in RebuildObjectInput) (bool, error) {
	if in.PoolID == 0 {
		return false, errors.New("meta: восстановление: не указан пул")
	}
	if len(in.WrappedDEK) == 0 && !in.IsDeleteMarker {
		// CHECK objects_dek_present не даст вставить версию с данными без
		// ключа, но сообщение от Postgres здесь менее понятно, чем от нас.
		return false, fmt.Errorf("meta: восстановление %s: пустая обёртка DEK", in.ID)
	}

	storage := in.StorageClass
	if storage == "" {
		storage = "GLACIER"
	}
	var mode any
	if in.RetentionMode != "" {
		mode = in.RetentionMode
	}
	if (in.RetentionMode == "") != (in.RetainUntil == nil) {
		return false, fmt.Errorf("meta: восстановление %s: режим блокировки %q и дата %v не парны",
			in.ID, in.RetentionMode, in.RetainUntil)
	}

	return in.insert(m, ctx, storage, mode)
}

func (in RebuildObjectInput) insert(m *Meta, ctx context.Context, storage string, mode any) (bool, error) {
	tx, err := m.Pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("meta: восстановление: начало транзакции: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Текущей может быть ровно одна версия ключа (objects_current). Первый
	// прогон оставил текущей не ту версию, если второй прогон считает порядок
	// иначе, — поэтому снимаем признак до вставки, а не полагаемся на то, что
	// порядок всегда совпадёт.
	if in.IsCurrent {
		if _, err := tx.Exec(ctx,
			`UPDATE objects SET is_current = false
			  WHERE bucket_id = $1 AND key = $2 AND is_current AND id <> $3`,
			in.BucketID, in.Key, in.ID); err != nil {
			return false, fmt.Errorf("meta: восстановление: снять текущесть с бакета %d ключа %s: %w",
				in.BucketID, in.Key, err)
		}
	}

	// online_until NOT NULL, но дисковой копии у восстановленной версии нет.
	// Ставим created_at: смысл поля — «до какого момента лежит на диске», и для
	// версии, которая никогда на диск не ложилась, любое прошедшее время
	// честнее будущего обещания.
	online := in.CreatedAt

	tag, err := tx.Exec(ctx,
		`INSERT INTO objects
		   (id, bucket_id, tenant_id, pool_id, key, size, etag, content_type, user_meta,
		    storage_class, state, landing, online_until, created_at, archived_at,
		    is_current, is_null_version, is_delete_marker, version_seq,
		    wrapped_dek, retention_mode, retain_until, legal_hold)
		 VALUES
		   ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'archived',false,$11,$12,$12,$13,$14,$15,$16,
		    $17,$18,$19,$20)
		 ON CONFLICT (id) DO NOTHING`,
		in.ID, in.BucketID, in.TenantID, in.PoolID, in.Key, in.Size, in.ETag,
		in.ContentType, jsonOrNull(in.UserMeta), storage, online, in.CreatedAt,
		in.IsCurrent, in.IsNullVersion, in.IsDeleteMarker, in.VersionSeq,
		in.WrappedDEK, mode, in.RetainUntil, in.LegalHold)
	if err != nil {
		return false, fmt.Errorf("meta: восстановление %s: %w", in.ID, err)
	}
	if tag.RowsAffected() == 0 {
		// Такая версия уже восстановлена. Части ниже не трогаем: они либо уже
		// лежат от первого прогона, либо их не было.
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("meta: восстановление: коммит: %w", err)
		}
		return false, nil
	}

	for _, p := range in.Parts {
		if _, err := tx.Exec(ctx,
			`INSERT INTO object_parts (version_id, part_number, plain_size, cipher_offset, salt)
			 VALUES ($1,$2,$3,$4,$5)
			 ON CONFLICT (version_id, part_number) DO UPDATE
			    SET plain_size = EXCLUDED.plain_size,
			        cipher_offset = EXCLUDED.cipher_offset,
			        salt = EXCLUDED.salt`,
			in.ID, p.Number, p.PlainSize, p.CipherOffset, p.Salt); err != nil {
			return false, fmt.Errorf("meta: восстановление %s часть %d: %w", in.ID, p.Number, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("meta: восстановление: коммит: %w", err)
	}
	return true, nil
}

// jsonOrNull — nil в jsonb вместо 'null': пустой map user_meta не должен
// отличаться от отсутствующего, а 'null' отличался бы при чтении.
func jsonOrNull(mm map[string]string) any {
	if len(mm) == 0 {
		return nil
	}
	b, err := json.Marshal(mm)
	if err != nil {
		return nil
	}
	return b
}
