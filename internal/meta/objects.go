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

// Object — строка таблицы objects.
//
// ID генерирует вызывающий, а не база: файл в спуле записывается до транзакции
// (правило 6), и имя файла обязано совпасть со строкой. Путь spool.Path(id)
// по тому же UUID собирает и mover в другом процессе (ADR-0007).
type Object struct {
	ID           uuid.UUID
	BucketID     int64
	Key          string
	Size         int64
	ETag         string
	Checksums    map[string]string
	ContentType  *string
	UserMeta     map[string]string
	ACL          []byte
	Tags         map[string]string
	StorageClass string
	State        string
	// Landing — на диске есть копия, которую можно прочитать без ленты.
	Landing bool
	// OnlineUntil — до какой даты копия гарантированно доступна. NOT NULL с
	// миграции 0004: NULL в этом столбце молча отключал бы вытеснение.
	OnlineUntil time.Time
	CreatedAt   time.Time
	ArchivedAt  *time.Time
	// IsDeleteMarker — версия-маркер удаления: строка без данных из-за которой
	// ключ выглядит удалённым. Чтение обязано различать её: GET по такому ключу
	// отдаёт NoSuchKey, а GET по id самого маркера — MethodNotAllowed.
	IsDeleteMarker bool
	// IsNullVersion — версия с versionId = "null" в смысле AWS (записана в
	// бакете без версий или на приостановленном). По ней чтение решает, что
	// положить в x-amz-version-id: id или "null".
	IsNullVersion bool
	// Versioning — состояние версионирования бакета на момент вставки.
	// Заполняет только PutObject: по нему бэкенд решает, какой x-amz-version-id
	// поставить в ответ. Для unversioned заголовка нет, для suspended — "null",
	// для enabled — id версии.
	Versioning string
}

// ReadableFromDisk — можно ли отдать содержимое без RestoreObject.
//
// pending и archiving защищены состоянием, а не датой: migrator может стоять
// часами, и объект никуда с диска при этом не денется. Для archived дата и
// работает — иначе HEAD пообещал бы доступность, а GET отдал бы
// InvalidObjectState. См. ADR-0003.
func (o *Object) ReadableFromDisk(now time.Time) bool {
	switch o.State {
	case "pending", "archiving":
		return true
	case "archived":
		return o.Landing && o.OnlineUntil.After(now)
	default:
		return false
	}
}

// PutObjectInput — данные нового текущей строки объекта.
type PutObjectInput struct {
	ID           uuid.UUID
	Bucket       string
	Key          string
	Size         int64
	ETag         string
	Checksums    map[string]string
	ContentType  *string
	UserMeta     map[string]string
	ACL          []byte
	Tags         map[string]string
	StorageClass string
	// OnlineUntil — конец срока удержания дисковой копии. Считает вызывающий
	// из spool.landing_min_retention: миграция конфиг не видит, а обещание
	// клиенту выдаём именно здесь.
	OnlineUntil time.Time
	// Precondition — условная запись (If-Match / If-None-Match). Вызывается
	// внутри транзакции после взятия advisory-лока, на etag текущей строки:
	// только так проверка атомарна относительно самой записи. nil — условий нет.
	//
	// Колбэк, а не готовые заголовки: разбором заголовков и кодами ошибок
	// владеет versitygw, а пакет meta про него ничего знать не должен.
	Precondition func(curEtag string, exists bool) error
}

// PutObject вставляет объект и делает его текущим, вытесняя прежнюю версию
// того же ключа. Одна транзакция — правило 7 из AGENTS.md.
//
// Порядок локаков жёсткий (ADR-0005):
//
//  1. pg_advisory_xact_lock(hashtextextended(bucket || '/' || key, 0)) —
//     сериализует перезапись одного ключа между узлами и внутри узла. Лок
//     транзакционный, снимется сам на commit.
//  2. бакет FOR SHARE — чтобы DeleteBucket (берёт FOR UPDATE) не мог закоммититься
//     между нашей проверкой и вставкой. Иначе PUT в «удаляемый» бакет создал бы
//     строку-сироту в несуществующем бакете.
//
// Advisory-лок выбран вместо retry по unique_violation: при двух параллельных
// перезаписях одного ключа откатываться и повторять значит дважды писать один и
// тот же файл, а при горячем ключе это превращается в livelock.
//
// Локи берём в этом порядке всегда и только в нём: обратный порядок дал бы
// взаимоблокировку с DeleteBucket, который сначала блокирует бакет.
func (m *Meta) PutObject(ctx context.Context, tenant TenantID, in PutObjectInput) (*Object, error) {
	checksums, err := json.Marshal(in.Checksums)
	if err != nil {
		return nil, fmt.Errorf("marshal checksums: %w", err)
	}
	userMeta, err := json.Marshal(in.UserMeta)
	if err != nil {
		return nil, fmt.Errorf("marshal user meta: %w", err)
	}
	tags, err := json.Marshal(in.Tags)
	if err != nil {
		return nil, fmt.Errorf("marshal tags: %w", err)
	}
	if in.StorageClass == "" {
		in.StorageClass = "GLACIER"
	}

	var out *Object
	err = m.inTx(ctx, func(tx pgx.Tx) error {
		// 1. Лок по ключу. hashtextextended даёт bigint из текста и seed;
		//    seed 0 — константа, смысл только в том, что он не меняется.
		if _, err := tx.Exec(ctx,
			`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
			in.Bucket+"/"+in.Key); err != nil {
			return fmt.Errorf("advisory lock %s/%s: %w", in.Bucket, in.Key, err)
		}

		// 2. Бакет на чтение: конфликтует с FOR UPDATE у DeleteBucket, но не
		//    мешает параллельным PUT в тот же бакет. Предикат тенанта здесь же:
		//    писать в чужой бакет не за что проверять, запрос просто не найдёт
		//    его и отдаст ErrNotFound.
		var bucketID int64
		var versioning string
		var bucketTenant int64
		bucketQ, bucketArgs := withTenant(
			`SELECT id, versioning, tenant_id FROM buckets
			  WHERE name = $1 AND deleted_at IS NULL`, tenant, in.Bucket)
		err := tx.QueryRow(ctx, bucketQ+` FOR SHARE`, bucketArgs...).Scan(&bucketID, &versioning, &bucketTenant)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("lock bucket %q: %w", in.Bucket, err)
		}

		// Объект наследует тенанта БАКЕТА, а не учётки (ADR-0017 п. 6). У
		// учётки с обходом по роли тенанта нет вовсе, и подстановка её значения
		// в objects.tenant_id означала бы FK violation на tenants — то есть 500
		// на каждый PUT администратором. Фильтр выше при этом не ослабевает:
		// чужой бакет запрос просто не находит.

		// Пока версии не включены, у объекта versionId = "null" в смысле AWS.
		// По этому же признаку прежняя строка уходит в deleting (шаг 4), а не
		// остаётся живой версией.
		nullVersion := IsNullVersionBucket(versioning)

		// 3. Условная запись. Проверяем после advisory-лока и до вставки: между
		//    проверкой и записью никто не успевает сменить текущую версию, и
		//    гонка двух If-None-Match: * разрешается честно — проигравший
		//    получает PreconditionFailed, а не «оба записали».
		if in.Precondition != nil {
			var curEtag string
			err := tx.QueryRow(ctx,
				`SELECT etag FROM objects
				  WHERE bucket_id = $1 AND key = $2 AND is_current`,
				bucketID, in.Key).Scan(&curEtag)
			switch {
			case errors.Is(err, pgx.ErrNoRows):
				if err := in.Precondition("", false); err != nil {
					return err
				}
			case err != nil:
				return fmt.Errorf("etag текущей версии %s/%s: %w", in.Bucket, in.Key, err)
			default:
				if err := in.Precondition(curEtag, true); err != nil {
					return err
				}
			}
		}

		// 4. Прежняя текущая строка теряет актуальность. Что с ней дальше —
		//    зависит от версионирования бакета (ADR-0015).
		//
		//    versioning = 'enabled': прежняя версия остаётся живой, просто
		//    перестаёт быть текущей. Перевод в deleting здесь означал бы
		//    уничтожение истории и обход Object Lock: клиент включил версии
		//    именно затем, чтобы прежние данные не пропадали.
		//
		//    unversioned / suspended: по AWS предыдущая null-версия не
		//    сохраняется, а заменяется. Поэтому строка уходит в deleting — одного
		//    is_current = false мало: в pending её заархивировал бы migrator
		//    (лишняя запись на ленту), а deleter её не увидел бы, и копия
		//    осталась бы и в landing, и на ленте навсегда. В deleting строку
		//    подхватывает индекс objects_queue.
		//
		//    С прежней null-версии в той же транзакции снимается флаг: частичный
		//    уникальный индекс objects_null_version разрешает одну null-версию на
		//    ключ, и без этого шага новая строка упрётся в него.
		//
		//    retired_at здесь не ставим: по миграции 0001 он помечает терминальные
		//    строки (deleted/failed) для выгрузки, а deleting — ещё не терминальное.
		if versioning == VersioningEnabled {
			if _, err := tx.Exec(ctx,
				`UPDATE objects
				    SET is_current = false
				  WHERE bucket_id = $1 AND key = $2 AND is_current`,
				bucketID, in.Key); err != nil {
				return fmt.Errorf("retire previous version of %s/%s: %w", in.Bucket, in.Key, err)
			}
		} else {
			// unversioned и suspended — два запроса, а не один, и это не
			// перестраховка.
			//
			// В приостановленном бакете текущей может быть **обычная** версия,
			// записанная, пока версии были включены. Её удалять нельзя — клиент
			// сохранил её именно ради истории. По AWS заменяется только
			// null-версия.
			//
			// 1. Текущая обычная версия просто перестаёт быть текущей.
			if _, err := tx.Exec(ctx,
				`UPDATE objects
				    SET is_current = false
				  WHERE bucket_id = $1 AND key = $2 AND is_current AND NOT is_null_version`,
				bucketID, in.Key); err != nil {
				return fmt.Errorf("retire current version of %s/%s: %w", in.Bucket, in.Key, err)
			}

			// 2. Null-версия заменяется — та, что сейчас текущая, и та, что
			//    перестала быть текущей раньше. Если не снять флаг здесь, новый
			//    INSERT с is_null_version = true упрётся в objects_null_version.
			if _, err := tx.Exec(ctx,
				`UPDATE objects
				    SET is_current = false,
				        state = 'deleting',
				        is_null_version = false
				  WHERE bucket_id = $1 AND key = $2 AND is_null_version`,
				bucketID, in.Key); err != nil {
				return fmt.Errorf("replace null version of %s/%s: %w", in.Bucket, in.Key, err)
			}
		}

		// 5. Новая версия.
		//
		// tenant_id и pool_id передаются явно: колонки NOT NULL без DEFAULT
		// (миграция 0005), чтобы забытый тенант падал, а не писался в первый.
		// Объект наследует тенанта бакета, в который он пишется, — страж уже
		// не дал записать его в чужой.
		//
		// is_null_version ставится по состоянию бакета: пока версии не включены,
		// у объекта versionId = "null" в смысле AWS. Именно эта пометка потом
		// позволяет записи в suspended-бакет заменить предыдущую null-версию.
		created, err := tx.Query(ctx,
			`INSERT INTO objects (
			    id, bucket_id, key, is_current, size, etag, checksums,
			    content_type, user_meta, acl, tags, storage_class,
			    state, landing, online_until, tenant_id, pool_id, is_null_version)
			 VALUES ($1,$2,$3,true,$4,$5,$6,$7,$8,$9,$10,$11,'pending',true,$12,$13,$14,$15)
			 RETURNING id, bucket_id, key, size, etag, checksums, content_type,
			           user_meta, acl, tags, storage_class, state, landing,
			           online_until, created_at, archived_at, is_delete_marker,
			           is_null_version`,
			in.ID, bucketID, in.Key, in.Size, in.ETag, checksums,
			in.ContentType, userMeta, in.ACL, tags, in.StorageClass,
			in.OnlineUntil, bucketTenant, DefaultPoolID, nullVersion)
		if err != nil {
			return fmt.Errorf("insert object %s/%s: %w", in.Bucket, in.Key, err)
		}
		defer created.Close()
		if !created.Next() {
			if err := created.Err(); err != nil {
				return fmt.Errorf("insert object %s/%s: %w", in.Bucket, in.Key, err)
			}
			// RETURNING без строки при успешном INSERT невозможен, но пустой
			// результат не должен превращаться в nil-указатель выше.
			return fmt.Errorf("insert object %s/%s: RETURNING не вернул строку", in.Bucket, in.Key)
		}

		obj, err := scanObjectRow(created)
		if err != nil {
			return err
		}
		obj.Versioning = versioning
		out = obj
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetObject возвращает текущую строку объекта того же тенанта, что и бакет.
func (m *Meta) GetObject(ctx context.Context, tenant TenantID, bucket, key string) (*Object, error) {
	q, args := withTenantCol(
		`SELECT o.id, o.bucket_id, o.key, o.size, o.etag, o.checksums,
		        o.content_type, o.user_meta, o.acl, o.tags, o.storage_class,
		        o.state, o.landing, o.online_until, o.created_at, o.archived_at,
		        o.is_delete_marker, o.is_null_version
		   FROM objects o
		   JOIN buckets b ON b.id = o.bucket_id
		  WHERE b.name = $1 AND b.deleted_at IS NULL
		    AND o.key = $2 AND o.is_current`,
		"b.tenant_id", tenant, bucket, key)
	row := m.Pool.QueryRow(ctx, q, args...)
	return scanObject(row)
}

// GetObjectByID — строка по UUID. Нужен mover-у: у него в аргументах только id.
// Тенант фильтруется по objects.tenant_id — денормализация из миграции 0005
// ради того, чтобы такие выборки не требовали JOIN с buckets.
func (m *Meta) GetObjectByID(ctx context.Context, tenant TenantID, id uuid.UUID) (*Object, error) {
	q, args := withTenantCol(
		`SELECT o.id, o.bucket_id, o.key, o.size, o.etag, o.checksums,
		        o.content_type, o.user_meta, o.acl, o.tags, o.storage_class,
		        o.state, o.landing, o.online_until, o.created_at, o.archived_at,
		        o.is_delete_marker, o.is_null_version
		   FROM objects o WHERE o.id = $1`,
		"o.tenant_id", tenant, id)
	row := m.Pool.QueryRow(ctx, q, args...)
	return scanObject(row)
}

func scanObject(row pgx.Row) (*Object, error) {
	return scanObjectRow(row)
}

// scanObjectRow читает строку objects из любого pgx-источника. Колонки
// перечислены в одном месте, чтобы запросы из GetObject и из INSERT ... RETURNING
// не разъехались.
func scanObjectRow(src interface {
	Scan(dest ...any) error
}) (*Object, error) {
	var (
		o                              Object
		checksums, userMeta, tags      []byte
		storageClass, state, etag, key string
		onlineUntil                    time.Time
	)
	err := src.Scan(
		&o.ID, &o.BucketID, &key, &o.Size, &etag, &checksums,
		&o.ContentType, &userMeta, &o.ACL, &tags, &storageClass,
		&state, &o.Landing, &onlineUntil, &o.CreatedAt, &o.ArchivedAt,
		&o.IsDeleteMarker, &o.IsNullVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scan object: %w", err)
	}
	o.Key, o.ETag, o.StorageClass, o.State, o.OnlineUntil = key, etag, storageClass, state, onlineUntil

	if len(checksums) > 0 {
		if err := json.Unmarshal(checksums, &o.Checksums); err != nil {
			return nil, fmt.Errorf("decode checksums: %w", err)
		}
	}
	if len(userMeta) > 0 {
		if err := json.Unmarshal(userMeta, &o.UserMeta); err != nil {
			return nil, fmt.Errorf("decode user meta: %w", err)
		}
	}
	if len(tags) > 0 {
		if err := json.Unmarshal(tags, &o.Tags); err != nil {
			return nil, fmt.Errorf("decode tags: %w", err)
		}
	}
	return &o, nil
}
