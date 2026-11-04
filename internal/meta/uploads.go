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

// Multipart-загрузки (M3 + T2 PR#2).
//
// Части шифруются одним DEK на всю загрузку: номер части входит в nonce и AAD,
// поэтому части не путаются между собой и между версиями. DEK живёт в
// uploads.wrapped_dek, обёрнутый с AAD = upload_id, а при Complete
// переоборачивается ключом тенанта с AAD = UUID версии и переезжает в
// objects.wrapped_dek.
//
// Страж для загрузок работает через JOIN с buckets: своей колонки tenant_id у
// uploads нет, а дублировать её смысла нет — загрузка живёт минуты, а не годы,
// и выбирается всегда вместе с бакетом.

// Upload — строка uploads вместе с частями.
type Upload struct {
	ID string
	// ObjectID — UUID будущей версии objects.id. nonce и AAD сегментов частей
	// строятся от него, а не от upload_id: читать собранный файл будут под
	// UUID строки объекта, и эти два значения обязаны быть одним и тем же.
	ObjectID  uuid.UUID
	Bucket    string
	Key       string
	Initiated time.Time
	// WrappedDEK — обёртка DEK загрузки. Наружу не отдаётся: им пользуются
	// только UploadPart (запись) и Complete (переобёртка).
	WrappedDEK []byte
	Meta       UploadMeta
	Parts      []UploadPart
}

// UploadMeta — то, что клиент задал при CreateMultipartUpload и что обязано
// дожить до Complete. S3 разрешает задать это в обоих местах, и явные заголовки
// Complete перекрывают сохранённые здесь.
type UploadMeta struct {
	ContentType        *string           `json:"content_type,omitempty"`
	ContentEncoding    *string           `json:"content_encoding,omitempty"`
	ContentDisposition *string           `json:"content_disposition,omitempty"`
	CacheControl       *string           `json:"cache_control,omitempty"`
	UserMeta           map[string]string `json:"user_meta,omitempty"`
	Tags               map[string]string `json:"tags,omitempty"`
	ChecksumAlgorithm  string            `json:"checksum_algorithm,omitempty"`
	ChecksumType       string            `json:"checksum_type,omitempty"`
}

// UploadPart — часть загрузки: её размер, ETag и файл в спуле.
type UploadPart struct {
	Number    int
	Size      int64
	ETag      string
	Checksums map[string]string
	FileID    uuid.UUID
}

// CreateUploadInput — данные CreateMultipartUpload.
type CreateUploadInput struct {
	Bucket string
	Key    string
	ID     string
	// ObjectID — UUID, который станет objects.id при Complete.
	ObjectID uuid.UUID
	Meta     UploadMeta
	// WrappedDEK — обёртка DEK загрузки. Обязательна: без неё части нечем
	// шифровать, а «запишем открытым текстом» здесь не допускается.
	WrappedDEK []byte
}

// CreateUpload заводит загрузку.
func (m *Meta) CreateUpload(ctx context.Context, tenant TenantID, in CreateUploadInput) error {
	metaJSON, err := json.Marshal(in.Meta)
	if err != nil {
		return fmt.Errorf("meta: marshal метаданных загрузки: %w", err)
	}
	if len(in.WrappedDEK) == 0 {
		return fmt.Errorf("meta: у загрузки %s нет обёртки DEK", in.ID)
	}
	return m.inTx(ctx, func(tx pgx.Tx) error {
		var bucketID int64
		q, args := withTenant(
			`SELECT id FROM buckets WHERE name = $1 AND deleted_at IS NULL`, tenant, in.Bucket)
		if err := tx.QueryRow(ctx, q, args...).Scan(&bucketID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("meta: бакет %q: %w", in.Bucket, err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO uploads (upload_id, bucket_id, key, meta, wrapped_dek, object_id)
			 VALUES ($1, $2, $3, $4, $5, $6)`,
			in.ID, bucketID, in.Key, metaJSON, in.WrappedDEK, in.ObjectID); err != nil {
			return fmt.Errorf("meta: загрузка %s: %w", in.ID, err)
		}
		return nil
	})
}

// GetUpload — загрузка с частями по номерам.
func (m *Meta) GetUpload(ctx context.Context, tenant TenantID, uploadID string) (*Upload, error) {
	var u Upload
	var metaJSON []byte
	q, args := withTenantCol(
		`SELECT u.upload_id, u.object_id, b.name, u.key, u.initiated, u.meta, u.wrapped_dek
		   FROM uploads u JOIN buckets b ON b.id = u.bucket_id
		  WHERE u.upload_id = $1 AND b.deleted_at IS NULL`,
		"b.tenant_id", tenant, uploadID)
	err := m.Pool.QueryRow(ctx, q, args...).Scan(&u.ID, &u.ObjectID, &u.Bucket, &u.Key, &u.Initiated,
		&metaJSON, &u.WrappedDEK)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("meta: загрузка %s: %w", uploadID, err)
	}
	if len(metaJSON) > 0 {
		if err := json.Unmarshal(metaJSON, &u.Meta); err != nil {
			return nil, fmt.Errorf("meta: метаданные загрузки %s: %w", uploadID, err)
		}
	}

	parts, err := m.uploadParts(ctx, tenant, uploadID, 0)
	if err != nil {
		return nil, err
	}
	u.Parts = parts
	return &u, nil
}

// uploadParts — части загрузки. maxParts = 0 означает «все».
func (m *Meta) uploadParts(ctx context.Context, tenant TenantID, uploadID string, maxParts int) ([]UploadPart, error) {
	q, args := withTenantCol(
		`SELECT p.part_number, p.size, p.etag, p.checksums, p.file_id
		   FROM upload_parts p
		   JOIN uploads u ON u.upload_id = p.upload_id
		   JOIN buckets b ON b.id = u.bucket_id
		  WHERE p.upload_id = $1 AND b.deleted_at IS NULL`,
		"b.tenant_id", tenant, uploadID)
	q += " ORDER BY p.part_number"
	if maxParts > 0 {
		q += fmt.Sprintf(" LIMIT %d", maxParts)
	}
	rows, err := m.Pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("meta: части загрузки %s: %w", uploadID, err)
	}
	defer rows.Close()

	var out []UploadPart
	for rows.Next() {
		var (
			p         UploadPart
			checksums []byte
		)
		if err := rows.Scan(&p.Number, &p.Size, &p.ETag, &checksums, &p.FileID); err != nil {
			return nil, fmt.Errorf("meta: части загрузки %s: %w", uploadID, err)
		}
		if len(checksums) > 0 {
			if err := json.Unmarshal(checksums, &p.Checksums); err != nil {
				return nil, fmt.Errorf("meta: checksum части %s/%d: %w", uploadID, p.Number, err)
			}
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("meta: части загрузки %s: %w", uploadID, err)
	}
	return out, nil
}

// PutPartInput — результат UploadPart: файл уже на диске, здесь только строка.
type PutPartInput struct {
	Number    int
	Size      int64
	ETag      string
	Checksums map[string]string
	FileID    uuid.UUID
}

// PutPart записывает (перезаписывает) часть загрузки.
//
// Перезапись одной части — штатное поведение S3: клиент вправе переотправить
// часть с тем же номером, и прежний файл обязан быть удалён. Файлы удаляет
// вызывающий: meta про спул не знает, а старый id возвращаются наружу.
func (m *Meta) PutPart(ctx context.Context, tenant TenantID, uploadID string, in PutPartInput) (uuid.UUID, error) {
	checksums, err := json.Marshal(in.Checksums)
	if err != nil {
		return uuid.Nil, fmt.Errorf("meta: marshal checksum: %w", err)
	}
	var old uuid.UUID
	err = m.inTx(ctx, func(tx pgx.Tx) error {
		var bucketID int64
		q, args := withTenant(
			`SELECT b.id FROM buckets b
			   JOIN uploads u ON u.bucket_id = b.id
			  WHERE u.upload_id = $1 AND b.deleted_at IS NULL`, tenant, uploadID)
		if err := tx.QueryRow(ctx, q, args...).Scan(&bucketID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("meta: загрузка %s: %w", uploadID, err)
		}
		// Прежний файл, если он был: без этого переотправленная часть оставила
		// бы в спуле сироту на каждый retry.
		_ = tx.QueryRow(ctx,
			`SELECT file_id FROM upload_parts WHERE upload_id = $1 AND part_number = $2`,
			uploadID, in.Number).Scan(&old)
		_, err := tx.Exec(ctx,
			`INSERT INTO upload_parts (upload_id, part_number, size, etag, checksums, file_id)
			 VALUES ($1, $2, $3, $4, $5, $6)
			 ON CONFLICT (upload_id, part_number) DO UPDATE
			    SET size = EXCLUDED.size, etag = EXCLUDED.etag,
			        checksums = EXCLUDED.checksums, file_id = EXCLUDED.file_id`,
			uploadID, in.Number, in.Size, in.ETag, checksums, in.FileID)
		if err != nil {
			return fmt.Errorf("meta: часть %d загрузки %s: %w", in.Number, uploadID, err)
		}
		return nil
	})
	if err != nil {
		return uuid.Nil, err
	}
	return old, nil
}

// DeleteUpload удаляет загрузку и возвращает id файлов её частей, чтобы
// вызывающий стёр их с диска.
//
// Порядок — «сначала строки, потом файлы»: удаление строк откатывается вместе с
// транзакцией, а unlink уже нет. Файлы без строк безвредны (janitor), строки без
// файлов означают 500 на каждом чтении.
func (m *Meta) DeleteUpload(ctx context.Context, tenant TenantID, uploadID string) ([]uuid.UUID, error) {
	var ids []uuid.UUID
	err := m.inTx(ctx, func(tx pgx.Tx) error {
		q, args := withTenant(
			`SELECT b.id FROM buckets b
			   JOIN uploads u ON u.bucket_id = b.id
			  WHERE u.upload_id = $1 AND b.deleted_at IS NULL`, tenant, uploadID)
		var bucketID int64
		if err := tx.QueryRow(ctx, q, args...).Scan(&bucketID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("meta: загрузка %s: %w", uploadID, err)
		}
		rows, err := tx.Query(ctx,
			`SELECT file_id FROM upload_parts WHERE upload_id = $1`, uploadID)
		if err != nil {
			return fmt.Errorf("meta: файлы частей %s: %w", uploadID, err)
		}
		for rows.Next() {
			var id uuid.UUID
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return fmt.Errorf("meta: файлы частей %s: %w", uploadID, err)
			}
			ids = append(ids, id)
		}
		rows.Close()
		if _, err := tx.Exec(ctx, `DELETE FROM uploads WHERE upload_id = $1`, uploadID); err != nil {
			return fmt.Errorf("meta: удаление загрузки %s: %w", uploadID, err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ids, nil
}

// UploadsByBucket — незавершённые загрузки бакета для ListMultipartUploads.
// Маркеры — (ключ, upload_id) в том же порядке, что и ORDER BY.
func (m *Meta) UploadsByBucket(ctx context.Context, tenant TenantID, bucket, prefix, keyMarker,
	uploadMarker string, max int) ([]Upload, error) {
	where := `WHERE b.name = $1 AND b.deleted_at IS NULL`
	args := []any{bucket}
	if prefix != "" {
		args = append(args, prefix+"%")
		where += fmt.Sprintf(` AND u.key LIKE $%d`, len(args))
	}
	if keyMarker != "" {
		args = append(args, keyMarker)
		where += fmt.Sprintf(` AND u.key > $%d`, len(args))
		if uploadMarker != "" {
			args = append(args, uploadMarker)
			where += fmt.Sprintf(` AND NOT (u.key = $%d AND u.upload_id <= $%d)`,
				len(args)-1, len(args))
		}
	}
	if max <= 0 {
		max = 1000
	}

	q, qargs := withTenantCol(
		`SELECT u.upload_id, u.object_id, b.name, u.key, u.initiated, u.meta, u.wrapped_dek
		   FROM uploads u JOIN buckets b ON b.id = u.bucket_id
		 `+where,
		"b.tenant_id", tenant, args...)
	q += fmt.Sprintf(" ORDER BY u.key, u.upload_id LIMIT %d", max)

	rows, err := m.Pool.Query(ctx, q, qargs...)
	if err != nil {
		return nil, fmt.Errorf("meta: загрузки бакета %q: %w", bucket, err)
	}
	defer rows.Close()

	var out []Upload
	for rows.Next() {
		var (
			u        Upload
			metaJSON []byte
		)
		if err := rows.Scan(&u.ID, &u.ObjectID, &u.Bucket, &u.Key, &u.Initiated, &metaJSON, &u.WrappedDEK); err != nil {
			return nil, fmt.Errorf("meta: загрузки бакета %q: %w", bucket, err)
		}
		if len(metaJSON) > 0 {
			if err := json.Unmarshal(metaJSON, &u.Meta); err != nil {
				return nil, fmt.Errorf("meta: метаданные загрузки %s: %w", u.ID, err)
			}
		}
		out = append(out, u)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("meta: загрузки бакета %q: %w", bucket, err)
	}
	return out, nil
}

// DeleteUploadInTx — удаление загрузки внутри чужой транзакции. Нужен Complete:
// вставка объекта и исчезновение загрузки обязаны быть атомарными, иначе между
// ними повторный Complete создаст вторую версию из тех же частей.
//
// Ноль удалённых строк — ошибка, а не «ну и ладно»: это значит, что загрузку
// уже удалили, и её части тем временем могли быть заняты другой версией.
func DeleteUploadInTx(ctx context.Context, tx pgx.Tx, uploadID string) error {
	tag, err := tx.Exec(ctx, `DELETE FROM uploads WHERE upload_id = $1`, uploadID)
	if err != nil {
		return fmt.Errorf("meta: удаление загрузки %s: %w", uploadID, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: загрузка %s уже удалена", ErrNotFound, uploadID)
	}
	return nil
}
