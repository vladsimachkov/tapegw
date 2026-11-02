package meta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Bucket — строка таблицы buckets.
type Bucket struct {
	ID        int64
	Name      string
	Owner     string
	ACL       []byte // jsonb; nil, если не задан
	Policy    []byte // jsonb
	Tags      map[string]string
	CreatedAt time.Time
}

// CreateBucket вставляет бакет. existing — различает «уже есть» и реальную ошибку:
// AWS на повторный CreateBucket того же владельцем отвечает 200, чужому — 409.
func (m *Meta) CreateBucket(ctx context.Context, name, owner string, acl []byte) (existing bool, err error) {
	tag, err := m.Pool.Exec(ctx,
		`INSERT INTO buckets (name, owner, acl) VALUES ($1, $2, $3)
		 ON CONFLICT (name) DO NOTHING`, name, owner, acl)
	if err != nil {
		return false, fmt.Errorf("insert bucket %q: %w", name, err)
	}
	if tag.RowsAffected() > 0 {
		return false, nil
	}

	// Конфликт: чей бакет?
	var got string
	if err := m.Pool.QueryRow(ctx,
		`SELECT owner FROM buckets WHERE name = $1`, name).Scan(&got); err != nil {
		return false, fmt.Errorf("lookup bucket %q: %w", name, err)
	}
	if got != owner {
		return false, ErrBucketOwnedByOther
	}
	return true, nil
}

// ErrBucketOwnedByOther — бакет с таким именем принадлежит другому владельцу.
// В S3 это 409 BucketAlreadyOwnedByYou / BucketAlreadyExists.
var ErrBucketOwnedByOther = errors.New("meta: bucket owned by another account")

// GetBucket возвращает бакет по имени.
func (m *Meta) GetBucket(ctx context.Context, name string) (*Bucket, error) {
	row := m.Pool.QueryRow(ctx,
		`SELECT id, name, owner, acl, policy, tags, created_at
		   FROM buckets WHERE name = $1`, name)
	return scanBucket(row)
}

// BucketExists — дешёвая проверка наличия без разбора полей.
func (m *Meta) BucketExists(ctx context.Context, name string) (bool, error) {
	var ok bool
	err := m.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM buckets WHERE name = $1)`, name).Scan(&ok)
	if err != nil {
		return false, fmt.Errorf("exists bucket %q: %w", name, err)
	}
	return ok, nil
}

// ListBuckets перечисляет бакеты. При owner == "" — все (режим администратора).
func (m *Meta) ListBuckets(ctx context.Context, owner string) ([]Bucket, error) {
	q := `SELECT id, name, owner, acl, policy, tags, created_at FROM buckets`
	args := []any{}
	if owner != "" {
		q += ` WHERE owner = $1`
		args = append(args, owner)
	}
	q += ` ORDER BY name COLLATE "C"`

	rows, err := m.Pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list buckets: %w", err)
	}
	defer rows.Close()

	var out []Bucket
	for rows.Next() {
		b, err := scanBucketRows(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *b)
	}
	return out, rows.Err()
}

// inTx — транзакция с commit при успехе и rollback при любой ошибке.
// В pgxpool v5 нет BeginFunc, поэтому обёртка своя.
func (m *Meta) inTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := m.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx) // no-op после commit
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// DeleteBucket удаляет бакет, только если в нём нет текущих объектов.
// Проверка и удаление — одна транзакция, иначе между ними может вставиться PUT.
func (m *Meta) DeleteBucket(ctx context.Context, name string) error {
	return m.inTx(ctx, func(tx pgx.Tx) error {
		var id int64
		err := tx.QueryRow(ctx, `SELECT id FROM buckets WHERE name = $1 FOR UPDATE`, name).Scan(&id)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("lock bucket %q: %w", name, err)
		}

		var n int64
		if err := tx.QueryRow(ctx,
			`SELECT count(*) FROM objects WHERE bucket_id = $1 AND is_current`, id).Scan(&n); err != nil {
			return fmt.Errorf("count objects in %q: %w", name, err)
		}
		if n > 0 {
			return ErrBucketNotEmpty
		}

		if _, err := tx.Exec(ctx, `DELETE FROM buckets WHERE id = $1`, id); err != nil {
			return fmt.Errorf("delete bucket %q: %w", name, err)
		}
		return nil
	})
}

// ErrBucketNotEmpty — DeleteBucket к непустому бакету (S3 BucketNotEmpty).
var ErrBucketNotEmpty = errors.New("meta: bucket not empty")

// SetBucketACL обновляет ACL бакета.
func (m *Meta) SetBucketACL(ctx context.Context, name string, acl []byte) error {
	tag, err := m.Pool.Exec(ctx, `UPDATE buckets SET acl = $2 WHERE name = $1`, name, acl)
	if err != nil {
		return fmt.Errorf("set acl %q: %w", name, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// SetBucketTags заменяет теги бакета. nil удаляет их полностью.
func (m *Meta) SetBucketTags(ctx context.Context, name string, tags map[string]string) error {
	var raw []byte
	if tags != nil {
		b, err := json.Marshal(tags)
		if err != nil {
			return fmt.Errorf("marshal tags: %w", err)
		}
		raw = b
	}
	tag, err := m.Pool.Exec(ctx, `UPDATE buckets SET tags = $2 WHERE name = $1`, name, raw)
	if err != nil {
		return fmt.Errorf("set tags %q: %w", name, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func scanBucket(row pgx.Row) (*Bucket, error) {
	var (
		b           Bucket
		acl, policy []byte
		tagsRaw     []byte
	)
	err := row.Scan(&b.ID, &b.Name, &b.Owner, &acl, &policy, &tagsRaw, &b.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scan bucket: %w", err)
	}
	b.ACL, b.Policy = acl, policy
	if len(tagsRaw) > 0 {
		if err := json.Unmarshal(tagsRaw, &b.Tags); err != nil {
			return nil, fmt.Errorf("decode tags: %w", err)
		}
	}
	return &b, nil
}

func scanBucketRows(rows pgx.Rows) (*Bucket, error) {
	var (
		b           Bucket
		acl, policy []byte
		tagsRaw     []byte
	)
	if err := rows.Scan(&b.ID, &b.Name, &b.Owner, &acl, &policy, &tagsRaw, &b.CreatedAt); err != nil {
		return nil, fmt.Errorf("scan bucket row: %w", err)
	}
	b.ACL, b.Policy = acl, policy
	if len(tagsRaw) > 0 {
		if err := json.Unmarshal(tagsRaw, &b.Tags); err != nil {
			return nil, fmt.Errorf("decode tags: %w", err)
		}
	}
	return &b, nil
}
