package meta

import (
	"context"
	"encoding/base64"
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
	DeletedAt *time.Time // nil = бакет активен
}

// ListBucketsOpt — параметры перечисления. Zero value = «всё, без фильтра».
type ListBucketsOpt struct {
	// Owner пустой = все бакеты (режим администратора).
	Owner  string
	Prefix string
	// Max — максимум строк за вызов (S3 max-buckets). <=0 = без лимита.
	Max int
	// After — имя, после которого продолжать (декодированный ContinuationToken).
	After string
}

// CreateBucket вставляет активный бакет. existing различает «уже есть свой» и
// реальную ошибку.
//
// Теги передаются здесь же, а не отдельным запросом: иначе между вставкой и
// тегами падение процесса оставляет бакет без тегов, и клиент получит 200 с
// неполными метаданными.
func (m *Meta) CreateBucket(ctx context.Context, name, owner string, acl []byte, tags map[string]string) (existing bool, err error) {
	var tagsRaw []byte
	if len(tags) > 0 {
		if tagsRaw, err = json.Marshal(tags); err != nil {
			return false, fmt.Errorf("marshal tags: %w", err)
		}
	}

	// ON CONFLICT по частичному уникальному индексу buckets_name_active:
	// мягко удалённый бакет имени не занимает.
	tag, err := m.Pool.Exec(ctx,
		`INSERT INTO buckets (name, owner, acl, tags)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (name) WHERE deleted_at IS NULL DO NOTHING`,
		name, owner, acl, tagsRaw)
	if err != nil {
		return false, fmt.Errorf("insert bucket %q: %w", name, err)
	}
	if tag.RowsAffected() > 0 {
		return false, nil
	}

	var got string
	if err := m.Pool.QueryRow(ctx,
		`SELECT owner FROM buckets WHERE name = $1 AND deleted_at IS NULL`, name).Scan(&got); err != nil {
		return false, fmt.Errorf("lookup bucket %q: %w", name, err)
	}
	if got != owner {
		return false, ErrBucketOwnedByOther
	}
	return true, nil
}

// ErrBucketOwnedByOther — активный бакет с таким именем принадлежит другому
// владельцу. В S3 это BucketAlreadyOwnedByYou (свой) / BucketAlreadyExists (чужой),
// см. backend/posix/posix.go.
var ErrBucketOwnedByOther = errors.New("meta: bucket owned by another account")

// GetBucket возвращает активный бакет по имени.
func (m *Meta) GetBucket(ctx context.Context, name string) (*Bucket, error) {
	row := m.Pool.QueryRow(ctx,
		`SELECT id, name, owner, acl, policy, tags, created_at, deleted_at
		   FROM buckets WHERE name = $1 AND deleted_at IS NULL`, name)
	return scanBucket(row)
}

// BucketExists — дешёвая проверка наличия без разбора полей.
func (m *Meta) BucketExists(ctx context.Context, name string) (bool, error) {
	var ok bool
	err := m.Pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM buckets WHERE name = $1 AND deleted_at IS NULL)`, name).Scan(&ok)
	if err != nil {
		return false, fmt.Errorf("exists bucket %q: %w", name, err)
	}
	return ok, nil
}

// ListBuckets перечисляет активные бакеты в порядке S3 (порядок байтов).
//
// Префикс и max-buckets фильтруются в SQL, а не в Go: иначе запрос за
// «первой страницей» вытаскивает все бакеты, а на кластере с десятками тысяч
// бакетов это ещё и полный прогон индекса на каждый ListBuckets.
func (m *Meta) ListBuckets(ctx context.Context, opt ListBucketsOpt) ([]Bucket, string, error) {
	q := `SELECT id, name, owner, acl, policy, tags, created_at, deleted_at
	        FROM buckets
	       WHERE deleted_at IS NULL`
	args := []any{}
	add := func(cond string, val any) {
		args = append(args, val)
		q += ` AND ` + fmt.Sprintf(cond, len(args))
	}

	if opt.Owner != "" {
		add(`owner = $%d`, opt.Owner)
	}
	if opt.Prefix != "" {
		add(`name LIKE $%d`, escapeLike(opt.Prefix)+"%")
	}
	if opt.After != "" {
		add(`name > $%d COLLATE "C"`, opt.After)
	}
	q += ` ORDER BY name COLLATE "C"`
	if opt.Max > 0 {
		args = append(args, opt.Max)
		q += fmt.Sprintf(` LIMIT $%d`, len(args))
	}

	rows, err := m.Pool.Query(ctx, q, args...)
	if err != nil {
		return nil, "", fmt.Errorf("list buckets: %w", err)
	}
	defer rows.Close()

	var out []Bucket
	for rows.Next() {
		b, err := scanBucketRows(rows)
		if err != nil {
			return nil, "", err
		}
		out = append(out, *b)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	// Токен продолжения отдаём только если реально отрезали по лимиту.
	next := ""
	if opt.Max > 0 && len(out) == opt.Max && len(out) > 0 {
		next = EncodeToken(out[len(out)-1].Name)
	}
	return out, next, nil
}

// EncodeToken делает из имени бакета непрозрачный ContinuationToken.
func EncodeToken(name string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(name))
}

// DecodeToken разворачивает ContinuationToken. Пустая строка — начало выдачи.
func DecodeToken(token string) (string, error) {
	if token == "" {
		return "", nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return "", fmt.Errorf("bad continuation token: %w", err)
	}
	return string(raw), nil
}

// DeleteBucket помечает бакет удалённым, только если в нём нет текущих объектов.
//
// Проверка и пометка — одна транзакция, иначе между ними может вставиться PUT.
// Удаление мягкое: строки objects в deleting/deleted всё ещё ссылаются на бакет
// по FK, жёсткое удаление дало бы FK violation.
func (m *Meta) DeleteBucket(ctx context.Context, name string) error {
	return m.inTx(ctx, func(tx pgx.Tx) error {
		var id int64
		err := tx.QueryRow(ctx,
			`SELECT id FROM buckets WHERE name = $1 AND deleted_at IS NULL FOR UPDATE`, name).Scan(&id)
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

		if _, err := tx.Exec(ctx,
			`UPDATE buckets SET deleted_at = now() WHERE id = $1`, id); err != nil {
			return fmt.Errorf("delete bucket %q: %w", name, err)
		}
		return nil
	})
}

// ErrBucketNotEmpty — DeleteBucket к непустому бакету (S3 BucketNotEmpty).
var ErrBucketNotEmpty = errors.New("meta: bucket not empty")

// SetBucketACL обновляет ACL активного бакета.
func (m *Meta) SetBucketACL(ctx context.Context, name string, acl []byte) error {
	tag, err := m.Pool.Exec(ctx,
		`UPDATE buckets SET acl = $2 WHERE name = $1 AND deleted_at IS NULL`, name, acl)
	if err != nil {
		return fmt.Errorf("set acl %q: %w", name, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// SetBucketTags заменяет теги активного бакета. nil удаляет их полностью.
func (m *Meta) SetBucketTags(ctx context.Context, name string, tags map[string]string) error {
	var raw []byte
	if tags != nil {
		b, err := json.Marshal(tags)
		if err != nil {
			return fmt.Errorf("marshal tags: %w", err)
		}
		raw = b
	}
	tag, err := m.Pool.Exec(ctx,
		`UPDATE buckets SET tags = $2 WHERE name = $1 AND deleted_at IS NULL`, name, raw)
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
	err := row.Scan(&b.ID, &b.Name, &b.Owner, &acl, &policy, &tagsRaw, &b.CreatedAt, &b.DeletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scan bucket: %w", err)
	}
	b.ACL, b.Policy = acl, policy
	if err := decodeTags(tagsRaw, &b.Tags); err != nil {
		return nil, err
	}
	return &b, nil
}

func scanBucketRows(rows pgx.Rows) (*Bucket, error) {
	var (
		b           Bucket
		acl, policy []byte
		tagsRaw     []byte
	)
	if err := rows.Scan(&b.ID, &b.Name, &b.Owner, &acl, &policy, &tagsRaw, &b.CreatedAt, &b.DeletedAt); err != nil {
		return nil, fmt.Errorf("scan bucket row: %w", err)
	}
	b.ACL, b.Policy = acl, policy
	if err := decodeTags(tagsRaw, &b.Tags); err != nil {
		return nil, err
	}
	return &b, nil
}

func decodeTags(raw []byte, dst *map[string]string) error {
	if len(raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		return fmt.Errorf("decode tags: %w", err)
	}
	return nil
}

// escapeLike экранирует спецсимволы LIKE, чтобы префикс с % или _ не начал
// матчить больше, чем набрал пользователь.
func escapeLike(s string) string {
	out := make([]rune, 0, len(s)+4)
	for _, r := range s {
		switch r {
		case '\\', '%', '_':
			out = append(out, '\\', r)
		default:
			out = append(out, r)
		}
	}
	return string(out)
}
