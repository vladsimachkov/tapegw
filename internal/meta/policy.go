package meta

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// ErrNoBucketPolicy — у бакета нет политики. Отличается от ErrNotFound
// (бакета нет вообще) принципиально: на «нет политики» versitygw отвечает
// ErrNoSuchBucketPolicy и продолжает проверку доступа, а любая другая ошибка
// прерывает запрос. См. auth/access-control.go:421 и auth/object_lock.go:335.
var ErrNoBucketPolicy = errors.New("policy not set")

// SetBucketPolicy записывает политику бакета. policy == nil удаляет её —
// так устроен DeleteBucketPolicy в posix-бэкенде, и удаление отсутствующей
// политики там не ошибка.
//
// Байты сохраняются дословно: документ к этому моменту уже проверил versitygw
// (auth.ValidatePolicyDocument), а отдавать мы обязаны ровно то, что прислал
// клиент.
func (m *Meta) SetBucketPolicy(ctx context.Context, bucket string, policy []byte) error {
	tag, err := m.Pool.Exec(ctx,
		`UPDATE buckets SET policy = $2
		  WHERE name = $1 AND deleted_at IS NULL`,
		bucket, policy)
	if err != nil {
		return fmt.Errorf("set policy for %q: %w", bucket, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// GetBucketPolicy возвращает сырые байты политики.
//
// ErrNotFound — бакета нет (или он удалён). ErrNoBucketPolicy — бакет есть,
// политики нет. Разделение обязательно: если свести оба к одной ошибке,
// «бакета нет» начнёт выглядеть как «политики нет», и проверка доступа пойдёт
// дальше по пути «политика отсутствует» вместо того чтобы отказать.
func (m *Meta) GetBucketPolicy(ctx context.Context, bucket string) ([]byte, error) {
	var policy *string
	err := m.Pool.QueryRow(ctx,
		`SELECT policy FROM buckets WHERE name = $1 AND deleted_at IS NULL`,
		bucket).Scan(&policy)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get policy for %q: %w", bucket, err)
	}
	if policy == nil {
		return nil, ErrNoBucketPolicy
	}
	return []byte(*policy), nil
}

// ChangeBucketOwner передаёт бакет другой учётной записи. Вызывается только из
// admin API (PATCH /change-bucket-owner), где versitygw уже проверил, что
// новый владелец существует в IAM.
func (m *Meta) ChangeBucketOwner(ctx context.Context, bucket, owner string) error {
	tag, err := m.Pool.Exec(ctx,
		`UPDATE buckets SET owner = $2
		  WHERE name = $1 AND deleted_at IS NULL`,
		bucket, owner)
	if err != nil {
		return fmt.Errorf("change owner of %q: %w", bucket, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// BucketOwner — пара имя/владелец для админского перечисления.
type BucketOwner struct {
	Name  string
	Owner string
}

// ListAllBucketsAndOwners перечисляет ВСЕ бакеты со владельцами — без фильтра
// по владельцу, в отличие от ListBuckets. Это опора admin API
// (PATCH /list-buckets), где список нужен оператору целиком.
//
// Порядок — байтовый порядок имени, как в ListBuckets: админские скрипты
// сравнивают вывод между вызовами.
func (m *Meta) ListAllBucketsAndOwners(ctx context.Context) ([]BucketOwner, error) {
	rows, err := m.Pool.Query(ctx,
		`SELECT name, owner FROM buckets
		  WHERE deleted_at IS NULL
		  ORDER BY name COLLATE "C"`)
	if err != nil {
		return nil, fmt.Errorf("list buckets and owners: %w", err)
	}
	defer rows.Close()

	var out []BucketOwner
	for rows.Next() {
		var b BucketOwner
		if err := rows.Scan(&b.Name, &b.Owner); err != nil {
			return nil, fmt.Errorf("scan bucket owner: %w", err)
		}
		out = append(out, b)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate buckets: %w", err)
	}
	return out, nil
}
