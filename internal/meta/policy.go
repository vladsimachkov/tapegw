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
func (m *Meta) SetBucketPolicy(ctx context.Context, tenant TenantID, bucket string, policy []byte) error {
	q, args := withTenant(
		`UPDATE buckets SET policy = $2
		  WHERE name = $1 AND deleted_at IS NULL`,
		tenant, bucket, policy)
	tag, err := m.Pool.Exec(ctx, q, args...)
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
// ErrNotFound — бакета нет (или он удалён, или он чужой: для этого вызывающего
// они неразличимы, см. BucketExistsAnyTenant). ErrNoBucketPolicy — бакет есть,
// политики нет. Разделение обязательно: если свести оба к одной ошибке,
// «бакета нет» начнёт выглядеть как «политики нет», и проверка доступа пойдёт
// дальше по пути «политика отсутствует» вместо того чтобы отказать.
func (m *Meta) GetBucketPolicy(ctx context.Context, tenant TenantID, bucket string) ([]byte, error) {
	var policy *string
	q, args := withTenant(
		`SELECT policy FROM buckets WHERE name = $1 AND deleted_at IS NULL`,
		tenant, bucket)
	err := m.Pool.QueryRow(ctx, q, args...).Scan(&policy)
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
// новый владелец существует в IAM. Тенант здесь — обход по роли: админ
// перекладывает бакет между учётками, и фильтр по тенанту только помешал бы.
//
// Одной колонкой owner дело не ограничивается, и это та же форма тихой ошибки,
// что заглушки с NotFound: versitygw решает, кто владелец, не по buckets.owner,
// а по ACL, который мы отдаём через GetBucketAcl. Если обновить только owner,
// операция снаружи выглядит успешной, а права остаются старыми — новый владелец
// (роль user, ради которой метод и нужен) доступа не получает, а прежний
// сохраняет FULL_CONTROL и через ACL, и через старую политику.
//
// Posix (auth.UpdateBucketACLOwner) делает три шага: пишет ACL, где владелец —
// новый аккаунт и единственный грант FULL_CONTROL тоже ему, затем удаляет
// политику бакета. Мы делаем то же одним UPDATE: сбой посередине не оставит
// бакет с новым владельцем и старым ACL, что при трёх вызовах бэкенда возможно.
//
// acl передаётся собранным извне: формат задают структуры versitygw, а пакет
// meta про S3-обвязку знать не должен.
func (m *Meta) ChangeBucketOwner(ctx context.Context, tenant TenantID, bucket, owner string, acl []byte) error {
	q, args := withTenant(
		`UPDATE buckets
		    SET owner = $2,
		        acl = $3,
		        policy = NULL
		  WHERE name = $1 AND deleted_at IS NULL`,
		tenant, bucket, owner, acl)
	tag, err := m.Pool.Exec(ctx, q, args...)
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
