package backend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	stypes "github.com/aws/aws-sdk-go-v2/service/s3/types"
	vbackend "github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"

	"github.com/versity/versitygw/auth"

	"github.com/vladsimachkov/tapegw/internal/meta"
)

// Bucket Policy, смена владельца и админское перечисление.
//
// Поведение скопировано с posix-бэкенда versitygw (backend/posix/posix.go),
// а не придумано: он эталон для обычных операций. Три различимых ответа, и
// все три значат разное:
//
//	имя бакета невалидно      → ErrInvalidBucketName
//	бакета нет                → ErrNoSuchBucket
//	бакет есть, политики нет  → ErrNoSuchBucketPolicy
//
// Последний код — единственный, который versitygw считает штатным ответом на
// GetBucketPolicy. Он вызывается на каждом запросе (auth/access-control.go:421,
// :534) и в проверке Object Lock (auth/object_lock.go:335,349); любая другая
// ошибка там обрывает запрос, поэтому без этого кода любой не-root ключ получил
// бы 500 на всё подряд.

// PutBucketPolicy сохраняет документ дословно. Валидацию прошёл versitygw
// (auth.ValidatePolicyDocument) до вызова: первая '{', парсится, Statement
// непустой, каждый principal существует в IAM.
//
// nil вместо документа — это удаление политики, а не «записать пустую»; так же
// устроен posix. Удаление отсутствующей политики не ошибка.
func (t *TapeGW) PutBucketPolicy(ctx context.Context, bucket string, policy []byte) error {
	tenant, err := tenantFromContext(ctx)
	if err != nil {
		return err
	}
	if err := validateBucketName(bucket); err != nil {
		return s3err.GetBucketErr(s3err.ErrInvalidBucketName, bucket)
	}
	if err := t.meta.SetBucketPolicy(ctx, tenant, bucket, policy); err != nil {
		return t.bucketErr(ctx, bucket, tenant, err)
	}
	return nil
}

// GetBucketPolicy отдаёт байты ровно в том виде, в каком их прислал клиент.
// Колонка в БД — text именно поэтому: jsonb пересобрал бы документ и
// переставил ключи.
func (t *TapeGW) GetBucketPolicy(ctx context.Context, bucket string) ([]byte, error) {
	// Этот метод versitygw дёргает на каждом запросе (auth/access-control.go:421)
	// и в проверке Object Lock, всегда с тем же контекстом. Без отказа здесь
	// чужой бакет стал бы различим по тому, на каком шагу пришёл отказ.
	tenant, err := tenantFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := validateBucketName(bucket); err != nil {
		return nil, s3err.GetBucketErr(s3err.ErrInvalidBucketName, bucket)
	}
	policy, err := t.meta.GetBucketPolicy(ctx, tenant, bucket)
	if err != nil {
		return nil, t.bucketErr(ctx, bucket, tenant, err)
	}
	return policy, nil
}

// DeleteBucketPolicy — тот же путь, что PutBucketPolicy(nil), как в posix.
func (t *TapeGW) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	return t.PutBucketPolicy(ctx, bucket, nil)
}

// ChangeBucketOwner — admin API (PATCH /change-bucket-owner). versitygw перед
// вызовом проверяет по IAM, что новый владелец существует, так что здесь
// остаётся привести права в соответствие.
//
// ACL собирается из структур самого versitygw (auth.ACL / auth.Grantee), а не
// строкой: json.Marshal даёт ровно те байты, что даёт auth.UpdateBucketACLOwner
// в posix, и если versitygw переименует поле — мы узнаем об этом на сборке,
// а не на первом же GetBucketAcl.
func (t *TapeGW) ChangeBucketOwner(ctx context.Context, bucket, owner string) error {
	// Admin API: versitygw не пускает сюда не-администратора (middlewares.IsAdmin),
	// поэтому страж здесь отдаёт обход по роли. Отказ для обычного ключа —
	// вторая линия: если маршрут когда-нибудь поедет, метод не должен остаться
	// «дырявым».
	tenant, err := tenantFromContext(ctx)
	if err != nil {
		return err
	}
	if err := validateBucketName(bucket); err != nil {
		return s3err.GetBucketErr(s3err.ErrInvalidBucketName, bucket)
	}

	acl, err := json.Marshal(auth.ACL{
		Owner: owner,
		Grantees: []auth.Grantee{{
			Permission: auth.PermissionFullControl,
			Access:     owner,
			Type:       stypes.TypeCanonicalUser,
		}},
	})
	if err != nil {
		return fmt.Errorf("marshal ACL for %q: %w", bucket, err)
	}

	// Видимость бакета этим тенантом проверяется ДО разбора учётки-получателя.
	// Иначе отказ «такой владелец не найден» заступил бы стража: чужой тенант
	// получил бы InvalidRequest вместо AccessDenied, и тест-перебор перестал бы
	// ловить снятый страж (на нём это и поймалось).
	if ok, err := t.meta.BucketExists(ctx, tenant, bucket); err != nil {
		return mapMetaErr(err, bucket)
	} else if !ok {
		return t.bucketErr(ctx, bucket, tenant, meta.ErrNotFound)
	}

	// Тенант нового владельца берётся из IAM (план PR#2b). Без этого бакет
	// оставался бы в прежнем тенанте, и страж отказывал бы новому владельцу во
	// всех запросах — основной сценарий роли user не работал бы вовсе.
	//
	// Отозванная учётка не может принять бакет: надгробие (PR#2a) сохраняет
	// строку, и «передать ключ, которым уже нельзя войти» — значит спрятать
	// бакет в мёртвом тенанте.
	acc, err := t.meta.GetAccount(ctx, owner)
	if errors.Is(err, meta.ErrNotFound) {
		return s3err.GetAPIError(s3err.ErrInvalidRequest)
	}
	if err != nil {
		return fmt.Errorf("account %q: %w", owner, err)
	}
	if acc.Status != meta.AccountActive {
		return s3err.GetAPIError(s3err.ErrInvalidRequest)
	}

	if err := t.meta.ChangeBucketOwner(ctx, tenant, bucket, owner, acc.TenantID, acl); err != nil {
		if errors.Is(err, meta.ErrBucketNotEmpty) {
			// Не «внутренняя ошибка», а осмысленный отказ: перенос между
			// тенантами с объектами внутри запрещён намеренно.
			return s3err.GetBucketErr(s3err.ErrInvalidRequest, bucket)
		}
		return t.bucketErr(ctx, bucket, tenant, err)
	}
	return nil
}

// ListBucketsAndOwners — admin API (PATCH /list-buckets): все бакеты и их
// владельцы, без фильтра по владельцу.
func (t *TapeGW) ListBucketsAndOwners(ctx context.Context) ([]s3response.Bucket, error) {
	rows, err := t.meta.ListAllBucketsAndOwners(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]s3response.Bucket, 0, len(rows))
	for _, r := range rows {
		out = append(out, s3response.Bucket{Name: r.Name, Owner: r.Owner})
	}
	return out, nil
}

// compile-time: методы перестали быть заглушками BackendUnsupported.
var _ vbackend.Backend = (*TapeGW)(nil)
