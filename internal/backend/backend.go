// Package backend — реализация backend.Backend из versitygw поверх Postgres
// и Phobos.
//
// Бэкенд встраивает backend.BackendUnsupported, поэтому любая не реализованная
// пока операция возвращает NotImplemented сама — это правило 4 из AGENTS.md:
// неподдерживаемое выражается явно, а не «молча делается как-нибудь».
//
// Реализовано на этапе M1: бакеты (Create/Head/List/Delete, ACL, tagging).
package backend

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/versity/versitygw/auth"
	vbackend "github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"

	"github.com/vladsimachkov/tapegw/internal/config"
	"github.com/vladsimachkov/tapegw/internal/meta"
	tapegwerr "github.com/vladsimachkov/tapegw/internal/s3err"
)

// TapeGW — бэкенд S3-поверхности. Состояние только в Postgres (meta) и на
// диске/ленте; в самой структуре мутируемых полей нет, поэтому она безопасна
// для конкурентного вызова.
type TapeGW struct {
	*vbackend.BackendUnsupported

	meta *meta.Meta
	cfg  *config.Config
}

// compile-time check: не даём интерфейсу versitygw тихо уехать вперёд.
var _ vbackend.Backend = (*TapeGW)(nil)

// New собирает бэкенд. meta обязателен: без метаданных бэкенд не может
// ответить ни на один запрос.
func New(m *meta.Meta, cfg *config.Config) *TapeGW {
	return &TapeGW{
		BackendUnsupported: &vbackend.BackendUnsupported{},
		meta:               m,
		cfg:                cfg,
	}
}

func (t *TapeGW) String() string { return "tapegw" }

// Shutdown вызывается versitygw при остановке. Соединения закрывает вызывающий
// через meta.Close, здесь только то, что принадлежит бэкенду.
func (t *TapeGW) Shutdown() {}

// owner извлекает аккаунт запроса. Кладёт его в контекст сам versitygw под
// ключом "bucket-owner" (s3api/internal/httpctx: ContextKeyBucketOwner).
//
// Типизированный доступ utils.ContextKeyBucketOwner.Get требует fiber.Ctx,
// а в бэкенд приходит обычный context.Context (versitygw сам делает
// ctx.RequestCtx()), поэтому остаётся строковый ключ — тот же приём, что у
// posix-бэкенда.
func owner(ctx context.Context) string {
	if acct, ok := ctx.Value("bucket-owner").(auth.Account); ok && acct.Access != "" {
		return acct.Access
	}
	return "anonymous"
}

// ListBuckets — ListObjects для бакетов. В режиме администратора versitygw
// просит все бакеты (Owner пустой). Префикс, max-buckets и continuation token
// обрабатывает SQL — см. meta.ListBuckets.
func (t *TapeGW) ListBuckets(ctx context.Context, input s3response.ListBucketsInput) (s3response.ListAllMyBucketsResult, error) {
	ownerFilter := input.Owner
	if input.IsAdmin {
		ownerFilter = ""
	}

	after, err := meta.DecodeToken(input.ContinuationToken)
	if err != nil {
		return s3response.ListAllMyBucketsResult{}, s3err.GetAPIError(s3err.ErrInvalidToken)
	}

	buckets, next, err := t.meta.ListBuckets(ctx, meta.ListBucketsOpt{
		Owner:  ownerFilter,
		Prefix: input.Prefix,
		Max:    int(input.MaxBuckets),
		After:  after,
	})
	if err != nil {
		return s3response.ListAllMyBucketsResult{}, mapMetaErr(err, "")
	}

	res := s3response.ListAllMyBucketsResult{
		Owner:             s3response.CanonicalUser{ID: ownerFilter, DisplayName: ownerFilter},
		Prefix:            input.Prefix,
		ContinuationToken: input.ContinuationToken,
	}
	for _, b := range buckets {
		res.Buckets.Bucket = append(res.Buckets.Bucket, s3response.ListAllMyBucketsEntry{
			Name:         b.Name,
			CreationDate: b.CreatedAt,
		})
	}
	// В ListAllMyBucketsResult versitygw нет отдельного поля NextContinuationToken:
	// по соглашению самих бэкендов (см. backend/azure) следующим токеном служит
	// то же поле ContinuationToken.
	if next != "" {
		res.ContinuationToken = next
	}
	return res, nil
}

func (t *TapeGW) HeadBucket(ctx context.Context, input *s3.HeadBucketInput) (*s3.HeadBucketOutput, error) {
	if _, err := t.meta.GetBucket(ctx, *input.Bucket); err != nil {
		return nil, mapMetaErr(err, *input.Bucket)
	}
	return &s3.HeadBucketOutput{}, nil
}

func (t *TapeGW) CreateBucket(ctx context.Context, input *s3.CreateBucketInput, defaultACL []byte) error {
	name := *input.Bucket
	if err := validateBucketName(name); err != nil {
		return err
	}

	// Разбор тегов берём из versitygw: там уже есть лимит в 50 тегов и нужные
	// коды ошибок (правило 5).
	var rawTags []types.Tag
	if input.CreateBucketConfiguration != nil {
		rawTags = input.CreateBucketConfiguration.Tags
	}
	tags, err := vbackend.ParseCreateBucketTags(rawTags)
	if err != nil {
		return err
	}

	// Теги пишутся тем же INSERT, что и бакет: отдельным запросом они бы
	// потерялись при падении между запросами, а клиент получил бы 200.
	existing, err := t.meta.CreateBucket(ctx, name, owner(ctx), defaultACL, tags)
	switch {
	case err == nil && existing:
		// Имя занято нашим же бакетом. В AWS это 409 BucketAlreadyOwnedByYou,
		// а не 200 — см. backend/posix/posix.go.
		return s3err.GetBucketErr(s3err.ErrBucketAlreadyOwnedByYou, name)
	case err == nil:
		return nil
	case errors.Is(err, meta.ErrBucketOwnedByOther):
		// Имя занято чужим бакетом.
		return s3err.GetBucketErr(s3err.ErrBucketAlreadyExists, name)
	default:
		return mapMetaErr(err, name)
	}
}

func (t *TapeGW) DeleteBucket(ctx context.Context, bucket string) error {
	err := t.meta.DeleteBucket(ctx, bucket)
	if err != nil {
		if errors.Is(err, meta.ErrBucketNotEmpty) {
			return s3err.GetBucketErr(s3err.ErrBucketNotEmpty, bucket)
		}
		return mapMetaErr(err, bucket)
	}
	return nil
}

func (t *TapeGW) GetBucketAcl(ctx context.Context, input *s3.GetBucketAclInput) ([]byte, error) {
	b, err := t.meta.GetBucket(ctx, *input.Bucket)
	if err != nil {
		return nil, mapMetaErr(err, *input.Bucket)
	}
	if len(b.ACL) == 0 {
		// CreateBucket всегда пишет ACL, который ему передал versitygw, поэтому
		// пустой ACL здесь — рассинхрон БД, а не штатная ситуация. Не выдумываем
		// код: отдаём внутреннюю ошибку, ProcessController превратит её в 500
		// и залогирует с первоначальной причиной.
		return nil, fmt.Errorf("bucket %q has no stored acl", *input.Bucket)
	}
	return b.ACL, nil
}

func (t *TapeGW) PutBucketAcl(ctx context.Context, bucket string, data []byte) error {
	if err := t.meta.SetBucketACL(ctx, bucket, data); err != nil {
		return mapMetaErr(err, bucket)
	}
	return nil
}

func (t *TapeGW) GetBucketTagging(ctx context.Context, bucket string) (map[string]string, error) {
	b, err := t.meta.GetBucket(ctx, bucket)
	if err != nil {
		return nil, mapMetaErr(err, bucket)
	}
	if len(b.Tags) == 0 {
		return nil, s3err.GetBucketErr(s3err.ErrBucketTaggingNotFound, bucket)
	}
	return b.Tags, nil
}

func (t *TapeGW) PutBucketTagging(ctx context.Context, bucket string, tags map[string]string) error {
	if err := t.meta.SetBucketTags(ctx, bucket, tags); err != nil {
		return mapMetaErr(err, bucket)
	}
	return nil
}

func (t *TapeGW) DeleteBucketTagging(ctx context.Context, bucket string) error {
	if err := t.meta.SetBucketTags(ctx, bucket, nil); err != nil {
		return mapMetaErr(err, bucket)
	}
	return nil
}

// mapMetaErr переводит ошибки слоя meta в коды S3. Ошибку БД превращать в
// InternalError не надо — это делает ProcessController в versitygw.
func mapMetaErr(err error, bucket string) error {
	return tapegwerr.FromMeta(err, bucket)
}

// validateBucketName — быстрая проверка до обращения к БД. Полные правила
// валидации имён бакетов уже есть в versitygw (strict mode включён по умолчанию
// в embedgw), поэтому здесь только то, без чего запрос ушёл бы в базу зря.
func validateBucketName(name string) error {
	if name == "" {
		return s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	return nil
}
