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
	"github.com/vladsimachkov/tapegw/internal/spool"
)

// TapeGW — бэкенд S3-поверхности. Состояние только в Postgres (meta) и на
// диске/ленте; в самой структуре мутируемых полей нет, поэтому она безопасна
// для конкурентного вызова.
type TapeGW struct {
	*vbackend.BackendUnsupported

	meta  *meta.Meta
	cfg   *config.Config
	spool *spool.Spool
}

// compile-time check: не даём интерфейсу versitygw тихо уехать вперёд.
var _ vbackend.Backend = (*TapeGW)(nil)

// New собирает бэкенд. meta обязателен: без метаданных бэкенд не может
// ответить ни на один запрос. spool — дисковый буфер landing; без него PutObject
// некуда писать, поэтому nil на месте spool означает «объектные операции
// выключены» и они честно вернут NotImplemented.
func New(m *meta.Meta, s *spool.Spool, cfg *config.Config) *TapeGW {
	return &TapeGW{
		BackendUnsupported: &vbackend.BackendUnsupported{},
		meta:               m,
		cfg:                cfg,
		spool:              s,
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

// ListBuckets — ListObjects для бакетов. Страж здесь не проверяет, а фильтрует:
// внутри тенанта владельцы будут разные, когда появятся несколько ключей.
// В режиме администратора versitygw просит все бакеты (Owner пустой), и
// обход по роли снимает фильтр тенанта. Префикс, max-buckets и continuation
// token обрабатывает SQL — см. meta.ListBuckets.
func (t *TapeGW) ListBuckets(ctx context.Context, input s3response.ListBucketsInput) (s3response.ListAllMyBucketsResult, error) {
	tenant, err := tenantFromContext(ctx)
	if err != nil {
		return s3response.ListAllMyBucketsResult{}, err
	}

	ownerFilter := input.Owner
	if input.IsAdmin {
		ownerFilter = ""
	}

	after, err := meta.DecodeToken(input.ContinuationToken)
	if err != nil {
		return s3response.ListAllMyBucketsResult{}, s3err.GetAPIError(s3err.ErrInvalidToken)
	}

	buckets, next, err := t.meta.ListBuckets(ctx, tenant, meta.ListBucketsOpt{
		Owner:  ownerFilter,
		Prefix: input.Prefix,
		Max:    int(input.MaxBuckets),
		After:  after,
	})
	if err != nil {
		return s3response.ListAllMyBucketsResult{}, mapMetaErr(err, "")
	}

	res := s3response.ListAllMyBucketsResult{
		Owner:  s3response.CanonicalUser{ID: ownerFilter, DisplayName: ownerFilter},
		Prefix: input.Prefix,
		// В ListAllMyBucketsResult versitygw нет отдельного поля NextContinuationToken:
		// по соглашению самих бэкендов (см. backend/azure) следующим токеном служит
		// то же поле ContinuationToken, и оно **пустое, если следующей страницы нет**.
		// Экхо входного токена зацикливает пагинатор: на последней странице клиент
		// получил бы обратно свой же токен и запросил бы её же снова.
		ContinuationToken: next,
	}
	for _, b := range buckets {
		res.Buckets.Bucket = append(res.Buckets.Bucket, s3response.ListAllMyBucketsEntry{
			Name:         b.Name,
			CreationDate: b.CreatedAt,
		})
	}
	return res, nil
}

func (t *TapeGW) HeadBucket(ctx context.Context, input *s3.HeadBucketInput) (*s3.HeadBucketOutput, error) {
	tenant, err := tenantFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := t.meta.GetBucket(ctx, tenant, *input.Bucket); err != nil {
		return nil, t.bucketErr(ctx, *input.Bucket, tenant, err)
	}
	return &s3.HeadBucketOutput{}, nil
}

// CreateBucket — единственный метод с бакетом, где страж не проверяет, а
// назначает: бакета ещё нет, сравнивать не с чем. Отказ при этом остаётся:
// аноним (ProjectID == 0) не получает права заводить бакет.
//
// Учётка с обходом по роли не имеет ProjectID, поэтому ей назначается
// тенант `default` — см. meta.AdminTenantID.
func (t *TapeGW) CreateBucket(ctx context.Context, input *s3.CreateBucketInput, defaultACL []byte) error {
	tenant, err := tenantFromContext(ctx)
	if err != nil {
		return err
	}
	if tenant.IsBypass() {
		tenant = meta.AdminTenantID()
	}

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
	existing, err := t.meta.CreateBucket(ctx, tenant, name, owner(ctx), defaultACL, tags)
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
	tenant, err := tenantFromContext(ctx)
	if err != nil {
		return err
	}
	err = t.meta.DeleteBucket(ctx, tenant, bucket)
	if err != nil {
		if errors.Is(err, meta.ErrBucketNotEmpty) {
			return s3err.GetBucketErr(s3err.ErrBucketNotEmpty, bucket)
		}
		return t.bucketErr(ctx, bucket, tenant, err)
	}
	return nil
}

func (t *TapeGW) GetBucketAcl(ctx context.Context, input *s3.GetBucketAclInput) ([]byte, error) {
	// Этот метод versitygw вызывает сам в ходе проверки доступа, с тем же
	// контекстом. Если здесь отказа не будет, чужой бакет станет различим по
	// тому, на каком шагу пришёл отказ (ADR-0017, п. 4).
	tenant, err := tenantFromContext(ctx)
	if err != nil {
		return nil, err
	}
	b, err := t.meta.GetBucket(ctx, tenant, *input.Bucket)
	if err != nil {
		return nil, t.bucketErr(ctx, *input.Bucket, tenant, err)
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
	tenant, err := tenantFromContext(ctx)
	if err != nil {
		return err
	}
	if err := t.meta.SetBucketACL(ctx, tenant, bucket, data); err != nil {
		return t.bucketErr(ctx, bucket, tenant, err)
	}
	return nil
}

func (t *TapeGW) GetBucketTagging(ctx context.Context, bucket string) (map[string]string, error) {
	tenant, err := tenantFromContext(ctx)
	if err != nil {
		return nil, err
	}
	b, err := t.meta.GetBucket(ctx, tenant, bucket)
	if err != nil {
		return nil, t.bucketErr(ctx, bucket, tenant, err)
	}
	if len(b.Tags) == 0 {
		return nil, s3err.GetBucketErr(s3err.ErrBucketTaggingNotFound, bucket)
	}
	return b.Tags, nil
}

func (t *TapeGW) PutBucketTagging(ctx context.Context, bucket string, tags map[string]string) error {
	tenant, err := tenantFromContext(ctx)
	if err != nil {
		return err
	}
	if err := t.meta.SetBucketTags(ctx, tenant, bucket, tags); err != nil {
		return t.bucketErr(ctx, bucket, tenant, err)
	}
	return nil
}

func (t *TapeGW) DeleteBucketTagging(ctx context.Context, bucket string) error {
	tenant, err := tenantFromContext(ctx)
	if err != nil {
		return err
	}
	if err := t.meta.SetBucketTags(ctx, tenant, bucket, nil); err != nil {
		return t.bucketErr(ctx, bucket, tenant, err)
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
