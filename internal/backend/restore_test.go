package backend_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/versity/versitygw/s3err"
)

// RestoreObject (S4): успех — всегда 200 (ADR-0001), состояние клиент читает
// x-amz-restore; живой копии очередь не ставится (критерий ревью S4).

func restoreInput(bucket, key string, days int32, tier types.Tier) *s3.RestoreObjectInput {
	return &s3.RestoreObjectInput{
		Bucket: ptr(bucket), Key: ptr(key),
		RestoreRequest: &types.RestoreRequest{
			Days:                 ptr(days),
			GlacierJobParameters: &types.GlacierJobParameters{Tier: tier},
		},
	}
}

func TestRestoreObjectQueuesArchived(t *testing.T) {
	be, m, _ := newBackend(t)
	ctx := testCtx()
	mustBucket(t, be)
	putBody(t, be, "b", "k", "данные")

	var id uuid.UUID
	if err := m.Pool.QueryRow(ctx, `SELECT id FROM objects WHERE key='k'`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Pool.Exec(ctx,
		`UPDATE objects SET state='archived', landing=false, archived_at=now(),
		        online_until = now() - interval '1 hour' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}

	if err := be.RestoreObject(ctx, restoreInput("b", "k", 7, types.TierStandard)); err != nil {
		t.Fatalf("новый restore обязан быть 200 (ADR-0001), получили %v", err)
	}

	var state string
	var days int
	if err := m.Pool.QueryRow(ctx,
		`SELECT state, days FROM restores WHERE object_id=$1`, id).Scan(&state, &days); err != nil {
		t.Fatalf("строка restores: %v", err)
	}
	if state != "requested" || days != 7 {
		t.Errorf("restores: %v/%v, ждали requested/7", state, days)
	}

	// HEAD показывает ongoing-request="true" — по нему клиент опрашивает
	// статус после RestoreObject (блокер из ревью M2 про HEAD).
	head, err := be.HeadObject(ctx, &s3.HeadObjectInput{Bucket: ptr("b"), Key: ptr("k")})
	if err != nil {
		t.Fatalf("HEAD: %v", err)
	}
	if head.Restore == nil || *head.Restore != `ongoing-request="true"` {
		t.Errorf("после запроса заголовок = %v", head.Restore)
	}

	// GET при этом остаётся InvalidObjectState: копия ещё не готова.
	if _, err := be.GetObject(ctx, &s3.GetObjectInput{Bucket: ptr("b"), Key: ptr("k")}); !isS3Code(err, s3err.ErrInvalidObjectState) {
		t.Errorf("GET до восстановления: %v, ждали InvalidObjectState", err)
	}

	// Повторный запрос — RestoreAlreadyInProgress (409), а не второй 200.
	if err := be.RestoreObject(ctx, restoreInput("b", "k", 30, types.TierBulk)); !isS3CodeString(err, "RestoreAlreadyInProgress") {
		t.Errorf("повторный restore: %v, ждали RestoreAlreadyInProgress", err)
	}
}

// Объект с живой копией в landing: только продление, в очередь recaller'а не
// ставится (критерий ревью S4), HEAD сразу показывает выданную копию.
func TestRestoreObjectLiveCopyExtendsOnly(t *testing.T) {
	be, m, _ := newBackend(t)
	ctx := testCtx()
	mustBucket(t, be)
	putBody(t, be, "b", "k", "данные")

	var id uuid.UUID
	if err := m.Pool.QueryRow(ctx, `SELECT id FROM objects WHERE key='k'`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Pool.Exec(ctx,
		`UPDATE objects SET state='archived', landing=true, archived_at=now(),
		        online_until = now() + interval '1 hour' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}

	if err := be.RestoreObject(ctx, restoreInput("b", "k", 7, types.TierStandard)); err != nil {
		t.Fatalf("restore живой копии: %v", err)
	}
	var n int
	if err := m.Pool.QueryRow(ctx, `SELECT count(*) FROM restores WHERE object_id=$1`, id).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("живая копия поставлена в очередь restores (%d строк) — recaller гонялся бы за данными, которые на диске", n)
	}
	var until time.Time
	if err := m.Pool.QueryRow(ctx, `SELECT online_until FROM objects WHERE id=$1`, id).Scan(&until); err != nil {
		t.Fatal(err)
	}
	if !until.After(time.Now().Add(6 * 24 * time.Hour)) {
		t.Errorf("online_until %v не продлён на 7 дней", until)
	}

	head, err := be.HeadObject(ctx, &s3.HeadObjectInput{Bucket: ptr("b"), Key: ptr("k")})
	if err != nil {
		t.Fatal(err)
	}
	if head.Restore == nil || *head.Restore == `ongoing-request="true"` {
		t.Errorf("живая копия обязана быть показана выданной: %v", head.Restore)
	}
}

// Не-архив: restore нечего восстанавливать — InvalidObjectState, как AWS для
// не-Glacier объекта.
func TestRestoreObjectNotArchived(t *testing.T) {
	be, _, _ := newBackend(t)
	ctx := testCtx()
	mustBucket(t, be)
	putBody(t, be, "b", "k", "данные") // pending

	if err := be.RestoreObject(ctx, restoreInput("b", "k", 7, types.TierStandard)); !isS3Code(err, s3err.ErrInvalidObjectState) {
		t.Errorf("restore pending: %v, ждали InvalidObjectState", err)
	}
}

// Разбор тела: Days обязателен и ограничен 1..365 (предел AWS Glacier),
// tier — три значения, незнакомый — InvalidRequest.
func TestRestoreObjectValidation(t *testing.T) {
	be, m, _ := newBackend(t)
	ctx := testCtx()
	mustBucket(t, be)
	putBody(t, be, "b", "k", "данные")
	if _, err := m.Pool.Exec(ctx,
		`UPDATE objects SET state='archived', landing=false, archived_at=now(),
		        online_until = now() - interval '1 hour'`); err != nil {
		t.Fatal(err)
	}

	bad := []struct {
		name string
		in   *s3.RestoreObjectInput
	}{
		{"без Days", &s3.RestoreObjectInput{Bucket: ptr("b"), Key: ptr("k"),
			RestoreRequest: &types.RestoreRequest{}}},
		{"Days=0", restoreInput("b", "k", 0, types.TierStandard)},
		{"Days=366", restoreInput("b", "k", 366, types.TierStandard)},
		{"tier не из списка", restoreInput("b", "k", 7, types.Tier("DeepArchive"))},
	}
	for _, c := range bad {
		if err := be.RestoreObject(ctx, c.in); !isS3Code(err, s3err.ErrInvalidRequest) {
			t.Errorf("%s: %v, ждали InvalidRequest", c.name, err)
		}
	}
	// Пустое тело — MalformedXML (при прямом вызове; контроллер versitygw
	// отсекает раньше).
	if err := be.RestoreObject(ctx, &s3.RestoreObjectInput{Bucket: ptr("b"), Key: ptr("k")}); !isS3Code(err, s3err.ErrMalformedXML) {
		t.Errorf("пустое тело: %v, ждали MalformedXML", err)
	}
	// Ничего из перечисленного не создало очередь.
	var n int
	if err := m.Pool.QueryRow(ctx, `SELECT count(*) FROM restores`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("отвергнутые запросы создали %d строк restores", n)
	}
}

// Адресация версии — тот же путь readVersion, что у GET и HEAD: версия
// находится по versionId, чужой id даёт NoSuchVersion, «null» на
// неверсионированном бакете работает как в GET.
func TestRestoreObjectVersionId(t *testing.T) {
	be, m, _ := newBackend(t)
	ctx := testCtx()
	mustBucket(t, be)
	if err := be.PutBucketVersioning(ctx, "b", types.BucketVersioningStatusEnabled); err != nil {
		t.Fatal(err)
	}
	putBody(t, be, "b", "k", "данные")

	var id uuid.UUID
	if err := m.Pool.QueryRow(ctx, `SELECT id FROM objects WHERE key='k'`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Pool.Exec(ctx,
		`UPDATE objects SET state='archived', landing=false, archived_at=now(),
		        online_until = now() - interval '1 hour' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}

	in := restoreInput("b", "k", 7, types.TierStandard)
	in.VersionId = ptr(id.String())
	if err := be.RestoreObject(ctx, in); err != nil {
		t.Fatalf("restore по versionId: %v", err)
	}

	foreign := uuid.NewString()
	in.VersionId = &foreign
	if err := be.RestoreObject(ctx, in); !isS3Code(err, s3err.ErrNoSuchVersion) {
		t.Errorf("restore по несуществующей версии: %v, ждали NoSuchVersion", err)
	}
}
