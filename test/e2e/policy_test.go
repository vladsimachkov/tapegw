package e2e

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Проверка доступа под обычным ключом.
//
// Пока GetBucketPolicy возвращал NotImplemented, любой не-root ключ получал 501
// на всё: versitygw вызывает его на каждом запросе и терпит ровно одну ошибку,
// ErrNoSuchBucketPolicy (auth/access-control.go:421). Тесты на root этого не
// ловили — root и admin идут мимо политик и ACL вообще.
//
// Сценарий строится **внутри клиентского тенанта 2** двумя его ключами
// (ADR-0018): owner создаёт бакет и выдаёт права, user читает. Прежний вид
// теста — бакет создаёт root, а читает userplus — по факту проверял доступ к
// бакету системного тенанта. Разница для зелёного цвета не видна, поэтому здесь
// она проверяется явно: отказ без политики и разрешение с политикой получают два
// ключа одного тенанта, а не клиент и системный тенант.

// policyFor — документ, дающий read только нашему пользователю. Principal в
// файловом IAM — это access key, не ARN.
func policyFor(bucket, user string) []byte {
	return []byte(fmt.Sprintf(
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"%s"},`+
			`"Action":["s3:GetObject"],"Resource":["arn:aws:s3:::%s/*"]}]}`, user, bucket))
}

func isAccessDenied(err error) bool {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode() == "AccessDenied"
	}
	return false
}

func TestBucketPolicyGrantsUser(t *testing.T) {
	owner := ownerClient(t)
	user := userClient(t)
	ctx := context.Background()
	bucket := uniqueBucket("pol")

	// Бакет создаёт ключ тенанта 2, значит и лежать он должен в тенанте 2.
	// Если бы бакет достался системному тенанту, шаг 2 ниже не прошёл бы:
	// страж отказывает раньше, чем политика успевает что-то разрешить.
	if _, err := owner.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	body := []byte("secret payload")
	if _, err := owner.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k"), Body: bytes.NewReader(body),
	}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	// 1. Без политики — отказ. Именно этот шаг раньше давал 501.
	_, err := user.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k"),
	})
	if !isAccessDenied(err) {
		t.Fatalf("без политики получили %v, хотели AccessDenied", err)
	}

	// 2. Политика выдаёт право — доступ появляется. Этот шаг и есть
	// доказательство, что отказ выше пришёл от ACL и политики, а не от стража
	// тенанта: чужому бакету политика не помог бы.
	if _, err := owner.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{
		Bucket: aws.String(bucket), Policy: aws.String(string(policyFor(bucket, userAccess(t)))),
	}); err != nil {
		t.Fatalf("PutBucketPolicy: %v", err)
	}
	out, err := user.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k"),
	})
	if err != nil {
		t.Fatalf("с политикой GetObject не прошёл: %v", err)
	}
	got, err := io.ReadAll(out.Body)
	out.Body.Close()
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("тело не совпало: %q", got)
	}

	// 3. Политика возвращается дословно — колонка text затем и была, чтобы
	//    jsonb не переставил ключи.
	p, err := owner.GetBucketPolicy(ctx, &s3.GetBucketPolicyInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatalf("GetBucketPolicy: %v", err)
	}
	if p.Policy == nil || *p.Policy != string(policyFor(bucket, userAccess(t))) {
		t.Errorf("политика вернулась не дословно: %s", aws.ToString(p.Policy))
	}

	// 4. Сняли политику — снова отказ.
	if _, err := owner.DeleteBucketPolicy(ctx, &s3.DeleteBucketPolicyInput{
		Bucket: aws.String(bucket),
	}); err != nil {
		t.Fatalf("DeleteBucketPolicy: %v", err)
	}
	if _, err := user.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k"),
	}); !isAccessDenied(err) {
		t.Fatalf("после снятия политики получили %v, хотели AccessDenied", err)
	}

	// 5. У бакета без политики — штатный NoSuchBucketPolicy, а не 500.
	_, err = owner.GetBucketPolicy(ctx, &s3.GetBucketPolicyInput{Bucket: aws.String(bucket)})
	var ae smithy.APIError
	if !errors.As(err, &ae) || ae.ErrorCode() != "NoSuchBucketPolicy" {
		t.Fatalf("без политики ответ %v, хотели NoSuchBucketPolicy", err)
	}
}

// Политика не переписывает тенант: бакет системного тенанта остаётся закрытым
// для ключа клиентского тенанта, даже когда документ прямо его разрешает.
// Иначе «политика» стала бы способом перебраться через страж (ADR-0017).
func TestBucketPolicyDoesNotCrossTenants(t *testing.T) {
	root := client(t)
	user := userClient(t)
	ctx := context.Background()
	bucket := uniqueBucket("xten")

	// Бакет создаёт root — он попадает в системный тенант (ADR-0018), а у
	// ключа стенда тенант клиентский.
	if _, err := root.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if _, err := root.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k"), Body: bytes.NewReader([]byte("foreign")),
	}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if _, err := root.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{
		Bucket: aws.String(bucket), Policy: aws.String(string(policyFor(bucket, userAccess(t)))),
	}); err != nil {
		t.Fatalf("PutBucketPolicy: %v", err)
	}

	_, err := user.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k"),
	})
	if !isAccessDenied(err) {
		t.Fatalf("чужой бакет с разрешающей политикой вернул %v, хотели AccessDenied", err)
	}
}

// Владелец бакета может читать, менять и удалять его политику независимо от
// того, что в ней написано, — иначе документ мог бы закрыть владельцу доступ к
// собственному бакету и починить его было бы нельзя. Проверяем, что отказ по
// «запрещающей всё» политике не распространяется на саму политику.
func TestBucketPolicyOwnerNotLockedOut(t *testing.T) {
	root := client(t)
	ctx := context.Background()
	bucket := uniqueBucket("lock")

	if _, err := root.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	deny := `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":{"AWS":"*"},` +
		`"Action":["s3:*"],"Resource":["arn:aws:s3:::` + bucket + `","arn:aws:s3:::` + bucket + `/*"]}]}`
	if _, err := root.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{
		Bucket: aws.String(bucket), Policy: aws.String(deny),
	}); err != nil {
		t.Fatalf("PutBucketPolicy(deny all): %v", err)
	}

	if _, err := root.GetBucketPolicy(ctx, &s3.GetBucketPolicyInput{
		Bucket: aws.String(bucket),
	}); err != nil {
		t.Fatalf("владелец не может прочитать политику: %v", err)
	}
	if _, err := root.DeleteBucketPolicy(ctx, &s3.DeleteBucketPolicyInput{
		Bucket: aws.String(bucket),
	}); err != nil {
		t.Fatalf("владелец не может удалить политику: %v", err)
	}
}

// standDB — подключение к базе стенда. Нужно затем, чтобы писать ACL и политику
// мимо API: через API межтенантный грант не пройдёт валидацию ADR-0010, и это
// правильно. Без прямой записи в БД тест проверял бы ACL versitygw, а не страж
// тенанта — на PR#2a именно так и выходило.
func standDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TAPEGW_E2E_DSN")
	if dsn == "" {
		if os.Getenv("TAPEGW_REQUIRE_ACCESS_TESTS") == "1" {
			t.Fatal("TAPEGW_E2E_DSN не задан, а доступ обязателен")
		}
		t.Skip("TAPEGW_E2E_DSN не задан — пропускаем")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func standUser(t *testing.T, access, secret, label string) *s3.Client {
	t.Helper()
	if access == "" || secret == "" {
		if os.Getenv("TAPEGW_REQUIRE_ACCESS_TESTS") == "1" {
			t.Fatalf("%s обязателен в CI", label)
		}
		t.Skipf("%s не задан — пропускаем", label)
	}
	return clientWith(t, access, secret)
}

// Единственный барьер — страж тенанта (находка 1 к PR#2a).
//
// Предыдущая версия межтенантного теста проходила и при снятом страже: отказ
// приходил от ACL versitygw ещё до стража, то есть тест проверял ACL, а не
// изоляцию тенантов. Чтобы оставить стража одного, убираются ВСЕ остальные
// барьеры: бакет в тенанте 2, ключ из тенанта 3, а грант в ACL и разрешающая
// политика пишутся прямо в БД.
//
// Сначала тест обязан показать, что гранты из БД работают: ключ своего тенанта
// после них читает. Иначе «отказ» чужого ключа ничего не значил бы.
func TestCrossTenantGuardIsTheOnlyBarrier(t *testing.T) {
	owner := ownerClient(t) // тенант 2, роль userplus
	mine := standUser(t, os.Getenv("TAPEGW_E2E_USER_ACCESS"),
		os.Getenv("TAPEGW_E2E_USER_SECRET"), "ключ своего тенанта")
	foreign := standUser(t, os.Getenv("TAPEGW_E2E_FOREIGN_ACCESS"),
		os.Getenv("TAPEGW_E2E_FOREIGN_SECRET"), "ключ чужого тенанта")
	foreignAccess := os.Getenv("TAPEGW_E2E_FOREIGN_ACCESS")
	db := standDB(t)

	ctx := context.Background()
	bucket := uniqueBucket("tbar")

	if _, err := owner.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if _, err := owner.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k"), Body: bytes.NewReader([]byte("payload")),
	}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	// Пишем ACL и политику прямо в БД: ACL даёт FULL_CONTROL обоим ключам,
	// политика разрешает GetObject обоим.
	acl := fmt.Sprintf(
		`{"Owner":%q,"Grantees":[{"Permission":"FULL_CONTROL","Access":%q,"Type":"CanonicalUser"},`+
			`{"Permission":"FULL_CONTROL","Access":%q,"Type":"CanonicalUser"}]}`,
		os.Getenv("TAPEGW_E2E_OWNER_ACCESS"),
		os.Getenv("TAPEGW_E2E_OWNER_ACCESS"), foreignAccess)
	policy := fmt.Sprintf(
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":["%s","%s"]},`+
			`"Action":["s3:GetObject"],"Resource":["arn:aws:s3:::%s/*"]}]}`,
		os.Getenv("TAPEGW_E2E_USER_ACCESS"), foreignAccess, bucket)
	tag, err := db.Exec(ctx, `UPDATE buckets SET acl = $2::jsonb, policy = $3 WHERE name = $1`,
		bucket, acl, policy)
	if err != nil {
		t.Fatalf("записать ACL/policy: %v", err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatalf("бакет %s не найден в базе стенда", bucket)
	}

	// 1. Свой ключ читает: значит гранты из БД действительно работают, и отказ
	//    чужого нельзя списать на криво записанные ACL или политику.
	if _, err := mine.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k"),
	}); err != nil {
		t.Fatalf("свой ключ не читает при выданных грантах: %v — тест ничего не доказал бы", err)
	}

	// 2. Чужой ключ — отказ, и отказать ему может только страж: ACL и политика
	//    разрешают, бакет существует, объект на месте.
	if _, err := foreign.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k"),
	}); !isAccessDenied(err) {
		t.Fatalf("чужой тенант получил доступ при снятом барьере: %v — страж не работает", err)
	}
}
