package e2e

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// Проверка доступа под обычным ключом.
//
// Пока GetBucketPolicy возвращал NotImplemented, любой не-root ключ получал 501
// на всё: versitygw вызывает его на каждом запросе и терпит ровно одну ошибку,
// ErrNoSuchBucketPolicy (auth/access-control.go:421). Тесты на root этого не
// ловили — root и admin идут мимо политик и ACL вообще.

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
	root := client(t)
	user := userClient(t)
	ctx := context.Background()
	bucket := uniqueBucket("pol")

	if _, err := root.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	body := []byte("secret payload")
	if _, err := root.PutObject(ctx, &s3.PutObjectInput{
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

	// 2. Политика выдаёт право — доступ появляется.
	if _, err := root.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{
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
	p, err := root.GetBucketPolicy(ctx, &s3.GetBucketPolicyInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatalf("GetBucketPolicy: %v", err)
	}
	if p.Policy == nil || *p.Policy != string(policyFor(bucket, userAccess(t))) {
		t.Errorf("политика вернулась не дословно: %s", aws.ToString(p.Policy))
	}

	// 4. Сняли политику — снова отказ.
	if _, err := root.DeleteBucketPolicy(ctx, &s3.DeleteBucketPolicyInput{
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
	_, err = root.GetBucketPolicy(ctx, &s3.GetBucketPolicyInput{Bucket: aws.String(bucket)})
	var ae smithy.APIError
	if !errors.As(err, &ae) || ae.ErrorCode() != "NoSuchBucketPolicy" {
		t.Fatalf("без политики ответ %v, хотели NoSuchBucketPolicy", err)
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
