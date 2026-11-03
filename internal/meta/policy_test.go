package meta_test

import (
	"context"
	"errors"
	"testing"

	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// Документ с намеренно «неудобным» JSON: ключи в неалфавитном порядке и
// дубликат ключа внутри Statement. jsonb пересобрал бы такое и переставил
// ключи — тест на побайтовом совпадении это ловит.
const policyDoc = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
	`"Principal":{"AWS":"userplus"},"Action":["s3:GetObject"],` +
	`"Resource":"arn:aws:s3:::pol/*"}]}`

func TestBucketPolicyRoundTrip(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()
	mustBucket(t, m, "pol")

	// До записи — именно «политики нет», а не «бакета нет».
	if _, err := m.GetBucketPolicy(ctx, "pol"); !errors.Is(err, meta.ErrNoBucketPolicy) {
		t.Fatalf("без политики получили %v, хотели ErrNoBucketPolicy", err)
	}

	if err := m.SetBucketPolicy(ctx, "pol", []byte(policyDoc)); err != nil {
		t.Fatalf("SetBucketPolicy: %v", err)
	}
	got, err := m.GetBucketPolicy(ctx, "pol")
	if err != nil {
		t.Fatalf("GetBucketPolicy: %v", err)
	}
	if string(got) != policyDoc {
		t.Errorf("политика вернулась не дословно:\nполучили %s\nотправляли %s", got, policyDoc)
	}
}

// Разница между «бакета нет» и «политики нет» принципиальна: versitygw на
// ErrNoSuchBucketPolicy продолжает проверку доступа, а на любую другую ошибку
// обрывает запрос.
func TestBucketPolicyMissingBucket(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	if _, err := m.GetBucketPolicy(ctx, "nope"); !errors.Is(err, meta.ErrNotFound) {
		t.Fatalf("чужой бакет: получили %v, хотели ErrNotFound", err)
	}
	if err := m.SetBucketPolicy(ctx, "nope", []byte(policyDoc)); !errors.Is(err, meta.ErrNotFound) {
		t.Fatalf("запись в чужой бакет: получили %v, хотели ErrNotFound", err)
	}
}

// Удаление отсутствующей политики — не ошибка (как в posix), а повторное
// удаление удалённой тоже.
func TestBucketPolicyDelete(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()
	mustBucket(t, m, "pol")

	if err := m.SetBucketPolicy(ctx, "pol", []byte(policyDoc)); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := m.SetBucketPolicy(ctx, "pol", nil); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := m.GetBucketPolicy(ctx, "pol"); !errors.Is(err, meta.ErrNoBucketPolicy) {
		t.Fatalf("после удаления получили %v, хотели ErrNoBucketPolicy", err)
	}
	if err := m.SetBucketPolicy(ctx, "pol", nil); err != nil {
		t.Fatalf("повторное удаление: %v", err)
	}
}

func TestChangeBucketOwnerAndList(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()
	mustBucket(t, m, "b1")
	mustBucket(t, m, "b2")

	if err := m.ChangeBucketOwner(ctx, "b2", "userplus"); err != nil {
		t.Fatalf("ChangeBucketOwner: %v", err)
	}
	if err := m.ChangeBucketOwner(ctx, "ghost", "userplus"); !errors.Is(err, meta.ErrNotFound) {
		t.Fatalf("чужой бакет: получили %v, хотели ErrNotFound", err)
	}

	all, err := m.ListAllBucketsAndOwners(ctx)
	if err != nil {
		t.Fatalf("ListAllBucketsAndOwners: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("бакетов %d, ожидали 2: %v", len(all), all)
	}
	// Порядок — байтовый по имени, на нём держатся админские сравнения вывода.
	if all[0].Name != "b1" || all[0].Owner != "tester" {
		t.Errorf("первый = %+v, хотели b1/tester", all[0])
	}
	if all[1].Name != "b2" || all[1].Owner != "userplus" {
		t.Errorf("второй = %+v, хотели b2/userplus", all[1])
	}
}
