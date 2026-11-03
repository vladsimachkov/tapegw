package backend_test

// Поведение стража поштучно, без перебора: какие именно коды приходят и что
// назначаются за тенанты. Перебор (tenant_guard_sweep_test.go) отвечает на
// вопрос «ни один ли метод не забыт», эти тесты — «правильный ли отказ».

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"

	"github.com/vladsimachkov/tapegw/internal/meta"
)

// CreateBucket — единственный метод, где страж не проверяет, а назначает.
// Проверок здесь две, и обе про учётки, у которых «нет своего тенанта»:
//
//   - root (RoleAdmin при ProjectID 0) обязан бакет создать, иначе не работает
//     ни «Быстрый старт», ни e2e-стенд; тенантом ему назначается `default`;
//   - ключ роли user с ProjectID 0 — это аноним по признаку тенанта, и ему
//     создавать бакеты нельзя.
func TestCreateBucketAssignsTenant(t *testing.T) {
	be, m, _ := newBackend(t)

	if err := be.CreateBucket(adminCtx(), &s3.CreateBucketInput{Bucket: ptr("root-owned")}, nil); err != nil {
		t.Fatalf("root обязан создавать бакеты: %v", err)
	}
	// Бакет обязан был попасть в тенант `default` (id = 1), а не в «тенанта root»,
	// которого не существует.
	bucket, err := m.GetBucket(testCtx(), meta.NewTenant(testProjectID), "root-owned")
	if err != nil {
		t.Fatalf("бакет root не найден в тенанте %d: %v", testProjectID, err)
	}
	if bucket.Name != "root-owned" {
		t.Fatalf("нашёлся бакет %q, а не root-owned", bucket.Name)
	}

	// Тот же бакет видим обычному ключу этого же тенанта — проверка «назначился
	// тенант», а не «записалась строка».
	if _, err := be.HeadBucket(testCtx(), &s3.HeadBucketInput{Bucket: ptr("root-owned")}); err != nil {
		t.Errorf("свой бакет должен читаться: %v", err)
	}

	// Аноним: пустая учётка (ProjectID 0, роль не admin) — отказ и на CreateBucket.
	anon := accountCtx(auth.Account{})
	if err := be.CreateBucket(anon, &s3.CreateBucketInput{Bucket: ptr("anon-owned")}, nil); err == nil {
		t.Error("аноним создал бакет — страж не сработал на назначении")
	} else if !isS3Code(err, s3err.ErrAccessDenied) {
		t.Errorf("анониму ожидали AccessDenied, получили %v", err)
	}
}

// Различение «бакета нет» и «бакет чужой» — второй, дешёвый запрос, и только
// когда основной ничего не нашёл (ADR-0017, п. 1). Если свести оба к
// NoSuchBucket, клиент не сможет понять, почему CreateBucket отвечает
// BucketAlreadyExists на «несуществующее» имя.
func TestTenantGuardDistinguishesMissingAndForeignBucket(t *testing.T) {
	be, _, _ := newBackend(t)
	createVictim(t, be)

	foreign := accountCtx(auth.Account{Access: "intruder", Role: auth.RoleUser, ProjectID: testProjectID + 1})

	if _, err := be.HeadBucket(foreign, &s3.HeadBucketInput{Bucket: ptr(victimBucket)}); !isS3Code(err, s3err.ErrAccessDenied) {
		t.Errorf("чужой бакет: ожидали AccessDenied, получили %v", err)
	}
	if _, err := be.HeadBucket(foreign, &s3.HeadBucketInput{Bucket: ptr("no-such-bucket")}); !isS3Code(err, s3err.ErrNoSuchBucket) {
		t.Errorf("несуществующий бакет: ожидали NoSuchBucket, получили %v", err)
	}
}

// Объектные пути: чужой бакет обязан дать AccessDenied, а не NoSuchKey, и
// «ключа нет» в своём бакете обязан остаться NoSuchKey — иначе клиент, который
// ищет объект, начнёт ретраить отказ доступа.
func TestTenantGuardObjectPaths(t *testing.T) {
	be, _, _ := newBackend(t)
	mustBucket(t, be)
	putBody(t, be, "b", "k", "тело")

	foreign := accountCtx(auth.Account{Access: "intruder", Role: auth.RoleUser, ProjectID: testProjectID + 1})

	if _, err := be.GetObject(foreign, &s3.GetObjectInput{Bucket: ptr("b"), Key: ptr("k")}); !isS3Code(err, s3err.ErrAccessDenied) {
		t.Errorf("чужой объект: ожидали AccessDenied, получили %v", err)
	}
	if _, err := be.HeadObject(foreign, &s3.HeadObjectInput{Bucket: ptr("b"), Key: ptr("k")}); !isS3Code(err, s3err.ErrAccessDenied) {
		t.Errorf("чужой объект (HEAD): ожидали AccessDenied, получили %v", err)
	}
	if _, err := be.PutObject(foreign, s3response.PutObjectInput{
		Bucket: ptr("b"), Key: ptr("new"), StorageClass: "GLACIER",
	}); !isS3Code(err, s3err.ErrAccessDenied) {
		t.Errorf("запись в чужой бакет: ожидали AccessDenied, получили %v", err)
	}

	// Свой бакет и отсутствующий ключ — по-прежнему NoSuchKey.
	if _, err := be.GetObject(testCtx(), &s3.GetObjectInput{Bucket: ptr("b"), Key: ptr("нет-такого")}); !isS3Code(err, s3err.ErrNoSuchKey) {
		t.Errorf("отсутствующий ключ в своём бакете: ожидали NoSuchKey, получили %v", err)
	}
}

// Обход по роли: root и администратор видят бакет любого тенанта — ровно так
// же они идут мимо политик versitygw. Если бы страж отказывал раньше роли,
// админский путь и e2e-стенд на root перестали бы работать.
func TestTenantGuardAdminSeesAnyTenant(t *testing.T) {
	be, _, _ := newBackend(t)
	createVictim(t, be)

	if _, err := be.HeadBucket(adminCtx(), &s3.HeadBucketInput{Bucket: ptr(victimBucket)}); err != nil {
		t.Errorf("администратор обязан видеть чужой бакет: %v", err)
	}
	// Список бакетов: администратор видит бакет чужого тенанта, обычный ключ
	// другого тенанта — нет.
	adminList, err := be.ListBuckets(adminCtx(), s3response.ListBucketsInput{})
	if err != nil {
		t.Fatalf("ListBuckets администратору: %v", err)
	}
	if !hasBucket(adminList, victimBucket) {
		t.Errorf("администратор не увидел %s: %+v", victimBucket, adminList.Buckets.Bucket)
	}

	foreign := accountCtx(auth.Account{Access: "intruder", Role: auth.RoleUser, ProjectID: testProjectID + 1})
	foreignList, err := be.ListBuckets(foreign, s3response.ListBucketsInput{})
	if err != nil {
		t.Fatalf("ListBuckets чужому тенанту: %v", err)
	}
	if hasBucket(foreignList, victimBucket) {
		t.Errorf("чужой тенант увидел %s в списке: %+v", victimBucket, foreignList.Buckets.Bucket)
	}
}

func hasBucket(list s3response.ListAllMyBucketsResult, name string) bool {
	for _, b := range list.Buckets.Bucket {
		if b.Name == name {
			return true
		}
	}
	return false
}
