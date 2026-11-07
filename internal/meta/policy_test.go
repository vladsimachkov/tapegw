package meta_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

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
	if _, err := m.GetBucketPolicy(ctx, testTenant, "pol"); !errors.Is(err, meta.ErrNoBucketPolicy) {
		t.Fatalf("без политики получили %v, хотели ErrNoBucketPolicy", err)
	}

	if err := m.SetBucketPolicy(ctx, testTenant, "pol", []byte(policyDoc)); err != nil {
		t.Fatalf("SetBucketPolicy: %v", err)
	}
	got, err := m.GetBucketPolicy(ctx, testTenant, "pol")
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

	if _, err := m.GetBucketPolicy(ctx, testTenant, "nope"); !errors.Is(err, meta.ErrNotFound) {
		t.Fatalf("чужой бакет: получили %v, хотели ErrNotFound", err)
	}
	if err := m.SetBucketPolicy(ctx, testTenant, "nope", []byte(policyDoc)); !errors.Is(err, meta.ErrNotFound) {
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

	if err := m.SetBucketPolicy(ctx, testTenant, "pol", []byte(policyDoc)); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := m.SetBucketPolicy(ctx, testTenant, "pol", nil); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := m.GetBucketPolicy(ctx, testTenant, "pol"); !errors.Is(err, meta.ErrNoBucketPolicy) {
		t.Fatalf("после удаления получили %v, хотели ErrNoBucketPolicy", err)
	}
	if err := m.SetBucketPolicy(ctx, testTenant, "pol", nil); err != nil {
		t.Fatalf("повторное удаление: %v", err)
	}
}

func TestChangeBucketOwnerAndList(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()
	mustBucket(t, m, "b1")
	mustBucket(t, m, "b2")

	if err := m.ChangeBucketOwner(ctx, testTenant, "b2", "userplus", 1, []byte(`{"Owner":"userplus","Grantees":[]}`)); err != nil {
		t.Fatalf("ChangeBucketOwner: %v", err)
	}
	if err := m.ChangeBucketOwner(ctx, testTenant, "ghost", "userplus", 1, nil); !errors.Is(err, meta.ErrNotFound) {
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

// Перенос бакета между тенантами (план PR#2b).
//
// Пустой — переносится; в бакете что-то осталось — отказ. «Осталось» считается
// по всем строкам objects (старые версии, delete markers, deleting) и по
// незавершённым uploads: иначе в чужой тенант уезжают хвосты, которые не видно
// списком, но которые потом воскресают.
func TestChangeBucketOwnerMovesTenant(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	// Тенант-получатель: справочник в тестовой базе содержит только default.
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO tenants (id, name, pool_id) VALUES (77, 't77', 1)
		  ON CONFLICT (id) DO NOTHING`); err != nil {
		t.Fatalf("tenant 77: %v", err)
	}
	const tenant77 = int64(77)

	mustBucket(t, m, "mv-empty")
	mustBucket(t, m, "mv-full")

	// 1. Пустой бакет переезжает.
	if err := m.ChangeBucketOwner(ctx, testTenant, "mv-empty", "bob", tenant77,
		[]byte(`{"Owner":"bob","Grantees":[]}`)); err != nil {
		t.Fatalf("перенос пустого: %v", err)
	}
	var got int64
	if err := m.Pool.QueryRow(ctx,
		`SELECT tenant_id FROM buckets WHERE name = 'mv-empty'`).Scan(&got); err != nil {
		t.Fatalf("tenant_id: %v", err)
	}
	if got != tenant77 {
		t.Errorf("tenant_id = %d, хотели %d — новый владелец упрётся в стража", got, tenant77)
	}

	// 2. Бакет с объектом не переезжает.
	if _, err := m.PutObject(ctx, testTenant, meta.PutObjectInput{
		Bucket: "mv-full", Key: "k", Size: 3, FileBytes: 64,
		OnlineUntil: time.Now().Add(time.Hour), WrappedDEK: []byte("x"),
		Salt: make([]byte, 16),
	}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if err := m.ChangeBucketOwner(ctx, testTenant, "mv-full", "bob", tenant77, nil); !errors.Is(err, meta.ErrBucketNotEmpty) {
		t.Errorf("перенос непустого: %v, хотели ErrBucketNotEmpty", err)
	}

	// 3. Незавершённая multipart-загрузка — тоже «не пусто».
	mustBucket(t, m, "mv-mpu")
	if _, err := m.Pool.Exec(ctx,
		// wrapped_dek обязателен с миграции 0014: без него части нечем
		// шифровать, и заглушить эту колонку в тесте значило бы проверить
		// перенос бакета с загрузкой, которой в реальности не существует.
		`INSERT INTO uploads (upload_id, bucket_id, key, meta, wrapped_dek, object_id)
		 SELECT 'u1', id, 'mp', '{}'::jsonb, decode('00','hex'), gen_random_uuid()
		   FROM buckets WHERE name = 'mv-mpu'`); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if err := m.ChangeBucketOwner(ctx, testTenant, "mv-mpu", "bob", tenant77, nil); !errors.Is(err, meta.ErrBucketNotEmpty) {
		t.Errorf("перенос бакета с незавершённой загрузкой: %v, хотели ErrBucketNotEmpty", err)
	}

	// 4. Перенос «в тот же тенант» пустоты не требует: хвосты остаются там же,
	// где были, а владелец меняется.
	if err := m.ChangeBucketOwner(ctx, testTenant, "mv-mpu", "bob", 1, nil); err != nil {
		t.Fatalf("перенос в свой тенант не должен требовать пустоты: %v", err)
	}
}

// Гонка «параллельный PUT против смены владельца» (план PR#2b).
//
// ChangeBucketOwner берёт бакет FOR UPDATE, PUT — FOR SHARE (ADR-0005), поэтому
// третий исход невозможен: либо смена отклонена (объект успел), либо PUT упал
// по тенанту. Оба успеха одновременно означали бы, что бакет уехал в чужой
// тенант вместе со свежим объектом текущего.
func TestChangeOwnerRacesWithPut(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	ctx := context.Background()

	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO tenants (id, name, pool_id) VALUES (78, 't78', 1)
		  ON CONFLICT (id) DO NOTHING`); err != nil {
		t.Fatalf("tenant 78: %v", err)
	}

	for round := 0; round < 20; round++ {
		bucket := fmt.Sprintf("race-%02d", round)
		mustBucket(t, m, bucket)

		var putErr, chgErr error
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_, putErr = m.PutObject(ctx, testTenant, meta.PutObjectInput{
				Bucket: bucket, Key: "k", Size: 1, FileBytes: 64,
				OnlineUntil: time.Now().Add(time.Hour), WrappedDEK: []byte("x"),
				Salt: make([]byte, 16),
			})
		}()
		go func() {
			defer wg.Done()
			<-start
			chgErr = m.ChangeBucketOwner(ctx, testTenant, bucket, "bob", 78, nil)
		}()
		close(start)
		wg.Wait()

		if putErr == nil && chgErr == nil {
			t.Fatalf("%s: оба прошли — бакет уехал в чужой тенант вместе со свежим объектом", bucket)
		}
		if putErr != nil && chgErr != nil {
			// Допустимо: PUT не дождался блокировки и попал уже в чужой
			// тенант, а смена увидила строку objects из откатившейся попытки?
			// Нет — откатившаяся транзакция не оставляет строк. Значит отказ
			// смены обязан быть именно «не пусто».
			if !errors.Is(chgErr, meta.ErrBucketNotEmpty) {
				t.Fatalf("%s: смена отказала %v при PUT %v", bucket, chgErr, putErr)
			}
		}
	}
}
