package keys_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/encrypt"
	"github.com/vladsimachkov/tapegw/internal/iam"
	"github.com/vladsimachkov/tapegw/internal/keys"
	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/testdb"
)

func genKey(t *testing.T) []byte {
	t.Helper()
	k := make([]byte, 32)
	if _, err := rand.Read(k); err != nil {
		t.Fatalf("KEK: %v", err)
	}
	return k
}

func ringOf(t *testing.T, active int, keysMap map[int][]byte) *iam.KeyRing {
	t.Helper()
	ring, err := iam.NewKeyRing(active, keysMap)
	if err != nil {
		t.Fatalf("NewKeyRing: %v", err)
	}
	return ring
}

// newRing — связка из одноимённых версий со СВОИМ материалом на каждую.
func newRing(t *testing.T, versions ...int) *iam.KeyRing {
	t.Helper()
	keysMap := map[int][]byte{}
	for _, v := range versions {
		keysMap[v] = genKey(t)
	}
	return ringOf(t, versions[len(versions)-1], keysMap)
}

// clearKeys сбрасывает состояние ключей у всех тенантов, включая seed-строку.
//
// Нужно потому, что Clean оставляет seed-строки и не восстанавливает значения
// колонок: wrapped_key, записанный первым тестом, достался бы второму, а KEK у
// каждого теста свой. Особенно это видно на RotateKEK — она по определению
// ходит по всем тенантам с ключами и спотыкается о чужой остаток.
func clearKeys(t *testing.T, m *meta.Meta) {
	t.Helper()
	_, err := m.Pool.Exec(context.Background(),
		`UPDATE tenants SET wrapped_key = NULL, kek_version = 1, key_destroyed_at = NULL`)
	if err != nil {
		t.Fatalf("сброс ключей: %v", err)
	}
}

// seedTenant заводит тенант под тест.
//
// Тенант 1 для этих тестов не годится: он seed-строка, а Clean оставляет
// seed-строки и не восстанавливает значения колонок. Значит, wrapped_key,
// записанный первым тестом, достался бы второму — с TEK, завернутым уже другим
// KEK, и разбор обёртки падал бы с «ключ не подошёл» там, где баг не в коде.
// Тестовые тенанты (2, 3) Clean удаляет, и каждый стартует с NULL.
func seedTenant(t *testing.T, m *meta.Meta, id int64, name string) {
	t.Helper()
	_, err := m.Pool.Exec(context.Background(),
		// system обязан совпасть с id: CHECK tenants_default_is_system из
		// миграции 0007 не даёт тенанту 1 быть несистемным.
		`INSERT INTO tenants (id, name, pool_id, system) VALUES ($1, $2, 1, $1::bigint = 1)
		 ON CONFLICT (id) DO NOTHING`, id, name)
	if err != nil {
		t.Fatalf("посеять тенант %d: %v", id, err)
	}
}

func TestTEKCreatedLazily(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	clearKeys(t, m)
	ctx := context.Background()
	seedTenant(t, m, 2, "test-tenant")

	v := keys.New(m, newRing(t, 1))
	// До первой записи ключа нет — это нормальное состояние, а не ошибка.
	row, err := m.TenantKey(ctx, 2)
	if err != nil {
		t.Fatalf("TenantKey: %v", err)
	}
	if len(row.Wrapped) != 0 {
		t.Errorf("ключ появился до первой записи")
	}

	v1 := uuid.New()
	dek, wrapped, err := v.NewVersionDEK(ctx, 2, v1)
	if err != nil {
		t.Fatalf("NewVersionDEK: %v", err)
	}
	if len(dek) != encrypt.KeySize || len(wrapped) == 0 {
		t.Fatalf("dek=%d wrapped=%d", len(dek), len(wrapped))
	}

	row, err = m.TenantKey(ctx, 2)
	if err != nil {
		t.Fatalf("TenantKey после записи: %v", err)
	}
	if len(row.Wrapped) == 0 {
		t.Errorf("TEK не записан в tenants.wrapped_key")
	}

	// Вторая версия того же тенанта обязана развернуться: TEK тот же, хотя
	// вызов ничего не создаёт.
	v2 := uuid.New()
	dek2, wrapped2, err := v.NewVersionDEK(ctx, 2, v2)
	if err != nil {
		t.Fatalf("вторая версия: %v", err)
	}
	if bytes.Equal(dek, dek2) {
		t.Errorf("DEK повторился между версиями — ключ обязан быть на каждую версию")
	}
	got, err := v.OpenVersionDEK(ctx, 2, wrapped2, v2)
	if err != nil {
		t.Fatalf("OpenVersionDEK: %v", err)
	}
	if !bytes.Equal(got, dek2) {
		t.Errorf("DEK развернулся не тем")
	}
	encrypt.Zero(got)
}

// TestTenantKeyDestroyed — уничтожение ключа делает данные нечитаемыми. Это и
// есть смысл крипто-уничтожения: если после стирания ключа что-то читается,
// шифрование ничего не даёт.
func TestTenantKeyDestroyed(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	clearKeys(t, m)
	ctx := context.Background()
	seedTenant(t, m, 2, "test-tenant")

	v := keys.New(m, newRing(t, 1))
	vid := uuid.New()
	_, wrapped, err := v.NewVersionDEK(ctx, 2, vid)
	if err != nil {
		t.Fatalf("NewVersionDEK: %v", err)
	}

	if err := m.DestroyTenantKey(ctx, 2); err != nil {
		t.Fatalf("DestroyTenantKey: %v", err)
	}
	if _, err := v.OpenVersionDEK(ctx, 2, wrapped, vid); err == nil {
		t.Errorf("DEK развернулся после уничтожения ключа тенанта")
	}

	// И новая запись после уничтожения невозможна: молча завести второй TEK
	// означало бы, что «уничтожение» ничего не уничтожило.
	if _, _, err := v.NewVersionDEK(ctx, 2, uuid.New()); err == nil {
		t.Errorf("новая версия создана после уничтожения ключа тенанта")
	} else if !errors.Is(err, keys.ErrKeyGone) {
		t.Errorf("ошибка %v, хотели ErrKeyGone", err)
	}
}

// TestKEKRotation — после смены KEK всё записанное раньше продолжает читаться.
// Переоборачиваются только TEK; DEK и данные не трогаются.
func TestKEKRotation(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	clearKeys(t, m)
	ctx := context.Background()
	seedTenant(t, m, 2, "test-tenant")
	seedTenant(t, m, 3, "second-tenant")

	// Материал KEK версий 1 и 2 создаётся один раз и переиспользуется в обеих
	// связках. newRing(t, 1, 2) здесь не годится: он выдал бы «версию 1»
	// заново, уже другими байтами, и переобёртка не развернула бы то, что
	// завернула первая связка.
	kek1, kek2 := genKey(t), genKey(t)
	ring := ringOf(t, 1, map[int][]byte{1: kek1})
	v := keys.New(m, ring)

	type written struct {
		tenant  int64
		version uuid.UUID
		dek     []byte
		wrapped []byte
	}
	var before []written
	for _, tn := range []int64{2, 3} {
		vid := uuid.New()
		dek, wrapped, err := v.NewVersionDEK(ctx, tn, vid)
		if err != nil {
			t.Fatalf("тенант %d: %v", tn, err)
		}
		before = append(before, written{tn, vid, dek, wrapped})
	}

	// Новый KEK стал активным, старый остался в связке (так оператор делает
	// ротацию: старую версию не выбрасывают, пока не переобёрнуты все строки).
	ring2 := ringOf(t, 2, map[int][]byte{1: kek1, 2: kek2})
	v2 := keys.New(m, ring2)
	n, err := v2.RotateKEK(ctx)
	if err != nil {
		t.Fatalf("RotateKEK: %v", err)
	}
	if n != 2 {
		t.Errorf("переобёрнуто %d ключей, хотели 2", n)
	}
	// Повторный прогон ничего не меняет: ротация идемпотентна.
	if n, err := v2.RotateKEK(ctx); err != nil || n != 0 {
		t.Errorf("повтор RotateKEK: %d, %v, хотели 0 и nil", n, err)
	}

	for _, w := range before {
		got, err := v2.OpenVersionDEK(ctx, w.tenant, w.wrapped, w.version)
		if err != nil {
			t.Errorf("тенант %d после ротации: %v", w.tenant, err)
			continue
		}
		if !bytes.Equal(got, w.dek) {
			t.Errorf("тенант %d: DEK после ротации другой", w.tenant)
		}
		encrypt.Zero(got)
	}

	row, err := m.TenantKey(ctx, 2)
	if err != nil {
		t.Fatalf("TenantKey: %v", err)
	}
	if row.KekVersion != 2 {
		t.Errorf("kek_version = %d, хотели 2", row.KekVersion)
	}
}

// TestDEKBoundToVersion — обёртка DEK привязана к UUID версии и к тенанту:
// переставить её в другую строку нельзя.
func TestDEKBoundToVersion(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	clearKeys(t, m)
	ctx := context.Background()
	seedTenant(t, m, 2, "test-tenant")
	seedTenant(t, m, 3, "second-tenant")

	v := keys.New(m, newRing(t, 1))
	vA := uuid.New()
	_, wrappedA, err := v.NewVersionDEK(ctx, 2, vA)
	if err != nil {
		t.Fatalf("NewVersionDEK: %v", err)
	}

	if _, err := v.OpenVersionDEK(ctx, 2, wrappedA, uuid.New()); err == nil {
		t.Errorf("обёртка развернулась для чужой версии")
	}
	if _, err := v.OpenVersionDEK(ctx, 3, wrappedA, vA); err == nil {
		t.Errorf("обёртка развернулась для чужого тенанта")
	}
}

// TestKeysNotInErrors — ни KEK, ни TEK, ни DEK не должны попадать в текст
// ошибок. Ошибки пишутся в лог, а лог читает кто угодно.
func TestKeysNotInErrors(t *testing.T) {
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	clearKeys(t, m)
	ctx := context.Background()
	seedTenant(t, m, 2, "test-tenant")

	ring := newRing(t, 1)
	v := keys.New(m, ring)

	var materials [][]byte
	k1 := make([]byte, 32)
	if _, err := rand.Read(k1); err != nil {
		t.Fatalf("rand: %v", err)
	}
	materials = append(materials, k1)

	vid := uuid.New()
	dek, wrapped, err := v.NewVersionDEK(ctx, 2, vid)
	if err != nil {
		t.Fatalf("NewVersionDEK: %v", err)
	}
	materials = append(materials, dek, wrapped)

	if err := m.DestroyTenantKey(ctx, 2); err != nil {
		t.Fatalf("DestroyTenantKey: %v", err)
	}
	// Собираем все ошибки путей, где ключ участвует.
	var msgs []string
	if _, err := v.OpenVersionDEK(ctx, 2, wrapped, vid); err != nil {
		msgs = append(msgs, err.Error())
	}
	if _, _, err := v.NewVersionDEK(ctx, 2, uuid.New()); err != nil {
		msgs = append(msgs, err.Error())
	}
	if _, err := v.OpenVersionDEK(ctx, 2, []byte("мусор"), vid); err != nil {
		msgs = append(msgs, err.Error())
	}
	// Обёртка не развернулась бы и при неизвестной версии KEK.
	if _, err := v.OpenVersionDEK(ctx, 2, wrapped, vid); err != nil {
		msgs = append(msgs, err.Error())
	}

	for _, msg := range msgs {
		for _, mat := range materials {
			// Ищем и сырые байты, и hex: лог иногда печатает %x.
			if strings.Contains(msg, string(mat)) || strings.Contains(strings.ToLower(msg), hex(mat)) {
				t.Errorf("в ошибке оказался ключевой материал: %q", msg)
			}
		}
	}
}

func hex(b []byte) string {
	const d = "0123456789abcdef"
	out := make([]byte, 0, len(b)*2)
	for _, c := range b {
		out = append(out, d[c>>4], d[c&0xf])
	}
	return string(out)
}
