package keys_test

// Тесты экспорта ключей тенантов (ADR-0014).
//
// Экспорт — единственное место, где обёрнутые TEK переживают потерю БД, и
// одновременно место, где обещание «мы удалили ваши данные» либо выполняется,
// либо нет. Поэтому три проверки здесь главные:
//
//   - уничтоженный тенант обязан ИСЧЕЗНУТЬ из следующего экспорта. Что он
//     выпадает из SQL-выборки — свойство запроса, а не гарантия: держится она на
//     тесте;
//   - подмена обёртки или перестановка «обёртка ↔ тенант» обязана быть ошибкой,
//     иначе экспорт можно подделать тем, кто к нему имеет доступ;
//   - «в кольце нет нужного KEK» и «файл испорчен» — разные ошибки: первая
//     означает «поднимите кольцо из хранилища», вторая — «разбирайтесь, почему
//     носитель изменился».

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/iam"
	"github.com/vladsimachkov/tapegw/internal/keys"
	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// exportTenants — тенанты, у которых в этих тестах заведён TEK.
var exportTenants = []int64{2, 3}

// exportSetup — чистая БД, кольцо KEK и Vault, в котором у обоих тенантов уже
// есть ключ. Кольцо возвращается наружу: читать экспорт обязан тем же кольцом,
// которым сворачивали, иначе проверялась бы не подмена, а несовпадение ключей.
func exportSetup(t *testing.T) (*meta.Meta, *iam.KeyRing, *keys.Vault) {
	t.Helper()
	m := testdb.Connect(t)
	testdb.Clean(t, m)
	clearKeys(t, m)

	seedTenant(t, m, 2, "экспорт-а")
	seedTenant(t, m, 3, "экспорт-б")
	ring := newRing(t, 1)
	v := keys.New(m, ring)
	ctx := context.Background()
	for _, tn := range exportTenants {
		if _, _, err := v.NewVersionDEK(ctx, tn, uuid.New()); err != nil {
			t.Fatalf("завести ключ тенанта %d: %v", tn, err)
		}
	}
	return m, ring, v
}

// exportBytes — собранный и записанный экспорт.
func exportBytes(t *testing.T, m *meta.Meta, ring *iam.KeyRing) []byte {
	t.Helper()
	e, err := keys.BuildExport(context.Background(), m, ring)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := e.Write(&buf); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// TestExportOnlyLiveTenants — уничтожение TEK убирает тенанта из экспорта.
//
// Это тот тест, на котором держится обещание удаления. Запрос TenantsWithKeys
// выбирает только wrapped_key IS NOT NULL, но «так получилось в SQL» — не
// причина ничего менять, если завтра запрос поправят.
func TestExportOnlyLiveTenants(t *testing.T) {
	m, ring, _ := exportSetup(t)
	ctx := context.Background()

	e1, err := keys.BuildExport(ctx, m, ring)
	if err != nil {
		t.Fatal(err)
	}
	for _, tn := range exportTenants {
		if _, ok := e1.Tenant(tn); !ok {
			t.Fatalf("тенанта %d нет в первом экспорте", tn)
		}
	}

	if err := m.DestroyTenantKey(ctx, 3); err != nil {
		t.Fatal(err)
	}
	e2, err := keys.BuildExport(ctx, m, ring)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := e2.Tenant(3); ok {
		t.Fatal("уничтоженный тенант остался в экспорте: его данные по-прежнему восстановимы")
	}
	if _, ok := e2.Tenant(2); !ok {
		t.Fatal("живой тенант пропал из экспорта")
	}
}

// TestExportRoundTrip — запись, проверка и заголовок.
func TestExportRoundTrip(t *testing.T) {
	m, ring, _ := exportSetup(t)
	raw := exportBytes(t, m, ring)

	got, err := keys.ReadExport(bytes.NewReader(raw), ring)
	if err != nil {
		t.Fatalf("чтение экспорта: %v", err)
	}
	if len(got.Entries) != len(exportTenants) {
		t.Fatalf("записей %d, ждали %d", len(got.Entries), len(exportTenants))
	}
	// Дата и версии KEK нужны rebuild-meta: без них не понять, каким KEK
	// разворачивать обёртки, и «ключа нет в кольце» не отличить от «файл битый».
	if got.Created.IsZero() {
		t.Error("дата создания потеряна")
	}
	if len(got.KEKVersions) == 0 {
		t.Error("версии KEK не записаны")
	}
	if got.ActiveKEK != ring.Active() {
		t.Errorf("активная версия: %d, кольцо %d", got.ActiveKEK, ring.Active())
	}
}

// TestExportRejectsSwappedWrappers — перестановка обёрток между тенантами.
//
// Обёртка TEK завернута AES-GCM с AAD = id тенанта, поэтому «взять обёртку
// соседа» даёт ошибку аутентичности. Без такой привязки подмена подсовила бы
// чужой ключ, и восстановление выдало бы мусор как успешный результат.
func TestExportRejectsSwappedWrappers(t *testing.T) {
	m, ring, _ := exportSetup(t)
	doc := mutateExport(t, exportBytes(t, m, ring))
	tenants := doc["tenants"].([]any)
	if len(tenants) < 2 {
		t.Fatal("нужны два тенанта с ключами")
	}
	t0, t1 := tenants[0].(map[string]any), tenants[1].(map[string]any)
	t0["wrapped"], t1["wrapped"] = t1["wrapped"], t0["wrapped"]

	if _, err := keys.ReadExport(bytes.NewReader(marshalExport(t, doc)), ring); !errors.Is(err, keys.ErrExportCorrupt) {
		t.Fatalf("перестановка обёрток не отвергнута: %v", err)
	}
}

// TestExportNeedsSameKek — экспорт под KEK v1 не читается кольцом без v1, и
// ошибка при этом про отсутствующий KEK, а не про испорченный файл.
func TestExportNeedsSameKek(t *testing.T) {
	m, ring, _ := exportSetup(t)
	raw := exportBytes(t, m, ring)

	other := newRing(t, 7)
	if _, err := keys.ReadExport(bytes.NewReader(raw), other); !errors.Is(err, keys.ErrExportUnknownKek) {
		t.Fatalf("отсутствие нужного KEK должно быть отдельной ошибкой, получили: %v", err)
	}
}

// TestExportRejectsTampering — дубликат тенанта, пустой файл, чужой формат и
// обёртка не base64.
func TestExportRejectsTampering(t *testing.T) {
	m, ring, _ := exportSetup(t)
	raw := exportBytes(t, m, ring)

	t.Run("дубликат", func(t *testing.T) {
		doc := mutateExport(t, raw)
		tenants := doc["tenants"].([]any)
		// Две строки на одного тенанта: при чтении одна «победила» бы молча.
		doc["tenants"] = append(tenants, tenants[0])
		if _, err := keys.ReadExport(bytes.NewReader(marshalExport(t, doc)), ring); !errors.Is(err, keys.ErrExportCorrupt) {
			t.Errorf("дубликат тенанта не отвергнут")
		}
	})

	t.Run("пустой", func(t *testing.T) {
		doc := mutateExport(t, raw)
		// Пустой экспорт почти всегда ошибка выгрузки, а не «тенантов нет».
		doc["tenants"] = []any{}
		if _, err := keys.ReadExport(bytes.NewReader(marshalExport(t, doc)), ring); !errors.Is(err, keys.ErrExportCorrupt) {
			t.Errorf("пустой экспорт принят")
		}
	})

	t.Run("чужой формат", func(t *testing.T) {
		doc := mutateExport(t, raw)
		doc["format"] = 99
		if _, err := keys.ReadExport(bytes.NewReader(marshalExport(t, doc)), ring); !errors.Is(err, keys.ErrExportCorrupt) {
			t.Errorf("неизвестный формат принят")
		}
	})

	t.Run("обёртка не base64", func(t *testing.T) {
		doc := mutateExport(t, raw)
		doc["tenants"].([]any)[0].(map[string]any)["wrapped"] = "!!! не base64"
		if _, err := keys.ReadExport(bytes.NewReader(marshalExport(t, doc)), ring); !errors.Is(err, keys.ErrExportCorrupt) {
			t.Errorf("обёртка не base64 принята")
		}
	})

	t.Run("kek вне заголовка", func(t *testing.T) {
		doc := mutateExport(t, raw)
		// Заголовок обязан перечислять все версии, которыми свёрнуты обёртки:
		// иначе «KEK не найден» и «заголовок врёт» неразличимы.
		doc["kek_versions"] = []any{}
		if _, err := keys.ReadExport(bytes.NewReader(marshalExport(t, doc)), ring); !errors.Is(err, keys.ErrExportCorrupt) {
			t.Errorf("обёртка с KEK вне заголовка принята")
		}
	})
}

func mutateExport(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	return doc
}

func marshalExport(t *testing.T, doc map[string]any) []byte {
	t.Helper()
	out, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return out
}
