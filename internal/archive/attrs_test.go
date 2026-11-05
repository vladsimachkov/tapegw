package archive_test

import (
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/archive"
)

func testVersion() archive.Version {
	return archive.Version{
		Version:    uuid.New(),
		Tenant:     42,
		WrappedDEK: []byte("обёртка DEK — 48 байт на самом деле"),
		KEK:        "v3",
	}
}

// TestAttributeSetIsExactlyWhatIsAllowed — главный тест файла.
//
// Атрибуты Phobos лежат в открытом виде в индексе LTFS на каждой ленте. Любая
// лишняя пара — это то, что пережило крипто-уничтожение тенанта. Поэтому
// сверяется не «в списке есть нужное», а точное равенство наборов.
func TestAttributeSetIsExactlyWhatIsAllowed(t *testing.T) {
	attrs, err := archive.Attributes(testVersion())
	if err != nil {
		t.Fatal(err)
	}
	if len(attrs) != len(archive.AttrAttributes) {
		t.Fatalf("атрибутов %d, разрешено %d", len(attrs), len(archive.AttrAttributes))
	}
	for _, k := range archive.AttrAttributes {
		if _, ok := attrs[k]; !ok {
			t.Fatalf("атрибут %q пропал", k)
		}
	}
	for k := range attrs {
		found := false
		for _, want := range archive.AttrAttributes {
			if k == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("в атрибуты попал не разрешённый ключ %q", k)
		}
	}
}

// TestAttributesCarryNoNames — ни одно значение атрибута не должно напоминать
// имя бакета или ключа объекта. Тест грубый намеренно: он про свойство, а не про
// конкретное поле, и обязан сработать на любом будущем атрибуте.
func TestAttributesCarryNoNames(t *testing.T) {
	v := testVersion()
	attrs, err := archive.Attributes(v)
	if err != nil {
		t.Fatal(err)
	}
	// Значения, которые обязаны были бы оставить след, если бы бакет или ключ
	// туда попали.
	for k, val := range attrs {
		if strings.ContainsAny(val, "/") && k != archive.AttrVersion {
			t.Fatalf("атрибут %q содержит слэш — похоже на имя объекта: %q", k, val)
		}
	}
	if _, ok := attrs["tapegw.bucket"]; ok {
		t.Fatal("бакет в атрибутах")
	}
	if _, ok := attrs["tapegw.key"]; ok {
		t.Fatal("ключ объекта в атрибутах")
	}
	if _, ok := attrs["tapegw.etag"]; ok {
		t.Fatal("ETag в атрибутах")
	}
}

// TestAttributesHaveNoTEK — обёрнутой TEK в атрибутах быть не может.
//
// Это не «лишний ключ», а отмена крипто-уничтожения. Цепочка:
//
//   - удаление тенанта (ADR-0014) — это уничтожение его TEK в tenants;
//   - атрибуты Phobos записаны в индекс LTFS на КАЖДОЙ ленте, где есть объект
//     тенанта, и переписать их там нельзя;
//   - KEK по тому же ADR хранится, пока на его версию ссылается хоть одна
//     обёртка, а теперь ссылаются обёртки на лентах — то есть почти навсегда;
//   - итог: «уничтоженный» TEK разворачивается из любой ленты тем же KEK, а за
//     ним все DEK и все данные тенанта.
//
// Обёрнутые TEK живут в отдельном экспорте ключей тенантов (ADR-0014), который
// хранится отдельно от лент и перестаёт содержать тенант сразу после его
// удаления. Проверка дублирует TestAttributeSetIsExactlyWhatIsAllowed намеренно:
// тот ловит «появился новый ключ», этот — «вернули именно TEK».
func TestAttributesHaveNoTEK(t *testing.T) {
	attrs, err := archive.Attributes(testVersion())
	if err != nil {
		t.Fatal(err)
	}
	for k := range attrs {
		if strings.Contains(strings.ToLower(k), "tek") {
			t.Fatalf("в атрибуты попала обёртка TEK (%q): крипто-уничтожение тенанта перестанет быть окончательным", k)
		}
	}
	for _, a := range archive.AttrAttributes {
		if strings.Contains(strings.ToLower(a), "tek") {
			t.Fatalf("в списке разрешённых атрибутов есть TEK-ключ %q", a)
		}
	}
}

func TestAttributesRoundTrip(t *testing.T) {
	v := testVersion()
	attrs, err := archive.Attributes(v)
	if err != nil {
		t.Fatal(err)
	}
	got, err := archive.ParseAttributes(attrs)
	if err != nil {
		t.Fatalf("обратный разбор: %v", err)
	}
	if got.Version != v.Version || got.Tenant != v.Tenant || got.KEK != v.KEK {
		t.Fatalf("разъехались скаляры: %+v", got)
	}
	if string(got.WrappedDEK) != string(v.WrappedDEK) {
		t.Fatal("обёртка DEK испорчена при переносе")
	}
}

// TestAttributesRejectEmpty — пустая обёртка не должна уходить на ленту:
// объект без ключа — это объект, который невозможно прочитать, и узнать об этом
// надо при архивации, а не при восстановлении.
func TestAttributesRejectEmpty(t *testing.T) {
	v := testVersion()
	v.WrappedDEK = nil
	if _, err := archive.Attributes(v); err == nil {
		t.Fatal("пустая обёртка DEK принята")
	}
	v = testVersion()
	v.Tenant = 0
	if _, err := archive.Attributes(v); err == nil {
		t.Fatal("тенант 0 принят")
	}
}

// TestParseAttributesStrict — неизвестный ключ это ошибка. Молча проигнорировать
// — значит потерять атрибут, который появился в более новой версии формата.
func TestParseAttributesStrict(t *testing.T) {
	v := testVersion()
	attrs, err := archive.Attributes(v)
	if err != nil {
		t.Fatal(err)
	}
	extra := map[string]string{}
	for k, val := range attrs {
		extra[k] = val
	}
	extra["tapegw.что-то-новое"] = "1"
	if _, err := archive.ParseAttributes(extra); err == nil {
		t.Fatal("неизвестный атрибут проигнорирован")
	}

	for _, drop := range archive.AttrAttributes {
		less := map[string]string{}
		for k, val := range attrs {
			if k != drop {
				less[k] = val
			}
		}
		if _, err := archive.ParseAttributes(less); err == nil {
			t.Fatalf("отсутствующий атрибут %q не замечен", drop)
		}
	}
}

// TestFormatVersionUnknown — читатель обязан отказаться, а не угадывать.
func TestFormatVersionUnknown(t *testing.T) {
	v := testVersion()
	v.Format = 7
	attrs, err := archive.Attributes(v)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := archive.ParseAttributes(attrs); err == nil {
		t.Fatal("неизвестная версия формата проглочена")
	}
}
