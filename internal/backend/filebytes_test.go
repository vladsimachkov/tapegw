package backend_test

import (
	"os"
	"strings"
	"testing"

	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/spool"
)

// file_bytes — длина файла на диске: участник пачки = файл целиком (ADR-0020),
// и загрузчик объявляет длину участника по этой колонке. PUT и
// CompleteMultipart обязаны записать её в той же транзакции, что и строка
// объекта, ровно такой, какой файл увидит stat: ошибка здесь — обрезанный
// манифест на ленте, то есть нечитаемые данные. Поэтому проверяем обе формы
// записи по факту на ФС, а не по тому, что вернул бэкенд.
func TestPutAndCompleteRecordFileBytes(t *testing.T) {
	be, m, land := newBackend(t)
	mustBucket(t, be)

	// 1. Обычный PUT: одна часть, манифест дописан в тот же проход.
	putBody(t, be, "b", "plain.bin", strings.Repeat("тело ", 2000))
	assertFileBytes(t, land, currentObject(t, m, "b", "plain.bin"))

	// 2. Multipart: файл склеен из частей (размеры — как на боевом пути,
	// части кроме последней не меньше MinPartSize), хвост манифеста один.
	fullMPU(t, be, "b", "mp.bin")
	assertFileBytes(t, land, currentObject(t, m, "b", "mp.bin"))
}

// assertFileBytes сверяет колонку с фактической длиной файла на диске.
func assertFileBytes(t *testing.T, land *spool.Spool, obj *meta.Object) {
	t.Helper()
	if obj.FileBytes == nil {
		t.Fatalf("file_bytes у %s NULL: PUT обязан записать её в той же транзакции", obj.ID)
	}
	st, err := os.Stat(land.Path(obj.ID))
	if err != nil {
		t.Fatalf("stat файла %s: %v", obj.ID, err)
	}
	if *obj.FileBytes != st.Size() {
		t.Errorf("file_bytes = %d, на диске %d", *obj.FileBytes, st.Size())
	}
	// Признак, что в колонку легла длина файла, а не открытые байты: файл
	// зашифрован и с манифестом, значит обязан быть больше Size.
	if *obj.FileBytes <= obj.Size {
		t.Errorf("file_bytes = %d не больше Size = %d: похоже, записали открытые байты",
			*obj.FileBytes, obj.Size)
	}
}
