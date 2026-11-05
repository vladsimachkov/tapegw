// Package archive — то, что ложится рядом с данными объекта на носителе.
//
// Это не про S3 и не про нашу БД: здесь описан минимум метаданных, который
// mover (ADR-0007) передаёт Phobos при архивации, и из которого rebuild-meta
// потом поднимает объект, не имея ничего, кроме ленты и KEK.
//
// Правило, из которого растёт весь этот файл: в атрибутах Phobos лежат только
// те поля, без которых нельзя развернуть ключ. Всё остальное — бакет, ключ,
// ETag, content-type, пользовательские метаданные, блокировка — уехало в
// зашифрованный манифест (ADR-0019). Причина простая: атрибуты хранятся в
// открытом виде в индексе LTFS на каждой ленте и в DSS. Оставь в них имена, и
// после крипто-уничтожения тенанта (ADR-0014) имена его объектов переживут сами
// данные, а обещание «удалим ваши данные» будет выполнено только наполовину.
//
// Второе правило, строже первого: на ленту не попадает НИЧЕГО, что вместе с KEK
// даёт доступ к данным тенанта после его удаления (ADR-0014). Поэтому здесь нет
// обёртки TEK. Она расшифровывается тем же KEK, лежит в индексе каждой ленты и
// непереписываема, так что «уничтоженный» тенант поднялся бы из любого своего
// объекта — а KEK как раз и хранится, пока на него кто-то ссылается. Обёрнутый
// DEK при этом остаётся: без TEK он бесполезен.
package archive

import (
	"encoding/base64"
	"fmt"
	"sort"
	"strconv"

	"github.com/google/uuid"
)

// Имена атрибутов. Префикс tapegw обязателен: Phobos кладёт в тот же набор
// свои служебные ключи (object_uuid, object_size, md5, layout), и конфликтовать
// с ними незачем.
const (
	AttrVersion = "tapegw.version"
	AttrTenant  = "tapegw.tenant"
	AttrDEK     = "tapegw.dek"
	AttrKEK     = "tapegw.kek"
	AttrFormat  = "tapegw.format"
)

// FormatVersion — версия формата файла объекта. Читатель обязан отказать на
// неизвестном значении, а не пытаться угадать: молча разобрать файл чужим
// форматом значит выдать мусор вместо ошибки.
const FormatVersion = 1

// Version — минимум, без которого объект с ленты не развернуть.
type Version struct {
	// Version — UUID версии объекта; он же oid объекта в Phobos.
	Version uuid.UUID
	// Tenant — id тенанта: по нему ищется TEK, которым обёрнут DEK, и в отчёте
	// rebuild-meta группы объектов собираются по тенанту.
	Tenant int64
	// WrappedDEK — обёрнутый DEK версии, сырые байты. Кладётся base64: Phobos
	// молча заменяет не-UTF-8 байты на U+FFFD (спайк M2-01), и двоичное значение
	// иначе было бы испорчено без единого признака ошибки.
	//
	// Обёртки TEK здесь НЕТ, и это не недосмотр, а правило ADR-0014: на ленту не
	// попадает ничего, что вместе с KEK даёт доступ к данным тенанта после его
	// удаления. Обёрнутый DEK можно — без TEK он бесполезен. Обёрнутая TEK
	// нельзя: она лежит в индексе LTFS на каждой ленте, переписать её нельзя, и
	// «уничтоженный» тенант восстановился бы из любого его объекта.
	//
	// Обёрнутые TEK живут в отдельном хранилище ключей тенантов (экспорт
	// tenants.wrapped_key), которое хранится там же, где копия KEK, и
	// перестает содержать тенант сразу после его удаления. См. ADR-0014.
	WrappedDEK []byte
	// KEK — версия KEK, которой развёрнута обёртка DEK. Без неё не понять,
	// каким ключом браться, а ротация KEK (ADR-0014) делает таких версий
	// несколько.
	KEK string
	// Format — версия формата файла объекта.
	Format int
}

// Attributes — атрибуты Phobos для этой версии: ровно набор ключей
// AttrAttributes, не больше.
//
// Функция чистая и ничего не знает о Phobos именно затем, чтобы её можно было
// проверить: атрибуты пишет mover (M4), и мутацию «ключ объекта уехал в
// атрибуты» иначе ловить было бы не на чём.
func Attributes(v Version) (map[string]string, error) {
	if len(v.WrappedDEK) == 0 {
		return nil, fmt.Errorf("archive: пустая обёртка DEK для версии %s", v.Version)
	}
	if v.Tenant <= 0 {
		return nil, fmt.Errorf("archive: тенант %d у версии %s", v.Tenant, v.Version)
	}
	if v.KEK == "" {
		return nil, fmt.Errorf("archive: не указана версия KEK у версии %s", v.Version)
	}
	f := v.Format
	if f == 0 {
		f = FormatVersion
	}
	return map[string]string{
		AttrVersion: v.Version.String(),
		AttrTenant:  strconv.FormatInt(v.Tenant, 10),
		AttrDEK:     base64.StdEncoding.EncodeToString(v.WrappedDEK),
		AttrKEK:     v.KEK,
		AttrFormat:  strconv.Itoa(f),
	}, nil
}

// AttrAttributes — полный и единственный допустимый набор ключей атрибутов.
//
// Тест сверяет с ним результат Attributes построчно: лишняя пара «tapegw.key:
// photos/2027/01/договор-4.pdf» появилась бы здесь как расхождение, а не
// осталась незамеченной до первой ленты.
var AttrAttributes = []string{
	AttrDEK, AttrFormat, AttrKEK, AttrTenant, AttrVersion,
}

// ParseAttributes — разобрать атрибуты, снятые с объекта Phobos. Обратная к
// Attributes; нужна rebuild-meta, и написана строго: неизвестный ключ это
// ошибка, а не «проигнорируем».
func ParseAttributes(attrs map[string]string) (Version, error) {
	for k := range attrs {
		known := false
		for _, want := range AttrAttributes {
			if k == want {
				known = true
				break
			}
		}
		if !known {
			return Version{}, fmt.Errorf("archive: неизвестный атрибут %q", k)
		}
	}
	for _, want := range AttrAttributes {
		if _, ok := attrs[want]; !ok {
			return Version{}, fmt.Errorf("archive: атрибут %q отсутствует", want)
		}
	}
	ver, err := uuid.Parse(attrs[AttrVersion])
	if err != nil {
		return Version{}, fmt.Errorf("archive: атрибут %q: %w", AttrVersion, err)
	}
	tenant, err := strconv.ParseInt(attrs[AttrTenant], 10, 64)
	if err != nil {
		return Version{}, fmt.Errorf("archive: атрибут %q: %w", AttrTenant, err)
	}
	dek, err := base64.StdEncoding.DecodeString(attrs[AttrDEK])
	if err != nil {
		return Version{}, fmt.Errorf("archive: атрибут %q не base64: %w", AttrDEK, err)
	}
	format, err := strconv.Atoi(attrs[AttrFormat])
	if err != nil {
		return Version{}, fmt.Errorf("archive: атрибут %q: %w", AttrFormat, err)
	}
	if format != FormatVersion {
		return Version{}, fmt.Errorf("archive: формат %d, поддерживается %d", format, FormatVersion)
	}
	if len(dek) == 0 {
		return Version{}, fmt.Errorf("archive: пустая обёртка DEK у версии %s", ver)
	}
	return Version{
		Version: ver, Tenant: tenant, WrappedDEK: dek,
		KEK: attrs[AttrKEK], Format: format,
	}, nil
}

// SortedAttrNames — AttrAttributes в алфавитном порядке, для отчётов и логов.
func SortedAttrNames() []string {
	out := append([]string(nil), AttrAttributes...)
	sort.Strings(out)
	return out
}
