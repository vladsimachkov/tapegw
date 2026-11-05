package keys

// Экспорт ключей тенантов (ADR-0014).
//
// Это то место, где живут обёрнутые TEK, когда БД потеряна. На ленту они попасть
// не могут — обёрнутая TEK вместе с KEK восстанавливает «уничтоженный» тенант, а
// атрибуты Phobos на ленте непереписываемы. Значит восстановлению без БД нужен
// отдельный носитель: маленький файл, который хранится там же, где копия KEK, и
// который перестаёт содержать тенанта сразу после его удаления.
//
// Отсюда два правила формата.
//
// Первый: в экспорте только живые тенанты. meta.TenantsWithKeys выбирает строки с
// wrapped_key IS NOT NULL, а уничтожение ключа ставит NULL (миграция 0013), так
// что уничтоженный тенант выпадает из выборки сам. Но «выпал из SQL» — не
// гарантия: проверка на «в экспорте нет уничтоженного тенанта» обязана быть
// тестом, а не свойством запроса, потому что именно на этом держится обещание
// удаления.
//
// Второй: каждая обёртка аутентифицирована сама по себе. Обёртка TEK завернута
// AES-GCM под KEK с AAD = id тенанта (tekAAD), поэтому переставить обёртку на
// другого тенанта нельзя — тег не сойдётся. Чтение экспорта ОБЯЗАНА проверять
// каждую запись, а не доверять файлу: файл лежит на чужом носителе, и «данные
// похожи на правду» для него не аргумент.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/vladsimachkov/tapegw/internal/iam"
	"github.com/vladsimachkov/tapegw/internal/meta"
)

// ExportFormat — версия формата файла экспорта. Читатель обязан отказать на
// неизвестной версии: молча разобрать чужой формат значит восстановить не то.
const ExportFormat = 1

// Ошибки чтения экспорта. Разные — потому что и реакция разная: отсутствующий
// KEK означает «поднимите кольцо из хранилища», испорченный файл — «разберитесь,
// почему носитель изменился».
var (
	// ErrExportCorrupt — файл повреждён или подделан: обёртка не сходится со
	// своим тенантом, записи повторяются или заголовок противоречит содержимому.
	ErrExportCorrupt = errors.New("keys: экспорт ключей повреждён или подделан")

	// ErrExportUnknownKek — в экспорте есть обёртка, свёрнутая версией KEK,
	// которой нет в кольце. Это не порча файла: нужно то же кольцо KEK, при
	// котором экспорт создавался.
	ErrExportUnknownKek = errors.New("keys: в кольце нет версии KEK из экспорта")
)

// ExportEntry — одна строка экспорта: обёртка TEK конкретного тенанта.
type ExportEntry struct {
	Tenant  int64  `json:"tenant"`
	KEK     int    `json:"kek"`
	Wrapped []byte `json:"wrapped"`
}

// Export — содержимое файла.
type Export struct {
	Format  int       `json:"format"`
	Created time.Time `json:"created"`
	// KEKVersions — версии KEK, которыми свёрнуты лежащие ниже обёртки. Без
	// этого rebuild-meta не узнает, каким ключом браться, и не отличит «нужного
	// KEK нет» от «файл битый».
	KEKVersions []int         `json:"kek_versions"`
	ActiveKEK   int           `json:"active_kek"`
	Entries     []ExportEntry `json:"tenants"`
}

// BuildExport собирает экспорт из текущего состояния БД.
//
// Открытые TEK наружу не отдаются нигде: в файл попадают ровно те байты, что
// лежат в tenants.wrapped_key.
func BuildExport(ctx context.Context, m *meta.Meta, ring *iam.KeyRing) (*Export, error) {
	rows, err := m.TenantsWithKeys(ctx)
	if err != nil {
		return nil, err
	}
	e := &Export{
		Format:      ExportFormat,
		Created:     time.Now().UTC(),
		KEKVersions: ring.Versions(),
		ActiveKEK:   ring.Active(),
		Entries:     make([]ExportEntry, 0, len(rows)),
	}
	seen := make(map[int64]bool, len(rows))
	for _, r := range rows {
		if seen[r.ID] {
			// Дубликат в выборке означал бы, что по id тенанта возвращается
			// больше одной строки, и ниже по файлу одна из них «победила» бы
			// молча.
			return nil, fmt.Errorf("keys: тенант %d в выборке ключей дважды", r.ID)
		}
		seen[r.ID] = true
		e.Entries = append(e.Entries, ExportEntry{
			Tenant: r.ID, KEK: r.KekVersion, Wrapped: r.Wrapped,
		})
	}
	sort.Slice(e.Entries, func(i, j int) bool { return e.Entries[i].Tenant < e.Entries[j].Tenant })
	return e, nil
}

// exportFile — то, что лежит на диске. Обёртки в base64: значения обязано быть
// читаемым человеком (его проверяют при восстановлении DR), а base64 — единственный
// способ не испортить двоичные байты ни кодировкой, ни редактором.
type exportFile struct {
	Format      int    `json:"format"`
	Created     string `json:"created"`
	KEKVersions []int  `json:"kek_versions"`
	ActiveKEK   int    `json:"active_kek"`
	Tenants     []struct {
		Tenant  int64  `json:"tenant"`
		KEK     int    `json:"kek"`
		Wrapped string `json:"wrapped"`
	} `json:"tenants"`
}

// Write пишет экспорт в поток.
func (e *Export) Write(w io.Writer) error {
	if e.Format == 0 {
		e.Format = ExportFormat
	}
	var f exportFile
	f.Format = e.Format
	f.Created = e.Created.UTC().Format(time.RFC3339)
	f.KEKVersions = e.KEKVersions
	f.ActiveKEK = e.ActiveKEK
	for _, en := range e.Entries {
		f.Tenants = append(f.Tenants, struct {
			Tenant  int64  `json:"tenant"`
			KEK     int    `json:"kek"`
			Wrapped string `json:"wrapped"`
		}{en.Tenant, en.KEK, base64.StdEncoding.EncodeToString(en.Wrapped)})
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(f)
}

// ReadExport читает и ПРОВЕРЯЕТ экспорт.
//
// Проверка — не «разобрался ли JSON», а вскрытие каждой обёртки под AAD своего
// тенанта. Пока обёртка не вскрылась, записи не существует: подменённая обёртка,
// обёртка, переставленная на другого тенанта, и запись, у которой поменяли id,
// дают ошибку здесь, а не молча подсовывают чужой ключ при восстановлении.
func ReadExport(r io.Reader, ring *iam.KeyRing) (*Export, error) {
	var f exportFile
	if err := json.NewDecoder(r).Decode(&f); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrExportCorrupt, err)
	}
	if f.Format != ExportFormat {
		return nil, fmt.Errorf("%w: формат %d, поддерживается %d", ErrExportCorrupt, f.Format, ExportFormat)
	}
	created, err := time.Parse(time.RFC3339, f.Created)
	if err != nil {
		return nil, fmt.Errorf("%w: дата %q: %v", ErrExportCorrupt, f.Created, err)
	}
	if len(f.Tenants) == 0 {
		// Пустой экспорт — не «нет тенантов в системе», а почти всегда ошибка
		// выгрузки. Принять его означало бы «восстановили ноль объектов, всё
		// хорошо».
		return nil, fmt.Errorf("%w: в экспорте ни одной записи", ErrExportCorrupt)
	}

	e := &Export{
		Format:      f.Format,
		Created:     created,
		KEKVersions: f.KEKVersions,
		ActiveKEK:   f.ActiveKEK,
		Entries:     make([]ExportEntry, 0, len(f.Tenants)),
	}
	declared := make(map[int]bool, len(f.KEKVersions))
	for _, v := range f.KEKVersions {
		declared[v] = true
	}
	seen := make(map[int64]bool, len(f.Tenants))
	for _, t := range f.Tenants {
		if seen[t.Tenant] {
			return nil, fmt.Errorf("%w: тенант %d встречается дважды", ErrExportCorrupt, t.Tenant)
		}
		seen[t.Tenant] = true
		if !declared[t.KEK] {
			return nil, fmt.Errorf("%w: обёртка тенанта %d свёрнута KEK %d, которого нет в заголовке",
				ErrExportCorrupt, t.Tenant, t.KEK)
		}
		wrapped, err := base64.StdEncoding.DecodeString(t.Wrapped)
		if err != nil {
			return nil, fmt.Errorf("%w: тенант %d: %v", ErrExportCorrupt, t.Tenant, err)
		}
		if len(wrapped) == 0 {
			return nil, fmt.Errorf("%w: тенант %d с пустой обёрткой", ErrExportCorrupt, t.Tenant)
		}
		// Вот здесь подмена и перестаёт быть незаметной.
		if _, err := ring.Open(wrapped, t.KEK, tekAAD(t.Tenant)); err != nil {
			if errors.Is(err, iam.ErrUnknownKekVersion) {
				return nil, fmt.Errorf("%w: тенант %d, KEK %d: %v", ErrExportUnknownKek, t.Tenant, t.KEK, err)
			}
			return nil, fmt.Errorf("%w: обёртка тенанта %d не вскрылась: %v", ErrExportCorrupt, t.Tenant, err)
		}
		e.Entries = append(e.Entries, ExportEntry{Tenant: t.Tenant, KEK: t.KEK, Wrapped: wrapped})
	}
	return e, nil
}

// Tenant — обёртка тенанта, если она есть в экспорте.
func (e *Export) Tenant(tenant int64) (ExportEntry, bool) {
	for _, en := range e.Entries {
		if en.Tenant == tenant {
			return en, true
		}
	}
	return ExportEntry{}, false
}
