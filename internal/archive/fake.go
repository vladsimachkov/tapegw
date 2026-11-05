package archive

// Дисковый фейк архива (см. archive.go). Каталог, где каждый объект — файл с
// именем `<oid>` и соседним `<oid>.attrs` в JSON.
//
// Фейк намеренно не «умный»: он позволяет собрать архив любой степени лживости —
// объект без атрибута, атрибуты без объекта, манифест не под тем DEK, объект
// тенанта, которого нет в экспорте ключей. Именно на таких сборках и проверяется,
// что rebuild-meta отказает или пропустит с записью в отчёте, а не смолчит.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const attrsSuffix = ".attrs"

// Fake — архив в каталоге.
type Fake struct {
	dir string
}

func NewFake(dir string) *Fake { return &Fake{dir: dir} }

// Put кладёт объект и его атрибуты. В тестах — способ собрать архив вручную.
func (f *Fake) Put(oid string, data []byte, attrs map[string]string) error {
	if err := os.WriteFile(filepath.Join(f.dir, oid), data, 0o600); err != nil {
		return fmt.Errorf("archive: объект %s: %w", oid, err)
	}
	raw, err := json.Marshal(attrs)
	if err != nil {
		return fmt.Errorf("archive: атрибуты %s: %w", oid, err)
	}
	return os.WriteFile(filepath.Join(f.dir, oid+attrsSuffix), raw, 0o600)
}

// RemoveObject убирает данные объекта, оставив атрибуты: способ проверить, что
// восстановление не молчит на объекте, который перечислен, но не читается.
func (f *Fake) RemoveObject(oid string) error {
	return os.Remove(filepath.Join(f.dir, oid))
}

// Path — путь к файлу данных объекта. Нужен тестам: они сравнивают байты архива
// с исходными и портят их, чтобы проверить обнаружение порчи.
func (f *Fake) Path(oid string) string { return filepath.Join(f.dir, oid) }

func (f *Fake) List(context.Context) ([]ObjectRef, error) {
	entries, err := os.ReadDir(f.dir)
	if err != nil {
		return nil, fmt.Errorf("archive: каталог %s: %w", f.dir, err)
	}
	var out []ObjectRef
	for _, e := range entries {
		if e.IsDir() || strings.HasSuffix(e.Name(), attrsSuffix) {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(f.dir, e.Name()+attrsSuffix))
		if err != nil {
			// Объект без атрибутов — не «пропустим»: именно так выглядит
			// архив, в который mover записал данные и не записал метаданные.
			return nil, fmt.Errorf("archive: у объекта %s нет атрибутов: %w", e.Name(), err)
		}
		var attrs map[string]string
		if err := json.Unmarshal(raw, &attrs); err != nil {
			return nil, fmt.Errorf("archive: атрибуты %s: %w", e.Name(), err)
		}
		out = append(out, ObjectRef{Oid: e.Name(), Attrs: attrs})
	}
	// Порядок детерминированный: отчёт восстановления должен быть сравниваем.
	sort.Slice(out, func(i, j int) bool { return out[i].Oid < out[j].Oid })
	return out, nil
}

func (f *Fake) Size(_ context.Context, oid string) (int64, error) {
	st, err := os.Stat(filepath.Join(f.dir, oid))
	if err != nil {
		return 0, fmt.Errorf("archive: размер %s: %w", oid, err)
	}
	return st.Size(), nil
}

func (f *Fake) ReadAt(_ context.Context, oid string, p []byte, off int64) (int, error) {
	fh, err := os.Open(filepath.Join(f.dir, oid))
	if err != nil {
		return 0, fmt.Errorf("archive: чтение %s: %w", oid, err)
	}
	defer fh.Close()
	return fh.ReadAt(p, off)
}

// Compile-time: Fake обязан оставаться Archive.
var _ Archive = (*Fake)(nil)
