package backend

// Манифест объекта (ADR-0019).
//
// Здесь собирается то, что дописывается в конец файла объекта: метаданные
// объекта и таблица частей. Собирается ДО fsync, одним проходом по телу, —
// поэтому PUT передаёт построение в спул колбэком: ETag обычного PUT это MD5 от
// тела, а второго прохода по телу не будет.
//
// Что сюда попадает и почему: бакет, ключ, ETag, content-type, пользовательские
// метаданные, блокировка и части. Всё это раньше лежало в атрибутах Phobos в
// открытом виде; теперь оно под тем же DEK, что и данные, и крипто-уничтожение
// тенанта действительно стирает имена, а не только содержимое.

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/encrypt"
)

// manifestInput — всё, что нужно для манифеста одной версии.
type manifestInput struct {
	Bucket      string
	Key         string
	ETag        string
	Size        int64
	ContentType string
	Meta        map[string]string
	// Mode/Until/Hold — Object Lock версии. Берутся уже вычисленными: манифест
	// обязан описывать ровно то, что уйдёт в строку objects, иначе
	// восстановленный объект отличался бы от исходного.
	Mode  string
	Until *time.Time
	Hold  bool
	Parts []encrypt.ManifestPart
	// Created — время записи версии. Ноль means «сейчас»: обычная запись не
	// передаёт его, а тестам нужен детерминированный манифест.
	Created time.Time
}

// hasLock — блокировка есть, если задан хоть один из трёх признаков. Отдельный
// режим «включена пустая блокировка» в S3 не бывает.
func (in manifestInput) hasLock() bool {
	return in.Mode != "" || in.Until != nil || in.Hold
}

// buildManifestDoc — открытый манифест версии.
func buildManifestDoc(in manifestInput) *encrypt.ManifestDoc {
	doc := &encrypt.ManifestDoc{
		V:           encrypt.ManifestFormat,
		Bucket:      in.Bucket,
		Key:         in.Key,
		ETag:        in.ETag,
		Size:        in.Size,
		ContentType: in.ContentType,
		Parts:       in.Parts,
		Created:     in.Created.UTC(),
	}
	if in.Created.IsZero() {
		doc.Created = time.Now().UTC()
	}
	if len(in.Meta) > 0 {
		doc.Meta = make(map[string]string, len(in.Meta))
		for k, v := range in.Meta {
			doc.Meta[k] = v
		}
	}
	if in.hasLock() {
		lock := &encrypt.ManifestLock{Mode: in.Mode, LegalHold: in.Hold}
		if in.Until != nil {
			lock.RetainUntil = in.Until.UTC().Format(time.RFC3339Nano)
		}
		doc.Lock = lock
	}
	return doc
}

// manifestTrailer — готовый хвост: шифротекст манифеста и футер.
//
// size из манифеста и size строки objects обязан быть одним числом, поэтому
// проверка на несовпадение размеров частей здесь, а не в читателе: файл с
// манифестом, который обещает не то, что содержит, дешевле заметить при записи.
func (t *TapeGW) manifestTrailer(dek []byte, version uuid.UUID, in manifestInput) ([]byte, error) {
	var sum int64
	for _, p := range in.Parts {
		if p.Plain < 0 || p.Off < 0 {
			return nil, fmt.Errorf("манифест %s: часть %d с отрицательными параметрами", version, p.Number)
		}
		sum += p.Plain
	}
	if sum != in.Size {
		return nil, fmt.Errorf("манифест %s: части дают %d байт, объявлено %d", version, sum, in.Size)
	}
	return encrypt.ManifestTrailer(nil, dek, version, buildManifestDoc(in))
}

// putManifestParts — таблица частей обычного PUT: одна часть №1. Номер обязан
// совпасть с тем, что заведёт meta.PutObject, и с номером, под которым
// шифровались сегменты.
func putManifestParts(plainSize int64, salt []byte) []encrypt.ManifestPart {
	return []encrypt.ManifestPart{{Number: 1, Plain: plainSize, Off: 0, Salt: salt}}
}
