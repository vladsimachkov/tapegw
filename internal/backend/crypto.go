package backend

// Шифрование тела объекта (ADR-0014). Формат и ключи живут в internal/encrypt и
// internal/keys, здесь — только точки вставки в путь S3.
//
// Правило, которое тут главное: шифрование стоит ПОСЛЕ сверителей сумм и ПЕРЕД
// записью на диск. ETag и x-amz-checksum-* обязаны описывать открытые байты,
// потому что клиент сверяет именно их. Поставить шифрование в начало цепочки
// технически проще, и оно молча сломало бы сверку загрузок: сумма шифротекста
// тоже «сходится» сама с собой.

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/encrypt"
	"github.com/vladsimachkov/tapegw/internal/meta"
)

// Значение x-amz-server-side-encryption берётся из types.ServerSideEncryptionAes256
// (как у SSE-S3): ключ наш, клиенту сообщаем факт шифрования, но выбирать
// алгоритм ему нечем — SSE-C и SSE-KMS у нас NotImplemented.

// versionKey — DEK для записываемой версии и его обёртка.
//
// tenant — id тенанта БАКЕТА, а не учётки запроса: root проходит стража как
// обход и своего ProjectID не имеет, а объекты его бакетов живут в системном
// тенанте.
func (t *TapeGW) versionKey(ctx context.Context, tenant int64, version uuid.UUID) (dek, wrapped []byte, err error) {
	if t.keys == nil {
		// Не «шифруем открытым текстом», а отказ: молча записать открытые байты
		// означало бы, что объект выглядит зашифрованным, а на ленте лежит
		// читаемое содержимое.
		return nil, nil, fmt.Errorf("backend: хранилище ключей не настроено")
	}
	return t.keys.NewVersionDEK(ctx, tenant, version)
}

// plainReader — читатель открытых данных поверх файла шифротекста.
//
// length < 0 означает «весь объект». Возвращённый io.ReadCloser закрывает и
// файл: versitygw закрывает Body сам и только один раз.
func (t *TapeGW) plainReader(ctx context.Context, obj *meta.Object, f *os.File, start, length int64) (io.ReadCloser, error) {
	dek, err := t.keys.OpenVersionDEK(ctx, obj.TenantID, obj.WrappedDEK, obj.ID)
	if err != nil {
		// Ключ не развернулся: тенант уничтожен, KEK потерян или строка
		// испорчена. Ни одно не лечится на месте и ни одно нельзя выдать как
		// «объекта нет» — данные есть, прочитать их нельзя.
		return nil, fmt.Errorf("ключ версии %s: %w", obj.ID, err)
	}
	r, err := t.partsReader(ctx, obj, f, dek)
	encrypt.Zero(dek)
	if err != nil {
		return nil, fmt.Errorf("читатель %s: %w", obj.ID, err)
	}
	if length < 0 {
		return &closingReader{r: encrypt.NewSeqReader(r), c: f, c2: r}, nil
	}
	// Range — через io.SectionReader: он умеет ReadAt, а наш Reader его
	// реализует, поэтому границы сегментов считает encrypt, а не здесь.
	return &closingReader{
		r:  io.NewSectionReader(r, start, length),
		c:  f,
		c2: r,
	}, nil
}

// closingReader — читатель, закрывающий источник при Close. Раньше для Range
// был отдельный rangeReader поверх io.LimitReader: дескриптор нужно было
// закрывать вместе с ограничением. С шифрованием читатель и так обёртка,
// поэтому один тип на оба случая.
type closingReader struct {
	r  io.Reader
	c  io.Closer
	c2 io.Closer
}

func (c *closingReader) Read(p []byte) (int, error) { return c.r.Read(p) }

func (c *closingReader) Close() error {
	err := c.c.Close()
	if c.c2 != nil {
		if err2 := c.c2.Close(); err == nil {
			err = err2
		}
	}
	return err
}

// partsReader — читатель открытых данных: по таблице частей либо, если частей
// нет, одной частью нулевого номера.
//
// Без таблицы частей Range по multipart не вычислит, в какой части лежит
// запрошенный диапазон: у каждой части своё смещение шифротекста и свой размер
// в AAD. Один путь для обоих случаев — multipart с одной частью.
func (t *TapeGW) partsReader(ctx context.Context, obj *meta.Object, f *os.File, dek []byte) (*encrypt.Reader, error) {
	parts, err := t.meta.ObjectParts(ctx, obj.ID)
	if err != nil {
		return nil, err
	}
	if len(parts) == 0 {
		// Не «предположим одну часть», а отказ: с миграции 0014 часть есть у
		// любой версии с данными, и пустая таблица означает рассинхрон БД с
		// диском. Молча выбрать соль неоткуда — а без неё nonce не собрать.
		return nil, fmt.Errorf("у версии %s нет частей в object_parts", obj.ID)
	}
	enc := make([]encrypt.Part, 0, len(parts))
	for _, p := range parts {
		enc = append(enc, encrypt.Part{
			Number: uint32(p.Number), PlainSize: p.PlainSize,
			CipherOffset: p.CipherOffset, Salt: p.Salt,
		})
	}
	return encrypt.NewPartsReader(f, dek, obj.ID, enc, obj.Size)
}

// newSalt — соль новой части: 16 случайных байт.
//
// Генерируется на КАЖДУЮ попытку загрузки, а не на часть: переотправка части с
// тем же номером обязана дать другой набор nonce (ADR-0014).
func newSalt() ([]byte, error) {
	salt := make([]byte, encrypt.SaltSize)
	if _, err := rand.Read(salt); err != nil {
		return nil, fmt.Errorf("соль части: %w", err)
	}
	return salt, nil
}
