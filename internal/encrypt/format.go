// Package encrypt — формат шифрования тела объекта (ADR-0014).
//
// Объект шифруется сегментами по 64 KiB открытого текста, каждый сегмент —
// отдельный AES-256-GCM. Такое дробление нужно не для «красивости», а по трём
// причинам:
//
//   - Range-чтение. Читать объект с ленты целиком ради последних 100 байт
//     нельзя; нужен диапазон сегментов, и он вычисляется арифметически.
//   - Порча на ленте. Каждый сегмент проверяется тегом, поэтому битый байт
//     даёт ошибку, а не мусор в ответе. Это обязательное требование, а не
//     опция: молча отдать испорченные байты хуже, чем отказаться.
//   - Потоковая запись. PUT приходит без тела в памяти, шифрование идёт на
//     лету, размер шифротекста известен заранее.
//
// Ключи: DEK свой на каждую версию объекта, обёрнут в TEK тенанта, TEK обёрнут
// в KEK системы. Обёртку делает iam.KeyRing, здесь только работа с DEK.
package encrypt

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

const (
	// SegmentSize — размер сегмента открытого текста. После первой записи на
	// ленту число неизменяемо: формат данных на носителе не поменять.
	SegmentSize = 65536

	// TagSize — размер тега AES-GCM.
	TagSize = 16

	// CipherSegmentSize — размер зашифрованного сегмента целиком.
	CipherSegmentSize = SegmentSize + TagSize

	// KeySize — длина DEK/TEK (AES-256).
	KeySize = 32
)

// ErrCorruptedSegment — сегмент не прошёл проверку тега.
//
// Отличается от «файл короче, чем ждём» намеренно: короткая строка означает
// порванную запись или рассинхрон размеров, а неверный тег — изменённые байты.
// Клиенту в обоих случаях отдаётся одна и та же InternalError, но в лог должны
// попадать разные причины.
var ErrCorruptedSegment = errors.New("encrypt: сегмент не прошёл проверку аутентичности")

// ErrShortRead — шифротекста меньше, чем требует размер открытого текста.
var ErrShortRead = errors.New("encrypt: шифротекст короче ожидаемого")

// domain — префикс nonce. Меняется только вместе с форматом: он нужен, чтобы
// при гипотетической смене формата nonce старых объектов случайно не совпали с
// nonce новых при том же DEK.
const domain = "tapegw-seg-v1"

// CipherSize — размер шифротекста для plain байт открытого текста.
//
// Формула должна быть точной в обе стороны: по ней Range пересчитывает
// открытый диапазон в шифротекстовый, и ошибка на единицу даст либо чтение не
// тех байт, либо выход за конец файла.
func CipherSize(plain int64) int64 {
	if plain <= 0 {
		return 0
	}
	full := plain / SegmentSize
	rest := plain % SegmentSize
	size := full * CipherSegmentSize
	if rest > 0 {
		size += rest + TagSize
	}
	return size
}

// PlainSize — размер открытого текста по размеру шифротекста. Обратная к
// CipherSize функция; нужна, когда известен только размер файла на диске.
func PlainSize(cipherSize int64) int64 {
	if cipherSize <= 0 {
		return 0
	}
	full := cipherSize / CipherSegmentSize
	rest := cipherSize % CipherSegmentSize
	size := full * SegmentSize
	if rest > 0 {
		size += rest - TagSize
	}
	return size
}

// SegmentCount — сколько сегментов у открытого текста заданного размера.
// Пустой объект — ноль сегментов (не один пустой: шифровать нечего).
func SegmentCount(plain int64) int64 {
	if plain <= 0 {
		return 0
	}
	n := plain / SegmentSize
	if plain%SegmentSize != 0 {
		n++
	}
	return n
}

// nonce — 12 байт для AES-GCM из UUID версии, номера части и номера сегмента.
//
// В 12 байт тройка байт-в-байт не влезаёт (только UUID — 16), поэтому берётся
// SHA-256 от неё. Повтор nonce при одном DEK разрушает GCM целиком, поэтому
// здесь важна уникальность тройки, а не обратимость вывода: DEK свой на
// каждую версию, а внутри версии (часть, сегмент) не повторяются по
// построению.
func nonce(version uuid.UUID, part uint32, seg uint64) []byte {
	// Размер буфера считается от длины домена, а не пишется числом: опечатка в
	// таком числе — это паника на первом же сегменте.
	var buf [len(domain) + 16 + 4 + 8]byte
	n := copy(buf[:], domain)
	copy(buf[n:], version[:])
	binary.BigEndian.PutUint32(buf[n+16:], part)
	binary.BigEndian.PutUint64(buf[n+20:], seg)
	sum := sha256.Sum256(buf[:])
	return sum[:12]
}

// aad — открытые данные, привязанные к сегменту: UUID версии, номер части и
// ПОЛНЫЙ размер открытого текста этой части.
//
// Первые два поля не дают переставить сегмент между версиями и частями. Третье
// нужно от отдельного класса атак — усечения: без размера ни один сегмент не
// знает, сколько их всего, поэтому отрезанный по границе сегмента хвост GCM не
// заметит (каждый оставшийся сегмент по отдельности подлинный). Сейчас от этого
// защищает только Reader: он берёт размер из БД и при нехватке байт возвращает
// ErrShortRead.
//
// Это плохо именно для rebuild-meta (ADR-0014): там размер берётся из
// атрибутов Phobos, то есть с того же носителя, что и данные. Порча или подмена
// атрибута размера дала бы «успешное» расшифрование объекта без последних
// сегментов. С размером в AAD такой объект не пройдёт проверку аутентичности:
// целостность держится на криптографии, а не на целости метаданных.
//
// plainSize — размер открытого текста части: для обычного PUT размер объекта,
// для multipart — размер своей части (он лежит в object_parts навсегда).
func aad(version uuid.UUID, part uint32, plainSize int64) []byte {
	out := make([]byte, 0, 28)
	out = append(out, version[:]...)
	var p [12]byte
	binary.BigEndian.PutUint32(p[:4], part)
	binary.BigEndian.PutUint64(p[4:], uint64(plainSize))
	return append(out, p[:]...)
}

func newGCM(key []byte) (cipher.AEAD, error) {
	if len(key) != KeySize {
		return nil, fmt.Errorf("encrypt: ключ %d байт, нужно %d", len(key), KeySize)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("encrypt: aes: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("encrypt: gcm: %w", err)
	}
	return gcm, nil
}

// SealSegment — шифротекст одного сегмента.
//
// buf — буфер под результат длиной не меньше CipherSize(len(plain)); передаётся
// снаружи, чтобы в потоке не аллоцировать на каждый сегмент.
func SealSegment(dst, key []byte, version uuid.UUID, part uint32, seg uint64, plainSize int64, plain []byte) ([]byte, error) {
	gcm, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	if len(plain) > SegmentSize {
		return nil, fmt.Errorf("encrypt: сегмент %d байт, максимум %d", len(plain), SegmentSize)
	}
	out := gcm.Seal(dst[:0], nonce(version, part, seg), plain, aad(version, part, plainSize))
	return out, nil
}

// OpenSegment — открытый текст одного сегмента. Ошибка, если тег не сошёлся.
func OpenSegment(dst, key []byte, version uuid.UUID, part uint32, seg uint64, plainSize int64, data []byte) ([]byte, error) {
	gcm, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	out, err := gcm.Open(dst[:0], nonce(version, part, seg), data, aad(version, part, plainSize))
	if err != nil {
		return nil, fmt.Errorf("%w: часть %d сегмент %d", ErrCorruptedSegment, part, seg)
	}
	return out, nil
}

// Zero — обнуление памяти ключа.
//
// Go не гарантирует, что мусор в памяти соберёт сборщик, а DEK живёт в слайсе,
// который после расшифровки больше не нужен. Гарантия слабая (копирование при
// росте слайса остаётся), но дешевле нуля это ничего не стоит, поэтому делаем.
func Zero(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
