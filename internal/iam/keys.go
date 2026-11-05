// Package iam — учётные записи versitygw поверх Postgres (ADR-0009).
//
// Это реализация auth.IAMService из versitygw: она попадает в
// embedgw.Config.IAMService и обходит все встроенные IAM-бэкенды, включая
// файловый iam_dir. Интерфейс ровно семь методов, и все они про одно: достать
// учётку, завести её, изменить, удалить, перечислить.
//
// Два инварианта, вокруг которых построен пакет:
//
//   - Секрет нигде не существует в открытом виде дольше, чем длится вызов.
//     В БД лежит secret_wrapped — AES-GCM, ключ вне БД (KEK), версия KEK рядом.
//     Открытый секрет не попадает ни в текст ошибки, ни в лог: тексты ошибок
//     собираются так, чтобы не печатать ни секрет, ни обёртку.
//   - Тенант — часть учётки, а не «дополнительное поле»: ProjectID versitygw и
//     есть tenant_id, поэтому отказ неверной пары «роль + системный тенант»
//     живёт здесь (ADR-0018), а схема — только вторая линия.
package iam

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
)

// Размеры AES-GCM. Nonce 12 байт — стандартный размер GCM, при котором
// шифрование не требует внутреннего хеширования nonce.
const (
	keySize   = 32 // AES-256
	nonceSize = 12
)

// Ошибки кольца ключей. В тексте нет ни ключей, ни обёрток — только версия,
// по которой можно понять, чего не хватает.
var (
	// ErrUnknownKekVersion — обёртка сделана версией KEK, которой нет в кольце.
	// Ошибка ожидаемая при ротации: старую версию KEK нельзя выбрасывать, пока
	// на неё ссылается хоть одна строка (ADR-0014).
	ErrUnknownKekVersion = errors.New("iam: версия KEK неизвестна")
	// ErrUnwrap — обёртка не развернулась: чужой ключ, испорченные байты или
	// обёртка от другой учётки. Причина не уточняется намеренно: по разнице
	// «тег не сошёлся» против «нет ключа» атакующий не должен различать, что
	// именно он угадал.
	ErrUnwrap = errors.New("iam: не удалось развернуть секрет учётки")
)

// KeyRing — версия KEK → ключ. Активная версия выбирается для новых обёрток,
// остальные остаются только для расшифровки: именно поэтому ротируется KEK, а
// не переписываются все строки accounts разом.
type KeyRing struct {
	active int
	keys   map[int][]byte
}

// NewKeyRing собирает кольцо из map «версия → 32-байтный ключ». Активная версия
// обязана присутствовать: иначе «заворачиваем во что-то непонятное» вместо
// отказа на старте.
func NewKeyRing(active int, keys map[int][]byte) (*KeyRing, error) {
	if active < 1 {
		return nil, fmt.Errorf("iam: активная версия KEK = %d, ожидается >= 1", active)
	}
	if len(keys) == 0 {
		return nil, errors.New("iam: кольцо ключей пуст")
	}

	ring := &KeyRing{active: active, keys: make(map[int][]byte, len(keys))}
	for v, key := range keys {
		if v < 1 {
			return nil, fmt.Errorf("iam: версия KEK = %d, ожидается >= 1", v)
		}
		if len(key) != keySize {
			return nil, fmt.Errorf("iam: ключ KEK версии %d = %d байт, ожидается %d",
				v, len(key), keySize)
		}
		// Копируем: вызывающий не должен иметь возможности поменять ключ
		// после того, как кольцо собрали.
		k := make([]byte, keySize)
		copy(k, key)
		ring.keys[v] = k
	}

	if _, ok := ring.keys[active]; !ok {
		return nil, fmt.Errorf("iam: активной версии KEK %d нет в кольце", active)
	}
	return ring, nil
}

// LoadKeyRing читает файл кольца KEK. Формат — JSON «версия → base64(ключ)»:
//
//	{ "1": "MDIz...", "2": "..." }
//
// Файл обязан быть доступен только владельцу (ADR-0009: «файл 0400 или Vault»):
// права шире 0400 — ошибка запуска, а не предупреждение. Потеря KEK означает
// нечитаемость всех данных всех тенантов, поэтому «файл читают все» — это
// дыра того же масштаба, что и KEK в репозитории.
func LoadKeyRing(path string, active int) (*KeyRing, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("iam: файл KEK: %w", err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("iam: файл KEK %s доступен %v, ожидается 0400 или 0600",
			path, info.Mode().Perm())
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("iam: прочитать файл KEK: %w", err)
	}

	var encoded map[string]string
	if err := json.Unmarshal(raw, &encoded); err != nil {
		// Текст файла не печатаем: там ключи.
		return nil, fmt.Errorf("iam: разобрать файл KEK: %v", err)
	}

	keys := make(map[int][]byte, len(encoded))
	for s, b64 := range encoded {
		var v int
		if _, err := fmt.Sscanf(s, "%d", &v); err != nil || v < 1 {
			return nil, fmt.Errorf("iam: файл KEK: версия %q — ожидалось целое >= 1", s)
		}
		key, err := base64.StdEncoding.DecodeString(b64)
		if err != nil {
			return nil, fmt.Errorf("iam: файл KEK: версия %d — не base64", v)
		}
		keys[v] = key
	}

	return NewKeyRing(active, keys)
}

// Active — версия KEK, которой заворачивают новые секреты.
func (r *KeyRing) Active() int { return r.active }

// Versions — все версии KEK в кольце, по возрастанию. Нужен экспорту ключей
// тенантов (ADR-0014): в заголовке файла обязано быть перечислено, какими KEK
// завернуты лежащие ниже обёртки, иначе rebuild-meta не поймёт, каким ключом
// браться, и не отличит «нужного KEK нет в кольце» от «файл испорчен».
func (r *KeyRing) Versions() []int {
	out := make([]int, 0, len(r.keys))
	for v := range r.keys {
		out = append(out, v)
	}
	sort.Ints(out)
	return out
}

// Seal заворачивает plaintext открытым текстом в AES-256-GCM. aad — контекст
// аутентификации (для учётки это access_key): обёртка, снятая с одной строки и
// переставленная в другую, не развернётся, потому что aad не совпадёт.
//
// Возвращает nonce || ciphertext||tag и версию KEK, которой всё завернуто, —
// версию пишет рядом колонка kek_version, и по ней же Open потом выбирает ключ.
func (r *KeyRing) Seal(plaintext, aad []byte) (wrapped []byte, kekVersion int, err error) {
	key, ok := r.keys[r.active]
	if !ok {
		return nil, 0, ErrUnknownKekVersion
	}

	gcm, err := newGCM(key)
	if err != nil {
		return nil, 0, err
	}

	nonce := make([]byte, nonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return nil, 0, fmt.Errorf("iam: nonce: %w", err)
	}

	sealed := gcm.Seal(nonce, nonce, plaintext, aad)
	return sealed, r.active, nil
}

// Open разворачивает обёртку ключом версии kekVersion. Именно версии из строки,
// а не активной: после ротации KEK старые учётки обязаны продолжать входить —
// переписывание всех secret_wrapped разом не вписывается в окно обслуживания.
func (r *KeyRing) Open(wrapped []byte, kekVersion int, aad []byte) ([]byte, error) {
	key, ok := r.keys[kekVersion]
	if !ok {
		return nil, fmt.Errorf("%w: %d", ErrUnknownKekVersion, kekVersion)
	}
	if len(wrapped) <= nonceSize {
		return nil, ErrUnwrap
	}

	gcm, err := newGCM(key)
	if err != nil {
		return nil, err
	}

	plaintext, err := gcm.Open(nil, wrapped[:nonceSize], wrapped[nonceSize:], aad)
	if err != nil {
		// Первичную причину (cipher: message authentication failed) не
		// пробрасываем: она ничего не объясняет оператору, но подтверждает
		// атакующему, что обёртка «почти правильная».
		return nil, ErrUnwrap
	}
	return plaintext, nil
}

func newGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("iam: AES: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("iam: GCM: %w", err)
	}
	if gcm.NonceSize() != nonceSize {
		return nil, fmt.Errorf("iam: размер nonce GCM = %d, ожидали %d", gcm.NonceSize(), nonceSize)
	}
	return gcm, nil
}
