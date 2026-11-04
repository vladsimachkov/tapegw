// Package keys — иерархия ключей шифрования (ADR-0014): KEK → TEK → DEK.
//
// KEK живёт вне БД (iam.KeyRing), TEK — обёрнутый в tenants.wrapped_key, DEK —
// обёрнутый в objects.wrapped_dek, свой на каждую версию объекта.
//
// Пакет стоит отдельно от iam.KeyRing по той же причине, по которой он отдельно
// от meta: meta импортирует iam, поэтому общий слой, которому нужны оба, не может
// быть ни тем, ни другим.
package keys

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/encrypt"
	"github.com/vladsimachkov/tapegw/internal/iam"
	"github.com/vladsimachkov/tapegw/internal/meta"
)

// ErrKeyGone — ключ тенанта уничтожен. Данные этого тенанта больше не читаются
// никогда: это и есть крипто-уничтожение.
var ErrKeyGone = errors.New("keys: ключ тенанта уничтожен")

// AAD обёрток. Разные префиксы нужны, чтобы обёртка TEK, случайно попавшая в
// колонку wrapped_dek (ошибка в коде — не атака), не развернулась в DEK.
const (
	tekDomain = "tapegw-tek-v1"
	dekDomain = "tapegw-dek-v1"
)

// Vault выдаёт ключи и прячет обёртку. Кэша нет намеренно: на PUT это один
// SELECT и одно расшифрование, а рядом fsync гигабайт — сравнивать не с чем.
// Кэш появится вместе с причиной (замер на тысячах тенантов), а не раньше.
type Vault struct {
	meta *meta.Meta
	ring *iam.KeyRing
}

func New(m *meta.Meta, ring *iam.KeyRing) *Vault {
	return &Vault{meta: m, ring: ring}
}

func tekAAD(tenant int64) []byte {
	var b [len(tekDomain) + 8]byte
	copy(b[:], tekDomain)
	binary.BigEndian.PutUint64(b[len(tekDomain):], uint64(tenant))
	return b[:]
}

func dekAAD(version uuid.UUID) []byte {
	out := make([]byte, 0, len(dekDomain)+16)
	out = append(out, dekDomain...)
	return append(out, version[:]...)
}

// tek — ключ тенанта, заведённый при первом обращении.
//
// Идея ленивого заведения в том, что миграция KEK не знает: вставить обёртку
// SQL-ом невозможно, а NULL в колонке означает «ключа ещё нет», а не «данные не
// шифруются». Проигравший гонку перечитывает ключ победителя — двух TEK у
// тенанта быть не может.
func (v *Vault) tek(ctx context.Context, tenant int64) ([]byte, error) {
	row, err := v.meta.TenantKey(ctx, tenant)
	if err != nil {
		return nil, err
	}
	if row.Destroyed {
		// Данные тенанта крипто-уничтожены. Заводить новый TEK нельзя: это
		// означало бы, что уничтожение ничего не уничтожило, а старые байты
		// остались бы в доступном виде для нового ключа.
		return nil, ErrKeyGone
	}
	if len(row.Wrapped) == 0 {
		tek := make([]byte, encrypt.KeySize)
		if _, err := rand.Read(tek); err != nil {
			return nil, fmt.Errorf("keys: генерация TEK: %w", err)
		}
		wrapped, kekVersion, err := v.ring.Seal(tek, tekAAD(tenant))
		encrypt.Zero(tek)
		if err != nil {
			return nil, fmt.Errorf("keys: обёртка TEK: %w", err)
		}
		row, err = v.meta.EnsureTenantKey(ctx, tenant, wrapped, kekVersion)
		if err != nil {
			return nil, err
		}
		if len(row.Wrapped) == 0 {
			// Тенант удалён параллельно, или ключ уничтожен между чтением и
			// записью. Молча заводить второй ключ нельзя.
			return nil, ErrKeyGone
		}
	}

	tek, err := v.ring.Open(row.Wrapped, row.KekVersion, tekAAD(tenant))
	if err != nil {
		if errors.Is(err, iam.ErrUnknownKekVersion) {
			return nil, fmt.Errorf("keys: версия KEK %d недоступна: %w", row.KekVersion, err)
		}
		// Обёртка не развернулась. Причина может быть в порче строки или в
		// уничтоженном KEK; ни то ни другое не лечится на месте.
		return nil, fmt.Errorf("keys: развернуть TEK тенанта %d: %w", tenant, err)
	}
	return tek, nil
}

// NewVersionDEK — новый DEK для версии объекта: возвращает сам ключ и его
// обёртку для колонки objects.wrapped_dek.
//
// Обёртка привязана к UUID версии (AAD): переставить обёртку одной версии в
// строку другой — значит получить ошибку расшифрования, а не чужие данные.
func (v *Vault) NewVersionDEK(ctx context.Context, tenant int64, version uuid.UUID) (dek, wrapped []byte, err error) {
	tek, err := v.tek(ctx, tenant)
	if err != nil {
		return nil, nil, err
	}
	defer encrypt.Zero(tek)

	dek = make([]byte, encrypt.KeySize)
	if _, err := rand.Read(dek); err != nil {
		return nil, nil, fmt.Errorf("keys: генерация DEK: %w", err)
	}
	wrapped, err = sealWithTEK(tek, dek, dekAAD(version))
	if err != nil {
		encrypt.Zero(dek)
		return nil, nil, fmt.Errorf("keys: обёртка DEK: %w", err)
	}
	return dek, wrapped, nil
}

// OpenVersionDEK — DEK по обёртке из строки объекта.
func (v *Vault) OpenVersionDEK(ctx context.Context, tenant int64, wrapped []byte, version uuid.UUID) ([]byte, error) {
	if len(wrapped) == 0 {
		return nil, fmt.Errorf("keys: у версии %s нет обёртки DEK", version)
	}
	tek, err := v.tek(ctx, tenant)
	if err != nil {
		return nil, err
	}
	defer encrypt.Zero(tek)
	return sealWithTEKOpen(tek, wrapped, dekAAD(version))
}

// RotateKEK переоборачивает все TEK активной версией KEK. DEK не трогаются
// вообще: перешифровываются тысячи строк, а не петабайты данных — в этом и
// смысл иерархии (ADR-0014).
//
// Идемпотентен: строка уже на активной версии пропускается. Прерванный прогон
// повторяется без последствий.
func (v *Vault) RotateKEK(ctx context.Context) (int, error) {
	tenants, err := v.meta.TenantsWithKeys(ctx)
	if err != nil {
		return 0, err
	}
	active := v.ring.Active()
	rotated := 0
	for _, row := range tenants {
		if row.KekVersion == active {
			continue
		}
		tek, err := v.ring.Open(row.Wrapped, row.KekVersion, tekAAD(row.ID))
		if err != nil {
			return rotated, fmt.Errorf("keys: тенант %d: %w", row.ID, err)
		}
		wrapped, kekVersion, err := v.ring.Seal(tek, tekAAD(row.ID))
		encrypt.Zero(tek)
		if err != nil {
			return rotated, fmt.Errorf("keys: тенант %d: %w", row.ID, err)
		}
		if err := v.meta.SetTenantKey(ctx, row.ID, wrapped, kekVersion); err != nil {
			return rotated, err
		}
		rotated++
	}
	return rotated, nil
}

// sealWithTEK / openWithTEK — обёртка DEK ключом TEK. AES-256-GCM с случайным
// nonce: в отличие от сегментов данных, здесь нет потока, который надо
// воспроизводить по позиции, поэтому nonce берётся из генератора.
func sealWithTEK(tek, plain, aad []byte) ([]byte, error) {
	gcm, err := gcmFor(tek)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("keys: nonce: %w", err)
	}
	return gcm.Seal(nonce, nonce, plain, aad), nil
}

func sealWithTEKOpen(tek, wrapped, aad []byte) ([]byte, error) {
	gcm, err := gcmFor(tek)
	if err != nil {
		return nil, err
	}
	if len(wrapped) <= gcm.NonceSize() {
		return nil, fmt.Errorf("keys: обёртка DEK слишком короткая")
	}
	plain, err := gcm.Open(nil, wrapped[:gcm.NonceSize()], wrapped[gcm.NonceSize():], aad)
	if err != nil {
		// Первопричину не пробрасываем по той же причине, что и в KeyRing.Open:
		// она подтверждает атакующему близость обёртки, оператору не говорит
		// ничего.
		return nil, fmt.Errorf("keys: развернуть DEK: ключ тенанта не подошёл")
	}
	return plain, nil
}

// gcmFor — AES-256-GCM для TEK. Реализация не берётся из encrypt: там GCM
// заточен под формат сегментов (детерминированный nonce), а здесь нужен
// обычный AEAD над коротким ключом.
func gcmFor(key []byte) (cipher.AEAD, error) {
	if len(key) != encrypt.KeySize {
		return nil, fmt.Errorf("keys: ключ %d байт, нужно %d", len(key), encrypt.KeySize)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("keys: aes: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("keys: gcm: %w", err)
	}
	return gcm, nil
}

// uploadAAD — AAD обёртки DEK незавершённой загрузки.
//
// Отдельный домен и отдельный материал нужны затем, чтобы обёртку из
// uploads.wrapped_dek нельзя было вставить в objects.wrapped_dek: там AAD
// строится от UUID версии. Разные домены делают такую подмену ошибкой
// разворачивания, а не «ключ подошёл, данные чужие».
func uploadAAD(uploadID string) []byte {
	out := make([]byte, 0, len(dekDomain)+len(uploadID)+1)
	out = append(out, dekDomain...)
	out = append(out, 'U')
	return append(out, uploadID...)
}

// NewUploadDEK — DEK для multipart-загрузки: один на все части (ADR-0014,
// различают части nonce и AAD, а не ключи).
func (v *Vault) NewUploadDEK(ctx context.Context, tenant int64, uploadID string) (dek, wrapped []byte, err error) {
	tek, err := v.tek(ctx, tenant)
	if err != nil {
		return nil, nil, err
	}
	defer encrypt.Zero(tek)

	dek = make([]byte, encrypt.KeySize)
	if _, err := rand.Read(dek); err != nil {
		return nil, nil, fmt.Errorf("keys: генерация DEK загрузки: %w", err)
	}
	wrapped, err = sealWithTEK(tek, dek, uploadAAD(uploadID))
	if err != nil {
		encrypt.Zero(dek)
		return nil, nil, fmt.Errorf("keys: обёртка DEK загрузки: %w", err)
	}
	return dek, wrapped, nil
}

// OpenUploadDEK — DEK загрузки по её обёртке.
func (v *Vault) OpenUploadDEK(ctx context.Context, tenant int64, wrapped []byte, uploadID string) ([]byte, error) {
	tek, err := v.tek(ctx, tenant)
	if err != nil {
		return nil, err
	}
	defer encrypt.Zero(tek)
	return sealWithTEKOpen(tek, wrapped, uploadAAD(uploadID))
}

// RewrapUploadDEKForVersion — переобёртка DEK загрузки под UUID версии.
//
// Данные частей при этом не перешифровываются: ключ тот же, меняется только
// обёртка. После Complete обёртка живёт в objects.wrapped_dek и привязана к
// версии, поэтому переставить её в другой объект нельзя.
func (v *Vault) RewrapUploadDEKForVersion(ctx context.Context, tenant int64,
	uploadWrapped []byte, uploadID string, version uuid.UUID) ([]byte, error) {
	tek, err := v.tek(ctx, tenant)
	if err != nil {
		return nil, err
	}
	defer encrypt.Zero(tek)

	dek, err := sealWithTEKOpen(tek, uploadWrapped, uploadAAD(uploadID))
	if err != nil {
		return nil, fmt.Errorf("keys: развернуть DEK загрузки: ключ тенанта не подошёл")
	}
	defer encrypt.Zero(dek)
	return sealWithTEK(tek, dek, dekAAD(version))
}
