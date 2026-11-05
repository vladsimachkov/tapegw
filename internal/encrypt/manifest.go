package encrypt

// Манифест объекта (ADR-0019).
//
// В конце файла объекта лежит зашифрованный манифест: метаданные объекта и
// таблица частей. Поверх — открытый футер фиксированного размера, в котором
// записаны длины и соль этой попытки шифрования.
//
// Зачем манифест вообще: восстановить объект с ленты, не имея нашей БД, можно
// только зная таблицу частей. Без соли nonce не вычислить, без открытого
// размера не проверить AAD. В атрибуты Phobos таблица не влезает — лимит LTFS
// на весь user_md 4096 байт, это 84 части против 10 000 допустимых (спайк
// M2-01).
//
// Зачем в манифесте метаданные: атрибуты Phobos лежат в открытом виде в индексе
// LTFS на каждой ленте. Оставь там бакет и ключ — и после крипто-уничтожения
// тенанта (ADR-0014) имена его объектов переживут сами данные.

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
)

const (
	// FooterSize — размер открытого футера. Фиксированный: искать манифест с
	// конца нужно, не зная его длины.
	FooterSize = 48

	// ManifestFormat — версия формата манифеста. Лежит в футере открыто и
	// входит в AAD, поэтому подмена даёт ошибку аутентичности, а не «разбор по
	// чужим правилам».
	ManifestFormat = 1
)

// manifestMagic — первые 4 байта футера.
var manifestMagic = [4]byte{'T', 'G', 'M', 'F'}

// manifestDomain — префикс nonce манифеста. Отдельный от домена сегментов
// (domain в format.go) намеренно: блок манифеста нельзя подставить вместо
// сегмента данных даже под одним DEK.
const manifestDomain = "tapegw-man-v1"

// Ошибки разбора футера и манифеста.
var (
	// ErrBadFooter — футер не похож на наш: неверный magic или неподдержанная
	// версия формата.
	ErrBadFooter = errors.New("encrypt: футер не распознан")

	// ErrManifestAuth — манифест не прошёл проверку аутентичности. Причина
	// любая: испорчен сам манифест или любая байта футера (футер входит в AAD).
	ErrManifestAuth = errors.New("encrypt: манифест не прошёл проверку аутентичности")
)

// ManifestPart — одна часть в манифесте. Имена полей короткие и совпадают с
// ADR-0019: манифест лежит в каждом объекте, раздувать его ключами JSON
// незачем.
type ManifestPart struct {
	Number uint32 `json:"n"`
	Plain  int64  `json:"plain"`
	Off    int64  `json:"off"`
	// Salt — соль ЭТОЙ попытки загрузки части, base64 (encoding/json кодирует
	// []byte именно так).
	Salt []byte `json:"salt"`
}

// ManifestLock — блокировка версии в манифесте. Нужна rebuild-meta: без неё
// восстановленный объект стал бы удаляемым, даже когда был заперт.
type ManifestLock struct {
	Mode        string `json:"mode,omitempty"`
	RetainUntil string `json:"retain_until,omitempty"`
	LegalHold   bool   `json:"legal_hold"`
}

// ManifestDoc — открытое содержимое манифеста.
//
// Здесь живёт всё, что раньше лежало в атрибутах Phobos в открытом виде: бакет,
// ключ, ETag, content-type, пользовательские метаданные, размер и блокировка.
// В атрибутах остаётся ровно то, без чего не развернуть ключ, — см.
// internal/archive.
type ManifestDoc struct {
	V           int               `json:"v"`
	Bucket      string            `json:"bucket"`
	Key         string            `json:"key"`
	ETag        string            `json:"etag"`
	Size        int64             `json:"size"`
	ContentType string            `json:"content_type,omitempty"`
	Meta        map[string]string `json:"meta,omitempty"`
	Lock        *ManifestLock     `json:"lock,omitempty"`
	Parts       []ManifestPart    `json:"parts"`
	// Created — когда версия была записана. Нужен исключительно восстановлению:
	// порядок версий одного ключа в живой базе задаёт version_seq, а его
	// назначает Postgres уже ПОСЛЕ записи файла (advisory-лок + max+1), так что
	// в момент сборки манифеста его просто нет. Без времени rebuild-meta не смог
	// бы ни упорядочить версии в ListObjectVersions, ни выбрать, какая из них
	// текущая, — а это единственное, что он обязан угадать правильно.
	//
	// Динамика с created_at строки отличается на микросекунды; порядок от этого
	// не меняется, и version_seq при восстановлении считается заново по этому
	// времени, а не переносится.
	Created time.Time `json:"created"`
}

// Footer — разобранный открытый футер.
type Footer struct {
	Format    uint8
	CipherLen uint64
	PlainLen  uint64
	Salt      []byte
	Reserved  [11]byte
	raw       [FooterSize]byte
	hasRaw    bool
}

// Bytes — 48 байт футера ровно в том виде, в каком они лежат на диске. Именно
// этот срез является AAD, поэтому наружу отдаётся байт-в-байт то, что было
// собрано до шифрования, а не пересобранное из полей после.
func (f *Footer) Bytes() [FooterSize]byte {
	if f.hasRaw {
		return f.raw
	}
	var b [FooterSize]byte
	copy(b[:4], manifestMagic[:])
	b[4] = f.Format
	binary.BigEndian.PutUint64(b[5:13], f.CipherLen)
	binary.BigEndian.PutUint64(b[13:21], f.PlainLen)
	copy(b[21:37], f.Salt)
	copy(b[37:48], f.Reserved[:])
	return b
}

// ParseFooter —разбор открытого футера. Проверяет magic, версию формата и то,
// что длины физически возможны.
func ParseFooter(b []byte) (Footer, error) {
	if len(b) != FooterSize {
		return Footer{}, fmt.Errorf("%w: %d байт, нужно %d", ErrBadFooter, len(b), FooterSize)
	}
	var f Footer
	f.raw = *(*[FooterSize]byte)(b)
	f.hasRaw = true
	if string(b[:4]) != string(manifestMagic[:]) {
		return Footer{}, fmt.Errorf("%w: magic %q", ErrBadFooter, b[:4])
	}
	f.Format = b[4]
	if f.Format != ManifestFormat {
		return Footer{}, fmt.Errorf("%w: формат %d, поддерживается %d", ErrBadFooter, f.Format, ManifestFormat)
	}
	f.CipherLen = binary.BigEndian.Uint64(b[5:13])
	f.PlainLen = binary.BigEndian.Uint64(b[13:21])
	f.Salt = append([]byte(nil), b[21:37]...)
	f.Reserved = *(*[11]byte)(b[37:48])
	// Шифротекст GCM длиннее открытого текста ровно на тег. Если длины
	// противоречат друг другу, файл испорчен, и лучше узнать сейчас.
	if f.CipherLen < TagSize || f.CipherLen-TagSize != f.PlainLen {
		return Footer{}, fmt.Errorf("%w: шифротекст %d, открытый %d", ErrBadFooter, f.CipherLen, f.PlainLen)
	}
	return f, nil
}

// manifestNonce — 12 байт GCM от UUID версии и соли этой попытки.
//
// Соль обязательна, и вот почему: UUID версии и DEK multipart-объекта
// назначаются ещё на CreateMultipartUpload. Если Complete записал файл, а
// транзакция не закоммитилась, клиент повторит Complete — S3 это разрешает, и
// список частей во второй попытке может быть другим. Детерминированный nonce
// дал бы два разных манифеста под одним DEK с одним nonce: XOR шифротекстов
// выдаёт XOR открытых текстов, а дальше восстанавливается ключ GHASH, чем
// подделывается любой манифест под этим DEK.
func manifestNonce(version uuid.UUID, salt []byte) []byte {
	var buf [len(manifestDomain) + 16 + SaltSize]byte
	n := copy(buf[:], manifestDomain)
	copy(buf[n:], version[:])
	if len(salt) != SaltSize {
		panic(fmt.Sprintf("encrypt: соль манифеста %d байт, нужно %d", len(salt), SaltSize))
	}
	copy(buf[n+16:], salt)
	sum := sha256.Sum256(buf[:])
	return sum[:12]
}

// SealManifest — шифротекст манифеста и готовый футер.
//
// Футер собирается ДО шифрования и целиком идёт в AAD: все его поля известны
// заранее (шифротекст длиннее открытого текста ровно на TagSize), поэтому
// цикличности нет. Привязка футера означает, что правка любой его байты —
// magic, формата, длин или соли — ломает тег.
func SealManifest(dst, key []byte, version uuid.UUID, doc *ManifestDoc) (cipherOut, footer []byte, err error) {
	if len(key) != KeySize {
		return nil, nil, fmt.Errorf("encrypt: ключ %d байт, нужно %d", len(key), KeySize)
	}
	if doc == nil {
		return nil, nil, fmt.Errorf("encrypt: манифест не задан")
	}
	if doc.V == 0 {
		doc.V = ManifestFormat
	}
	plain, err := json.Marshal(doc)
	if err != nil {
		return nil, nil, fmt.Errorf("encrypt: манифест: %w", err)
	}

	salt := make([]byte, SaltSize)
	if _, err := rand.Read(salt); err != nil {
		return nil, nil, fmt.Errorf("encrypt: соль манифеста: %w", err)
	}

	f := Footer{
		Format:    ManifestFormat,
		CipherLen: uint64(len(plain) + TagSize),
		PlainLen:  uint64(len(plain)),
		Salt:      salt,
	}
	raw := f.Bytes()

	gcm, err := newGCM(key)
	if err != nil {
		return nil, nil, err
	}
	out := gcm.Seal(dst[:0], manifestNonce(version, salt), plain, raw[:])
	if uint64(len(out)) != f.CipherLen {
		// Молча отдать шифротекст другой длины нельзя: футер уже обещал другое,
		// и чтение споткнётся далеко от причины.
		return nil, nil, fmt.Errorf("encrypt: манифест %d байт, футер обещает %d", len(out), f.CipherLen)
	}
	return out, raw[:], nil
}

// OpenManifest — открытое содержимое манифеста по шифротексту и футеру.
//
// AAD — переданный футер байт-в-байт. Именно поэтому проверка аутентичности
// распространяется и на футер: подделать манифест, не испортив тег, нельзя.
func OpenManifest(dst, key []byte, version uuid.UUID, cipherIn, footer []byte) (*ManifestDoc, error) {
	if len(key) != KeySize {
		return nil, fmt.Errorf("encrypt: ключ %d байт, нужно %d", len(key), KeySize)
	}
	f, err := ParseFooter(footer)
	if err != nil {
		return nil, err
	}
	if uint64(len(cipherIn)) != f.CipherLen {
		return nil, fmt.Errorf("%w: шифротекст %d байт, футер обещает %d",
			ErrManifestAuth, len(cipherIn), f.CipherLen)
	}
	gcm, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	plain, err := gcm.Open(dst[:0], manifestNonce(version, f.Salt), cipherIn, f.raw[:FooterSize])
	if err != nil {
		return nil, fmt.Errorf("%w: формат %d", ErrManifestAuth, f.Format)
	}
	if uint64(len(plain)) != f.PlainLen {
		return nil, fmt.Errorf("%w: открытый текст %d байт, футер обещает %d",
			ErrManifestAuth, len(plain), f.PlainLen)
	}
	var doc ManifestDoc
	if err := json.Unmarshal(plain, &doc); err != nil {
		return nil, fmt.Errorf("encrypt: манифест расшифрован, но не разобран: %w", err)
	}
	return &doc, nil
}

// ManifestTrailer — шифротекст манифеста и футер одним блоком: ровно то, что
// дописывается в конец файла объекта. Для спула это непрозрачные байты: формат
// он не разбирает, он просто дописывает хвост.
func ManifestTrailer(dst, key []byte, version uuid.UUID, doc *ManifestDoc) ([]byte, error) {
	ct, foot, err := SealManifest(dst, key, version, doc)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(ct)+FooterSize)
	out = append(out, ct...)
	return append(out, foot...), nil
}

// OpenManifestAt — развернуть манифест из файла заданного размера.
//
// Читатель принимает io.ReaderAt, а не os.File: тесты подсовывают bytes.Reader,
// а в бою это и файл спула, и объект с ленты.
func OpenManifestAt(at io.ReaderAt, fileSize int64, key []byte, version uuid.UUID) (*ManifestDoc, error) {
	if fileSize < FooterSize {
		return nil, fmt.Errorf("%w: файл %d байт, футер %d", ErrBadFooter, fileSize, FooterSize)
	}
	var foot [FooterSize]byte
	if _, err := at.ReadAt(foot[:], fileSize-FooterSize); err != nil {
		return nil, fmt.Errorf("encrypt: прочитать футер: %w", err)
	}
	f, err := ParseFooter(foot[:])
	if err != nil {
		return nil, err
	}
	start := fileSize - FooterSize - int64(f.CipherLen)
	if start < 0 {
		return nil, fmt.Errorf("%w: шифротекст манифеста не влезает в файл", ErrBadFooter)
	}
	ct := make([]byte, f.CipherLen)
	if _, err := at.ReadAt(ct, start); err != nil {
		return nil, fmt.Errorf("encrypt: прочитать манифест: %w", err)
	}
	return OpenManifest(nil, key, version, ct, foot[:])
}
