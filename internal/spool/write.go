package spool

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/versity/versitygw/s3api/utils"
)

// SumSpec — что считаем и что сверяем при записи.
//
// Сверка делается на лету, во время записи: тело запроса читается один раз,
// второго прохода по ленте-буферу не будет. Алгоритм взят из versitygw
// (s3api/utils.HashReader), а не написан заново, — вместе с ним бесплатно
// приезжают и правильные коды ошибок (BadDigest / InvalidRequestChecksum),
// которые контроллер versitygw умеет превращать в ответы S3. См. правило 5.
type SumSpec struct {
	// ContentMD5 — значение заголовка Content-MD5 (base64). Пусто — не сверяем.
	ContentMD5 string
	// Algorithm — x-amz-checksum-algorithm из запроса. Пусто — не сверяем.
	Algorithm types.ChecksumAlgorithm
	// Expected — значение x-amz-checksum-<algorithm> из запроса.
	Expected string
}

// ObjectInfo — итог записи: что легло на диск и какие суммы получились.
type ObjectInfo struct {
	Size int64
	// MD5Hex — hex-представление MD5, оно же ETag для обычной записи.
	MD5Hex string
	// MD5B64 — base64 того же MD5: его кладём в заголовок Content-MD5 ответа.
	MD5B64 string
	// Algorithm и Checksum — эхо запрошенного клиентом checksum. Пустой Algorithm
	// означает, что контрольный суммы клиент не присылал.
	Algorithm types.ChecksumAlgorithm
	Checksum  string
}

// Write атомарно записывает тело объекта и возвращает его размер и суммы.
//
// Порядок жёсткий и продиктован правилом 6 из AGENTS.md:
//
//	запись во временный файл → fsync файла → rename на место → fsync каталога
//
// Только после возврата из Write разрешено открывать транзакцию Postgres. Если
// процесс упадёт между rename и commit, на диске останется файл без строки в
// БД — сирота. Он безвреден: читается объект по строке БД, значит прочитан не
// будет, а janitor подчистит. Обратный порядок (сначала commit) был бы хуже:
// строка без файла дала бы 500 на каждом GET.
//
// limit > 0 ограничивает размер; превышение — ErrTooLarge, файл не остаётся.
func (s *Spool) Write(id uuid.UUID, r io.Reader, spec SumSpec, limit int64) (ObjectInfo, error) {
	var info ObjectInfo

	// Ограничитель ставим ниже сверителей, чтобы HashReader увидел EOF и успел
	// проверить сумму: если обрезать поток сверху, проверка не состоится.
	var src io.Reader = r
	if limit > 0 {
		src = io.LimitReader(r, limit+1)
	}

	// MD5 нужен всегда: он же ETag. Считаем его отдельно от проверок клиента,
	// чтобы не зависеть от того, прислал ли клиент контрольную сумму.
	md5h := md5.New()
	src = io.TeeReader(src, md5h)

	if spec.ContentMD5 != "" {
		hr, err := utils.NewHashReader(src, spec.ContentMD5, utils.HashTypeContentMD5)
		if err != nil {
			return info, fmt.Errorf("spool: content-md5: %w", err)
		}
		src = hr
	}
	if spec.Algorithm != "" {
		ht, ok := hashTypeFor(spec.Algorithm)
		if !ok {
			return info, fmt.Errorf("spool: неизвестный checksum-алгоритм %q", spec.Algorithm)
		}
		hr, err := utils.NewHashReader(src, spec.Expected, ht)
		if err != nil {
			return info, fmt.Errorf("spool: checksum %q: %w", spec.Algorithm, err)
		}
		src = hr
	}

	final := s.Path(id)
	if err := os.MkdirAll(filepath.Dir(final), 0o750); err != nil {
		return info, fmt.Errorf("spool: каталог под объект: %w", err)
	}

	tmp, err := os.CreateTemp(s.tmp, "obj-*")
	if err != nil {
		return info, fmt.Errorf("spool: временный файл: %w", err)
	}
	tmpName := tmp.Name()
	// Любой выход до rename обязан убрать временный файл, иначе tmp/ разрастётся
	// после каждой ошибки клиента.
	defer func() {
		if tmpName != "" {
			os.Remove(tmpName)
		}
	}()

	n, err := io.Copy(tmp, src)
	if err != nil {
		tmp.Close()
		return info, err
	}
	if limit > 0 && n > limit {
		tmp.Close()
		return info, ErrTooLarge
	}

	sum := md5h.Sum(nil)
	info = ObjectInfo{
		Size:   n,
		MD5Hex: hex.EncodeToString(sum),
		MD5B64: utils.Base64SumString(sum),
	}
	if spec.Algorithm != "" {
		info.Algorithm = spec.Algorithm
		// Значение уже проверено HashReader-ом при чтении; берём его из spec,
		// потому что для составных (multipart) сумм оно может прийти заранее.
		info.Checksum = spec.Expected
	}

	// fsync файла до rename: иначе после падения машины в каталоге может
	// оказаться запись о файле с нулевым содержимым.
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return info, fmt.Errorf("spool: fsync файла: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return info, fmt.Errorf("spool: закрыть временный файл: %w", err)
	}

	if err := os.Rename(tmpName, final); err != nil {
		return info, fmt.Errorf("spool: rename на %s: %w", final, err)
	}
	// rename состоялся: временного файла больше нет, убирать нечего.
	tmpName = ""

	if err := fsyncDir(filepath.Dir(final)); err != nil {
		return info, err
	}
	return info, nil
}

// hashTypeFor переводит алгоритм из заголовка в тип versitygw.
func hashTypeFor(a types.ChecksumAlgorithm) (utils.HashType, bool) {
	switch a {
	case types.ChecksumAlgorithmMd5:
		return utils.HashTypeMd5, true
	case types.ChecksumAlgorithmCrc32:
		return utils.HashTypeCRC32, true
	case types.ChecksumAlgorithmCrc32c:
		return utils.HashTypeCRC32C, true
	case types.ChecksumAlgorithmSha1:
		return utils.HashTypeSha1, true
	case types.ChecksumAlgorithmSha256:
		return utils.HashTypeSha256, true
	case types.ChecksumAlgorithmSha512:
		return utils.HashTypeSha512, true
	case types.ChecksumAlgorithmCrc64nvme:
		return utils.HashTypeCRC64NVME, true
	case types.ChecksumAlgorithmXxhash64:
		return utils.HashTypeXXHASH64, true
	case types.ChecksumAlgorithmXxhash3:
		return utils.HashTypeXXHASH3, true
	case types.ChecksumAlgorithmXxhash128:
		return utils.HashTypeXXHASH128, true
	default:
		return "", false
	}
}
