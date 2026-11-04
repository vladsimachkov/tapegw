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

	"github.com/vladsimachkov/tapegw/internal/encrypt"
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
	// Algorithm — x-amz-checksum-algorithm из запроса. Пусто — не считаем.
	Algorithm types.ChecksumAlgorithm
	// Expected — значение x-amz-checksum-<algorithm> из заголовка. Пустое
	// допустимо и означает «не сверяй, просто посчитай»: так приходит современный
	// SDK, который объявляет алгоритм в x-amz-sdk-checksum-algorithm, а значение
	// кладёт в трейлер aws-chunked.
	Expected string
	// SkipAlgorithmValidation — не оборачивать тело сверителем Algorithm.
	// Ставится для trailing-сумм: их уже проверил чанк-ридер versitygw, и
	// валидировать второй раз бессмысленно. Сумму при этом всё равно считаем
	// сами — она нужна в ответе и в метаданных.
	SkipAlgorithmValidation bool
}

// ObjectInfo — итог записи: что легло на диск и какие суммы получились.
type ObjectInfo struct {
	Size int64
	// MD5Hex — hex-представление MD5, оно же ETag для обычной записи.
	MD5Hex string
	// MD5B64 — base64 того же MD5: его кладём в заголовок Content-MD5 ответа.
	MD5B64 string
	// Algorithm и Checksum — эхо запрошенного клиентом checksum. Значение
	// ВСЕГДА вычислено здесь, а не взято из заголовка: заголовок может
	// отсутствовать (trailing-сумма), а вернуть клиенту надо то, что реально
	// посчитано по байтам.
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
	return s.write(id, r, spec, limit, nil)
}

// WriteEncrypted — то же, что Write, но на диск ложится шифротекст (ADR-0014).
//
// Шифрование стоит ПОСЛЕ всех сверителей, а не до них: ETag и x-amz-checksum-*
// считаются по открытым данным, то есть по тому, что видит клиент. Если
// поставить его в начало цепочки, ETag станет суммой шифротекста, и клиент
// перестанет сверять загрузки — молча, потому что сумма «сошлась бы» сама с
// собой.
//
// version — UUID версии объекта (он же id файла спула), part = 0: у обычного PUT
// частей нет.
func (s *Spool) WriteEncrypted(id uuid.UUID, r io.Reader, spec SumSpec, limit int64, key []byte) (ObjectInfo, error) {
	return s.write(id, r, spec, limit, key)
}

func (s *Spool) write(id uuid.UUID, r io.Reader, spec SumSpec, limit int64, encKey []byte) (ObjectInfo, error) {
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

	// Ссылка на сверитель алгоритма, чтобы после чтения забрать вычисленное
	// значение: оно нужно в ответе даже когда клиент значения не присылал.
	var algoReader *utils.HashReader

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
		expected := spec.Expected
		if spec.SkipAlgorithmValidation {
			// Трейлер проверил чанк-ридер; нам остаётся только посчитать.
			expected = ""
		}
		hr, err := utils.NewHashReader(src, expected, ht)
		if err != nil {
			return info, fmt.Errorf("spool: checksum %q: %w", spec.Algorithm, err)
		}
		src = hr
		algoReader = hr
	}

	final := s.Path(id)
	// Каталог-шард здесь не создаём: все 256 созданы при Open и корень
	// fsync-нут один раз. Ленивое создание оставило бы запись о каталоге
	// в корне без fsync — см. Spool.initShards.

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

	// dst — файл или поток шифрования поверх файла. src при этом остаётся
	// цепочкой над открытыми данными, и все суммы считаются по ней.
	var dst io.Writer = tmp
	var enc *encrypt.Writer
	if encKey != nil {
		enc, err = encrypt.NewWriter(tmp, encKey, id, 0)
		if err != nil {
			tmp.Close()
			return info, err
		}
		dst = enc
	}

	n, err := io.Copy(dst, src)
	if err != nil {
		tmp.Close()
		return info, err
	}
	if enc != nil {
		// Последний сегмент дописывается только в Close. Без него файл был бы
		// короче, чем обещает info.Size, и чтение упало бы на ErrShortRead.
		if cerr := enc.Close(); cerr != nil {
			tmp.Close()
			return info, cerr
		}
		// Размер для клиента — байты открытого текста, а не шифротекста:
		// Content-Length, ETag и колонка size должны описывать то, что клиент
		// и загрузил.
		n = enc.Wrote()
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
		// Берём вычисленное значение, а не эхо заголовка: при trailing-сумме
		// заголовка нет вовсе, а вернуть клиенту надо то, что посчитано по
		// фактическим байтам.
		if algoReader != nil {
			info.Checksum = algoReader.Sum()
		}
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
