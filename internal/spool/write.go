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
	// DiskBytes — сколько байт реально легло на диск: шифротекст плюс хвост
	// манифеста. Size при этом остаётся размером открытых данных (его видит
	// клиент), а DiskBytes — длина файла, которую потом объявит загрузчик
	// пачки: участник — файл целиком (ADR-0020), а манифест переменной длины
	// и формулой из Size не выводится.
	DiskBytes int64
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
	return s.write(id, id, r, spec, limit, nil, 0, 0, nil, nil)
}

// WriteEncrypted — то же, что Write, но на диск ложится шифротекст (ADR-0014).
//
// Шифрование стоит ПОСЛЕ всех сверителей, а не до них: ETag и x-amz-checksum-*
// считаются по открытым данным, то есть по тому, что видит клиент. Если
// поставить его в начало цепочки, ETag станет суммой шифротекста, и клиент
// перестанет сверять загрузки — молча, потому что сумма «сошлась бы» сама с
// собой.
//
// version — UUID версии объекта (он же id файла спула), part = 1: обычный PUT —
// это одна часть (см. meta.PutObject), номер входит в nonce и должен совпасть с
// номером из object_parts.
// plainSize — объявленный размер открытого тела. Он обязателен: входит в AAD
// каждого сегмента (ADR-0014), поэтому шифровать можно только зная длину
// заранее. Факт несовпадения с фактическими байтами — ErrSizeMismatch.
//
// trailerFor — хвост манифеста (encrypt.ManifestTrailer): вызывается после того,
// как открытые данные записаны и суммы посчитаны, но до fsync и rename. Отложен
// он именно затем, что ETag обычного PUT — MD5 от тела, а второго прохода по
// телу не будет: манифест обязан влезть в тот же проход.
//
// Размер в ObjectInfo при этом остаётся размером открытых данных — манифест
// клиенту не виден.
func (s *Spool) WriteEncrypted(id uuid.UUID, r io.Reader, spec SumSpec, limit int64, key []byte, plainSize int64, salt []byte,
	trailerFor func(ObjectInfo) ([]byte, error)) (ObjectInfo, error) {
	return s.write(id, id, r, spec, limit, key, plainSize, 1, salt, trailerFor)
}

// WritePartEncrypted — та же запись для одной части multipart: part — номер
// части, он входит в nonce и AAD каждого сегмента (ADR-0014).
//
// Файл части — обычный файл спула, ничем не отличается от файла объекта: при
// Complete они просто склеиваются.
// Версией здесь обязан быть UUID БУДУЩЕГО объекта, а не файла части: nonce и
// AAD каждого сегмента строятся от UUID версии, а читать собранный файл будут
// под UUID строки objects. Файл части при этом живёт под собственным именем —
// два разных UUID, и путать их нельзя.
func (s *Spool) WritePartEncrypted(id, version uuid.UUID, r io.Reader, spec SumSpec, limit int64,
	key []byte, plainSize int64, part uint32, salt []byte) (ObjectInfo, error) {
	// Хвоста здесь нет намеренно: файл части склеивается с другими, а манифест
	// бывает один на весь объект и дописывается после последней части.
	return s.write(id, version, r, spec, limit, key, plainSize, part, salt, nil)
}

func (s *Spool) write(id, version uuid.UUID, r io.Reader, spec SumSpec, limit int64, encKey []byte, plainSize int64, part uint32, salt []byte,
	trailerFor func(ObjectInfo) ([]byte, error)) (ObjectInfo, error) {
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
		enc, err = encrypt.NewWriter(tmp, encKey, version, part, salt, plainSize)
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
	// Порядок проверок: сначала лимит. Если тело больше допустимого, ответ
	// EntityTooLarge полезнее, чем «длина не совпала с объявленной»: клиенту
	// важно, что объект слишком большой, а не то, что он ещё и наврал про длину.
	if limit > 0 && n > limit {
		tmp.Close()
		return info, ErrTooLarge
	}
	if enc != nil && n != plainSize {
		// Тело не совпало с объявленной длиной. Молча записать то, что пришло,
		// нельзя: AAD сегментов завернут под plainSize, и такой файл потом всё
		// равно не расшифруется, а ошибка через месяц чтения хуже отказа сейчас.
		tmp.Close()
		return info, fmt.Errorf("%w: объявлено %d, записано %d", ErrSizeMismatch, plainSize, n)
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

	// Хвост манифеста дописывается в последнюю очередь и уже в сам файл:
	// шифрование данных к этому моменту закрыто (enc.Close), суммы посчитаны,
	// а fsync ещё впереди — весь объект уходит на диск одним проходом.
	if trailerFor != nil {
		tr, err := trailerFor(info)
		if err != nil {
			tmp.Close()
			return info, err
		}
		if len(tr) > 0 {
			if _, err := tmp.Write(tr); err != nil {
				tmp.Close()
				return info, fmt.Errorf("spool: хвост манифеста: %w", err)
			}
		}
	}

	// Длина файла на диске — после хвоста манифеста и до fsync: это ровно то,
	// что увидит читатель, и ровно то, что пойдёт в objects.file_bytes.
	// Берём из stat, а не суммой «шифротекст + длина хвоста»: пусть источником
	// будет файловая система, а не наша арифметика.
	st, err := tmp.Stat()
	if err != nil {
		tmp.Close()
		return info, fmt.Errorf("spool: размер файла: %w", err)
	}
	info.DiskBytes = st.Size()

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

// Concatenate собирает файл объекта из файлов частей (CompleteMultipartUpload).
//
// Шифротексты частей просто склеиваются: все части зашифрованы одним DEK, а
// различаются номером части в nonce и AAD, поэтому перешифрования не нужно —
// ровно это и обещает ADR-0014 («итоговый файл собирается без перешифрования»).
//
// Порядок жёсткий, как и в Write: склейка → fsync → rename → fsync каталога, и
// только после возврата разрешена транзакция Postgres (правило 6).
//
// Копирование идёт через io.Copy: между двумя os.File ядро использует
// copy_file_range, а на reflink-файловой системе это почти clone. Posix-бэкенд
// versitygw делает так же (posix.go:2975-2990), изобретать своё нет причины.
//
// trailer — хвост манифеста (encrypt.ManifestTrailer): дописывается после
// последней части, поэтому Complete остаётся конкатенацией и перешифрования
// частей не требует.
//
// Возвращает два размера: суммарный размер шифротекстов частей БЕЗ хвоста — на
// этой цифре вызывающий сверяет смещения частей, примешивать к ней манифест
// нельзя, — и итоговую длину файла на диске (со склеенным хвостом), которую
// вызывающий пишет в objects.file_bytes как объявленную длину участника пачки.
func (s *Spool) Concatenate(dst uuid.UUID, srcs []uuid.UUID, trailer []byte) (cipherTotal, fileBytes int64, err error) {
	tmp, err := os.CreateTemp(s.tmp, "cat-*")
	if err != nil {
		return 0, 0, fmt.Errorf("spool: временный файл склейки: %w", err)
	}
	tmpName := tmp.Name()
	defer func() {
		if tmpName != "" {
			os.Remove(tmpName)
		}
	}()

	var total int64
	for _, src := range srcs {
		f, err := s.Open(src)
		if err != nil {
			tmp.Close()
			return total, 0, fmt.Errorf("spool: часть %s: %w", src, err)
		}
		n, err := io.Copy(tmp, f)
		f.Close()
		if err != nil {
			tmp.Close()
			return total, 0, fmt.Errorf("spool: склейка %s: %w", src, err)
		}
		total += n
	}

	if len(trailer) > 0 {
		if _, err := tmp.Write(trailer); err != nil {
			tmp.Close()
			return total, 0, fmt.Errorf("spool: хвост манифеста: %w", err)
		}
	}

	// Длина готового файла — из stat, как в write(): источник истины — файловая
	// система, а не наша арифметика по частям и хвосту.
	st, err := tmp.Stat()
	if err != nil {
		tmp.Close()
		return total, 0, fmt.Errorf("spool: размер склейки: %w", err)
	}
	fileBytes = st.Size()

	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return total, 0, fmt.Errorf("spool: fsync склейки: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return total, 0, fmt.Errorf("spool: закрыть склейку: %w", err)
	}
	final := s.Path(dst)
	if err := os.Rename(tmpName, final); err != nil {
		return total, 0, fmt.Errorf("spool: rename склейки на %s: %w", final, err)
	}
	tmpName = ""
	if err := fsyncDir(filepath.Dir(final)); err != nil {
		return total, 0, err
	}
	return total, fileBytes, nil
}
