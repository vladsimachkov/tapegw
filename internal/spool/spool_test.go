package spool

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/versity/versitygw/s3api/utils"
)

func newSpool(t *testing.T) *Spool {
	t.Helper()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return s
}

func TestWriteRoundTrip(t *testing.T) {
	s := newSpool(t)
	id := uuid.New()
	body := []byte(strings.Repeat("tapegw", 1000))

	info, err := s.Write(id, bytes.NewReader(body), SumSpec{}, 0)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	sum := md5.Sum(body)
	if want := hex.EncodeToString(sum[:]); info.MD5Hex != want {
		t.Errorf("MD5Hex = %s, хотим %s", info.MD5Hex, want)
	}
	if want := utils.Base64SumString(sum[:]); info.MD5B64 != want {
		t.Errorf("MD5B64 = %s, хотим %s", info.MD5B64, want)
	}
	if info.Size != int64(len(body)) {
		t.Errorf("Size = %d, хотим %d", info.Size, len(body))
	}

	// Файл обязан лежать ровно там, где его найдёт mover по UUID.
	got, err := os.ReadFile(s.Path(id))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("содержимое файла разошлось с телом запроса")
	}

	// tmp/ обязан остаться пустым: временный файл переживает только саму запись.
	left, _ := filepath.Glob(filepath.Join(s.Root(), "tmp", "*"))
	if len(left) != 0 {
		t.Errorf("в tmp/ остались файлы: %v", left)
	}
}

func TestWriteRejectsBadContentMD5(t *testing.T) {
	s := newSpool(t)
	id := uuid.New()

	// Сумма от другого содержимого — классическая гонка «клиент посчитал не то».
	other := md5.Sum([]byte("совсем другие данные"))
	badMD5 := utils.Base64SumString(other[:])

	_, err := s.Write(id, bytes.NewReader([]byte("настоящие данные")),
		SumSpec{ContentMD5: badMD5}, 0)
	if err == nil {
		t.Fatalf("ожидали ошибку несовпадения Content-MD5")
	}
	// Файл не должен остаться ни на месте, ни во временном каталоге.
	if s.Exists(id) {
		t.Errorf("файл остался после неудачной записи")
	}
	left, _ := filepath.Glob(filepath.Join(s.Root(), "tmp", "*"))
	if len(left) != 0 {
		t.Errorf("в tmp/ остался мусор: %v", left)
	}
}

func TestWriteRejectsBadChecksum(t *testing.T) {
	s := newSpool(t)
	id := uuid.New()

	// Правильная сумма для CRC32 считается тем же хелпером versitygw.
	good, err := utils.NewHashReader(bytes.NewReader([]byte("x")), "", utils.HashTypeCRC32)
	if err != nil {
		t.Fatalf("NewHashReader: %v", err)
	}
	if _, err := io.ReadAll(good); err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if good.Sum() == "" {
		t.Skip("crc32 не поддерживается в этой сборке")
	}

	_, err = s.Write(id, bytes.NewReader([]byte("настоящие данные")),
		SumSpec{Algorithm: types.ChecksumAlgorithmCrc32, Expected: good.Sum()}, 0)
	if err == nil {
		t.Fatalf("ожидали ошибку несовпадения x-amz-checksum-crc32")
	}
	if s.Exists(id) {
		t.Errorf("файл остался после неудачной записи")
	}
}

func TestWriteAcceptsGoodChecksum(t *testing.T) {
	s := newSpool(t)
	id := uuid.New()
	body := []byte("настоящие данные")

	// Сначала получаем корректную сумму тем же алгоритмом.
	hr, err := utils.NewHashReader(bytes.NewReader(body), "", utils.HashTypeCRC32)
	if err != nil {
		t.Fatalf("NewHashReader: %v", err)
	}
	if _, err := io.ReadAll(hr); err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	info, err := s.Write(id, bytes.NewReader(body),
		SumSpec{Algorithm: types.ChecksumAlgorithmCrc32, Expected: hr.Sum()}, 0)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if info.Checksum != hr.Sum() || info.Algorithm != types.ChecksumAlgorithmCrc32 {
		t.Errorf("checksum не вернулся: %+v", info)
	}
}

func TestWriteEnforcesLimit(t *testing.T) {
	s := newSpool(t)
	id := uuid.New()

	_, err := s.Write(id, bytes.NewReader([]byte("1234567890")), SumSpec{}, 4)
	if err != ErrTooLarge {
		t.Fatalf("ожидали ErrTooLarge, получили %v", err)
	}
	if s.Exists(id) {
		t.Errorf("файл остался после EntityTooLarge")
	}

	// Ровно на границе — уже законно: лимит включительно.
	if _, err := s.Write(id, bytes.NewReader([]byte("1234")), SumSpec{}, 4); err != nil {
		t.Errorf("размер ровно по лимиту должен проходить: %v", err)
	}
}

func TestPathIsDeterministicAndSharded(t *testing.T) {
	s := newSpool(t)
	id := uuid.MustParse("1b4e28ba-2fa1-11d2-883f-0016d3cca427")

	p := s.Path(id)
	if p != s.Path(id) {
		t.Errorf("Path не детерминирована")
	}
	// Первые два символа UUID — имя подкаталога: без шардинга janitor
	// обходил бы каталог с сотнями тысяч файлов.
	if filepath.Base(filepath.Dir(p)) != "1b" {
		t.Errorf("нет шардинга по префиксу: %s", p)
	}
	if filepath.Base(p) != id.String() {
		t.Errorf("имя файла не равно UUID: %s", filepath.Base(p))
	}
}

func TestRemoveIsIdempotent(t *testing.T) {
	s := newSpool(t)
	id := uuid.New()

	// Повторное удаление и удаление несуществующего — не ошибка: janitor
	// и вытеснение вызываются повторно.
	if err := s.Remove(id); err != nil {
		t.Fatalf("Remove несуществующего: %v", err)
	}
	if _, err := s.Write(id, bytes.NewReader([]byte("x")), SumSpec{}, 0); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !s.Exists(id) {
		t.Fatalf("после записи файл должен существовать")
	}
	if err := s.Remove(id); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if s.Exists(id) {
		t.Errorf("файл остался после Remove")
	}
	if err := s.Remove(id); err != nil {
		t.Errorf("повторный Remove: %v", err)
	}
}

func TestOpenMissingIsNotSilent(t *testing.T) {
	s := newSpool(t)
	if _, err := s.Open(uuid.New()); err == nil {
		t.Errorf("ожидали ошибку при открытии несуществующего объекта")
	}
}

func TestOpenRejectsEmptyRoot(t *testing.T) {
	if _, err := Open(""); err == nil {
		t.Errorf("ожидали ошибку на пустом корне")
	}
}

// Смысл теста в том, что после успешного Write файл уже на диске и целый.
// Именно это даёт правилу 6 смысл: commit Postgres идёт позже и может упасть,
// а прочитан сирота не будет, потому что читают по строке БД.
func TestWrittenFileIsCompleteBeforeReturn(t *testing.T) {
	s := newSpool(t)
	id := uuid.New()
	body := make([]byte, 4<<20)
	for i := range body {
		body[i] = byte(i)
	}

	if _, err := s.Write(id, bytes.NewReader(body), SumSpec{}, 0); err != nil {
		t.Fatalf("Write: %v", err)
	}

	f, err := s.Open(id)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if st.Size() != int64(len(body)) {
		t.Errorf("размер на диске %d, ожидали %d", st.Size(), len(body))
	}
	got, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("содержимое на диске разошлось")
	}
}
