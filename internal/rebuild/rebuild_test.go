package rebuild_test

// Тесты восстановления метаданных из архива (ADR-0019).
//
// Сценарий, ради которого всё затевалось: база потеряна, остались ленты, KEK и
// экспорт ключей. Поэтому список объектов в этих тестах берётся из фейкового
// архива, а из базы не читается ничего: если rebuild возьмёт что-то из objects,
// тест перестанет доказывать восстановление и начнёт доказывать чтение БД.
//
// Проверка содержимого идёт не через GetObject (данных в спуле после аварии
// нет), а напрямую: таблица частей из восстановленной строки + DEK + байты
// архива. Так проверяется ровно то, что rebuild обязан угадать правильно, —
// размеры, смещения и соли частей.

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"strconv"
	"testing"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/archive"
	"github.com/vladsimachkov/tapegw/internal/backend"
	"github.com/vladsimachkov/tapegw/internal/config"
	"github.com/vladsimachkov/tapegw/internal/encrypt"
	"github.com/vladsimachkov/tapegw/internal/iam"
	"github.com/vladsimachkov/tapegw/internal/keys"
	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/rebuild"
	"github.com/vladsimachkov/tapegw/internal/spool"
	"github.com/vladsimachkov/tapegw/internal/testdb"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/versity/versitygw/auth"
	s3response "github.com/versity/versitygw/s3response"
)

const (
	bucketName = "b"
	tenantID   = int64(1)
)

var kek = func() []byte {
	b := make([]byte, 32)
	for i := range b {
		b[i] = byte(i * 7)
	}
	return b
}()

type harness struct {
	be   *backend.TapeGW
	m    *meta.Meta
	land *spool.Spool
	ring *iam.KeyRing
	arch *archive.Fake
	ctx  context.Context
}

func setup(t *testing.T) *harness {
	t.Helper()
	m := testdb.Connect(t)
	testdb.Clean(t, m)

	ring, err := iam.NewKeyRing(1, map[int][]byte{1: kek})
	if err != nil {
		t.Fatal(err)
	}
	land, err := spool.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	be := backend.New(m, land, config.Default(), keys.New(m, ring))
	h := &harness{be: be, m: m, land: land, ring: ring,
		arch: archive.NewFake(t.TempDir()),
		ctx:  accountCtx("tester")}

	if err := be.CreateBucket(h.ctx, &s3.CreateBucketInput{
		Bucket: ptr(bucketName),
	}, nil); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	return h
}

func accountCtx(access string) context.Context {
	//lint:ignore SA1029 ключ диктует versitygw
	return context.WithValue(context.Background(), "account",
		auth.Account{Access: access, Role: auth.RoleUser, ProjectID: int(tenantID)})
}

func ptr[T any](v T) *T { return &v }

func filler(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*31 + 7)
	}
	return b
}

// putOne — обычный PUT, возвращает открытое содержимое.
func (h *harness) putOne(t *testing.T, key string, size int) []byte {
	t.Helper()
	data := filler(size)
	if _, err := h.be.PutObject(h.ctx, s3response.PutObjectInput{
		Bucket: ptr(bucketName), Key: ptr(key), Body: bytes.NewReader(data),
		ContentLength: ptr(int64(size)),
	}); err != nil {
		t.Fatalf("PutObject %s: %v", key, err)
	}
	return data
}

// putMultipart — multipart с ПОВТОРНОЙ загрузкой части №2: сначала неверные
// байты той же длины, потом верные. Именно переотправка части — тот случай,
// который ломает восстановление, если соль берётся не из манифеста.
func (h *harness) putMultipart(t *testing.T, key string, sizes []int) []byte {
	t.Helper()
	up, err := h.be.CreateMultipartUpload(h.ctx, s3response.CreateMultipartUploadInput{
		Bucket: ptr(bucketName), Key: ptr(key),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	id := up.UploadId

	var full []byte
	var parts []types.CompletedPart
	for i, n := range sizes {
		num := int32(i + 1)
		chunk := filler(n * 3) // заведомо не то содержимое
		if _, err := h.be.UploadPart(h.ctx, &s3.UploadPartInput{
			Bucket: ptr(bucketName), Key: ptr(key), UploadId: ptr(id),
			PartNumber: &num, Body: bytes.NewReader(chunk),
			ContentLength: ptr(int64(len(chunk))),
		}); err != nil {
			t.Fatalf("первая попытка части %d: %v", num, err)
		}
		chunk = filler(n)
		out, err := h.be.UploadPart(h.ctx, &s3.UploadPartInput{
			Bucket: ptr(bucketName), Key: ptr(key), UploadId: ptr(id),
			PartNumber: &num, Body: bytes.NewReader(chunk),
			ContentLength: ptr(int64(len(chunk))),
		})
		if err != nil {
			t.Fatalf("вторая попытка части %d: %v", num, err)
		}
		full = append(full, chunk...)
		parts = append(parts, types.CompletedPart{PartNumber: ptr(num), ETag: out.ETag})
	}

	if _, _, err := h.be.CompleteMultipartUpload(h.ctx, &s3.CompleteMultipartUploadInput{
		Bucket: ptr(bucketName), Key: ptr(key), UploadId: ptr(id),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	}); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	return full
}

// snapshot — перенести записанное в фейковый архив так, как это сделал бы mover:
// байты файла из спула плюс пять атрибутов (ADR-0019).
func (h *harness) snapshot(t *testing.T) map[string][]byte {
	t.Helper()
	rows, err := h.m.Pool.Query(h.ctx,
		`SELECT o.id, o.tenant_id, o.wrapped_dek, t.kek_version
		  FROM objects o JOIN tenants t ON t.id = o.tenant_id
		  WHERE NOT o.is_delete_marker ORDER BY o.id`)
	if err != nil {
		t.Fatal(err)
	}
	type row struct {
		id  uuid.UUID
		ten int64
		dek []byte
		kek int
	}
	var all []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.ten, &r.dek, &r.kek); err != nil {
			t.Fatal(err)
		}
		all = append(all, r)
	}
	rows.Close()

	plain := map[string][]byte{}
	for _, r := range all {
		raw, err := os.ReadFile(h.land.Path(r.id))
		if err != nil {
			t.Fatalf("файл %s: %v", r.id, err)
		}
		attrs, err := archive.Attributes(archive.Version{
			Version: r.id, Tenant: r.ten, WrappedDEK: r.dek,
			KEK: strconv.Itoa(r.kek), Format: archive.FormatVersion,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := h.arch.Put(r.id.String(), raw, attrs); err != nil {
			t.Fatal(err)
		}
		// Открытый текст «до аварии» — то, с чем сравниваем после.
		plain[r.id.String()] = h.readPlain(t, r.id, r.ten, r.dek, raw)
	}
	return plain
}

// readPlain — расшифровать байты архива по таблице частей из БД.
func (h *harness) readPlain(t *testing.T, id uuid.UUID, tenant int64, wrapped, raw []byte) []byte {
	t.Helper()
	vault := keys.New(h.m, h.ring)
	dek, err := vault.OpenVersionDEK(h.ctx, tenant, wrapped, id)
	if err != nil {
		t.Fatal(err)
	}
	defer encrypt.Zero(dek)
	ps, err := h.m.ObjectParts(h.ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	var size int64
	if err := h.m.Pool.QueryRow(h.ctx, `SELECT size FROM objects WHERE id = $1`, id).Scan(&size); err != nil {
		t.Fatal(err)
	}
	var enc []encrypt.Part
	for _, p := range ps {
		enc = append(enc, encrypt.Part{Number: uint32(p.Number), PlainSize: p.PlainSize,
			CipherOffset: p.CipherOffset, Salt: p.Salt})
	}
	r, err := encrypt.NewPartsReader(bytes.NewReader(raw), dek, id, enc, size)
	if err != nil {
		t.Fatalf("NewPartsReader id=%s size=%d частей=%d байт=%d: %v", id, size, len(enc), len(raw), err)
	}
	buf := make([]byte, size)
	// io.EOF здесь — норма: читатель дошёл до конца открытого текста и вернул
	// ровно size байт вместе с признаком конца.
	if n, err := r.ReadAt(buf, 0); err != nil && err != io.EOF {
		t.Fatalf("ReadAt id=%s size=%d частей=%d байт=%d: %v", id, size, len(enc), len(raw), err)
	} else if int64(n) != size {
		t.Fatalf("прочитано %d из %d (id=%s)", n, size, id)
	}
	return buf
}

// wipe — имитация потери базы: объекты, бакеты и учётки стёрты, остались пулы и
// строки тенантов (их в реальном сценарии поднимают из экспорта и конфига).
func (h *harness) wipe(t *testing.T) {
	t.Helper()
	for _, q := range []string{
		`DELETE FROM object_parts`,
		`DELETE FROM objects`,
		`DELETE FROM upload_parts`,
		`DELETE FROM uploads`,
		`DELETE FROM buckets`,
	} {
		if _, err := h.m.Pool.Exec(h.ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
}

func (h *harness) run(t *testing.T) *rebuild.Report {
	t.Helper()
	exp, err := keys.BuildExport(h.ctx, h.m, h.ring)
	if err != nil {
		t.Fatal(err)
	}
	rep, err := rebuild.Run(h.ctx, rebuild.Options{
		Archive: h.arch, Meta: h.m, Ring: h.ring, Export: exp,
		Log: slog.New(slog.NewTextHandler(os.Stderr, nil)),
	})
	if err != nil {
		t.Fatalf("rebuild.Run: %v", err)
	}
	return rep
}

func (h *harness) count(t *testing.T) int {
	t.Helper()
	var n int
	if err := h.m.Pool.QueryRow(h.ctx, `SELECT count(*) FROM objects`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// TestRebuildFromArchive — главный тест постановки: объекты записаны (обычный
// PUT и multipart с переотправленной частью), база потеряна, восстановлены
// только из лент, KEK и экспорта. Сравниваются ETag, размеры и содержимое.
func TestRebuildFromArchive(t *testing.T) {
	h := setup(t)
	single := h.putOne(t, "single", 100*1024)
	multi := h.putMultipart(t, "multi", []int{5 * 1024 * 1024, 5*1024*1024 + 1000, 3000})

	before := map[string]struct {
		etag string
		size int64
	}{}
	for _, k := range []string{"single", "multi"} {
		o := h.current(t, k)
		before[k] = struct {
			etag string
			size int64
		}{o.ETag, o.Size}
	}

	plain := h.snapshot(t)
	if len(plain) != 2 {
		t.Fatalf("в архиве %d объектов, ждали два", len(plain))
	}
	h.wipe(t)
	if h.count(t) != 0 {
		t.Fatal("протирка не сработала")
	}

	rep := h.run(t)
	if len(rep.Skipped) > 0 {
		for _, s := range rep.Skipped {
			t.Errorf("пропущен %s", s)
		}
		t.FailNow()
	}
	if rep.Restored != 2 {
		t.Fatalf("восстановлено %d, ждали два", rep.Restored)
	}
	// Бакет на ленте не живёт: он обязан появиться сам и быть отмечен.
	if len(rep.BucketsCreated) != 1 || rep.BucketsCreated[0] != bucketName {
		t.Errorf("бакеты созданы: %v, ждали [%s]", rep.BucketsCreated, bucketName)
	}

	for _, k := range []string{"single", "multi"} {
		o := h.current(t, k)
		if o.ETag != before[k].etag {
			t.Errorf("%s: ETag %q, был %q", k, o.ETag, before[k].etag)
		}
		if o.Size != before[k].size {
			t.Errorf("%s: размер %d, был %d", k, o.Size, before[k].size)
		}
		if !h.isCurrent(t, o.ID) {
			t.Errorf("%s: восстановленная версия не текущая", k)
		}
		if o.State != "archived" || o.Landing {
			t.Errorf("%s: state=%v landing=%v — восстановленный объект не должен попасть в очередь миграции",
				k, o.State, o.Landing)
		}
		raw, err := os.ReadFile(h.archPath(o.ID))
		if err != nil {
			t.Fatal(err)
		}
		want := single
		if k == "multi" {
			want = multi
		}
		got := h.readPlain(t, o.ID, tenantID, o.WrappedDEK, raw)
		if !bytes.Equal(got, want) {
			t.Errorf("%s: содержимое после восстановления не совпало (%d против %d байт)",
				k, len(got), len(want))
		}
	}
}

// TestRebuildSkipsDestroyedTenant — объект тенанта, которого нет в экспорте,
// пропускается с записью в отчёте и не роняет восстановление остальных.
func TestRebuildSkipsDestroyedTenant(t *testing.T) {
	h := setup(t)
	h.putOne(t, "live", 4096)
	h.snapshot(t)

	// Уничтожаем ключ тестового тенанта: экспорт станет пустым, и единственный
	// объект архива окажется «сиротой» с ленты.
	if err := h.m.DestroyTenantKey(h.ctx, tenantID); err != nil {
		t.Fatal(err)
	}
	// Пустой экспорт — ошибка по построению, поэтому тенант с ключом нужен.
	// Заводим второго и проверяем пропуск по его отсутствию в экспорте.
	seedTenant(t, h.m, 4)
	if _, _, err := keys.New(h.m, h.ring).NewVersionDEK(h.ctx, 4, uuid.New()); err != nil {
		t.Fatal(err)
	}
	h.wipe(t)

	rep := h.run(t)
	if rep.Restored != 0 {
		t.Errorf("восстановлено %d, ждали ноль: объект уничтоженного тенанта вернулся", rep.Restored)
	}
	var found bool
	for _, s := range rep.Skipped {
		if s.Reason == rebuild.ReasonTenantAbsent {
			found = true
		}
	}
	if !found {
		t.Errorf("в отчёте нет пропуска tenant_absent: %v", rep.Skipped)
	}
}

// TestRebuildIdempotent — повторный прогон не дублирует строки и не падает.
func TestRebuildIdempotent(t *testing.T) {
	h := setup(t)
	h.putOne(t, "single", 4096)
	h.putMultipart(t, "multi", []int{5 * 1024 * 1024, 1000})
	h.snapshot(t)
	h.wipe(t)

	first := h.run(t)
	if first.Restored != 2 {
		t.Fatalf("первый прогон: %d", first.Restored)
	}
	n := h.count(t)

	second := h.run(t)
	if second.Restored != 0 {
		t.Errorf("второй прогон добавил %d строк — восстановление не идемпотентно", second.Restored)
	}
	if second.Existing != 2 {
		t.Errorf("второй прогон: Existing=%d, ждали 2", second.Existing)
	}
	if h.count(t) != n {
		t.Errorf("строк стало %d вместо %d", h.count(t), n)
	}
}

// TestRebuildCorruptManifest — битый файл объекта не роняет прогон.
func TestRebuildCorruptManifest(t *testing.T) {
	h := setup(t)
	h.putOne(t, "good", 4096)
	h.putOne(t, "bad", 4096)
	h.snapshot(t)

	// Портим один объект: бьём по последнему байту шифротекста манифеста — это
	// часть тега GCM. Портить середину файла бессмысленно: там тело объекта, а
	// манифест лежит в хвосте перед 48-байтным футером, и проверка аутентичности
	// относится именно к нему.
	var badOid string
	refs, err := h.arch.List(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range refs {
		badOid = r.Oid
		break
	}
	raw, err := os.ReadFile(h.archPathStr(badOid))
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-encrypt.FooterSize-1] ^= 0xff
	if err := os.WriteFile(h.archPathStr(badOid), raw, 0o600); err != nil {
		t.Fatal(err)
	}

	h.wipe(t)
	rep := h.run(t)
	if rep.Restored != 1 {
		t.Errorf("восстановлено %d, ждали один: битый объект не должен мешать остальным", rep.Restored)
	}
	var found bool
	for _, s := range rep.Skipped {
		if s.Reason == rebuild.ReasonManifest {
			found = true
		}
	}
	if !found {
		t.Errorf("битый манифест не попал в отчёт: %v", rep.Skipped)
	}
}

func (h *harness) isCurrent(t *testing.T, id uuid.UUID) bool {
	t.Helper()
	var cur bool
	if err := h.m.Pool.QueryRow(h.ctx,
		`SELECT is_current FROM objects WHERE id = $1`, id).Scan(&cur); err != nil {
		t.Fatal(err)
	}
	return cur
}

func (h *harness) current(t *testing.T, key string) meta.Object {
	t.Helper()
	o, err := h.m.GetObject(h.ctx, meta.NewTenant(int(tenantID)), bucketName, key)
	if err != nil {
		t.Fatalf("GetObject %s: %v", key, err)
	}
	return *o
}

func (h *harness) archPath(id uuid.UUID) string  { return h.arch.Path(id.String()) }
func (h *harness) archPathStr(oid string) string { return h.arch.Path(oid) }

func seedTenant(t *testing.T, m *meta.Meta, id int64) {
	t.Helper()
	_, err := m.Pool.Exec(context.Background(),
		// $1 дважды в разных ролях Postgres не выносит: для id он bigint, для
		// конкатенации text. Поэтому имя передаём отдельным параметром.
		`INSERT INTO tenants (id, name, pool_id, system)
		 VALUES ($1, $2, 1, false) ON CONFLICT (id) DO NOTHING`, id, "tenant-test")
	if err != nil {
		t.Fatal(err)
	}
}

// TestRebuildDryRun — отчёт есть, записи нет.
//
// Счётчики обязаны совпасть с боевым прогоном: dry-run смотрят именно затем,
// чтобы решить, запускать ли восстановление, и «в dry-run 5, в бою 3» обесценивает
// такое решение.
func TestRebuildDryRun(t *testing.T) {
	h := setup(t)
	h.putOne(t, "single", 4096)
	h.snapshot(t)
	h.wipe(t)

	exp, err := keys.BuildExport(h.ctx, h.m, h.ring)
	if err != nil {
		t.Fatal(err)
	}
	rep, err := rebuild.Run(h.ctx, rebuild.Options{
		Archive: h.arch, Meta: h.m, Ring: h.ring, Export: exp,
		Log: slog.New(slog.NewTextHandler(os.Stderr, nil)), DryRun: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Restored != 1 {
		t.Errorf("dry-run: Restored=%d, ждали 1 — отчёт обязан совпадать с боевым прогоном", rep.Restored)
	}
	if len(rep.BucketsCreated) != 1 {
		t.Errorf("dry-run: бакетов %d, ждали 1", len(rep.BucketsCreated))
	}
	if h.count(t) != 0 {
		t.Errorf("dry-run записал %d объектов", h.count(t))
	}
	if _, err := h.m.GetBucket(h.ctx, meta.NewTenant(int(tenantID)), bucketName); err == nil {
		t.Error("dry-run создал бакет")
	}
}
