package backend_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/google/uuid"
	"github.com/versity/versitygw/s3response"

	"github.com/vladsimachkov/tapegw/internal/encrypt"
	"github.com/vladsimachkov/tapegw/internal/keys"
	"github.com/vladsimachkov/tapegw/internal/migrator"
	"github.com/vladsimachkov/tapegw/internal/pack"
)

// Восстановление из пачки без таблиц объектов — то, ради чего участник пачки
// равен файлу целиком (ADR-0020, правка ревью S2): соль частей, имя бакета и
// ключа, ETag и метаданные живут только в манифесте участника, а в pax их нет.
//
// Проверка идёт запасным проходом — pack.Walk по tar-заголовкам, без индекса
// пачки и без строк objects/object_parts: только pax-заголовок участника и его
// байты. Открытые байты обязаны сойтись побайтно и у объекта в 17 МиБ (больше
// кадра mover'а и больше окна кредитов — правило размера из ревью #116), и у
// multipart. Цепочка ключей (tenants + кольцо KEK) при этом остаётся: ключи
// переживают потерю БД отдельно от таблиц объектов, ровно так же их берёт
// rebuild.
func TestPackFallbackDecryptsWithoutObjectTables(t *testing.T) {
	be, m, land := newBackend(t)
	mustBucket(t, be)
	ctx := context.Background()

	// 17 МиБ одним PUT: за рамками и кадра (4 МиБ), и окна кредитов (16 МиБ).
	big := make([]byte, 17<<20)
	for i := range big {
		big[i] = byte(i*7 + i>>9)
	}
	if _, err := be.PutObject(testCtx(), s3response.PutObjectInput{
		Bucket: ptr("b"), Key: ptr("big.bin"),
		Body: bytes.NewReader(big), ContentLength: aws.Int64(int64(len(big))),
	}); err != nil {
		t.Fatalf("PUT 17 МиБ: %v", err)
	}
	bigObj := currentObject(t, m, "b", "big.bin")

	// Multipart: файл склеен из частей, у каждой своя соль.
	_, mpData := fullMPU(t, be, "b", "mp.bin")
	mpObj := currentObject(t, m, "b", "mp.bin")

	// Пачка собирается тем же загрузчиком, каким будет пользоваться воркер.
	load := migrator.NewLoader(m, land)
	ids := []uuid.UUID{bigObj.ID, mpObj.ID}
	items, err := load(ctx, ids)
	if err != nil {
		t.Fatalf("загрузчик: %v", err)
	}
	// Порядок — часть контракта: он входит в сумму пачки.
	for i := range items {
		if items[i].Version != ids[i] {
			t.Fatalf("загрузчик переставил участников: позиция %d — %s, ждали %s",
				i, items[i].Version, ids[i])
		}
	}
	plan, err := migrator.Measure(items)
	if err != nil {
		t.Fatalf("первый проход: %v", err)
	}
	var buf bytes.Buffer
	if _, err := migrator.BuildIndexed(&buf, items, plan); err != nil {
		t.Fatalf("второй проход: %v", err)
	}

	// «БД потеряна»: ниже не читается ни objects, ни object_parts — только
	// пачка и кольцо KEK с экспортом (tenants).
	vault := keys.New(m, testKeyRing(t))
	want := map[uuid.UUID]struct {
		plain []byte
		key   string
	}{
		bigObj.ID: {big, "big.bin"},
		mpObj.ID:  {mpData, "mp.bin"},
	}
	got := 0
	err = pack.Walk(bytes.NewReader(buf.Bytes()), func(mh pack.MemberHeader, r io.Reader) error {
		version, err := uuid.FromBytes(mh.Version[:])
		if err != nil {
			return err
		}
		exp, ok := want[version]
		if !ok {
			t.Errorf("в пачке участник %s, которого не закладывали", version)
			return nil
		}
		data, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		// Длина из pax-заголовка = длина файла = file_bytes из строки:
		// участник не обрезан до шифротекста.
		if mh.FileLen != uint64(len(data)) {
			t.Errorf("%s: pax обещает %d байт, прочитано %d", version, mh.FileLen, len(data))
		}
		dek, err := vault.OpenVersionDEK(ctx, int64(mh.Tenant), mh.WrappedDEK, version)
		if err != nil {
			return err
		}
		defer encrypt.Zero(dek)

		at := bytes.NewReader(data)
		doc, err := encrypt.OpenManifestAt(at, int64(len(data)), dek, version)
		if err != nil {
			t.Errorf("%s: манифест участника не вскрылся: %v", version, err)
			return nil
		}
		if doc.Bucket != "b" || doc.Key != exp.key {
			t.Errorf("%s: манифест говорит %s/%s, ждали b/%s", version, doc.Bucket, doc.Key, exp.key)
		}
		if doc.Size != int64(len(exp.plain)) {
			t.Errorf("%s: манифест обещает %d байт, закладывали %d", version, doc.Size, len(exp.plain))
		}
		parts := make([]encrypt.Part, 0, len(doc.Parts))
		for _, p := range doc.Parts {
			parts = append(parts, encrypt.Part{
				Number: p.Number, PlainSize: p.Plain, CipherOffset: p.Off, Salt: p.Salt,
			})
		}
		rd, err := encrypt.NewPartsReader(at, dek, version, parts, doc.Size)
		if err != nil {
			return err
		}
		defer rd.Close()
		// Reader отдаёт чтение по смещению (так его использует GET с Range),
		// весь поток читают через SectionReader.
		plain, err := io.ReadAll(io.NewSectionReader(rd, 0, doc.Size))
		if err != nil {
			return err
		}
		if !bytes.Equal(plain, exp.plain) {
			t.Errorf("%s: восстановленные байты не совпали с исходными (%d против %d)",
				version, len(plain), len(exp.plain))
		}
		got++
		return nil
	})
	if err != nil {
		t.Fatalf("запасной проход по пачке: %v", err)
	}
	if got != len(want) {
		t.Errorf("восстановлено %d участников, ждали %d", got, len(want))
	}
}

// Загрузчик обязан отказать участнику без file_bytes, а не угадать длину:
// угаданная длина обрежет манифест уже на ленте, и объект станет нечитаемым.
// Строки без колонки — это версии, записанные до миграции 0023.
func TestLoaderRefusesMissingFileBytes(t *testing.T) {
	be, m, land := newBackend(t)
	mustBucket(t, be)
	ctx := context.Background()

	var bucketID int64
	if err := m.Pool.QueryRow(ctx,
		`SELECT id FROM buckets WHERE name = 'b'`).Scan(&bucketID); err != nil {
		t.Fatalf("id бакета: %v", err)
	}
	orphan := uuid.New()
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO objects (id, bucket_id, key, size, etag, storage_class, state,
		                      landing, online_until, tenant_id, pool_id, is_current, wrapped_dek)
		  VALUES ($1, $2, 'до-миграции', 10, 'etag', 'GLACIER', 'archiving', true,
		          now() + interval '1 day', 1, 1, true, 'x'::bytea)`,
		orphan, bucketID); err != nil {
		t.Fatalf("вставка строки без file_bytes: %v", err)
	}

	load := migrator.NewLoader(m, land)
	if _, err := load(ctx, []uuid.UUID{orphan}); err == nil {
		t.Fatal("загрузчик принял участника без file_bytes")
	} else if !bytes.Contains([]byte(err.Error()), []byte("file_bytes")) {
		t.Errorf("отказ без объяснения причины: %v", err)
	}
}
