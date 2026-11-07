package recall_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/vladsimachkov/tapegw/internal/encrypt"
	"github.com/vladsimachkov/tapegw/internal/iam"
	"github.com/vladsimachkov/tapegw/internal/keys"
	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/mover"
	"github.com/vladsimachkov/tapegw/internal/recall"
	"github.com/vladsimachkov/tapegw/internal/spool"
	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// Минимальный recaller (S5). Лента подменяется словарём байтов: проверяется
// не протокол mover'а (у него свои тесты), а порядок «файл → fsync →
// проверка → publish → переход» и реакция на каждую из сверок: несовпавшая
// сумма пачки, испорченный шифротекст, чужой ETag, неверная длина, ошибка
// чтения.

type fakeGetter struct {
	streams map[uuid.UUID][]byte
	err     error
	calls   int
}

func (f *fakeGetter) Get(op, id uuid.UUID, w io.Writer) (*mover.Result, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	b, ok := f.streams[id]
	if !ok {
		return &mover.Result{Code: mover.CodeNotFound, State: mover.StateFailed}, nil
	}
	if _, err := w.Write(b); err != nil {
		return nil, err
	}
	sum := sha256.Sum256(b)
	return &mover.Result{Code: mover.CodeOK, State: mover.StateDone, SHA256: hex.EncodeToString(sum[:])}, nil
}

type fixture struct {
	m     *meta.Meta
	vault *keys.Vault
	cache *spool.Spool
	get   *fakeGetter
	rc    *recall.Recall
	ctx   context.Context
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	m := testdb.Connect(t)
	testdb.Clean(t, m)

	ring, err := iam.NewKeyRing(1, map[int][]byte{1: make([]byte, 32)})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO tenants (id, name, pool_id) VALUES (2, 'recall', 1)
		 ON CONFLICT (id) DO NOTHING`); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Pool.Exec(ctx,
		`INSERT INTO buckets (name, tenant_id, owner, acl, versioning)
		 VALUES ('rb', 2, 'o', '{}'::jsonb, 'unversioned')`); err != nil {
		t.Fatal(err)
	}
	cache, err := spool.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{
		m: m, vault: keys.New(m, ring), cache: cache,
		get: &fakeGetter{streams: map[uuid.UUID][]byte{}},
		ctx: ctx,
	}
	f.rc = &recall.Recall{
		Meta: m, Get: f.get, Cache: cache, Keys: f.vault,
		BatchMax: 10, ClaimTTL: time.Minute, MaxAttempts: 3, RetryBackoff: time.Second,
	}
	return f
}

// pack создаёт пачку с готовой копией; поток ленты задаётся позже через
// replaceStream — он собирается из участников, а участники ссылаются на
// пачку.
func (f *fixture) pack(t *testing.T) uuid.UUID {
	t.Helper()
	ctx := f.ctx
	packID := uuid.New()
	if _, err := f.m.Pool.Exec(ctx,
		`INSERT INTO packs (id, pool_id, kind, state, total_bytes, live_bytes, members_total, members_live)
		 VALUES ($1, 1, 'data', 'archived', 0, 0, 1, 1)`, packID); err != nil {
		t.Fatal(err)
	}
	label := "rt-" + uuid.NewString()[:8]
	if _, err := f.m.Pool.Exec(ctx,
		`INSERT INTO media (label, lifecycle, pool_id, generation, medium_type, free_bytes_cached)
		 VALUES ($1, 'active', 1, 'L8', 'L8', 1000000)`, label); err != nil {
		t.Fatal(err)
	}
	var mediaID int64
	if err := f.m.Pool.QueryRow(ctx, `SELECT id FROM media WHERE label = $1`, label).Scan(&mediaID); err != nil {
		t.Fatal(err)
	}
	oid := uuid.New()
	if _, err := f.m.RecordPackCopy(ctx, packID, "main", oid.String(), 1, 1, make([]byte, 32), mediaID); err != nil {
		t.Fatal(err)
	}
	return packID
}

// replaceStream кладёт поток в фейковую ленту под oid копии и пересчитывает
// сумму копии — как если бы на ленту легли именно эти байты.
func (f *fixture) replaceStream(t *testing.T, packID uuid.UUID, stream []byte) {
	t.Helper()
	var oid string
	if err := f.m.Pool.QueryRow(f.ctx,
		`SELECT phobos_oid FROM archive_copies WHERE pack_id = $1`, packID).Scan(&oid); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(stream)
	if _, err := f.m.Pool.Exec(f.ctx,
		`UPDATE archive_copies SET sha256 = $2 WHERE pack_id = $1`, packID, sum[:]); err != nil {
		t.Fatal(err)
	}
	f.get.streams[uuid.MustParse(oid)] = stream
}

// object заводит архивного участника пачки: DEK версии, зашифрованный файл
// участника (ровно те байты, что лежат на диске и в пачке), строка objects с
// pack-координатами и часть в object_parts. Один вызов — один DEK: шифр и
// обёртка обязаны соответствовать друг другу, как в PUT.
func (f *fixture) object(t *testing.T, packID uuid.UUID, n int, offset int64, plain []byte) (
	uuid.UUID, []byte) {
	t.Helper()
	ctx := f.ctx

	id := uuid.New()
	dek, wrapped, err := f.vault.NewVersionDEK(ctx, 2, id)
	if err != nil {
		t.Fatal(err)
	}
	salt := make([]byte, encrypt.SaltSize)
	if _, err := rand.Read(salt); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	w, err := encrypt.NewWriter(&buf, dek, id, 1, salt, int64(len(plain)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(plain); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	member := buf.Bytes()

	var bucketID int64
	if err := f.m.Pool.QueryRow(ctx, `SELECT id FROM buckets WHERE name = 'rb'`).Scan(&bucketID); err != nil {
		t.Fatal(err)
	}
	sum := md5.Sum(plain)
	if _, err := f.m.Pool.Exec(ctx,
		`INSERT INTO objects (id, bucket_id, key, size, etag, storage_class, state,
		                      landing, online_until, tenant_id, pool_id, is_current,
		                      wrapped_dek, pack_id, pack_offset, pack_length, file_bytes)
		 VALUES ($1,$2,$3,$4,$5,'GLACIER','archived',false, now() - interval '1 hour',
		         2, 1, true, $6, $7, $8, $9, $9)`,
		id, bucketID, fmt.Sprintf("k%d", n), len(plain), hex.EncodeToString(sum[:]),
		wrapped, packID, offset, len(member)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.m.Pool.Exec(ctx,
		`INSERT INTO object_parts (version_id, part_number, plain_size, cipher_offset, salt)
		 VALUES ($1, 1, $2, 0, $3)`, id, len(plain), salt); err != nil {
		t.Fatal(err)
	}
	if _, err := f.m.RequestRestore(ctx, meta.NewTenant(2), id, 7, "Standard"); err != nil {
		t.Fatal(err)
	}
	return id, member
}

func (f *fixture) restoreRow(t *testing.T, id uuid.UUID) (state string, attempts int) {
	t.Helper()
	if err := f.m.Pool.QueryRow(f.ctx,
		`SELECT state, attempts FROM restores WHERE object_id = $1`, id).Scan(&state, &attempts); err != nil {
		t.Fatal(err)
	}
	return state, attempts
}

func (f *fixture) cacheBytes(t *testing.T, id uuid.UUID) []byte {
	t.Helper()
	fl, err := f.cache.Open(id)
	if err != nil {
		t.Fatalf("открыть cache: %v", err)
	}
	defer fl.Close()
	data, err := io.ReadAll(fl)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// Счастливый путь: файл в cache ровно с байтами участника, строка в
// 'restored', срок продлён от завершения (online_until = expires_at).
func TestRecallRestoresMemberToCache(t *testing.T) {
	f := newFixture(t)
	plain := []byte("исходные байты для проверки после restore")
	packID := f.pack(t)
	id, member := f.object(t, packID, 0, 100, plain)
	stream := append(append(make([]byte, 100), member...), 7)
	f.replaceStream(t, packID, stream)

	n, err := f.rc.Once(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("восстановлено %d, ждали 1", n)
	}
	if !f.cache.Exists(id) {
		t.Fatal("файла нет в cache")
	}
	if got := f.cacheBytes(t, id); !bytes.Equal(got, member) {
		t.Errorf("в cache %d байт, ждали участник целиком (%d)", len(got), len(member))
	}
	if st, att := f.restoreRow(t, id); st != "restored" || att != 0 {
		t.Errorf("строка %s attempts=%d, ждали restored/0", st, att)
	}
	var expires, until time.Time
	if err := f.m.Pool.QueryRow(f.ctx,
		`SELECT r.expires_at, o.online_until FROM restores r JOIN objects o ON o.id = r.object_id
		 WHERE r.object_id = $1`, id).Scan(&expires, &until); err != nil {
		t.Fatal(err)
	}
	if d := time.Until(expires); d < 6*24*time.Hour || d > 8*24*time.Hour {
		t.Errorf("expires_at через %v, ждали ~7 дней от завершения", d)
	}
	if !until.Equal(expires) {
		t.Errorf("online_until %v != expires_at %v, ждали один срок", until, expires)
	}
}

// Сумма потока не совпала с записанной при архивации — участник не попадает в
// cache и не переходит в 'restored'; попытка засчитана.
func TestRecallPackSumMismatch(t *testing.T) {
	f := newFixture(t)
	plain := []byte("данные")
	packID := f.pack(t)
	id, member := f.object(t, packID, 0, 0, plain)
	f.replaceStream(t, packID, member)
	// Портим сумму копии: «лента читается не та».
	if _, err := f.m.Pool.Exec(f.ctx,
		`UPDATE archive_copies SET sha256 = set_byte(sha256, 0, 255) WHERE pack_id = $1`, packID); err != nil {
		t.Fatal(err)
	}

	n, err := f.rc.Once(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("восстановлено %d при несовпадении суммы пачки", n)
	}
	if f.cache.Exists(id) {
		t.Error("файл опубликован в cache несмотря на несовпадение суммы")
	}
	if st, att := f.restoreRow(t, id); st != "requested" || att != 1 {
		t.Errorf("строка %s attempts=%d, ждали requested/1", st, att)
	}
}

// Байт внутри шифротекста испорчен, но сумма пачки соответствует испорченному
// потоку: сверка пачки проходит, и именно расшифрование обязано поймать
// подмену (AEAD — та «сумма, сохранённая при записи», что требует ревью).
// Повтор здесь бессилен — байты на ленте именно те, что записывали, — поэтому
// строка уходит в 'failed' сразу, без раскрутки ленты до max_attempts.
func TestRecallTamperedCiphertext(t *testing.T) {
	f := newFixture(t)
	plain := []byte("данные, которые нельзя отдавать испорченными")
	packID := f.pack(t)
	id, member := f.object(t, packID, 0, 0, plain)
	tampered := append([]byte(nil), member...)
	tampered[len(tampered)/2] ^= 0xff
	f.replaceStream(t, packID, tampered) // сумма копии — по испорченному: пачка «цела»

	n, err := f.rc.Once(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("испорченный участник восстановлен (%d) — проверка не сработала", n)
	}
	if f.cache.Exists(id) {
		t.Error("испорченный файл опубликован в cache")
	}
	if st, att := f.restoreRow(t, id); st != "failed" || att != 1 {
		t.Errorf("строка %s attempts=%d, ждали failed/1 (повтор прочитал бы те же байты)", st, att)
	}
}

// ETag не совпал с md5 расшифрованных байтов — тот же исход: не restored.
func TestRecallETagMismatch(t *testing.T) {
	f := newFixture(t)
	plain := []byte("данные")
	packID := f.pack(t)
	id, member := f.object(t, packID, 0, 0, plain)
	f.replaceStream(t, packID, member)
	other := md5.Sum([]byte("другие байты"))
	if _, err := f.m.Pool.Exec(f.ctx,
		`UPDATE objects SET etag = $2 WHERE id = $1`, id, hex.EncodeToString(other[:])); err != nil {
		t.Fatal(err)
	}

	n, err := f.rc.Once(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("восстановлено %d при несовпадении ETag", n)
	}
	// Как и с испорченным шифротекстом: повтор прочитает те же байты, поэтому
	// сразу 'failed', а не очередь ретраев.
	if st, att := f.restoreRow(t, id); st != "failed" || att != 1 {
		t.Errorf("строка %s attempts=%d, ждали failed/1", st, att)
	}
}

// Ошибка чтения ленты — ретрай с backoff; на исчерпании max_attempts — failed.
func TestRecallGetErrorRetriesThenFails(t *testing.T) {
	f := newFixture(t)
	f.rc.MaxAttempts = 2
	plain := []byte("данные")
	packID := f.pack(t)
	id, member := f.object(t, packID, 0, 0, plain)
	f.replaceStream(t, packID, member)
	f.get.err = fmt.Errorf("драйв лёг")

	if n, err := f.rc.Once(f.ctx); err != nil || n != 0 {
		t.Fatalf("проход с ошибкой ленты: %d, %v", n, err)
	}
	st, att := f.restoreRow(t, id)
	if st != "requested" || att != 1 {
		t.Fatalf("после сбоя: %s attempts=%d, ждали requested/1", st, att)
	}

	// Вторая попытка (backoff «наступил») — на исчерпании max_attempts=2.
	if _, err := f.m.Pool.Exec(f.ctx,
		`UPDATE restores SET next_try_at = now() - interval '1 second' WHERE object_id = $1`, id); err != nil {
		t.Fatal(err)
	}
	if n, err := f.rc.Once(f.ctx); err != nil || n != 0 {
		t.Fatalf("второй проход: %d, %v", n, err)
	}
	if st, att := f.restoreRow(t, id); st != "failed" || att != 2 {
		t.Errorf("на исчерпании: %s attempts=%d, ждали failed/2", st, att)
	}
}

// Участники одной пачки читаются одним потоком: один Get — оба в cache.
func TestRecallTwoMembersOnePack(t *testing.T) {
	f := newFixture(t)
	plainA := []byte("участник A")
	plainB := []byte("участник B, подлиннее первого")
	packID := f.pack(t)
	idA, memberA := f.object(t, packID, 0, 0, plainA)
	idB, memberB := f.object(t, packID, 1, int64(len(memberA)), plainB)

	stream := append(append([]byte(nil), memberA...), memberB...)
	stream = append(stream, 0)
	f.replaceStream(t, packID, stream)

	n, err := f.rc.Once(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("восстановлено %d, ждали 2", n)
	}
	if f.get.calls != 1 {
		t.Errorf("обращений к ленте %d, ждали 1 на пачку", f.get.calls)
	}
	if got := f.cacheBytes(t, idA); !bytes.Equal(got, memberA) {
		t.Errorf("участник A в cache не совпал (%d байт)", len(got))
	}
	if got := f.cacheBytes(t, idB); !bytes.Equal(got, memberB) {
		t.Errorf("участник B в cache не совпал (%d байт)", len(got))
	}
}

// blockingGetter — «лента, которая не возвращается»: воркер стоит в чтении
// своей пачки, как на настоящей загрузке/позиционировании драйва.
type blockingGetter struct {
	started chan struct{}
	release chan struct{}
}

func (b *blockingGetter) Get(op, id uuid.UUID, w io.Writer) (*mover.Result, error) {
	close(b.started)
	<-b.release
	return nil, fmt.Errorf("лента отпущена в конце теста")
}

// Lease покрывает одно чтение ленты, а не батч (ревью S5, major 1): воркер,
// вставший на своей пачке, не должен блокировать второй. Второй обязан сразу
// взять вторую пачку, не тронув ни строки, ни попытки первой. С захватом
// батчем первый забрал бы всю очередь, и этот тест красный.
func TestRecallSecondWorkerTakesOtherPack(t *testing.T) {
	f := newFixture(t)

	// Пачка A — читается и стоит.
	packA := f.pack(t)
	idA, memberA := f.object(t, packA, 0, 0, []byte("зависшая пачка"))
	f.replaceStream(t, packA, memberA)

	// Пачка B — свободна, её берёт второй воркер.
	packB := f.pack(t)
	idB, memberB := f.object(t, packB, 1, 0, []byte("свободная пачка"))
	f.replaceStream(t, packB, memberB)

	block := &blockingGetter{started: make(chan struct{}), release: make(chan struct{})}
	defer close(block.release)
	rcA := &recall.Recall{
		Meta: f.m, Get: block, Cache: f.cache, Keys: f.vault,
		BatchMax: 10, ClaimTTL: time.Minute, MaxAttempts: 3, RetryBackoff: time.Second,
	}
	go func() { _, _ = rcA.Once(f.ctx) }()
	<-block.started // A захватил свою пачку и стоит в Get

	n, err := f.rc.Once(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("второй воркер восстановил %d, ждали 1 — его пачка не должна ждать чужую", n)
	}
	if !f.cache.Exists(idB) {
		t.Error("пачка B не восстановлена вторым воркером")
	}
	if st, att := f.restoreRow(t, idB); st != "restored" || att != 0 {
		t.Errorf("пачка B: %s attempts=%d, ждали restored/0", st, att)
	}
	if st, att := f.restoreRow(t, idA); st != "in_progress" || att != 0 {
		t.Errorf("чужая пачка тронута: %s attempts=%d, ждали in_progress/0", st, att)
	}
}

// Длина файла не совпала с обещанной в БД — участник не принимается.
func TestRecallSizeMismatch(t *testing.T) {
	f := newFixture(t)
	plain := []byte("данные")
	packID := f.pack(t)
	id, member := f.object(t, packID, 0, 0, plain)
	f.replaceStream(t, packID, member)
	// БД обещает файл длиннее фактического: проверку обязана отказать,
	// а не обрезать/дополнить.
	if _, err := f.m.Pool.Exec(f.ctx,
		`UPDATE objects SET file_bytes = $2 WHERE id = $1`, id, len(member)+5); err != nil {
		t.Fatal(err)
	}

	n, err := f.rc.Once(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("участник с неверной длиной принят (%d)", n)
	}
	if f.cache.Exists(id) {
		t.Error("файл неверной длины опубликован")
	}
	// Отказ проверки при совпавшей сумме пачки — терминальный: повтор прочитает
	// те же байты, лента не раскручивается.
	if st, att := f.restoreRow(t, id); st != "failed" || att != 1 {
		t.Errorf("строка %s attempts=%d, ждали failed/1", st, att)
	}
}

// Нулевой claim_ttl — ошибка сборки прохода, а не «lease истёк сразу»:
// те же грабли, что с landing_min_retention в архивации.
func TestRecallGuardsZeroConfig(t *testing.T) {
	f := newFixture(t)
	f.rc.ClaimTTL = 0
	if _, err := f.rc.Once(f.ctx); err == nil || !strings.Contains(err.Error(), "claim_ttl") {
		t.Errorf("нулевой claim_ttl: %v, ждали ошибку про claim_ttl", err)
	}
}
