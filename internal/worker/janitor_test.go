package worker_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/versity/versitygw/s3err"

	"github.com/vladsimachkov/tapegw/internal/worker"
)

// Janitor (S3): вытеснение по истечении online_until. Порядок — ADR-0004:
// флаг снимается в транзакции (ClaimEvictBatch), unlink — после commit;
// защита по состоянию, а не по дате; GET во время вытеснения — штатный
// InvalidObjectState, а не ENOENT и не 500.

// isInvalidObjectState — штатный ответ «нет дисковой копии» (блокер B1:
// именно он, а не ENOENT и не 500).
func isInvalidObjectState(err error) bool {
	var se s3err.S3Error
	return errors.As(err, &se) && se.BaseError() == s3err.GetAPIError(s3err.ErrInvalidObjectState)
}

// expireObject помечает объект archived со сроком в прошлом: landing-файл
// остался с PUT-а, флаг ещё поднят.
func expireObject(t *testing.T, h *harness, id uuid.UUID) {
	t.Helper()
	if _, err := h.m.Pool.Exec(context.Background(),
		`UPDATE objects SET state = 'archived', online_until = now() - interval '1 second'
		  WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
}

func fileExists(h *harness, id uuid.UUID) bool { return h.land.Exists(id) }

func landingFlag(t *testing.T, h *harness, id uuid.UUID) bool {
	t.Helper()
	var v bool
	if err := h.m.Pool.QueryRow(context.Background(),
		`SELECT landing FROM objects WHERE id = $1`, id).Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

func startJanitor(t *testing.T, h *harness, ctx context.Context) *worker.JanitorGroup {
	t.Helper()
	h.cfg.Janitor.Interval = 50 * time.Millisecond
	h.cfg.Janitor.ShutdownGrace = 10 * time.Second
	return worker.StartJanitor(ctx, worker.JanitorDeps{
		Cfg: h.cfg, Meta: h.m, Spool: h.land, Cache: h.cache, Log: h.log,
	})
}

// Обычный путь: archived с истёкшим сроком — файл исчезает, флаг снят,
// GET после этого отвечает InvalidObjectState (блокер B1: не ENOENT и не 500).
func TestJanitorEvictsExpiredObject(t *testing.T) {
	h := newHarness(t)
	body := []byte("вытеснение по сроку, не по watermark")
	id := h.putObject(t, "jb", "due.bin", body)
	expireObject(t, h, id)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	jan := startJanitor(t, h, ctx)

	waitFor(t, "файл вытеснен", func() bool { return !fileExists(h, id) })
	if landingFlag(t, h, id) {
		t.Error("файла нет, а landing всё ещё true — порядок по ADR-0004 нарушен")
	}
	cancel()
	if !jan.Wait(h.cfg.Janitor.ShutdownGrace) {
		t.Error("janitor не завершился по сигналу")
	}

	_, err := h.be.GetObject(h.adminCtx(), &s3.GetObjectInput{
		Bucket: aws.String("jb"), Key: aws.String("due.bin"),
	})
	if !isInvalidObjectState(err) {
		t.Errorf("GET после вытеснения: %v, ждали InvalidObjectState", err)
	}
}

// Защита по состоянию, а не по дате: pending с прошедшим online_until —
// штатная картина при задержке migrator дольше landing_min_retention (текст
// ADR-0003). Удалить такой файл — значит потерять данные, за которые клиент
// получил 200. Мутация «убрать условие по состоянию» ловится этим тестом.
func TestJanitorKeepsPendingDespiteDate(t *testing.T) {
	h := newHarness(t)
	id := h.putObject(t, "jb", "late.bin", []byte("migrator отстаёт — данные остаются на диске"))
	if _, err := h.m.Pool.Exec(context.Background(),
		`UPDATE objects SET online_until = now() - interval '1 hour' WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	jan := startJanitor(t, h, ctx)
	// Несколько проходов вытеснителя поверх истёкшей даты.
	time.Sleep(300 * time.Millisecond)
	cancel()
	jan.Wait(h.cfg.Janitor.ShutdownGrace)

	if !fileExists(h, id) {
		t.Fatal("pending с прошедшей датой вытеснен — защита обязана быть по состоянию")
	}
	if !landingFlag(t, h, id) {
		t.Error("у pending снят флаг landing")
	}
	// И GET обязан работать: данные на месте.
	out, err := h.be.GetObject(h.adminCtx(), &s3.GetObjectInput{
		Bucket: aws.String("jb"), Key: aws.String("late.bin"),
	})
	if err != nil {
		t.Fatalf("GET незаархивированного объекта: %v", err)
	}
	defer out.Body.Close()
	if _, err := io.ReadAll(out.Body); err != nil {
		t.Fatal(err)
	}
}

// Два параллельных участника (свойство про гонку GET и вытеснения, ADR-0004):
// читатель открыл файл до вытеснения — дочитывает до конца, unlink ему не
// мешает (Linux держит inode до закрытия дескриптора). GET, пришедший после
// смены флага, получает InvalidObjectState.
func TestGetDuringEviction(t *testing.T) {
	h := newHarness(t)
	body := []byte("читатель держит файл — janitor всё равно вытесняет, а чтение завершится")
	id := h.putObject(t, "jb", "race.bin", body)
	expireObject(t, h, id)

	reader, err := h.land.Open(id)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	// На диске лежит шифрованный файл (ADR-0019), а не тело: ожидаем ровно то,
	// что файл содержит, — снимаем копию вторым дескриптором до вытеснения.
	second, err := h.land.Open(id)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := io.ReadAll(second)
	second.Close()
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	jan := startJanitor(t, h, ctx)
	waitFor(t, "janitor вытеснил файл, пока читатель держит дескриптор",
		func() bool { return !fileExists(h, id) })

	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("чтение открытого файла после unlink: %v", err)
	}
	if !bytes.Equal(got, expected) {
		t.Errorf("дочитано %d байт из %d — чтение оборвалось на unlink", len(got), len(expected))
	}

	// GET после смены флага — штатный отказ, а не ENOENT/500.
	if _, err := h.be.GetObject(h.adminCtx(), &s3.GetObjectInput{
		Bucket: aws.String("jb"), Key: aws.String("race.bin"),
	}); err == nil {
		t.Error("GET после вытеснения прошёл — флаг не учтён")
	} else if !isInvalidObjectState(err) {
		t.Errorf("GET после вытеснения: %v, ждали InvalidObjectState", err)
	}
	cancel()
	jan.Wait(h.cfg.Janitor.ShutdownGrace)
}

// Граница: до наступления online_until файл не вытесняется — дата в
// x-amz-restore обещает доступность, и janitor не имеет права её нарушить.
func TestJanitorKeepsObjectBeforeDeadline(t *testing.T) {
	h := newHarness(t)
	id := h.putObject(t, "jb", "soon.bin", []byte("срок ещё не наступил"))
	if _, err := h.m.Pool.Exec(context.Background(),
		`UPDATE objects SET state = 'archived', online_until = now() + interval '1 hour'
		  WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	jan := startJanitor(t, h, ctx)
	time.Sleep(300 * time.Millisecond)
	cancel()
	jan.Wait(h.cfg.Janitor.ShutdownGrace)

	if _, err := os.Stat(h.land.Path(id)); err != nil {
		t.Errorf("файл исчез до срока: %v", err)
	}
	if !landingFlag(t, h, id) {
		t.Error("флаг landing снят до срока")
	}
}

// restoreToCache — результат работы recaller'а (S5): файл в cache, строка
// archived без landing, в restores живой 'restored'.
func restoreToCache(t *testing.T, h *harness, id uuid.UUID, until string) {
	t.Helper()
	ctx := context.Background()

	src, err := h.land.Open(id)
	if err != nil {
		t.Fatal(err)
	}
	tmp, err := h.cache.Tmp()
	if err != nil {
		src.Close()
		t.Fatal(err)
	}
	if _, err := io.Copy(tmp, src); err != nil {
		src.Close()
		tmp.Close()
		t.Fatal(err)
	}
	src.Close()
	if err := h.cache.Publish(tmp, id); err != nil {
		t.Fatal(err)
	}
	if err := h.land.Remove(id); err != nil {
		t.Fatal(err)
	}
	if _, err := h.m.Pool.Exec(ctx,
		`UPDATE objects SET state = 'archived', landing = false, archived_at = now(),
		        online_until = now() + $2::interval WHERE id = $1`, id, until); err != nil {
		t.Fatal(err)
	}
	if _, err := h.m.Pool.Exec(ctx,
		`INSERT INTO restores (object_id, tenant_id, state, tier, days, requested_at,
		                       restored_at, expires_at)
		 VALUES ($1, 1, 'restored', 'Standard', 7, now() - interval '8 days',
		         now() - interval '8 days', now() + $2::interval)`,
		id, until); err != nil {
		t.Fatal(err)
	}
}

// Критерий S6 «после срока restore — снова InvalidObjectState»: истёкший
// restore вытесняется из cache — строка уходит в транзакции выборки, файл
// снимается после commit (ADR-0004), GET отвечает штатный InvalidObjectState.
func TestJanitorEvictsExpiredRestore(t *testing.T) {
	h := newHarness(t)
	id := h.putObject(t, "jb", "restored.bin", []byte("данные после restore"))
	restoreToCache(t, h, id, "-1 second")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	jan := startJanitor(t, h, ctx)

	waitFor(t, "файл cache вытеснен", func() bool { return !h.cache.Exists(id) })
	var n int
	if err := h.m.Pool.QueryRow(context.Background(),
		`SELECT count(*) FROM restores WHERE object_id = $1`, id).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Error("строка restores осталась после вытеснения")
	}
	cancel()
	jan.Wait(h.cfg.Janitor.ShutdownGrace)

	_, err := h.be.GetObject(h.adminCtx(), &s3.GetObjectInput{
		Bucket: aws.String("jb"), Key: aws.String("restored.bin"),
	})
	if !isInvalidObjectState(err) {
		t.Errorf("GET после вытеснения restore: %v, ждали InvalidObjectState", err)
	}
}

// Живой restore (online_until в будущем) вытеснению не подлежит — обещание в
// x-amz-restore должно выполняться до последней секунды.
func TestJanitorKeepsLiveRestore(t *testing.T) {
	h := newHarness(t)
	id := h.putObject(t, "jb", "alive.bin", []byte("срок restore ещё идёт"))
	restoreToCache(t, h, id, "1 day")

	ctx, cancel := context.WithCancel(context.Background())
	jan := startJanitor(t, h, ctx)
	time.Sleep(300 * time.Millisecond)
	cancel()
	jan.Wait(h.cfg.Janitor.ShutdownGrace)

	if !h.cache.Exists(id) {
		t.Error("файл cache исчез до истечения online_until")
	}
	if _, err := h.be.GetObject(h.adminCtx(), &s3.GetObjectInput{
		Bucket: aws.String("jb"), Key: aws.String("alive.bin"),
	}); err != nil {
		t.Errorf("GET живого restore: %v, ждали успех", err)
	}
}
