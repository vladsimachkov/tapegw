package meta_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/testdb"
)

// lockNotAvailable — SQLSTATE lock_not_available: её получает `FOR UPDATE
// NOWAIT`, когда строку держит чужая транзакция.
const lockNotAvailable = "55P03"

const (
	// tenantLockWait — сколько журналируемая запись обязана ждать за локом на
	// строке тенанта. Здоровый путь записи эту строку не трогает и проходит за
	// миллисекунды; столько же ждал бы путь, который на тенанте блокируется.
	// Значение завышено: оно не ускоряет зелёный прогон, а только ограничивает
	// красный, чтобы тот не висел до таймаута `go test` без внятного сообщения.
	tenantLockWait = 5 * time.Second

	// lockTenant — числовой id testTenant: события считаются по числу.
	lockTenant = 1
)

// Строка тенанта не должна быть точкой, на которой журналируемые изменения
// выстраиваются в очередь. Это то самое свойство, из-за которого нумерацию
// событий перенесли на служебный id (миграция 0028, ADR-0023 поправка).
//
// Первый участник держит лок на строке тенанта, второй — обычная журналируемая
// запись другого объекта того же тенанта — обязан пройти, не оглядываясь на
// первого. Если путь записи снова залезет в строку тенанта (`UPDATE tenants …
// RETURNING`, как было до 0028, или `SELECT … FOR UPDATE`, или служебное поле
// вроде updated_at), второй встанет за первым и упрётся в таймаут. Ни скорость
// диска, ни число горутин здесь не участвуют: проверка зелёная на любом железе
// и красная на любом локале на строку тенанта — в отличие от отношения
// скоростей, которое на медленном окружении краснеет без вины (см. комментарий
// к throughputEnv).
//
// Почему блокада `FOR NO KEY UPDATE`, а не `FOR UPDATE`. У `journal_events`
// есть FK `tenant_id → tenants`, поэтому вставка события **сама** берёт на
// строку тенанта лок `FOR KEY SHARE` — видно по `pg_stat_activity`: под
// `FOR UPDATE` здоровая вставка стоит в очереди с `wait_event = transactionid`.
// Замерено на одной строке (вставка события, держатель на строке `tenants`
// 8 с): обычный `UPDATE` — прошла за 2.6 мс, `FOR NO KEY UPDATE` — за 2.3 мс,
// `FOR UPDATE` — ждала 6001 мс и упала по `lock_timeout`. То есть `FOR KEY
// SHARE` конфликтует только с `FOR UPDATE`, а с правкой строки — нет (затем
// `NO KEY UPDATE` и придуман: обычные UPDATE не мешают проверкам FK).
// Соответственно блокада ниже здоровому пути не мешает, но красная на любой
// настоящей правке строки тенанта: plain UPDATE берёт ровно `FOR NO KEY UPDATE`.
//
// Граница свойства — в точности то, что измерено: строка тенанта не в очереди
// журналируемых записей ни при каких штатных правках — ни обычный `UPDATE`
// (поворот KEK, правка лимита), ни `FOR NO KEY UPDATE`. Задержать их способен
// только `FOR UPDATE` на строке `tenants`; в коде шлюза его нет, и заводить не
// следует: на строку тенанта претендуют только проверки FK.
//
// Два участника обязательны: свойство «не ждут друг друга» проверяется только
// одновременно тем, кто лок держит, и тем, кто к нему лезет.
//
// Чего здесь НЕ проверяется: advisory-лок, ключеванный на тенанта (строкой
// `tenants` он не блокируется), и вообще любой лок, не связанный со строкой
// тенанта, — а также медленный путь записи сам по себе. Косвенный детектор для
// них один: отношение скоростей, и оно в CI выключено; включается
// `make throughput` (ADR-0023).
func TestJournalWriteNotBlockedByTenantRowLock(t *testing.T) {
	m := testdb.Connect(t)
	ctx := context.Background()
	testdb.Clean(t, m)
	mustLockBucket(t, m, "j-tenantlock")

	// Объекты заводятся до блокады: это объект, на который будет запись, а не
	// участник проверки, и держать его создание под локом не за чем.
	keep := insertVersion(t, m, "j-tenantlock", "keep", versionOpts{current: true})
	// Удаление берёт ключ, а не id версии: DeleteObject без versionId ставит
	// маркер на текущую версию.
	insertVersion(t, m, "j-tenantlock", "drop", versionOpts{current: true})
	until := time.Now().Add(24 * time.Hour)

	// Первый участник: держит лок на строке тенанта до конца теста.
	blocker, err := m.Pool.Begin(ctx)
	if err != nil {
		t.Fatalf("начать транзакцию-блокаду: %v", err)
	}
	defer blocker.Rollback(ctx) // nolint:errcheck
	var locked int64
	// Scan, а не Exec: `SELECT … FOR UPDATE` по несуществующей строке не ошибка,
	// и молча заблокировав ноль строк, тест стал бы зелёным ничего не проверяя.
	if err := blocker.QueryRow(ctx,
		`SELECT id FROM tenants WHERE id = $1 FOR NO KEY UPDATE`, lockTenant).Scan(&locked); err != nil {
		t.Fatalf("заблокировать строку тенанта %d: %v", lockTenant, err)
	}

	// Проверка самой блокады, до того как на неё что-то положилось: третья
	// транзакция обязана получить 55P03. Без этого зелёный прогон мог означать
	// «блокады нет», и тест ничего не проверял бы.
	//
	// Проба берёт ровно тот режим, конфликт с которым и есть смысл блокады, —
	// `FOR NO KEY UPDATE`, а не `FOR UPDATE`. Разница не косметическая: проба
	// посильнее (`FOR UPDATE`) прошла бы и на блокаде, ослабленной до
	// `FOR KEY SHARE`, — а такая блокада уже не замечает правку строки тенанта,
	// и тест молча перестал бы что-либо проверять. Проба ровно в меру: `KEY
	// SHARE`, которым здоровая вставка события проходит, её не видит.
	//
	// Вызывается дважды — до записей и после: если транзакция-блокада умерла в
	// середине цикла, записи проходили бы без очереди и тест стал бы зелёным,
	// ничего не проверяя.
	probeBlocked := func(when string) {
		t.Helper()
		probe, err := m.Pool.Begin(ctx)
		if err != nil {
			t.Fatalf("начать транзакцию-пробу (%s): %v", when, err)
		}
		defer probe.Rollback(ctx) // nolint:errcheck
		_, err = probe.Exec(ctx,
			`SELECT id FROM tenants WHERE id = $1 FOR NO KEY UPDATE NOWAIT`, lockTenant)
		var pe *pgconn.PgError
		if err == nil || !errors.As(err, &pe) || pe.Code != lockNotAvailable {
			t.Fatalf("строка тенанта %d не заблокирована правкой (%s; проба вернула %v, хотели %s): "+
				"зелёный прогон значил бы «блокады нет» — проверялось бы отсутствие "+
				"очереди за локом, которого нет, либо лока, которого недостаточно для "+
				"правки строки", lockTenant, when, err, lockNotAvailable)
		}
	}
	probeBlocked("до записей")

	before := journalCount(t, m, lockTenant)

	cases := []struct {
		name string
		act  func(context.Context) error
	}{
		{"продление retention", func(c context.Context) error {
			return m.PutObjectRetention(c, testTenant, "j-tenantlock", "keep", keep,
				"GOVERNANCE", until)
		}},
		{"массовое удаление (маркер на ключ)", func(c context.Context) error {
			_, err := m.DeleteObject(c, testTenant, meta.DeleteObjectInput{
				Bucket: "j-tenantlock", Key: "drop"})
			return err
		}},
		{"политика бакета", func(c context.Context) error {
			return m.SetBucketPolicy(c, testTenant, "j-tenantlock",
				[]byte(`{"Version":"2012-10-17"}`))
		}},
	}

	for i, c := range cases {
		waitCtx, cancel := context.WithTimeout(ctx, tenantLockWait)
		start := time.Now()
		err := c.act(waitCtx)
		elapsed := time.Since(start)
		cancel()

		if err != nil {
			if elapsed >= tenantLockWait {
				t.Fatalf("%s: запись не прошла, пока другой участник держит лок на строке "+
					"тенанта %d (ждала %v) — в путь записи вернулся лок на тенанта, и все "+
					"журналируемые изменения тенанта встанут в одну очередь за ним "+
					"(ADR-0023, поправка)", c.name, lockTenant, elapsed.Round(time.Second))
			}
			t.Fatalf("%s: %v", c.name, err)
		}
		t.Logf("  %d. %-38s %6v при заблокированной строке тенанта",
			i+1, c.name, elapsed.Round(time.Millisecond))
	}

	probeBlocked("после записей")

	// Зелёный не должен означать «запись прошла, но ничего не сделала»: каждое
	// изменение обязано оставить событие.
	if after := journalCount(t, m, lockTenant); after-before != int64(len(cases)) {
		t.Fatalf("событий %d, хотели %d: записи под блокадой прошли, но событий не оставили — "+
			"тест проверял не тот путь", after-before, len(cases))
	}
	// Второй пояс, а не детектор: при возврате `UPDATE tenants …` краснеет
	// прежде всего таймаут записи (проверено мутацией — с отключённой этой
	// проверкой тест всё равно красный). В тесте замера, наоборот, она единственный
	// ловец там, где отношение скоростей выключено.
	if seq := tenantJournalSeq(t, m, lockTenant); seq != 0 {
		t.Errorf("tenants.journal_seq = %d: путь записи пишет номер на строке тенанта, "+
			"и изменения тенанта снова сериализуются на ней (ADR-0023, поправка)", seq)
	}
}
