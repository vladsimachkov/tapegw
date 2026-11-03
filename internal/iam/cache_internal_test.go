package iam

import (
	"testing"
	"time"

	"github.com/vladsimachkov/tapegw/internal/meta"
)

// Внутренний тест гонки «прочитали до инвалидации, пишем после». Из внешнего
// тестового пакета её не поймать: она требует вклиниться между чтением строки
// и её кладкой в кэш, а это две операции внутри одного вызова.

// Кладка обязана быть отброшена, если между чтением и записью пришла
// инвалидация. Иначе отзыв теряется: горутина прочитала активную учётку, админ
// её отозвал и сбросил кэш, горутина кладёт прочитанное — и отозванный ключ
// живёт ещё весь cache_ttl.
func TestStoreDropsValueReadBeforeInvalidation(t *testing.T) {
	c := &Cache{ttl: time.Hour, max: 10, entries: make(map[string]entry)}

	gen := c.generation()
	// Отзыв пришёл, пока мы «читали БД».
	c.InvalidateByAccess("alice")

	c.store("alice", entry{row: meta.Account{Access: "alice"}, expires: time.Now().Add(time.Hour)}, gen)

	if n := c.Len(); n != 0 {
		t.Errorf("устаревшее значение попало в кэш: записей %d", n)
	}
	if got := c.Stats().StaleDropped; got != 1 {
		t.Errorf("StaleDropped = %d, хотели 1 — без счётчика тест не отличал бы «отброшено» от «не успели»", got)
	}
}

// Если инвалидаций не было, кладка обязана пройти: иначе кэш был бы пуст всегда.
func TestStoreKeepsValueWhenNothingChanged(t *testing.T) {
	c := &Cache{ttl: time.Hour, max: 10, entries: make(map[string]entry)}

	gen := c.generation()
	c.store("alice", entry{row: meta.Account{Access: "alice"}, expires: time.Now().Add(time.Hour)}, gen)

	if n := c.Len(); n != 1 {
		t.Errorf("свежее значение не попало в кэш: записей %d", n)
	}
	if got := c.Stats().StaleDropped; got != 0 {
		t.Errorf("StaleDropped = %d, хотели 0", got)
	}
}

// Потолок записей: access_key выбирает клиент, и без потолка перебор случайных
// ключей раздувал бы память.
func TestStoreRespectsMax(t *testing.T) {
	c := &Cache{ttl: time.Hour, max: 2, entries: make(map[string]entry)}

	gen := c.generation()
	for i, access := range []string{"a", "b", "c"} {
		c.store(access, entry{row: meta.Account{Access: access}, expires: time.Now().Add(time.Hour)}, gen)
		if i >= 2 && c.Stats().Refused == 0 {
			t.Error("третья запись обязана быть отклонена, а счётчик — поднят")
		}
	}
	if n := c.Len(); n != 2 {
		t.Errorf("записей %d, хотели 2", n)
	}
}
