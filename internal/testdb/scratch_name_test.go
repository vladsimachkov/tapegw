package testdb

import (
	"strings"
	"testing"
)

// Имена схем не должны сталкиваться после усечения.
//
// Postgres обрезает имя идентификатора до 63 байт молча (только NOTICE), а
// `go test` печатает NOTICE как информационное сообщение. Два теста с общим
// длинным началом имени получили бы одну схему на двоих — при параллельном
// запуске один удалит схему из-под другого.
func TestScratchNameNoCollision(t *testing.T) {
	long := "testmigration0018restrictkeepsjournalrowswhenmediadeleted"
	a := scratchName("tapegw_test_meta", 18, long+"first")
	b := scratchName("tapegw_test_meta", 18, long+"second")
	if a == b {
		t.Fatalf("имена схем совпали: %s", a)
	}
	for _, n := range []string{a, b} {
		if len(n) > maxIdent {
			t.Errorf("имя схемы %q в %d байт — Postgres усекает на %d", n, len(n), maxIdent)
		}
	}
	if !strings.HasPrefix(a, "tapegw_test_meta_mig18_") {
		t.Errorf("имя %q потеряло префикс — по нему ищутся остатки прогонов", a)
	}

	// Короткие имена остаются читаемыми целиком: отладка по имени схемы.
	short := scratchName("tapegw_test_meta", 7, "testmigration0007downandup")
	if short != "tapegw_test_meta_mig07_testmigration0007downandup" {
		t.Errorf("короткое имя переписано без нужды: %q", short)
	}
}
