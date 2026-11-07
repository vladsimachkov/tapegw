package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/vladsimachkov/tapegw/internal/mover/movertest"
)

// checkDriveIdentity ловит перекос идентичности на старте: процесс,
// настроенный на drive-A, с сертификатом drive-B принял бы запись не на свой
// драйв, и клиент по своему ожиданию этого не поймает.
func TestCheckDriveIdentity(t *testing.T) {
	p := movertest.NewPKI(t)

	if err := checkDriveIdentity(p.ServerCert, p.Drive); err != nil {
		t.Fatalf("свой сертификат отклонён: %v", err)
	}
	if err := checkDriveIdentity(p.ServerCert, p.OtherDrive); err == nil {
		t.Fatal("сертификат чужого драйва принят")
	}

	bad := filepath.Join(t.TempDir(), "bad.crt")
	if err := os.WriteFile(bad, []byte("не PEM"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := checkDriveIdentity(bad, p.Drive); err == nil {
		t.Fatal("битый файл принят")
	}
	if err := checkDriveIdentity(filepath.Join(t.TempDir(), "нет.crt"), p.Drive); err == nil {
		t.Fatal("несуществующий файл принят")
	}
}
