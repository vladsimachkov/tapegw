package mover_test

import (
	"testing"

	"github.com/vladsimachkov/tapegw/internal/mover"
)

// TestMoverURIFormat пинит формат SAN-URI из ADR-0021: `mover://<драйв>`,
// без библиотеки. Формат зашит в выпущенные сертификаты — поменять его
// задним числом нельзя, поэтому разъезд молча (например «добавим библиотеку
// через слэш») стоит дороже красного теста на опечатку в функции.
//
// Клиент и сервер строят ожидание одной MoverURI (самопроверка mover'а в
// cmd/tapegw-mover и проверка peer в ClientTLS), так что пин здесь закрывает
// обе стороны сразу.
func TestMoverURIFormat(t *testing.T) {
	for _, drive := range []string{"drive-A", "drive_1"} {
		if got := mover.MoverURI(drive).String(); got != "mover://"+drive {
			t.Errorf("MoverURI(%q) = %q, ждали mover://%s (ADR-0021)", drive, got, drive)
		}
	}
	if got := mover.ClientURI(mover.ClientScheduler).String(); got != "tapegw://scheduler" {
		t.Errorf("ClientURI(scheduler) = %q", got)
	}
}
