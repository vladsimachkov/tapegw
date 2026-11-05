package keys_test

import (
	"testing"
	"time"

	"github.com/vladsimachkov/tapegw/internal/keys"
)

// TestCanForget — решение «можно ли выбросить версию KEK».
//
// Здесь проверяется не арифметика, а то, что недоказуемое «срок прошёл» даёт
// отказ. Каждый случай — способ, которым оператор может промахнуться:
// активная версия, неизвестный момент уступки, незаданная глубина бэкапов,
// часы в будущем.
func TestCanForget(t *testing.T) {
	old := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	future := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	const (
		month   = 30 * 24 * time.Hour
		quarter = 90 * 24 * time.Hour
	)

	cases := []struct {
		name    string
		check   keys.ForgetCheck
		wantErr bool
	}{
		{
			name:    "активная версия",
			check:   keys.ForgetCheck{Version: 2, Active: 2, SupersededAt: &old, ExportRetention: month, BackupRetention: month, Now: future},
			wantErr: true,
		},
		{
			name:    "PITR не указан",
			check:   keys.ForgetCheck{Version: 1, Active: 2, SupersededAt: &old, ExportRetention: month, BackupRetention: 0, Now: future},
			wantErr: true,
		},
		{
			name:    "момент уступки неизвестен",
			check:   keys.ForgetCheck{Version: 1, Active: 2, SupersededAt: nil, ExportRetention: month, BackupRetention: month, Now: future},
			wantErr: true,
		},
		{
			name: "срок не прошёл по экспорту",
			check: keys.ForgetCheck{Version: 1, Active: 2,
				SupersededAt:    ptrTime(future.Add(-5 * 24 * time.Hour)),
				ExportRetention: month, BackupRetention: month, Now: future},
			wantErr: true,
		},
		{
			name: "бэкапы длиннее экспорта решают",
			check: keys.ForgetCheck{Version: 1, Active: 2,
				// прошло 45 суток: по экспорту (30) уже можно, по PITR (90) — нет
				SupersededAt:    ptrTime(future.Add(-45 * 24 * time.Hour)),
				ExportRetention: month, BackupRetention: quarter, Now: future},
			wantErr: true,
		},
		{
			name: "просрочено в будущем — часы врут",
			check: keys.ForgetCheck{Version: 1, Active: 2,
				SupersededAt: &future, ExportRetention: month, BackupRetention: month,
				Now: future.Add(-time.Hour)},
			wantErr: true,
		},
		{
			name: "срок прошёл",
			check: keys.ForgetCheck{Version: 1, Active: 2,
				SupersededAt:    ptrTime(future.Add(-31 * 24 * time.Hour)),
				ExportRetention: month, BackupRetention: month, Now: future},
			wantErr: false,
		},
		{
			name: "срок прошёл по худшему хранилищу",
			check: keys.ForgetCheck{Version: 1, Active: 2,
				SupersededAt:    ptrTime(future.Add(-91 * 24 * time.Hour)),
				ExportRetention: month, BackupRetention: quarter, Now: future},
			wantErr: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := keys.CanForget(c.check)
			if (err != nil) != c.wantErr {
				t.Fatalf("ошибка %v, хотели wantErr=%v", err, c.wantErr)
			}
		})
	}
}

func ptrTime(t time.Time) *time.Time { return &t }
