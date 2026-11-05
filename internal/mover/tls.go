package mover

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// Взаимный TLS обязателен всегда, включая деплой «всё на одном хосте»:
// отдельного режима «без шифрования» в этом пакете нет, поэтому и получить
// «забыли включить на боевом» нельзя — некуда.

// ServerTLS — конфиг mover'а: свой сертификат и CA, которой подписан клиент.
func ServerTLS(certFile, keyFile, caFile string) (*tls.Config, error) {
	pool, err := loadPool(caFile)
	if err != nil {
		return nil, err
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("mover: сертификат сервера: %w", err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		// Без проверки клиентского сертификата любой в сети записал бы в архив
		// мусор или прочитал шифротекст с обёрнутыми DEK.
		ClientAuth: tls.RequireAndVerifyClientCert,
		MinVersion: tls.VersionTLS13,
	}, nil
}

// ClientTLS — конфиг tapegw: свой сертификат и CA, которой подписан mover.
// ServerName задаётся явно: «доверяем любому из пула» — это не аутентификация.
func ClientTLS(certFile, keyFile, caFile, serverName string) (*tls.Config, error) {
	pool, err := loadPool(caFile)
	if err != nil {
		return nil, err
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("mover: сертификат клиента: %w", err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ServerName:   serverName,
		MinVersion:   tls.VersionTLS13,
	}, nil
}

func loadPool(caFile string) (*x509.CertPool, error) {
	b, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("mover: CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(b) {
		return nil, fmt.Errorf("mover: CA: в %s нет сертификата", caFile)
	}
	return pool, nil
}
