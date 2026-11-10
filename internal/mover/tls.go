package mover

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
)

// Ограничение по TLS 1.3 обязательно: mover возит шифротекст через сеть, где
// раньше был один unix socket, и падение до 1.2 означало бы молча потерять и
// шифры, и пост-квантовый handshake.
const MinTLSVersion = tls.VersionTLS13

// MoverDNSName — общее имя в DNS SAN всех сертификатов mover'ов. Нужно только
// для стандартной проверки имени: настоящая идентичность — в SAN-URI
// (MoverURI), а слушать mover может на любом адресе.
const MoverDNSName = "tapegw-mover"

// ClientID — роль стороны tapegw, зашитая в SAN-URI клиентского сертификата.
//
// Роль нельзя брать из hello: её объявляет сам подключающийся, поэтому
// проверка роли из сообщения не проверяет ничего. Идентичность берётся из
// сертификата — иначе сертификат любого mover'а от того же CA годится, чтобы
// подключиться к соседнему mover'у клиентом и писать на ленту.
type ClientID string

const (
	// ClientScheduler — планировщик архивации (migrator).
	ClientScheduler ClientID = "scheduler"
	// ClientRestorer — планировщик восстановления (recaller).
	ClientRestorer ClientID = "restorer"
	// ClientDeleter — удалитель (M6). Роль отдельная не для порядка: purge
	// удаляет целый архивный объект, и сертификат с такой правами не должен
	// доставаться планировщику архивации или восстановлению. Разделение ролей
	// — единственное, что отличает «снять неполную копию» от «уничтожить
	// архив», и держаться оно должно на сертификате, а не на аккуратности
	// вызывающего кода.
	ClientDeleter ClientID = "deleter"
)

// ClientURI — SAN-URI клиентского сертификата tapegw.
func ClientURI(id ClientID) *url.URL { return &url.URL{Scheme: "tapegw", Host: string(id)} }

// MoverURI — SAN-URI сертификата mover'а: имя драйва, к которому он привязан.
// Клиент обязан проверить, что подключился именно к ожидаемому mover'у, иначе
// «записать на драйв A» уедет на драйв B.
func MoverURI(drive string) *url.URL { return &url.URL{Scheme: "mover", Host: drive} }

func loadPool(caFile string) (*x509.CertPool, error) {
	pemCA, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read ca: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemCA) {
		return nil, errors.New("mover: в файле CA нет сертификата")
	}
	return pool, nil
}

// peerURIs возвращает SAN-URI листа и проверяет назначенное использование.
//
// Пустое расширение есть отказ. Go сертификат без ExtKeyUsage считает годным на
// любое назначение, поэтому «клиентский» сертификат без ClientAuth прошёл бы и
// как серверный, и наоборот; проверка назначения обязана быть второй линией
// после SAN-URI, а не первой и не единственной.
func peerURIs(rawCerts [][]byte, wantExt x509.ExtKeyUsage) ([]*url.URL, error) {
	if len(rawCerts) == 0 {
		return nil, errors.New("mover: сертификат не предъявлен")
	}
	cert, err := x509.ParseCertificate(rawCerts[0])
	if err != nil {
		return nil, fmt.Errorf("mover: разбор сертификата: %w", err)
	}
	found := false
	for _, u := range cert.ExtKeyUsage {
		if u == wantExt {
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("mover: сертификат не назначен для %v", wantExt)
	}
	return cert.URIs, nil
}

func hasURI(uris []*url.URL, want *url.URL) bool {
	for _, u := range uris {
		if strings.EqualFold(u.String(), want.String()) {
			return true
		}
	}
	return false
}

// ServerTLS — конфиг mover'а: слушатель принимает только клиентов, чей
// сертификат подписан доверенным CA, помечен как клиентский и содержит SAN-URI
// одной из разрешённых ролей tapegw.
func ServerTLS(certFile, keyFile, caFile string, allowed ...ClientID) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("mover: пара сервера: %w", err)
	}
	return serverTLS(cert, caFile, allowed...)
}

func serverTLS(cert tls.Certificate, caFile string, allowed ...ClientID) (*tls.Config, error) {
	pool, err := loadPool(caFile)
	if err != nil {
		return nil, err
	}
	if len(allowed) == 0 {
		return nil, errors.New("mover: не указана ни одна разрешённая роль клиента")
	}
	want := make([]*url.URL, 0, len(allowed))
	for _, id := range allowed {
		want = append(want, ClientURI(id))
	}
	return &tls.Config{
		MinVersion:   MinTLSVersion,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
		Certificates: []tls.Certificate{cert},
		VerifyPeerCertificate: func(raw [][]byte, _ [][]*x509.Certificate) error {
			uris, err := peerURIs(raw, x509.ExtKeyUsageClientAuth)
			if err != nil {
				return err
			}
			for _, w := range want {
				if hasURI(uris, w) {
					return nil
				}
			}
			return errors.New("mover: сертификат клиента не относится к разрешённой роли")
		},
	}, nil
}

// ClientTLS — конфиг tapegw как клиента: проверяет цепочку mover'а, что
// подключились именно к mover'у (ServerAuth) и именно к ожидаемому драйву
// (SAN-URI), и не даёт подменить mover'а сертификатом другого сервиса от того
// же CA.
func ClientTLS(certFile, keyFile, caFile, drive string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("mover: клиентская пара: %w", err)
	}
	return clientTLS(cert, caFile, drive)
}

func clientTLS(cert tls.Certificate, caFile, drive string) (*tls.Config, error) {
	pool, err := loadPool(caFile)
	if err != nil {
		return nil, err
	}
	want := MoverURI(drive)
	return &tls.Config{
		MinVersion:   MinTLSVersion,
		ServerName:   MoverDNSName,
		RootCAs:      pool,
		Certificates: []tls.Certificate{cert},
		VerifyPeerCertificate: func(raw [][]byte, _ [][]*x509.Certificate) error {
			uris, err := peerURIs(raw, x509.ExtKeyUsageServerAuth)
			if err != nil {
				return err
			}
			if !hasURI(uris, want) {
				return fmt.Errorf("mover: подключились не к %s", want)
			}
			return nil
		},
	}, nil
}
