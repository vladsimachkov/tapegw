package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// Readiness — минимальный /health/ready на слушателе metrics.
//
// Отметка про архивацию нужна потому, что выключенный migrator не виден ни в
// каком S3-ответе: шлюз работает, PUT отвечает 200, а очередь pending растёт
// молча. Здравого смысла у versitygw-ского health-эндпоинта на S3-порту нет —
// он умеет только 200, — поэтому отметка живёт на отдельном слушателе.
//
// Полный metrics/health (M9) заменит этот кусок целиком; здесь ровно то, без
// чего «архивация выключена» превращается в тихую аварию.
type Readiness struct {
	srv *http.Server
	ln  net.Listener
}

// Addr — фактический адрес слушателя: при addr с портом 0 это выбранный ядром
// порт, без него тест не сходит на свой же эндпоинт.
func (r *Readiness) Addr() string { return r.ln.Addr().String() }

// StartReadiness поднимает сервер на addr. archivation — включена ли архивация
// (migrator.enabled); текст ответа прямо говорит «выключена», а не «ok».
func StartReadiness(addr string, archivation bool, log *slog.Logger) (*Readiness, error) {
	if log == nil {
		log = slog.Default()
	}
	state := "enabled"
	if !archivation {
		state = "disabled"
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health/ready", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status":      "ok",
			"archivation": state,
		})
	})
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Error("readiness: сервер лёг", slog.String("err", err.Error()))
		}
	}()
	return &Readiness{srv: srv, ln: ln}, nil
}

// Close останавливает сервер.
func (r *Readiness) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = r.srv.Shutdown(ctx)
}
