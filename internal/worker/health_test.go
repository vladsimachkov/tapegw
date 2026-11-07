package worker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/vladsimachkov/tapegw/internal/worker"
)

// Выключенная архивация обязана быть видна снаружи: шлюз при этом отвечает
// 200 на PUT, а pending растёт молча.

func TestReadinessReportsArchivation(t *testing.T) {
	for _, tc := range []struct {
		enabled bool
		want    string
	}{
		{true, "enabled"},
		{false, "disabled"},
	} {
		r, err := worker.StartReadiness("127.0.0.1:0", tc.enabled, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()

		resp, err := http.Get("http://" + r.Addr() + "/health/ready")
		if err != nil {
			t.Fatalf("enabled=%v: %v", tc.enabled, err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("enabled=%v: код %d", tc.enabled, resp.StatusCode)
		}
		var body map[string]string
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body["archivation"] != tc.want {
			t.Errorf("enabled=%v: archivation=%q, ждали %q", tc.enabled, body["archivation"], tc.want)
		}
		r.Close()
	}
}
