package server

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"binance_trader/internal/trading"
)

func TestPrivateProbeStatusExposesOnlySanitizedHTTPUpgradeClassification(t *testing.T) {
	s, err := newWithFetcher(t.TempDir(), readDemoMock{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.vault.SaveDemo("demo-only-key", "demo-only-secret", "test-private-password"); err != nil {
		t.Fatal(err)
	}
	if err = s.demo.Connect(context.Background(), "demo-only-key", "demo-only-secret"); err != nil {
		t.Fatal(err)
	}
	s.privateProbeRunner = func(context.Context, string) trading.PrivateTransportResult {
		now := time.Now().UTC()
		return trading.PrivateTransportResult{
			Schema: 1, Status: "WS_HANDSHAKE_FAILED", StartedAt: now, FinishedAt: now.Add(time.Millisecond),
			EventTypes: []string{}, HistoricalOnly: true, Note: "일시적인 연결 진단만 수행했습니다. 과거 누락 이벤트, 실제 주문 상태 및 계정 연속성은 증명하지 않습니다.",
			DialRoute: "PRIVATE_PATH_LISTENKEY", DialPhase: "HTTP_STATUS", DialHTTPStatus: 403, DialElapsedMS: 13,
		}
	}
	h := s.Handler()
	got := request(h, "POST", "/api/v1/trading/private-probe/run", s.csrfToken, "", `{"passphrase":"test-private-password"}`)
	if got.Code != 200 || !strings.Contains(got.Body.String(), `"dial_route":"PRIVATE_PATH_LISTENKEY"`) || !strings.Contains(got.Body.String(), `"dial_phase":"HTTP_STATUS"`) || !strings.Contains(got.Body.String(), `"dial_http_status":403`) {
		t.Fatalf("unexpected probe: %v %s", got.Code, got.Body.String())
	}
	hist := request(h, "GET", "/api/v1/trading/private-probe/status", "", "", "")
	if hist.Code != 200 || !strings.Contains(hist.Body.String(), `"dial_route":"PRIVATE_PATH_LISTENKEY"`) || !strings.Contains(hist.Body.String(), `"dial_phase":"HTTP_STATUS"`) || !strings.Contains(hist.Body.String(), `"dial_http_status":403`) || !strings.Contains(hist.Body.String(), `"dial_elapsed_ms":13`) {
		t.Fatalf("unexpected status: %v %s", hist.Code, hist.Body.String())
	}
	for _, raw := range []string{got.Body.String(), hist.Body.String()} {
		if strings.Contains(raw, "demo-only-key") || strings.Contains(raw, "demo-only-secret") || strings.Contains(raw, "test-private-password") || strings.Contains(raw, "listenKey=") {
			t.Fatal("diagnostic leaked secrets")
		}
	}
	order := request(h, "POST", "/api/v1/trading/manual/orders", s.csrfToken, "", `{}`)
	if order.Code != http.StatusLocked {
		t.Fatalf("diagnostics unlocked order: %d", order.Code)
	}
}
