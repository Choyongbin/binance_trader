package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"binance_trader/internal/trading"
)

func TestProductProtectionWatchStatusReadOnlyLockedOnFreshServer(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.protectionWatcher == nil {
		t.Fatal("server did not construct process-lifetime protection watcher")
	}
	req := httptest.NewRequest(http.MethodGet, "http://127.0.0.1/api/v1/trading/protection/watch-status", nil)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("watch status HTTP %d: %s", w.Code, w.Body.String())
	}
	var got trading.ProductProtectionWatchStatus
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.State != trading.ProtectionWatchNotStarted || got.Running || got.TradingAuthorized ||
		got.SubmitAuthorized || got.CancelAuthorized || got.SettlementAuthorized || !got.ManualReviewRequired {
		t.Fatalf("fresh watcher claimed protection: %+v", got)
	}
	bad := httptest.NewRequest(http.MethodGet, "http://127.0.0.1/api/v1/trading/protection/watch-status?refresh=true", nil)
	w = httptest.NewRecorder()
	s.Handler().ServeHTTP(w, bad)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("watcher accepted a refresh/query mutation: %d", w.Code)
	}
}
