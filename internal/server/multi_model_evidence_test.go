package server

import (
	"encoding/json"
	"net/http"
	"testing"

	"binance_trader/internal/app"
	"binance_trader/internal/trading"
)

func TestMultiModelEvidenceHTTPReadOnlyAndRealLocked(t *testing.T) {
	s, err := newWithFetcher(t.TempDir(), currentWalletMock{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r := request(s.Handler(), "GET", "/api/v1/trading/multi-model/evidence", "", "", "")
	if r.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", r.Code, r.Body)
	}
	var v trading.MultiModelEvidenceStatus
	if err := json.Unmarshal(r.Body.Bytes(), &v); err != nil {
		t.Fatal(err)
	}
	if v.TradingAuthorized || v.ExecutionEnabled || v.HistoricalTradesVerified || v.OwnershipVerified || v.RiskPreviewReady || v.LocalSnapshotConsistent {
		t.Fatal(v)
	}
	if r := request(s.Handler(), "GET", "/api/v1/trading/multi-model/evidence?untrusted=1", "", "", ""); r.Code != 400 {
		t.Fatal(r.Code)
	}
	if r := request(s.Handler(), "POST", "/api/v1/trading/multi-model/evidence", "", "", `{}`); r.Code != 404 && r.Code != 405 {
		t.Fatal(r.Code)
	}
	if _, err := s.store.UpdateEnvironment(app.Real); err != nil {
		t.Fatal(err)
	}
	if r := request(s.Handler(), "GET", "/api/v1/trading/multi-model/evidence", "", "", ""); r.Code != 423 {
		t.Fatalf("real environment not locked: %d", r.Code)
	}
}
