package server

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestAutoExecutionPathAdvertisedButHTTPRemainsLocked(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r := request(s.Handler(), "GET", "/api/v1/bootstrap", "", "", "")
	if r.Code != http.StatusOK {
		t.Fatalf("bootstrap=%d %s", r.Code, r.Body.String())
	}
	var out struct {
		Automation     string `json:"automation"`
		TradingEnabled bool   `json:"trading_enabled"`
		Models         []struct {
			ID          string `json:"id"`
			Implemented bool   `json:"implemented"`
		} `json:"models"`
	}
	if err := json.Unmarshal(r.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.Automation != "controller_connected_protection_arming_locked" || out.TradingEnabled {
		t.Fatalf("unsafe bootstrap: %+v", out)
	}
	found := false
	for _, m := range out.Models {
		if m.ID == "ema_rsi_macd_v1" {
			found = true
			if !m.Implemented {
				t.Fatal("internal execution path still advertised unimplemented")
			}
		}
	}
	if !found {
		t.Fatal("model missing")
	}
	for _, path := range []string{"/api/v1/trading/auto/start", "/api/v1/trading/auto/stop", "/api/v1/trading/manual/orders"} {
		got := request(s.Handler(), "POST", path, s.csrfToken, "", `{}`)
		if got.Code != http.StatusLocked {
			t.Fatalf("%s unlocked: %d %s", path, got.Code, got.Body.String())
		}
	}
}
