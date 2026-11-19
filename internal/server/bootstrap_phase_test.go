package server

import (
	"encoding/json"
	"testing"

	"binance_trader/internal/version"
)

func TestBootstrapPhaseUsesVersionSource(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	b := request(s.Handler(), "GET", "/api/v1/bootstrap", "", "", "")
	if b.Code != 200 {
		t.Fatalf("bootstrap status %d", b.Code)
	}
	var payload struct {
		Phase          int    `json:"phase"`
		PhaseName      string `json:"phase_name"`
		Release        string `json:"release"`
		TradingEnabled bool   `json:"trading_enabled"`
	}
	if err := json.Unmarshal(b.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Phase != version.PhaseMajor || payload.PhaseName != version.Phase || payload.Release != version.Release || payload.TradingEnabled {
		t.Fatalf("bootstrap mismatches release and safety state: %+v", payload)
	}
}
