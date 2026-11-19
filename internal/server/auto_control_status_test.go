package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"binance_trader/internal/trading"
)

func TestAutoControlStatusIsReadOnlyAndRoutesRemainLocked(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r := httptest.NewRequest(http.MethodGet, "/api/v1/trading/auto/status", nil)
	r.Host = "localhost"
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var out struct {
		Control           trading.AutoRunStatus `json:"control"`
		TradingEnabled    bool                  `json:"trading_enabled"`
		AutomaticResume   bool                  `json:"automatic_resume"`
		MutationPerformed bool                  `json:"mutation_performed"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.Control.State != trading.AutoRunStopped || out.TradingEnabled || out.AutomaticResume || out.MutationPerformed {
		t.Fatalf("unexpected %+v", out)
	}
	for _, path := range []string{"/api/v1/trading/auto/start", "/api/v1/trading/auto/stop"} {
		resp := request(s.Handler(), "POST", path, s.csrfToken, "", `{}`)
		if resp.Code != http.StatusLocked {
			t.Fatalf("%s=%d", path, resp.Code)
		}
	}
}

func TestAutoControlStatusUsesServerOwnedInstance(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	lease, err := s.autoControl.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer s.autoControl.Stop()
	if lease.Generation == 0 {
		t.Fatal("missing lease")
	}
	r := httptest.NewRequest(http.MethodGet, "/api/v1/trading/auto/status", nil)
	r.Host = "localhost"
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d %s", w.Code, w.Body.String())
	}
	var out struct {
		Control trading.AutoRunStatus `json:"control"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.Control.State != trading.AutoRunRunning {
		t.Fatalf("expected RUNNING, got %+v", out.Control)
	}
}
