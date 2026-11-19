package server

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"binance_trader/internal/trading"
)

func TestLocalReadinessAlwaysAdvisory(t *testing.T) {
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	get := func() map[string]any {
		t.Helper()
		r := request(s.Handler(), "GET", "/api/v1/trading/local-readiness", "", "", "")
		if r.Code != 200 {
			t.Fatalf("diagnostic GET: %d %s", r.Code, r.Body)
		}
		var out map[string]any
		if err := json.Unmarshal(r.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		if out["trading_authorized"] != false || out["wallet_edit_authorized"] != false || out["signed_whole_account_audit_required"] != true {
			t.Fatalf("diagnostic granted unsafe privilege: %+v", out)
		}
		return out
	}
	clean := get()
	if clean["status"] != "SIGNED_DEMO_AUDIT_REQUIRED" {
		t.Fatal(clean)
	}
	lock := filepath.Join(root, "trading", "coordinator.active")
	if err := os.Mkdir(lock, 0700); err != nil {
		t.Fatal(err)
	}
	if got := get(); got["status"] != "LOCK_PRESENT_MANUAL_REVIEW" || got["coordinator_lock_present"] != true {
		t.Fatal(got)
	}
	if err := os.Remove(lock); err != nil {
		t.Fatal(err)
	}
	gate, err := trading.AcquireMutationGate(root)
	if err != nil {
		t.Fatal(err)
	}
	if got := get(); got["status"] != "LOCK_PRESENT_MANUAL_REVIEW" || got["mutation_gate_present"] != true {
		t.Fatal(got)
	}
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	if r := request(s.Handler(), "GET", "/api/v1/trading/local-readiness?danger=1", "", "", ""); r.Code != 400 {
		t.Fatalf("unexpected query accepted: %d", r.Code)
	}
}
