package server

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestTradingReadinessEndpointFailsClosedUntilSignedCapabilityExists(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r := request(s.Handler(), "GET", "/api/v1/trading/readiness", "", "", "")
	if r.Code != 200 {
		t.Fatalf("code=%d body=%s", r.Code, r.Body.String())
	}
	var out map[string]any
	if err := json.Unmarshal(r.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out["ready"] != false || out["trading_authorized"] != false || out["fresh_preflight_required"] != true || out["final_guard_required"] != true {
		t.Fatalf("unsafe readiness: %+v", out)
	}
	if !strings.Contains(r.Body.String(), "ACCOUNT_CAPABILITY_UNVERIFIED") && !strings.Contains(r.Body.String(), "MARKET_NOT_READY") {
		t.Fatalf("missing authoritative blocker: %s", r.Body.String())
	}
	if q := request(s.Handler(), "GET", "/api/v1/trading/readiness?force=1", "", "", ""); q.Code != 400 {
		t.Fatalf("query accepted: %d", q.Code)
	}
}
