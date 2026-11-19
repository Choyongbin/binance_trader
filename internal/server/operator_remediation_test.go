package server

import (
	"strings"
	"testing"
)

func TestOperatorRemediationReadOnlyAndNeverAuthorizesTrading(t *testing.T) {
	s, err := newWithFetcher(t.TempDir(), readDemoMock{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r := request(s.Handler(), "GET", "/api/v1/trading/operator-remediation", "", "", "")
	if r.Code != 200 {
		t.Fatalf("status=%d body=%s", r.Code, r.Body.String())
	}
	body := r.Body.String()
	for _, want := range []string{`"state":"NO_ACTIVE_REMEDIATION"`, `"trading_authorized":false`, `"automatic_repair":false`, `"mutation_performed":false`, `"code":"NO_HISTORICAL_EVIDENCE"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %s in %s", want, body)
		}
	}
	if w := request(s.Handler(), "POST", "/api/v1/trading/manual/orders", "", "", `{}`); w.Code != 403 && w.Code != 423 {
		t.Fatalf("manual unexpectedly unlocked %d", w.Code)
	}
}

func TestOperatorRemediationRejectsQuery(t *testing.T) {
	s, err := newWithFetcher(t.TempDir(), readDemoMock{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if r := request(s.Handler(), "GET", "/api/v1/trading/operator-remediation?repair=1", "", "", ""); r.Code != 400 {
		t.Fatalf("query status=%d", r.Code)
	}
}
