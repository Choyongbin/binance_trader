package server

import (
	"strings"
	"testing"
)

func TestOperatorIncidentTimelineReadOnly(t *testing.T) {
	s, err := newWithFetcher(t.TempDir(), readDemoMock{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r := request(s.Handler(), "GET", "/api/v1/trading/operator-incident-timeline", "", "", "")
	if r.Code != 200 {
		t.Fatalf("status=%d body=%s", r.Code, r.Body.String())
	}
	body := r.Body.String()
	for _, w := range []string{`"event_count":0`, `"trading_authorized":false`, `"automatic_repair":false`, `"mutation_performed":false`} {
		if !strings.Contains(body, w) {
			t.Fatalf("missing %s", w)
		}
	}
	if r := request(s.Handler(), "GET", "/api/v1/trading/operator-incident-timeline?x=1", "", "", ""); r.Code != 400 {
		t.Fatalf("query status=%d", r.Code)
	}
}
