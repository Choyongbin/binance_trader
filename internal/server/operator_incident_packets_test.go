package server

import (
	"strings"
	"testing"
)

func TestOperatorIncidentPacketsReadOnlyEmpty(t *testing.T) {
	s, err := newWithFetcher(t.TempDir(), readDemoMock{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r := request(s.Handler(), "GET", "/api/v1/trading/operator-incident-packets", "", "", "")
	if r.Code != 200 {
		t.Fatalf("status=%d body=%s", r.Code, r.Body.String())
	}
	body := r.Body.String()
	for _, want := range []string{`"critical_packet_count":0`, `"trading_authorized":false`, `"automatic_repair":false`, `"mutation_performed":false`} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %s in %s", want, body)
		}
	}
}

func TestOperatorIncidentPacketsRejectsQuery(t *testing.T) {
	s, err := newWithFetcher(t.TempDir(), readDemoMock{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if r := request(s.Handler(), "GET", "/api/v1/trading/operator-incident-packets?write=1", "", "", ""); r.Code != 400 {
		t.Fatalf("query status=%d", r.Code)
	}
}
