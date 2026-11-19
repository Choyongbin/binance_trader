package server

import (
	"binance_trader/internal/version"
	"strings"
	"testing"
)

func TestCumulativeVersionAndRoutesStayLocked(t *testing.T) {
	s, err := newWithFetcher(t.TempDir(), readDemoMock{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	h := s.Handler()
	reply := request(h, "GET", "/api/v1/version", "", "", "")
	if reply.Code != 200 || !strings.Contains(reply.Body.String(), version.Release) || !strings.Contains(reply.Body.String(), `"trading":"LOCKED"`) {
		t.Fatalf("version endpoint %d %s", reply.Code, reply.Body)
	}
	for _, path := range []string{"/api/v1/trading/manual/orders", "/api/v1/trading/auto/start", "/api/v1/trading/auto/stop"} {
		csrf := bootstrapToken(t, h)
		r := request(h, "POST", path, csrf, "http://127.0.0.1:8080", `{}`)
		if r.Code != 423 {
			t.Fatalf("%s unexpectedly enabled: %d %s", path, r.Code, r.Body)
		}
	}
}
