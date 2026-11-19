package server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBoundedDriftStatusAlwaysHistoricalAndLocked(t *testing.T) {
	dir := t.TempDir()
	srv, err := newWithFetcher(dir, readDemoMock{})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	check := func() (int, string) {
		w := request(srv.Handler(), "GET", "/api/v1/trading/bounded-drift/status", "", "", "")
		return w.Code, w.Body.String()
	}
	if code, body := check(); code != 200 || !strings.Contains(body, `"report_count":0`) || !strings.Contains(body, `"continuous_coverage_proven":false`) || !strings.Contains(body, `"trading_authorized":false`) {
		t.Fatalf("historical status %d %s", code, body)
	}
	p := filepath.Join(dir, "trading", "bounded_windows")
	if err := os.MkdirAll(p, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(p, "broken"), []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if code, body := check(); code != 503 || !strings.Contains(body, "잠금") {
		t.Fatalf("corrupted archive accepted %d %s", code, body)
	}
	if w := request(srv.Handler(), "POST", "/api/v1/trading/manual/orders", "", "", `{}`); w.Code != 403 && w.Code != 423 {
		t.Fatalf("manual trading unexpectedly unlocked %d", w.Code)
	}
}
