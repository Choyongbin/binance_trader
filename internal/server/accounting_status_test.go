package server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAccountingStatusIsHistoricalAndManualAutoStillLocked(t *testing.T) {
	dir := t.TempDir()
	srv, err := newWithFetcher(dir, readDemoMock{})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	check := func() (int, string) {
		w := request(srv.Handler(), "GET", "/api/v1/trading/accounting/status", "", "", "")
		return w.Code, w.Body.String()
	}
	if code, body := check(); code != 200 || !strings.Contains(body, `"status":"NOT_RUN"`) || !strings.Contains(body, `"trading_authorized":false`) || !strings.Contains(body, `"actual_net_verified":false`) {
		t.Fatalf("empty status %d %s", code, body)
	}
	if w := request(srv.Handler(), "GET", "/api/v1/trading/accounting/status?inject=1", "", "", ""); w.Code != 400 {
		t.Fatal(w.Code)
	}
	if w := request(srv.Handler(), "POST", "/api/v1/trading/manual/orders", "", "", `{}`); w.Code != 403 && w.Code != 423 {
		t.Fatalf("manual route unexpectedly enabled: %d", w.Code)
	}
	if w := request(srv.Handler(), "POST", "/api/v1/trading/auto/start", "", "", `{}`); w.Code != 403 && w.Code != 423 {
		t.Fatalf("auto route unexpectedly enabled: %d", w.Code)
	}
	p := filepath.Join(dir, "trading", "accounting_reports")
	if err := os.MkdirAll(p, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(p, "unexpected"), []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if code, body := check(); code != 503 || !strings.Contains(body, "손상") {
		t.Fatalf("corruption %d %s", code, body)
	}
}
