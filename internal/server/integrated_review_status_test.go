package server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestIntegratedStatusReadOnlyFailsClosed(t *testing.T) {
	dir := t.TempDir()
	srv, e := newWithFetcher(dir, readDemoMock{})
	if e != nil {
		t.Fatal(e)
	}
	defer srv.Close()
	get := func() (int, string) {
		w := request(srv.Handler(), "GET", "/api/v1/trading/integrated-review/status", "", "", "")
		return w.Code, w.Body.String()
	}
	if code, body := get(); code != 200 || !strings.Contains(body, `"report_count":0`) || !strings.Contains(body, `"trading_authorized":false`) || !strings.Contains(body, `"historical_only":true`) {
		t.Fatalf("bad status: %d %s", code, body)
	}
	if e = os.MkdirAll(filepath.Join(dir, "trading", "integrated_reviews"), 0700); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(dir, "trading", "integrated_reviews", "bad"), []byte(`{}`), 0600); e != nil {
		t.Fatal(e)
	}
	if code, body := get(); code != 503 || !strings.Contains(body, "잠금") {
		t.Fatalf("corruption should fail closed: %d %s", code, body)
	}
	if w := request(srv.Handler(), "POST", "/api/v1/trading/manual/orders", "", "", `{}`); w.Code != 403 && w.Code != 423 {
		t.Fatalf("live order unlocked: %d", w.Code)
	}
}
