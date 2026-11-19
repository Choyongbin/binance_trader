package server

import (
	"binance_trader/internal/trading"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStreamGapStatusReadOnlyAndFailClosed(t *testing.T) {
	root := t.TempDir()
	srv, err := newWithFetcher(root, readDemoMock{})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	path := "/api/v1/trading/stream-gaps/status"
	r := request(srv.Handler(), "GET", path, "", "", "")
	if r.Code != http.StatusOK || !strings.Contains(r.Body.String(), `"marker_count":0`) || !strings.Contains(r.Body.String(), `"trading_authorized":false`) || !strings.Contains(r.Body.String(), `"manual_review_required":true`) {
		t.Fatalf("empty evidence response: %d %s", r.Code, r.Body)
	}
	if bad := request(srv.Handler(), "GET", path+"?debug=1", "", "", ""); bad.Code != 400 {
		t.Fatalf("query accepted %d", bad.Code)
	}
	store, e := trading.OpenEventStore(root)
	if e != nil {
		t.Fatal(e)
	}
	if e = store.RecordDiscontinuity("STARTUP"); e != nil {
		t.Fatal(e)
	}
	if e = store.Close(); e != nil {
		t.Fatal(e)
	}
	r = request(srv.Handler(), "GET", path, "", "", "")
	if r.Code != 200 || !strings.Contains(r.Body.String(), `"marker_count":1`) || !strings.Contains(r.Body.String(), `"outside_historical_sessions":1`) || !strings.Contains(r.Body.String(), `"trading_authorized":false`) {
		t.Fatalf("gap response %d %s", r.Code, r.Body)
	}
	if e = os.WriteFile(filepath.Join(root, "trading", "stream_discontinuities", "unexpected"), []byte("broken"), 0600); e != nil {
		t.Fatal(e)
	}
	r = request(srv.Handler(), "GET", path, "", "", "")
	if r.Code != 503 || strings.Contains(r.Body.String(), `"marker_count":0`) {
		t.Fatalf("corruption must fail closed %d %s", r.Code, r.Body)
	}
	for _, orderPath := range []string{"/api/v1/trading/manual/orders", "/api/v1/trading/auto/start", "/api/v1/trading/auto/stop"} {
		z := request(srv.Handler(), "POST", orderPath, srv.csrfToken, "", `{}`)
		if z.Code != 423 {
			t.Fatalf("unexpected unlocked %s %d", orderPath, z.Code)
		}
	}
}
