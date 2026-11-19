package server

import (
	"binance_trader/internal/trading"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRecoveryStatusAlwaysLockedAndCorruptionFailsClosed(t *testing.T) {
	dir := t.TempDir()
	s, e := newWithFetcher(dir, readDemoMock{})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	r := request(s.Handler(), "GET", "/api/v1/recovery/status", "", "", "")
	if r.Code != 200 || !strings.Contains(r.Body.String(), `"trading_authorized":false`) || !strings.Contains(r.Body.String(), `"report_count":0`) {
		t.Fatalf("initial recovery status %d %s", r.Code, r.Body)
	}
	p, e := trading.OpenRecoveryProbeStore(dir)
	if e != nil {
		t.Fatal(e)
	}
	r = request(s.Handler(), "GET", "/api/v1/recovery/status", "", "", "")
	if r.Code != 200 || !strings.Contains(r.Body.String(), `"trading_authorized":false`) || !strings.Contains(r.Body.String(), `"diagnostic_running_or_stale_lock":true`) {
		t.Fatalf("active never ready %d %s", r.Code, r.Body)
	}
	if e = p.Close(); e != nil {
		t.Fatal(e)
	}
	dirPath := filepath.Join(dir, "trading", "recovery_sessions")
	if e = os.WriteFile(filepath.Join(dirPath, "unrecognized-file"), []byte("junk"), 0600); e != nil {
		t.Fatal(e)
	}
	r = request(s.Handler(), "GET", "/api/v1/recovery/status", "", "", "")
	if r.Code != 503 || !strings.Contains(r.Body.String(), "거래는 잠금") {
		t.Fatalf("corrupted status may mislead users: %d %s", r.Code, r.Body)
	}
}
