package server

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRestartForensicsGETSafeNoSideEffects(t *testing.T) {
	s, err := newWithFetcher(t.TempDir(), readDemoMock{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	h := s.Handler()
	path := "/api/v1/recovery/restart-forensics/status"
	get := request(h, "GET", path, "", "", "")
	if get.Code != http.StatusOK || !strings.Contains(get.Body.String(), `"restart_disposition":"NO_MONITOR_HISTORY"`) ||
		!strings.Contains(get.Body.String(), `"auto_rearm_allowed":false`) ||
		!strings.Contains(get.Body.String(), `"trading_authorized":false`) ||
		!strings.Contains(get.Body.String(), `"stream_continuity_proven":false`) {
		t.Fatalf("unsafe forensic GET %d: %s", get.Code, get.Body)
	}
	if noQuery := request(h, "GET", path+"?secret=bad", "", "", ""); noQuery.Code != http.StatusBadRequest {
		t.Fatal(noQuery.Code)
	}
	if resp := request(h, "POST", path, s.csrfToken, "", `{}`); resp.Code != http.StatusMethodNotAllowed && resp.Code != http.StatusNotFound {
		t.Fatalf("forensic endpoint mutates %d", resp.Code)
	}
	for _, order := range []string{"/api/v1/trading/manual/orders", "/api/v1/trading/auto/start", "/api/v1/trading/auto/stop"} {
		if resp := request(h, "POST", order, s.csrfToken, "", `{}`); resp.Code != http.StatusLocked {
			t.Fatalf("order unlocked %s %d", order, resp.Code)
		}
	}
}

func TestRestartForensicsHTTPRejectsTamperedLocalEvidence(t *testing.T) {
	s, err := newWithFetcher(t.TempDir(), readDemoMock{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	gap := filepath.Join(s.dataDir, "trading", "stream_discontinuities")
	if err = os.MkdirAll(gap, 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(gap, strings.Repeat("a", 32)+".json"), []byte("garbageSECRET"), 0600); err != nil {
		t.Fatal(err)
	}
	got := request(s.Handler(), "GET", "/api/v1/recovery/restart-forensics/status", "", "", "")
	if got.Code != http.StatusServiceUnavailable || strings.Contains(got.Body.String(), "garbageSECRET") {
		t.Fatalf("unsafe forensic HTTP failure: %d %s", got.Code, got.Body)
	}
}
