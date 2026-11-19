package server

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"binance_trader/internal/trading"
)

func TestReconnectWatchAuthAndLockedHTTP(t *testing.T) {
	s := connectedAccountingServer(t)
	defer s.Close()
	calls := 0
	s.reconnectRunner = func(context.Context, trading.DemoCredentials, string, int64, time.Duration) (trading.ReconnectWatchReport, error) {
		calls++
		return trading.ReconnectWatchReport{}, context.Canceled
	}
	h := s.Handler()
	path := "/api/v1/recovery/reconnect-watch"
	valid := `{"passphrase":"strong-passphrase","lookback_hours":24,"duration_seconds":30}`
	for _, v := range []struct {
		body, token string
		want        int
	}{
		{valid, "invalid", 403},
		{`{"passphrase":"wrong-password","lookback_hours":24,"duration_seconds":30}`, s.csrfToken, 403},
		{`{"passphrase":"strong-passphrase","lookback_hours":24,"duration_seconds":10}`, s.csrfToken, 400},
		{`{"passphrase":"strong-passphrase","lookback_hours":24,"duration_seconds":30,"unknown":true}`, s.csrfToken, 400},
	} {
		resp := request(h, "POST", path, v.token, "", v.body)
		if resp.Code != v.want {
			t.Fatalf("unexpected request %d want %d %s", resp.Code, v.want, resp.Body.String())
		}
	}
	if calls != 0 {
		t.Fatal("unauthorized test reached private signer")
	}
	res := request(h, "POST", path, s.csrfToken, "", valid)
	if res.Code != http.StatusServiceUnavailable || calls != 1 {
		t.Fatalf("fake runner permitted %d %d", res.Code, calls)
	}
	stat := request(h, "GET", path+"/status", "", "", "")
	if stat.Code != 200 || !strings.Contains(stat.Body.String(), `"trading_authorized":false`) || !strings.Contains(stat.Body.String(), `"stream_continuity_proven":false`) {
		t.Fatalf("unsafe status %d %s", stat.Code, stat.Body)
	}
	for _, route := range []string{"/api/v1/trading/manual/orders", "/api/v1/trading/auto/start", "/api/v1/trading/auto/stop"} {
		if x := request(h, "POST", route, s.csrfToken, "", `{}`); x.Code != 423 {
			t.Fatalf("trade unlocked %s: %d", route, x.Code)
		}
	}
}
func TestReconnectWatchRequiresConnectedDemo(t *testing.T) {
	s, e := newWithFetcher(t.TempDir(), readDemoMock{})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	got := request(s.Handler(), "POST", "/api/v1/recovery/reconnect-watch", s.csrfToken, "", `{"passphrase":"abc","lookback_hours":24,"duration_seconds":30}`)
	if got.Code != 423 {
		t.Fatalf("unconnected Demo allowed %d", got.Code)
	}
}

func TestReconnectWatchSharesAccountDiagnosticMutex(t *testing.T) {
	s := connectedAccountingServer(t)
	defer s.Close()
	s.accountDiagnosticBusy.Store(true)
	defer s.accountDiagnosticBusy.Store(false)
	res := request(s.Handler(), "POST", "/api/v1/recovery/reconnect-watch", s.csrfToken, "", `{"passphrase":"strong-passphrase","lookback_hours":24,"duration_seconds":30}`)
	if res.Code != 409 {
		t.Fatalf("overlapping private diagnostics accepted %d", res.Code)
	}
	stat := request(s.Handler(), "GET", "/api/v1/recovery/reconnect-watch/status", "", "", "")
	if stat.Code != 200 || !strings.Contains(stat.Body.String(), `"diagnostic_busy":true`) {
		t.Fatalf("unsafe busy status %d %s", stat.Code, stat.Body)
	}
}

func TestReconnectWatchStatusSurfacesDurableLockWithoutStartRecord(t *testing.T) {
	dir := t.TempDir()
	s, e := newWithFetcher(dir, readDemoMock{})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	lock := filepath.Join(dir, "trading", "reconnect_watch_sessions", "exclusive.active")
	if e := os.MkdirAll(lock, 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(lock, "owner.json"), []byte(`{"schema":1,"id":"0123456789abcdef0123456789abcdef"}`), 0600); e != nil {
		t.Fatal(e)
	}
	got := request(s.Handler(), "GET", "/api/v1/recovery/reconnect-watch/status", "", "", "")
	if got.Code != 200 || !strings.Contains(got.Body.String(), `"diagnostic_running_or_stale_lock":true`) {
		t.Fatalf("stale reconnect lock hidden: %d %s", got.Code, got.Body.String())
	}
}
