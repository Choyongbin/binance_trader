package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"binance_trader/internal/trading"
)

func TestContinuousMonitorOperatorOnlyLockedEndpoints(t *testing.T) {
	s := connectedAccountingServer(t)
	defer s.Close()
	h := s.Handler()
	path := "/api/v1/recovery/continuous-monitor"
	for _, test := range []struct {
		payload, csrf string
		code          int
	}{
		{`{"passphrase":"strong-passphrase","lookback_hours":24}`, "bad", 403},
		{`{"passphrase":"wrong","lookback_hours":24}`, s.csrfToken, 403},
		{`{"passphrase":"strong-passphrase","lookback_hours":6}`, s.csrfToken, 400},
		{`{"passphrase":"strong-passphrase","lookback_hours":24,"extra":true}`, s.csrfToken, 400},
	} {
		got := request(h, "POST", path+"/start", test.csrf, "", test.payload)
		if got.Code != test.code {
			t.Fatalf("unexpected auth/validation: %d want %d: %s", got.Code, test.code, got.Body)
		}
	}
	begin := make(chan struct{})
	s.monitorRunner = func(ctx context.Context, _ trading.DemoCredentials, _ string, _ time.Duration, cb func(trading.MonitorCheckpoint)) (trading.MonitorCheckpoint, error) {
		now := time.Now().UTC()
		cp := trading.MonitorCheckpoint{Schema: 1, State: trading.MonitorConnected, Epoch: 1, Issues: []string{"manual review required"}, ManualReviewRequired: true, StreamContinuityProven: true, ContinuityScope: "PROCESS_LOCAL_CHECKPOINT_TO_NOW", ContinuitySince: now, ContinuityGapDigest: strings.Repeat("a", 64), GapDigest: strings.Repeat("a", 64), LastSignedRESTAt: now}
		cb(cp)
		close(begin)
		<-ctx.Done()
		cp.State = trading.MonitorStopped
		cb(cp)
		return cp, nil
	}
	got := request(h, "POST", path+"/start", s.csrfToken, "", `{"passphrase":"strong-passphrase","lookback_hours":24}`)
	if got.Code != 202 || !strings.Contains(got.Body.String(), `"trading_authorized":false`) {
		t.Fatalf("bad start %d %s", got.Code, got.Body)
	}
	select {
	case <-begin:
	case <-time.After(2 * time.Second):
		t.Fatal("runner failed to start")
	}
	if again := request(h, "POST", path+"/start", s.csrfToken, "", `{"passphrase":"strong-passphrase","lookback_hours":24}`); again.Code != 409 {
		t.Fatalf("parallel monitor permitted %d", again.Code)
	}
	if other := request(h, "POST", "/api/v1/recovery/reconnect-watch", s.csrfToken, "", `{"passphrase":"strong-passphrase","lookback_hours":24,"duration_seconds":30}`); other.Code != 409 {
		t.Fatalf("monitor overlapped account diagnostic %d", other.Code)
	}
	status := request(h, "GET", path+"/status", "", "", "")
	var obj struct {
		State                  string `json:"state"`
		Running                bool   `json:"running"`
		TradingAuthorized      bool   `json:"trading_authorized"`
		StreamContinuityProven bool   `json:"stream_continuity_proven"`
		ManualReviewRequired   bool   `json:"manual_review_required"`
	}
	if status.Code != 200 || json.Unmarshal(status.Body.Bytes(), &obj) != nil || !obj.Running || obj.State != trading.MonitorConnected || obj.TradingAuthorized || !obj.StreamContinuityProven || !obj.ManualReviewRequired {
		t.Fatalf("unsafe live monitor status %d %+v", status.Code, obj)
	}
	for _, route := range []string{"/api/v1/trading/manual/orders", "/api/v1/trading/auto/start", "/api/v1/trading/auto/stop"} {
		if resp := request(h, "POST", route, s.csrfToken, "", `{}`); resp.Code != http.StatusLocked {
			t.Fatalf("order unlocked %s %d", route, resp.Code)
		}
	}
	if bad := request(h, "POST", path+"/stop", "bad", "", `{}`); bad.Code != 403 {
		t.Fatalf("stop allowed without csrf %d", bad.Code)
	}
	stopped := request(h, "POST", path+"/stop", s.csrfToken, "", `{}`)
	if stopped.Code != 200 || !strings.Contains(stopped.Body.String(), `"trading_authorized":false`) {
		t.Fatalf("unsafe stop %d %s", stopped.Code, stopped.Body)
	}
}
func TestContinuousMonitorStatusDoesNotClaimHistoricReadiness(t *testing.T) {
	s, e := newWithFetcher(t.TempDir(), readDemoMock{})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	status := request(s.Handler(), "GET", "/api/v1/recovery/continuous-monitor/status", "", "", "")
	if status.Code != 200 || !strings.Contains(status.Body.String(), `"state":"NOT_RUN"`) || !strings.Contains(status.Body.String(), `"trading_authorized":false`) {
		t.Fatalf("unsafe unarmed status %d %s", status.Code, status.Body)
	}
	start := request(s.Handler(), "POST", "/api/v1/recovery/continuous-monitor/start", s.csrfToken, "", `{"passphrase":"abc","lookback_hours":24}`)
	if start.Code != 423 {
		t.Fatalf("unconnected account started supervisor %d", start.Code)
	}
}

func TestContinuousMonitorCredentialLockCancelsWorkerBeforeClear(t *testing.T) {
	s := connectedAccountingServer(t)
	defer s.Close()
	started := make(chan struct{})
	exited := make(chan struct{})
	s.monitorRunner = func(ctx context.Context, _ trading.DemoCredentials, _ string, _ time.Duration, cb func(trading.MonitorCheckpoint)) (trading.MonitorCheckpoint, error) {
		cp := trading.MonitorCheckpoint{Schema: 1, State: trading.MonitorConnected, Issues: []string{}, ManualReviewRequired: true}
		cb(cp)
		close(started)
		<-ctx.Done()
		close(exited)
		cp.State = trading.MonitorStopped
		return cp, nil
	}
	h := s.Handler()
	start := request(h, "POST", "/api/v1/recovery/continuous-monitor/start", s.csrfToken, "", `{"passphrase":"strong-passphrase","lookback_hours":24}`)
	if start.Code != 202 {
		t.Fatalf("failed to arm monitor %d %s", start.Code, start.Body)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("monitor not started")
	}
	locked := request(h, "POST", "/api/v1/credentials/demo/lock", s.csrfToken, "", `{}`)
	if locked.Code != 200 {
		t.Fatalf("account locked without graceful monitor stop %d %s", locked.Code, locked.Body)
	}
	select {
	case <-exited:
	default:
		t.Fatal("cleared account while background monitor was still running")
	}
	if s.demo.Snapshot().Status == "connected" {
		t.Fatal("credential lock didn't clear account")
	}
}
