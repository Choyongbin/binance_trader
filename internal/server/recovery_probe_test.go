package server

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"binance_trader/internal/exchange"
	"binance_trader/internal/trading"
)

const boundedProbePath = "/api/v1/recovery/probe"
const boundedProbeBody = `{"passphrase":"strong-passphrase","lookback_hours":24}`

func TestBoundedRecoveryRequiresExplicitDemoAndFreshCredentials(t *testing.T) {
	s, err := newWithFetcher(t.TempDir(), readDemoMock{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	h := s.Handler()
	if r := request(h, "POST", boundedProbePath, s.csrfToken, "", boundedProbeBody); r.Code != 423 {
		t.Fatal(r.Code, r.Body)
	}
	if err := s.vault.SaveDemo("demo-only-key", "demo-only-secret", "strong-passphrase"); err != nil {
		t.Fatal(err)
	}
	if err := s.demo.Connect(context.Background(), "demo-only-key", "demo-only-secret"); err != nil {
		t.Fatal(err)
	}
	attempts := 0
	s.recoveryRunner = func(context.Context, trading.DemoCredentials, string, int64) (trading.RecoveryProbe, error) {
		attempts++
		return trading.RecoveryProbe{}, errors.New("not called in test")
	}
	for _, tc := range []struct {
		body, token string
		code        int
	}{
		{boundedProbeBody, "bad", 403},
		{`{"passphrase":"bad-passphrase","lookback_hours":24}`, s.csrfToken, 403},
		{`{"passphrase":"strong-passphrase","lookback_hours":8}`, s.csrfToken, 400},
		{`{"passphrase":"strong-passphrase","lookback_hours":24,"extra":1}`, s.csrfToken, 400},
		{`{"passphrase":"strong-passphrase","lookback_hours":24}` + `{}`, s.csrfToken, 400},
	} {
		r := request(h, "POST", boundedProbePath, tc.token, "", tc.body)
		if r.Code != tc.code {
			t.Fatalf("input %q returned %d: %s", tc.body, r.Code, r.Body)
		}
	}
	if attempts != 0 {
		t.Fatal("recovery contacted before authorization")
	}
	// Persisted credential file changed independently of live authenticated manager.
	if e := s.vault.SaveDemo("different-demo-account", "another-secret", "strong-passphrase"); e != nil {
		t.Fatal(e)
	}
	if r := request(h, "POST", boundedProbePath, s.csrfToken, "", boundedProbeBody); r.Code != 409 {
		t.Fatal("mismatched Demo account accepted", r.Code)
	}
	if attempts != 0 {
		t.Fatal("account mismatch reached probe")
	}
}

func TestBoundedRecoveryMockRealPersistenceAndOrdersRemainLocked(t *testing.T) {
	s := connectedAccountingServer(t)
	defer s.Close()
	h := s.Handler()
	s.recoveryRunner = func(ctx context.Context, creds trading.DemoCredentials, dir string, since int64) (trading.RecoveryProbe, error) {
		if creds.APIKey != "demo-only-key" || creds.Secret != "demo-only-secret" || time.Since(time.UnixMilli(since)) > 25*time.Hour {
			t.Fatal("unexpected private or stale input")
		}
		signed := fakeAccountingWindow(func(_ context.Context, key, secret string, start int64) (exchange.RecoveryBaseline, error) {
			if key != "demo-only-key" || secret != "demo-only-secret" {
				t.Fatal("wrong signed creds")
			}
			// The mock is a stable signed REST bracket, not proof of exchange coverage.
			return minimalSignedWindow(t, start), nil
		})
		observer := func(ctx context.Context, store *trading.EventStore, onConnected func()) error {
			if e := store.RecordDiscontinuity("STARTUP"); e != nil {
				return e
			}
			onConnected()
			<-ctx.Done()
			return ctx.Err()
		}
		return trading.ProbeDemoRecovery(ctx, signed, creds, dir, since, observer)
	}
	r := request(h, "POST", boundedProbePath, s.csrfToken, "http://127.0.0.1:8080", boundedProbeBody)
	if r.Code != 200 || !strings.Contains(r.Body.String(), `"trading_authorized":false`) ||
		!strings.Contains(r.Body.String(), `"stream_continuity_proven":false`) ||
		!strings.Contains(r.Body.String(), `"status":"HISTORICAL_REVIEW_ONLY"`) {
		t.Fatalf("unexpected diagnostic %d %s", r.Code, r.Body)
	}
	for _, sensitive := range []string{"demo-only-key", "demo-only-secret", "strong-passphrase"} {
		if strings.Contains(r.Body.String(), sensitive) {
			t.Fatal("credential leaked")
		}
	}
	status := request(h, "GET", "/api/v1/recovery/status", "", "", "")
	if status.Code != 200 || !strings.Contains(status.Body.String(), `"report_count":1`) ||
		!strings.Contains(status.Body.String(), `"trading_authorized":false`) {
		t.Fatalf("status unexpected: %d %s", status.Code, status.Body)
	}
	for _, route := range []string{"/api/v1/trading/manual/orders", "/api/v1/trading/auto/start"} {
		x := request(h, "POST", route, s.csrfToken, "", `{}`)
		if x.Code != http.StatusLocked {
			t.Fatal("trading unlocked", x.Code)
		}
	}
	// A single byte of archive corruption must fail closed, even if the old
	// JSON record still looks plausible to a browser.
	files, e := filepath.Glob(filepath.Join(s.dataDir, "trading", "recovery_sessions", "*.end.json"))
	if e != nil || len(files) != 1 {
		t.Fatal("archive missing", files, e)
	}
	f, e := os.OpenFile(files[0], os.O_APPEND|os.O_WRONLY, 0600)
	if e != nil {
		t.Fatal(e)
	}
	_, _ = f.WriteString("BAD")
	_ = f.Close()
	if z := request(h, "GET", "/api/v1/recovery/status", "", "", ""); z.Code != 503 {
		t.Fatal("corrupt archive accepted", z.Code)
	}
	if z := request(h, "POST", boundedProbePath, s.csrfToken, "", boundedProbeBody); z.Code != 503 {
		t.Fatal("new run started with corrupt archive", z.Code)
	}
}

func TestBoundedRecoveryConcurrentWithPrivateProbeAndSessionChanged(t *testing.T) {
	s := connectedAccountingServer(t)
	defer s.Close()
	h := s.Handler()
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	s.recoveryRunner = func(ctx context.Context, creds trading.DemoCredentials, dir string, since int64) (trading.RecoveryProbe, error) {
		once.Do(func() { close(entered) })
		<-release
		return trading.RecoveryProbe{}, errors.New("should_not_leak_demo-only-secret")
	}
	done := make(chan *int, 1)
	go func() { v := request(h, "POST", boundedProbePath, s.csrfToken, "", boundedProbeBody).Code; done <- &v }()
	<-entered
	if r := request(h, "POST", boundedProbePath, s.csrfToken, "", boundedProbeBody); r.Code != 409 {
		t.Fatal("overlapping recovery allowed", r.Code)
	}
	if r := request(h, "POST", "/api/v1/trading/private-probe/run", s.csrfToken, "", `{"passphrase":"strong-passphrase"}`); r.Code != 409 {
		t.Fatal("overlapping private diagnostics allowed", r.Code)
	}
	if r := request(h, "GET", "/api/v1/recovery/status", "", "", ""); r.Code != 200 || !strings.Contains(r.Body.String(), `"diagnostic_busy":true`) {
		t.Fatal("busy not visible", r.Body)
	}
	s.demo.Clear()
	close(release)
	if code := <-done; *code != 409 {
		t.Fatal("session change accepted", *code)
	}
	if r := request(h, "GET", "/api/v1/recovery/status", "", "", ""); r.Code != 200 || !strings.Contains(r.Body.String(), `"diagnostic_busy":false`) {
		t.Fatal("guard not cleared", r.Body)
	}
	if r := request(h, "POST", "/api/v1/trading/manual/orders", s.csrfToken, "", `{}`); r.Code != 423 {
		t.Fatal("manual trading unlocked")
	}
}

func TestPrivateProbePreventsSimultaneousRecovery(t *testing.T) {
	s := connectedAccountingServer(t)
	defer s.Close()
	h := s.Handler()
	entered := make(chan struct{})
	release := make(chan struct{})
	s.privateProbeRunner = func(ctx context.Context, key string) trading.PrivateTransportResult {
		close(entered)
		<-release
		now := time.Now().UTC()
		return trading.PrivateTransportResult{Schema: 1, Status: "HANDSHAKE_ONLY", StartedAt: now, FinishedAt: now.Add(time.Millisecond), HandshakeOK: true, EventTypes: []string{}, HistoricalOnly: true, Note: "일시적인 연결 진단만 수행했습니다. 과거 누락 이벤트, 실제 주문 상태 및 계정 연속성은 증명하지 않습니다."}
	}
	invoked := false
	s.recoveryRunner = func(context.Context, trading.DemoCredentials, string, int64) (trading.RecoveryProbe, error) {
		invoked = true
		return trading.RecoveryProbe{}, nil
	}
	finished := make(chan int, 1)
	go func() {
		finished <- request(h, "POST", "/api/v1/trading/private-probe/run", s.csrfToken, "", `{"passphrase":"strong-passphrase"}`).Code
	}()
	<-entered
	if got := request(h, "POST", boundedProbePath, s.csrfToken, "", boundedProbeBody); got.Code != http.StatusConflict {
		t.Fatal("recovery started during private transport probe", got.Code)
	}
	if invoked {
		t.Fatal("overlapping recovery runner was called")
	}
	close(release)
	if code := <-finished; code != 200 {
		t.Fatal("private probe failed after release", code)
	}
}

func TestRecoveryResponseCannotTrustUnarchivedInjectedResult(t *testing.T) {
	s := connectedAccountingServer(t)
	defer s.Close()
	h := s.Handler()
	s.recoveryRunner = func(ctx context.Context, creds trading.DemoCredentials, dir string, since int64) (trading.RecoveryProbe, error) {
		now := time.Now().UTC()
		return trading.RecoveryProbe{Schema: 1, ID: "00000000000000000000000000000000", Stage: "REVIEWED", StartedAt: now.Add(-time.Second), FinishedAt: now, ManualReviewRequired: true, StaleAfterObservation: true}, nil
	}
	r := request(h, "POST", boundedProbePath, s.csrfToken, "", boundedProbeBody)
	if r.Code != http.StatusServiceUnavailable || strings.Contains(r.Body.String(), "demo-only-secret") {
		t.Fatalf("unarchived fake review was trusted: %d %s", r.Code, r.Body)
	}
	if r := request(h, "POST", "/api/v1/trading/auto/start", s.csrfToken, "", `{}`); r.Code != http.StatusLocked {
		t.Fatal("trade unlocked")
	}
}

func TestRecoveryNewKoreanHTMLIsWiredAndDoesNotRenderSecrets(t *testing.T) {
	s, e := newWithFetcher(t.TempDir(), readDemoMock{})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	h := s.Handler()
	html := request(h, "GET", "/", "", "", "")
	js := request(h, "GET", "/app.js", "", "", "")
	if html.Code != 200 || js.Code != 200 {
		t.Fatalf("embedded assets not served: %d %d", html.Code, js.Code)
	}
	for _, id := range []string{"demoRecoveryForm", "recoveryPassphrase", "recoveryLookback", "recoveryProbeRun", "recoveryHistory", "recoveryProbeResult"} {
		if !strings.Contains(html.Body.String(), `id="`+id+`"`) || !strings.Contains(js.Body.String(), `$('`+id+`')`) {
			t.Fatal("Korean UI recovery binding missing", id)
		}
	}
	if !strings.Contains(js.Body.String(), "/api/v1/recovery/probe") || !strings.Contains(html.Body.String(), "읽기 전용") {
		t.Fatal("read-only recovery UI not wired")
	}
}
