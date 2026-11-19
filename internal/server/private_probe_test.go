package server

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"binance_trader/internal/trading"
)

func TestPrivateProbeExplicitPassphraseLockedByDefault(t *testing.T) {
	s, e := newWithFetcher(t.TempDir(), readDemoMock{})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	h := s.Handler()
	token := bootstrapToken(t, h)
	url := "/api/v1/trading/private-probe/run"
	if r := request(h, "GET", "/api/v1/trading/private-probe/status", "", "", ""); r.Code != 200 || !strings.Contains(r.Body.String(), `"status":"NOT_RUN"`) {
		t.Fatal(r.Code, r.Body)
	}
	if r := request(h, "POST", url, token, "", `{"passphrase":"abc"}`); r.Code != 423 {
		t.Fatal("allowed locked", r.Code)
	}
	if e := s.vault.SaveDemo("demo-only-key", "demo-only-secret", "strong-private-passphrase"); e != nil {
		t.Fatal(e)
	}
	if e := s.demo.Connect(context.Background(), "demo-only-key", "demo-only-secret"); e != nil {
		t.Fatal(e)
	}
	called := false
	s.privateProbeRunner = func(context.Context, string) trading.PrivateTransportResult {
		called = true
		return trading.PrivateTransportResult{Schema: 1}
	}
	if r := request(h, "POST", url, "wrong", "", `{"passphrase":"strong-private-passphrase"}`); r.Code != 403 {
		t.Fatal("allowed wrong CSRF")
	}
	if r := request(h, "POST", url, token, "", `{"passphrase":"incorrect"}`); r.Code != 403 {
		t.Fatal("allowed wrong vault passphrase", r.Code)
	}
	if called {
		t.Fatal("probe launched without proper approval")
	}
	// Produce a valid isolated mock result with no actual sockets.
	s.privateProbeRunner = func(ctx context.Context, key string) trading.PrivateTransportResult {
		if key != "demo-only-key" {
			t.Error("unexpected decrypted key")
		}
		started := time.Now().UTC()
		return trading.PrivateTransportResult{Schema: 1, Status: "HANDSHAKE_ONLY", StartedAt: started, FinishedAt: started.Add(time.Millisecond), HandshakeOK: true,
			EventTypes: []string{}, HistoricalOnly: true, Note: "일시적인 연결 진단만 수행했습니다. 과거 누락 이벤트, 실제 주문 상태 및 계정 연속성은 증명하지 않습니다."}
	}
	r := request(h, "POST", url, token, "http://127.0.0.1:8080", `{"passphrase":"strong-private-passphrase"}`)
	if r.Code != 200 || strings.Contains(r.Body.String(), "demo-only-key") || strings.Contains(r.Body.String(), "strong-private-passphrase") {
		t.Fatal("probe or secret response", r.Code, r.Body)
	}
	var out trading.PrivateTransportResult
	if e := json.Unmarshal(r.Body.Bytes(), &out); e != nil || out.TradingAuthorized || out.StreamContinuityProven || !out.CredentialBound || out.Status != "HANDSHAKE_ONLY" {
		t.Fatalf("bad probe %+v %v", out, e)
	}
	r = request(h, "GET", "/api/v1/trading/private-probe/status", "", "", "")
	if r.Code != 200 || !strings.Contains(r.Body.String(), `"report_count":1`) || !strings.Contains(r.Body.String(), `"historical_only":true`) {
		t.Fatal("status", r.Code, r.Body)
	}
	if r := request(h, "POST", "/api/v1/trading/manual/orders", token, "", `{}`); r.Code != http.StatusLocked {
		t.Fatal("order was unlocked")
	}
	if e := os.WriteFile(filepath.Join(s.dataDir, "diagnostics", "private_stream", "unexpected.txt"), []byte("evil"), 0600); e != nil {
		t.Fatal(e)
	}
	if r := request(h, "GET", "/api/v1/trading/private-probe/status", "", "", ""); r.Code != 503 {
		t.Fatal("accepted corrupted report", r.Code)
	}
}
func TestPrivateProbeRejectsSessionChangeAndConcurrency(t *testing.T) {
	s, e := newWithFetcher(t.TempDir(), readDemoMock{})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if e := s.vault.SaveDemo("demo-only-key", "demo-only-secret", "strong-private-passphrase"); e != nil {
		t.Fatal(e)
	}
	if e := s.demo.Connect(context.Background(), "demo-only-key", "demo-only-secret"); e != nil {
		t.Fatal(e)
	}
	started := make(chan struct{})
	continueCh := make(chan struct{})
	var once sync.Once
	s.privateProbeRunner = func(ctx context.Context, key string) trading.PrivateTransportResult {
		once.Do(func() { close(started) })
		<-continueCh
		now := time.Now().UTC()
		return trading.PrivateTransportResult{Schema: 1, Status: "HANDSHAKE_ONLY", StartedAt: now, FinishedAt: now.Add(time.Millisecond), HandshakeOK: true, EventTypes: []string{}, HistoricalOnly: true, Note: "일시적인 연결 진단만 수행했습니다. 과거 누락 이벤트, 실제 주문 상태 및 계정 연속성은 증명하지 않습니다."}
	}
	h := s.Handler()
	token := s.csrfToken // already connected; bootstrapToken expects disconnected state
	done := make(chan int, 1)
	go func() {
		done <- request(h, "POST", "/api/v1/trading/private-probe/run", token, "", `{"passphrase":"strong-private-passphrase"}`).Code
	}()
	<-started
	if r := request(h, "POST", "/api/v1/trading/private-probe/run", token, "", `{"passphrase":"strong-private-passphrase"}`); r.Code != 409 {
		t.Fatal("allowed concurrent probe", r.Code)
	}
	s.demo.Clear()
	close(continueCh)
	if code := <-done; code != 200 {
		t.Fatal("probe not archived after credential change", code)
	}
	r := request(h, "GET", "/api/v1/trading/private-probe/status", "", "", "")
	if !strings.Contains(r.Body.String(), `"credential_bound":false`) || !strings.Contains(r.Body.String(), "SESSION_CHANGED") {
		t.Fatal("session race unsafe", r.Body)
	}
}
