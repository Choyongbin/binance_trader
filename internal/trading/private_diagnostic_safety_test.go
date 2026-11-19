package trading

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"binance_trader/internal/exchange"
	"binance_trader/internal/wsclient"
)

func TestFailedPrivateDialArchivesStatusWithoutSecretsAndPreservesLegacyHistory(t *testing.T) {
	// Simulate a 404 upgrade refusal returned by the actual WebSocket stack.
	// New persisted metadata must not include the listenKey or untrusted text.
	key := strings.Repeat("SENSITIVE_EXAMPLE_", 4)
	issuer := &testProbeIssuer{key: key}
	dir := t.TempDir()
	r := ProbePrivateTransport(context.Background(), issuer, "secret-demo-key", func(context.Context, string) (exchange.UserEventConn, error) {
		return nil, wsclient.RedactDial(errors.New("https://secret?listenKey=" + key))
	}, 100*time.Millisecond)
	if r.Status != "WS_HANDSHAKE_FAILED" || r.HandshakeOK || r.DialPhase != "UNCLASSIFIED" || r.TradingAuthorized || r.StreamContinuityProven {
		t.Fatalf("unsafe result: %+v", r)
	}
	rec, err := ArchivePrivateProbe(dir, r)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(privateProbeDir(dir), rec.ID+".json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), key) || strings.Contains(string(raw), "secret-demo-key") || strings.Contains(string(raw), "https://secret") {
		t.Fatal("sensitive report")
	}
	hist, err := LoadPrivateProbes(dir)
	if err != nil || len(hist) != 1 || hist[0].Result.DialPhase != "UNCLASSIFIED" {
		t.Fatal(err, hist)
	}
	now := time.Now().UTC()
	legacy := PrivateTransportResult{Schema: 1, Status: "WS_HANDSHAKE_FAILED", StartedAt: now, FinishedAt: now.Add(time.Millisecond), EventTypes: []string{}, HistoricalOnly: true, Note: privateProbeDisclaimer}
	rec2, err := ArchivePrivateProbe(dir, legacy)
	if err != nil {
		t.Fatal(err)
	}
	raw, err = os.ReadFile(filepath.Join(privateProbeDir(dir), rec2.ID+".json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "dial_phase") || strings.Contains(string(raw), "dial_http_status") {
		t.Fatal("changed legacy JSON contract")
	}
	var recovered PrivateProbeRecord
	if json.Unmarshal(raw, &recovered) != nil || recovered.sum() != rec2.SHA256 {
		t.Fatal("legacy checksum incompatible")
	}
	if _, err = LoadPrivateProbes(dir); err != nil {
		t.Fatal("legacy archive invalid", err)
	}
}

// Exercises the full error propagation path: wsclient -> exchange stream ->
// account observer -> immutable BLOCKED recovery evidence. No real credentials.
func TestPrivateUpgradeHTTP404RecoveryArchivesSafePhase(t *testing.T) {
	lk := strings.Repeat("D", 64)
	secret := "NEVER_ARCHIVE_MY_SECRET"
	rest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/fapi/v1/listenKey" || r.Method != "POST" || r.Header.Get("X-MBX-APIKEY") != "mock-key" {
			t.Errorf("unexpected REST %s %s", r.Method, r.URL.Path)
			w.WriteHeader(400)
			return
		}
		_, _ = fmt.Fprintf(w, `{"listenKey":"%s"}`, lk)
	}))
	defer rest.Close()
	ws := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/private/ws/"+lk || r.URL.RawQuery != "" {
			t.Error("mock private URL mismatch")
		}
		w.Header().Set("X-Untrusted", secret)
		w.WriteHeader(404)
		_, _ = fmt.Fprint(w, secret)
	}))
	defer ws.Close()
	cli := exchange.NewWithBase(rest.URL, rest.Client())
	reader := diagnosticReader(func(context.Context, string, string, int64) (exchange.RecoveryBaseline, error) {
		t.Error("signed REST MUST NOT be called before private WebSocket connects")
		return exchange.RecoveryBaseline{}, errors.New("unexpected baseline")
	})
	root := t.TempDir()
	r, err := ProbeDemoRecovery(context.Background(), reader, DemoCredentials{APIKey: "mock-key", Secret: secret}, root, mockSince(), func(ctx context.Context, store *EventStore, onConnected func()) error {
		return runDemoAccountObservationReady(ctx, cli, "mock-key", store, func(ctx context.Context, _ string) (exchange.UserEventConn, error) {
			url := "ws" + strings.TrimPrefix(ws.URL, "http") + "/private/ws/" + lk
			return wsclient.Dial(ctx, url)
		}, time.Hour, onConnected)
	})
	if err == nil || r.Stage != "BLOCKED" || r.ObserverConnected || r.RESTReviewed || r.TradingAuthorized ||
		!issueHas(r.Blockers, "HTTP_STATUS") || !issueHas(r.Blockers, "HTTP 404") {
		t.Fatalf("unsafe classification: %+v error: %v", r, err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "trading", "recovery_sessions", r.ID+".end.json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), lk) || strings.Contains(string(raw), secret) || strings.Contains(string(raw), "/private/ws/") {
		t.Fatal("immutable recovery report leaked a secret")
	}
	hist, err := LoadRecoveryProbes(root)
	if err != nil || len(hist) != 1 || hist[0].Stage != "BLOCKED" {
		t.Fatalf("archive incompatible: %+v %v", hist, err)
	}
}
