package exchange

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"binance_trader/internal/wsclient"
)

// Regression for dev.3.2: the private-stream callback receives an entire URL,
// NOT the listenKey. Passing that URL into a key-taking dialer fails instantly
// with UNCLASSIFIED before a TCP or WebSocket handshake can start.
func TestProductionPrivateURLDialerReachesMockUpgrade(t *testing.T) {
	const rejected = "SENSITIVE_RESPONSE_MUST_NOT_LEAK"
	key := strings.Repeat("R", 64)
	requests := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/private/ws" || r.URL.RawQuery != "" {
			t.Errorf("wrong private URL path or query keys")
		}
		requests <- struct{}{}
		w.Header().Set("X-Sensitive-Debug", rejected)
		w.WriteHeader(http.StatusForbidden)
		_, _ = fmt.Fprint(w, rejected)
	}))
	defer server.Close()
	mockURL := "ws" + strings.TrimPrefix(server.URL, "http") +
		"/private/ws"
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := DialDemoUserStreamURL(ctx, mockURL)
	if conn != nil || err == nil {
		if conn != nil {
			_ = conn.Close()
		}
		t.Fatal("mock 403 upgrade should be rejected")
	}
	select {
	case <-requests:
	default:
		t.Fatal("no WebSocket handshake was attempted: URL may have been passed as listenKey")
	}
	d := wsclient.DiagnoseDial(err)
	if d.Phase != "HTTP_STATUS" || d.HTTPStatus != http.StatusForbidden ||
		strings.Contains(err.Error(), key) || strings.Contains(err.Error(), rejected) ||
		strings.Contains(err.Error(), mockURL) {
		t.Fatalf("classification failed: phase=%s HTTP=%d", d.Phase, d.HTTPStatus)
	}
}

// Real network is deliberately not used. Validate the production adapter's
// failure path without leaking malformed/untrusted input.
func TestProductionPrivateURLDialerRejectsKeyInPlaceOfURL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c, err := DialDemoUserStreamURL(ctx, strings.Repeat("k", 64))
	if c != nil || err == nil || wsclient.DiagnoseDial(err).Phase != "URL_VALIDATION" {
		if c != nil {
			_ = c.Close()
		}
		t.Fatal("key mistaken for URL must be rejected")
	}
}
