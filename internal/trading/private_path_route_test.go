package trading

import (
	"binance_trader/internal/exchange"
	"binance_trader/internal/wsclient"
	"context"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestPrivatePathDiagnosticNeverArchivesListenKey(t *testing.T) {
	key := strings.Repeat("Y", 64)
	issuer := &testProbeIssuer{key: key}
	got := ProbePrivateTransport(context.Background(), issuer, "secret-demo-key", func(_ context.Context, u string) (exchange.UserEventConn, error) {
		parsed, err := url.Parse(u)
		if err != nil || parsed.Path != "/private/ws/"+key || parsed.RawQuery != "" {
			t.Fatal("expected private path mode")
		}
		return nil, wsclient.RedactDial(&mockHTTPStatusError{})
	}, 120*time.Millisecond)
	if got.HandshakeOK || got.TradingAuthorized || got.DialRoute != "PRIVATE_PATH_LISTENKEY" || strings.Contains(got.Note, key) {
		t.Fatalf("unexpected safe diagnostic %+v", got)
	}
}

type mockHTTPStatusError struct{}

func (*mockHTTPStatusError) Error() string { return "opaque dial failure" }
