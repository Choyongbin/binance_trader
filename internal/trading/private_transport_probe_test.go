package trading

import (
	"binance_trader/internal/wsclient"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

type testProbeIssuer struct {
	key   string
	err   error
	calls int
}

func (x *testProbeIssuer) StartDemoUserStream(_ context.Context, k string) (string, error) {
	x.calls++
	if k != "secret-demo-key" {
		return "", errors.New("wrong test key")
	}
	return x.key, x.err
}

type testProbeConn struct {
	mu     sync.Mutex
	raw    [][]byte
	closed chan struct{}
	once   sync.Once
}

func (c *testProbeConn) ReadText() ([]byte, error) {
	c.mu.Lock()
	if len(c.raw) > 0 {
		item := c.raw[0]
		c.raw = c.raw[1:]
		c.mu.Unlock()
		return item, nil
	}
	c.mu.Unlock()
	<-c.closed
	return nil, errors.New("closed")
}
func (c *testProbeConn) Close() error { c.once.Do(func() { close(c.closed) }); return nil }
func newProbeConn(raw ...string) *testProbeConn {
	c := &testProbeConn{closed: make(chan struct{})}
	for _, r := range raw {
		c.raw = append(c.raw, []byte(r))
	}
	return c
}
func TestPrivateTransportProbeHandshakeAndEventDoesNotAuthorize(t *testing.T) {
	key := strings.Repeat("X", 64)
	valid := `{"e":"ACCOUNT_UPDATE","E":1790525396365,"T":1790525396364,"a":{"m":"ORDER","B":[],"P":[]}}`
	cases := []struct {
		name   string
		events []string
		state  string
		count  int
	}{{"quiet", nil, "HANDSHAKE_ONLY", 0}, {"received", []string{valid}, "EVENTS_OBSERVED", 1}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			issuer := &testProbeIssuer{key: key}
			conn := newProbeConn(tc.events...)
			r := ProbePrivateTransport(context.Background(), issuer, "secret-demo-key", func(_ context.Context, u string) (exchange.UserEventConn, error) {
				parsed, err := url.Parse(u)
				if err != nil || parsed.Path != "/private/ws/"+key || parsed.RawQuery != "" {
					t.Errorf("incorrect demo private path")
				}
				return conn, nil
			}, 120*time.Millisecond)
			if r.Status != tc.state || r.EventCount != tc.count || !r.HandshakeOK || r.TradingAuthorized || r.StreamContinuityProven || !r.HistoricalOnly || issuer.calls != 1 {
				t.Fatalf("probe %+v", r)
			}
			if strings.Contains(r.Note, key) || strings.Contains(r.Note, "secret") {
				t.Fatal("secret leaked")
			}
			dir := t.TempDir()
			rec, e := ArchivePrivateProbe(dir, r)
			if e != nil {
				t.Fatal(e)
			}
			loaded, e := LoadPrivateProbes(dir)
			if e != nil || len(loaded) != 1 || loaded[0].ID != rec.ID {
				t.Fatalf("reload %+v %v", loaded, e)
			}
			payload, e := os.ReadFile(filepath.Join(privateProbeDir(dir), rec.ID+".json"))
			if e != nil || strings.Contains(string(payload), key) || strings.Contains(string(payload), "secret-demo-key") || strings.Contains(string(payload), "listenKey") {
				t.Fatal("archive leaked sensitive data")
			}
		})
	}
}
func TestPrivateTransportProbeFailuresAndTampering(t *testing.T) {
	issuer := &testProbeIssuer{key: strings.Repeat("Y", 64)}
	cases := []struct {
		name   string
		dial   exchange.UserStreamDialer
		status string
	}{
		{"upgrade", func(context.Context, string) (exchange.UserEventConn, error) {
			return nil, errors.New("fake secret in upstream URL")
		}, "WS_HANDSHAKE_FAILED"},
		{"bad_event", func(context.Context, string) (exchange.UserEventConn, error) {
			return newProbeConn(`{"unexpected":"raw secret"}`), nil
		}, "STREAM_INTERRUPTED_OR_INVALID_EVENT"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := ProbePrivateTransport(context.Background(), issuer, "secret-demo-key", tc.dial, 150*time.Millisecond)
			if r.Status != tc.status || strings.Contains(r.Note, "fake secret") {
				t.Fatalf("unexpected %+v", r)
			}
		})
	}
	r := ProbePrivateTransport(context.Background(), &testProbeIssuer{err: errors.New("fake secret 123")}, "secret-demo-key", func(context.Context, string) (exchange.UserEventConn, error) {
		t.Fatal("dial on failed issuer")
		return nil, nil
	}, 100*time.Millisecond)
	if r.Status != "LISTEN_KEY_FAILED" || strings.Contains(r.Note, "123") {
		t.Fatal("unexpected error handling", r)
	}
	dir := t.TempDir()
	r.CredentialBound = true
	rec, err := ArchivePrivateProbe(dir, r)
	if err != nil {
		t.Fatal(err)
	}
	f := filepath.Join(privateProbeDir(dir), rec.ID+".json")
	data, _ := os.ReadFile(f)
	data = append(data, []byte("tampered")...)
	if err := os.WriteFile(f, data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadPrivateProbes(dir); err == nil {
		t.Fatal("accepted tampered report")
	}
}
func TestPrivateProbeMustNotRunOutsideBounds(t *testing.T) {
	issuer := &testProbeIssuer{}
	r := ProbePrivateTransport(context.Background(), issuer, "secret-demo-key", func(context.Context, string) (exchange.UserEventConn, error) { return nil, nil }, 24*time.Hour)
	if r.Status != "INVALID_CONFIGURATION" || issuer.calls != 0 {
		t.Fatal("unbounded probe", r)
	}
}

// End-to-end local RFC6455 handshake, including the new private query parser
// and actual masked-client websocket implementation. This never reaches Binance.
func TestPrivateTransportRFC6455LocalUpgrade(t *testing.T) {
	key := strings.Repeat("k", 64)
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/private/ws" || r.URL.RawQuery != "" {
			t.Errorf("invalid private local upgrade path")
			w.WriteHeader(400)
			return
		}
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			t.Error("mock cannot hijack")
			return
		}
		wire, buffer, err := hijacker.Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer wire.Close()
		sum := sha1.Sum([]byte(r.Header.Get("Sec-WebSocket-Key") + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
		if _, err = fmt.Fprintf(buffer, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n", base64.StdEncoding.EncodeToString(sum[:])); err != nil {
			t.Error(err)
			return
		}
		if err = buffer.Flush(); err != nil {
			t.Error(err)
			return
		}
		// A quiet real private WS is valid; do not manufacture an account event.
		_ = wire.SetReadDeadline(time.Now().Add(2 * time.Second))
		var p [1]byte
		_, _ = wire.Read(p[:])
	}))
	defer remote.Close()
	issuer := &testProbeIssuer{key: key}
	got := ProbePrivateTransport(context.Background(), issuer, "secret-demo-key", func(ctx context.Context, _ string) (exchange.UserEventConn, error) {
		u := "ws" + strings.TrimPrefix(remote.URL, "http") + "/private/ws"
		return wsclient.Dial(ctx, u)
	}, 120*time.Millisecond)
	if got.Status != "HANDSHAKE_ONLY" || !got.HandshakeOK || got.TradingAuthorized {
		t.Fatalf("local private WS fail: %+v", got)
	}
}
