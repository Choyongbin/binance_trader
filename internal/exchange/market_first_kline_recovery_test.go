package exchange

import (
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// A successful upgrade without a single kline must not leave the process
// permanently syncing; the alternate Demo-only endpoint must be tried.
func TestQuietMarketUpgradeFallsBackToSecondDemoEndpoint(t *testing.T) {
	prev := marketFirstKlineTimeout
	marketFirstKlineTimeout = 200 * time.Millisecond
	defer func() { marketFirstKlineTimeout = prev }()

	// Fix the candle open time once; crossing a wall-clock minute boundary
	// during the reconnect backoff must not make this offline test flaky.
	fixture := mockKlines()
	eventAt := time.UnixMilli(fixture[len(fixture)-1][0].(int64)).Add(25 * time.Second)
	rest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/fapi/v1/klines":
			_ = json.NewEncoder(w).Encode(fixture)
		case "/fapi/v1/time":
			_ = json.NewEncoder(w).Encode(map[string]any{"serverTime": time.Now().UnixMilli()})
		default:
			http.Error(w, "not found", http.StatusNotFound)
		}
	}))
	defer rest.Close()

	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, conn := wsUpgradeForMarketTest(t, w, r)
		defer conn.Close()
		// No kline is sent. A quiet but upgraded socket is NOT ready.
		time.Sleep(600 * time.Millisecond)
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf, conn := wsUpgradeForMarketTest(t, w, r)
		defer conn.Close()
		time.Sleep(30 * time.Millisecond)
		e := sampleStreamEvent(eventAt)
		e.K.Open = "90199"
		e.K.High = "90210"
		e.K.Low = "90190"
		e.K.Close = "90203"
		e.K.Volume = "15.5"
		data, _ := json.Marshal(e)
		_, _ = buf.Write([]byte{0x81, 126, byte(len(data) >> 8), byte(len(data))})
		_, _ = buf.Write(data)
		_ = buf.Flush()
		time.Sleep(1200 * time.Millisecond)
	}))
	defer second.Close()
	svc := newStream(NewMarketWithBase(rest.URL, rest.Client()), "ws"+strings.TrimPrefix(first.URL, "http")+"/market/ws/btcusdt@kline_1m", "ws"+strings.TrimPrefix(second.URL, "http")+"/market/ws/btcusdt@kline_1m")
	svc.Start()
	defer svc.Close()
	limit := time.Now().Add(4 * time.Second)
	for time.Now().Before(limit) {
		status := svc.Status()
		if status.Ready {
			if status.ReconnectCount < 1 || !strings.Contains(status.Endpoint, strings.TrimPrefix(second.URL, "http://")) {
				t.Fatalf("wrong failover endpoint: %+v", status)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("failed to fail over quiet upgrade: %+v", svc.Status())
}

func wsUpgradeForMarketTest(t *testing.T, w http.ResponseWriter, r *http.Request) (*testMarketWriter, interface{ Close() error }) {
	t.Helper()
	hj := w.(http.Hijacker)
	conn, buf, err := hj.Hijack()
	if err != nil {
		t.Error(err)
		return nil, nil
	}
	sum := sha1.Sum([]byte(r.Header.Get("Sec-WebSocket-Key") + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	_, _ = fmt.Fprintf(buf, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n", base64.StdEncoding.EncodeToString(sum[:]))
	_ = buf.Flush()
	return &testMarketWriter{buf}, conn
}

type testMarketWriter struct {
	w interface {
		Write([]byte) (int, error)
		Flush() error
	}
}

func (t *testMarketWriter) Write(p []byte) (int, error) { return t.w.Write(p) }
func (t *testMarketWriter) Flush() error                { return t.w.Flush() }
