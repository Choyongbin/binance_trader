package exchange

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func mockKlines() [][]any {
	rows := make([][]any, 200)
	for i := range rows {
		t := time.Now().UTC().Truncate(time.Minute).Add(time.Duration(i-199) * time.Minute).UnixMilli()
		price := 90000 + i
		rows[i] = []any{t, floatStr(price), floatStr(price + 2), floatStr(price - 2), floatStr(price + 1), "13.5", t + 59999, "0", 1, "0", "0", "0"}
	}
	return rows
}
func floatStr(v int) string { return json.Number(stringInt(v)).String() }
func stringInt(v int) string {
	if v == 0 {
		return "0"
	}
	s := ""
	for v > 0 {
		s = string(rune('0'+v%10)) + s
		v /= 10
	}
	return s
}
func TestPublicDemoMarketValidationAndCache(t *testing.T) {
	var calls atomic.Int32
	fixture, _ := json.Marshal(mockKlines())
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("X-MBX-APIKEY") != "" {
			t.Errorf("public market request unexpectedly carried API key")
		}
		switch r.URL.Path {
		case "/fapi/v1/klines":
			if r.Method != "GET" || r.URL.Query().Get("symbol") != "BTCUSDT" || r.URL.Query().Get("interval") != "1m" {
				t.Errorf("unexpected market request: %s %s", r.Method, r.URL)
			}
			_, _ = w.Write(fixture)
		case "/fapi/v1/time":
			_ = json.NewEncoder(w).Encode(map[string]any{"serverTime": time.Now().UnixMilli()})
		default:
			t.Errorf("unexpected market path: %s", r.URL.Path)
		}
	}))
	defer remote.Close()
	market := NewMarketWithBase(remote.URL, remote.Client())
	a, err := market.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	b, err := market.Get(context.Background())
	if err != nil || a.Source != "BINANCE_FUTURES_DEMO_PUBLIC" || len(a.Candles) != 200 || len(b.Candles) != 200 || calls.Load() != 2 {
		t.Fatalf("bad market result/caching: %v %d", err, calls.Load())
	}
	// Expired good values must not be returned when the next remote query fails.
	market.fetched = time.Now().Add(-20 * time.Second)
	market.client = &http.Client{Timeout: time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	after, err := market.Get(ctx)
	if err == nil || len(after.Candles) > 0 {
		t.Fatal("stale candles exposed as live on request failure")
	}
}
func TestPublicDemoMarketRejectsCorruption(t *testing.T) {
	corrupt := mockKlines()
	corrupt[100][0] = corrupt[99][0]
	body, _ := json.Marshal(corrupt)
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/fapi/v1/time" {
			_ = json.NewEncoder(w).Encode(map[string]any{"serverTime": time.Now().UnixMilli()})
			return
		}
		_, _ = w.Write(body)
	}))
	defer remote.Close()
	service := NewMarketWithBase(remote.URL, remote.Client())
	snapshot, err := service.Get(context.Background())
	if err == nil || len(snapshot.Candles) > 0 {
		t.Fatal("duplicate timestamps must reject entire snapshot")
	}
}

func TestPublicMarketFreshnessUsesBinanceServerTimeNotPCLocalClock(t *testing.T) {
	exchangeNow := time.Now().UTC().Truncate(time.Second)
	rows := mockKlines()
	body, _ := json.Marshal(rows)
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/fapi/v1/klines":
			_, _ = w.Write(body)
		case "/fapi/v1/time":
			_ = json.NewEncoder(w).Encode(map[string]any{"serverTime": exchangeNow.UnixMilli()})
		default:
			http.NotFound(w, r)
		}
	}))
	defer remote.Close()
	market := NewMarketWithBase(remote.URL, remote.Client())
	// Simulate a Windows clock two hours fast. The latest kline is still fresh
	// relative to Binance's own server time and therefore must be accepted.
	market.now = func() time.Time { return exchangeNow.Add(2 * time.Hour) }
	snapshot, err := market.Get(context.Background())
	if err != nil {
		t.Fatalf("valid Binance market rejected due to PC clock skew: %v", err)
	}
	if snapshot.ExchangeClockOffsetMs > -int64((time.Hour).Milliseconds()) {
		t.Fatalf("expected diagnostic exchange/local clock offset, got %dms", snapshot.ExchangeClockOffsetMs)
	}
}
