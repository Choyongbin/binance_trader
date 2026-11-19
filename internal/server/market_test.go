package server

import (
	"binance_trader/internal/exchange"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMarketReadOnlyDemoOnlyAndNewUI(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/fapi/v1/time" {
			_ = json.NewEncoder(w).Encode(map[string]int64{"serverTime": time.Now().UnixMilli()})
			return
		}
		if r.URL.Path != "/fapi/v1/klines" {
			http.NotFound(w, r)
			return
		}
		rows := make([][]any, 120)
		for i := range rows {
			ts := time.Now().UTC().Truncate(time.Minute).Add(time.Duration(i-119) * time.Minute).UnixMilli()
			rows[i] = []any{ts, "85000", "85003", "84999", "85001", "15", ts + 59999, "0", 1, "0", "0", "0"}
		}
		_ = json.NewEncoder(w).Encode(rows)
	}))
	defer remote.Close()
	srv, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	srv.market = exchange.NewMarketWithBase(remote.URL, remote.Client())
	handler := srv.Handler()
	chart := request(handler, "GET", "/api/v1/market/klines", "", "", "")
	if chart.Code != 200 || !strings.Contains(chart.Body.String(), `"source":"BINANCE_FUTURES_DEMO_PUBLIC"`) {
		t.Fatalf("market %d: %s", chart.Code, chart.Body.String())
	}
	if bad := request(handler, "GET", "/api/v1/market/klines?symbol=ETHUSDT", "", "", ""); bad.Code != 400 {
		t.Fatal("unsupported symbol accepted")
	}
	html := request(handler, "GET", "/", "", "", "")
	for _, label := range []string{"수동 거래", "자동매매 모델", "보유 포지션", "설정 열기", "RSI (14)", "체결 · 거래 내역"} {
		if !strings.Contains(html.Body.String(), label) {
			t.Fatal("unified UI missing", label)
		}
	}
	token := bootstrapToken(t, handler)
	if changed := request(handler, "POST", "/api/v1/settings/environment", token, "http://127.0.0.1:8080", `{"environment":"real"}`); changed.Code != 200 {
		t.Fatal("failed environment switch")
	}
	if forbidden := request(handler, "GET", "/api/v1/market/klines", "", "", ""); forbidden.Code != 423 {
		t.Fatal("demo price leaked into real locked screen")
	}
	if forbidden := request(handler, "POST", "/api/v1/trading/manual/orders", token, "http://127.0.0.1:8080", `{}`); forbidden.Code != 423 {
		t.Fatal("unlocked trading unexpectedly")
	}
}
