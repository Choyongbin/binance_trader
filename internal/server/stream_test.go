package server

import (
	"strings"
	"testing"
)

func TestLiveMarketAPIIsFailClosedAndRealIsLocked(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	h := s.Handler()
	status := request(h, "GET", "/api/v1/market/status", "", "", "")
	if status.Code != 200 || !strings.Contains(status.Body.String(), `"ready":false`) {
		t.Fatalf("unexpected pre-connect status %v", status.Body.String())
	}
	live := request(h, "GET", "/api/v1/market/live", "", "", "")
	if live.Code != 503 || strings.Contains(live.Body.String(), `"candles"`) {
		t.Fatalf("never publish REST as WS: %v", live.Body.String())
	}
	if invalid := request(h, "GET", "/api/v1/market/live?symbol=ETHUSDT", "", "", ""); invalid.Code != 400 {
		t.Fatal("unapproved market symbol accepted")
	}
	token := bootstrapToken(t, h)
	change := request(h, "POST", "/api/v1/settings/environment", token, "http://127.0.0.1:8080", `{"environment":"real"}`)
	if change.Code != 200 {
		t.Fatalf("environment switch failed: %v", change.Body.String())
	}
	for _, path := range []string{"/api/v1/market/status", "/api/v1/market/live"} {
		response := request(h, "GET", path, "", "", "")
		if response.Code != 423 {
			t.Fatalf("real environment leaked demo market data: %v", response.Body.String())
		}
	}
}

func TestMarketDashboardUsesSingle250msLivePolling(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	js := request(s.Handler(), "GET", "/app.js", "", "", "")
	if js.Code != 200 {
		t.Fatalf("app.js=%d", js.Code)
	}
	body := js.Body.String()
	for _, want := range []string{
		"const MARKET_POLL_INTERVAL_MS = 250;",
		"requestMarketLive()",
		"setInterval(syncMarket,MARKET_POLL_INTERVAL_MS)",
		"if(messageChanged){state.indicators=calculateIndicators(data.candles);renderMarket();}",
		"function marketMessageChanged(previous,next)",
		"if(needsRender)renderMarket();",
		"if (response.status === 503) return { ready:false, data };",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("app.js missing market polling contract %q", want)
		}
	}
	if strings.Contains(body, "request('/api/v1/market/status')") {
		t.Fatal("250ms market loop still performs the redundant status request")
	}
	if strings.Contains(body, "setInterval(syncMarket,1000)") {
		t.Fatal("legacy one-second market polling remains wired")
	}
}
