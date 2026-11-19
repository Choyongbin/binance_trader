package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

type configDemoMock struct{ readDemoMock }

func (configDemoMock) InspectTradingConfiguration(_ context.Context, key, secret, symbol string) (exchange.TradingConfiguration, error) {
	if key != "demo-only-key" || secret != "demo-only-secret" || symbol != "BTCUSDT" {
		return exchange.TradingConfiguration{}, context.Canceled
	}
	return exchange.TradingConfiguration{Environment: "demo", Symbol: "BTCUSDT", HedgeMode: true, MarginType: "ISOLATED", Leverage: 15, AsOf: time.Now().UTC().Format(time.RFC3339Nano), SettingsMatchV1: true}, nil
}
func TestReadOnlyConfigEndpointAndLockedTrading(t *testing.T) {
	s, err := newWithFetcher(t.TempDir(), configDemoMock{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	h := s.Handler()
	token := bootstrapToken(t, h)
	if r := request(h, "GET", "/api/v1/trading/config", "", "", ""); r.Code != 503 {
		t.Fatalf("unauthenticated config leaked %d %s", r.Code, r.Body)
	}
	body := `{"api_key":"demo-only-key","secret_key":"demo-only-secret","passphrase":"long-passphrase-to-encrypt"}`
	if r := request(h, "POST", "/api/v1/credentials/demo/save", token, "http://127.0.0.1:8080", body); r.Code != 200 {
		t.Fatalf("mock auth failed %d %s", r.Code, r.Body)
	}
	result := request(h, "GET", "/api/v1/trading/config", "", "", "")
	if result.Code != 200 {
		t.Fatalf("read config failed %d %s", result.Code, result.Body)
	}
	var config exchange.TradingConfiguration
	if err := json.Unmarshal(result.Body.Bytes(), &config); err != nil || !config.SettingsMatchV1 || !config.HedgeMode {
		t.Fatalf("bad config %+v %v", config, err)
	}
	if strings.Contains(result.Body.String(), "demo-only") {
		t.Fatal("configuration response leaked credentials")
	}
	for _, path := range []string{"/api/v1/trading/manual/orders", "/api/v1/trading/auto/start", "/api/v1/trading/config"} {
		if r := request(h, http.MethodPost, path, token, "http://127.0.0.1:8080", `{}`); r.Code != http.StatusLocked && r.Code != http.StatusMethodNotAllowed && r.Code != http.StatusNotFound {
			t.Fatalf("trading route %s is exposed %d %s", path, r.Code, r.Body)
		}
	}
	if r := request(h, "POST", "/api/v1/settings/environment", token, "http://127.0.0.1:8080", `{"environment":"real"}`); r.Code != 200 {
		t.Fatalf("real switch failed %d", r.Code)
	}
	if r := request(h, "GET", "/api/v1/trading/config", "", "", ""); r.Code != 423 {
		t.Fatalf("real config exposed %d", r.Code)
	}
	// Rebinding attempts cannot reveal unlocked config, either.
	r := httptest.NewRequest(http.MethodGet, "http://malicious.example/api/v1/trading/config", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatal("rebinding protection failed")
	}
}
