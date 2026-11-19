package server

import (
	"context"
	"net/http"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

type configApplyMock struct {
	hedge    bool
	margin   string
	leverage int
	writes   []string
}

func (m *configApplyMock) Fetch(_ context.Context, key, secret string) (exchange.Snapshot, error) {
	if key != "demo-only-key" || secret != "demo-only-secret" {
		return exchange.Snapshot{}, context.Canceled
	}
	return exchange.Snapshot{Status: "connected", Environment: "demo", AsOf: time.Now().UTC().Format(time.RFC3339), Balances: []exchange.Balance{{Asset: "USDT", Wallet: "100", Available: "100"}}, Positions: []exchange.Position{}, Orders: []exchange.Order{}, AlgoOrders: []exchange.AlgoOrder{}}, nil
}
func (m *configApplyMock) InspectTradingConfiguration(_ context.Context, key, secret, symbol string) (exchange.TradingConfiguration, error) {
	return exchange.TradingConfiguration{Environment: "demo", Symbol: symbol, HedgeMode: m.hedge, MarginType: m.margin, Leverage: m.leverage, AutoAddMargin: false, AsOf: time.Now().UTC().Format(time.RFC3339Nano), SettingsMatchV1: m.hedge && m.margin == "ISOLATED" && m.leverage == 15}, nil
}
func (m *configApplyMock) SetHedgeModeForDemo(_ context.Context, _, _ string, enabled bool) error {
	m.writes = append(m.writes, "hedge")
	m.hedge = enabled
	return nil
}
func (m *configApplyMock) SetIsolatedForDemo(context.Context, string, string) error {
	m.writes = append(m.writes, "isolated")
	m.margin = "ISOLATED"
	return nil
}
func (m *configApplyMock) SetLeverageForDemo(_ context.Context, _, _ string, l int) error {
	m.writes = append(m.writes, "leverage")
	m.leverage = l
	return nil
}

func TestApplyDemoTradingConfigEndpoint(t *testing.T) {
	remote := &configApplyMock{margin: "CROSSED", leverage: 2}
	s, err := newWithFetcher(t.TempDir(), remote)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	h := s.Handler()
	token := bootstrapToken(t, h)
	body := `{"api_key":"demo-only-key","secret_key":"demo-only-secret","passphrase":"long-passphrase-to-encrypt"}`
	if r := request(h, "POST", "/api/v1/credentials/demo/save", token, "http://127.0.0.1:8080", body); r.Code != 200 {
		t.Fatalf("auth %d %s", r.Code, r.Body)
	}
	if r := request(h, "POST", "/api/v1/trading/config/apply-demo-v1", token, "http://127.0.0.1:8080", `{"confirmation":"wrong"}`); r.Code != http.StatusBadRequest {
		t.Fatalf("bad confirmation accepted %d", r.Code)
	}
	r := request(h, "POST", "/api/v1/trading/config/apply-demo-v1", token, "http://127.0.0.1:8080", `{"confirmation":"APPLY DEMO BTCUSDT HEDGE ISOLATED 15X"}`)
	if r.Code != 200 {
		t.Fatalf("apply failed %d %s", r.Code, r.Body)
	}
	if len(remote.writes) != 3 || remote.writes[0] != "hedge" || remote.writes[1] != "isolated" || remote.writes[2] != "leverage" {
		t.Fatalf("wrong writes %#v", remote.writes)
	}
}
