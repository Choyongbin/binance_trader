package server

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

type executionPreflightMock struct{}

func (executionPreflightMock) Fetch(_ context.Context, key, secret string) (exchange.Snapshot, error) {
	return exchange.Snapshot{
		Status: "connected", Environment: "demo", AsOf: time.Now().UTC().Format(time.RFC3339),
		Balances:  []exchange.Balance{{Asset: "USDT", Wallet: "5000", Available: "5000"}},
		Positions: []exchange.Position{}, Orders: []exchange.Order{}, AlgoOrders: []exchange.AlgoOrder{},
	}, nil
}

func (executionPreflightMock) FetchDemoExecutionPreflight(_ context.Context, key, secret, symbol string) (exchange.ExecutionPreflightSnapshot, error) {
	raw := []byte(`{"canTrade":true,"multiAssetsMargin":false,"totalWalletBalance":"5000","totalUnrealizedProfit":"0","totalMarginBalance":"5000","totalInitialMargin":"0","totalPositionInitialMargin":"0","totalOpenOrderInitialMargin":"0","availableBalance":"5000","assets":[{"asset":"USDT","walletBalance":"5000","availableBalance":"5000","marginBalance":"5000"}],"positions":[{"symbol":"BTCUSDT","positionSide":"LONG","positionAmt":"0","initialMargin":"0","isolated":true},{"symbol":"BTCUSDT","positionSide":"SHORT","positionAmt":"0","initialMargin":"0","isolated":true}]}`)
	var p exchange.AccountPortfolio
	if err := json.Unmarshal(raw, &p); err != nil {
		return exchange.ExecutionPreflightSnapshot{}, err
	}
	now := time.Now().UTC()
	return exchange.ExecutionPreflightSnapshot{
		Environment: "demo", StartedAt: now.Add(-100 * time.Millisecond), FinishedAt: now,
		Portfolio: p, OpenOrders: []exchange.Order{}, OpenAlgos: []exchange.AlgoOrder{},
		Configuration: exchange.TradingConfiguration{Environment: "demo", Symbol: symbol, HedgeMode: true, MarginType: "ISOLATED", Leverage: 15, AutoAddMargin: false, SettingsMatchV1: true, AsOf: now.Format(time.RFC3339Nano)},
	}, nil
}

func TestExecutionPreflightDiagnosticIsReadOnlyAndReportsSignedCapability(t *testing.T) {
	s, err := newWithFetcher(t.TempDir(), executionPreflightMock{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	h := s.Handler()
	token := bootstrapToken(t, h)
	body := `{"api_key":"demo-only-key","secret_key":"demo-only-secret","passphrase":"long-passphrase-to-encrypt"}`
	if r := request(h, "POST", "/api/v1/credentials/demo/save", token, "http://127.0.0.1:8080", body); r.Code != http.StatusOK {
		t.Fatalf("auth %d %s", r.Code, r.Body.String())
	}
	if r := request(h, "POST", "/api/v1/trading/execution-preflight", token, "http://127.0.0.1:8080", `{"confirmation":"wrong"}`); r.Code != http.StatusBadRequest {
		t.Fatalf("bad confirmation accepted %d", r.Code)
	}
	r := request(h, "POST", "/api/v1/trading/execution-preflight", token, "http://127.0.0.1:8080", `{"confirmation":"RUN READ ONLY DEMO EXECUTION PREFLIGHT"}`)
	if r.Code != http.StatusOK {
		t.Fatalf("preflight failed %d %s", r.Code, r.Body.String())
	}
	var out map[string]any
	if err := json.Unmarshal(r.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out["can_trade"] != true || out["settings_match_v1"] != true || out["trading_authorized"] != false || out["mutation_performed"] != false {
		t.Fatalf("unsafe/unexpected response %+v", out)
	}
	if out["open_order_count"].(float64) != 0 || out["open_algo_count"].(float64) != 0 || out["nonzero_position_count"].(float64) != 0 {
		t.Fatalf("expected flat account %+v", out)
	}
}
