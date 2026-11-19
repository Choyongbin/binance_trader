package server

import (
	"binance_trader/internal/exchange"
	"binance_trader/internal/trading"
	"context"
	"net/http"
	"strings"
	"testing"
	"time"
)

type previewMock struct {
	ruleCalls   int
	configCalls int
}

func (*previewMock) Fetch(context.Context, string, string) (exchange.Snapshot, error) {
	return exchange.Snapshot{Environment: "demo", Status: "connected", AsOf: time.Now().UTC().Format(time.RFC3339Nano), Balances: []exchange.Balance{{Asset: "USDT", Wallet: "1000", Available: "500"}}, Positions: []exchange.Position{}, Orders: []exchange.Order{}, AlgoOrders: []exchange.AlgoOrder{}}, nil
}
func (p *previewMock) InspectTradingConfiguration(context.Context, string, string, string) (exchange.TradingConfiguration, error) {
	p.configCalls++
	return exchange.TradingConfiguration{Environment: "demo", Symbol: "BTCUSDT", HedgeMode: true, MarginType: "ISOLATED", Leverage: 15, AsOf: time.Now().UTC().Format(time.RFC3339Nano)}, nil
}
func (p *previewMock) FetchSymbolRules(ctx context.Context, symbol string) (exchange.SymbolRules, error) {
	p.ruleCalls++
	// Fixture used by trading tests, copied directly here without importing
	// unexported functions from a different Go package.
	return exchange.ParseSymbolRules([]byte(`{"symbols":[{"symbol":"BTCUSDT","status":"TRADING","contractType":"PERPETUAL","quoteAsset":"USDT","marginAsset":"USDT","filters":[{"filterType":"LOT_SIZE","minQty":"0.001","maxQty":"100","stepSize":"0.001"},{"filterType":"MARKET_LOT_SIZE","minQty":"0.001","maxQty":"100","stepSize":"0.001"},{"filterType":"PRICE_FILTER","minPrice":"100","maxPrice":"1000000","tickSize":"0.1"},{"filterType":"MIN_NOTIONAL","notional":"5"}]}]}`), symbol, time.Now().UTC())
}
func TestReadOnlyManualPreviewRoute(t *testing.T) {
	remote := &previewMock{}
	s, err := newWithFetcher(t.TempDir(), remote)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.manualQuote = func() (trading.Quote, error) {
		return trading.Quote{Symbol: "BTCUSDT", Price: "100000", ReceivedAt: time.Now().UTC()}, nil
	}
	h := s.Handler()
	token := bootstrapToken(t, h)
	body := `{"side":"LONG","order_type":"MARKET","leverage":15,"margin_usdt":"100"}`
	got := request(h, "POST", "/api/v1/trading/manual/preview", token, "http://127.0.0.1:8080", body)
	if got.Code != 423 || remote.configCalls != 0 {
		t.Fatalf("unauthed %d", got.Code)
	}
	if err = s.demo.Connect(context.Background(), "test-key", "test-secret"); err != nil {
		t.Fatal(err)
	}
	got = request(h, "POST", "/api/v1/trading/manual/preview", "bad", "http://127.0.0.1:8080", body)
	if got.Code != 403 || remote.configCalls != 0 {
		t.Fatal("csrf", got.Body)
	}
	got = request(h, "POST", "/api/v1/trading/manual/preview", token, "http://127.0.0.1:8080", body)
	if got.Code != 200 || !strings.Contains(got.Body.String(), `"state":"ESTIMATE_ONLY"`) || !strings.Contains(got.Body.String(), `"can_place_order":false`) {
		t.Fatalf("invalid preview %d %s", got.Code, got.Body)
	}
	if remote.configCalls != 1 || remote.ruleCalls != 1 {
		t.Fatalf("unexpected signed/public reads %v", remote)
	}
	order := request(h, "POST", "/api/v1/trading/manual/orders", token, "http://127.0.0.1:8080", body)
	if order.Code != http.StatusLocked {
		t.Fatalf("unlocked order endpoint: %d", order.Code)
	}
	got = request(h, "POST", "/api/v1/trading/manual/preview", token, "http://127.0.0.1:8080", `{"side":"LONG","order_type":"LIMIT","leverage":15,"margin_usdt":"100"}`)
	if got.Code != 400 || remote.configCalls != 1 {
		t.Fatalf("limit should not trigger reads %d", got.Code)
	}
	got = request(h, "POST", "/api/v1/trading/manual/preview", token, "http://127.0.0.1:8080", `{"side":"SHORT","order_type":"LIMIT","leverage":15,"margin_usdt":"100","limit_price":"99999.9"}`)
	if got.Code != 200 || !strings.Contains(got.Body.String(), `"indicative_price_usdt":"99999.9"`) || !strings.Contains(got.Body.String(), `"can_place_order":false`) || remote.configCalls != 2 {
		t.Fatalf("read only LIMIT preview failed %d %s", got.Code, got.Body)
	}
	got = request(h, "POST", "/api/v1/trading/manual/preview", token, "http://127.0.0.1:8080", `{"side":"SHORT","order_type":"MARKET","leverage":15,"margin_usdt":"100","limit_price":"99999.9"}`)
	if got.Code != 400 || remote.configCalls != 2 {
		t.Fatalf("market with LIMIT field accepted: %d", got.Code)
	}
	got = request(h, "POST", "/api/v1/trading/manual/orders", token, "http://127.0.0.1:8080", `{"type":"MARKET"}`)
	if got.Code != 423 {
		t.Fatalf("locked order endpoint opened: %d", got.Code)
	}
}

func TestServerManualPreviewRespectsAutoWalletAllocation(t *testing.T) {
	mock := &previewMock{}
	s, err := newWithFetcher(t.TempDir(), mock)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.manualQuote = func() (trading.Quote, error) {
		return trading.Quote{Symbol: "BTCUSDT", Price: "100000", ReceivedAt: time.Now().UTC()}, nil
	}
	if err = s.demo.Connect(context.Background(), "test-key", "test-secret"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.wallet.Change("alloconce0001", "ALLOCATE", "450", "500", s.demo.KeyFingerprint()); err != nil {
		t.Fatal(err)
	}
	token := bootstrapTokenForConnected(t, s.Handler())
	body := `{"side":"SHORT","order_type":"MARKET","leverage":15,"margin_usdt":"100"}`
	got := request(s.Handler(), "POST", "/api/v1/trading/manual/preview", token, "http://127.0.0.1:8080", body)
	if got.Code != 409 || !strings.Contains(got.Body.String(), "자동매매") {
		t.Fatalf("preview spent auto wallet: %d %s", got.Code, got.Body)
	}
}
