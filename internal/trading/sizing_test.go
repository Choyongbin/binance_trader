package trading

import (
	"binance_trader/internal/exchange"
	"math/big"
	"strings"
	"testing"
	"time"
)

func sizingRules(now time.Time) exchange.SymbolRules {
	return exchange.SymbolRules{Symbol: "BTCUSDT", Status: "TRADING", ContractType: "PERPETUAL", QuoteAsset: "USDT", MarginAsset: "USDT", FetchedAt: now, Lot: exchange.QuantityFilter{Min: "0.001", Max: "100", Step: "0.001"}, MarketLot: exchange.QuantityFilter{Min: "0.002", Max: "20", Step: "0.002"}, Price: exchange.PriceFilter{Min: "0.1", Max: "1000000", Tick: "0.1"}, MinNotional: "100"}
}
func TestExactSizingFeesBuffersAndFilterGrid(t *testing.T) {
	now := time.Now()
	r := sizingRules(now)
	in := EntryBudget{AvailableUSDT: "5000.0713", SharePercent: 10, Leverage: 15, FeeReserveBPS: 20, PriceBufferBPS: 100, Quote: Quote{Symbol: "BTCUSDT", Price: "84510.6", ReceivedAt: now}, Now: now}
	got, err := SizeMarketEntry(r, in)
	if err != nil {
		t.Fatal(err)
	}
	qty, err := pos(got.Quantity)
	if err != nil {
		t.Fatal(err)
	}
	// Compare with exact computation rather than a rounded display of 1/15.
	exactDenom := ratMul(ratMul(mustRat(t, in.Quote.Price), mustRat(t, "1.01")), mustRatFrac(t, 103, 1500)) // (1/15 + .002) = 103/1500
	trueMax := ratDiv(ratMul(mustRat(t, in.AvailableUSDT), mustRat(t, "0.1")), exactDenom)
	if qty.Cmp(trueMax) > 0 {
		t.Fatalf("rounded UP quantity %s > %s", got.Quantity, trueMax.RatString())
	}
	if ok, _ := onGrid(qty, "0.002"); !ok {
		t.Fatalf("bad market step %s", got.Quantity)
	}
	if got.Quantity == "0" || got.AllocatedMarginUSDT != "500.00713" {
		t.Fatalf("unexpected result %+v", got)
	}
	for _, test := range []struct {
		name string
		edit func(*EntryBudget)
	}{
		{"stale", func(b *EntryBudget) { b.Quote.ReceivedAt = now.Add(-4 * time.Second) }},
		{"future", func(b *EntryBudget) { b.Quote.ReceivedAt = now.Add(2 * time.Second) }},
		{"zero", func(b *EntryBudget) { b.AvailableUSDT = "0" }},
		{"NaN", func(b *EntryBudget) { b.Quote.Price = "NaN" }},
		{"exponent", func(b *EntryBudget) { b.Quote.Price = "8.451e4" }},
		{"fee missing", func(b *EntryBudget) { b.FeeReserveBPS = 0 }},
		{"wrong symbol", func(b *EntryBudget) { b.Quote.Symbol = "ETHUSDT" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			broken := in
			test.edit(&broken)
			if _, err := SizeMarketEntry(r, broken); err == nil {
				t.Fatal("fail-closed check bypassed")
			}
		})
	}
	r.MinNotional = "99999"
	if _, err := SizeMarketEntry(r, in); err == nil || !strings.Contains(err.Error(), "명목") {
		t.Fatalf("tiny order must fail %v", err)
	}
}
func mustRat(t *testing.T, s string) *big.Rat {
	t.Helper()
	r, e := pos(s)
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func mustRatFrac(t *testing.T, n, d int64) *big.Rat { t.Helper(); return big.NewRat(n, d) }
func TestOrderRulesMarketAndLimit(t *testing.T) {
	now := time.Now()
	r := sizingRules(now)
	plan := PlannedOrder{ClientOrderID: "bt_0123456789012345678901234567", Symbol: "BTCUSDT", PositionSide: Long, ExchangeSide: "BUY", Type: Market, Quantity: "0.004"}
	if err := ValidatePlannedOrder(plan, r, "84510.6", now); err != nil {
		t.Fatal(err)
	}
	for _, quantity := range []string{"0.003", "0.001", "0.000", "1000", "1e2"} {
		broken := plan
		broken.Quantity = quantity
		if err := ValidatePlannedOrder(broken, r, "84510.6", now); err == nil {
			t.Fatalf("accepted %s", quantity)
		}
	}
	plan.Type = Limit
	plan.Price = "84510.6"
	plan.TimeInForce = "GTC"
	plan.Quantity = "0.003"
	if err := ValidatePlannedOrder(plan, r, plan.Price, now); err != nil {
		t.Fatal(err)
	}
	plan.Price = "84510.65"
	if err := ValidatePlannedOrder(plan, r, plan.Price, now); err == nil {
		t.Fatal("accepted off-tick limit")
	}
	plan.Price = "84510.6"
	plan.TimeInForce = ""
	if err := ValidatePlannedOrder(plan, r, plan.Price, now); err == nil {
		t.Fatal("GTC omitted")
	}
	plan.TimeInForce = "GTC"
	r.FetchedAt = now.Add(-2 * time.Minute)
	if err := ValidatePlannedOrder(plan, r, plan.Price, now); err == nil {
		t.Fatal("stale exchange rules accepted")
	}
}

func TestLimitNotionalIgnoresMarketReference(t *testing.T) {
	now := time.Now()
	r := sizingRules(now)
	r.MinNotional = "150"
	p := PlannedOrder{ClientOrderID: "limit_notional_test", Symbol: "BTCUSDT", PositionSide: Long, ExchangeSide: "BUY", Type: Limit, Quantity: "0.002", Price: "50000", TimeInForce: "GTC"}
	if err := ValidatePlannedOrder(p, r, "100000", now); err == nil {
		t.Fatal("market reference incorrectly satisfied limit minimum")
	}
	p.Price = "100000"
	if err := ValidatePlannedOrder(p, r, "50000", now); err != nil {
		t.Fatalf("valid limit rejected due to market reference: %v", err)
	}
}
