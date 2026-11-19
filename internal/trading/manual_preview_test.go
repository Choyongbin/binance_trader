package trading

import (
	"binance_trader/internal/exchange"
	"testing"
	"time"
)

func manualPreviewFixture(now time.Time) (ManualPreviewInput, exchange.SymbolRules, Quote, exchange.Snapshot, exchange.TradingConfiguration) {
	in := ManualPreviewInput{Side: Long, OrderType: Market, Leverage: 15, MarginUSDT: "100"}
	rules := sizingRules(now)
	q := Quote{Symbol: "BTCUSDT", Price: "100000", ReceivedAt: now}
	snap := exchange.Snapshot{Environment: "demo", Status: "connected", AsOf: now.Format(time.RFC3339Nano), Balances: []exchange.Balance{{Asset: "USDT", Wallet: "5000", Available: "200"}}, Positions: []exchange.Position{}, Orders: []exchange.Order{}, AlgoOrders: []exchange.AlgoOrder{}}
	cfg := exchange.TradingConfiguration{Environment: "demo", Symbol: "BTCUSDT", HedgeMode: true, MarginType: "ISOLATED", Leverage: 15, AsOf: now.Format(time.RFC3339Nano)}
	return in, rules, q, snap, cfg
}
func TestManualPreviewIndicativeOnly(t *testing.T) {
	now := time.Now().UTC()
	in, r, q, a, c := manualPreviewFixture(now)
	for _, side := range []PositionSide{Long, Short} {
		in.Side = side
		got, err := PreviewManualMarket(in, r, q, a, c, "0", now)
		if err != nil || got.QuantityBTC == "" || got.CanPlaceOrder || got.State != "ESTIMATE_ONLY" || got.MarginBudgetUSDT != "100" {
			t.Fatalf("preview side %s: %+v %v", side, got, err)
		}
	}
}
func TestManualPreviewRejectsUnsafeConditions(t *testing.T) {
	now := time.Now().UTC()
	cases := []struct {
		name string
		edit func(*ManualPreviewInput, *exchange.SymbolRules, *Quote, *exchange.Snapshot, *exchange.TradingConfiguration)
	}{
		{"wrong leverage", func(i *ManualPreviewInput, r *exchange.SymbolRules, q *Quote, a *exchange.Snapshot, c *exchange.TradingConfiguration) {
			i.Leverage = 20
		}},
		{"not hedge", func(i *ManualPreviewInput, r *exchange.SymbolRules, q *Quote, a *exchange.Snapshot, c *exchange.TradingConfiguration) {
			c.HedgeMode = false
		}},
		{"wrong margin mode", func(i *ManualPreviewInput, r *exchange.SymbolRules, q *Quote, a *exchange.Snapshot, c *exchange.TradingConfiguration) {
			c.MarginType = "CROSSED"
		}},
		{"stale config", func(i *ManualPreviewInput, r *exchange.SymbolRules, q *Quote, a *exchange.Snapshot, c *exchange.TradingConfiguration) {
			c.AsOf = now.Add(-time.Minute).Format(time.RFC3339Nano)
		}},
		{"stale account", func(i *ManualPreviewInput, r *exchange.SymbolRules, q *Quote, a *exchange.Snapshot, c *exchange.TradingConfiguration) {
			a.AsOf = now.Add(-10 * time.Second).Format(time.RFC3339Nano)
		}},
		{"stale WS", func(i *ManualPreviewInput, r *exchange.SymbolRules, q *Quote, a *exchange.Snapshot, c *exchange.TradingConfiguration) {
			q.ReceivedAt = now.Add(-5 * time.Second)
		}},
		{"existing owner", func(i *ManualPreviewInput, r *exchange.SymbolRules, q *Quote, a *exchange.Snapshot, c *exchange.TradingConfiguration) {
			a.Positions = append(a.Positions, exchange.Position{Symbol: "BTCUSDT", Side: "LONG", Qty: "0.01"})
		}},
		{"existing algo", func(i *ManualPreviewInput, r *exchange.SymbolRules, q *Quote, a *exchange.Snapshot, c *exchange.TradingConfiguration) {
			a.AlgoOrders = append(a.AlgoOrders, exchange.AlgoOrder{Symbol: "BTCUSDT", AlgoID: 1})
		}},
		{"no signed account", func(i *ManualPreviewInput, r *exchange.SymbolRules, q *Quote, a *exchange.Snapshot, c *exchange.TradingConfiguration) {
			a.Status = "locked"
		}},
		{"over available", func(i *ManualPreviewInput, r *exchange.SymbolRules, q *Quote, a *exchange.Snapshot, c *exchange.TradingConfiguration) {
			i.MarginUSDT = "201"
		}},
		{"invalid amount", func(i *ManualPreviewInput, r *exchange.SymbolRules, q *Quote, a *exchange.Snapshot, c *exchange.TradingConfiguration) {
			i.MarginUSDT = "100xyz"
		}},
		{"limit unsupported", func(i *ManualPreviewInput, r *exchange.SymbolRules, q *Quote, a *exchange.Snapshot, c *exchange.TradingConfiguration) {
			i.OrderType = Limit
		}},
		{"stale rules", func(i *ManualPreviewInput, r *exchange.SymbolRules, q *Quote, a *exchange.Snapshot, c *exchange.TradingConfiguration) {
			r.FetchedAt = now.Add(-time.Minute - time.Second)
		}},
		{"real snapshot", func(i *ManualPreviewInput, r *exchange.SymbolRules, q *Quote, a *exchange.Snapshot, c *exchange.TradingConfiguration) {
			a.Environment = "real"
		}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			i, r, q, a, c := manualPreviewFixture(now)
			tt.edit(&i, &r, &q, &a, &c)
			v, e := PreviewManualMarket(i, r, q, a, c, "0", now)
			if e == nil || v.CanPlaceOrder {
				t.Fatalf("accepted %+v", v)
			}
		})
	}
}

func TestManualPreviewRespectInternalAutoWallet(t *testing.T) {
	now := time.Now().UTC()
	in, r, q, a, c := manualPreviewFixture(now)
	if _, err := PreviewManualMarket(in, r, q, a, c, "150", now); err == nil {
		t.Fatal("manual preview spent auto reserved wallet")
	}
	if _, err := PreviewManualMarket(in, r, q, a, c, "100", now); err != nil {
		t.Fatalf("manual budget should fit remaining 100: %v", err)
	}
	if _, err := PreviewManualMarket(in, r, q, a, c, "bogus", now); err == nil {
		t.Fatal("invalid virtual budget accepted")
	}
}

func TestManualLimitServerPreviewIsEstimateOnlyAndExactTick(t *testing.T) {
	now := time.Now().UTC()
	r := sizingRules(now)
	q := Quote{Symbol: "BTCUSDT", Price: "100000", ReceivedAt: now}
	a := accountSnapshot(now, "0", "0")
	a.Balances = []exchange.Balance{{Asset: "USDT", Wallet: "5000", Available: "1000"}}
	c := exchange.TradingConfiguration{Environment: "demo", Symbol: "BTCUSDT", HedgeMode: true, MarginType: "ISOLATED", Leverage: 15, AsOf: now.Format(time.RFC3339Nano)}
	in := ManualPreviewInput{Side: Short, OrderType: Limit, Leverage: 15, MarginUSDT: "100", LimitPrice: "99999.9"}
	preview, err := PreviewManualMarket(in, r, q, a, c, "50", now)
	if err != nil || preview.State != "ESTIMATE_ONLY" || preview.CanPlaceOrder || preview.QuantityBTC == "" || preview.IndicativePriceUSDT != "99999.9" {
		t.Fatalf("invalid LIMIT preview %+v %v", preview, err)
	}
	for _, price := range []string{"99999.95", "1000000.1", "NaN", "0", "1e9"} {
		in.LimitPrice = price
		if _, err := PreviewManualMarket(in, r, q, a, c, "50", now); err == nil {
			t.Fatalf("invalid limit %q accepted", price)
		}
	}
	in.LimitPrice = "99999.9"
	q.ReceivedAt = now.Add(-10 * time.Second)
	if _, err := PreviewManualMarket(in, r, q, a, c, "50", now); err == nil {
		t.Fatal("stale quote accepted")
	}
	q.ReceivedAt = now
	a.Balances[0].Available = "130"
	if _, err := PreviewManualMarket(in, r, q, a, c, "50", now); err == nil {
		t.Fatal("auto-wallet allocation bypass")
	}
	in.OrderType = Market
	if _, err := PreviewManualMarket(in, r, q, a, c, "50", now); err == nil {
		t.Fatal("market with limit price accepted")
	}
}
