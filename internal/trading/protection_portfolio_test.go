package trading

import (
	"binance_trader/internal/exchange"
	"testing"
	"time"
)

func portfolioFixture(now time.Time) (exchange.RecoveryBaseline, []OwnerPosition, []ExpectedProtection) {
	owners := []OwnerPosition{
		{Source: Model, ModelID: "trend", Symbol: "BTCUSDT", Side: Long, Quantity: "0.01"},
		{Source: Model, ModelID: "counter", Symbol: "BTCUSDT", Side: Long, Quantity: "0.02"},
		{Source: Manual, Symbol: "BTCUSDT", Side: Short, Quantity: "0.005"},
	}
	expected := []ExpectedProtection{
		{Source: Model, ModelID: "trend", Side: Long, Quantity: "0.01", TPClientAlgoID: "tp1", SLClientAlgoID: "sl1"},
		{Source: Model, ModelID: "counter", Side: Long, Quantity: "0.02", TPClientAlgoID: "tp2", SLClientAlgoID: "sl2"},
		{Source: Manual, Side: Short, Quantity: "0.005", TPClientAlgoID: "tp3", SLClientAlgoID: "sl3"},
	}
	algos := []exchange.AlgoOrder{}
	for i, p := range expected {
		orderSide := "SELL"
		if p.Side == Short {
			orderSide = "BUY"
		}
		for j, id := range []string{p.TPClientAlgoID, p.SLClientAlgoID} {
			typ, work := "TAKE_PROFIT_MARKET", "CONTRACT_PRICE"
			if j == 1 {
				typ, work = "STOP_MARKET", "MARK_PRICE"
			}
			algos = append(algos, exchange.AlgoOrder{ClientAlgoID: id, AlgoID: int64(i*2 + j + 1), Symbol: "BTCUSDT", Side: orderSide, PositionSide: string(p.Side), OrderType: typ, WorkingType: work, AlgoStatus: "NEW", Quantity: p.Quantity, TriggerPrice: "100000"})
		}
	}
	started := now.Add(-2 * time.Second)
	return exchange.RecoveryBaseline{
		Environment: "demo", Coverage: "BOUNDED_HISTORY_ONLY_NOT_CONTINUOUS", StartedAt: started,
		FinishedAt: now, WindowStartMS: started.Add(-time.Hour).UnixMilli(), WindowEndMS: started.UnixMilli(),
		Portfolio: exchange.AccountPortfolio{CanTrade: true, TotalWalletBalance: "5000", TotalUnrealizedProfit: "0", TotalMarginBalance: "5000", TotalInitialMargin: "200", TotalPositionInitialMargin: "200", TotalOpenOrderInitialMargin: "0", AvailableBalance: "4800",
			Assets:    []exchange.PortfolioAsset{{Asset: "USDT", WalletBalance: "5000", AvailableBalance: "4800", MarginBalance: "5000"}},
			Positions: []exchange.PortfolioPosition{{Symbol: "BTCUSDT", PositionSide: "LONG", PositionAmt: "0.03", InitialMargin: "150", Isolated: true}, {Symbol: "BTCUSDT", PositionSide: "SHORT", PositionAmt: "-0.005", InitialMargin: "50", Isolated: true}},
		}, OpenOrders: []exchange.Order{}, OpenAlgos: algos, RecentBTCOrders: []exchange.Order{}, RecentBTCAlgos: []exchange.AlgoOrder{}, RecentBTCTrades: []exchange.AccountTrade{}, RecentIncome: []exchange.IncomeRecord{},
	}, owners, expected
}
func TestPortfolioProtectionAggregateHedgeMultipleOwners(t *testing.T) {
	now := time.Now().UTC()
	b, owned, expected := portfolioFixture(now)
	result := ReviewProtectionPortfolio(b, owned, expected, true, now)
	if result.State != "OBSERVED_MATCH_NOT_OCO" || result.TradingAuthorized || !result.ManualReviewRequired || result.LongQty != "0.03" || result.ShortQty != "0.005" || result.OwnerCount != 3 || result.ActiveAlgoCount != 6 {
		t.Fatalf("unexpected proof: %+v", result)
	}
}
func TestPortfolioProtectionFailClosedVariants(t *testing.T) {
	now := time.Now().UTC()
	cases := []struct {
		name   string
		change func(*exchange.RecoveryBaseline, *[]OwnerPosition, *[]ExpectedProtection)
		audit  bool
	}{
		{"unaudited", nil, false},
		{"missing btc short", func(b *exchange.RecoveryBaseline, o *[]OwnerPosition, e *[]ExpectedProtection) {
			b.Portfolio.Positions[1].PositionAmt = "0"
		}, true},
		{"external same-side", func(b *exchange.RecoveryBaseline, o *[]OwnerPosition, e *[]ExpectedProtection) {
			b.Portfolio.Positions[0].PositionAmt = "0.031"
		}, true},
		{"foreign eth", func(b *exchange.RecoveryBaseline, o *[]OwnerPosition, e *[]ExpectedProtection) {
			b.Portfolio.Positions = append(b.Portfolio.Positions, exchange.PortfolioPosition{Symbol: "ETHUSDT", PositionSide: "LONG", PositionAmt: "1", InitialMargin: "100"})
		}, true},
		{"one normal order", func(b *exchange.RecoveryBaseline, o *[]OwnerPosition, e *[]ExpectedProtection) {
			b.OpenOrders = append(b.OpenOrders, exchange.Order{Symbol: "BTCUSDT", OrderID: 99})
		}, true},
		{"foreign algo", func(b *exchange.RecoveryBaseline, o *[]OwnerPosition, e *[]ExpectedProtection) {
			b.OpenAlgos = append(b.OpenAlgos, exchange.AlgoOrder{ClientAlgoID: "external", AlgoID: 99, Symbol: "BTCUSDT", AlgoStatus: "NEW"})
		}, true},
		{"wrong TP trigger source", func(b *exchange.RecoveryBaseline, o *[]OwnerPosition, e *[]ExpectedProtection) {
			b.OpenAlgos[0].WorkingType = "MARK_PRICE"
		}, true},
		{"stale", func(b *exchange.RecoveryBaseline, o *[]OwnerPosition, e *[]ExpectedProtection) {
			b.FinishedAt = now.Add(-7 * time.Second)
		}, true},
		{"nil algo incomplete", func(b *exchange.RecoveryBaseline, o *[]OwnerPosition, e *[]ExpectedProtection) { b.OpenAlgos = nil }, true},
		{"nil normal order incomplete", func(b *exchange.RecoveryBaseline, o *[]OwnerPosition, e *[]ExpectedProtection) { b.OpenOrders = nil }, true},
		{"stale baseline false interval", func(b *exchange.RecoveryBaseline, o *[]OwnerPosition, e *[]ExpectedProtection) { b.WindowEndMS++ }, true},
		{"missing owner", func(b *exchange.RecoveryBaseline, o *[]OwnerPosition, e *[]ExpectedProtection) { *o = (*o)[1:] }, true},
		{"duplicate owner", func(b *exchange.RecoveryBaseline, o *[]OwnerPosition, e *[]ExpectedProtection) {
			*o = append(*o, (*o)[0])
		}, true},
		{"unknown source", func(b *exchange.RecoveryBaseline, o *[]OwnerPosition, e *[]ExpectedProtection) {
			(*o)[0].Source = Source("external")
		}, true},
		{"cross collateral", func(b *exchange.RecoveryBaseline, o *[]OwnerPosition, e *[]ExpectedProtection) {
			b.Portfolio.MultiAssetsMargin = true
		}, true},
		{"not tradable", func(b *exchange.RecoveryBaseline, o *[]OwnerPosition, e *[]ExpectedProtection) {
			b.Portfolio.CanTrade = false
		}, true},
		{"BOTH nonzero", func(b *exchange.RecoveryBaseline, o *[]OwnerPosition, e *[]ExpectedProtection) {
			b.Portfolio.Positions = append(b.Portfolio.Positions, exchange.PortfolioPosition{Symbol: "BTCUSDT", PositionSide: "BOTH", PositionAmt: "1", InitialMargin: "50"})
		}, true},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			b, o, e := portfolioFixture(now)
			if tt.change != nil {
				tt.change(&b, &o, &e)
			}
			r := ReviewProtectionPortfolio(b, o, e, tt.audit, now)
			if r.State != "BLOCKED" || r.TradingAuthorized || len(r.Blockers) == 0 {
				t.Fatalf("not blocked %+v", r)
			}
		})
	}
}
func TestProtectionEmptyStillDetectsUnprotectedOwner(t *testing.T) {
	now := time.Now().UTC()
	s := exchange.Snapshot{Environment: "demo", Status: "connected", AsOf: now.Format(time.RFC3339Nano), AlgoOrders: []exchange.AlgoOrder{}}
	p := OwnerPosition{Symbol: "BTCUSDT", Source: Manual, Side: Long, Quantity: "0.01"}
	if r := ReviewProtectionInventory(s, []OwnerPosition{p}, nil, now); r.Status != "BLOCKED" {
		t.Fatalf("unprotected owner erroneously empty %+v", r)
	}
	if r := ReviewProtectionInventory(s, nil, nil, now); r.Status != "EMPTY" {
		t.Fatalf("empty invalid %+v", r)
	}
}
func TestPortfolioEmptyNotTradePermit(t *testing.T) {
	now := time.Now().UTC()
	b, _, _ := portfolioFixture(now)
	b.Portfolio.Positions = []exchange.PortfolioPosition{}
	b.Portfolio.TotalInitialMargin = "0"
	b.Portfolio.TotalPositionInitialMargin = "0"
	b.OpenAlgos = []exchange.AlgoOrder{}
	r := ReviewProtectionPortfolio(b, nil, nil, true, now)
	if r.State != "EMPTY" || r.TradingAuthorized || !r.ManualReviewRequired {
		t.Fatalf("empty authorization %+v", r)
	}
}
