package trading

import (
	"binance_trader/internal/exchange"
	"testing"
	"time"
)

func TestPooledSideExitExposureIsNeverOCO(t *testing.T) {
	now := time.Now().UTC()
	b, owned, expected := portfolioFixture(now)
	got := EvaluatePooledExitRisk(b, owned, expected, true, now)
	if got.State != "NON_ATOMIC_EXIT_RISK" || got.TradingAuthorized || !got.ManualReviewRequired || len(got.Sides) != 2 {
		t.Fatalf("false OCO assumption: %+v", got)
	}
	long, short := got.Sides[0], got.Sides[1]
	if long.Side != Long || long.IndependentOwners != 2 || long.SignedPositionBTC != "0.03" ||
		long.ActiveTPBTC != "0.03" || long.ActiveSLBTC != "0.03" ||
		long.TotalPossibleCloseRequestsBTC != "0.06" || long.ExcessRequestsIfAllTriggerBTC != "0.03" {
		t.Fatalf("pooled LONG exposure %+v", long)
	}
	if short.Side != Short || short.IndependentOwners != 1 || short.SignedPositionBTC != "0.005" ||
		short.TotalPossibleCloseRequestsBTC != "0.01" || short.ExcessRequestsIfAllTriggerBTC != "0.005" {
		t.Fatalf("SHORT exposure %+v", short)
	}
}
func TestPooledSideExposureBlocksMissingOrForeignInventory(t *testing.T) {
	now := time.Now().UTC()
	for _, tc := range []struct {
		name    string
		change  func(*exchange.RecoveryBaseline)
		audited bool
	}{
		{"missing TP after it possibly triggered", func(b *exchange.RecoveryBaseline) { b.OpenAlgos = b.OpenAlgos[1:] }, true},
		{"incomplete signed observation", func(b *exchange.RecoveryBaseline) { b.OpenAlgos = nil }, true},
		{"external order", func(b *exchange.RecoveryBaseline) {
			b.OpenOrders = append(b.OpenOrders, exchange.Order{OrderID: 88, Symbol: "BTCUSDT"})
		}, true},
		{"unmatched side quantity", func(b *exchange.RecoveryBaseline) { b.Portfolio.Positions[0].PositionAmt = "0.02" }, true},
		{"audit missing", nil, false},
		{"stale", func(b *exchange.RecoveryBaseline) { b.FinishedAt = now.Add(-8 * time.Second) }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, o, e := portfolioFixture(now)
			if tc.change != nil {
				tc.change(&b)
			}
			x := EvaluatePooledExitRisk(b, o, e, tc.audited, now)
			if x.State != "BLOCKED" || x.TradingAuthorized || len(x.Blockers) == 0 || len(x.Sides) != 0 {
				t.Fatalf("unsafe: %+v", x)
			}
		})
	}
}
func TestPooledSideRiskEmptyStillNotPermit(t *testing.T) {
	now := time.Now().UTC()
	b, _, _ := portfolioFixture(now)
	b.Portfolio.Positions = []exchange.PortfolioPosition{}
	b.OpenAlgos = []exchange.AlgoOrder{}
	r := EvaluatePooledExitRisk(b, nil, nil, true, now)
	if r.State != "EMPTY" || r.TradingAuthorized || !r.ManualReviewRequired || len(r.Sides) != 0 {
		t.Fatalf("empty assumption %+v", r)
	}
}
