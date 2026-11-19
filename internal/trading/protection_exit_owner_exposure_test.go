package trading

import (
	"binance_trader/internal/exchange"
	"context"
	"testing"
	"time"
)

func ownerExitFixture(t *testing.T) (ProtectionDispatchPlan, SignedExitFillReview, []OwnerPosition, exchange.RecoveryBaseline, time.Time) {
	t.Helper()
	subs, reader, tp, sl := signedExitFixture(t)
	plan, err := subs.read("group-p-001")
	if err != nil {
		t.Fatal(err)
	}
	exit, err := InspectSignedExitFills(context.Background(), subs, reader, DemoCredentials{APIKey: "mock", Secret: "mock"}, plan.GroupID, tp, sl)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	b, _, _ := portfolioFixture(now)
	b.OpenAlgos = []exchange.AlgoOrder{}
	b.Portfolio.Positions = []exchange.PortfolioPosition{{Symbol: "BTCUSDT", PositionSide: string(plan.PositionSide), PositionAmt: "0", InitialMargin: "0", Isolated: true}}
	owned := []OwnerPosition{{Source: plan.Source, ModelID: plan.ModelID, Symbol: "BTCUSDT", Side: plan.PositionSide, Quantity: plan.TP.Quantity}}
	return plan, exit, owned, b, now
}

func TestExitOwnerExposureOnlyObservesNeverSettles(t *testing.T) {
	plan, exit, owned, b, now := ownerExitFixture(t)
	got, err := ReviewExitOwnerExposure(plan, exit, owned, b, now)
	if err != nil || got.State != "SINGLE_OWNER_FLAT_OBSERVED_NOT_SETTLED" || got.FinanciallySettled || got.TradingAuthorized || !got.ManualReviewRequired || len(got.Blockers) == 0 {
		t.Fatalf("result %+v err %v", got, err)
	}
	got2, err := ReviewExitOwnerExposure(plan, exit, owned, b, now)
	if err != nil || got2.State != got.State || got2.TradingAuthorized || got2.FinanciallySettled {
		t.Fatalf("repeat %+v %v", got2, err)
	}
}

func TestExitOwnerExposurePooledAndResidual(t *testing.T) {
	plan, exit, owned, b, now := ownerExitFixture(t)
	other := OwnerPosition{Source: Manual, Symbol: "BTCUSDT", Side: plan.PositionSide, Quantity: "0.02"}
	if plan.Source == Manual {
		other.Source = Model
		other.ModelID = "other"
	}
	owned = append(owned, other)
	b.Portfolio.Positions[0].PositionAmt = "0.02"
	if plan.PositionSide == Short {
		b.Portfolio.Positions[0].PositionAmt = "-0.02"
	}
	got, err := ReviewExitOwnerExposure(plan, exit, owned, b, now)
	if err != nil || got.State != "POOLED_SIDE_AMBIGUOUS_NOT_SETTLED" || got.OtherOwnerQty != "0.02" || got.TradingAuthorized || got.FinanciallySettled {
		t.Fatalf("pooled %+v err %v", got, err)
	}
	owned = owned[:1]
	got, err = ReviewExitOwnerExposure(plan, exit, owned, b, now)
	if err != nil || got.State != "EXCHANGE_SIDE_NOT_FLAT_NOT_SETTLED" {
		t.Fatalf("residual %+v %v", got, err)
	}
}

func TestExitOwnerExposureFailClosed(t *testing.T) {
	tests := []struct {
		name   string
		change func(*ProtectionDispatchPlan, *SignedExitFillReview, *[]OwnerPosition, *exchange.RecoveryBaseline, time.Time)
	}{
		{"wrong group", func(_ *ProtectionDispatchPlan, e *SignedExitFillReview, _ *[]OwnerPosition, _ *exchange.RecoveryBaseline, _ time.Time) {
			e.GroupID = "not-this-group"
		}},
		{"fake settled", func(_ *ProtectionDispatchPlan, e *SignedExitFillReview, _ *[]OwnerPosition, _ *exchange.RecoveryBaseline, _ time.Time) {
			e.FinanciallySettled = true
		}},
		{"partial", func(_ *ProtectionDispatchPlan, e *SignedExitFillReview, _ *[]OwnerPosition, _ *exchange.RecoveryBaseline, _ time.Time) {
			e.TotalFilledQty = "0.004"
		}},
		{"double leg", func(_ *ProtectionDispatchPlan, e *SignedExitFillReview, _ *[]OwnerPosition, _ *exchange.RecoveryBaseline, _ time.Time) {
			e.SLFilledQty = "0.001"
		}},
		{"missing owner", func(_ *ProtectionDispatchPlan, _ *SignedExitFillReview, o *[]OwnerPosition, _ *exchange.RecoveryBaseline, _ time.Time) {
			*o = nil
		}},
		{"different owner", func(_ *ProtectionDispatchPlan, _ *SignedExitFillReview, o *[]OwnerPosition, _ *exchange.RecoveryBaseline, _ time.Time) {
			(*o)[0].ModelID = "other"
		}},
		{"owner amount", func(_ *ProtectionDispatchPlan, _ *SignedExitFillReview, o *[]OwnerPosition, _ *exchange.RecoveryBaseline, _ time.Time) {
			(*o)[0].Quantity = "0.03"
		}},
		{"duplicate", func(_ *ProtectionDispatchPlan, _ *SignedExitFillReview, o *[]OwnerPosition, _ *exchange.RecoveryBaseline, _ time.Time) {
			*o = append(*o, (*o)[0])
		}},
		{"stale", func(_ *ProtectionDispatchPlan, _ *SignedExitFillReview, _ *[]OwnerPosition, b *exchange.RecoveryBaseline, n time.Time) {
			b.FinishedAt = n.Add(-10 * time.Second)
		}},
		{"nil trades", func(_ *ProtectionDispatchPlan, _ *SignedExitFillReview, _ *[]OwnerPosition, b *exchange.RecoveryBaseline, _ time.Time) {
			b.RecentBTCTrades = nil
		}},
		{"open algo", func(_ *ProtectionDispatchPlan, _ *SignedExitFillReview, _ *[]OwnerPosition, b *exchange.RecoveryBaseline, _ time.Time) {
			b.OpenAlgos = append(b.OpenAlgos, exchange.AlgoOrder{AlgoID: 2})
		}},
		{"invalid portfolio", func(_ *ProtectionDispatchPlan, _ *SignedExitFillReview, _ *[]OwnerPosition, b *exchange.RecoveryBaseline, _ time.Time) {
			b.Portfolio.Assets = nil
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, e, o, b, n := ownerExitFixture(t)
			tt.change(&p, &e, &o, &b, n)
			r, err := ReviewExitOwnerExposure(p, e, o, b, n)
			if err == nil || r.State != "HALT" || r.TradingAuthorized || r.FinanciallySettled {
				t.Fatalf("fail-open %+v %v", r, err)
			}
		})
	}
}

func TestSignedExitOwnerExposureFetchAndFailure(t *testing.T) {
	subs, reader, tp, sl := signedExitFixture(t)
	plan, err := subs.read("group-p-001")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	b, _, _ := portfolioFixture(now)
	b.OpenAlgos = []exchange.AlgoOrder{}
	b.Portfolio.Positions = []exchange.PortfolioPosition{}
	owners := []OwnerPosition{{Source: plan.Source, ModelID: plan.ModelID, Symbol: "BTCUSDT", Side: plan.PositionSide, Quantity: plan.TP.Quantity}}
	creds := DemoCredentials{APIKey: "mock", Secret: "mock"}
	got, err := InspectSignedExitOwnerExposure(context.Background(), subs, reader, creds, plan.GroupID, tp, sl, owners, b, now)
	if err != nil || got.State != "SINGLE_OWNER_FLAT_OBSERVED_NOT_SETTLED" || got.FinanciallySettled {
		t.Fatalf("unexpected %+v %v", got, err)
	}
	delete(reader.algos, sl)
	got, err = InspectSignedExitOwnerExposure(context.Background(), subs, reader, creds, plan.GroupID, tp, sl, owners, b, now)
	if err == nil || got.State != "HALT" || got.TradingAuthorized {
		t.Fatalf("accepted missing signed leg %+v %v", got, err)
	}
}
