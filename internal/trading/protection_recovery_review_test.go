package trading

import (
	"binance_trader/internal/exchange"
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func recoveryReviewFixture(t *testing.T, side PositionSide) (exchange.RecoveryBaseline, []ProtectionDispatchPlan, []OwnerPosition, []ProtectionRecoveryLeg, time.Time) {
	t.Helper()
	_, p, _, _ := testProtectionGroup(t, side)
	now := time.Now().UTC()
	b, _, _ := portfolioFixture(now)
	owner := OwnerPosition{Source: Manual, Symbol: "BTCUSDT", Side: side, Quantity: "0.01", AverageEntry: "100000"}
	raw := `[{"symbol":"BTCUSDT","positionSide":"LONG","positionAmt":"0.01","initialMargin":"50","isolated":true}]`
	if side == Short {
		raw = `[{"symbol":"BTCUSDT","positionSide":"SHORT","positionAmt":"-0.01","initialMargin":"50","isolated":true}]`
	}
	if err := json.Unmarshal([]byte(raw), &b.Portfolio.Positions); err != nil {
		t.Fatal(err)
	}
	b.OpenAlgos = []exchange.AlgoOrder{}
	legs := []ProtectionRecoveryLeg{}
	for i, inst := range []ProtectionInstruction{p.TP, p.SL} {
		id := int64(1900 + i)
		o := exchange.AlgoOrder{AlgoID: id, ClientAlgoID: inst.ClientAlgoID, Symbol: inst.Symbol, Side: inst.Side, PositionSide: string(inst.PositionSide), OrderType: inst.OrderType, AlgoStatus: "WORKING", Quantity: inst.Quantity, TriggerPrice: inst.TriggerPrice, WorkingType: inst.WorkingType}
		b.OpenAlgos = append(b.OpenAlgos, o)
		kind := "tp"
		if i == 1 {
			kind = "sl"
		}
		legs = append(legs, ProtectionRecoveryLeg{GroupID: p.GroupID, Kind: kind, Observation: AlgoObservation{AlgoID: id, ClientID: inst.ClientAlgoID, Status: "WORKING", Symbol: inst.Symbol, PositionSide: inst.PositionSide, Quantity: inst.Quantity, ActualQty: "0", FromSignedGet: true, ObservedAt: now}})
	}
	return b, []ProtectionDispatchPlan{p}, []OwnerPosition{owner}, legs, now
}
func makeRecoveryExit(b *exchange.RecoveryBaseline, legs []ProtectionRecoveryLeg, now time.Time, side PositionSide, qty string, finished bool) {
	for i := range legs {
		o := &legs[i].Observation
		if i == 0 {
			o.Status = "FINISHED"
			o.ActualOrderID = "780012"
			o.ActualQty = qty
			trSide := "SELL"
			if side == Short {
				trSide = "BUY"
			}
			tr := exchange.AccountTrade{Symbol: "BTCUSDT", OrderID: 780012, TradeID: 990001, Side: trSide, PositionSide: string(side), Quantity: qty, Price: "101000", Commission: "0.1", CommissionAsset: "USDT", RealizedPnL: "5", TimeMS: now.UnixMilli() - 100}
			legs[i].Trades = []exchange.AccountTrade{tr}
			b.RecentBTCTrades = []exchange.AccountTrade{tr}
		} else if finished {
			o.Status = "CANCELED"
		}
	}
	b.OpenAlgos = b.OpenAlgos[1:]
	remaining := "0"
	if qty == "0.004" {
		remaining = "0.006"
	}
	if side == Short && remaining != "0" {
		remaining = "-" + remaining
	}
	raw, _ := json.Marshal([]map[string]any{{"symbol": "BTCUSDT", "positionSide": string(side), "positionAmt": remaining, "initialMargin": "5", "isolated": true}})
	_ = json.Unmarshal(raw, &b.Portfolio.Positions)
}
func TestProtectionRecoveryReviewLongShortStateMachine(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			b, p, o, legs, now := recoveryReviewFixture(t, side)
			r := ReviewDemoProtectionRecovery(b, p, o, legs, true, now)
			if r.State != "REVIEW_ONLY" || len(r.Groups) != 1 || r.Groups[0].State != RecoveryReviewMonitoring || r.TradingAuthorized || r.OrderDispatchAllowed || r.CancelReplayAllowed || r.FinanciallySettled || !r.ManualReviewRequired {
				t.Fatalf("active: %+v", r)
			}
			makeRecoveryExit(&b, legs, now, side, "0.01", false)
			r = ReviewDemoProtectionRecovery(b, p, o, legs, true, now)
			if r.State != "REVIEW_ONLY" || r.Groups[0].State != RecoveryReviewCancelReview {
				t.Fatalf("sibling: %+v", r)
			}
			legs[1].Observation.Status = "CANCELED"
			b.OpenAlgos = []exchange.AlgoOrder{}
			r = ReviewDemoProtectionRecovery(b, p, o, legs, true, now)
			if r.State != "REVIEW_ONLY" || r.Groups[0].State != RecoveryReviewClosed || r.FinanciallySettled {
				t.Fatalf("closed: %+v", r)
			}
		})
	}
}
func TestProtectionRecoveryReviewPartialAndDoubleTrigger(t *testing.T) {
	b, p, o, legs, now := recoveryReviewFixture(t, Long)
	makeRecoveryExit(&b, legs, now, Long, "0.004", false)
	r := ReviewDemoProtectionRecovery(b, p, o, legs, true, now)
	if r.Groups[0].State != RecoveryReviewPartial || r.State != "HALT" {
		t.Fatalf("partial: %+v", r)
	}
	legs[1].Observation.Status = "TRIGGERED"
	b.OpenAlgos = []exchange.AlgoOrder{}
	r = ReviewDemoProtectionRecovery(b, p, o, legs, true, now)
	if r.Groups[0].State != RecoveryReviewDouble || r.State != "HALT" {
		t.Fatalf("double: %+v", r)
	}
}
func TestProtectionRecoveryReviewOrphanConflictsAndFailures(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		cases := []struct {
			name   string
			change func(*exchange.RecoveryBaseline, *[]ProtectionDispatchPlan, *[]OwnerPosition, *[]ProtectionRecoveryLeg, *time.Time)
		}{
			{"missing-leg", func(_ *exchange.RecoveryBaseline, _ *[]ProtectionDispatchPlan, _ *[]OwnerPosition, l *[]ProtectionRecoveryLeg, _ *time.Time) {
				*l = (*l)[:1]
			}},
			{"foreign-algo", func(b *exchange.RecoveryBaseline, _ *[]ProtectionDispatchPlan, _ *[]OwnerPosition, _ *[]ProtectionRecoveryLeg, _ *time.Time) {
				b.OpenAlgos = append(b.OpenAlgos, exchange.AlgoOrder{AlgoID: 909, ClientAlgoID: "foreign"})
			}},
			{"duplicate-algo", func(b *exchange.RecoveryBaseline, _ *[]ProtectionDispatchPlan, _ *[]OwnerPosition, _ *[]ProtectionRecoveryLeg, _ *time.Time) {
				b.OpenAlgos = append(b.OpenAlgos, b.OpenAlgos[0])
			}},
			{"no-full-audit", nil},
			{"stale", func(b *exchange.RecoveryBaseline, _ *[]ProtectionDispatchPlan, _ *[]OwnerPosition, _ *[]ProtectionRecoveryLeg, _ *time.Time) {
				b.FinishedAt = b.FinishedAt.Add(-12 * time.Second)
			}},
			{"tampered-plan", func(_ *exchange.RecoveryBaseline, p *[]ProtectionDispatchPlan, _ *[]OwnerPosition, _ *[]ProtectionRecoveryLeg, _ *time.Time) {
				(*p)[0].TP.Quantity = "999"
			}},
			{"signed-leg-missing", func(_ *exchange.RecoveryBaseline, _ *[]ProtectionDispatchPlan, _ *[]OwnerPosition, l *[]ProtectionRecoveryLeg, _ *time.Time) {
				(*l)[0].Observation.FromSignedGet = false
			}},
			{"unverified-active-inventory", func(b *exchange.RecoveryBaseline, _ *[]ProtectionDispatchPlan, _ *[]OwnerPosition, _ *[]ProtectionRecoveryLeg, _ *time.Time) {
				b.OpenAlgos[0].WorkingType = "MARK_PRICE"
			}},
			{"unexpected-normal-order", func(b *exchange.RecoveryBaseline, _ *[]ProtectionDispatchPlan, _ *[]OwnerPosition, _ *[]ProtectionRecoveryLeg, _ *time.Time) {
				b.OpenOrders = append(b.OpenOrders, exchange.Order{OrderID: 901})
			}},
			{"unknown-owner", func(_ *exchange.RecoveryBaseline, _ *[]ProtectionDispatchPlan, o *[]OwnerPosition, _ *[]ProtectionRecoveryLeg, _ *time.Time) {
				(*o)[0].ModelID = "unexpected"
			}},
			{"same-side-pooled", func(_ *exchange.RecoveryBaseline, p *[]ProtectionDispatchPlan, o *[]OwnerPosition, _ *[]ProtectionRecoveryLeg, _ *time.Time) {
				extra := (*p)[0]
				extra.GroupID = "another-owner"
				extra.Source = Model
				extra.ModelID = "A"
				extra.TP.ClientAlgoID = "btp_other_tp"
				extra.SL.ClientAlgoID = "btp_other_sl"
				extra.Digest = extra.digest()
				*p = append(*p, extra)
				*o = append(*o, OwnerPosition{Source: Model, ModelID: "A", Symbol: "BTCUSDT", Side: side, Quantity: "0.01"})
			}},
		}
		for _, tt := range cases {
			t.Run(string(side)+"/"+tt.name, func(t *testing.T) {
				b, p, o, l, now := recoveryReviewFixture(t, side)
				if tt.change != nil {
					tt.change(&b, &p, &o, &l, &now)
				}
				r := ReviewDemoProtectionRecovery(b, p, o, l, tt.name != "no-full-audit", now)
				if r.State != "HALT" || len(r.Blockers) == 0 || r.TradingAuthorized || r.OrderDispatchAllowed || r.CancelReplayAllowed || r.FinanciallySettled {
					t.Fatalf("unsafe %s: %+v", tt.name, r)
				}
			})
		}
	}
}
func TestProtectionRecoveryReviewFillProvenanceAndRemainder(t *testing.T) {
	tests := []struct {
		name   string
		change func(*exchange.RecoveryBaseline, *[]ProtectionRecoveryLeg)
	}{
		{"missing-bounded-fill", func(b *exchange.RecoveryBaseline, _ *[]ProtectionRecoveryLeg) {
			b.RecentBTCTrades = []exchange.AccountTrade{}
		}},
		{"modified-fill", func(_ *exchange.RecoveryBaseline, l *[]ProtectionRecoveryLeg) { (*l)[0].Trades[0].Commission = "999" }},
		{"duplicate-fill", func(_ *exchange.RecoveryBaseline, l *[]ProtectionRecoveryLeg) {
			(*l)[0].Trades = append((*l)[0].Trades, (*l)[0].Trades[0])
		}},
		{"mismatched-actual-qty", func(_ *exchange.RecoveryBaseline, l *[]ProtectionRecoveryLeg) {
			(*l)[0].Observation.ActualQty = "0.009"
		}},
		{"missing-actual-order-id", func(_ *exchange.RecoveryBaseline, l *[]ProtectionRecoveryLeg) { (*l)[0].Observation.ActualOrderID = "" }},
		{"overclose", func(b *exchange.RecoveryBaseline, l *[]ProtectionRecoveryLeg) {
			(*l)[0].Observation.ActualQty = "0.02"
			(*l)[0].Trades[0].Quantity = "0.02"
			b.RecentBTCTrades = append([]exchange.AccountTrade{}, (*l)[0].Trades...)
		}},
		{"external-close", func(b *exchange.RecoveryBaseline, _ *[]ProtectionRecoveryLeg) {
			e := b.RecentBTCTrades[0]
			e.TradeID = 990002
			e.OrderID = 88000
			b.RecentBTCTrades = append(b.RecentBTCTrades, e)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, p, o, l, now := recoveryReviewFixture(t, Long)
			makeRecoveryExit(&b, l, now, Long, "0.01", true)
			tt.change(&b, &l)
			r := ReviewDemoProtectionRecovery(b, p, o, l, true, now)
			if r.State != "HALT" || r.TradingAuthorized || r.FinanciallySettled {
				t.Fatalf("unsafe %+v", r)
			}
		})
	}
	b, p, o, l, now := recoveryReviewFixture(t, Long)
	a := ReviewDemoProtectionRecovery(b, p, o, l, true, now)
	z := ReviewDemoProtectionRecovery(b, p, o, l, true, now)
	if !reflect.DeepEqual(a, z) {
		t.Fatal("review is nondeterministic")
	}
}

// A stopped process can rebuild the same observation from a persisted copy;
// this pure review never writes markers or silently advances a lifecycle.
func TestProtectionRecoveryReviewRestartSnapshotReplayIsPure(t *testing.T) {
	b, p, o, legs, now := recoveryReviewFixture(t, Short)
	original := ReviewDemoProtectionRecovery(b, p, o, legs, true, now)
	payload := struct {
		Baseline exchange.RecoveryBaseline `json:"baseline"`
		Plans    []ProtectionDispatchPlan  `json:"plans"`
		Owners   []OwnerPosition           `json:"owners"`
		Legs     []ProtectionRecoveryLeg   `json:"legs"`
	}{b, p, o, legs}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	var revived struct {
		Baseline exchange.RecoveryBaseline `json:"baseline"`
		Plans    []ProtectionDispatchPlan  `json:"plans"`
		Owners   []OwnerPosition           `json:"owners"`
		Legs     []ProtectionRecoveryLeg   `json:"legs"`
	}
	if err := json.Unmarshal(raw, &revived); err != nil {
		t.Fatal(err)
	}
	second := ReviewDemoProtectionRecovery(revived.Baseline, revived.Plans, revived.Owners, revived.Legs, true, now)
	if !reflect.DeepEqual(original, second) || original.State != "REVIEW_ONLY" || original.OrderDispatchAllowed || original.CancelReplayAllowed {
		t.Fatalf("replay drifted/authorized: before=%+v after=%+v", original, second)
	}
}
