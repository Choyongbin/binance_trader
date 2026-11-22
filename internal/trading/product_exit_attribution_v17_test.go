package trading

import (
	"strings"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

func makeExitLegEvidenceV17(t *testing.T) ([]ProtectionDispatchPlan, []ProtectionStoredFill, []exchange.AccountTrade, []exchange.DemoAlgoResult, []exchange.DemoAlgoResult) {
	t.Helper()
	_, _, plans, _ := makeEntryLineageCaseV16(t)
	p := plans[0]
	trade := exchange.AccountTrade{Symbol: "BTCUSDT", TradeID: 9991, OrderID: 5001, Side: "SELL", PositionSide: "LONG", Quantity: "0.01", Price: "101000", Commission: "0.02", RealizedPnL: "10", CommissionAsset: "USDT", TimeMS: time.Now().Add(-time.Second).UnixMilli()}
	fill := ProtectionStoredFill{Schema: 1, GroupID: p.GroupID, PlanDigest: p.Digest, Leg: "tp", Source: p.Source, ModelID: p.ModelID, PositionSide: p.PositionSide, AlgoID: 4001, ActualOrderID: 5001, Trade: trade}
	fill.Checksum = fill.digest()
	tp := exchange.DemoAlgoResult{AlgoID: 4001, ClientAlgoID: p.TP.ClientAlgoID, Symbol: "BTCUSDT", Side: "SELL", PositionSide: "LONG", Type: "TAKE_PROFIT_MARKET", AlgoType: "CONDITIONAL", WorkingType: "CONTRACT_PRICE", Quantity: "0.01", TriggerPrice: "101000", Status: "FINISHED", ActualOrderID: "5001", ActualQty: "0.01"}
	sl := exchange.DemoAlgoResult{AlgoID: 4002, ClientAlgoID: p.SL.ClientAlgoID, Symbol: "BTCUSDT", Side: "SELL", PositionSide: "LONG", Type: "STOP_MARKET", AlgoType: "CONDITIONAL", WorkingType: "MARK_PRICE", Quantity: "0.01", TriggerPrice: "99000", Status: "CANCELED", ActualQty: "0"}
	return plans, []ProtectionStoredFill{fill}, []exchange.AccountTrade{trade}, []exchange.DemoAlgoResult{tp, sl}, []exchange.DemoAlgoResult{tp, sl}
}

func TestProductExitAttributionV17MatchesSignedProtectionLedger(t *testing.T) {
	plans, fills, trades, first, second := makeExitLegEvidenceV17(t)
	got, err := buildSignedExitLegsV17(plans, fills, trades, first, second)
	if err != nil || len(got) != 2 || got[0].Leg != "tp" || len(got[0].Trades) != 1 || got[1].Leg != "sl" || got[1].ActualOrderID != 0 {
		t.Fatalf("adapter %+v err=%v", got, err)
	}
}
func TestProductExitAttributionV17RejectsMissingMismatchedOrMutatingSignedEvidence(t *testing.T) {
	cases := []struct {
		name   string
		change func([]ProtectionDispatchPlan, []ProtectionStoredFill, []exchange.AccountTrade, []exchange.DemoAlgoResult, []exchange.DemoAlgoResult)
	}{
		{"mutating algo status", func(_ []ProtectionDispatchPlan, _ []ProtectionStoredFill, _ []exchange.AccountTrade, _ []exchange.DemoAlgoResult, b []exchange.DemoAlgoResult) {
			b[0].Status = "TRIGGERED"
		}},
		{"wrong algo quantity", func(_ []ProtectionDispatchPlan, _ []ProtectionStoredFill, _ []exchange.AccountTrade, _ []exchange.DemoAlgoResult, b []exchange.DemoAlgoResult) {
			b[0].Quantity = "0.02"
		}},
		{"changed signed trade", func(_ []ProtectionDispatchPlan, _ []ProtectionStoredFill, ts []exchange.AccountTrade, _ []exchange.DemoAlgoResult, _ []exchange.DemoAlgoResult) {
			ts[0].RealizedPnL = "20"
		}},
		{"wrong client algo id", func(_ []ProtectionDispatchPlan, _ []ProtectionStoredFill, _ []exchange.AccountTrade, _ []exchange.DemoAlgoResult, b []exchange.DemoAlgoResult) {
			b[0].ClientAlgoID = "not-local-9999"
		}},
		{"duplicate algo id", func(_ []ProtectionDispatchPlan, _ []ProtectionStoredFill, _ []exchange.AccountTrade, b []exchange.DemoAlgoResult, _ []exchange.DemoAlgoResult) {
			b[1].AlgoID = b[0].AlgoID
		}},
		{"stale unrecognized trade", func(_ []ProtectionDispatchPlan, fs []ProtectionStoredFill, _ []exchange.AccountTrade, _ []exchange.DemoAlgoResult, _ []exchange.DemoAlgoResult) {
			fs[0].Trade.TradeID = 9988
		}},
		{"corrupt fill checksum", func(_ []ProtectionDispatchPlan, fs []ProtectionStoredFill, _ []exchange.AccountTrade, _ []exchange.DemoAlgoResult, _ []exchange.DemoAlgoResult) {
			fs[0].Checksum = strings.Repeat("a", 64)
		}},
		{"wrong local exit group", func(_ []ProtectionDispatchPlan, fs []ProtectionStoredFill, _ []exchange.AccountTrade, _ []exchange.DemoAlgoResult, _ []exchange.DemoAlgoResult) {
			fs[0].GroupID = "entry_unknown_001"
			fs[0].Checksum = fs[0].digest()
		}},
		{"wrong algo id on local fill", func(_ []ProtectionDispatchPlan, fs []ProtectionStoredFill, _ []exchange.AccountTrade, _ []exchange.DemoAlgoResult, _ []exchange.DemoAlgoResult) {
			fs[0].AlgoID = 123
			fs[0].Checksum = fs[0].digest()
		}},
		{"duplicate actual order id", func(_ []ProtectionDispatchPlan, _ []ProtectionStoredFill, _ []exchange.AccountTrade, b []exchange.DemoAlgoResult, a []exchange.DemoAlgoResult) {
			b[1].ActualOrderID = "5001"
			a[1].ActualOrderID = "5001"
		}},
		{"plan digest mismatch", func(ps []ProtectionDispatchPlan, _ []ProtectionStoredFill, _ []exchange.AccountTrade, _ []exchange.DemoAlgoResult, _ []exchange.DemoAlgoResult) {
			ps[0].Digest = strings.Repeat("a", 64)
		}},
		{"unsigned foreign algo", func(_ []ProtectionDispatchPlan, _ []ProtectionStoredFill, _ []exchange.AccountTrade, _ []exchange.DemoAlgoResult, b []exchange.DemoAlgoResult) {
			b[1].Symbol = "ETHUSDT"
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, f, tr, a, b := makeExitLegEvidenceV17(t)
			tc.change(p, f, tr, a, b)
			if got, e := buildSignedExitLegsV17(p, f, tr, a, b); e == nil {
				t.Fatalf("unsafe output: %+v", got)
			}
		})
	}
}
