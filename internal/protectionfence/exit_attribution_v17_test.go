package protectionfence

import (
	"reflect"
	"strings"
	"testing"
)

func exitFixtureV17(t *testing.T) (Fence, SignedAccountFrameV15, SignedAccountFrameV15, []EntryGroupV16, []ExitLegV17) {
	t.Helper()
	f, a, b, gs, _ := entryLineageFixtureV16(t)
	a.LongQtyBTC = "0"
	b.LongQtyBTC = "0"
	gs[0].ExitOrProtectionFillSeen = true
	tp := ExitLegV17{GroupID: gs[0].GroupID, Leg: "tp", AlgoID: 101, ClientAlgoID: gs[0].TPClientAlgoID, ActualOrderID: 401, Status: "FINISHED", ActualQtyBTC: "0.01000000",
		Trades: []ExitTradeV17{
			{TradeID: 1001, OrderID: 401, QuantityBTC: "0.004", PriceUSDT: "101000.01", CommissionUSDT: "0.02", RealizedPnLUSDT: "2", CommissionAsset: "USDT", Side: "SELL", PositionSide: "LONG"},
			{TradeID: 1002, OrderID: 401, QuantityBTC: "0.006", PriceUSDT: "101005", CommissionUSDT: "0.03", RealizedPnLUSDT: "3", CommissionAsset: "USDT", Side: "SELL", PositionSide: "LONG"},
		}}
	sl := ExitLegV17{GroupID: gs[0].GroupID, Leg: "sl", AlgoID: 102, ClientAlgoID: gs[0].SLClientAlgoID, Status: "CANCELED", ActualQtyBTC: "0", Trades: []ExitTradeV17{}}
	return f, a, b, gs, []ExitLegV17{tp, sl}
}
func checkNoApprovalV17(t *testing.T, r ExitAttributionReviewV17) {
	t.Helper()
	if !r.ManualReviewRequired || r.TradingAuthorized || r.SubmitAuthorized || r.CancelAuthorized || r.RetryAuthorized || r.SettlementAuthorized || r.FinanciallySettled {
		t.Fatalf("unexpected mutation/settlement permission %+v", r)
	}
}
func TestExitAttributionV17SingleEntryBoundedNotSettled(t *testing.T) {
	f, a, b, gs, legs := exitFixtureV17(t)
	// The post-exit signed frames are fresh and non-overlapping.
	now := b.FinishedAt.Add(200000000)
	saved := append([]ExitTradeV17(nil), legs[0].Trades...)
	got := ReviewExitAttributionV17(f, a, b, gs, legs, now)
	if got.State != "SINGLE_ENTRY_EXIT_BOUNDED_CONSISTENT_NOT_SETTLED" || got.TargetExitQtyBTC != "0.01000000" ||
		got.TargetRealizedPnLUSDT != "5" || got.TargetCommissionUSDT != "0.05" || got.TotalExitTrades != 2 || len(got.InputDigestSHA256) != 64 {
		t.Fatalf("single %+v", got)
	}
	checkNoApprovalV17(t, got)
	if !reflect.DeepEqual(legs[0].Trades, saved) {
		t.Fatal("caller trades mutated")
	}
	legs[0].Trades[0], legs[0].Trades[1] = legs[0].Trades[1], legs[0].Trades[0]
	legs[0], legs[1] = legs[1], legs[0]
	again := ReviewExitAttributionV17(f, a, b, gs, legs, now)
	if got.InputDigestSHA256 != again.InputDigestSHA256 {
		t.Fatalf("noncanonical digest %s %s", got.InputDigestSHA256, again.InputDigestSHA256)
	}
}

func TestExitAttributionV17PooledSameSideNeverAllocated(t *testing.T) {
	f, a, b, gs, legs := exitFixtureV17(t)
	other := gs[0]
	other.GroupID = "entry_bt_order_00000002"
	other.EntryClientOrderID = "bt_order_00000002"
	other.EntryExchangeOrderID = 302
	other.ModelID = "different_model"
	other.TPClientAlgoID = "tp_other_0002"
	other.SLClientAlgoID = "sl_other_0002"
	other.EntryTrades = []EntryTradeV16{{TradeID: 903, OrderID: 302, QuantityBTC: "0.01"}}
	gs = append(gs, other)
	legs = append(legs, ExitLegV17{GroupID: other.GroupID, Leg: "tp", AlgoID: 103, ClientAlgoID: other.TPClientAlgoID, Status: "CANCELED", ActualQtyBTC: "0"},
		ExitLegV17{GroupID: other.GroupID, Leg: "sl", AlgoID: 104, ClientAlgoID: other.SLClientAlgoID, Status: "CANCELED", ActualQtyBTC: "0"})
	a.LongQtyBTC = "0.01"
	b.LongQtyBTC = "0.01000000"
	now := b.FinishedAt.Add(200000000)
	got := ReviewExitAttributionV17(f, a, b, gs, legs, now)
	if got.State != "POOLED_HEDGE_EXIT_UNATTRIBUTABLE" || got.SameSideEntryGroups != 2 || got.TotalEntryGroups != 2 {
		t.Fatalf("pooled %+v", got)
	}
	checkNoApprovalV17(t, got)
	// Even two different entry orders from the SAME model remain pooled.
	gs[1].ModelID = gs[0].ModelID
	got = ReviewExitAttributionV17(f, a, b, gs, legs, now)
	if got.State != "POOLED_HEDGE_EXIT_UNATTRIBUTABLE" {
		t.Fatalf("same model entries not pooled %+v", got)
	}
}

func TestExitAttributionV17PartialAndDoubleTriggerNeverSettled(t *testing.T) {
	t.Run("partial", func(t *testing.T) {
		f, a, b, gs, legs := exitFixtureV17(t)
		legs[0].Trades = legs[0].Trades[:1]
		legs[0].ActualQtyBTC = "0.004"
		a.LongQtyBTC = "0.006"
		b.LongQtyBTC = "0.00600000"
		got := ReviewExitAttributionV17(f, a, b, gs, legs, b.FinishedAt.Add(200000000))
		if got.State != "PARTIAL_EXIT_HALT_NOT_SETTLED" {
			t.Fatalf("partial %+v", got)
		}
		checkNoApprovalV17(t, got)
	})
	t.Run("both legs triggered", func(t *testing.T) {
		f, a, b, gs, legs := exitFixtureV17(t)
		legs[0].Trades = legs[0].Trades[:1]
		legs[0].ActualQtyBTC = "0.004"
		legs[1].Status = "FINISHED"
		legs[1].ActualQtyBTC = "0.006"
		legs[1].ActualOrderID = 402
		trade := legs[0].Trades[0]
		trade.TradeID = 1003
		trade.OrderID = 402
		trade.QuantityBTC = "0.006"
		legs[1].Trades = []ExitTradeV17{trade}
		got := ReviewExitAttributionV17(f, a, b, gs, legs, b.FinishedAt.Add(200000000))
		if got.State != "DOUBLE_TP_SL_TRIGGER_HALT_NOT_SETTLED" {
			t.Fatalf("double trigger %+v", got)
		}
		checkNoApprovalV17(t, got)
	})
}

func TestExitAttributionV17RejectsCorruptionAndUncertainSignedEvidence(t *testing.T) {
	cases := []struct {
		name   string
		change func(*Fence, *SignedAccountFrameV15, *SignedAccountFrameV15, *[]EntryGroupV16, *[]ExitLegV17)
	}{
		{"signed account digest changed", func(_ *Fence, _ *SignedAccountFrameV15, b *SignedAccountFrameV15, _ *[]EntryGroupV16, _ *[]ExitLegV17) {
			b.AccountEvidenceSHA256 = strings.Repeat("a", 64)
		}},
		{"unresolved algo still open", func(_ *Fence, _ *SignedAccountFrameV15, _ *SignedAccountFrameV15, _ *[]EntryGroupV16, l *[]ExitLegV17) {
			(*l)[1].Status = "WORKING"
		}},
		{"account side exposure mismatch", func(_ *Fence, _ *SignedAccountFrameV15, b *SignedAccountFrameV15, _ *[]EntryGroupV16, _ *[]ExitLegV17) {
			b.LongQtyBTC = "0.01"
		}},
		{"wrong protection leg", func(_ *Fence, _ *SignedAccountFrameV15, _ *SignedAccountFrameV15, _ *[]EntryGroupV16, l *[]ExitLegV17) {
			(*l)[1].ClientAlgoID = "different-id"
		}},
		{"missing sibling", func(_ *Fence, _ *SignedAccountFrameV15, _ *SignedAccountFrameV15, _ *[]EntryGroupV16, l *[]ExitLegV17) {
			*l = (*l)[:1]
		}},
		{"duplicate sibling identity", func(_ *Fence, _ *SignedAccountFrameV15, _ *SignedAccountFrameV15, _ *[]EntryGroupV16, l *[]ExitLegV17) {
			(*l)[1].AlgoID = (*l)[0].AlgoID
		}},
		{"duplicate exit trade id", func(_ *Fence, _ *SignedAccountFrameV15, _ *SignedAccountFrameV15, _ *[]EntryGroupV16, l *[]ExitLegV17) {
			(*l)[0].Trades[1].TradeID = (*l)[0].Trades[0].TradeID
		}},
		{"exit trade reuse entry id", func(_ *Fence, _ *SignedAccountFrameV15, _ *SignedAccountFrameV15, _ *[]EntryGroupV16, l *[]ExitLegV17) {
			(*l)[0].Trades[0].TradeID = 901
		}},
		{"wrong order id", func(_ *Fence, _ *SignedAccountFrameV15, _ *SignedAccountFrameV15, _ *[]EntryGroupV16, l *[]ExitLegV17) {
			(*l)[0].Trades[1].OrderID = 888
		}},
		{"wrong actual qty", func(_ *Fence, _ *SignedAccountFrameV15, _ *SignedAccountFrameV15, _ *[]EntryGroupV16, l *[]ExitLegV17) {
			(*l)[0].ActualQtyBTC = "0.009"
		}},
		{"overclosing actual qty", func(_ *Fence, _ *SignedAccountFrameV15, _ *SignedAccountFrameV15, _ *[]EntryGroupV16, l *[]ExitLegV17) {
			(*l)[0].ActualQtyBTC = "0.02"
			(*l)[0].Trades[0].QuantityBTC = "0.014"
		}},
		{"invalid price", func(_ *Fence, _ *SignedAccountFrameV15, _ *SignedAccountFrameV15, _ *[]EntryGroupV16, l *[]ExitLegV17) {
			(*l)[0].Trades[0].PriceUSDT = "inf"
		}},
		{"negative price", func(_ *Fence, _ *SignedAccountFrameV15, _ *SignedAccountFrameV15, _ *[]EntryGroupV16, l *[]ExitLegV17) {
			(*l)[0].Trades[0].PriceUSDT = "-1"
		}},
		{"non-USDT commission", func(_ *Fence, _ *SignedAccountFrameV15, _ *SignedAccountFrameV15, _ *[]EntryGroupV16, l *[]ExitLegV17) {
			(*l)[0].Trades[0].CommissionAsset = "BNB"
		}},
		{"invalid realized pnl", func(_ *Fence, _ *SignedAccountFrameV15, _ *SignedAccountFrameV15, _ *[]EntryGroupV16, l *[]ExitLegV17) {
			(*l)[0].Trades[0].RealizedPnLUSDT = "NaN"
		}},
		{"wrong trade hedge side", func(_ *Fence, _ *SignedAccountFrameV15, _ *SignedAccountFrameV15, _ *[]EntryGroupV16, l *[]ExitLegV17) {
			(*l)[0].Trades[0].PositionSide = "SHORT"
		}},
		{"wrong trade buy sell", func(_ *Fence, _ *SignedAccountFrameV15, _ *SignedAccountFrameV15, _ *[]EntryGroupV16, l *[]ExitLegV17) {
			(*l)[0].Trades[0].Side = "BUY"
		}},
		{"invalid entry fill sum", func(_ *Fence, _ *SignedAccountFrameV15, _ *SignedAccountFrameV15, g *[]EntryGroupV16, _ *[]ExitLegV17) {
			(*g)[0].EntryTrades[0].QuantityBTC = "0.003"
		}},
		{"fence checksum tampering", func(f *Fence, _ *SignedAccountFrameV15, _ *SignedAccountFrameV15, _ *[]EntryGroupV16, _ *[]ExitLegV17) {
			f.Checksum = strings.Repeat("f", 64)
		}},
		{"exit missing from entry lineage", func(_ *Fence, _ *SignedAccountFrameV15, _ *SignedAccountFrameV15, g *[]EntryGroupV16, _ *[]ExitLegV17) {
			(*g)[0].ExitOrProtectionFillSeen = false
		}},
		{"plan checksum mismatch", func(_ *Fence, _ *SignedAccountFrameV15, _ *SignedAccountFrameV15, g *[]EntryGroupV16, _ *[]ExitLegV17) {
			(*g)[0].ProtectionPlanSHA256 = strings.Repeat("f", 64)
		}},
		{"missing actual order id", func(_ *Fence, _ *SignedAccountFrameV15, _ *SignedAccountFrameV15, _ *[]EntryGroupV16, l *[]ExitLegV17) {
			(*l)[0].ActualOrderID = 0
		}},
		{"filled sibling not finished", func(_ *Fence, _ *SignedAccountFrameV15, _ *SignedAccountFrameV15, _ *[]EntryGroupV16, l *[]ExitLegV17) {
			(*l)[0].Status = "CANCELED"
		}},
		{"exit fee overprecision", func(_ *Fence, _ *SignedAccountFrameV15, _ *SignedAccountFrameV15, _ *[]EntryGroupV16, l *[]ExitLegV17) {
			(*l)[0].Trades[0].CommissionUSDT = "0.0000000000001"
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, a, b, gs, ls := exitFixtureV17(t)
			tc.change(&f, &a, &b, &gs, &ls)
			result := ReviewExitAttributionV17(f, a, b, gs, ls, b.FinishedAt.Add(200000000))
			if result.State != "BLOCKED_EXIT_ATTRIBUTION" {
				t.Fatalf("unsafe accepted %s: %+v", tc.name, result)
			}
			checkNoApprovalV17(t, result)
		})
	}
}

func TestExitAttributionV17FinancialDecimalNoRounding(t *testing.T) {
	for _, tc := range []struct {
		raw string
		ok  bool
	}{
		{"12.000000000001", true}, {"-0.000000000001", true}, {"0", true},
		{"-", false}, {"+1", false}, {"1e3", false}, {".1", false}, {"1.", false},
		{"-1.1234567890123", false}, {"", false}, {" 1", false},
	} {
		_, ok := StrictSignedMoneyV17(tc.raw)
		if ok != tc.ok {
			t.Fatalf("wrong decimal outcome for %q: %v", tc.raw, ok)
		}
	}
}

func TestExitAttributionV17MicroFeeExactNoRounding(t *testing.T) {
	f, a, b, gs, ls := exitFixtureV17(t)
	ls[0].Trades[0].CommissionUSDT = "0.000000000001"
	ls[0].Trades[1].CommissionUSDT = "0.000000000002"
	ls[0].Trades[0].RealizedPnLUSDT = "-0.000000000001"
	ls[0].Trades[1].RealizedPnLUSDT = "0.000000000003"
	r := ReviewExitAttributionV17(f, a, b, gs, ls, b.FinishedAt.Add(200000000))
	if r.State != "SINGLE_ENTRY_EXIT_BOUNDED_CONSISTENT_NOT_SETTLED" ||
		r.TargetCommissionUSDT != "0.000000000003" || r.TargetRealizedPnLUSDT != "0.000000000002" {
		t.Fatalf("money rounded or misattributed: %+v", r)
	}
	checkNoApprovalV17(t, r)
}
