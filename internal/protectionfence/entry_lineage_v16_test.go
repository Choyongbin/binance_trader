package protectionfence

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func entryLineageFixtureV16(t *testing.T) (Fence, SignedAccountFrameV15, SignedAccountFrameV15, []EntryGroupV16, time.Time) {
	t.Helper()
	now := time.Now().UTC()
	i := testIntent(SubmitTP)
	i.GroupID = "entry_bt_order_00000001"
	i.TargetClientAlgoID = "tp_entry_one"
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f, err := s.SealBeforePossibleSend(i, now)
	if err != nil {
		t.Fatal(err)
	}
	a := SignedAccountFrameV15{Environment: "demo", Coverage: "BOUNDED_HISTORY_ONLY_NOT_CONTINUOUS",
		AccountFingerprintSHA256: i.AccountFingerprintSHA256,
		StartedAt:                now.Add(-1300 * time.Millisecond), FinishedAt: now.Add(-1050 * time.Millisecond),
		WindowStartMS: now.Add(-time.Hour).UnixMilli(), AccountEvidenceSHA256: strings.Repeat("e", 64),
		LongQtyBTC: "0.01000000", ShortQtyBTC: "0", CanTrade: true}
	a.WindowEndMS = a.StartedAt.UnixMilli()
	b := a
	b.StartedAt = now.Add(-500 * time.Millisecond)
	b.FinishedAt = now.Add(-250 * time.Millisecond)
	b.WindowEndMS = b.StartedAt.UnixMilli()
	g := EntryGroupV16{GroupID: i.GroupID, EntryClientOrderID: "bt_order_00000001", EntryExchangeOrderID: 301,
		Source: "model", ModelID: "ema_rsi", Symbol: "BTCUSDT", PositionSide: "LONG", OriginalQtyBTC: "0.01", ExecutedQtyBTC: "0.01000000",
		EntryTrades:          []EntryTradeV16{{TradeID: 901, OrderID: 301, QuantityBTC: "0.004"}, {TradeID: 902, OrderID: 301, QuantityBTC: "0.006"}},
		ProtectionPlanSHA256: i.PlanSHA256, TPClientAlgoID: i.TargetClientAlgoID, SLClientAlgoID: "sl_entry_one",
		TPPlannedQtyBTC: "0.01", SLPlannedQtyBTC: "0.01000000"}
	return f, a, b, []EntryGroupV16{g}, now
}
func assertLineageNoApprovalV16(t *testing.T, r EntryLineageReviewV16) {
	t.Helper()
	if !r.ManualReviewRequired || r.TradingAuthorized || r.SubmitAuthorized || r.CancelAuthorized || r.RetryAuthorized || r.SettlementAuthorized {
		t.Fatalf("mutating approval: %+v", r)
	}
}
func TestEntryLineageV16SingleEntryAndFilledParts(t *testing.T) {
	f, a, b, gs, now := entryLineageFixtureV16(t)
	original := append([]EntryTradeV16(nil), gs[0].EntryTrades...)
	got := ReviewEntryGroupLineageV16(f, a, b, gs, now)
	if got.State != "SINGLE_ENTRY_GROUP_BOUNDED_LINKED_NOT_AUTHORIZED" || got.TotalEntryGroups != 1 || got.SameSideEntryGroups != 1 || got.LongEntryQtyBTC != "0.01000000" || len(got.LocalLineageSHA256) != 64 {
		t.Fatalf("single: %+v", got)
	}
	assertLineageNoApprovalV16(t, got)
	if !reflect.DeepEqual(gs[0].EntryTrades, original) {
		t.Fatal("caller entry-trade array modified")
	}
	gs[0].EntryTrades[0], gs[0].EntryTrades[1] = gs[0].EntryTrades[1], gs[0].EntryTrades[0]
	again := ReviewEntryGroupLineageV16(f, a, b, gs, now)
	if got.LocalLineageSHA256 != again.LocalLineageSHA256 {
		t.Fatal("receipt ordering changed digest")
	}
}
func TestEntryLineageV16MultipleEntriesSameModelStillPooled(t *testing.T) {
	f, a, b, gs, now := entryLineageFixtureV16(t)
	other := gs[0]
	other.GroupID = "entry_bt_order_00000002"
	other.EntryClientOrderID = "bt_order_00000002"
	other.EntryExchangeOrderID = 302
	other.TPClientAlgoID = "tp_entry_two"
	other.SLClientAlgoID = "sl_entry_two"
	other.EntryTrades = []EntryTradeV16{{TradeID: 903, OrderID: 302, QuantityBTC: "0.01"}}
	gs = append(gs, other)
	a.LongQtyBTC = "0.02"
	b.LongQtyBTC = "0.02000000"
	got := ReviewEntryGroupLineageV16(f, a, b, gs, now)
	if got.State != "MULTI_ENTRY_POOLED_HEDGE_UNATTRIBUTABLE" || got.SameSideEntryGroups != 2 || got.TotalEntryGroups != 2 {
		t.Fatalf("pooled: %+v", got)
	}
	assertLineageNoApprovalV16(t, got)
	gs[1].ModelID = "macd"
	got = ReviewEntryGroupLineageV16(f, a, b, gs, now)
	if got.State != "MULTI_ENTRY_POOLED_HEDGE_UNATTRIBUTABLE" {
		t.Fatalf("different models not pooled: %+v", got)
	}
}
func TestEntryLineageV16OppositeSideAndExitUnsafe(t *testing.T) {
	f, a, b, gs, now := entryLineageFixtureV16(t)
	sh := gs[0]
	sh.GroupID = "entry_bt_short_00000002"
	sh.EntryClientOrderID = "bt_short_00000002"
	sh.EntryExchangeOrderID = 302
	sh.ModelID = "breakout"
	sh.PositionSide = "SHORT"
	sh.TPClientAlgoID = "tp_entry_short"
	sh.SLClientAlgoID = "sl_entry_short"
	sh.EntryTrades = []EntryTradeV16{{TradeID: 903, OrderID: 302, QuantityBTC: "0.01"}}
	gs = append(gs, sh)
	a.ShortQtyBTC = "0.01"
	b.ShortQtyBTC = "0.01"
	got := ReviewEntryGroupLineageV16(f, a, b, gs, now)
	if got.State != "OTHER_HEDGE_SIDE_PRESENT_MANUAL_REVIEW" {
		t.Fatalf("opposite side: %+v", got)
	}
	gs[0].ExitOrProtectionFillSeen = true
	got = ReviewEntryGroupLineageV16(f, a, b, gs, now)
	if got.State != "EXIT_FILL_ATTRIBUTION_UNPROVEN_MANUAL_REVIEW" {
		t.Fatalf("ambiguous exit: %+v", got)
	}
	assertLineageNoApprovalV16(t, got)
}
func TestEntryLineageV16CorruptionAndMismatchFailClosed(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Fence, *SignedAccountFrameV15, *SignedAccountFrameV15, *[]EntryGroupV16)
	}{
		{"no entry groups", func(_ *Fence, _ *SignedAccountFrameV15, _ *SignedAccountFrameV15, g *[]EntryGroupV16) { *g = nil }},
		{"wrong target group", func(_ *Fence, _ *SignedAccountFrameV15, _ *SignedAccountFrameV15, g *[]EntryGroupV16) {
			(*g)[0].GroupID = "entry_bt_wrong_0000"
		}},
		{"wrong client order", func(_ *Fence, _ *SignedAccountFrameV15, _ *SignedAccountFrameV15, g *[]EntryGroupV16) {
			(*g)[0].EntryClientOrderID = "bt_wrong_0000"
		}},
		{"wrong plan digest", func(_ *Fence, _ *SignedAccountFrameV15, _ *SignedAccountFrameV15, g *[]EntryGroupV16) {
			(*g)[0].ProtectionPlanSHA256 = strings.Repeat("f", 64)
		}},
		{"wrong tp client ID", func(_ *Fence, _ *SignedAccountFrameV15, _ *SignedAccountFrameV15, g *[]EntryGroupV16) {
			(*g)[0].TPClientAlgoID = "some-other-client-id"
		}},
		{"duplicate sibling ID", func(_ *Fence, _ *SignedAccountFrameV15, _ *SignedAccountFrameV15, g *[]EntryGroupV16) {
			(*g)[0].SLClientAlgoID = (*g)[0].TPClientAlgoID
		}},
		{"partial fill", func(_ *Fence, _ *SignedAccountFrameV15, _ *SignedAccountFrameV15, g *[]EntryGroupV16) {
			(*g)[0].ExecutedQtyBTC = "0.006"
		}},
		{"wrong trade sum", func(_ *Fence, _ *SignedAccountFrameV15, _ *SignedAccountFrameV15, g *[]EntryGroupV16) {
			(*g)[0].EntryTrades[1].QuantityBTC = "0.005"
		}},
		{"wrong trade order", func(_ *Fence, _ *SignedAccountFrameV15, _ *SignedAccountFrameV15, g *[]EntryGroupV16) {
			(*g)[0].EntryTrades[0].OrderID = 999
		}},
		{"duplicate fill id", func(_ *Fence, _ *SignedAccountFrameV15, _ *SignedAccountFrameV15, g *[]EntryGroupV16) {
			(*g)[0].EntryTrades[1].TradeID = (*g)[0].EntryTrades[0].TradeID
		}},
		{"wrong plan quantity", func(_ *Fence, _ *SignedAccountFrameV15, _ *SignedAccountFrameV15, g *[]EntryGroupV16) {
			(*g)[0].SLPlannedQtyBTC = "0.02"
		}},
		{"signed total drifts", func(_ *Fence, _ *SignedAccountFrameV15, b *SignedAccountFrameV15, _ *[]EntryGroupV16) {
			b.LongQtyBTC = "0.02"
		}},
		{"foreign signed algo", func(_ *Fence, _ *SignedAccountFrameV15, b *SignedAccountFrameV15, _ *[]EntryGroupV16) {
			b.OpenAlgoOrders = 1
		}},
		{"signed evidence changes", func(_ *Fence, _ *SignedAccountFrameV15, b *SignedAccountFrameV15, _ *[]EntryGroupV16) {
			b.AccountEvidenceSHA256 = strings.Repeat("c", 64)
		}},
		{"fingerprint changes", func(_ *Fence, _ *SignedAccountFrameV15, b *SignedAccountFrameV15, _ *[]EntryGroupV16) {
			b.AccountFingerprintSHA256 = strings.Repeat("d", 64)
		}},
		{"fence tampered", func(f *Fence, _ *SignedAccountFrameV15, _ *SignedAccountFrameV15, _ *[]EntryGroupV16) {
			f.Checksum = strings.Repeat("f", 64)
		}},
		{"unknown model", func(_ *Fence, _ *SignedAccountFrameV15, _ *SignedAccountFrameV15, g *[]EntryGroupV16) {
			(*g)[0].ModelID = ""
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, a, b, gs, now := entryLineageFixtureV16(t)
			tc.mutate(&f, &a, &b, &gs)
			got := ReviewEntryGroupLineageV16(f, a, b, gs, now)
			if got.State != "BLOCKED_ENTRY_GROUP_LINEAGE" {
				t.Fatalf("didn't fail-closed: %+v", got)
			}
			assertLineageNoApprovalV16(t, got)
		})
	}
}
