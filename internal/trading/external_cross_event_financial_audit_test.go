package trading

import (
	"binance_trader/internal/exchange"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

// Distinct offline cases are deliberately kept in different data roots.
// The production proposal writer is single-global; this test does NOT turn it
// into a multi-owner allocator or permit simultaneous settlement.
func crossFinancialFixture(t *testing.T, side PositionSide, suffix string, seq int64, age time.Duration, fundingID int64, tradeSeqOverride ...int64) ExternalFinancialEventSource {
	return crossFinancialFixtureForModel(t, side, suffix, seq, age, fundingID, "ema_rsi_macd_v1", tradeSeqOverride...)
}

func crossFinancialFixtureForModel(t *testing.T, side PositionSide, suffix string, seq int64, age time.Duration, fundingID int64, modelID string, tradeSeqOverride ...int64) ExternalFinancialEventSource {
	t.Helper()
	data := t.TempDir()
	ctx := context.Background()
	engine, err := NewEngine(data, &uniqueOrders{next: 700 + seq})
	if err != nil {
		t.Fatal(err)
	}
	owned, err := NewOwnershipStore(data)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(data, "trading", "reservations"), 0700); err != nil {
		t.Fatal(err)
	}
	intent := makeIntent("cross-financial-"+suffix, Model, modelID, side, Enter, "0.001")
	rec, err := engine.Submit(ctx, intent, allow)
	if err != nil {
		t.Fatal(err)
	}
	entryOrder := orderFromRecord(rec, "FILLED", "0.001")
	tradeSeq := seq
	if len(tradeSeqOverride) != 0 {
		tradeSeq = tradeSeqOverride[0]
	}
	entry := receipt(rec, 901+tradeSeq, "0.001", "80000", "0.03", "0", time.Now().UTC().Add(-age).UnixMilli())
	remote := &readerOrders{orders: map[string]exchange.OrderResult{rec.Plan.ClientOrderID: entryOrder}, trades: map[int64][]exchange.AccountTrade{entryOrder.OrderID: {entry}}}
	if n, err := owned.ImportDemoOrder(ctx, remote, DemoCredentials{APIKey: "mockkey", Secret: "mocksecret"}, intent); n != 1 || err != nil {
		t.Fatalf("import: %d %v", n, err)
	}
	exitOrderID := int64(899) + seq
	closeSide := "BUY"
	if side == Long {
		closeSide = "SELL"
	}
	exit := exchange.AccountTrade{Symbol: "BTCUSDT", TradeID: 902 + tradeSeq, OrderID: exitOrderID,
		Side: closeSide, PositionSide: string(side), Quantity: "0.001", Price: "79900", Commission: "0.025", CommissionAsset: "USDT", RealizedPnL: "1.25", TimeMS: entry.TimeMS + 120000}
	exitOrder := exchange.OrderResult{Symbol: "BTCUSDT", OrderID: exitOrderID, Side: closeSide, PositionSide: string(side), Status: "FILLED", ExecutedQty: "0.001", OriginalQty: "0.001"}
	mock := &ledgerMock{readerOrders: remote, history: []exchange.AccountTrade{entry, exit}, snapshot: accountSnapshot(time.Now().UTC(), "0", "0"), externalOrders: map[int64]exchange.OrderResult{exitOrderID: exitOrder}}
	funding := []exchange.FundingIncome{}
	if fundingID > 0 {
		funding = append(funding, exchange.FundingIncome{Symbol: "BTCUSDT", IncomeType: "FUNDING_FEE", Asset: "USDT", TranID: fundingID, Income: "-0.01", TimeMS: entry.TimeMS + 1000})
	}
	details := struct {
		EntryOrder, ExitOrder          exchange.OrderResult
		EntryFills, ExitFills, History []exchange.AccountTrade
		Funding                        []exchange.FundingIncome
	}{entryOrder, exitOrder, []exchange.AccountTrade{entry}, []exchange.AccountTrade{exit}, []exchange.AccountTrade{entry, exit}, funding}
	ev := SignedCloseEvidence{
		EntryOrder: entryOrder, ExitOrder: exitOrder, EntryFills: []exchange.AccountTrade{entry}, ExitFills: []exchange.AccountTrade{exit},
		HistoricalTrades: []exchange.AccountTrade{entry, exit}, FundingRows: funding,
		EntryOrderID: entryOrder.OrderID, ExitOrderID: exitOrderID, EntryTradeIDs: []int64{entry.TradeID}, ExitTradeIDs: []int64{exit.TradeID},
		AccountFlat: true, ObservedAt: time.Now().UTC(), SignedDetailSHA256: externalCloseHash(details),
		BoundedFundingRows: len(funding), BoundedFundingNetUSDT: "0", HistoryFromMS: entry.TimeMS, HistoryToMS: exit.TimeMS,
		BoundedTradeRows: 2, TradeWindowsRead: 1, FundingWindowsRead: 1, HistoryScope: "ENTRY_FIRST_FILL_TO_EXIT_LAST_FILL_ONLY",
	}
	if fundingID > 0 {
		ev.BoundedFundingNetUSDT = "-0.01"
	}
	review := reviewFixture(data, mock)
	if len(review.ExternalCloseRehearsals) != 1 {
		t.Fatal("no external close rehearsal")
	}
	reviewed, err := NewExternalCloseReviewLedger(data)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reviewed.Stage(review.ExternalCloseRehearsals[0], time.Now().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := reviewed.Decide(rec.Plan.ClientOrderID, ExternalReviewAcknowledged, "operator_01", "offline review only", time.Now()); err != nil {
		t.Fatal(err)
	}
	proposalStore, err := NewExternalCloseSettlementProposalStore(data)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := proposalStore.Prepare(rec.Plan.ClientOrderID, time.Now()); err != nil {
		t.Fatal(err)
	}
	incident, err := CapturePackageAIncidentArchive(ctx, data, rec.Plan.ClientOrderID, exitOrderID,
		mock, mockCloseReader{v: ev}, DemoCredentials{APIKey: "key", Secret: "secret"}, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	return ExternalFinancialEventSource{DataDir: data, ClientOrderID: rec.Plan.ClientOrderID, Incident: incident}
}

func containsCrossConflict(g ExternalCrossEventFinancialAudit, code string) bool {
	for _, c := range g.Conflicts {
		if c.Code == code {
			return true
		}
	}
	return false
}

func TestCrossEventFinancialAuditDisjointLongShortAndRestart(t *testing.T) {
	cases := [][]ExternalFinancialEventSource{
		{crossFinancialFixture(t, Long, "long", 0, 30*time.Minute, 0), crossFinancialFixture(t, Short, "short", 1000, 30*time.Minute, 0)},
		{crossFinancialFixture(t, Long, "old", 0, 80*time.Minute, 0), crossFinancialFixture(t, Long, "new", 1000, 20*time.Minute, 0)},
	}
	for _, cases := range cases {
		before := make([]string, len(cases))
		for i, e := range cases {
			before[i], _ = snapshotExecutionEvidence(e.DataDir)
		}
		a, err := InspectExternalCrossEventFinancials(context.Background(), cases)
		if err != nil || a.Stage != ExternalCrossEventNoConflict || len(a.Conflicts) != 0 || len(a.Members) != 2 ||
			a.DigestSHA256 != a.digest() || a.SettlementAuthorized || a.TradingAuthorized || a.PostingApplied ||
			a.SharedAccountProven || a.CompleteHistoryProven || a.FundingAllocated || len(a.BlockerCodes) < 5 {
			t.Fatalf("disjoint review: %+v %v", a, err)
		}
		reversed := []ExternalFinancialEventSource{cases[1], cases[0]}
		after, err := InspectExternalCrossEventFinancials(context.Background(), reversed)
		if err != nil || !reflect.DeepEqual(a, after) {
			t.Fatalf("unstable restart: %+v %v", after, err)
		}
		for i, e := range cases {
			digest, err := snapshotExecutionEvidence(e.DataDir)
			if err != nil || before[i] != digest {
				t.Fatal("financial source mutated")
			}
		}
	}
}

func TestCrossEventFinancialAuditBlocksOverlapsAndReusedIdentifiers(t *testing.T) {
	for _, tc := range []struct {
		name          string
		first, second int64
		side          PositionSide
		age1, age2    time.Duration
		funding       int64
		want          string
	}{
		{"same-side-overlap", 0, 1000, Long, 30 * time.Minute, 30 * time.Minute, 0, "OVERLAPPING_POOLED_HEDGE_SIDE_OWNERSHIP_UNPROVEN"},
		{"reused-orders", 0, 0, Short, 80 * time.Minute, 20 * time.Minute, 0, "ORDER_ID_REUSED"},
		{"reused-fills", 0, 0, Long, 80 * time.Minute, 20 * time.Minute, 0, "TRADE_ID_REUSED"},
		{"reused-funding", 0, 1000, Short, 80 * time.Minute, 20 * time.Minute, 77, "BOUNDED_FUNDING_TX_REUSED_UNALLOCATED"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := crossFinancialFixture(t, tc.side, "first", tc.first, tc.age1, tc.funding)
			b := crossFinancialFixture(t, tc.side, "second", tc.second, tc.age2, tc.funding)
			if tc.name == "reused-orders" {
				b = crossFinancialFixture(t, tc.side, "second-unique-fills", tc.second, tc.age2, tc.funding, 1000)
			}
			if tc.name == "reused-fills" {
				b = crossFinancialFixture(t, tc.side, "second-shared-fills", 1000, tc.age2, tc.funding, 0)
			}
			// Exact duplicate source ID is forbidden even when data roots differ.
			report, err := InspectExternalCrossEventFinancials(context.Background(), []ExternalFinancialEventSource{a, b})
			if err != nil {
				t.Fatal(err)
			}
			if report.Stage != ExternalCrossEventConflict || !containsCrossConflict(report, tc.want) || report.PostingApplied || report.TradingAuthorized {
				t.Fatalf("unsafe overlap not blocked: %+v", report)
			}
		})
	}
}

func TestCrossEventFinancialAuditRejectsCorruptOrDuplicatedEvidence(t *testing.T) {
	a := crossFinancialFixture(t, Long, "a", 0, 60*time.Minute, 0)
	b := crossFinancialFixture(t, Short, "b", 1000, 30*time.Minute, 0)
	cases := []struct {
		name  string
		apply func(*ExternalFinancialEventSource, *ExternalFinancialEventSource)
	}{
		{"repeated_id", func(a, b *ExternalFinancialEventSource) { b.ClientOrderID = a.ClientOrderID }},
		{"repeated_data_root", func(a, b *ExternalFinancialEventSource) { b.DataDir = a.DataDir }},
		{"wrong_archive", func(a, b *ExternalFinancialEventSource) { b.Incident = a.Incident }},
		{"tampered_signed_history", func(a, b *ExternalFinancialEventSource) { b.Incident.Close.ExitFills[0].Commission = "999" }},
		{"forged_authority", func(a, b *ExternalFinancialEventSource) {
			b.Incident.SettlementAuthorized = true
			b.Incident.Checksum = b.Incident.digest()
		}},
		{"missing_local_fill", func(a, b *ExternalFinancialEventSource) {
			_ = os.Remove(filepath.Join(b.DataDir, "trading", "fills", "BTCUSDT_1901.json"))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			x, y := a, b
			// avoid mutating fixture for subsequent cases
			x.Incident = a.Incident
			y.Incident = b.Incident
			y.Incident.Close.ExitFills = append([]exchange.AccountTrade(nil), b.Incident.Close.ExitFills...)
			tc.apply(&x, &y)
			if _, err := InspectExternalCrossEventFinancials(context.Background(), []ExternalFinancialEventSource{x, y}); err == nil {
				t.Fatal("unverified cross-event source accepted")
			}
		})
	}
	c, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := InspectExternalCrossEventFinancials(c, []ExternalFinancialEventSource{a, b}); err == nil {
		t.Fatal("cancelled audit accepted")
	}
	if _, err := InspectExternalCrossEventFinancials(context.Background(), []ExternalFinancialEventSource{a}); err == nil {
		t.Fatal("one event accepted as cross-event audit")
	}
}
