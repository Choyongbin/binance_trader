package trading

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

func packageAFixture(t *testing.T, side PositionSide) (string, *PackageAPreflightStore, string, SignedCloseEvidence) {
	t.Helper()
	dir, dual, id := dualLedgerFixture(t, side)
	if _, err := dual.Prepare(id); err != nil {
		t.Fatal(err)
	}
	if _, err := dual.Verify(id); err != nil {
		t.Fatal(err)
	}
	p, err := dual.shadow.intents.replay.proposal.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	var entry StoredFill
	if err := strictReviewJSON(filepath.Join(dir, "trading", "fills", "BTCUSDT_901.json"), &entry); err != nil {
		t.Fatal(err)
	}
	exitSide := "BUY"
	if side == Long {
		exitSide = "SELL"
	}
	exit := exchange.AccountTrade{Symbol: "BTCUSDT", TradeID: 902, OrderID: p.ExitOrderID, Side: exitSide, PositionSide: string(side), Quantity: "0.001", Price: "79900", Commission: "0.025", CommissionAsset: "USDT", RealizedPnL: "1.25", TimeMS: entry.Trade.TimeMS + 120000}
	ev := SignedCloseEvidence{EntryOrderID: p.EntryOrderID, ExitOrderID: p.ExitOrderID, EntryTradeIDs: append([]int64(nil), p.EntryTradeIDs...), ExitTradeIDs: append([]int64(nil), p.ExitTradeIDs...), EntryFills: []exchange.AccountTrade{entry.Trade}, ExitFills: []exchange.AccountTrade{exit}, AccountFlat: true, ObservedAt: time.Now().UTC(), SignedDetailSHA256: strings.Repeat("a", 64), BoundedFundingNetUSDT: "0", HistoryFromMS: entry.Trade.TimeMS, HistoryToMS: exit.TimeMS, BoundedTradeRows: 2, TradeWindowsRead: 1, FundingWindowsRead: 1, HistoryScope: "ENTRY_FIRST_FILL_TO_EXIT_LAST_FILL_ONLY"}
	s, err := NewPackageAPreflightStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	return dir, s, id, ev
}
func TestPackageAPreflightLongShortReplayAndNoMutation(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			dir, s, id, ev := packageAFixture(t, side)
			before, e := snapshotExecutionEvidence(dir)
			if e != nil {
				t.Fatal(e)
			}
			now := time.Now().UTC()
			var wg sync.WaitGroup
			errs := make(chan error, 30)
			for i := 0; i < 30; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					rec, e := s.Stage(context.Background(), id, mockCloseReader{v: ev}, now)
					if e == nil && (rec.TradingAuthorized || rec.SettlementAuthorized || rec.PostingApplied || len(rec.UnownedExitTradeIDs) != 1 || rec.ProvisionalNetExcludingFundingUSDT != "1.195") {
						e = errUnsafeShadow
					}
					errs <- e
				}()
			}
			wg.Wait()
			close(errs)
			for e := range errs {
				if e != nil {
					t.Fatal(e)
				}
			}
			restarted, e := NewPackageAPreflightStore(dir)
			if e != nil {
				t.Fatal(e)
			}
			for i := 0; i < 2; i++ {
				r, e := restarted.Recheck(context.Background(), id, mockCloseReader{v: ev}, now)
				if e != nil || r.Stage != PackageARechecked || r.PreviousChecksum == "" {
					t.Fatalf("recheck %v %+v", e, r)
				}
			}
			after, e := snapshotExecutionEvidence(dir)
			if e != nil || after != before {
				t.Fatalf("real ledger mutated: %v", e)
			}
			entries, e := os.ReadDir(s.dir)
			if e != nil || len(entries) != 2 {
				t.Fatalf("unexpected journal files %v %v", entries, e)
			}
			// Changed fresh signed financial data after restart cannot match original.
			ev.ExitFills[0].RealizedPnL = "90"
			if _, e := restarted.Recheck(context.Background(), id, mockCloseReader{v: ev}, now); e == nil {
				t.Fatal("accepted changed PnL")
			}
		})
	}
}
func TestPackageAPreflightFundingAndMismatchFailClosed(t *testing.T) {
	for _, kind := range []string{"entry_fee", "exit_fee", "duplicate_funding", "wrong_funding_total", "missing_entry", "stale", "history", "active", "tamper", "torn", "unknown", "forged"} {
		t.Run(kind, func(t *testing.T) {
			dir, s, id, ev := packageAFixture(t, Short)
			before, _ := snapshotExecutionEvidence(dir)
			switch kind {
			case "entry_fee":
				ev.EntryFills[0].Commission = "1"
			case "exit_fee":
				ev.ExitFills[0].Commission = "2"
			case "duplicate_funding":
				ev.FundingRows = []exchange.FundingIncome{{Symbol: "BTCUSDT", IncomeType: "FUNDING_FEE", Asset: "USDT", TranID: 1, Income: "0.1", TimeMS: ev.HistoryFromMS}, {Symbol: "BTCUSDT", IncomeType: "FUNDING_FEE", Asset: "USDT", TranID: 1, Income: "0.1", TimeMS: ev.HistoryToMS}}
				ev.BoundedFundingRows = 2
				ev.BoundedFundingNetUSDT = "0.2"
			case "wrong_funding_total":
				ev.FundingRows = []exchange.FundingIncome{{Symbol: "BTCUSDT", IncomeType: "FUNDING_FEE", Asset: "USDT", TranID: 1, Income: "0.1", TimeMS: ev.HistoryFromMS}}
				ev.BoundedFundingRows = 1
				ev.BoundedFundingNetUSDT = "0.5"
			case "missing_entry":
				if e := os.Remove(filepath.Join(dir, "trading", "fills", "BTCUSDT_901.json")); e != nil {
					t.Fatal(e)
				}
			case "stale":
				ev.ObservedAt = time.Now().UTC().Add(-10 * time.Minute)
			case "history":
				ev.HistoricalCoverageComplete = true
			case "active":
				if e := os.Mkdir(filepath.Join(dir, "trading", "coordinator.active"), 0700); e != nil {
					t.Fatal(e)
				}
			case "tamper":
				if _, e := s.Stage(context.Background(), id, mockCloseReader{v: ev}, time.Now().UTC()); e != nil {
					t.Fatal(e)
				}
				r := PackageAPreflight{}
				if e := strictReviewJSON(s.path(PackageAPrepared), &r); e != nil {
					t.Fatal(e)
				}
				r.PostingApplied = true
				r.Checksum = r.digest()
				raw, _ := json.Marshal(r)
				if e := os.WriteFile(s.path(PackageAPrepared), raw, 0600); e != nil {
					t.Fatal(e)
				}
			case "torn":
				if e := s.directory(true); e != nil {
					t.Fatal(e)
				}
				if e := os.WriteFile(s.path(PackageAPrepared), []byte("{"), 0600); e != nil {
					t.Fatal(e)
				}
			case "unknown":
				if e := s.directory(true); e != nil {
					t.Fatal(e)
				}
				if e := os.WriteFile(filepath.Join(s.dir, "surprise.txt"), []byte("x"), 0600); e != nil {
					t.Fatal(e)
				}
			case "forged":
				ev.SignedDetailSHA256 = "not-a-sha"
			}
			got, e := s.Stage(context.Background(), id, mockCloseReader{v: ev}, time.Now().UTC())
			if kind == "missing_entry" {
				if e == nil || got.Checksum != "" {
					t.Fatalf("upstream drift accepted: %+v %v", got, e)
				}
			} else if e == nil {
				t.Fatalf("unsafe %s accepted %+v", kind, got)
			}
			if kind != "missing_entry" && kind != "active" {
				after, e := snapshotExecutionEvidence(dir)
				if e != nil || after != before {
					t.Fatal("original ledger altered", e)
				}
			}
		})
	}
}
