package trading

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

func entryRecoveryFixture(t *testing.T, side PositionSide, missing bool) (string, PackageAEntryArchive) {
	t.Helper()
	data, mock, _ := externalCloseFixture(t, side)
	recs, err := readJournalForEntryGapTest(data)
	if err != nil || len(recs) != 1 {
		t.Fatalf("journal: %+v %v", recs, err)
	}
	if missing {
		if err := os.Remove(filepath.Join(data, "trading", "fills", "BTCUSDT_901.json")); err != nil {
			t.Fatal(err)
		}
	}
	a, err := CapturePackageAEntryArchive(context.Background(), data, recs[0].Plan.ClientOrderID, mock.readerOrders,
		DemoCredentials{APIKey: "fake", Secret: "fake"}, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	return data, a
}
func includesBlocker(v []string, want string) bool {
	for _, s := range v {
		if s == want {
			return true
		}
	}
	return false
}
func TestPackageAEntryRecoveryPlanLongShortMissingPresentAndNoMutation(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		for _, missing := range []bool{false, true} {
			label := string(side) + "/present"
			if missing {
				label = string(side) + "/missing"
			}
			t.Run(label, func(t *testing.T) {
				data, a := entryRecoveryFixture(t, side, missing)
				before, err := snapshotExecutionEvidence(data)
				if err != nil {
					t.Fatal(err)
				}
				p, err := PlanPackageAEntryRecovery(context.Background(), data, a)
				if err != nil {
					t.Fatal(err)
				}
				if err := VerifyPackageAEntryRecoveryPlan(context.Background(), data, a, p); err != nil {
					t.Fatal(err)
				}
				again, err := PlanPackageAEntryRecovery(context.Background(), data, a)
				if err != nil || again.Checksum != p.Checksum {
					t.Fatalf("nondeterminism %v", err)
				}
				after, err := snapshotExecutionEvidence(data)
				if err != nil || before != after {
					t.Fatal("original data mutated")
				}
				if p.Stage != PackageAEntryRecoveryPlanStage || p.ModelID == "" || p.PositionSide != side || p.TradingAuthorized || p.SettlementAuthorized || p.RecoveryApplied || p.LifecycleChanged || p.OwnershipChanged || p.FundingAttributionVerified || p.FullHistoryVerified {
					t.Fatalf("unsafe plan %+v", p)
				}
				if !includesBlocker(p.BlockerCodes, "FRESH_SIGNED_RECHECK_REQUIRED") || !includesBlocker(p.BlockerCodes, "ATOMIC_LEDGER_POSTING_UNIMPLEMENTED") {
					t.Fatal("mandatory blockers absent")
				}
				if missing {
					if len(p.Candidates) != 1 || len(p.AlreadyImportedIDs) != 0 || p.Candidates[0].TradeID != 901 || p.Candidates[0].JournalBasename != "BTCUSDT_901.json" || p.Candidates[0].ProposedStoredFillSHA256 == "" || !includesBlocker(p.BlockerCodes, "OWNED_ENTRY_FILLS_NOT_IMPORTED") {
						t.Fatalf("inaccurate missing entry candidates %+v", p)
					}
					// A plan is invalid as soon as signed import alters the originals.
					recs, _ := readJournalForEntryGapTest(data)
					store := &OwnershipStore{journalDir: filepath.Join(data, "trading", "requests"), fillDir: filepath.Join(data, "trading", "fills")}
					reader := entryArchiveReader{order: a.Order, fills: a.Fills}
					n, err := store.ImportDemoOrder(context.Background(), reader, DemoCredentials{APIKey: "mock", Secret: "mock"}, recs[0].Intent)
					if err != nil || n != 1 {
						t.Fatalf("signed import fixture: %d %v", n, err)
					}
					if VerifyPackageAEntryRecoveryPlan(context.Background(), data, a, p) == nil {
						t.Fatal("stale plan survived local fill import")
					}
				} else if len(p.Candidates) != 0 || len(p.AlreadyImportedIDs) != 1 || includesBlocker(p.BlockerCodes, "OWNED_ENTRY_FILLS_NOT_IMPORTED") {
					t.Fatalf("existing fill falsely proposed %+v", p)
				}
			})
		}
	}
}
func TestPackageAEntryRecoveryPlanRejectsForgedAuthorityAndTamper(t *testing.T) {
	data, a := entryRecoveryFixture(t, Short, true)
	p, err := PlanPackageAEntryRecovery(context.Background(), data, a)
	if err != nil {
		t.Fatal(err)
	}
	variants := []struct {
		name   string
		mutate func(*PackageAEntryRecoveryPlan)
	}{
		{"authorization", func(p *PackageAEntryRecoveryPlan) { p.TradingAuthorized = true }},
		{"recovery", func(p *PackageAEntryRecoveryPlan) { p.RecoveryApplied = true }},
		{"settlement", func(p *PackageAEntryRecoveryPlan) { p.SettlementAuthorized = true }},
		{"candidate", func(p *PackageAEntryRecoveryPlan) { p.Candidates[0].ArchivedSignedTrade.Price = "1" }},
		{"blockers", func(p *PackageAEntryRecoveryPlan) { p.BlockerCodes = nil }},
		{"source", func(p *PackageAEntryRecoveryPlan) { p.LocalEvidenceSHA256 = strings.Repeat("0", 64) }},
	}
	for _, tc := range variants {
		t.Run(tc.name, func(t *testing.T) {
			bad := p
			bad.Candidates = append([]PackageAEntryRecoveryCandidate(nil), p.Candidates...)
			bad.BlockerCodes = append([]string(nil), p.BlockerCodes...)
			tc.mutate(&bad)
			bad.Checksum = bad.digest() // attacker can recompute a LOCAL checksum
			if err := VerifyPackageAEntryRecoveryPlan(context.Background(), data, a, bad); err == nil {
				t.Fatal("accepted forged/re-written local plan")
			}
		})
	}
}
func TestPackageAEntryRecoveryPlanRejectsOtherOwnerAndUnknownOwnedFill(t *testing.T) {
	for _, kind := range []string{"other-owner-same-trade", "unknown-own-trade", "corrupt-fill"} {
		t.Run(kind, func(t *testing.T) {
			data, a := entryRecoveryFixture(t, Short, true)
			trade := a.Fills[0]
			clientID := a.Gap.ClientOrderID
			switch kind {
			case "other-owner-same-trade":
				clientID = "other-order"
			case "unknown-own-trade":
				trade.TradeID = 999
			case "corrupt-fill":
				trade.TradeID = 999
			}
			f := StoredFill{Schema: 1, ClientOrderID: clientID, Trade: trade}
			f.seal()
			raw, err := json.Marshal(f)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(data, "trading", "fills", "BTCUSDT_"+strconv.FormatInt(trade.TradeID, 10)+".json")
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			if kind == "corrupt-fill" {
				if err := os.WriteFile(path, []byte("{"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := PlanPackageAEntryRecovery(context.Background(), data, a); err == nil {
				t.Fatal("accepted conflicting/invalid fill")
			}
		})
	}
}
func TestPackageAEntryRecoveryPlanRejectsChangedSignedEvidence(t *testing.T) {
	data, a := entryRecoveryFixture(t, Long, true)
	a.Fills = append([]exchange.AccountTrade(nil), a.Fills...)
	a.Fills[0].Commission = "300"
	a.Checksum = a.digest()
	if _, err := PlanPackageAEntryRecovery(context.Background(), data, a); err == nil {
		t.Fatal("tampered signed snapshot accepted")
	}
}
