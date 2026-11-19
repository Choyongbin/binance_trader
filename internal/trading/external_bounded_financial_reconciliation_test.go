package trading

import (
	"binance_trader/internal/exchange"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func boundedFinancialFixture(t *testing.T, side PositionSide) (string, string, PackageAIncidentArchive, ExternalCloseSettlementProposal) {
	return boundedFinancialFixtureWithFunding(t, side, "")
}

func boundedFinancialFixtureWithFunding(t *testing.T, side PositionSide, funding string) (string, string, PackageAIncidentArchive, ExternalCloseSettlementProposal) {
	t.Helper()
	ctx := context.Background()
	data, id, mock, signed := incidentFixture(t, side)
	if funding != "" {
		signed.FundingRows = []exchange.FundingIncome{{Symbol: "BTCUSDT", IncomeType: "FUNDING_FEE", Asset: "USDT", TranID: 2001, TimeMS: signed.HistoryFromMS + 1000, Income: funding}}
		signed.BoundedFundingRows = 1
		signed.BoundedFundingNetUSDT = funding
		details := struct {
			EntryOrder, ExitOrder          exchange.OrderResult
			EntryFills, ExitFills, History []exchange.AccountTrade
			Funding                        []exchange.FundingIncome
		}{signed.EntryOrder, signed.ExitOrder, signed.EntryFills, signed.ExitFills, signed.HistoricalTrades, signed.FundingRows}
		signed.SignedDetailSHA256 = externalCloseHash(details)
	}
	review := reviewFixture(data, mock)
	if len(review.ExternalCloseRehearsals) != 1 {
		t.Fatal("missing signed external close rehearsal")
	}
	reviewed, err := NewExternalCloseReviewLedger(data)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = reviewed.Stage(review.ExternalCloseRehearsals[0], time.Now().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err = reviewed.Decide(id, ExternalReviewAcknowledged, "operator_01", "Review only; no posting", time.Now()); err != nil {
		t.Fatal(err)
	}
	store, err := NewExternalCloseSettlementProposalStore(data)
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := store.Prepare(id, time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	incident, err := CapturePackageAIncidentArchive(ctx, data, id, proposal.ExitOrderID, mock, mockCloseReader{v: signed}, DemoCredentials{APIKey: "key", Secret: "secret"}, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	return data, id, incident, proposal
}

func TestBoundedFinancialReconciliationLongShortRestartReadOnly(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			data, id, archived, proposal := boundedFinancialFixture(t, side)
			before, err := snapshotExecutionEvidence(data)
			if err != nil {
				t.Fatal(err)
			}
			report, err := InspectExternalCloseBoundedFinancials(context.Background(), data, id, archived)
			if err != nil {
				t.Fatal(err)
			}
			if report.Stage != ExternalBoundedFinancialReconciledNotPosted || report.ClientOrderID != id ||
				report.ModelID != proposal.ModelID || report.PositionSide != side ||
				report.ProposalSHA256 != proposal.Checksum || report.IncidentArchiveSHA256 != archived.Checksum ||
				report.DigestSHA256 != report.digest() || report.EntryQuantityBTC != "0.001" ||
				report.ExitQuantityBTC != report.EntryQuantityBTC ||
				report.ProvisionalNetExFundingUSDT != proposal.ProvisionalNetExcludingFunding ||
				report.UnallocatedFundingUSDT != "0" || report.PostingApplied || report.LifecycleChanged ||
				report.OwnershipChanged || report.TradingAuthorized || report.SettlementAuthorized ||
				len(report.BlockerCodes) < 6 {
				t.Fatalf("unexpected bounded financial review: %+v", report)
			}
			payload, err := json.Marshal(report)
			if err != nil || !strings.Contains(string(payload), `"settlement_authorized":false`) {
				t.Fatal("authority leaked")
			}
			replay, err := InspectExternalCloseBoundedFinancials(context.Background(), data, id, archived)
			if err != nil || !reflect.DeepEqual(report, replay) {
				t.Fatalf("non-deterministic restart review: %v", err)
			}
			after, err := snapshotExecutionEvidence(data)
			if err != nil || before != after {
				t.Fatal("read-only financial review modified original files")
			}
		})
	}
}

func TestBoundedFinancialReconciliationRejectsAlteredOrMissingEvidence(t *testing.T) {
	for _, kind := range []string{"foreign_id", "archive_corrupt", "rehashed_fee", "rehashed_funding", "rehashed_history", "proposal_tamper", "local_drift", "cancelled"} {
		t.Run(kind, func(t *testing.T) {
			data, id, archived, _ := boundedFinancialFixture(t, Long)
			ctx := context.Background()
			switch kind {
			case "foreign_id":
				id = "bt_ffffffffffffffffffffffffffff"
			case "archive_corrupt":
				archived.Checksum = strings.Repeat("0", 64)
			case "rehashed_fee":
				archived.Close.ExitFills[0].Commission = "123.456"
				archived.Checksum = archived.digest()
			case "rehashed_funding":
				archived.Close.BoundedFundingNetUSDT = "999"
				archived.Checksum = archived.digest()
			case "rehashed_history":
				archived.Close.HistoricalTrades = archived.Close.HistoricalTrades[:1]
				archived.Checksum = archived.digest()
			case "proposal_tamper":
				p := filepath.Join(data, "trading", "external_close_settlement_proposals", "proposal.json")
				if err := os.WriteFile(p, []byte(`{}`), 0600); err != nil {
					t.Fatal(err)
				}
			case "local_drift":
				p := filepath.Join(data, "trading", "fills", "BTCUSDT_901.json")
				if err := os.Remove(p); err != nil {
					t.Fatal(err)
				}
			case "cancelled":
				c, cancel := context.WithCancel(ctx)
				cancel()
				ctx = c
			}
			if _, err := InspectExternalCloseBoundedFinancials(ctx, data, id, archived); err == nil {
				t.Fatal("untrusted or stale evidence accepted")
			}
		})
	}
}

func TestBoundedFinancialReconciliationFundingNeverAllocated(t *testing.T) {
	for _, amount := range []string{"-0.125", "0.375"} {
		t.Run(amount, func(t *testing.T) {
			data, id, incident, p := boundedFinancialFixtureWithFunding(t, Short, amount)
			result, err := InspectExternalCloseBoundedFinancials(context.Background(), data, id, incident)
			if err != nil {
				t.Fatal(err)
			}
			if result.UnallocatedFundingUSDT != amount || result.FundingRowCount != 1 ||
				result.ProvisionalNetExFundingUSDT != p.ProvisionalNetExcludingFunding ||
				result.SettlementAuthorized || result.TradingAuthorized ||
				!includesJournalBlocker(result.BlockerCodes, "MODEL_FUNDING_AND_FEES") {
				t.Fatalf("funding accidentally allocated or authorized: %+v", result)
			}
		})
	}
}
