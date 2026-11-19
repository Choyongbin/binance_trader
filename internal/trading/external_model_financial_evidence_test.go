package trading

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func makeExternalModelClaim(r ExternalModelFinancialEvidence) ExternalModelFinancialAssertion {
	return ExternalModelFinancialAssertion{ClientOrderID: r.ClientOrderID, ModelID: r.ModelID,
		EvidenceSHA256: r.DigestSHA256, EntryCommissionUSDT: r.EntryCommissionUSDT,
		ExitCommissionUSDT: r.ExitCommissionUSDT, ExchangeRealizedUSDT: r.ExchangeExitRealizedUSDT,
		ProvisionalExFundingUSDT: r.ProvisionalExFundingUSDT}
}

func TestExternalModelFinancialEvidenceLongShortFundingAndRestart(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		for _, funding := range []string{"", "-0.125", "0.375", "0"} {
			t.Run(string(side)+"/"+funding, func(t *testing.T) {
				data, id, incident, _ := boundedFinancialFixtureWithFunding(t, side, funding)
				originalBefore, err := snapshotExecutionEvidence(data)
				if err != nil {
					t.Fatal(err)
				}
				r, err := InspectExternalModelFinancialEvidence(context.Background(), data, id, incident)
				if err != nil {
					t.Fatal(err)
				}
				wantFunding := funding
				if wantFunding == "" {
					wantFunding = "0"
				}
				if r.Stage != ExternalModelFinancialEvidenceOnly || r.Schema != 1 || r.DigestSHA256 != r.digest() ||
					r.ModelFundingUSDT != nil || r.ModelFinalNetUSDT != nil || r.FundingAllocated ||
					r.CompleteHistoryProven || r.ModelOwnershipProven || r.PostingApplied || r.SettlementAuthorized || r.TradingAuthorized ||
					r.BoundedAccountFundingUSDT != wantFunding || len(r.FeeRows) != 2 ||
					len(r.FundingRows) != incident.Close.BoundedFundingRows || len(r.BlockerCodes) < 5 ||
					r.IncidentArchiveSHA256 != incident.Checksum || r.HistoryScope != "ENTRY_FIRST_FILL_TO_EXIT_LAST_FILL_ONLY" {
					t.Fatalf("model-financial trace leaked authority or mismatched amounts: %+v", r)
				}
				if r.FeeRows[0].TradeID >= r.FeeRows[1].TradeID || r.FeeRows[0].Leg != "ENTRY" || r.FeeRows[1].Leg != "EXIT" ||
					r.FeeRows[0].FeeAsset != "USDT" || r.FeeRows[1].FeeAsset != "USDT" ||
					r.TotalCommissionUSDT == "" || r.ProvisionalExFundingUSDT == "" {
					t.Fatalf("invalid trade trace: %+v", r)
				}
				b, err := json.Marshal(r)
				if err != nil || !strings.Contains(string(b), `"model_funding_usdt":null`) ||
					!strings.Contains(string(b), `"settlement_authorized":false`) {
					t.Fatalf("unsafe JSON result: %s %v", b, err)
				}
				if err := ValidateExternalModelFinancialAssertion(r, makeExternalModelClaim(r)); err != nil {
					t.Fatal(err)
				}
				again, err := InspectExternalModelFinancialEvidence(context.Background(), data, id, incident)
				if err != nil || !reflect.DeepEqual(r, again) {
					t.Fatalf("non-deterministic replay: %v", err)
				}
				originalAfter, err := snapshotExecutionEvidence(data)
				if err != nil || originalAfter != originalBefore {
					t.Fatal("read-only model financial audit modified original ledger")
				}
			})
		}
	}
}

func TestExternalModelFinancialClaimRejectsUnprovenFundingAndAmounts(t *testing.T) {
	data, id, incident, _ := boundedFinancialFixtureWithFunding(t, Short, "-0.125")
	r, err := InspectExternalModelFinancialEvidence(context.Background(), data, id, incident)
	if err != nil {
		t.Fatal(err)
	}
	pointer := func(s string) *string { return &s }
	for _, kind := range []string{"funding_zero", "funding_nonzero", "final_net", "entry_fee", "exit_fee", "gross", "provisional", "other_model", "other_id", "fake_hash", "posting", "settlement", "trading", "altered_report", "fake_report_authority", "fake_report_funding", "rehashed_fee_row", "rehashed_net", "rehashed_funding_row", "rehashed_dupe_id", "rehashed_blockers"} {
		t.Run(kind, func(t *testing.T) {
			report := r
			a := makeExternalModelClaim(r)
			switch kind {
			case "funding_zero":
				a.ModelFundingUSDT = pointer("0")
			case "funding_nonzero":
				a.ModelFundingUSDT = pointer("-0.125")
			case "final_net":
				a.ModelFinalNetUSDT = pointer("999")
			case "entry_fee":
				a.EntryCommissionUSDT = "99"
			case "exit_fee":
				a.ExitCommissionUSDT = "99"
			case "gross":
				a.ExchangeRealizedUSDT = "99"
			case "provisional":
				a.ProvisionalExFundingUSDT = "99"
			case "other_model":
				a.ModelID = "external"
			case "other_id":
				a.ClientOrderID = "bt_ffffffffffffffffffffffffffff"
			case "fake_hash":
				a.EvidenceSHA256 = strings.Repeat("a", 64)
			case "posting":
				a.PostingApplied = true
			case "settlement":
				a.SettlementAuthorized = true
			case "trading":
				a.TradingAuthorized = true
			case "altered_report":
				report.TotalCommissionUSDT = "999"
			case "fake_report_authority":
				report.SettlementAuthorized = true
				report.DigestSHA256 = report.digest()
			case "fake_report_funding":
				report.ModelFundingUSDT = pointer("0")
				report.DigestSHA256 = report.digest()
			case "rehashed_fee_row":
				report.FeeRows[0].FeeUSDT = "999"
				report.DigestSHA256 = report.digest()
			case "rehashed_net":
				report.ProvisionalExFundingUSDT = "999"
				report.DigestSHA256 = report.digest()
			case "rehashed_funding_row":
				report.FundingRows[0].IncomeUSDT = "999"
				report.DigestSHA256 = report.digest()
			case "rehashed_dupe_id":
				report.FeeRows[1].TradeID = report.FeeRows[0].TradeID
				report.DigestSHA256 = report.digest()
			case "rehashed_blockers":
				report.BlockerCodes[0] = ""
				report.DigestSHA256 = report.digest()
			}
			if err := ValidateExternalModelFinancialAssertion(report, a); err == nil {
				t.Fatal("unsupported model attribution accepted")
			}
		})
	}
}

func TestExternalModelFinancialEvidenceRejectsTamperedSources(t *testing.T) {
	for _, kind := range []string{"archive", "fee", "funding", "history", "proposal", "original", "cancelled"} {
		t.Run(kind, func(t *testing.T) {
			data, id, incident, _ := boundedFinancialFixtureWithFunding(t, Long, "0.375")
			ctx := context.Background()
			switch kind {
			case "archive":
				incident.Checksum = strings.Repeat("0", 64)
			case "fee":
				incident.Close.EntryFills[0].Commission = "0.999"
				incident.Checksum = incident.digest()
			case "funding":
				incident.Close.FundingRows[0].Income = "999"
				incident.Checksum = incident.digest()
			case "history":
				incident.Close.HistoricalTrades = incident.Close.HistoricalTrades[:1]
				incident.Checksum = incident.digest()
			case "proposal":
				if err := os.Remove(filepath.Join(data, "trading", "external_close_settlement_proposals", "proposal.json")); err != nil {
					t.Fatal(err)
				}
			case "original":
				if err := os.Remove(filepath.Join(data, "trading", "fills", "BTCUSDT_901.json")); err != nil {
					t.Fatal(err)
				}
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if _, err := InspectExternalModelFinancialEvidence(ctx, data, id, incident); err == nil {
				t.Fatal("untrusted source accepted")
			}
		})
	}
}

func TestExternalModelFinancialEvidenceConcurrentReadOnly(t *testing.T) {
	data, id, incident, _ := boundedFinancialFixtureWithFunding(t, Short, "-0.125")
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := InspectExternalModelFinancialEvidence(context.Background(), data, id, incident)
			if err == nil {
				err = ValidateExternalModelFinancialAssertion(r, makeExternalModelClaim(r))
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestExternalModelFinancialEvidenceRejectsSilentPrecisionLoss(t *testing.T) {
	for _, input := range []string{"0.0000000000001", "-0.0000000000001", "0.1234567890123"} {
		value, err := signedRat(input)
		if err != nil {
			t.Fatal(err)
		}
		if err := ensureExactModelFinancialDecimal(value); err == nil {
			t.Fatalf("sub-precision amount silently rounded: %s", input)
		}
	}
	for _, input := range []string{"0.000000000001", "0.123456789012", "-0.123456789012", "0"} {
		value, err := signedRat(input)
		if err != nil || ensureExactModelFinancialDecimal(value) != nil {
			t.Fatalf("representable precision rejected: %s %v", input, err)
		}
	}
}
