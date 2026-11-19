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

func baselineFixture(t *testing.T, side PositionSide) (string, string, PackageAIncidentArchive, *ExternalHistoryCaptureStore, ExternalHistoryBaselineClaim) {
	t.Helper()
	data, id, incident, _ := boundedFinancialFixtureWithFunding(t, side, "-0.125")
	store, err := NewExternalHistoryCaptureStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	src := externalCloseHash("offline controlled response")
	claim, err := PrepareExternalHistoryBaselineClaim(id, incident, src)
	if err != nil {
		t.Fatal(err)
	}
	return data, id, incident, store, claim
}

func TestHistoryBaselineRequiresSealedBoundedCheckpointAndNeverAuthorizes(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			data, id, incident, store, claim := baselineFixture(t, side)
			before, err := snapshotExecutionEvidence(data)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			partial, err := InspectExternalHistoryBaseline(ctx, store, data, id, incident, claim.OfflineSourceSHA256, claim)
			if err != nil {
				t.Fatal(err)
			}
			if partial.Stage != ExternalHistoryBaselineIncomplete || partial.BoundedCaptureComplete || partial.CoveredUntilMS != claim.FromMS-1 {
				t.Fatalf("unexpected initial report: %+v", partial)
			}
			if _, err = store.Capture(ctx, data, id, incident, claim.OfflineSourceSHA256, captureFixtureReader(incident)); err != nil {
				t.Fatal(err)
			}
			report, err := InspectExternalHistoryBaseline(ctx, store, data, id, incident, claim.OfflineSourceSHA256, claim)
			if err != nil {
				t.Fatal(err)
			}
			if report.Stage != ExternalHistoryBaselineMatched || !report.BoundedCaptureComplete || report.CoveredUntilMS != claim.ToMS ||
				report.WholeAccountHistoryProven || report.ModelSettlementAuthorized || report.ModelNetPnLFinal || report.ModelFundingAllocated ||
				report.TradingAuthorized || report.PostingApplied || report.AllSymbolsCovered || report.ExchangeSourceAttested {
				t.Fatalf("unsafe report: %+v", report)
			}
			if err := ValidateExternalHistoryBaselineReview(report); err != nil {
				t.Fatal(err)
			}
			repeat, err := InspectExternalHistoryBaseline(ctx, store, data, id, incident, claim.OfflineSourceSHA256, claim)
			if err != nil || !reflect.DeepEqual(report, repeat) {
				t.Fatalf("restart mismatch: %v", err)
			}
			var jobs sync.WaitGroup
			errs := make(chan error, 4)
			for i := 0; i < 4; i++ {
				jobs.Add(1)
				go func() {
					defer jobs.Done()
					r, e := InspectExternalHistoryBaseline(ctx, store, data, id, incident, claim.OfflineSourceSHA256, claim)
					if e == nil && !reflect.DeepEqual(r, report) {
						e = os.ErrInvalid
					}
					errs <- e
				}()
			}
			jobs.Wait()
			close(errs)
			for e := range errs {
				if e != nil {
					t.Fatal(e)
				}
			}
			after, err := snapshotExecutionEvidence(data)
			if err != nil || before != after {
				t.Fatal("read-only baseline mutated original evidence")
			}
		})
	}
}

func TestHistoryBaselineRejectsElevatedClaimEvenIfRehashed(t *testing.T) {
	data, id, incident, store, good := baselineFixture(t, Long)
	ctx := context.Background()
	if _, err := store.Capture(ctx, data, id, incident, good.OfflineSourceSHA256, captureFixtureReader(incident)); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"whole_account", "all_symbols", "stream", "model_funding", "settlement", "older_start", "later_end", "narrower", "scope", "missing_dataset", "duplicate_dataset", "wrong_client", "wrong_source", "wrong_incident", "wrong_checksum", "wrong_schema"} {
		t.Run(kind, func(t *testing.T) {
			c := good
			c.DataSets = append([]string(nil), good.DataSets...)
			switch kind {
			case "whole_account":
				c.WholeAccountHistoryClaimed = true
			case "all_symbols":
				c.AllSymbolsClaimed = true
			case "stream":
				c.PrivateStreamContinuityClaimed = true
			case "model_funding":
				c.ModelFundingAllocatedClaimed = true
			case "settlement":
				c.ModelSettlementAuthorizedClaimed = true
			case "older_start":
				c.FromMS--
			case "later_end":
				c.ToMS++
			case "narrower":
				c.FromMS++
			case "scope":
				c.Scope = "ALL_SYMBOLS_ACCOUNT_LIFETIME"
			case "missing_dataset":
				c.DataSets = c.DataSets[:1]
			case "duplicate_dataset":
				c.DataSets = append(c.DataSets, c.DataSets[0])
			case "wrong_client":
				c.ClientOrderID += "x"
			case "wrong_source":
				c.OfflineSourceSHA256 = strings.Repeat("a", 64)
			case "wrong_incident":
				c.IncidentArchiveSHA256 = strings.Repeat("a", 64)
			case "wrong_checksum":
				c.Checksum = strings.Repeat("0", 64)
			case "wrong_schema":
				c.Schema = 2
			}
			if kind != "wrong_checksum" {
				c.Checksum = c.digest()
			}
			if _, err := InspectExternalHistoryBaseline(ctx, store, data, id, incident, good.OfflineSourceSHA256, c); err == nil {
				t.Fatal("over-claim was accepted")
			}
		})
	}
}

func TestHistoryBaselineTamperAndIncompleteSealBlocked(t *testing.T) {
	data, id, incident, store, claim := baselineFixture(t, Short)
	ctx := context.Background()
	if _, err := store.Capture(ctx, data, id, incident, claim.OfflineSourceSHA256, captureFixtureReader(incident)); err != nil {
		t.Fatal(err)
	}
	report, err := InspectExternalHistoryBaseline(ctx, store, data, id, incident, claim.OfflineSourceSHA256, claim)
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*ExternalHistoryBaselineReview){
		func(r *ExternalHistoryBaselineReview) { r.ModelSettlementAuthorized = true },
		func(r *ExternalHistoryBaselineReview) { r.ModelFundingAllocated = true },
		func(r *ExternalHistoryBaselineReview) { r.WholeAccountHistoryProven = true },
		func(r *ExternalHistoryBaselineReview) { r.ModelNetPnLFinal = true },
		func(r *ExternalHistoryBaselineReview) { r.BlockerCodes = []string{} },
		func(r *ExternalHistoryBaselineReview) { r.Stage = "ACCOUNT_WIDE_VERIFIED" },
	} {
		forged := report
		forged.BlockerCodes = append([]string(nil), report.BlockerCodes...)
		change(&forged)
		forged.DigestSHA256 = forged.digest()
		if err := ValidateExternalHistoryBaselineReview(forged); err == nil {
			t.Fatalf("rehashed authorizing report passed: %+v", forged)
		}
	}
	// A checkpoint deleted while a completion seal exists must fail closed.
	slot := filepath.Join(store.dir, captureSlot(1))
	slotBytes, err := os.ReadFile(slot)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(slot); err != nil {
		t.Fatal(err)
	}
	if _, err := InspectExternalHistoryBaseline(ctx, store, data, id, incident, claim.OfflineSourceSHA256, claim); err == nil {
		t.Fatal("lost checkpoint accepted despite completion seal")
	}
	if err := os.WriteFile(slot, slotBytes, 0600); err != nil {
		t.Fatal(err)
	}
	// If the separate local seal is removed, do not mislabel the prefix as complete.
	// Local files cannot prove that an adversary never removed a former seal.
	if err := os.Remove(filepath.Join(store.dir, "seal.json")); err != nil {
		t.Fatal(err)
	}
	partial, err := InspectExternalHistoryBaseline(ctx, store, data, id, incident, claim.OfflineSourceSHA256, claim)
	if err != nil || partial.BoundedCaptureComplete || partial.Stage != ExternalHistoryBaselineIncomplete {
		t.Fatalf("missing seal accepted: %+v %v", partial, err)
	}

}

func TestHistoryBaselineRejectsMalformedManifestRoundtrip(t *testing.T) {
	_, _, _, _, claim := baselineFixture(t, Long)
	b, err := json.Marshal(claim)
	if err != nil {
		t.Fatal(err)
	}
	var out ExternalHistoryBaselineClaim
	if err = json.Unmarshal(b, &out); err != nil || !reflect.DeepEqual(claim, out) {
		t.Fatalf("roundtrip %v", err)
	}
}
