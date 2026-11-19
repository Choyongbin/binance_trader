package trading

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestExternalSettlementEvidenceMatrixRestartBothSides(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			ctx := context.Background()
			data, store, id, archive := settlementJournalFixture(t, side)
			before, err := snapshotExecutionEvidence(data)
			if err != nil {
				t.Fatal(err)
			}
			check := func(expected string) {
				t.Helper()
				r, err := store.InspectEvidenceMatrix(ctx, id, archive)
				if err != nil {
					t.Fatal(err)
				}
				if r.JournalStage != expected || len(r.MissingEntryTradeIDs) != 0 || !r.Checks[2].Evidenced || r.PostingAuthorized || r.SettlementAuthorized || r.TradingAuthorized {
					t.Fatalf("unsafe inspection: %+v", r)
				}
				if err := ValidateExternalSettlementEvidenceMatrix(r); err != nil {
					t.Fatal(err)
				}
				again, err := store.InspectEvidenceMatrix(ctx, id, archive)
				if err != nil || !reflect.DeepEqual(r, again) {
					t.Fatalf("not deterministic: %v", err)
				}
				r.TradingAuthorized = true
				if ValidateExternalSettlementEvidenceMatrix(r) == nil {
					t.Fatal("authorization forgery accepted")
				}
				r.TradingAuthorized = false
				r.Checks[3].Evidenced = true
				r.DigestSHA256 = r.digest()
				if ValidateExternalSettlementEvidenceMatrix(r) == nil {
					t.Fatal("fresh signed proof forgery accepted")
				}
			}
			check(ExternalRecoveryPrerequisitesOnly)
			if _, err := store.Prepare(ctx, id, archive); err != nil {
				t.Fatal(err)
			}
			check(ExternalRecoveryPreparedOnly)
			if _, err := store.Fence(ctx, id, archive); err != nil {
				t.Fatal(err)
			}
			restarted, err := NewExternalSettlementJournalStore(data)
			if err != nil {
				t.Fatal(err)
			}
			store = restarted
			check(ExternalRecoveryFencedOnly)
			after, err := snapshotExecutionEvidence(data)
			if err != nil || after != before {
				t.Fatalf("mutated original evidence: %v", err)
			}
		})
	}
}
func TestExternalSettlementEvidenceMatrixRejectsSourceTamper(t *testing.T) {
	ctx := context.Background()
	data, store, id, archive := settlementJournalFixture(t, Short)
	if _, err := store.InspectEvidenceMatrix(ctx, id, archive); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(data, "trading", "fills", "BTCUSDT_901.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.InspectEvidenceMatrix(ctx, id, archive); err == nil {
		t.Fatal("stale local fills accepted")
	}
}
