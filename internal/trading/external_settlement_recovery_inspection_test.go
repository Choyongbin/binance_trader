package trading

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestExternalSettlementRecoveryInspectionRestartAndNoPosting(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			ctx := context.Background()
			data, store, id, archive := settlementJournalFixture(t, side)
			before, err := snapshotExecutionEvidence(data)
			if err != nil {
				t.Fatal(err)
			}
			first, err := store.InspectRecovery(ctx, id, archive)
			if err != nil {
				t.Fatal(err)
			}
			if first.Stage != ExternalRecoveryPrerequisitesOnly || first.PostingAuthorized || first.SettlementAuthorized || first.TradingAuthorized || len(first.EntryTradeIDs) == 0 {
				t.Fatalf("invalid initial inspection: %+v", first)
			}
			if _, err := os.Stat(store.dir); !os.IsNotExist(err) {
				t.Fatalf("inspection created checkpoint: %v", err)
			}
			if _, err = store.Prepare(ctx, id, archive); err != nil {
				t.Fatal(err)
			}
			restarted, err := NewExternalSettlementJournalStore(data)
			if err != nil {
				t.Fatal(err)
			}
			second, err := restarted.InspectRecovery(ctx, id, archive)
			if err != nil {
				t.Fatal(err)
			}
			if second.Stage != ExternalRecoveryPreparedOnly || second.PreparedChecksum == "" || second.FencedChecksum != "" {
				t.Fatalf("bad prepared: %+v", second)
			}
			if _, err = restarted.Fence(ctx, id, archive); err != nil {
				t.Fatal(err)
			}
			finalStore, err := NewExternalSettlementJournalStore(data)
			if err != nil {
				t.Fatal(err)
			}
			third, err := finalStore.InspectRecovery(ctx, id, archive)
			if err != nil {
				t.Fatal(err)
			}
			fourth, err := finalStore.InspectRecovery(ctx, id, archive)
			if err != nil {
				t.Fatal(err)
			}
			if third.Stage != ExternalRecoveryFencedOnly || third.FencedChecksum == "" || !reflect.DeepEqual(third, fourth) || third.PostingAuthorized || third.SettlementAuthorized || third.LifecycleChanged || third.OwnershipChanged {
				t.Fatalf("bad fenced: %+v", third)
			}
			after, err := snapshotExecutionEvidence(data)
			if err != nil || before != after {
				t.Fatalf("modified original journals: %v", err)
			}
		})
	}
}

func TestExternalSettlementRecoveryInspectionFailsClosedOnTornAndTampered(t *testing.T) {
	for _, tc := range []string{"torn_prepare", "orphan_fence", "changed_source", "unknown_file", "torn_fence"} {
		t.Run(tc, func(t *testing.T) {
			ctx := context.Background()
			data, store, id, archive := settlementJournalFixture(t, Short)
			if _, err := store.Prepare(ctx, id, archive); err != nil {
				t.Fatal(err)
			}
			var err error
			switch tc {
			case "torn_prepare":
				err = os.WriteFile(store.path(ExternalSettlementJournalPrepared), []byte("{"), 0600)
			case "orphan_fence":
				_, err = store.Fence(ctx, id, archive)
				if err == nil {
					err = os.Remove(store.path(ExternalSettlementJournalPrepared))
				}
			case "changed_source":
				err = os.Remove(filepath.Join(data, "trading", "fills", "BTCUSDT_901.json"))
			case "unknown_file":
				err = os.WriteFile(filepath.Join(store.dir, "surprise.json"), []byte("{}"), 0600)
			case "torn_fence":
				_, err = store.Fence(ctx, id, archive)
				if err == nil {
					err = os.WriteFile(store.path(ExternalSettlementJournalFenced), []byte("{"), 0600)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.InspectRecovery(ctx, id, archive); err == nil {
				t.Fatal("unsafe evidence was accepted")
			}
		})
	}
}
