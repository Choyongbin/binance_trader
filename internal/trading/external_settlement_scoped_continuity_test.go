package trading

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func linkedScopedFixture(t *testing.T, side PositionSide) (string, string, PackageAEntryArchive, *ExternalAtomicRehearsalStore) {
	t.Helper()
	ctx := context.Background()
	data, legacy, id, archive := settlementJournalFixture(t, side)
	if _, err := os.Lstat(legacy.dir); !os.IsNotExist(err) {
		t.Fatalf("fixture must not write legacy journal: %v", err)
	}
	journal, err := NewExternalSettlementEventJournalStore(data, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := journal.Prepare(ctx, id, archive); err != nil {
		t.Fatal(err)
	}
	if _, err := journal.Fence(ctx, id, archive); err != nil {
		t.Fatal(err)
	}
	atomic, err := NewExternalAtomicLinkedEventStore(data, id)
	if err != nil {
		t.Fatal(err)
	}
	return data, id, archive, atomic
}

func sealLinkedScoped(t *testing.T, store *ExternalAtomicRehearsalStore, id string, archive PackageAEntryArchive) string {
	t.Helper()
	ctx := context.Background()
	if _, err := store.Prepare(ctx, id, archive); err != nil {
		t.Fatal(err)
	}
	if _, err := store.StageLifecycle(ctx, id, archive); err != nil {
		t.Fatal(err)
	}
	if _, err := store.StageOwnership(ctx, id, archive); err != nil {
		t.Fatal(err)
	}
	sealed, err := store.Seal(ctx, id, archive)
	if err != nil || !sealed.AtomicProjectionVisible {
		t.Fatalf("seal: %+v %v", sealed, err)
	}
	return sealed.CommitSHA256
}

func TestScopedSettlementContinuityLongShortAndRestart(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			data, id, archive, store := linkedScopedFixture(t, side)
			ctx := context.Background()
			before, err := snapshotExecutionEvidence(data)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.InspectScopedSettlementContinuity(ctx, id, archive, strings.Repeat("f", 64)); err == nil {
				t.Fatal("unsealed candidate was accepted")
			}
			pinned := sealLinkedScoped(t, store, id, archive)
			got, err := store.InspectScopedSettlementContinuity(ctx, id, archive, pinned)
			if err != nil {
				t.Fatal(err)
			}
			if got.Stage != ExternalScopedContinuityNotPosted || got.DigestSHA256 != got.digest() ||
				got.ClientOrderID != id || got.PositionSide != side || got.TransactionKey == "" ||
				got.LedgerPair.CommitSHA256 != pinned || got.LedgerPair.Lifecycle.Proposal == "" ||
				got.LedgerPair.Ownership.Proposal == "" || got.PostingApplied ||
				got.SettlementAuthorized || got.TradingAuthorized || len(got.BlockerCodes) < 6 {
				t.Fatalf("invalid scoped continuity: %+v", got)
			}
			restarted, err := NewExternalAtomicLinkedEventStore(data, id)
			if err != nil {
				t.Fatal(err)
			}
			again, err := restarted.InspectScopedSettlementContinuity(ctx, id, archive, pinned)
			if err != nil || !reflect.DeepEqual(got, again) {
				t.Fatalf("restart drift: %v", err)
			}
			if _, err := restarted.InspectScopedSettlementContinuity(ctx, id, archive, strings.Repeat("f", 64)); err == nil {
				t.Fatal("foreign commit accepted")
			}
			if _, err := restarted.InspectScopedSettlementContinuity(ctx, "bt_ffffffffffffffffffffffffffff", archive, pinned); err == nil {
				t.Fatal("foreign incident accepted")
			}
			oldAtomic, err := NewExternalAtomicEventStore(data, id)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := oldAtomic.InspectScopedSettlementContinuity(ctx, id, archive, pinned); err == nil {
				t.Fatal("legacy-backed atomic store was incorrectly joined with scoped journal")
			}
			after, err := snapshotExecutionEvidence(data)
			if err != nil || before != after {
				t.Fatalf("original execution files changed: %v", err)
			}
		})
	}
}

func TestScopedSettlementContinuityFailsClosedAndNoAutoMigration(t *testing.T) {
	for _, tc := range []string{"journal_fence_missing", "journal_rehashed_tamper", "atomic_seal_missing", "atomic_rehashed_tamper", "source_drift", "wrong_archive", "legacy_only"} {
		t.Run(tc, func(t *testing.T) {
			ctx := context.Background()
			if tc == "legacy_only" {
				data, legacy, id, archive := atomicRehearsalFixture(t, Long)
				if _, err := legacy.Prepare(ctx, id, archive); err != nil {
					t.Fatal(err)
				}
				linked, err := NewExternalAtomicLinkedEventStore(data, id)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := linked.Prepare(ctx, id, archive); err == nil {
					t.Fatal("legacy duplicate accepted as linked scoped event")
				}
				return
			}
			data, id, archive, store := linkedScopedFixture(t, Short)
			pinned := sealLinkedScoped(t, store, id, archive)
			journal := store.journal
			switch tc {
			case "journal_fence_missing":
				if err := os.Remove(journal.path(ExternalSettlementJournalFenced)); err != nil {
					t.Fatal(err)
				}
			case "journal_rehashed_tamper":
				p := journal.path(ExternalSettlementJournalFenced)
				var r ExternalSettlementJournalRecord
				if err := strictReviewJSON(p, &r); err != nil {
					t.Fatal(err)
				}
				r.TransactionKey = strings.Repeat("e", 64)
				r.Checksum = r.digest()
				b, err := json.Marshal(r)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(p, b, 0600); err != nil {
					t.Fatal(err)
				}
			case "atomic_seal_missing":
				if err := os.Remove(filepath.Join(store.dir, "seal.json")); err != nil {
					t.Fatal(err)
				}
			case "atomic_rehashed_tamper":
				p := filepath.Join(store.dir, "seal.json")
				var r ExternalAtomicRehearsalRecord
				if err := strictReviewJSON(p, &r); err != nil {
					t.Fatal(err)
				}
				r.LocalEvidenceSHA256 = strings.Repeat("e", 64)
				r.Checksum = r.digest()
				b, err := json.Marshal(r)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(p, b, 0600); err != nil {
					t.Fatal(err)
				}
			case "source_drift":
				if err := os.Remove(filepath.Join(data, "trading", "fills", "BTCUSDT_901.json")); err != nil {
					t.Fatal(err)
				}
			case "wrong_archive":
				archive.Gap.ClientOrderID = "bt_eeeeeeeeeeeeeeeeeeeeeeeeeeee"
			}
			if _, err := store.InspectScopedSettlementContinuity(ctx, id, archive, pinned); err == nil {
				t.Fatal("inconsistent or changed event evidence was accepted")
			}
		})
	}
}
