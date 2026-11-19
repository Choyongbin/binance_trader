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

func TestFinancialCommitPreflightLinkedLongShortReadOnly(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			ctx := context.Background()
			data, id, archive, store := linkedScopedFixture(t, side)
			seal := sealLinkedScoped(t, store, id, archive)
			original, err := snapshotExecutionEvidence(data)
			if err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadDir(store.dir)
			if err != nil {
				t.Fatal(err)
			}
			got, err := store.InspectFinancialCommitPreflight(ctx, id, archive, seal)
			if err != nil {
				t.Fatal(err)
			}
			if got.Schema != 1 || got.Stage != ExternalFinancialCommitBlocked || got.DigestSHA256 != got.digest() ||
				got.ClientOrderID != id || got.ModelID == "" || got.PositionSide != side ||
				got.PinnedCommitSHA256 != seal || !externalReviewHash.MatchString(got.ContinuitySHA256) ||
				got.LifecycleCandidate == "" || got.OwnershipCandidate == "" ||
				len(got.OutstandingRequirements) < 7 || got.PostingApplied || got.LifecycleChanged ||
				got.OwnershipChanged || got.SettlementAuthorized || got.TradingAuthorized {
				t.Fatalf("unexpected financial commit preflight: %+v", got)
			}
			for _, requirement := range []string{"HTTP_423_ORDER_LOCK_REQUIRED", "FRESH_SIGNED_ACCOUNT_AND_ORDERS", "COMPLETE_RELEVANT_TRADE_HISTORY", "MODEL_FUNDING_AND_FEES", "EXTERNAL_EXIT_OWNERSHIP", "ATOMIC_CROSS_LEDGER_POSTING", "INDEPENDENT_SETTLEMENT_APPROVAL"} {
				if !includesJournalBlocker(got.OutstandingRequirements, requirement) {
					t.Fatalf("missing mandatory blocker %s", requirement)
				}
			}
			// A fresh store must produce a byte-for-byte equivalent blocked result.
			restarted, err := NewExternalAtomicLinkedEventStore(data, id)
			if err != nil {
				t.Fatal(err)
			}
			again, err := restarted.InspectFinancialCommitPreflight(ctx, id, archive, seal)
			if err != nil || !reflect.DeepEqual(got, again) {
				t.Fatalf("restart changed result: %v", err)
			}
			after, err := os.ReadDir(store.dir)
			if err != nil || len(before) != len(after) {
				t.Fatalf("inspection modified event records: %v", err)
			}
			current, err := snapshotExecutionEvidence(data)
			if err != nil || current != original {
				t.Fatalf("inspection modified original records: %v", err)
			}
			// This JSON can never be mistaken for authorization.
			raw, err := json.Marshal(got)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]interface{}
			if err := json.Unmarshal(raw, &fields); err != nil {
				t.Fatal(err)
			}
			for _, k := range []string{"posting_applied", "settlement_authorized", "trading_authorized"} {
				if fields[k] != false {
					t.Fatalf("output authorized %s", k)
				}
			}
		})
	}
}

func TestFinancialCommitPreflightRejectsUnsealedForeignAndCorruptEvidence(t *testing.T) {
	for _, kind := range []string{"unsealed", "foreign_seal", "wrong_event", "wrong_archive", "altered_seal", "missing_fenced", "non_linked_store"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			data, id, archive, store := linkedScopedFixture(t, Long)
			seal := strings.Repeat("a", 64)
			if kind != "unsealed" {
				seal = sealLinkedScoped(t, store, id, archive)
			}
			switch kind {
			case "foreign_seal":
				seal = strings.Repeat("f", 64)
			case "wrong_event":
				id = "bt_ffffffffffffffffffffffffffff"
			case "wrong_archive":
				archive.Gap.ClientOrderID = "bt_eeeeeeeeeeeeeeeeeeeeeeeeeeee"
			case "altered_seal":
				p := filepath.Join(store.dir, "seal.json")
				var r ExternalAtomicRehearsalRecord
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
			case "missing_fenced":
				if err := os.Remove(store.journal.path(ExternalSettlementJournalFenced)); err != nil {
					t.Fatal(err)
				}
			case "non_linked_store":
				var err error
				store, err = NewExternalAtomicEventStore(data, id)
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, err := store.InspectFinancialCommitPreflight(ctx, id, archive, seal); err == nil {
				t.Fatal("unsafe financial commit candidate accepted")
			}
		})
	}
}

func TestFinancialCommitPreflightConcurrentReadersDoNotPost(t *testing.T) {
	data, id, archive, store := linkedScopedFixture(t, Short)
	seal := sealLinkedScoped(t, store, id, archive)
	before, err := snapshotExecutionEvidence(data)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	failures := make(chan error, 5)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := store.InspectFinancialCommitPreflight(context.Background(), id, archive, seal)
			if err == nil && (result.PostingApplied || result.SettlementAuthorized || result.TradingAuthorized) {
				failures <- os.ErrPermission
			} else if err != nil {
				failures <- err
			}
		}()
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	after, err := snapshotExecutionEvidence(data)
	if err != nil || after != before {
		t.Fatalf("concurrent reader modified records: %v", err)
	}
}
