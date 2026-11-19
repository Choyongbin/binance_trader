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
)

func settlementJournalFixture(t *testing.T, side PositionSide) (string, *ExternalSettlementJournalStore, string, PackageAEntryArchive) {
	t.Helper()
	ctx := context.Background()
	data, mock, _ := externalCloseFixture(t, side)
	recs, err := readJournalForEntryGapTest(data)
	if err != nil || len(recs) != 1 {
		t.Fatalf("entry journal: %v", err)
	}
	id := recs[0].Plan.ClientOrderID
	archive, err := CapturePackageAEntryArchive(ctx, data, id, mock.readerOrders, DemoCredentials{APIKey: "fake", Secret: "fake"}, time.Now().UTC())
	if err != nil {
		t.Fatalf("signed entry fixture: %v", err)
	}
	report := reviewFixture(data, mock)
	if len(report.ExternalCloseRehearsals) != 1 {
		t.Fatal("missing reviewed external close")
	}
	review, err := NewExternalCloseReviewLedger(data)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = review.Stage(report.ExternalCloseRehearsals[0], time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err = review.Decide(id, ExternalReviewAcknowledged, "operator_01", "Review only, no posting", time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	proposals, err := NewExternalCloseSettlementProposalStore(data)
	if err != nil {
		t.Fatal(err)
	}
	p, err := proposals.Prepare(id, time.Now().Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	replay, err := NewExternalCloseReplayCheckpointStore(data)
	if err != nil {
		t.Fatal(err)
	}
	rd := mockCloseReader{v: SignedCloseEvidence{EntryOrderID: p.EntryOrderID, ExitOrderID: p.ExitOrderID, EntryTradeIDs: p.EntryTradeIDs, ExitTradeIDs: p.ExitTradeIDs, AccountFlat: true, ObservedAt: time.Now().UTC()}}
	if _, err = replay.Stage(ctx, id, rd, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err = replay.Recheck(ctx, id, rd, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	intent, err := NewExternalClosePostingIntentStore(data)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = intent.Reserve(id); err != nil {
		t.Fatal(err)
	}
	shadow, err := NewExternalCloseShadowLedger(data)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = shadow.Prepare(id); err != nil {
		t.Fatal(err)
	}
	if _, err = shadow.Replay(id); err != nil {
		t.Fatal(err)
	}
	dual, err := NewExternalCloseDualLedgerPlanStore(data)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = dual.Prepare(id); err != nil {
		t.Fatal(err)
	}
	if _, err = dual.Verify(id); err != nil {
		t.Fatal(err)
	}
	store, err := NewExternalSettlementJournalStore(data)
	if err != nil {
		t.Fatal(err)
	}
	return data, store, id, archive
}
func TestExternalSettlementJournalLongShortRestartConcurrencyNoPosting(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			ctx := context.Background()
			dir, store, id, archive := settlementJournalFixture(t, side)
			before, err := snapshotExecutionEvidence(dir)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = os.Lstat(store.dir); !os.IsNotExist(err) {
				t.Fatalf("constructor made journal: %v", err)
			}
			const n = 12
			var wg sync.WaitGroup
			outcomes := make(chan error, n)
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					got, e := store.Prepare(ctx, id, archive)
					if e == nil && (got.PostingApplied || got.EntryRecoveryApplied || got.LifecycleChanged || got.OwnershipChanged || got.TradingAuthorized || got.SettlementAuthorized || got.Stage != ExternalSettlementJournalPrepared) {
						e = errUnsafeShadow
					}
					outcomes <- e
				}()
			}
			wg.Wait()
			close(outcomes)
			for e := range outcomes {
				if e != nil {
					t.Fatalf("concurrent prepare: %v", e)
				}
			}
			restarted, err := NewExternalSettlementJournalStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			root, ev, err := restarted.Load(ctx, id, archive)
			if err != nil || ev != nil || root.ClientOrderID != id || root.PositionSide != side || !includesJournalBlocker(root.BlockerCodes, "CROSS_LEDGER_ATOMIC_POSTING_NOT_IMPLEMENTED") {
				t.Fatalf("bad prepare: %+v %v", root, err)
			}
			outcomes = make(chan error, n)
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					got, e := restarted.Fence(ctx, id, archive)
					if e == nil && (got.PostingApplied || got.TradingAuthorized || got.SettlementAuthorized || got.PreviousChecksum != root.Checksum || got.Stage != ExternalSettlementJournalFenced) {
						e = errUnsafeShadow
					}
					outcomes <- e
				}()
			}
			wg.Wait()
			close(outcomes)
			for e := range outcomes {
				if e != nil {
					t.Fatal(e)
				}
			}
			fresh, err := NewExternalSettlementJournalStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			_, final, err := fresh.Load(ctx, id, archive)
			if err != nil || final == nil || final.Stage != ExternalSettlementJournalFenced {
				t.Fatalf("restart: %+v %v", final, err)
			}
			if again, err := fresh.Prepare(ctx, id, archive); err != nil || again.Checksum != root.Checksum {
				t.Fatal("idempotent prepare", err)
			}
			if again, err := fresh.Fence(ctx, id, archive); err != nil || again.Checksum != final.Checksum {
				t.Fatal("idempotent fence", err)
			}
			if after, err := snapshotExecutionEvidence(dir); err != nil || after != before {
				t.Fatal("original evidence mutated", err)
			}
			entries, err := os.ReadDir(fresh.dir)
			if err != nil || len(entries) != 2 {
				t.Fatal("unexpected checkpoint entries", err)
			}
			if _, err := fresh.Prepare(ctx, "bt_0000000000000000000000000000", archive); err == nil {
				t.Fatal("second transaction accepted")
			}
		})
	}
}
func TestExternalSettlementJournalRefusesMissingPrerequisiteAndDrift(t *testing.T) {
	ctx := context.Background()
	dir, store, id, archive := settlementJournalFixture(t, Short)
	if _, err := store.Fence(ctx, id, archive); err == nil {
		t.Fatal("fence before prepare")
	}
	bad := archive
	bad.Checksum = strings.Repeat("f", 64)
	if _, err := store.Prepare(ctx, id, bad); err == nil {
		t.Fatal("tampered archive accepted")
	}
	if _, err := store.Prepare(ctx, id, archive); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "trading", "fills", "BTCUSDT_901.json")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Load(ctx, id, archive); err == nil {
		t.Fatal("missing signed entry fill survived")
	}
	if _, err := store.Fence(ctx, id, archive); err == nil {
		t.Fatal("posted stale evidence")
	}
}
func TestExternalSettlementJournalTamperTornOrphanLockFailClosed(t *testing.T) {
	for _, tc := range []string{"checksum", "fake_posting", "pretend_settled", "torn_prepare", "torn_fence", "orphan", "unknown", "symlink", "active_runtime", "parent_tamper", "archive_changed", "local_drift"} {
		t.Run(tc, func(t *testing.T) {
			ctx := context.Background()
			dir, store, id, a := settlementJournalFixture(t, Long)
			root, err := store.Prepare(ctx, id, a)
			if err != nil {
				t.Fatal(err)
			}
			var rewrite bool
			switch tc {
			case "checksum":
				root.Checksum = "0"
				rewrite = true
			case "fake_posting":
				root.PostingApplied = true
				root.Checksum = root.digest()
				rewrite = true
			case "pretend_settled":
				root.SettlementAuthorized = true
				root.Checksum = root.digest()
				rewrite = true
			case "torn_prepare":
				err = os.WriteFile(store.path(ExternalSettlementJournalPrepared), []byte(`{"schema":`), 0600)
			case "torn_fence":
				_, err = store.Fence(ctx, id, a)
				if err == nil {
					err = os.WriteFile(store.path(ExternalSettlementJournalFenced), []byte(`{}`), 0600)
				}
			case "orphan":
				err = os.Remove(store.path(ExternalSettlementJournalPrepared))
			case "unknown":
				err = os.WriteFile(filepath.Join(store.dir, "danger.json"), []byte(`{}`), 0600)
			case "symlink":
				err = os.Remove(store.path(ExternalSettlementJournalPrepared))
				if err == nil {
					err = os.Symlink(store.path(ExternalSettlementJournalFenced), store.path(ExternalSettlementJournalPrepared))
					if err != nil {
						t.Skip(err)
					}
				}
			case "active_runtime":
				err = os.Mkdir(filepath.Join(dir, "trading", "coordinator.active"), 0700)
			case "parent_tamper":
				err = os.WriteFile(store.dual.path(ExternalDualLedgerVerified), []byte(`{}`), 0600)
			case "archive_changed":
				a.Fills[0].Price = "123"
				a.Checksum = a.digest()
			case "local_drift":
				err = os.Remove(filepath.Join(dir, "trading", "fills", "BTCUSDT_901.json"))
			}
			if err != nil {
				t.Fatal(err)
			}
			if rewrite {
				raw, e := json.Marshal(root)
				if e != nil {
					t.Fatal(e)
				}
				if e = os.WriteFile(store.path(ExternalSettlementJournalPrepared), raw, 0600); e != nil {
					t.Fatal(e)
				}
			}
			if _, _, err := store.Load(ctx, id, a); err == nil {
				t.Fatal("unsafe journal passed read")
			}
			if _, err := store.Fence(ctx, id, a); err == nil {
				t.Fatal("unsafe journal advanced")
			}
		})
	}
}
func TestExternalSettlementJournalRejectsAbsentDualVerification(t *testing.T) {
	ctx := context.Background()
	dir, store, id, archive := settlementJournalFixture(t, Short)
	if err := os.Remove(store.dual.path(ExternalDualLedgerVerified)); err != nil {
		t.Fatal(err)
	}
	s, err := NewExternalSettlementJournalStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Prepare(ctx, id, archive); err == nil {
		t.Fatal("unverified dual ledger accepted")
	}
}
