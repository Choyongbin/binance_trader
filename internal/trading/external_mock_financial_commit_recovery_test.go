package trading

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

// Restart between every durable stage. A missing caller ACK is deliberately
// simulated by discarding the response and opening a NEW store instance.
// No exchange/posting interface is present in this fixture.
func TestMockMultiLedgerRecoveryBeforeAfterEveryCheckpoint(t *testing.T) {
	ctx := context.Background()
	registry, store, events := mockCommitFixture(t)
	originalA, err := snapshotExecutionEvidence(events[0].DataDir)
	if err != nil {
		t.Fatal(err)
	}
	originalB, err := snapshotExecutionEvidence(events[1].DataDir)
	if err != nil {
		t.Fatal(err)
	}
	for i, expectedStage := range mockCommitKinds {
		// Interruption BEFORE the next checkpoint is opened.
		restarted, err := NewExternalMockFinancialCommitStore(store.root)
		if err != nil {
			t.Fatal(err)
		}
		pre, err := restarted.InspectRecovery(ctx, registry, events)
		if err != nil {
			t.Fatal(err)
		}
		if pre.CompletedSteps != i || !pre.ResumeAvailable || pre.NextStage != expectedStage || pre.SimulatedViewsVisible || pre.CommitSHA256 != "" || pre.PostingApplied || pre.SettlementAuthorized || pre.TradingAuthorized {
			t.Fatalf("unsafe pre-%s resume plan: %+v", expectedStage, pre)
		}
		// Interruption AFTER the checkpoint was persisted, BEFORE the
		// caller received an ACK. A fresh process must inspect, NOT replay.
		if _, err := restarted.ResumeNext(ctx, registry, events, pre.CompletedSteps, pre.ConsistencySHA256, ExternalMockCommitAcknowledgement); err != nil {
			t.Fatalf("write %s: %v", expectedStage, err)
		}
		restarted, err = NewExternalMockFinancialCommitStore(store.root)
		if err != nil {
			t.Fatal(err)
		}
		after, err := restarted.InspectRecovery(ctx, registry, events)
		if err != nil {
			t.Fatal(err)
		}
		if after.CompletedSteps != i+1 || after.LastDurableStage != expectedStage || after.PostingApplied || after.SettlementAuthorized || after.TradingAuthorized {
			t.Fatalf("lost-ACK recovery %s: %+v", expectedStage, after)
		}
		if i < len(mockCommitKinds)-1 {
			if !after.ResumeAvailable || after.NextStage != mockCommitKinds[i+1] || after.SimulatedViewsVisible || after.CommitSHA256 != "" {
				t.Fatalf("partial views visible %s: %+v", expectedStage, after)
			}
		} else if after.ResumeAvailable || after.NextStage != "" || after.Status != ExternalMockRecoveryFinal || !after.SimulatedViewsVisible || after.CommitSHA256 == "" {
			t.Fatalf("final marker not recovered: %+v", after)
		}
		// A retransmitted already-durable stage must be idempotent.
		again, err := restarted.Advance(ctx, registry, events, expectedStage, ExternalMockCommitAcknowledgement)
		if err != nil || again.CompletedSteps != i+1 || again.PostingApplied || again.SettlementAuthorized {
			t.Fatalf("duplicate stage %s: %+v %v", expectedStage, again, err)
		}
	}
	last, err := store.Inspect(ctx, registry, events)
	if err != nil {
		t.Fatal(err)
	}
	// COMMIT ACK lost, followed by a redundant resume request: no new write.
	again, err := store.ResumeNext(ctx, registry, events, len(mockCommitKinds), last.ConsistencySHA256, ExternalMockCommitAcknowledgement)
	if err != nil || !reflect.DeepEqual(last, again) {
		t.Fatalf("replayed already committed batch: %+v %v", again, err)
	}
	finalA, err := snapshotExecutionEvidence(events[0].DataDir)
	if err != nil || !reflect.DeepEqual(originalA, finalA) {
		t.Fatalf("source A was modified: %v", err)
	}
	finalB, err := snapshotExecutionEvidence(events[1].DataDir)
	if err != nil || !reflect.DeepEqual(originalB, finalB) {
		t.Fatalf("source B was modified: %v", err)
	}
}

// Inject two different realistic interrupted writes: an O_EXCL-created zero
// byte slot and a truncated JSON slot. No recovery path may overwrite either.
func TestMockMultiLedgerRecoveryPartialWritesBlockAndPreserveEvidence(t *testing.T) {
	for _, tc := range []struct {
		name    string
		content []byte
	}{
		{"created_empty", []byte{}},
		{"truncated_record", []byte(`{"schema":1,"stage":"LIFECYCLE"`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			registry, store, events := mockCommitFixture(t)
			if _, err := store.Advance(ctx, registry, events, "INTENT", ExternalMockCommitAcknowledgement); err != nil {
				t.Fatal(err)
			}
			state, err := InspectExternalGlobalAtomicConsistency(ctx, registry, events)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(store.dir, "batch-"+state.BatchKey, "lifecycle.json")
			if err := os.WriteFile(path, tc.content, 0600); err != nil {
				t.Fatal(err)
			}
			restarted, err := NewExternalMockFinancialCommitStore(store.root)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := restarted.InspectRecovery(ctx, registry, events); err == nil {
				t.Fatal("torn evidence accepted")
			}
			if _, err := restarted.ResumeNext(ctx, registry, events, 1, state.DigestSHA256, ExternalMockCommitAcknowledgement); err == nil {
				t.Fatal("torn evidence automatically repaired")
			}
			if _, err := restarted.Advance(ctx, registry, events, "LIFECYCLE", ExternalMockCommitAcknowledgement); err == nil {
				t.Fatal("torn checkpoint overwritten")
			}
			got, err := os.ReadFile(path)
			if err != nil || !reflect.DeepEqual(got, tc.content) {
				t.Fatalf("torn evidence modified: %v", err)
			}
		})
	}
}

func TestMockMultiLedgerRecoveryFinalSealDamageNeverDowngradesToStaging(t *testing.T) {
	ctx := context.Background()
	registry, store, events := mockCommitFixture(t)
	for _, stage := range mockCommitKinds {
		if _, err := store.Advance(ctx, registry, events, stage, ExternalMockCommitAcknowledgement); err != nil {
			t.Fatal(err)
		}
	}
	state, err := InspectExternalGlobalAtomicConsistency(ctx, registry, events)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(store.dir, "batch-"+state.BatchKey, "commit.json")
	if err = os.WriteFile(path, []byte(`{"schema":1,"stage":"COMMIT","checksum":"FAKE"}`), 0600); err != nil {
		t.Fatal(err)
	}
	restarted, err := NewExternalMockFinancialCommitStore(store.root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Inspect(ctx, registry, events); err == nil {
		t.Fatal("corrupted final marker exposed ledger")
	}
	if _, err := restarted.InspectRecovery(ctx, registry, events); err == nil {
		t.Fatal("corrupted final marker treated as staged")
	}
	if _, err := restarted.ResumeNext(ctx, registry, events, 1, state.DigestSHA256, ExternalMockCommitAcknowledgement); err == nil {
		t.Fatal("corrupted final marker repaired")
	}
	// A deleted middle checkpoint is an invalid prefix even if the end marker exists.
	if err := os.Remove(filepath.Join(store.dir, "batch-"+state.BatchKey, "ownership.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.InspectRecovery(ctx, registry, events); err == nil {
		t.Fatal("deleted middle checkpoint accepted")
	}
}

func TestMockMultiLedgerRecoveryRequiresAckAndNoConcurrentDuplicateProgress(t *testing.T) {
	ctx := context.Background()
	registry, store, events := mockCommitFixture(t)
	if _, err := store.ResumeNext(ctx, registry, events, 0, "0000000000000000000000000000000000000000000000000000000000000000", ""); err == nil {
		t.Fatal("missing approval accepted")
	}
	if _, err := store.ResumeNext(ctx, registry, events, 0, "0000000000000000000000000000000000000000000000000000000000000000", "REAL_FINANCIAL_COMMIT"); err == nil {
		t.Fatal("authority escalation accepted")
	}
	recovery, err := store.InspectRecovery(ctx, registry, events)
	if err != nil {
		t.Fatal(err)
	}
	const attempts = 4
	var wg sync.WaitGroup
	results := make(chan error, attempts)
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := store.ResumeNext(ctx, registry, events, recovery.CompletedSteps, recovery.ConsistencySHA256, ExternalMockCommitAcknowledgement)
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Logf("concurrent retry safely blocked: %v", err)
		}
	}
	got, err := store.InspectRecovery(ctx, registry, events)
	if err != nil || got.CompletedSteps != 1 || got.NextStage != "LIFECYCLE" || got.SimulatedViewsVisible {
		t.Fatalf("duplicate progress or unexpected visibility: %+v %v", got, err)
	}
	// Retry with the original pin cannot silently move to the next stage.
	if _, err := store.ResumeNext(ctx, registry, events, recovery.CompletedSteps, recovery.ConsistencySHA256, ExternalMockCommitAcknowledgement); err == nil {
		t.Fatal("stale resume pin accepted")
	}
	// Different original incident after restart cannot use the old checkpoint.
	events[0].ClientOrderID = "foreign-owner"
	if _, err := store.InspectRecovery(ctx, registry, events); err == nil {
		t.Fatal("foreign source accepted")
	}
}
