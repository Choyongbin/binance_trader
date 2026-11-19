package trading

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func mockCommitFixture(t *testing.T) (*ExternalGlobalEvidenceReservationStore, *ExternalMockFinancialCommitStore, []ExternalFinancialEventSource) {
	t.Helper()
	a := crossFinancialFixture(t, Long, "mock-atomic-a", 0, 80*time.Minute, 0)
	b := crossFinancialFixture(t, Short, "mock-atomic-b", 1000, 80*time.Minute, 0)
	events := []ExternalFinancialEventSource{a, b}
	registry, err := NewExternalGlobalEvidenceReservationStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = registry.ReserveBatch(context.Background(), events); err != nil {
		t.Fatal(err)
	}
	prepareGlobalAtomicFixture(t, a, true)
	prepareGlobalAtomicFixture(t, b, true)
	store, err := NewExternalMockFinancialCommitStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return registry, store, events
}

func TestMockMultiLedgerCommitBoundaryAndRestart(t *testing.T) {
	ctx := context.Background()
	registry, store, events := mockCommitFixture(t)
	beforeA, err := snapshotExecutionEvidence(events[0].DataDir)
	if err != nil {
		t.Fatal(err)
	}
	beforeB, err := snapshotExecutionEvidence(events[1].DataDir)
	if err != nil {
		t.Fatal(err)
	}
	absent, err := store.Inspect(ctx, registry, events)
	if err != nil || absent.Stage != ExternalMockCommitAbsent || absent.SimulatedViewsVisible {
		t.Fatalf("expected absent: %+v %v", absent, err)
	}
	if _, err = store.Advance(ctx, registry, events, "COMMIT", ExternalMockCommitAcknowledgement); err == nil {
		t.Fatal("out-of-order COMMIT accepted")
	}
	if _, err = store.Advance(ctx, registry, events, "INTENT", "REAL_SETTLEMENT"); err == nil {
		t.Fatal("financial authority injected")
	}
	for i, stage := range mockCommitKinds {
		got, e := store.Advance(ctx, registry, events, stage, ExternalMockCommitAcknowledgement)
		if e != nil {
			t.Fatalf("advance %s: %v", stage, e)
		}
		if got.CompletedSteps != i+1 || got.PostingApplied || got.TradingAuthorized || got.SettlementAuthorized {
			t.Fatalf("bad progress: %+v", got)
		}
		if i < 4 {
			if got.SimulatedViewsVisible || got.CommitSHA256 != "" || len(got.Lifecycle) != 0 || len(got.Ownership) != 0 || len(got.Financial) != 0 {
				t.Fatalf("partial ledger leaked after %s: %+v", stage, got)
			}
		} else if !got.SimulatedViewsVisible || got.Stage != ExternalMockCommitVisible || got.CommitSHA256 == "" || len(got.Lifecycle) != 2 || len(got.Ownership) != 2 || len(got.Financial) != 2 {
			t.Fatalf("mock three-ledger seal not visible atomically: %+v", got)
		}
		again, e := store.Advance(ctx, registry, events, stage, ExternalMockCommitAcknowledgement)
		if e != nil || !reflect.DeepEqual(again, got) {
			t.Fatalf("idempotence %s: %v", stage, e)
		}
	}
	restarted, err := NewExternalMockFinancialCommitStore(store.root)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := restarted.Inspect(ctx, registry, []ExternalFinancialEventSource{events[1], events[0]})
	if err != nil || recovered.Stage != ExternalMockCommitVisible || len(recovered.Financial) != 2 {
		t.Fatalf("restart: %+v %v", recovered, err)
	}
	afterA, err := snapshotExecutionEvidence(events[0].DataDir)
	if err != nil || !reflect.DeepEqual(beforeA, afterA) {
		t.Fatalf("source A mutated: %v", err)
	}
	afterB, err := snapshotExecutionEvidence(events[1].DataDir)
	if err != nil || !reflect.DeepEqual(beforeB, afterB) {
		t.Fatalf("source B mutated: %v", err)
	}
}

func TestMockMultiLedgerCommitTornAndTamperFailClosed(t *testing.T) {
	ctx := context.Background()
	reg, store, events := mockCommitFixture(t)
	if _, err := store.Advance(ctx, reg, events, "INTENT", ExternalMockCommitAcknowledgement); err != nil {
		t.Fatal(err)
	}
	state, err := InspectExternalGlobalAtomicConsistency(ctx, reg, events)
	if err != nil {
		t.Fatal(err)
	}
	batch := filepath.Join(store.dir, "batch-"+state.BatchKey)
	// A partial checkpoint must never be treated as a normal absent file.
	torn := filepath.Join(batch, "lifecycle.json")
	if err = os.WriteFile(torn, []byte("{\"schema\":1"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Inspect(ctx, reg, events); err == nil {
		t.Fatal("torn lifecycle checkpoint accepted")
	}
	if _, err = store.Advance(ctx, reg, events, "LIFECYCLE", ExternalMockCommitAcknowledgement); err == nil {
		t.Fatal("torn checkpoint silently overwritten")
	}
	if _, err = os.Stat(torn); err != nil {
		t.Fatalf("torn slot erased: %v", err)
	}
}

func TestMockMultiLedgerCommitMissingMiddleSlotRejected(t *testing.T) {
	ctx := context.Background()
	reg, store, events := mockCommitFixture(t)
	for _, step := range mockCommitKinds {
		if _, err := store.Advance(ctx, reg, events, step, ExternalMockCommitAcknowledgement); err != nil {
			t.Fatal(err)
		}
	}
	state, err := InspectExternalGlobalAtomicConsistency(ctx, reg, events)
	if err != nil {
		t.Fatal(err)
	}
	batch := filepath.Join(store.dir, "batch-"+state.BatchKey)
	if err = os.Remove(filepath.Join(batch, "ownership.json")); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Inspect(ctx, reg, events); err == nil {
		t.Fatal("missing middle checkpoint accepted")
	}
}

func TestMockMultiLedgerCommitConcurrentAndSeparation(t *testing.T) {
	ctx := context.Background()
	reg, store, events := mockCommitFixture(t)
	for _, root := range []string{reg.root, events[0].DataDir} {
		invalid, err := NewExternalMockFinancialCommitStore(root)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = invalid.Inspect(ctx, reg, events); err == nil {
			t.Fatal("overlapping writable root accepted")
		}
	}
	const n = 6
	var wg sync.WaitGroup
	wg.Add(n)
	results := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			_, err := store.Advance(ctx, reg, events, "INTENT", ExternalMockCommitAcknowledgement)
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	ok := 0
	for err := range results {
		if err == nil {
			ok++
		} else if !strings.Contains(err.Error(), "concurrent") && !strings.Contains(err.Error(), "checkpoint") {
			t.Logf("concurrent refused: %v", err)
		}
	}
	if ok < 1 {
		t.Fatal("none of concurrent writes completed")
	}
	got, err := store.Inspect(ctx, reg, events)
	if err != nil || got.CompletedSteps != 1 || got.SimulatedViewsVisible {
		t.Fatalf("concurrent scan: %+v %v", got, err)
	}
}

func TestMockMultiLedgerCommitRejectsRehashedForeignState(t *testing.T) {
	ctx := context.Background()
	reg, store, events := mockCommitFixture(t)
	if _, err := store.Advance(ctx, reg, events, "INTENT", ExternalMockCommitAcknowledgement); err != nil {
		t.Fatal(err)
	}
	events[0].ClientOrderID = "not-the-original-order"
	if _, err := store.Inspect(ctx, reg, events); err == nil {
		t.Fatal("foreign event accepted against existing reservation")
	}
}
