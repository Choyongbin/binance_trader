package trading

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

func fencedOfflineTick(f *entryLifecycleFixture, m *entryOfflineExchange, fences *OfflineSupervisorFenceStore, e *ProtectionEvidence) (OfflineProtectionSupervisorTick, error) {
	return RehearseFencedProtectionSupervisorTick(context.Background(), f.reservation,
		f.coordinator, f.lifecycle, f.protection, f.cancels, fences, m,
		DemoCredentials{APIKey: "demo", Secret: "secret"}, e)
}

func TestFencedOfflineSupervisorNormalArmAndCancelAcrossRestart(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			f, m := offlineEntryFixture(t, side)
			fences, err := OpenOfflineSupervisorFenceStore(f.root)
			if err != nil {
				t.Fatal(err)
			}
			first, err := fencedOfflineTick(f, m, fences, nil)
			if err != nil || first.State != OfflineSupervisorSimulated || m.armingTransportAdapter.m.submitCalls != 2 {
				t.Fatalf("initial arm: %+v %v", first, err)
			}
			assertOfflineSupervisorLocked(t, first)
			fences, err = OpenOfflineSupervisorFenceStore(f.root)
			if err != nil {
				t.Fatal(err)
			}
			second, err := fencedOfflineTick(f, m, fences, nil)
			if err != nil || second.ChosenAction != EntryRehearsalInspect || m.armingTransportAdapter.m.submitCalls != 2 {
				t.Fatalf("restarted read-only: %+v %v", second, err)
			}
			evidence := offlineTriggerAndEvidence(t, f, m)
			canceled, err := fencedOfflineTick(f, m, fences, &evidence)
			if err != nil || canceled.State != OfflineSupervisorSimulated || m.deletes != 1 {
				t.Fatalf("single sibling cancel: %+v %v", canceled, err)
			}
			assertOfflineSupervisorLocked(t, canceled)
			fences, err = OpenOfflineSupervisorFenceStore(f.root)
			if err != nil {
				t.Fatal(err)
			}
			recovered, err := fencedOfflineTick(f, m, fences, nil)
			if err != nil || recovered.State != OfflineSupervisorSignedRecover || m.deletes != 1 {
				t.Fatalf("duplicate cancel after restart: %+v %v", recovered, err)
			}
			assertOfflineSupervisorLocked(t, recovered)
		})
	}
}

func TestFencedOfflineSupervisorCrashBeforeArmDoesNotPostAfterRestart(t *testing.T) {
	f, m := offlineEntryFixture(t, Long)
	fences, err := OpenOfflineSupervisorFenceStore(f.root)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate fsynced fence followed by process termination before any POST.
	fresh, err := fences.ReserveOnce(f.plan, f.reservation, EntryRehearsalArm, EntryLifecyclePreparedOnly)
	if err != nil || !fresh {
		t.Fatalf("pre-dispatch fence: fresh=%v err=%v", fresh, err)
	}
	fences, err = OpenOfflineSupervisorFenceStore(f.root)
	if err != nil {
		t.Fatal(err)
	}
	got, err := fencedOfflineTick(f, m, fences, nil)
	if err != nil || got.State != OfflineSupervisorSignedRecover || got.ChosenAction != EntryRehearsalInspect || got.Before.State != EntryLifecyclePreparedOnly || m.armingTransportAdapter.m.submitCalls != 0 || m.deletes != 0 {
		t.Fatalf("crash before POST caused replay: %+v %v", got, err)
	}
	assertOfflineSupervisorLocked(t, got)
}

func TestFencedOfflineSupervisorCrashBeforeCancelDoesNotDeleteAfterRestart(t *testing.T) {
	f, m := offlineEntryFixture(t, Short)
	fences, err := OpenOfflineSupervisorFenceStore(f.root)
	if err != nil {
		t.Fatal(err)
	}
	if first, err := fencedOfflineTick(f, m, fences, nil); err != nil || first.Dispatch.State != EntryLifecyclePairActive {
		t.Fatalf("arm: %+v %v", first, err)
	}
	evidence := offlineTriggerAndEvidence(t, f, m)
	fresh, err := fences.ReserveOnce(f.plan, f.reservation, EntryRehearsalCancel, EntryLifecycleCancelReview)
	if err != nil || !fresh {
		t.Fatalf("cancel fence pre-dispatch: fresh=%v err=%v", fresh, err)
	}
	fences, err = OpenOfflineSupervisorFenceStore(f.root)
	if err != nil {
		t.Fatal(err)
	}
	got, err := fencedOfflineTick(f, m, fences, &evidence)
	if err != nil || got.State != OfflineSupervisorSignedRecover || got.ChosenAction != EntryRehearsalInspect || m.deletes != 0 {
		t.Fatalf("crash before cancel replayed DELETE: %+v %v", got, err)
	}
	assertOfflineSupervisorLocked(t, got)
}

func TestFencedOfflineSupervisorCorruptMarkerAndWrongEntryHalt(t *testing.T) {
	f, m := offlineEntryFixture(t, Long)
	fences, err := OpenOfflineSupervisorFenceStore(f.root)
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := fences.ReserveOnce(f.plan, f.reservation, EntryRehearsalArm, EntryLifecyclePreparedOnly)
	if err != nil || !fresh {
		t.Fatal(err)
	}
	path, err := fences.file(f.plan.GroupID, EntryRehearsalArm)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{"schema":1,"group_id":"forged"}`), 0600); err != nil {
		t.Fatal(err)
	}
	got, err := fencedOfflineTick(f, m, fences, nil)
	if err == nil || got.State != OfflineSupervisorHalted || m.armingTransportAdapter.m.submitCalls != 0 || m.deletes != 0 {
		t.Fatalf("corrupted checkpoint permitted mutation: %+v %v", got, err)
	}
	assertOfflineSupervisorLocked(t, got)
}

func TestFencedOfflineSupervisorInvalidCancelEvidenceDoesNotReserve(t *testing.T) {
	f, m := offlineEntryFixture(t, Long)
	fences, err := OpenOfflineSupervisorFenceStore(f.root)
	if err != nil {
		t.Fatal(err)
	}
	if first, err := fencedOfflineTick(f, m, fences, nil); err != nil || first.Dispatch.State != EntryLifecyclePairActive {
		t.Fatalf("arm: %+v %v", first, err)
	}
	evidence := offlineTriggerAndEvidence(t, f, m)
	evidence.Owner.ModelID = "wrong"
	got, err := fencedOfflineTick(f, m, fences, &evidence)
	if err == nil || got.State != OfflineSupervisorHalted || m.deletes != 0 {
		t.Fatalf("forged evidence permitted cancel: %+v %v", got, err)
	}
	path, err := fences.file(f.plan.GroupID, EntryRehearsalCancel)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("untrusted evidence reserved cancel fence: %v", err)
	}
}

func TestFencedOfflineSupervisorSignedGETFailureDoesNotReserve(t *testing.T) {
	f, m := offlineEntryFixture(t, Long)
	fences, err := OpenOfflineSupervisorFenceStore(f.root)
	if err != nil {
		t.Fatal(err)
	}
	m.failGET = true
	got, err := fencedOfflineTick(f, m, fences, nil)
	if err == nil || got.State != OfflineSupervisorHalted || m.armingTransportAdapter.m.submitCalls != 0 {
		t.Fatalf("signed GET failure: %+v %v", got, err)
	}
	path, err := fences.file(f.plan.GroupID, EntryRehearsalArm)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("GET failure reserved checkpoint: %v", err)
	}
}

// A type with only the normal exchange methods cannot satisfy the private
// local-only marker, so the wrapper cannot accept *exchange.Client for POST.
var _ interface {
	FetchDemoExecutionPreflight(context.Context, string, string, string) (exchange.ExecutionPreflightSnapshot, error)
} = (*entryOfflineExchange)(nil)

func TestFencedOfflineSupervisorConcurrentDecisionReservationsOnlyOnce(t *testing.T) {
	f, m := offlineEntryFixture(t, Long)
	first, err := OpenOfflineSupervisorFenceStore(f.root)
	if err != nil {
		t.Fatal(err)
	}
	second, err := OpenOfflineSupervisorFenceStore(f.root)
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan bool, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, s := range []*OfflineSupervisorFenceStore{first, second} {
		wg.Add(1)
		go func(s *OfflineSupervisorFenceStore) {
			defer wg.Done()
			fresh, err := s.ReserveOnce(f.plan, f.reservation, EntryRehearsalArm, EntryLifecyclePreparedOnly)
			results <- fresh
			errs <- err
		}(s)
	}
	wg.Wait()
	close(results)
	close(errs)
	freshCount := 0
	for yes := range results {
		if yes {
			freshCount++
		}
	}
	// O_EXCL creates the file before writing and fsync. A concurrent process
	// can conservatively reject an INCOMPLETE marker instead of waiting for
	// the first writer. That is fail-closed, not a second mutation ticket.
	for err := range errs {
		if err != nil && !strings.Contains(err.Error(), "prior fence corrupted") {
			t.Fatal(err)
		}
	}
	if freshCount != 1 || m.armingTransportAdapter.m.submitCalls != 0 {
		t.Fatalf("expected exactly one durable decision, got %d", freshCount)
	}
	// Once the first writer has fsynced, a sequential restart can read the
	// completed record, and still cannot reserve a second mutation.
	fresh, err := second.ReserveOnce(f.plan, f.reservation, EntryRehearsalArm, EntryLifecyclePreparedOnly)
	if err != nil || fresh {
		t.Fatalf("checkpoint did not stabilize after concurrent writer: fresh=%v err=%v", fresh, err)
	}
}

func TestFencedOfflineSupervisorCrossRootFailClosed(t *testing.T) {
	f, m := offlineEntryFixture(t, Long)
	wrong, err := OpenOfflineSupervisorFenceStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	got, err := fencedOfflineTick(f, m, wrong, nil)
	if err == nil || got.State != OfflineSupervisorHalted || m.armingTransportAdapter.m.submitCalls != 0 || m.deletes != 0 {
		t.Fatalf("cross-root fence accepted: %+v %v", got, err)
	}
}

func TestFencedOfflineSupervisorPreexistingConflictRejectsNewPlan(t *testing.T) {
	f, m := offlineEntryFixture(t, Long)
	fences, err := OpenOfflineSupervisorFenceStore(f.root)
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := fences.ReserveOnce(f.plan, f.reservation, EntryRehearsalArm, EntryLifecyclePreparedOnly)
	if err != nil || !fresh {
		t.Fatal(err)
	}
	// The same group cannot authorize a new strategy/price by overwriting the
	// checksum-sealed original. The prior checkpoint continues to block POST.
	altered := f.plan
	altered.CreatedAt = time.Now().UTC().Add(time.Second).Format(time.RFC3339Nano)
	altered.Digest = altered.digest()
	fresh, err = fences.ReserveOnce(altered, f.reservation, EntryRehearsalArm, EntryLifecyclePreparedOnly)
	if err == nil || fresh {
		t.Fatalf("modified immutable plan accepted: fresh=%v err=%v", fresh, err)
	}
	got, err := fencedOfflineTick(f, m, fences, nil)
	if err != nil || got.State != OfflineSupervisorSignedRecover || m.armingTransportAdapter.m.submitCalls != 0 {
		t.Fatalf("conflict allowed mutation: %+v %v", got, err)
	}
}
