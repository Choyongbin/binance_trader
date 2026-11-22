package trading

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func waitProtectionWatch(t *testing.T, watch *ProductProtectionPoller, pred func(ProductProtectionWatchStatus) bool) ProductProtectionWatchStatus {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s := watch.Status()
		if pred(s) {
			return s
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("protection watch deadline, last=%+v", watch.Status())
	return ProductProtectionWatchStatus{}
}

func TestProductProtectionPollerStopLeaseIndependentAndFailClosed(t *testing.T) {
	// This independent process-lifetime monitor is not tied to the START/STOP
	// lease; even when STOP is active it keeps inspecting, but cannot trade.
	var calls atomic.Int32
	var fail atomic.Bool
	w, err := NewProductProtectionPoller(func(ctx context.Context) (PendingProtectionInventory, error) {
		calls.Add(1)
		if fail.Load() {
			return PendingProtectionInventory{}, errors.New("fake signed GET unavailable")
		}
		return PendingProtectionInventory{State: PendingProtectionInspectionObserved,
			ManualReviewRequired: true, InspectedEntries: 1, Entries: []EntryProtectionLifecycleReview{{State: EntryLifecyclePairActive, ManualReviewRequired: true}}}, nil
	}, 15*time.Millisecond, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	initial := w.Status()
	if initial.State != ProtectionWatchNotStarted || initial.TradingAuthorized || initial.ContinuousProtection {
		t.Fatalf("unexpected initial state %+v", initial)
	}
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	if err := w.Start(); err != nil {
		t.Fatal("start must be idempotent: ", err)
	}
	observed := waitProtectionWatch(t, w, func(s ProductProtectionWatchStatus) bool {
		return s.Checks >= 1 && s.State == ProtectionWatchObserved
	})
	if !observed.Running || !observed.ManualReviewRequired || observed.TradingAuthorized ||
		observed.SubmitAuthorized || observed.CancelAuthorized || observed.SettlementAuthorized || observed.ContinuousProtection {
		t.Fatalf("observation improperly authorized trading: %+v", observed)
	}
	control, err := OpenAutoRunControl(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := control.Start(); err != nil {
		t.Fatal(err)
	}
	if err := control.Stop(); err != nil {
		t.Fatal(err)
	}
	// Revoking the actual durable entry lease must not close the independent
	// observer, although the observer's results never authorize new entries.
	if !w.Status().Running {
		t.Fatal("STOP unexpectedly stopped protection observer")
	}
	// Simulate revocation of the trading lease: this service does not depend
	// on it, and the next GET failure removes the last observed state.
	fail.Store(true)
	blocked := waitProtectionWatch(t, w, func(s ProductProtectionWatchStatus) bool {
		return s.Checks >= 2 && s.State == ProtectionWatchBlocked
	})
	if blocked.LastErrorCategory != "SIGNED_INSPECTION_FAILED" || blocked.Inventory.State != PendingProtectionInspectionBlocked {
		t.Fatalf("signed failure not fail-closed: %+v", blocked)
	}
	fail.Store(false)
	waitProtectionWatch(t, w, func(s ProductProtectionWatchStatus) bool {
		return s.Checks >= 3 && s.State == ProtectionWatchObserved
	})
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	stopped := w.Status()
	if stopped.Running || stopped.State != ProtectionWatchStopped || calls.Load() < 3 {
		t.Fatalf("observer did not stop: %+v", stopped)
	}
	if err := w.Start(); err == nil {
		t.Fatal("closed watcher restarted")
	}
}

func TestProductProtectionPollerNoOverlappingReadsAndCancellation(t *testing.T) {
	started := make(chan struct{})
	var active atomic.Int32
	var maximum atomic.Int32
	w, err := NewProductProtectionPoller(func(ctx context.Context) (PendingProtectionInventory, error) {
		if a := active.Add(1); a > maximum.Load() {
			maximum.Store(a)
		}
		select {
		case <-started:
		default:
			close(started)
		}
		defer active.Add(-1)
		<-ctx.Done()
		return PendingProtectionInventory{}, ctx.Err()
	}, 15*time.Millisecond, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("observer did not start")
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if maximum.Load() != 1 || active.Load() != 0 {
		t.Fatalf("overlapping or orphaned reads: max=%d active=%d", maximum.Load(), active.Load())
	}
}

func TestProductProtectionPollerRejectsInventedPermissions(t *testing.T) {
	w, err := NewProductProtectionPoller(func(ctx context.Context) (PendingProtectionInventory, error) {
		return PendingProtectionInventory{State: PendingProtectionInspectionObserved, TradingAuthorized: true,
			ManualReviewRequired: false, InspectedEntries: 1}, nil
	}, 50*time.Millisecond, 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	blocked := waitProtectionWatch(t, w, func(s ProductProtectionWatchStatus) bool { return s.Checks >= 1 })
	if blocked.State != ProtectionWatchBlocked || blocked.TradingAuthorized || blocked.Inventory.TradingAuthorized || blocked.LastErrorCategory != "UNSAFE_OR_UNRECOGNIZED_INVENTORY" {
		t.Fatalf("fabricated authorization incorrectly accepted: %+v", blocked)
	}
}

func TestProductProtectionPollerRejectsNestedApprovalClaims(t *testing.T) {
	w, err := NewProductProtectionPoller(func(context.Context) (PendingProtectionInventory, error) {
		return PendingProtectionInventory{State: PendingProtectionInspectionObserved, ManualReviewRequired: true,
			InspectedEntries: 1,
			Entries: []EntryProtectionLifecycleReview{{ClientOrderID: "auto_abc", GroupID: "entry_auto_abc",
				State: EntryLifecyclePairActive, ManualReviewRequired: true,
				Checkpoint: ProtectionArmingCheckpoint{ProtectionSubmitAuthorized: true,
					TP: ProtectionLegState{LiveAuthorized: true}}}},
			Steps: []ProductProtectionStep{{ClientOrderID: "auto_abc", GroupID: "entry_auto_abc",
				LifecycleState: EntryLifecyclePairActive, NextStep: ProductStepMonitorPair,
				ManualReviewRequired: true}},
		}, nil
	}, time.Hour, 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	got := waitProtectionWatch(t, w, func(s ProductProtectionWatchStatus) bool { return s.Checks > 0 })
	if got.State != ProtectionWatchBlocked || got.LastErrorCategory != "UNSAFE_OR_UNRECOGNIZED_INVENTORY" ||
		len(got.Inventory.Entries) != 1 || got.Inventory.Entries[0].Checkpoint.ProtectionSubmitAuthorized ||
		got.Inventory.Entries[0].Checkpoint.TP.LiveAuthorized {
		t.Fatalf("nested approval escaped fail-closed watcher: %+v", got)
	}
}

func TestProductProtectionPollerRejectsNestedSettlementClaim(t *testing.T) {
	w, err := NewProductProtectionPoller(func(context.Context) (PendingProtectionInventory, error) {
		return PendingProtectionInventory{State: PendingProtectionInspectionObserved, ManualReviewRequired: true,
			InspectedEntries: 1,
			Entries: []EntryProtectionLifecycleReview{{State: EntryLifecyclePairActive, ManualReviewRequired: true,
				ExitFills: SignedExitFillReview{FinanciallySettled: true}}},
		}, nil
	}, time.Hour, 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	got := waitProtectionWatch(t, w, func(s ProductProtectionWatchStatus) bool { return s.Checks > 0 })
	if got.State != ProtectionWatchBlocked || got.LastErrorCategory != "UNSAFE_OR_UNRECOGNIZED_INVENTORY" ||
		len(got.Inventory.Entries) != 1 || got.Inventory.Entries[0].ExitFills.FinanciallySettled ||
		got.Inventory.Entries[0].ExitFills.TradingAuthorized || !got.Inventory.Entries[0].ExitFills.ManualReviewRequired {
		t.Fatalf("nested settlement claim escaped fail-closed watcher: %+v", got)
	}
}

func TestProductProtectionPollerValidationAndEmpty(t *testing.T) {
	if _, err := NewProductProtectionPoller(nil, time.Second, time.Millisecond); err == nil {
		t.Fatal("nil inspector accepted")
	}
	if _, err := NewProductProtectionPoller(func(context.Context) (PendingProtectionInventory, error) {
		return PendingProtectionInventory{}, nil
	}, time.Millisecond, time.Second); err == nil {
		t.Fatal("invalid timeout accepted")
	}
	w, err := NewProductProtectionPoller(func(context.Context) (PendingProtectionInventory, error) {
		return PendingProtectionInventory{State: PendingProtectionInspectionEmpty, ManualReviewRequired: true}, nil
	}, 50*time.Millisecond, 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	got := waitProtectionWatch(t, w, func(s ProductProtectionWatchStatus) bool { return s.Checks >= 1 })
	if got.State != ProtectionWatchEmpty || got.ContinuousProtection {
		t.Fatalf("empty local inventory must not prove account flat: %+v", got)
	}
}

func TestInspectDemoOutstandingProtectionsOnceRejectsInvalidOrCanceledWithoutWrites(t *testing.T) {
	out, err := InspectDemoOutstandingProtectionsOnce(context.Background(), "", nil, nil)
	if err == nil || out.State != PendingProtectionInspectionBlocked {
		t.Fatalf("missing dependencies: %+v %v", out, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	out, err = InspectDemoOutstandingProtectionsOnce(ctx, t.TempDir(), nil, nil)
	if !errors.Is(err, context.Canceled) && err == nil {
		t.Fatalf("canceled scan: %+v %v", out, err)
	}
}

func TestProductProtectionPollerPreservesBlockedIncidentWithoutApproval(t *testing.T) {
	w, err := NewProductProtectionPoller(func(context.Context) (PendingProtectionInventory, error) {
		return PendingProtectionInventory{State: PendingProtectionInspectionBlocked, ManualReviewRequired: true,
				Blockers: []string{"TP_SL_DOUBLE_TRIGGER"}, InspectedEntries: 1,
				Incidents: []ProtectionIncident{{ClientOrderID: "entry1", Code: "TP_SL_DOUBLE_TRIGGER", ManualReviewRequired: true}},
				Entries:   []EntryProtectionLifecycleReview{{State: EntryLifecycleDoubleTriggerHalt, ManualReviewRequired: true}}},
			errors.New("account reconciliation needed")
	}, 50*time.Millisecond, 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	status := waitProtectionWatch(t, w, func(s ProductProtectionWatchStatus) bool { return s.Checks >= 1 })
	if status.State != ProtectionWatchBlocked || status.LastErrorCategory != "PROTECTION_INCIDENT_REQUIRES_RECONCILIATION" ||
		len(status.Inventory.Incidents) != 1 || status.Inventory.Incidents[0].Code != "TP_SL_DOUBLE_TRIGGER" ||
		status.TradingAuthorized || status.SubmitAuthorized || status.CancelAuthorized || status.SettlementAuthorized {
		t.Fatalf("incident was cleared or authorized: %+v", status)
	}
	// The API must expose a deep-copy of the incident slice, not live mutable state.
	status.Inventory.Incidents[0].Code = "FORGED"
	if w.Status().Inventory.Incidents[0].Code != "TP_SL_DOUBLE_TRIGGER" {
		t.Fatal("caller mutated shared watcher incident state")
	}
}

func TestProductProtectionPollerRejectsIncidentClaimedAsObserved(t *testing.T) {
	w, err := NewProductProtectionPoller(func(context.Context) (PendingProtectionInventory, error) {
		return PendingProtectionInventory{State: PendingProtectionInspectionObserved, ManualReviewRequired: true,
			InspectedEntries: 1, Entries: []EntryProtectionLifecycleReview{{State: EntryLifecyclePairActive, ManualReviewRequired: true}},
			Incidents: []ProtectionIncident{{Code: "SINGLE_PROTECTION_LEG", ManualReviewRequired: true}}}, nil
	}, 50*time.Millisecond, 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	status := waitProtectionWatch(t, w, func(s ProductProtectionWatchStatus) bool { return s.Checks >= 1 })
	if status.State != ProtectionWatchBlocked || status.TradingAuthorized || status.ContinuousProtection {
		t.Fatalf("unsafe observed inventory was trusted: %+v", status)
	}
}
