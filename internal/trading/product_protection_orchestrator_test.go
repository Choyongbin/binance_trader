package trading

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestProductProtectionStepPolicyNeverAuthorizesExchangeWrites(t *testing.T) {
	cases := []struct {
		state        string
		cancel, sent bool
		want         ProductProtectionStepKind
		recoveryOnly bool
		reject       bool
	}{
		{state: EntryLifecyclePreparedOnly, want: ProductStepReviewArm},
		{state: EntryLifecycleUnknownSubmission, want: ProductStepRecoverPost, recoveryOnly: true},
		{state: EntryLifecyclePairActive, want: ProductStepMonitorPair},
		{state: EntryLifecycleCancelReview, want: ProductStepReviewCancel},
		{state: EntryLifecycleCancelAmbiguous, cancel: true, sent: true, want: ProductStepRecoverDelete, recoveryOnly: true},
		{state: EntryLifecycleClosedNotSettled, want: ProductStepReviewSettlement},
		{state: EntryLifecycleClosedNotSettled, cancel: true, sent: true, want: ProductStepRecoverDelete, recoveryOnly: true},
		{state: EntryLifecycleSingleLeg, reject: true},
		{state: EntryLifecyclePartialHalt, reject: true},
		{state: EntryLifecycleDoubleTriggerHalt, reject: true},
		{state: "FUTURE_UNKNOWN", reject: true},
		{state: EntryLifecycleCancelAmbiguous, cancel: false, sent: true, reject: true},
		{state: EntryLifecyclePairActive, cancel: true, reject: true},
	}
	for _, tc := range cases {
		t.Run(tc.state+string(tc.want), func(t *testing.T) {
			view := EntryProtectionLifecycleReview{ClientOrderID: "test_entry_abc", GroupID: "entry_test_entry_abc",
				State: tc.state, CancelIntentRecorded: tc.cancel, CancelMayHaveBeenSent: tc.sent,
				ManualReviewRequired: true}
			got, err := classifyProductProtectionStep(view)
			if tc.reject {
				if err == nil || got.NextStep != ProductStepHalt {
					t.Fatalf("unsafe state escaped: %+v %v", got, err)
				}
			} else if err != nil || got.NextStep != tc.want || got.SignedRecoveryOnly != tc.recoveryOnly {
				t.Fatalf("unexpected step: %+v %v", got, err)
			}
			if !got.ManualReviewRequired || got.TradingAuthorized || got.SubmitAuthorized ||
				got.CancelAuthorized || got.SettlementAuthorized {
				t.Fatalf("product step authorized live trade: %+v", got)
			}
		})
	}
	if got, err := classifyProductProtectionStep(EntryProtectionLifecycleReview{
		ClientOrderID: "test_entry_abc", GroupID: "entry_test_entry_abc",
		State: EntryLifecyclePairActive, ManualReviewRequired: false,
	}); err == nil || got.NextStep != ProductStepHalt {
		t.Fatalf("fake claim treated as approved: %+v %v", got, err)
	}
}

func TestProductOrchestratorReadsSignedOnceAndKeepsWritesDisabled(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			f := newEntryLifecycleFixture(t, side)
			product := productInspectorFixture(t, f)
			control, err := OpenAutoRunControl(f.root)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := control.Start(); err != nil {
				t.Fatal(err)
			}
			if err := control.Stop(); err != nil {
				t.Fatal(err)
			}
			beforeGets := f.signed.getCalls
			step, err := product.ReconcileEntryOnce(context.Background(), f.reservation)
			if err != nil || step.NextStep != ProductStepReviewArm || f.signed.getCalls <= beforeGets {
				t.Fatalf("no post-stop signed orchestration: %+v %v", step, err)
			}
			if f.signed.submitCalls != 0 || f.signed.cancelCalls != 0 {
				t.Fatal("signed orchestrator transmitted a live order")
			}
			f.armPair(t)
			product = productInspectorFixture(t, f)
			step, err = product.ReconcileEntryOnce(context.Background(), f.reservation)
			if err != nil || step.NextStep != ProductStepMonitorPair {
				t.Fatalf("restart failed to inspect active legs: %+v %v", step, err)
			}
			inventory, err := product.InspectOutstandingProtections(context.Background())
			if err != nil || len(inventory.Steps) != 1 || inventory.Steps[0].NextStep != ProductStepMonitorPair {
				t.Fatalf("inventory next step mismatch: %+v %v", inventory, err)
			}
			_, armErr := product.Arm(context.Background(), f.plan.GroupID, OwnerPosition{})
			_, cancelErr := product.CancelTriggeredSibling(context.Background(), f.plan.GroupID, ProtectionEvidence{})
			if !errors.Is(armErr, ErrProtectionApproval) || !errors.Is(cancelErr, ErrProtectionApproval) {
				t.Fatalf("actual exchange mutations unexpectedly enabled: %v %v", armErr, cancelErr)
			}
		})
	}
}

func TestProductOrchestrationAckLossFailureAndNoReplay(t *testing.T) {
	f := newEntryLifecycleFixture(t, Long)
	product := productInspectorFixture(t, f)
	if err := f.protection.MarkMayHaveSent(f.plan.GroupID, "tp", true); err != nil {
		t.Fatal(err)
	}
	step, err := product.ReconcileEntryOnce(context.Background(), f.reservation)
	if err != nil || step.NextStep != ProductStepRecoverPost || !step.SignedRecoveryOnly {
		t.Fatalf("ACK loss was replayable: %+v %v", step, err)
	}
	inv, err := product.InspectOutstandingProtections(context.Background())
	if err == nil || len(inv.Steps) != 1 || inv.Steps[0].NextStep != ProductStepRecoverPost ||
		inv.State != PendingProtectionInspectionBlocked {
		t.Fatalf("ACK loss inventory was accepted: %+v %v", inv, err)
	}
	f.signed.failGET = true
	step, err = product.ReconcileEntryOnce(context.Background(), f.reservation)
	if err == nil || step.NextStep != ProductStepHalt {
		t.Fatalf("signed outage did not halt: %+v %v", step, err)
	}
	if f.signed.submitCalls != 0 || f.signed.cancelCalls != 0 {
		t.Fatal("recovery sent an exchange mutation")
	}
}

func TestProductWatchRejectsForgedNextStepAndDeepCopies(t *testing.T) {
	sourceBlockers := []string{"original"}
	fake := func(context.Context) (PendingProtectionInventory, error) {
		return PendingProtectionInventory{State: PendingProtectionInspectionObserved,
			ManualReviewRequired: true, InspectedEntries: 1,
			Entries: []EntryProtectionLifecycleReview{{ClientOrderID: "auto_abc", GroupID: "entry_auto_abc", State: EntryLifecyclePairActive,
				Checkpoint: ProtectionArmingCheckpoint{Blockers: sourceBlockers}, ManualReviewRequired: true}},
			Steps: []ProductProtectionStep{{ClientOrderID: "auto_abc", GroupID: "entry_auto_abc", LifecycleState: EntryLifecyclePairActive,
				NextStep: ProductStepMonitorPair, ManualReviewRequired: true, SubmitAuthorized: true}},
		}, nil
	}
	watch, err := NewProductProtectionPoller(fake, time.Hour, 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := watch.Start(); err != nil {
		t.Fatal(err)
	}
	defer watch.Close()
	status := waitProtectionWatch(t, watch, func(s ProductProtectionWatchStatus) bool { return s.Checks > 0 })
	if status.State != ProtectionWatchBlocked || status.Inventory.Steps[0].SubmitAuthorized {
		t.Fatalf("forged mutation capability survived watcher: %+v", status)
	}
	status.Inventory.Steps[0].NextStep = "FORGED_FROM_CALLER"
	status.Inventory.Entries[0].Checkpoint.Blockers[0] = "FORGED_FROM_CALLER"
	if watch.Status().Inventory.Steps[0].NextStep != ProductStepMonitorPair {
		t.Fatal("watcher returned shared mutable product step slice")
	}
	if watch.Status().Inventory.Entries[0].Checkpoint.Blockers[0] != "original" {
		t.Fatal("watcher returned shared nested checkpoint blockers")
	}
	sourceBlockers[0] = "MUTATED_SOURCE"
	if watch.Status().Inventory.Entries[0].Checkpoint.Blockers[0] != "original" {
		t.Fatal("watcher retained inspector-owned nested checkpoint blockers")
	}
}

func TestProductWatchCheckingWithNoCompletedSignedReadIsBlocked(t *testing.T) {
	watch, err := NewProductProtectionPoller(func(ctx context.Context) (PendingProtectionInventory, error) {
		<-ctx.Done()
		return PendingProtectionInventory{}, ctx.Err()
	}, 20*time.Millisecond, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := watch.Start(); err != nil {
		t.Fatal(err)
	}
	defer watch.Close()
	first := watch.Status()
	if first.State == ProtectionWatchObserved || first.TradingAuthorized || first.ContinuousProtection {
		t.Fatalf("initial checking state certified protection: %+v", first)
	}
	blocked := waitProtectionWatch(t, watch, func(s ProductProtectionWatchStatus) bool { return s.Checks > 0 })
	if blocked.State != ProtectionWatchBlocked {
		t.Fatalf("signed read timed out but remained healthy: %+v", blocked)
	}
}
