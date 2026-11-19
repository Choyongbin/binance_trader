package trading

// Offline-only STOP + restart incident tests. STOP forbids new model entries,
// but must not force-close owned positions or erase protective incident ledgers.
// Explicit signed-only recovery and *test-simulator-only* operator cancel
// remain separately reviewable after STOP; no live POST/DELETE is enabled.

import (
	"context"
	"errors"
	"testing"

	"binance_trader/internal/exchange"
)

func TestEntryOfflineStoppedRunKeepsExistingPairAndRequiresExplicitSiblingReview(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			f := newEndToEndOfflineFixture(t, side, "FILLED", "0.01")
			control, err := OpenAutoRunControl(f.root)
			if err != nil {
				t.Fatal(err)
			}
			lease, err := control.Start()
			if err != nil {
				t.Fatal(err)
			}
			armed, err := f.run(EntryRehearsalArm, nil)
			if err != nil || armed.State != EntryLifecyclePairActive {
				t.Fatalf("arm: %+v %v", armed, err)
			}
			f.plan = armed.Preparation.Plan
			if err := control.Stop(); err != nil {
				t.Fatal(err)
			}
			if err := control.Guard(lease)(context.Background()); !errors.Is(err, ErrAutoRunStopped) {
				t.Fatalf("STOP must invalidate model entry run: %v", err)
			}
			originalPosts := f.armingTransportAdapter.m.submitCalls
			f.reopen(t)
			inspected, err := f.run(EntryRehearsalInspect, nil)
			if err != nil || inspected.State != EntryLifecyclePairActive || f.deletes != 0 ||
				f.armingTransportAdapter.m.submitCalls != originalPosts {
				t.Fatalf("STOP force-closed pair or retried POST: %+v %v", inspected, err)
			}
			assertEndToEndOfflineLocked(t, inspected)
			if _, err := f.run(EntryRehearsalArm, nil); err == nil || f.armingTransportAdapter.m.submitCalls != originalPosts {
				t.Fatal("STOP/restart allowed an unreviewed re-POST")
			}
			evidence := offlineTriggerAndEvidence(t, f.entryLifecycleFixture, f.entryOfflineExchange)
			inspected, err = f.run(EntryRehearsalInspect, nil)
			if err != nil || inspected.State != EntryLifecycleCancelReview || f.deletes != 0 {
				t.Fatalf("triggered exit forced auto cancellation: %+v %v", inspected, err)
			}
			// STOP does not suppress a separately explicit, supervised
			// test-only sibling cancel for an already exited position.
			canceled, err := f.run(EntryRehearsalCancel, &evidence)
			if err != nil || canceled.State != EntryLifecycleClosedNotSettled || f.deletes != 1 {
				t.Fatalf("manual supervised cancel after STOP: %+v %v", canceled, err)
			}
			assertEndToEndOfflineLocked(t, canceled)
			if status, err := control.Status(); err != nil || status.State != AutoRunStopped {
				t.Fatalf("STOP accidentally resumed: %+v %v", status, err)
			}
		})
	}
}

func TestEntryOfflineCrashedRunNeverResumesButSignedReviewStillWorks(t *testing.T) {
	f := newEndToEndOfflineFixture(t, Long, "FILLED", "0.01")
	control, err := OpenAutoRunControl(f.root)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := control.Start()
	if err != nil {
		t.Fatal(err)
	}
	armed, err := f.run(EntryRehearsalArm, nil)
	if err != nil || armed.State != EntryLifecyclePairActive {
		t.Fatalf("arm: %+v %v", armed, err)
	}
	crashed, err := OpenAutoRunControl(f.root)
	if err != nil {
		t.Fatal(err)
	}
	if status, err := crashed.Status(); err != nil || status.State != AutoRunRestartBlocked {
		t.Fatalf("crash status: %+v %v", status, err)
	}
	if _, err := crashed.Start(); !errors.Is(err, ErrAutoRunRestartBlocked) {
		t.Fatalf("crash auto restarted: %v", err)
	}
	if err := crashed.Guard(lease)(context.Background()); !errors.Is(err, ErrAutoRunRestartBlocked) {
		t.Fatalf("old lease survived: %v", err)
	}
	f.reopen(t)
	inspected, err := f.run(EntryRehearsalInspect, nil)
	if err != nil || inspected.State != EntryLifecyclePairActive || f.armingTransportAdapter.m.submitCalls != 2 || f.deletes != 0 {
		t.Fatalf("crash review modified pair: %+v %v", inspected, err)
	}
	assertEndToEndOfflineLocked(t, inspected)
}

func TestEntryOfflineCancelAppliedButACKLostSignedRestartNoReplay(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			f := newEndToEndOfflineFixture(t, side, "FILLED", "0.01")
			armed, err := f.run(EntryRehearsalArm, nil)
			if err != nil || armed.State != EntryLifecyclePairActive {
				t.Fatalf("arm: %+v %v", armed, err)
			}
			f.plan = armed.Preparation.Plan
			evidence := offlineTriggerAndEvidence(t, f.entryLifecycleFixture, f.entryOfflineExchange)
			f.loseCancelAfterApply = true
			uncertain, err := f.run(EntryRehearsalCancel, &evidence)
			if err == nil || uncertain.State != OfflineEntryLifecycleBlocked || f.deletes != 1 {
				t.Fatalf("ack loss not propagated: %+v %v", uncertain, err)
			}
			f.reopen(t)
			restored, err := f.run(EntryRehearsalCancel, nil)
			if err != nil || restored.State != EntryLifecycleClosedNotSettled || f.deletes != 1 ||
				restored.Dispatch == nil || restored.Dispatch.Cancel.State != string(CancelRecoveryCanceled) {
				t.Fatalf("accepted DELETE ack-loss reissued instead of recovered: %+v %v", restored, err)
			}
			assertEndToEndOfflineLocked(t, restored)
		})
	}
}

func TestEntryOfflineStopPartialAndDoubleTriggerNeverAutoMutate(t *testing.T) {
	f := newEndToEndOfflineFixture(t, Short, "FILLED", "0.01")
	control, err := OpenAutoRunControl(f.root)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := control.Start()
	if err != nil {
		t.Fatal(err)
	}
	armed, err := f.run(EntryRehearsalArm, nil)
	if err != nil || armed.State != EntryLifecyclePairActive {
		t.Fatalf("arm: %+v %v", armed, err)
	}
	f.plan = armed.Preparation.Plan
	if err := control.Stop(); err != nil {
		t.Fatal(err)
	}
	if err := control.Guard(lease)(context.Background()); !errors.Is(err, ErrAutoRunStopped) {
		t.Fatal(err)
	}
	tp := f.armingTransportAdapter.m.algos[901]
	tp.Status, tp.ActualOrderID, tp.ActualQty = "FINISHED", "91234", "0.004"
	f.armingTransportAdapter.m.algos[901] = tp
	f.armingTransportAdapter.m.order = []int64{902}
	f.signed.trades[91234] = []exchange.AccountTrade{protectionTrade(91234, 55671, f.plan.TP.Side, string(f.plan.PositionSide), "0.004", "100500", "2")}
	partial, err := f.run(EntryRehearsalInspect, nil)
	if err != nil || partial.State != EntryLifecyclePartialHalt || f.deletes != 0 {
		t.Fatalf("partial: %+v %v", partial, err)
	}
	if _, err := f.run(EntryRehearsalCancel, nil); err == nil || f.deletes != 0 {
		t.Fatal("partial executed cancel")
	}
	sl := f.armingTransportAdapter.m.algos[902]
	sl.Status, sl.ActualOrderID, sl.ActualQty = "FINISHED", "95612", "0.006"
	f.armingTransportAdapter.m.algos[902] = sl
	f.armingTransportAdapter.m.order = nil
	f.signed.trades[95612] = []exchange.AccountTrade{protectionTrade(95612, 55672, f.plan.SL.Side, string(f.plan.PositionSide), "0.006", "99900", "-2")}
	f.reopen(t)
	both, err := f.run(EntryRehearsalInspect, nil)
	if err != nil || both.State != EntryLifecycleDoubleTriggerHalt || f.deletes != 0 || f.armingTransportAdapter.m.submitCalls != 2 {
		t.Fatalf("double trigger: %+v %v", both, err)
	}
	if _, err := f.run(EntryRehearsalCancel, nil); err == nil || f.deletes != 0 {
		t.Fatal("double trigger issued DELETE")
	}
	assertEndToEndOfflineLocked(t, both)
}

func TestOfflineDirectDispatchInvalidActionAndNilContextReadNothing(t *testing.T) {
	f := newEndToEndOfflineFixture(t, Long, "FILLED", "0.01")
	armed, err := f.run(EntryRehearsalArm, nil)
	if err != nil || armed.State != EntryLifecyclePairActive {
		t.Fatalf("arm: %+v %v", armed, err)
	}
	calls := f.signed.getCalls
	for _, tc := range []struct {
		name   string
		ctx    context.Context
		action EntryProtectionRehearsalAction
	}{
		{"nil context", nil, EntryRehearsalInspect},
		{"invalid action", context.Background(), "FORCE_CANCEL"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := RehearseEntryProtectionDispatch(tc.ctx, tc.action, f.reservation, f.coordinator,
				f.lifecycle, f.protection, f.cancels, f.entryOfflineExchange,
				DemoCredentials{APIKey: "demo", Secret: "secret"}, nil)
			if err == nil || res.State != "HALT" || f.signed.getCalls != calls || f.deletes != 0 || f.armingTransportAdapter.m.submitCalls != 2 {
				t.Fatalf("invalid dispatcher call produced side effect: %+v %v", res, err)
			}
		})
	}
}
