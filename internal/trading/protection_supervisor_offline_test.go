package trading

import (
	"binance_trader/internal/exchange"
	"context"
	"encoding/json"
	"testing"
)

func offlineSupervisorStep(f *entryLifecycleFixture, m *entryOfflineExchange, evidence *ProtectionEvidence) (OfflineProtectionSupervisorTick, error) {
	return RehearseEntryProtectionSupervisorTick(context.Background(), f.reservation, f.coordinator,
		f.lifecycle, f.protection, f.cancels, m,
		DemoCredentials{APIKey: "demo", Secret: "secret"}, evidence)
}

func assertOfflineSupervisorLocked(t *testing.T, step OfflineProtectionSupervisorTick) {
	t.Helper()
	if !step.ManualReviewRequired || step.TradingAuthorized || step.SubmitAuthorized || step.CancelAuthorized || step.SettlementAuthorized {
		t.Fatalf("offline action escaped lock: %+v", step)
	}
}

func TestOfflineSupervisorLongShortArmInspectRestartDoesNotResubmit(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			f, m := offlineEntryFixture(t, side)
			arm, err := offlineSupervisorStep(f, m, nil)
			if err != nil || arm.State != OfflineSupervisorSimulated || arm.ChosenAction != EntryRehearsalArm || arm.Dispatch.State != EntryLifecyclePairActive || m.armingTransportAdapter.m.submitCalls != 2 {
				t.Fatalf("first tick failed: %+v %v", arm, err)
			}
			assertOfflineSupervisorLocked(t, arm)
			f.protection, err = OpenProtectionSubmissionStore(f.root)
			if err != nil {
				t.Fatal(err)
			}
			f.cancels, err = NewCancelIntentStore(f.root)
			if err != nil {
				t.Fatal(err)
			}
			check, err := offlineSupervisorStep(f, m, nil)
			if err != nil || check.State != OfflineSupervisorObserved || check.ChosenAction != EntryRehearsalInspect || m.armingTransportAdapter.m.submitCalls != 2 || m.deletes != 0 {
				t.Fatalf("restart replayed exchange mutation: %+v %v", check, err)
			}
			assertOfflineSupervisorLocked(t, check)
		})
	}
}

func TestOfflineSupervisorAckLossSignedOnlyAndOneLegHalt(t *testing.T) {
	f, m := offlineEntryFixture(t, Long)
	m.loseTPAck = true
	first, err := offlineSupervisorStep(f, m, nil)
	if err == nil || first.State != OfflineSupervisorHalted || m.armingTransportAdapter.m.submitCalls != 1 {
		t.Fatalf("expected fault after first fake POST: %+v %v", first, err)
	}
	assertOfflineSupervisorLocked(t, first)
	f.protection, err = OpenProtectionSubmissionStore(f.root)
	if err != nil {
		t.Fatal(err)
	}
	next, err := offlineSupervisorStep(f, m, nil)
	if err != nil || next.State != OfflineSupervisorSignedRecover || next.ChosenAction != EntryRehearsalArm || next.Dispatch.State != EntryLifecycleSingleLeg || m.armingTransportAdapter.m.submitCalls != 1 {
		t.Fatalf("signed recovery must never resend: %+v %v", next, err)
	}
	blocked, err := offlineSupervisorStep(f, m, nil)
	if err == nil || blocked.State != OfflineSupervisorHalted || m.armingTransportAdapter.m.submitCalls != 1 || m.deletes != 0 {
		t.Fatalf("one leg must stay blocked: %+v %v", blocked, err)
	}
	assertOfflineSupervisorLocked(t, blocked)
}

func TestOfflineSupervisorTriggeredSiblingRequiresIndependentEvidence(t *testing.T) {
	f, m := offlineEntryFixture(t, Short)
	if out, err := offlineSupervisorStep(f, m, nil); err != nil || out.Dispatch.State != EntryLifecyclePairActive {
		t.Fatalf("arm failed: %+v %v", out, err)
	}
	evidence := offlineTriggerAndEvidence(t, f, m)
	inspect, err := offlineSupervisorStep(f, m, nil)
	if err != nil || inspect.ChosenAction != EntryRehearsalInspect || inspect.State != OfflineSupervisorObserved || m.deletes != 0 {
		t.Fatalf("no evidence must not DELETE: %+v %v", inspect, err)
	}
	bad := evidence
	bad.Owner.ModelID = "wrong-owner"
	attempted, err := offlineSupervisorStep(f, m, &bad)
	if err == nil || attempted.State != OfflineSupervisorHalted || m.deletes != 0 {
		t.Fatalf("forged evidence must fail: %+v %v", attempted, err)
	}
	canceled, err := offlineSupervisorStep(f, m, &evidence)
	if err != nil || canceled.State != OfflineSupervisorSimulated || canceled.Dispatch.State != EntryLifecycleClosedNotSettled || canceled.Dispatch.Cancel.DeleteAttempts != 1 || m.deletes != 1 {
		t.Fatalf("single mocked DELETE: %+v %v", canceled, err)
	}
	assertOfflineSupervisorLocked(t, canceled)
	f.cancels, err = NewCancelIntentStore(f.root)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := offlineSupervisorStep(f, m, nil)
	if err != nil || recovered.State != OfflineSupervisorSignedRecover || m.deletes != 1 || recovered.Dispatch.Cancel.DeleteAttempts != 0 {
		t.Fatalf("restart must GET only: %+v %v", recovered, err)
	}
	assertOfflineSupervisorLocked(t, recovered)
}

func TestOfflineSupervisorSignedGetFailureAndUnknownStateHalt(t *testing.T) {
	f, m := offlineEntryFixture(t, Long)
	m.failGET = true
	got, err := offlineSupervisorStep(f, m, nil)
	if err == nil || got.State != OfflineSupervisorHalted || m.deletes != 0 || m.armingTransportAdapter.m.submitCalls != 0 {
		t.Fatalf("GET failure must halt before POST: %+v %v", got, err)
	}
	assertOfflineSupervisorLocked(t, got)
	for _, state := range []string{EntryLifecycleSingleLeg, EntryLifecyclePartialHalt, EntryLifecycleDoubleTriggerHalt, EntryLifecycleBlocked, "UNKNOWN"} {
		action, _, err := nextOfflineSupervisorAction(EntryProtectionLifecycleReview{State: state}, nil)
		if err == nil || action != "" {
			t.Fatalf("incident %s allowed action %q, %v", state, action, err)
		}
	}
	_, _, err = nextOfflineSupervisorAction(EntryProtectionLifecycleReview{State: EntryLifecycleCancelAmbiguous}, nil)
	if err == nil {
		t.Fatal("ambiguous DELETE without marker must fail")
	}
}

func TestOfflineSupervisorPartialAndDoubleTriggerStayHalted(t *testing.T) {
	f, m := offlineEntryFixture(t, Long)
	armed, err := offlineSupervisorStep(f, m, nil)
	if err != nil || armed.Dispatch.State != EntryLifecyclePairActive {
		t.Fatalf("arm fake pair: %+v %v", armed, err)
	}
	// Trade evidence is partial while the account retains the original side.
	// Independently rechecking the same state must never cancel or resubmit.
	tp := m.armingTransportAdapter.m.algos[901]
	tp.Status, tp.ActualOrderID = "FINISHED", "91234"
	_ = json.Unmarshal([]byte(`"0.004"`), &tp.ActualQty)
	m.armingTransportAdapter.m.algos[901] = tp
	// The existing fixture helper creates an exact 0.01 exit; replace the
	// signed trade with a partial 0.004 and keep the pooled-side open.
	m.trades[91234] = []exchange.AccountTrade{protectionTrade(91234, 55671, f.plan.TP.Side, string(f.plan.TP.PositionSide), "0.004", "100500", "5")}
	m.armingTransportAdapter.m.order = []int64{902}
	m.zeroSide = false
	partial, err := offlineSupervisorStep(f, m, nil)
	if partial.Before.State != EntryLifecyclePartialHalt {
		t.Fatalf("expected classified partial exit, got %+v %v", partial, err)
	}
	if err == nil {
		// A conservative failure is acceptable; it cannot issue exchange writes.
		if partial.State != OfflineSupervisorHalted {
			t.Fatalf("partial fail state: %+v", partial)
		}
	} else if partial.State != OfflineSupervisorHalted {
		t.Fatalf("partial action must halt: %+v", partial)
	}
	if m.armingTransportAdapter.m.submitCalls != 2 || m.deletes != 0 {
		t.Fatalf("partial exit resubmitted or canceled: POST=%d DELETE=%d", m.armingTransportAdapter.m.submitCalls, m.deletes)
	}
	assertOfflineSupervisorLocked(t, partial)
}

func TestOfflineSupervisorCancelAckLossNoSecondDelete(t *testing.T) {
	f, m := offlineEntryFixture(t, Long)
	if armed, err := offlineSupervisorStep(f, m, nil); err != nil || armed.Dispatch.State != EntryLifecyclePairActive {
		t.Fatalf("arm: %+v %v", armed, err)
	}
	evidence := offlineTriggerAndEvidence(t, f, m)
	m.loseCancelAck = true
	first, err := offlineSupervisorStep(f, m, &evidence)
	if err == nil || first.State != OfflineSupervisorHalted || m.deletes != 1 {
		t.Fatalf("expected injected DELETE failure: %+v %v", first, err)
	}
	f.cancels, err = NewCancelIntentStore(f.root)
	if err != nil {
		t.Fatal(err)
	}
	second, err := offlineSupervisorStep(f, m, nil)
	if err != nil || second.State != OfflineSupervisorSignedRecover || m.deletes != 1 {
		t.Fatalf("ambiguous DELETE must be recovered by GET only: %+v %v", second, err)
	}
	assertOfflineSupervisorLocked(t, second)
}

func TestOfflineSupervisorDoubleTriggerNeverSendsAnotherCommand(t *testing.T) {
	f, m := offlineEntryFixture(t, Short)
	armed, err := offlineSupervisorStep(f, m, nil)
	if err != nil || armed.Dispatch.State != EntryLifecyclePairActive {
		t.Fatalf("arm: %+v %v", armed, err)
	}
	// Both distinct Algo orders are reported FINISHED and carry independent
	// signed exit trades, even if their aggregate happens to equal the entry.
	a := m.armingTransportAdapter.m.algos[901]
	a.Status, a.ActualOrderID = "FINISHED", "91234"
	_ = json.Unmarshal([]byte(`"0.004"`), &a.ActualQty)
	m.armingTransportAdapter.m.algos[901] = a
	b := m.armingTransportAdapter.m.algos[902]
	b.Status, b.ActualOrderID = "FINISHED", "95612"
	_ = json.Unmarshal([]byte(`"0.006"`), &b.ActualQty)
	m.armingTransportAdapter.m.algos[902] = b
	m.armingTransportAdapter.m.order = nil
	m.trades[91234] = []exchange.AccountTrade{protectionTrade(91234, 55671, f.plan.TP.Side, string(f.plan.TP.PositionSide), "0.004", "100500", "5")}
	m.trades[95612] = []exchange.AccountTrade{protectionTrade(95612, 55672, f.plan.SL.Side, string(f.plan.SL.PositionSide), "0.006", "99900", "-2")}
	m.zeroSide = true
	halted, err := offlineSupervisorStep(f, m, nil)
	if err == nil || halted.State != OfflineSupervisorHalted || halted.Before.State != EntryLifecycleDoubleTriggerHalt || m.armingTransportAdapter.m.submitCalls != 2 || m.deletes != 0 {
		t.Fatalf("double trigger must halt without mutation: %+v %v", halted, err)
	}
	assertOfflineSupervisorLocked(t, halted)
}
