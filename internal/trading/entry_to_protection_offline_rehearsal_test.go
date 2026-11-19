package trading

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

type endToEndOfflineFixture struct {
	*entryLifecycleFixture
	*entryOfflineExchange
}

func newEndToEndOfflineFixture(t *testing.T, side PositionSide, status, quantity string) *endToEndOfflineFixture {
	t.Helper()
	f := newEntryBridgeFixture(t, side, status, quantity)
	cancels, err := NewCancelIntentStore(f.root)
	if err != nil {
		t.Fatal(err)
	}
	signed := &entryLifecycleSignedMock{armingTransportAdapter: f.transport, trades: map[int64][]exchange.AccountTrade{}}
	m := &entryOfflineExchange{entryLifecycleSignedMock: signed}
	return &endToEndOfflineFixture{
		entryLifecycleFixture: &entryLifecycleFixture{entryBridgeFixture: f, cancels: cancels, signed: signed},
		entryOfflineExchange:  m,
	}
}

func (f *endToEndOfflineFixture) run(action EntryProtectionRehearsalAction, evidence *ProtectionEvidence) (EntryToProtectionOfflineResult, error) {
	return f.runCtx(context.Background(), action, evidence)
}
func (f *endToEndOfflineFixture) runCtx(ctx context.Context, action EntryProtectionRehearsalAction, evidence *ProtectionEvidence) (EntryToProtectionOfflineResult, error) {
	quote := Quote{Symbol: "BTCUSDT", Price: "100000", ReceivedAt: time.Now().UTC()}
	return RehearseRecoveredEntryToProtectionOffline(ctx, action, f.reservation,
		f.coordinator, f.lifecycle, f.ownership, f.protection, f.cancels,
		f.reader, f.entryOfflineExchange, DemoCredentials{APIKey: "demo", Secret: "secret"},
		sizingRules(time.Now().UTC()), quote, quote, evidence)
}
func (f *endToEndOfflineFixture) reopen(t *testing.T) {
	t.Helper()
	var err error
	f.lifecycle, err = NewExecutionLifecycleStore(f.root)
	if err != nil {
		t.Fatal(err)
	}
	f.ownership, err = NewOwnershipStore(f.root)
	if err != nil {
		t.Fatal(err)
	}
	f.protection, err = OpenProtectionSubmissionStore(f.root)
	if err != nil {
		t.Fatal(err)
	}
	f.cancels, err = NewCancelIntentStore(f.root)
	if err != nil {
		t.Fatal(err)
	}
}
func assertEndToEndOfflineLocked(t *testing.T, r EntryToProtectionOfflineResult) {
	t.Helper()
	if r.TradingAuthorized || r.ProtectionAuthorized || r.SettlementAuthorized || !r.ManualReviewRequired {
		t.Fatalf("unlocked offline flow: %+v", r)
	}
	if r.Preparation != nil && (r.Preparation.TradingAuthorized || r.Preparation.ProtectionSubmitAuthorized || r.Preparation.SettlementAuthorized) {
		t.Fatalf("unlocked preparation: %+v", r.Preparation)
	}
	if r.Dispatch != nil && (r.Dispatch.TradingAuthorized || r.Dispatch.FinanciallySettled || !r.Dispatch.ManualReviewRequired || r.Dispatch.After.TradingAuthorized || r.Dispatch.After.CancelAuthorized) {
		t.Fatalf("unlocked dispatch: %+v", r.Dispatch)
	}
}
func TestEntryEndToEndOfflineLongShortEntryRecoveryPairExitCancelRestart(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			f := newEndToEndOfflineFixture(t, side, "FILLED", "0.01")
			first, err := f.run(EntryRehearsalArm, nil)
			if err != nil || !first.PreparedThisCall || first.Preparation == nil || first.Preparation.Recovery.ImportedFillCount != 1 ||
				first.State != EntryLifecyclePairActive || f.armingTransportAdapter.m.submitCalls != 2 || f.executor.calls != 1 {
				t.Fatalf("signed entry → pair: %+v err=%v, POST=%d", first, err, f.armingTransportAdapter.m.submitCalls)
			}
			assertEndToEndOfflineLocked(t, first)
			if _, err := os.Stat(filepath.Join(f.root, "trading", "reservations_done", f.reservation.ClientOrderID+".json")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("entry reservation was settled: %v", err)
			}
			f.plan = first.Preparation.Plan
			f.reopen(t)
			inspected, err := f.run(EntryRehearsalInspect, nil)
			if err != nil || inspected.PreparedThisCall || inspected.Preparation != nil || inspected.State != EntryLifecyclePairActive || f.armingTransportAdapter.m.submitCalls != 2 {
				t.Fatalf("restart inspect: %+v %v", inspected, err)
			}
			assertEndToEndOfflineLocked(t, inspected)
			if retry, err := f.run(EntryRehearsalArm, nil); err == nil || retry.PreparedThisCall || f.armingTransportAdapter.m.submitCalls != 2 {
				t.Fatalf("restart re-POST allowed: %+v %v", retry, err)
			}
			evidence := offlineTriggerAndEvidence(t, f.entryLifecycleFixture, f.entryOfflineExchange)
			canceled, err := f.run(EntryRehearsalCancel, &evidence)
			if err != nil || canceled.State != EntryLifecycleClosedNotSettled || f.deletes != 1 || canceled.PreparedThisCall {
				t.Fatalf("exit → sibling cancel: %+v %v", canceled, err)
			}
			assertEndToEndOfflineLocked(t, canceled)
			f.reopen(t)
			recovered, err := f.run(EntryRehearsalCancel, nil)
			if err != nil || recovered.State != EntryLifecycleClosedNotSettled || f.deletes != 1 || f.armingTransportAdapter.m.submitCalls != 2 {
				t.Fatalf("restart DELETE replay: %+v %v", recovered, err)
			}
			assertEndToEndOfflineLocked(t, recovered)
		})
	}
}

func TestEntryEndToEndOfflineInspectUnpreparedIsStrictlyReadOnly(t *testing.T) {
	f := newEndToEndOfflineFixture(t, Long, "FILLED", "0.01")
	original, err := f.lifecycle.Load(f.reservation)
	if err != nil {
		t.Fatal(err)
	}
	f.reader.fail = true // Inspect must not attempt entry signed recovery.
	for i := 0; i < 3; i++ {
		inspected, err := f.run(EntryRehearsalInspect, nil)
		if err != nil || inspected.PreparedThisCall || inspected.Preparation != nil || inspected.Dispatch != nil ||
			inspected.State != OfflineEntryLifecycleUnprepared || f.armingTransportAdapter.m.submitCalls != 0 || f.deletes != 0 {
			t.Fatalf("inspect caused mutation: %+v %v", inspected, err)
		}
		assertEndToEndOfflineLocked(t, inspected)
		if _, err := f.protection.read(inspected.GroupID); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("inspect persisted protection plan: %v", err)
		}
		current, err := f.lifecycle.Load(f.reservation)
		if err != nil || !reflect.DeepEqual(original, current) {
			t.Fatalf("inspect modified entry lifecycle: %v", err)
		}
		if f.signed.getCalls != 0 {
			t.Fatalf("inspect performed signed account queries: %d", f.signed.getCalls)
		}
		f.reopen(t)
	}
	f.reader.fail = false
	armed, err := f.run(EntryRehearsalArm, nil)
	if err != nil || !armed.PreparedThisCall || armed.State != EntryLifecyclePairActive || f.armingTransportAdapter.m.submitCalls != 2 {
		t.Fatalf("explicit arm after inspect: %+v %v", armed, err)
	}
	assertEndToEndOfflineLocked(t, armed)
}

func TestEntryEndToEndOfflinePendingPartialAndGETFailureNeverPost(t *testing.T) {
	for _, tc := range []struct {
		name, status, qty string
		wantErr           bool
	}{
		{"new", "NEW", "0", false},
		{"partial", "PARTIALLY_FILLED", "0.005", false},
		{"terminal-partial", "CANCELED", "0.005", true},
		{"terminal-rejected", "REJECTED", "0", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newEndToEndOfflineFixture(t, Long, tc.status, tc.qty)
			res, err := f.run(EntryRehearsalArm, nil)
			if (err != nil) != tc.wantErr || res.PreparedThisCall || f.armingTransportAdapter.m.submitCalls != 0 || res.Dispatch != nil ||
				(!tc.wantErr && res.State != OfflineEntryLifecyclePending) || (tc.wantErr && res.State != OfflineEntryLifecycleBlocked) {
				t.Fatalf("pending/partial: %+v %v", res, err)
			}
			assertEndToEndOfflineLocked(t, res)
			if _, err := f.protection.read(res.GroupID); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("unexpected protection plan: %v", err)
			}
		})
	}
	f := newEndToEndOfflineFixture(t, Short, "FILLED", "0.01")
	f.reader.fail = true
	res, err := f.run(EntryRehearsalArm, nil)
	if err == nil || res.State != OfflineEntryLifecycleBlocked || f.armingTransportAdapter.m.submitCalls != 0 {
		t.Fatalf("GET failure created orders: %+v %v", res, err)
	}
}

func TestEntryEndToEndOfflineTPAckLossAndNoRePOSTOnRestart(t *testing.T) {
	f := newEndToEndOfflineFixture(t, Long, "FILLED", "0.01")
	f.loseTPAck = true
	first, err := f.run(EntryRehearsalArm, nil)
	if err == nil || !first.PreparedThisCall || first.State != OfflineEntryLifecycleBlocked || f.armingTransportAdapter.m.submitCalls != 1 {
		t.Fatalf("ACK-loss side effect missing: %+v %v", first, err)
	}
	f.reopen(t)
	recovered, err := f.run(EntryRehearsalArm, nil)
	if err != nil || recovered.PreparedThisCall || recovered.State != EntryLifecycleSingleLeg || f.armingTransportAdapter.m.submitCalls != 1 {
		t.Fatalf("GET-only recovery re-POSTed: %+v %v", recovered, err)
	}
	if again, err := f.run(EntryRehearsalArm, nil); err == nil || again.State != OfflineEntryLifecycleBlocked || f.armingTransportAdapter.m.submitCalls != 1 {
		t.Fatalf("single-leg restart re-POSTed: %+v %v", again, err)
	}
	assertEndToEndOfflineLocked(t, recovered)
}

func TestEntryEndToEndOfflineCancelACKLossAndNoReDELETE(t *testing.T) {
	f := newEndToEndOfflineFixture(t, Short, "FILLED", "0.01")
	first, err := f.run(EntryRehearsalArm, nil)
	if err != nil || first.State != EntryLifecyclePairActive {
		t.Fatalf("arm: %+v %v", first, err)
	}
	f.plan = first.Preparation.Plan
	evidence := offlineTriggerAndEvidence(t, f.entryLifecycleFixture, f.entryOfflineExchange)
	f.loseCancelAck = true
	attempted, err := f.run(EntryRehearsalCancel, &evidence)
	if err == nil || attempted.State != OfflineEntryLifecycleBlocked || f.deletes != 1 {
		t.Fatalf("DELETE loss: %+v %v", attempted, err)
	}
	f.reopen(t)
	recovered, err := f.run(EntryRehearsalCancel, nil)
	if err != nil || recovered.State != EntryLifecycleCancelAmbiguous || f.deletes != 1 || recovered.Dispatch.Cancel.State != string(CancelRecoveryPendingActive) {
		t.Fatalf("replayed DELETE: %+v %v", recovered, err)
	}
	assertEndToEndOfflineLocked(t, recovered)
}

func TestEntryEndToEndOfflineInvalidActionForeignStoreAndCorruptPlanFailClosed(t *testing.T) {
	f := newEndToEndOfflineFixture(t, Long, "FILLED", "0.01")
	if result, err := f.run("UNSAFE_ACTION", nil); err == nil || result.State != OfflineEntryLifecycleBlocked || f.executor.calls != 1 {
		t.Fatalf("invalid action accepted: %+v %v", result, err)
	}
	if result, err := f.run(EntryRehearsalCancel, nil); err == nil || result.State != OfflineEntryLifecycleBlocked {
		t.Fatalf("unprepared cancel accepted: %+v %v", result, err)
	}
	foreign, err := OpenProtectionSubmissionStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	original := f.protection
	f.protection = foreign
	if result, err := f.run(EntryRehearsalArm, nil); err == nil || result.State != OfflineEntryLifecycleBlocked {
		t.Fatalf("foreign store accepted: %+v %v", result, err)
	}
	f.protection = original
	if err := os.WriteFile(filepath.Join(f.root, "trading", "protection_submissions", entryProtectionGroupID(f.reservation)+".json"), []byte("{partial"), 0600); err != nil {
		t.Fatal(err)
	}
	if result, err := f.run(EntryRehearsalArm, nil); err == nil || result.State != OfflineEntryLifecycleBlocked || f.armingTransportAdapter.m.submitCalls != 0 {
		t.Fatalf("corrupt plan re-prepared: %+v %v", result, err)
	}
}

func TestEntryEndToEndOfflineCanceledContextNeverTouchesEvidence(t *testing.T) {
	f := newEndToEndOfflineFixture(t, Long, "FILLED", "0.01")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	res, err := f.runCtx(ctx, EntryRehearsalArm, nil)
	if !errors.Is(err, context.Canceled) || res.State != OfflineEntryLifecycleBlocked || f.armingTransportAdapter.m.submitCalls != 0 {
		t.Fatalf("canceled flow continued: %+v %v", res, err)
	}
}

func TestEntryEndToEndOfflinePartialExitDoubleTriggerAndForeignAlgoHalt(t *testing.T) {
	f := newEndToEndOfflineFixture(t, Long, "FILLED", "0.01")
	first, err := f.run(EntryRehearsalArm, nil)
	if err != nil || first.State != EntryLifecyclePairActive {
		t.Fatalf("arm: %+v %v", first, err)
	}
	f.plan = first.Preparation.Plan
	// A partial exit is NOT sufficient proof to cancel the second leg.
	tp := f.armingTransportAdapter.m.algos[901]
	tp.Status = "FINISHED"
	tp.ActualOrderID = "91234"
	tp.ActualQty = "0.004"
	f.armingTransportAdapter.m.algos[901] = tp
	f.armingTransportAdapter.m.order = []int64{902}
	f.signed.trades[91234] = []exchange.AccountTrade{protectionTrade(91234, 55671, f.plan.TP.Side, "LONG", "0.004", "100500", "2")}
	partial, err := f.run(EntryRehearsalInspect, nil)
	if err != nil || partial.State != EntryLifecyclePartialHalt {
		t.Fatalf("partial exit not halted: %+v %v", partial, err)
	}
	blocked, err := f.run(EntryRehearsalCancel, nil)
	if err == nil || blocked.State != OfflineEntryLifecycleBlocked || f.deletes != 0 {
		t.Fatalf("partial exit allowed DELETE: %+v %v", blocked, err)
	}
	sl := f.armingTransportAdapter.m.algos[902]
	sl.Status = "FINISHED"
	sl.ActualOrderID = "95612"
	sl.ActualQty = "0.006"
	f.armingTransportAdapter.m.algos[902] = sl
	f.armingTransportAdapter.m.order = nil
	f.signed.trades[95612] = []exchange.AccountTrade{protectionTrade(95612, 55672, f.plan.SL.Side, "LONG", "0.006", "99900", "-2")}
	f.reopen(t)
	double, err := f.run(EntryRehearsalInspect, nil)
	if err != nil || double.State != EntryLifecycleDoubleTriggerHalt {
		t.Fatalf("double trigger not halted: %+v %v", double, err)
	}
	blocked, err = f.run(EntryRehearsalCancel, nil)
	if err == nil || blocked.State != OfflineEntryLifecycleBlocked || f.deletes != 0 {
		t.Fatalf("double trigger allowed DELETE: %+v %v", blocked, err)
	}
	assertEndToEndOfflineLocked(t, double)

	second := newEndToEndOfflineFixture(t, Short, "FILLED", "0.01")
	out, err := second.run(EntryRehearsalArm, nil)
	if err != nil || out.State != EntryLifecyclePairActive {
		t.Fatal(err)
	}
	second.signed.foreignAlgo = true
	out, err = second.run(EntryRehearsalInspect, nil)
	if err == nil || out.State != OfflineEntryLifecycleBlocked || second.armingTransportAdapter.m.submitCalls != 2 {
		t.Fatalf("foreign algo not halted: %+v %v", out, err)
	}
}
