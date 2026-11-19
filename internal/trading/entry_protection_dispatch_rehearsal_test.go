package trading

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

// This is the ONLY adapter that can satisfy the private local-only marker.
// It explicitly forwards writes to an in-memory exchange simulator, never a
// real exchange.Client, and tracks every simulated POST/DELETE.
type entryOfflineExchange struct {
	*entryLifecycleSignedMock
	loseTPAck            bool
	loseCancelAck        bool
	loseCancelAfterApply bool
	deletes              int
}

func (*entryOfflineExchange) localProtectionRehearsalOnly() {}
func (m *entryOfflineExchange) SubmitDemoProtection(ctx context.Context, key, secret string, request exchange.NewDemoProtection) (exchange.DemoAlgoResult, error) {
	response, err := m.entryLifecycleSignedMock.armingTransportAdapter.SubmitDemoProtection(ctx, key, secret, request)
	if err != nil {
		return response, err
	}
	if m.loseTPAck {
		m.loseTPAck = false
		return exchange.DemoAlgoResult{}, errors.New("injected TP ACK loss after exchange accepted order")
	}
	return response, nil
}
func (m *entryOfflineExchange) CancelDemoAlgoOnce(ctx context.Context, key, secret string, id int64) (exchange.DemoAlgoResult, error) {
	m.deletes++
	if m.loseCancelAck {
		return exchange.DemoAlgoResult{}, errors.New("injected DELETE ACK loss")
	}
	order, ok := m.armingTransportAdapter.m.algos[id]
	if !ok {
		return exchange.DemoAlgoResult{}, errors.New("unknown DELETE target")
	}
	order.Status = "CANCELED"
	m.armingTransportAdapter.m.algos[id] = order
	remaining := []int64{}
	for _, existing := range m.armingTransportAdapter.m.order {
		if existing != id {
			remaining = append(remaining, existing)
		}
	}
	m.armingTransportAdapter.m.order = remaining
	if m.loseCancelAfterApply {
		m.loseCancelAfterApply = false
		return exchange.DemoAlgoResult{}, errors.New("injected DELETE ACK loss after exchange canceled sibling")
	}
	return order, nil
}
func offlineEntryFixture(t *testing.T, side PositionSide) (*entryLifecycleFixture, *entryOfflineExchange) {
	t.Helper()
	f := newEntryLifecycleFixture(t, side)
	return f, &entryOfflineExchange{entryLifecycleSignedMock: f.signed}
}
func (m *entryOfflineExchange) step(f *entryLifecycleFixture, action EntryProtectionRehearsalAction, e *ProtectionEvidence) (EntryProtectionRehearsalResult, error) {
	return RehearseEntryProtectionDispatch(context.Background(), action, f.reservation, f.coordinator, f.lifecycle, f.protection, f.cancels, m, DemoCredentials{APIKey: "demo", Secret: "secret"}, e)
}
func checkOfflineResult(t *testing.T, r EntryProtectionRehearsalResult) {
	t.Helper()
	if r.TradingAuthorized || r.FinanciallySettled || !r.ManualReviewRequired || r.Before.TradingAuthorized || r.After.CancelAuthorized || r.After.RetryAuthorized || r.After.SettlementAuthorized {
		t.Fatalf("offline flow escaped lock: %+v", r)
	}
}
func TestEntryOfflineArmLongShortPairAndRestartNoReplay(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			f, m := offlineEntryFixture(t, side)
			first, err := m.step(f, EntryRehearsalArm, nil)
			if err != nil || first.Before.State != EntryLifecyclePreparedOnly || first.State != EntryLifecyclePairActive || m.armingTransportAdapter.m.submitCalls != 2 {
				t.Fatalf("arm: %+v %v POST=%d", first, err, m.armingTransportAdapter.m.submitCalls)
			}
			checkOfflineResult(t, first)
			life, e := NewExecutionLifecycleStore(f.root)
			if e != nil {
				t.Fatal(e)
			}
			store, e := OpenProtectionSubmissionStore(f.root)
			if e != nil {
				t.Fatal(e)
			}
			cancels, e := NewCancelIntentStore(f.root)
			if e != nil {
				t.Fatal(e)
			}
			f.lifecycle, f.protection, f.cancels = life, store, cancels
			second, err := m.step(f, EntryRehearsalArm, nil)
			if err == nil || second.Before.State != EntryLifecyclePairActive || m.armingTransportAdapter.m.submitCalls != 2 {
				t.Fatalf("restart must not re-POST: %+v %v", second, err)
			}
			inspected, err := m.step(f, EntryRehearsalInspect, nil)
			if err != nil || inspected.State != EntryLifecyclePairActive {
				t.Fatalf("inspect: %+v %v", inspected, err)
			}
			checkOfflineResult(t, inspected)
		})
	}
}
func TestEntryOfflineArmAckLossGETOnlyRecovery(t *testing.T) {
	f, m := offlineEntryFixture(t, Long)
	m.loseTPAck = true
	first, err := m.step(f, EntryRehearsalArm, nil)
	if err == nil || first.State != "HALT" || m.armingTransportAdapter.m.submitCalls != 1 {
		t.Fatalf("ACK loss: %+v %v", first, err)
	}
	check, err := InspectProtectionArmingCheckpoint(f.protection, f.plan.GroupID)
	if err != nil || check.State != "TP_MAY_HAVE_SENT_UNRESOLVED" {
		t.Fatalf("missing durable marker: %+v %v", check, err)
	}
	f.protection, err = OpenProtectionSubmissionStore(f.root)
	if err != nil {
		t.Fatal(err)
	}
	second, err := m.step(f, EntryRehearsalArm, nil)
	if err != nil || second.Before.State != EntryLifecycleUnknownSubmission || second.State != EntryLifecycleSingleLeg || m.armingTransportAdapter.m.submitCalls != 1 {
		t.Fatalf("signed recovery must not POST SL: %+v %v POST=%d", second, err, m.armingTransportAdapter.m.submitCalls)
	}
	checkOfflineResult(t, second)
	third, err := m.step(f, EntryRehearsalArm, nil)
	if err == nil || third.Before.State != EntryLifecycleSingleLeg || m.armingTransportAdapter.m.submitCalls != 1 {
		t.Fatalf("single leg retry: %+v %v", third, err)
	}
}
func TestEntryOfflineArmSecondLegMissingAfterAckLossNoReplay(t *testing.T) {
	f, m := offlineEntryFixture(t, Short)
	m.armingTransportAdapter.m.failSecond = true
	first, err := m.step(f, EntryRehearsalArm, nil)
	if err == nil || m.armingTransportAdapter.m.submitCalls != 2 {
		t.Fatalf("second POST injected fault: %+v %v", first, err)
	}
	second, err := m.step(f, EntryRehearsalArm, nil)
	if err == nil || second.Before.State != EntryLifecycleUnknownSubmission || m.armingTransportAdapter.m.submitCalls != 2 {
		t.Fatalf("missing SL must halt: %+v %v POST=%d", second, err, m.armingTransportAdapter.m.submitCalls)
	}
	checkOfflineResult(t, second)
}
func offlineTriggerAndEvidence(t *testing.T, f *entryLifecycleFixture, m *entryOfflineExchange) ProtectionEvidence {
	t.Helper()
	tpID, slID := int64(901), int64(902)
	a := m.armingTransportAdapter.m.algos[tpID]
	a.Status = "FINISHED"
	a.ActualOrderID = "91234"
	if err := json.Unmarshal([]byte(`"0.01"`), &a.ActualQty); err != nil {
		t.Fatal(err)
	}
	m.armingTransportAdapter.m.algos[tpID] = a
	m.armingTransportAdapter.m.order = []int64{slID}
	m.trades[91234] = []exchange.AccountTrade{protectionTrade(91234, 55671, f.plan.TP.Side, string(f.plan.TP.PositionSide), "0.01", "100500", "5")}
	m.zeroSide = true
	now := time.Now().UTC()
	owner := OwnerPosition{Source: f.reservation.Intent.Source, ModelID: f.reservation.Intent.ModelID, Symbol: "BTCUSDT", Side: f.reservation.Intent.PositionSide, Quantity: "0.01"}
	return ProtectionEvidence{TP: AlgoObservation{ClientID: f.plan.TP.ClientAlgoID, AlgoID: tpID, Status: "FINISHED", Symbol: "BTCUSDT", PositionSide: owner.Side, Quantity: "0.01", ActualQty: "0.01", ActualOrderID: "91234", FromSignedGet: true, ObservedAt: now},
		SL: AlgoObservation{ClientID: f.plan.SL.ClientAlgoID, AlgoID: slID, Status: "NEW", Symbol: "BTCUSDT", PositionSide: owner.Side, Quantity: "0.01", FromSignedGet: true, ObservedAt: now}, Owner: owner, RemainingOwnerQty: "0", FullSignedOwnershipAudit: true, SignedAccountAsOf: now}
}
func TestEntryOfflineCancelTriggeredSiblingOneShotAndRestartGETOnly(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			f, m := offlineEntryFixture(t, side)
			if _, err := m.step(f, EntryRehearsalArm, nil); err != nil {
				t.Fatal(err)
			}
			evidence := offlineTriggerAndEvidence(t, f, m)
			out, err := m.step(f, EntryRehearsalCancel, &evidence)
			if err != nil || out.Before.State != EntryLifecycleCancelReview || out.State != EntryLifecycleClosedNotSettled || out.Cancel.DeleteAttempts != 1 || m.deletes != 1 {
				t.Fatalf("cancel: %+v %v DELETE=%d", out, err, m.deletes)
			}
			checkOfflineResult(t, out)
			f.cancels, err = NewCancelIntentStore(f.root)
			if err != nil {
				t.Fatal(err)
			}
			later, err := m.step(f, EntryRehearsalCancel, nil)
			if err != nil || later.Before.State != EntryLifecycleClosedNotSettled || m.deletes != 1 || later.Cancel.State != string(CancelRecoveryCanceled) {
				t.Fatalf("restart must GET only: %+v %v", later, err)
			}
			checkOfflineResult(t, later)
		})
	}
}
func TestEntryOfflineCancelAckLossRemainsActiveNoSecondDELETE(t *testing.T) {
	f, m := offlineEntryFixture(t, Long)
	if _, err := m.step(f, EntryRehearsalArm, nil); err != nil {
		t.Fatal(err)
	}
	evidence := offlineTriggerAndEvidence(t, f, m)
	m.loseCancelAck = true
	first, err := m.step(f, EntryRehearsalCancel, &evidence)
	if err == nil || first.State != "HALT" || m.deletes != 1 {
		t.Fatalf("cancel ACK loss: %+v %v", first, err)
	}
	f.cancels, err = NewCancelIntentStore(f.root)
	if err != nil {
		t.Fatal(err)
	}
	second, err := m.step(f, EntryRehearsalCancel, nil)
	if err != nil || second.Before.State != EntryLifecycleCancelAmbiguous || second.Cancel.State != string(CancelRecoveryPendingActive) || m.deletes != 1 {
		t.Fatalf("restarted active canceled? %+v %v", second, err)
	}
	checkOfflineResult(t, second)
}
func TestEntryOfflineCancelBadEvidenceAndSignedFaultFailClosed(t *testing.T) {
	f, m := offlineEntryFixture(t, Long)
	if _, err := m.step(f, EntryRehearsalArm, nil); err != nil {
		t.Fatal(err)
	}
	evidence := offlineTriggerAndEvidence(t, f, m)
	cases := []struct {
		name   string
		change func(*ProtectionEvidence)
	}{
		{"wrong owner", func(e *ProtectionEvidence) { e.Owner.ModelID = "forged" }},
		{"wrong sibling", func(e *ProtectionEvidence) { e.SL.AlgoID = 987 }},
		{"untrusted audit", func(e *ProtectionEvidence) { e.FullSignedOwnershipAudit = false }},
		{"partial remaining", func(e *ProtectionEvidence) { e.RemainingOwnerQty = "0.005" }},
		{"stale account", func(e *ProtectionEvidence) { e.SignedAccountAsOf = time.Now().Add(-time.Minute) }},
		{"double trigger", func(e *ProtectionEvidence) { e.SL.Status = "FINISHED" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			altered := evidence
			tc.change(&altered)
			out, err := m.step(f, EntryRehearsalCancel, &altered)
			if err == nil || out.State != "HALT" || m.deletes != 0 {
				t.Fatalf("bad evidence used for DELETE: %+v %v", out, err)
			}
		})
	}
	m.failGET = true
	out, err := m.step(f, EntryRehearsalCancel, &evidence)
	if err == nil || m.deletes != 0 || out.State != "HALT" {
		t.Fatalf("signed GET failure: %+v %v", out, err)
	}
	m.failGET = false
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = RehearseEntryProtectionDispatch(ctx, EntryRehearsalCancel, f.reservation, f.coordinator, f.lifecycle, f.protection, f.cancels, m, DemoCredentials{APIKey: "demo", Secret: "secret"}, &evidence)
	if !errors.Is(err, context.Canceled) || m.deletes != 0 {
		t.Fatalf("canceled context: %v", err)
	}
	if _, _, err := f.cancels.Inspect(f.plan.GroupID); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unapproved cancel intent persisted: %v", err)
	}
}

func TestEntryOfflineAmbiguousForeignAlgoAndTamperedIdentityNoRecovery(t *testing.T) {
	for _, mutate := range []struct {
		name   string
		change func(*exchange.DemoAlgoResult)
	}{
		{"foreign client ID", func(a *exchange.DemoAlgoResult) { a.ClientAlgoID = "foreign-client-algo" }},
		{"wrong position side", func(a *exchange.DemoAlgoResult) { a.PositionSide = "SHORT" }},
		{"wrong quantity", func(a *exchange.DemoAlgoResult) { _ = json.Unmarshal([]byte(`"0.02"`), &a.Quantity) }},
	} {
		t.Run(mutate.name, func(t *testing.T) {
			f, m := offlineEntryFixture(t, Long)
			if err := f.protection.MarkMayHaveSent(f.plan.GroupID, "tp", true); err != nil {
				t.Fatal(err)
			}
			a := algoFromInstruction(905, f.plan.TP, "NEW")
			mutate.change(&a)
			m.armingTransportAdapter.m.algos[905] = a
			m.armingTransportAdapter.m.order = []int64{905}
			out, err := m.step(f, EntryRehearsalArm, nil)
			if err == nil || out.State != "HALT" || m.armingTransportAdapter.m.submitCalls != 0 || m.deletes != 0 {
				t.Fatalf("untrusted open algo recovered: %+v %v", out, err)
			}
		})
	}
}
func TestProductProtectionConvenienceWritesAlwaysLocked(t *testing.T) {
	// These runtime wrappers must not directly call the real exchange even
	// when given the original operator-created ownership/evidence structs.
	var runtime *DemoProtectionRuntimeCoordinator
	arm, err := runtime.Arm(context.Background(), "group-p-001", OwnerPosition{Symbol: "BTCUSDT", Side: Long})
	if !errors.Is(err, ErrProtectionApproval) || arm.Armed || !arm.ManualReviewRequired {
		t.Fatalf("runtime Arm unlocked: %+v %v", arm, err)
	}
	_, err = runtime.CancelTriggeredSibling(context.Background(), "group-p-001", ProtectionEvidence{FullSignedOwnershipAudit: true})
	if !errors.Is(err, ErrProtectionApproval) {
		t.Fatalf("runtime DELETE unlocked: %v", err)
	}
}
