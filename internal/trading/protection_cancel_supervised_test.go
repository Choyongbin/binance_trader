package trading

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"

	"binance_trader/internal/exchange"
)

type supervisedCancelMock struct {
	tp          exchange.DemoAlgoResult
	sl          exchange.DemoAlgoResult
	getCalls    int
	deleteCalls int
	timeout     bool
	badIdentity bool
}

func (m *supervisedCancelMock) QueryDemoAlgo(_ context.Context, _, _ string, id int64) (exchange.DemoAlgoResult, error) {
	m.getCalls++
	if id == m.tp.AlgoID {
		return m.tp, nil
	}
	if id == m.sl.AlgoID {
		result := m.sl
		if m.badIdentity {
			result.ClientAlgoID = "different_owner"
		}
		return result, nil
	}
	return exchange.DemoAlgoResult{}, errors.New("unknown id")
}
func (m *supervisedCancelMock) CancelDemoAlgoOnce(_ context.Context, _, _ string, id int64) (exchange.DemoAlgoResult, error) {
	m.deleteCalls++
	if id != m.sl.AlgoID {
		return exchange.DemoAlgoResult{}, errors.New("wrong DELETE target")
	}
	if m.timeout {
		return exchange.DemoAlgoResult{}, errors.New("lost ACK")
	}
	m.sl.Status = "CANCELED"
	return m.sl, nil
}
func supervisedCancelFixture(t *testing.T) (*Coordinator, *ProtectionSubmissionStore, *CancelIntentStore, ProtectionEvidence, *supervisedCancelMock) {
	t.Helper()
	submissions, cancels, evidence, _, dir := siblingContinuityFixture(t)
	coord, err := OpenCoordinator(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = coord.Close() })
	plan, err := submissions.read("group-p-001")
	if err != nil {
		t.Fatal(err)
	}
	m := &supervisedCancelMock{
		tp: algoFromInstruction(evidence.TP.AlgoID, plan.TP, evidence.TP.Status),
		sl: algoFromInstruction(evidence.SL.AlgoID, plan.SL, evidence.SL.Status),
	}
	// Triggered leg's actual execution is not a cancellation. No settlement
	// is implied even when its exchange order has been observed.
	m.tp.ActualOrderID = evidence.TP.ActualOrderID
	_ = json.Unmarshal([]byte(`"0.010"`), &m.tp.ActualQty)
	return coord, submissions, cancels, evidence, m
}
func TestSupervisedCancelSingleDeleteThenSignedRecovery(t *testing.T) {
	coord, subs, cancels, evidence, m := supervisedCancelFixture(t)
	creds := DemoCredentials{APIKey: "demo", Secret: "test"}
	out, err := SuperviseSiblingCancelOnce(context.Background(), coord, subs, cancels, m, creds, "group-p-001", evidence)
	if err != nil || out.State != string(CancelRecoveryCanceled) || out.DeleteAttempts != 1 || m.deleteCalls != 1 || out.TradingAuthorized || out.FinanciallySettled {
		t.Fatalf("first cancel: %+v err=%v deletes=%d", out, err, m.deleteCalls)
	}
	out, err = SuperviseSiblingCancelOnce(context.Background(), coord, subs, cancels, m, creds, "group-p-001", evidence)
	if err != nil || out.DeleteAttempts != 0 || out.State != string(CancelRecoveryCanceled) || m.deleteCalls != 1 {
		t.Fatalf("restart replayed DELETE: %+v err=%v deletes=%d", out, err, m.deleteCalls)
	}
}
func TestSupervisedCancelAmbiguousAckNeverResends(t *testing.T) {
	coord, subs, cancels, evidence, m := supervisedCancelFixture(t)
	m.timeout = true
	creds := DemoCredentials{APIKey: "demo", Secret: "test"}
	out, err := SuperviseSiblingCancelOnce(context.Background(), coord, subs, cancels, m, creds, "group-p-001", evidence)
	if err == nil || out.State != "MAY_HAVE_SENT_RECHECK_REQUIRED" || m.deleteCalls != 1 {
		t.Fatalf("ACK loss: %+v %v", out, err)
	}
	m.timeout = false
	out, err = SuperviseSiblingCancelOnce(context.Background(), coord, subs, cancels, m, creds, "group-p-001", evidence)
	if err != nil || out.State != string(CancelRecoveryPendingActive) || out.DeleteAttempts != 0 || m.deleteCalls != 1 {
		t.Fatalf("restarted active must not DELETE: %+v %v calls=%d", out, err, m.deleteCalls)
	}
	m.sl.Status = "FINISHED"
	out, err = SuperviseSiblingCancelOnce(context.Background(), coord, subs, cancels, m, creds, "group-p-001", evidence)
	if err != nil || out.State != string(CancelRecoveryTriggered) || out.DeleteAttempts != 0 || m.deleteCalls != 1 {
		t.Fatalf("triggered sibling must reconcile: %+v %v calls=%d", out, err, m.deleteCalls)
	}
}
func TestSupervisedCancelRejectsAlteredSignedEvidenceBeforeDelete(t *testing.T) {
	coord, subs, cancels, evidence, m := supervisedCancelFixture(t)
	m.badIdentity = true
	out, err := SuperviseSiblingCancelOnce(context.Background(), coord, subs, cancels, m, DemoCredentials{APIKey: "demo", Secret: "test"}, "group-p-001", evidence)
	if err == nil || m.deleteCalls != 0 || out.TradingAuthorized {
		t.Fatalf("altered evidence: %+v %v", out, err)
	}
	if _, _, err := cancels.Inspect("group-p-001"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("unexpected durable intent")
	}
}
func TestSupervisedCancelRejectsSimultaneousTriggerBeforeDelete(t *testing.T) {
	coord, subs, cancels, evidence, m := supervisedCancelFixture(t)
	evidence.SL.Status = "TRIGGERED"
	m.sl.Status = "TRIGGERED"
	_, err := SuperviseSiblingCancelOnce(context.Background(), coord, subs, cancels, m, DemoCredentials{APIKey: "demo", Secret: "test"}, "group-p-001", evidence)
	if err == nil || m.deleteCalls != 0 {
		t.Fatalf("simultaneous trigger: %v", err)
	}
}
