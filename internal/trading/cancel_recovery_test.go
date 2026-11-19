package trading

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"binance_trader/internal/exchange"
)

type cancelRecoveryReader struct {
	result exchange.DemoAlgoResult
	err    error
	calls  int
}

func (r *cancelRecoveryReader) QueryDemoAlgo(_ context.Context, _, _ string, id int64) (exchange.DemoAlgoResult, error) {
	r.calls++
	if r.err != nil {
		return exchange.DemoAlgoResult{}, r.err
	}
	out := r.result
	if out.AlgoID == 0 {
		out.AlgoID = id
	}
	return out, nil
}

func cancelObserved(status string) exchange.DemoAlgoResult {
	return exchange.DemoAlgoResult{
		AlgoID: 901, ClientAlgoID: "btp_groupA", Symbol: "BTCUSDT",
		Side: "SELL", PositionSide: "LONG", AlgoType: "CONDITIONAL",
		Type: "STOP_MARKET", WorkingType: "MARK_PRICE", Quantity: "0.01",
		Status: status,
	}
}

func attemptedCancelFixture(t *testing.T) (*CancelIntentStore, CancelIntent, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := NewCancelIntentStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	c, err := s.Plan("groupA", 901, "btp_groupA")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.MarkMayHaveSent(c); err != nil {
		t.Fatal(err)
	}
	return s, c, dir
}

func TestRecoverAttemptedCancelActiveNeverReplays(t *testing.T) {
	s, _, _ := attemptedCancelFixture(t)
	reader := &cancelRecoveryReader{result: cancelObserved("WORKING")}
	r, err := s.RecoverAttemptedCancel(context.Background(), reader, "k", "s", "groupA")
	if err != nil {
		t.Fatal(err)
	}
	if r.State != CancelRecoveryPendingActive || r.ReplayAllowed || r.TradingAuthorized || reader.calls != 1 {
		t.Fatalf("unsafe active recovery: %+v calls=%d", r, reader.calls)
	}
	if _, attempted, err := s.Inspect("groupA"); err != nil || !attempted {
		t.Fatalf("MAY_HAVE_SENT lost: attempted=%v err=%v", attempted, err)
	}
}

func TestRecoverAttemptedCancelCanceledPersistsImmutableOutcome(t *testing.T) {
	s, _, dir := attemptedCancelFixture(t)
	reader := &cancelRecoveryReader{result: cancelObserved("CANCELED")}
	r, err := s.RecoverAttemptedCancel(context.Background(), reader, "k", "s", "groupA")
	if err != nil || r.State != CancelRecoveryCanceled || r.ReplayAllowed || r.TradingAuthorized {
		t.Fatalf("cancel recovery failed: %+v %v", r, err)
	}
	path := filepath.Join(dir, "trading", "algo_cancel_intents", "groupA.outcome.json")
	raw, err := os.ReadFile(path)
	if err != nil || len(raw) == 0 {
		t.Fatalf("missing outcome: %v", err)
	}

	// Same signed terminal observation is idempotent.
	if r2, err := s.RecoverAttemptedCancel(context.Background(), reader, "k", "s", "groupA"); err != nil || r2.State != CancelRecoveryCanceled {
		t.Fatalf("terminal reread not idempotent: %+v %v", r2, err)
	}

	// A conflicting later terminal state cannot overwrite historical evidence.
	reader.result = cancelObserved("FINISHED")
	if r3, err := s.RecoverAttemptedCancel(context.Background(), reader, "k", "s", "groupA"); err == nil || r3.State != CancelRecoveryHalt {
		t.Fatalf("conflicting terminal evidence accepted: %+v %v", r3, err)
	}
}

func TestRecoverAttemptedCancelTriggeredRequiresFillReconciliation(t *testing.T) {
	s, _, _ := attemptedCancelFixture(t)
	o := cancelObserved("FINISHED")
	o.ActualOrderID = "778899"
	o.ActualQty = "0.004"
	reader := &cancelRecoveryReader{result: o}
	r, err := s.RecoverAttemptedCancel(context.Background(), reader, "k", "s", "groupA")
	if err != nil || r.State != CancelRecoveryTriggered || r.ActualOrderID != "778899" || r.ActualQty != "0.004" || r.ReplayAllowed {
		t.Fatalf("triggered sibling misclassified: %+v %v", r, err)
	}
}

func TestRecoverAttemptedCancelSignedReadFailureFailsClosed(t *testing.T) {
	s, _, _ := attemptedCancelFixture(t)
	reader := &cancelRecoveryReader{err: errors.New("timeout")}
	r, err := s.RecoverAttemptedCancel(context.Background(), reader, "k", "s", "groupA")
	if err == nil || r.State != CancelRecoveryHalt || r.ReplayAllowed || r.TradingAuthorized {
		t.Fatalf("read failure not fail-closed: %+v %v", r, err)
	}
}

func TestAttemptAlgoCancelRejectsActiveAdapterResult(t *testing.T) {
	s, _ := NewCancelIntentStore(t.TempDir())
	c, err := s.Plan("groupA", 901, "btp_groupA")
	if err != nil {
		t.Fatal(err)
	}
	api := &activeCancelAdapter{}
	if _, err := s.AttemptAlgoCancel(context.Background(), api, "k", "s", c); err == nil {
		t.Fatal("active adapter result accepted as successful cancellation")
	}
	if api.calls != 1 {
		t.Fatalf("unexpected calls %d", api.calls)
	}
	if _, err := s.AttemptAlgoCancel(context.Background(), api, "k", "s", c); !errors.Is(err, ErrCancelAlreadyAttempted) {
		t.Fatalf("ambiguous active result replayed: %v", err)
	}
}

type activeCancelAdapter struct{ calls int }

func (a *activeCancelAdapter) CancelDemoAlgoOnce(_ context.Context, _, _ string, id int64) (exchange.DemoAlgoResult, error) {
	a.calls++
	o := cancelObserved("WORKING")
	o.AlgoID = id
	return o, nil
}

func TestCancelOutcomeTamperBlocksInspect(t *testing.T) {
	s, _, dir := attemptedCancelFixture(t)
	reader := &cancelRecoveryReader{result: cancelObserved("CANCELED")}
	if _, err := s.RecoverAttemptedCancel(context.Background(), reader, "k", "s", "groupA"); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "trading", "algo_cancel_intents", "groupA.outcome.json")
	if err := os.WriteFile(path, []byte(`{"schema":1,"intent_digest":"tampered"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Inspect("groupA"); err == nil {
		t.Fatal("tampered terminal outcome accepted")
	}
}

func TestRecoverAttemptedCancelActiveThenTriggeredAcrossRestartNeverDeletesAgain(t *testing.T) {
	s, _, _ := attemptedCancelFixture(t)
	reader := &cancelRecoveryReader{result: cancelObserved("WORKING")}

	first, err := s.RecoverAttemptedCancel(context.Background(), reader, "k", "s", "groupA")
	if err != nil || first.State != CancelRecoveryPendingActive || first.ReplayAllowed || first.TradingAuthorized {
		t.Fatalf("active cancel recovery unsafe: %+v %v", first, err)
	}

	// Later signed evidence may reveal that the sibling actually triggered
	// while the original DELETE result was ambiguous. Recovery must classify it
	// as fill-reconciliation work, not as a cancellation to retry.
	o := cancelObserved("FINISHED")
	o.ActualOrderID = "880077"
	o.ActualQty = "0.003"
	reader.result = o
	second, err := s.RecoverAttemptedCancel(context.Background(), reader, "k", "s", "groupA")
	if err != nil || second.State != CancelRecoveryTriggered || second.ReplayAllowed || second.TradingAuthorized || second.ActualQty != "0.003" {
		t.Fatalf("triggered-after-active recovery unsafe: %+v %v", second, err)
	}

	// The terminal observation is immutable/idempotent across another restart.
	third, err := s.RecoverAttemptedCancel(context.Background(), reader, "k", "s", "groupA")
	if err != nil || third.State != CancelRecoveryTriggered || third.ReplayAllowed || third.TradingAuthorized {
		t.Fatalf("terminal restart recovery unsafe: %+v %v", third, err)
	}
}
