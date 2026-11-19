package trading

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"binance_trader/internal/exchange"
	"binance_trader/internal/model"
)

// Exercise the real runtime->signed lifecycle recovery composition with a
// completely offline Binance mock. The executor is deliberately NOT exposed
// to ReconcileDispatched and must never be invoked during recovery.
func TestAutoRuntimeReconcileDispatchedFromMayHaveSent(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC().Add(-3 * time.Second)
	intent := makeIntent("recover-model-auto-01", Model, model.TestStrategyID, Long, Enter, "0.001")
	coordinator, store, lifecycle, reservation := lifecycleReservation(t, root, intent, now)
	defer coordinator.Close()
	executor := &lifecycleExecutor{id: 9321, err: errors.New("simulated ACK lost")}
	if _, err := lifecycle.DispatchOnce(context.Background(), reservation, executor, allow); err == nil {
		t.Fatal("expected lost ACK")
	}
	if executor.calls != 1 {
		t.Fatal("test did not simulate a single submission")
	}
	rec, order := finalOrderForReservation(t, root, reservation, 9321, "FILLED", "0.001")
	reader := &executionRecoveryReader{
		readerOrders: &readerOrders{
			orders: map[string]exchange.OrderResult{reservation.ClientOrderID: order},
			trades: map[int64][]exchange.AccountTrade{9321: {receipt(rec, 932101, "0.001", "100000", "0.1", "0", time.Now().UTC().Add(-time.Second).UnixMilli())}},
		},
		snapshot: accountSnapshot(time.Now().UTC(), "0.001", "0"),
	}
	rt := &AutoExecutionRuntime{Path: &AutoExecutionPath{modelID: model.TestStrategyID, coordinator: coordinator, lifecycle: lifecycle, ownership: store}, coordinator: coordinator}
	outcome, err := rt.ReconcileDispatched(context.Background(), reservation.ClientOrderID, reader, DemoCredentials{APIKey: "fake-key", Secret: "fake-secret"})
	if err != nil || outcome.FinalStage != ExecutionSettled || !outcome.Progressed || outcome.TradingAuthorized || executor.calls != 1 {
		t.Fatalf("failed signed-only reconciliation: %+v err=%v executor calls=%d", outcome, err, executor.calls)
	}
	// Run again: persistent replay remains idempotent.
	again, err := rt.ReconcileDispatched(context.Background(), reservation.ClientOrderID, reader, DemoCredentials{APIKey: "fake-key", Secret: "fake-secret"})
	if err != nil || again.FinalStage != ExecutionSettled || executor.calls != 1 {
		t.Fatalf("idempotent runtime recovery failed: %+v %v", again, err)
	}
	if _, err := rt.ReconcileDispatched(context.Background(), "bt_unknown", reader, DemoCredentials{APIKey: "fake-key", Secret: "fake-secret"}); err == nil || !strings.Contains(err.Error(), "reservation") {
		t.Fatalf("missing reservation accepted: %v", err)
	}
}
