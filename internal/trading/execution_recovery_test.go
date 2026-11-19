package trading

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

type executionRecoveryReader struct {
	*readerOrders
	snapshot exchange.Snapshot
	fetches  int
}

func (r *executionRecoveryReader) Fetch(_ context.Context, _, _ string) (exchange.Snapshot, error) {
	r.fetches++
	r.snapshot.AsOf = time.Now().UTC().Format(time.RFC3339Nano)
	return r.snapshot, nil
}

func TestRecoverExecutionMayHaveSentToSettledWithoutReplay(t *testing.T) {
	root := t.TempDir()
	start := time.Now().UTC().Add(-2 * time.Second)
	intent := makeIntent("recover-full-001", Manual, "", Long, Enter, "0.001")
	coordinator, store, lifecycle, reservation := lifecycleReservation(t, root, intent, start)
	defer coordinator.Close()

	// Simulate the important crash/timeout ambiguity: the durable marker exists,
	// the exchange may have received the order, but the submit call returned no
	// trustworthy ACK. Recovery must never invoke an executor again.
	executor := &lifecycleExecutor{id: 8801, err: errors.New("transport timeout")}
	if _, err := lifecycle.DispatchOnce(context.Background(), reservation, executor, allow); err == nil {
		t.Fatal("expected ambiguous dispatch error")
	}
	if executor.calls != 1 {
		t.Fatalf("initial submit calls=%d", executor.calls)
	}

	rec, order := finalOrderForReservation(t, root, reservation, 8801, "FILLED", "0.001")
	reader := &executionRecoveryReader{
		readerOrders: &readerOrders{
			orders: map[string]exchange.OrderResult{reservation.ClientOrderID: order},
			trades: map[int64][]exchange.AccountTrade{
				8801: {receipt(rec, 880101, "0.001", "100000", "0.1", "0", time.Now().UTC().Add(-time.Second).UnixMilli())},
			},
		},
		snapshot: accountSnapshot(time.Now().UTC(), "0.001", "0"),
	}

	result, err := lifecycle.RecoverExecution(context.Background(), reservation, coordinator, store, reader, DemoCredentials{APIKey: "demo-key", Secret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	if result.InitialStage != ExecutionMayHaveSent || result.FinalStage != ExecutionSettled || !result.Progressed || result.ImportedFillCount != 1 || result.TradingAuthorized {
		t.Fatalf("unexpected recovery result: %+v", result)
	}
	if executor.calls != 1 {
		t.Fatalf("recovery replayed submit: %d", executor.calls)
	}
	current, err := lifecycle.Load(reservation)
	if err != nil || current.Stage != ExecutionSettled {
		t.Fatalf("lifecycle not settled: %+v %v", current, err)
	}
	if reader.fetches < 2 {
		t.Fatalf("expected fresh account evidence for reconcile+settle, fetches=%d", reader.fetches)
	}

	// Re-running a settled recovery is local/idempotent and still never submits.
	again, err := lifecycle.RecoverExecution(context.Background(), reservation, coordinator, store, reader, DemoCredentials{APIKey: "demo-key", Secret: "secret"})
	if err != nil || again.FinalStage != ExecutionSettled || executor.calls != 1 {
		t.Fatalf("idempotent recovery failed: %+v err=%v submits=%d", again, err, executor.calls)
	}
}

func TestRecoverExecutionNonTerminalStopsAtSignedObserved(t *testing.T) {
	root := t.TempDir()
	start := time.Now().UTC().Add(-time.Second)
	intent := makeIntent("recover-pending01", Manual, "", Long, Enter, "0.001")
	coordinator, store, lifecycle, reservation := lifecycleReservation(t, root, intent, start)
	defer coordinator.Close()

	executor := &lifecycleExecutor{id: 8802, err: errors.New("timeout")}
	_, _ = lifecycle.DispatchOnce(context.Background(), reservation, executor, allow)
	rec, order := finalOrderForReservation(t, root, reservation, 8802, "NEW", "0")
	_ = rec
	reader := &executionRecoveryReader{
		readerOrders: &readerOrders{orders: map[string]exchange.OrderResult{reservation.ClientOrderID: order}, trades: map[int64][]exchange.AccountTrade{8802: {}}},
		snapshot:     accountSnapshot(time.Now().UTC(), "0", "0"),
	}

	result, err := lifecycle.RecoverExecution(context.Background(), reservation, coordinator, store, reader, DemoCredentials{APIKey: "demo-key", Secret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	if result.FinalStage != ExecutionSignedObserved || !strings.Contains(result.PendingReason, "아직 종료") || !result.Progressed {
		t.Fatalf("unexpected pending result: %+v", result)
	}
	if executor.calls != 1 {
		t.Fatalf("pending recovery replayed submit: %d", executor.calls)
	}
}

func TestRecoverExecutionMissingSignedOrderFailsClosedAtMayHaveSent(t *testing.T) {
	root := t.TempDir()
	start := time.Now().UTC().Add(-time.Second)
	intent := makeIntent("recover-missing01", Manual, "", Long, Enter, "0.001")
	coordinator, store, lifecycle, reservation := lifecycleReservation(t, root, intent, start)
	defer coordinator.Close()

	executor := &lifecycleExecutor{id: 8803, err: errors.New("timeout")}
	_, _ = lifecycle.DispatchOnce(context.Background(), reservation, executor, allow)
	reader := &executionRecoveryReader{
		readerOrders: &readerOrders{orders: map[string]exchange.OrderResult{}, trades: map[int64][]exchange.AccountTrade{}, fail: true},
		snapshot:     accountSnapshot(time.Now().UTC(), "0", "0"),
	}

	result, err := lifecycle.RecoverExecution(context.Background(), reservation, coordinator, store, reader, DemoCredentials{APIKey: "demo-key", Secret: "secret"})
	if err == nil || !strings.Contains(err.Error(), "자동 재전송 금지") {
		t.Fatalf("missing order was not fail-closed: %+v %v", result, err)
	}
	current, loadErr := lifecycle.Load(reservation)
	if loadErr != nil || current.Stage != ExecutionMayHaveSent || executor.calls != 1 {
		t.Fatalf("ambiguous stage changed/replayed: stage=%s load=%v submits=%d", current.Stage, loadErr, executor.calls)
	}
}
