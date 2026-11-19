package trading

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

// TestExecutionFailureInjectionPartialThenTerminalCrashRecovery exercises the
// restart boundary where a MAY_HAVE_SENT order is first observed partial,
// partial fills are durably imported, and only later becomes terminal. Every
// recovery pass is read-only with respect to order submission and must remain
// safe to repeat after a crash between any two local stages.
func TestExecutionFailureInjectionPartialThenTerminalCrashRecovery(t *testing.T) {
	root := t.TempDir()
	start := time.Now().UTC().Add(-2 * time.Second)
	intent := makeIntent("recover-partial-crash-001", Manual, "", Long, Enter, "0.001")
	coordinator, store, lifecycle, reservation := lifecycleReservation(t, root, intent, start)
	defer coordinator.Close()

	executor := &lifecycleExecutor{id: 9901, err: errors.New("injected post timeout")}
	if _, err := lifecycle.DispatchOnce(context.Background(), reservation, executor, allow); err == nil {
		t.Fatal("expected ambiguous dispatch")
	}
	if executor.calls != 1 {
		t.Fatalf("initial submit calls=%d", executor.calls)
	}

	rec, partial := finalOrderForReservation(t, root, reservation, 9901, "PARTIALLY_FILLED", "0.0004")
	trade := receipt(rec, 990101, "0.0004", "100000", "0.04", "0", time.Now().UTC().Add(-time.Second).UnixMilli())
	reader := &executionRecoveryReader{
		readerOrders: &readerOrders{
			orders: map[string]exchange.OrderResult{reservation.ClientOrderID: partial},
			trades: map[int64][]exchange.AccountTrade{9901: {trade}},
		},
		snapshot: accountSnapshot(time.Now().UTC(), "0.0004", "0"),
	}

	// First restart: identify the remote order, but do not freeze fills while
	// the order is still live.
	first, err := lifecycle.RecoverExecution(context.Background(), reservation, coordinator, store, reader, DemoCredentials{APIKey: "demo-key", Secret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	if first.FinalStage != ExecutionSignedObserved || !strings.Contains(first.PendingReason, "아직 종료") {
		t.Fatalf("unexpected first partial recovery: %+v", first)
	}
	if executor.calls != 1 {
		t.Fatalf("recovery replayed submit: %d", executor.calls)
	}

	// Second restart: importing the currently-known partial fill is allowed,
	// but lifecycle must remain SIGNED_OBSERVED until a terminal signed GET.
	second, err := lifecycle.RecoverExecution(context.Background(), reservation, coordinator, store, reader, DemoCredentials{APIKey: "demo-key", Secret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	if second.FinalStage != ExecutionSignedObserved || second.ImportedFillCount != 1 || !second.Progressed {
		t.Fatalf("partial import did not remain pending: %+v", second)
	}
	if executor.calls != 1 {
		t.Fatalf("second recovery replayed submit: %d", executor.calls)
	}

	// Simulate another crash and then a terminal cancel with the same partial
	// execution. Import must be idempotent and the lifecycle can now settle.
	terminal := partial
	terminal.Status = "CANCELED"
	reader.orders[reservation.ClientOrderID] = terminal
	reader.snapshot = accountSnapshot(time.Now().UTC(), "0.0004", "0")

	third, err := lifecycle.RecoverExecution(context.Background(), reservation, coordinator, store, reader, DemoCredentials{APIKey: "demo-key", Secret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	if third.FinalStage != ExecutionSettled || !third.Progressed || third.ImportedFillCount != 0 || third.TradingAuthorized {
		t.Fatalf("terminal partial recovery failed: %+v", third)
	}
	if executor.calls != 1 {
		t.Fatalf("terminal recovery replayed submit: %d", executor.calls)
	}

	// A fourth process restart is fully idempotent.
	fourth, err := lifecycle.RecoverExecution(context.Background(), reservation, coordinator, store, reader, DemoCredentials{APIKey: "demo-key", Secret: "secret"})
	if err != nil || fourth.FinalStage != ExecutionSettled || executor.calls != 1 {
		t.Fatalf("settled replay boundary failed: %+v err=%v submits=%d", fourth, err, executor.calls)
	}
}

func TestExecutionFailureInjectionPartialEvidenceCannotRegressExecutedQty(t *testing.T) {
	root := t.TempDir()
	start := time.Now().UTC().Add(-2 * time.Second)
	intent := makeIntent("recover-partial-regress-001", Manual, "", Long, Enter, "0.001")
	coordinator, store, lifecycle, reservation := lifecycleReservation(t, root, intent, start)
	defer coordinator.Close()

	executor := &lifecycleExecutor{id: 9902, err: errors.New("injected post timeout")}
	_, _ = lifecycle.DispatchOnce(context.Background(), reservation, executor, allow)
	rec, observed := finalOrderForReservation(t, root, reservation, 9902, "PARTIALLY_FILLED", "0.0006")
	reader := &executionRecoveryReader{
		readerOrders: &readerOrders{orders: map[string]exchange.OrderResult{reservation.ClientOrderID: observed}, trades: map[int64][]exchange.AccountTrade{9902: {receipt(rec, 990201, "0.0006", "100000", "0.06", "0", time.Now().UTC().Add(-time.Second).UnixMilli())}}},
		snapshot:     accountSnapshot(time.Now().UTC(), "0", "0"),
	}
	first, err := lifecycle.RecoverExecution(context.Background(), reservation, coordinator, store, reader, DemoCredentials{APIKey: "demo-key", Secret: "secret"})
	if err != nil || first.FinalStage != ExecutionSignedObserved {
		t.Fatalf("initial partial observation failed: %+v %v", first, err)
	}

	// A later signed GET that claims a smaller cumulative execution cannot be
	// accepted as a harmless retry. Import/reconciliation must fail closed.
	regressed := observed
	regressed.Status = "CANCELED"
	regressed.ExecutedQty = "0.0004"
	reader.orders[reservation.ClientOrderID] = regressed
	if _, err := lifecycle.RecoverExecution(context.Background(), reservation, coordinator, store, reader, DemoCredentials{APIKey: "demo-key", Secret: "secret"}); err == nil {
		t.Fatal("regressed cumulative execution was accepted")
	}
	current, loadErr := lifecycle.Load(reservation)
	if loadErr != nil || current.Stage != ExecutionSignedObserved || executor.calls != 1 {
		t.Fatalf("regression changed lifecycle or replayed submit: stage=%s load=%v submits=%d", current.Stage, loadErr, executor.calls)
	}
}
