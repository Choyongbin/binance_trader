package trading

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

// An external close can make the exchange side flat while the original owned
// entry must still be journaled. The entry-only recovery must NOT interpret the
// flat account as license to settle/consume the original reservation.
func TestRecoverOwnedEntryFillsOnlyLongShortRestartAndNoWholeSideSettlement(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			root := t.TempDir()
			start := time.Now().UTC().Add(-3 * time.Second)
			intent := makeIntent("recover-entry-only", Model, "test_model", side, Enter, "0.001")
			coordinator, store, lifecycle, res := lifecycleReservation(t, root, intent, start)
			defer coordinator.Close()
			executor := &lifecycleExecutor{id: 88991, err: errors.New("ACK timeout")}
			_, _ = lifecycle.DispatchOnce(context.Background(), res, executor, allow)
			rec, order := finalOrderForReservation(t, root, res, 88991, "FILLED", "0.001")
			reader := &readerOrders{
				orders: map[string]exchange.OrderResult{res.ClientOrderID: order},
				trades: map[int64][]exchange.AccountTrade{
					88991: {receipt(rec, 8899101, "0.001", "100000", "0.1", "0", time.Now().UTC().Add(-time.Second).UnixMilli())},
				},
			}
			before, err := store.readFills()
			if err != nil || len(before) != 0 {
				t.Fatalf("unexpected initial fills: %v %v", before, err)
			}
			out, err := lifecycle.RecoverOwnedEntryFillsOnly(context.Background(), res, coordinator, store, reader, DemoCredentials{APIKey: "demo", Secret: "secret"})
			if err != nil {
				t.Fatal(err)
			}
			if out.InitialStage != ExecutionMayHaveSent || out.FinalStage != ExecutionFillsImported || out.ImportedFillCount != 1 || !out.Progressed || out.TradingAuthorized || !strings.Contains(out.PendingReason, "거래 잠금 유지") {
				t.Fatalf("unexpected unsafe recovery: %+v", out)
			}
			actual, err := store.readFills()
			if err != nil || len(actual) != 1 || actual[0].Trade.TradeID != 8899101 || actual[0].ClientOrderID != res.ClientOrderID {
				t.Fatalf("missing original fill: %+v %v", actual, err)
			}
			if executor.calls != 1 {
				t.Fatal("unexpected exchange POST replay")
			}
			life, err := lifecycle.Load(res)
			if err != nil || life.Stage != ExecutionFillsImported {
				t.Fatalf("unsafe lifecycle stage: %+v %v", life, err)
			}
			// Reopen the stores after a simulated restart. No duplicate fills,
			// account query, settlement marker, or exchange submission.
			reopen, err := NewExecutionLifecycleStore(root)
			if err != nil {
				t.Fatal(err)
			}
			freshStore, err := NewOwnershipStore(root)
			if err != nil {
				t.Fatal(err)
			}
			again, err := reopen.RecoverOwnedEntryFillsOnly(context.Background(), res, coordinator, freshStore, reader, DemoCredentials{APIKey: "demo", Secret: "secret"})
			if err != nil || again.FinalStage != ExecutionFillsImported || again.ImportedFillCount != 0 || again.Progressed || executor.calls != 1 {
				t.Fatalf("restart unsafe: %+v %v", again, err)
			}
			after, err := freshStore.readFills()
			if err != nil || len(after) != 1 {
				t.Fatalf("duplicate fill after restart: %+v %v", after, err)
			}
			// There must be no done marker for the ownership reservation.
			entries, err := filepath.Glob(filepath.Join(root, "trading", "reservations_done", "*.json"))
			if err != nil || len(entries) != 0 {
				t.Fatalf("finalized reservation: %+v %v", entries, err)
			}
		})
	}
}

func TestRecoverOwnedEntryFillsOnlyRefusesMissingSignedAndExit(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC().Add(-2 * time.Second)
	intent := makeIntent("recover-only-bad", Manual, "", Short, Enter, "0.001")
	c, store, lifecycle, r := lifecycleReservation(t, root, intent, now)
	defer c.Close()
	_, _ = lifecycle.DispatchOnce(context.Background(), r, &lifecycleExecutor{id: 88099, err: errors.New("ACK lost")}, allow)
	missing := &readerOrders{orders: map[string]exchange.OrderResult{}, trades: map[int64][]exchange.AccountTrade{}, fail: true}
	result, err := lifecycle.RecoverOwnedEntryFillsOnly(context.Background(), r, c, store, missing, DemoCredentials{APIKey: "demo", Secret: "secret"})
	if err == nil || result.TradingAuthorized {
		t.Fatalf("missing signed evidence accepted: %+v %v", result, err)
	}
	got, err := lifecycle.Load(r)
	if err != nil || got.Stage != ExecutionMayHaveSent {
		t.Fatalf("modified unresolved lifecycle: %+v %v", got, err)
	}
	fills, err := store.readFills()
	if err != nil || len(fills) != 0 {
		t.Fatalf("modified fills despite missing evidence: %d %v", len(fills), err)
	}
	bad := r
	bad.Intent.Action = Exit
	if _, err := lifecycle.RecoverOwnedEntryFillsOnly(context.Background(), bad, c, store, missing, DemoCredentials{APIKey: "demo", Secret: "secret"}); err == nil {
		t.Fatal("accepted non-entry for entry-only recovery")
	}
}

func TestRecoverOwnedEntryFillsOnlyResumesCrashAfterSignedImport(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC().Add(-3 * time.Second)
	intent := makeIntent("recover-partial", Manual, "", Short, Enter, "0.001")
	c, store, lifecycle, r := lifecycleReservation(t, root, intent, now)
	defer c.Close()
	_, _ = lifecycle.DispatchOnce(context.Background(), r, &lifecycleExecutor{id: 88221, err: errors.New("ACK missing")}, allow)
	rec, order := finalOrderForReservation(t, root, r, 88221, "FILLED", "0.001")
	reader := &readerOrders{
		orders: map[string]exchange.OrderResult{r.ClientOrderID: order},
		trades: map[int64][]exchange.AccountTrade{88221: {receipt(rec, 882211, "0.001", "100000", "0.1", "0", time.Now().UTC().Add(-time.Second).UnixMilli())}},
	}
	// Simulate crash AFTER signed observation and fill O_EXCL persistence,
	// but BEFORE FILLS_IMPORTED lifecycle sidecar is committed.
	evidence, err := FetchSignedOrderEvidence(context.Background(), reader, DemoCredentials{APIKey: "demo", Secret: "secret"}, rec)
	if err != nil {
		t.Fatal(err)
	}
	_, err = lifecycle.RecordSignedObservation(r, OrderObservation{Order: evidence.After, CheckedAt: evidence.AfterReadAt}, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	n, err := store.ImportDemoOrder(context.Background(), reader, DemoCredentials{APIKey: "demo", Secret: "secret"}, r.Intent)
	if err != nil || n != 1 {
		t.Fatalf("failed to make crash fixture: %d %v", n, err)
	}
	resumed, err := NewExecutionLifecycleStore(root)
	if err != nil {
		t.Fatal(err)
	}
	out, err := resumed.RecoverOwnedEntryFillsOnly(context.Background(), r, c, store, reader, DemoCredentials{APIKey: "demo", Secret: "secret"})
	if err != nil || out.FinalStage != ExecutionFillsImported || out.ImportedFillCount != 0 || out.TradingAuthorized || !out.Progressed {
		t.Fatalf("failed crash recovery: %+v %v", out, err)
	}
	fills, err := store.readFills()
	if err != nil || len(fills) != 1 {
		t.Fatalf("fill duplicated: %+v %v", fills, err)
	}
}

// Contrast the explicitly limited entry repair with full-account recovery.
// The external close makes the Binance side flat, but no corresponding owned
// close fill exists, so the original lifecycle must not be finalized.
func TestRecoverEntryFillsOnlyKeepsFlatExternalCloseUnsettled(t *testing.T) {
	root := t.TempDir()
	intent := makeIntent("external-flat-repair", Manual, "", Short, Enter, "0.001")
	c, store, lifecycle, r := lifecycleReservation(t, root, intent, time.Now().UTC().Add(-3*time.Second))
	defer c.Close()
	_, _ = lifecycle.DispatchOnce(context.Background(), r, &lifecycleExecutor{id: 99111, err: errors.New("ACK lost")}, allow)
	rec, order := finalOrderForReservation(t, root, r, 99111, "FILLED", "0.001")
	reader := &executionRecoveryReader{
		readerOrders: &readerOrders{
			orders: map[string]exchange.OrderResult{r.ClientOrderID: order},
			trades: map[int64][]exchange.AccountTrade{99111: {receipt(rec, 991111, "0.001", "100000", "0.1", "0", time.Now().UTC().Add(-time.Second).UnixMilli())}},
		},
		snapshot: accountSnapshot(time.Now().UTC(), "0", "0"),
	}
	out, err := lifecycle.RecoverOwnedEntryFillsOnly(context.Background(), r, c, store, reader, DemoCredentials{APIKey: "demo", Secret: "secret"})
	if err != nil || out.FinalStage != ExecutionFillsImported || reader.fetches != 0 || out.TradingAuthorized {
		t.Fatalf("entry-only unexpectedly reconciled flat account: %+v %v", out, err)
	}
	_, err = lifecycle.RecoverExecution(context.Background(), r, c, store, reader, DemoCredentials{APIKey: "demo", Secret: "secret"})
	if err == nil {
		t.Fatal("external flat account erroneously settled unmatched owned entry")
	}
	state, err := lifecycle.Load(r)
	if err != nil || state.Stage != ExecutionFillsImported {
		t.Fatalf("external close incorrectly marked settled: %+v %v", state, err)
	}
}
