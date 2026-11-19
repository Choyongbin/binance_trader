package trading

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

type lifecycleExecutor struct {
	calls     int
	id        int64
	err       error
	malformed bool
}

func (e *lifecycleExecutor) Submit(_ context.Context, p PlannedOrder) (Submission, error) {
	e.calls++
	if e.err != nil {
		return Submission{}, e.err
	}
	if e.malformed {
		return Submission{ClientOrderID: "wrong", OrderID: e.id, Status: "NEW"}, nil
	}
	return Submission{ClientOrderID: p.ClientOrderID, OrderID: e.id, Status: "NEW"}, nil
}

func (e *lifecycleExecutor) durableFinalGuardContract() {}

func (e *lifecycleExecutor) SubmitWithFinalCheck(ctx context.Context, p PlannedOrder, finalCheck func(context.Context) error) (Submission, error) {
	if finalCheck == nil {
		return Submission{}, errors.New("missing final check")
	}
	if err := finalCheck(ctx); err != nil {
		return Submission{}, err
	}
	return e.Submit(ctx, p)
}

func lifecycleReservation(t *testing.T, root string, intent Intent, now time.Time) (*Coordinator, *OwnershipStore, *ExecutionLifecycleStore, Reservation) {
	t.Helper()
	store, err := NewOwnershipStore(root)
	if err != nil {
		t.Fatal(err)
	}
	c, err := OpenCoordinator(root)
	if err != nil {
		t.Fatal(err)
	}
	basis := cleanReservationBasis(now)
	res, _, err := c.reservePreview(intent, basis, now)
	if err != nil {
		c.Close()
		t.Fatal(err)
	}
	lifecycle, err := NewExecutionLifecycleStore(root)
	if err != nil {
		c.Close()
		t.Fatal(err)
	}
	got, err := lifecycle.Begin(res, now)
	if err != nil || got.Stage != ExecutionReserved {
		c.Close()
		t.Fatalf("begin lifecycle %+v %v", got, err)
	}
	return c, store, lifecycle, res
}

func finalOrderForReservation(t *testing.T, root string, r Reservation, orderID int64, status, executed string) (Record, exchange.OrderResult) {
	t.Helper()
	rec, err := readRecord(filepath.Join(root, "trading", "requests", r.ClientOrderID+".json"))
	if err != nil {
		t.Fatal(err)
	}
	rec.ExchangeOrderID = orderID
	return rec, exchange.OrderResult{
		Symbol: r.Intent.Symbol, ClientOrderID: r.ClientOrderID, OrderID: orderID,
		Side: rec.Plan.ExchangeSide, PositionSide: string(rec.Plan.PositionSide), Type: string(rec.Plan.Type),
		Status: status, OriginalQty: rec.Plan.Quantity, ExecutedQty: executed,
	}
}

func TestExecutionLifecycleDispatchMarkerPreventsReplayAcrossRestart(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC().Add(-time.Second)
	c, _, lifecycle, res := lifecycleReservation(t, root, makeIntent("life-dispatch01", Manual, "", Long, Enter, "0.001"), now)
	defer c.Close()

	exec := &lifecycleExecutor{id: 701}
	if _, err := lifecycle.DispatchOnce(context.Background(), res, exec, allow); err != nil {
		t.Fatal(err)
	}
	if exec.calls != 1 {
		t.Fatalf("first dispatch count=%d", exec.calls)
	}
	current, err := lifecycle.Load(res)
	if err != nil || current.Stage != ExecutionMayHaveSent {
		t.Fatalf("stage %+v %v", current, err)
	}

	reopened, err := NewExecutionLifecycleStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.DispatchOnce(context.Background(), res, exec, allow); !errors.Is(err, ErrExecutionDispatchAlreadyMarked) {
		t.Fatalf("restart dispatch was not blocked: %v", err)
	}
	if exec.calls != 1 {
		t.Fatalf("duplicate submit after restart: %d", exec.calls)
	}
}

func TestExecutionLifecycleAmbiguousSubmitNeverReplays(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC().Add(-time.Second)
	c, _, lifecycle, res := lifecycleReservation(t, root, makeIntent("life-timeout001", Manual, "", Long, Enter, "0.001"), now)
	defer c.Close()
	exec := &lifecycleExecutor{id: 702, err: errors.New("transport timeout")}
	if _, err := lifecycle.DispatchOnce(context.Background(), res, exec, allow); err == nil || !strings.Contains(err.Error(), "자동 재전송 금지") {
		t.Fatalf("ambiguous result not retained: %v", err)
	}
	if exec.calls != 1 {
		t.Fatal(exec.calls)
	}
	current, err := lifecycle.Load(res)
	if err != nil || current.Stage != ExecutionMayHaveSent {
		t.Fatalf("unexpected ambiguous stage %+v %v", current, err)
	}
	if _, err := lifecycle.DispatchOnce(context.Background(), res, exec, allow); !errors.Is(err, ErrExecutionDispatchAlreadyMarked) {
		t.Fatalf("ambiguous retry not blocked: %v", err)
	}
	if exec.calls != 1 {
		t.Fatalf("ambiguous order replayed: %d", exec.calls)
	}
}

func TestExecutionLifecycleFullLinearSettlement(t *testing.T) {
	root := t.TempDir()
	start := time.Now().UTC().Add(-2 * time.Second)
	intent := makeIntent("life-complete01", Manual, "", Long, Enter, "0.001")
	c, store, lifecycle, res := lifecycleReservation(t, root, intent, start)
	defer c.Close()
	exec := &lifecycleExecutor{id: 703}
	if _, err := lifecycle.DispatchOnce(context.Background(), res, exec, allow); err != nil {
		t.Fatal(err)
	}

	reader := &readerOrders{orders: map[string]exchange.OrderResult{}, trades: map[int64][]exchange.AccountTrade{}}
	rec, order := finalOrderForReservation(t, root, res, 703, "FILLED", "0.001")
	reader.orders[res.ClientOrderID] = order
	reader.trades[703] = []exchange.AccountTrade{receipt(rec, 70301, "0.001", "100000", "0.1", "0", time.Now().UTC().Add(-time.Second).UnixMilli())}

	observedAt := time.Now().UTC()
	obs := OrderObservation{Order: order, CheckedAt: observedAt}
	current, err := lifecycle.RecordSignedObservation(res, obs, observedAt)
	if err != nil || current.Stage != ExecutionSignedObserved {
		t.Fatalf("signed stage %+v %v", current, err)
	}
	added, err := store.ImportDemoOrder(context.Background(), reader, DemoCredentials{APIKey: "demo-key", Secret: "secret"}, intent)
	if err != nil || added != 1 {
		t.Fatalf("import fills added=%d err=%v", added, err)
	}
	fillsAt := time.Now().UTC()
	obs.CheckedAt = fillsAt
	current, err = lifecycle.RecordFillsImported(res, store, obs, fillsAt)
	if err != nil || current.Stage != ExecutionFillsImported || current.Events[len(current.Events)-1].ImportedFillCount != 1 {
		t.Fatalf("fills stage %+v %v", current, err)
	}

	reconcileAt := time.Now().UTC()
	snapshot := accountSnapshot(reconcileAt, "0.001", "0")
	orders := map[string]OrderObservation{res.ClientOrderID: {Order: order, CheckedAt: reconcileAt}}
	current, err = lifecycle.RecordWholeSideReconciled(res, store, snapshot, orders, reconcileAt)
	if err != nil || current.Stage != ExecutionWholeReconciled {
		t.Fatalf("whole stage %+v %v", current, err)
	}
	finalAt := time.Now().UTC()
	snapshot.AsOf = finalAt.Format(time.RFC3339Nano)
	orders[res.ClientOrderID] = OrderObservation{Order: order, CheckedAt: finalAt}
	current, err = lifecycle.Finalize(res, c, store, snapshot, orders, finalAt)
	if err != nil || current.Stage != ExecutionSettled || len(current.Events) != 5 {
		t.Fatalf("settled %+v %v", current, err)
	}
	if current.Events[0].Stage != ExecutionMayHaveSent || current.Events[1].Stage != ExecutionSignedObserved || current.Events[2].Stage != ExecutionFillsImported || current.Events[3].Stage != ExecutionWholeReconciled || current.Events[4].Stage != ExecutionSettled {
		t.Fatalf("wrong lifecycle sequence %+v", current.Events)
	}
	audit, err := ReviewExecutionLifecycles(root)
	if err != nil || audit.State != ExecutionAuditClear || audit.Total != 1 || audit.Settled != 1 || audit.Pending != 0 || audit.TradingAuthorized {
		t.Fatalf("audit %+v %v", audit, err)
	}
}

func TestExecutionLifecycleRequiresTerminalOrderBeforeFillFreeze(t *testing.T) {
	root := t.TempDir()
	start := time.Now().UTC().Add(-time.Second)
	c, store, lifecycle, res := lifecycleReservation(t, root, makeIntent("life-openord001", Manual, "", Long, Enter, "0.001"), start)
	defer c.Close()
	exec := &lifecycleExecutor{id: 704}
	if _, err := lifecycle.DispatchOnce(context.Background(), res, exec, allow); err != nil {
		t.Fatal(err)
	}
	_, order := finalOrderForReservation(t, root, res, 704, "NEW", "0")
	now := time.Now().UTC()
	obs := OrderObservation{Order: order, CheckedAt: now}
	if _, err := lifecycle.RecordSignedObservation(res, obs, now); err != nil {
		t.Fatal(err)
	}
	if _, err := lifecycle.RecordFillsImported(res, store, obs, now); err == nil || !strings.Contains(err.Error(), "아직 종료되지") {
		t.Fatalf("open order was frozen as complete: %v", err)
	}
}

func TestExecutionLifecycleCrashAfterDoneKeepsSideFencedUntilRecoverSettled(t *testing.T) {
	root := t.TempDir()
	start := time.Now().UTC().Add(-2 * time.Second)
	intent := makeIntent("life-crashdone1", Manual, "", Long, Enter, "0.001")
	c, store, lifecycle, res := lifecycleReservation(t, root, intent, start)
	defer c.Close()
	exec := &lifecycleExecutor{id: 705}
	if _, err := lifecycle.DispatchOnce(context.Background(), res, exec, allow); err != nil {
		t.Fatal(err)
	}
	reader := &readerOrders{orders: map[string]exchange.OrderResult{}, trades: map[int64][]exchange.AccountTrade{}}
	rec, order := finalOrderForReservation(t, root, res, 705, "FILLED", "0.001")
	reader.orders[res.ClientOrderID] = order
	reader.trades[705] = []exchange.AccountTrade{receipt(rec, 70501, "0.001", "100000", "0.1", "0", time.Now().UTC().Add(-time.Second).UnixMilli())}
	now := time.Now().UTC()
	if _, err := lifecycle.RecordSignedObservation(res, OrderObservation{Order: order, CheckedAt: now}, now); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ImportDemoOrder(context.Background(), reader, DemoCredentials{APIKey: "demo-key", Secret: "secret"}, intent); err != nil {
		t.Fatal(err)
	}
	now = time.Now().UTC()
	if _, err := lifecycle.RecordFillsImported(res, store, OrderObservation{Order: order, CheckedAt: now}, now); err != nil {
		t.Fatal(err)
	}
	now = time.Now().UTC()
	snap := accountSnapshot(now, "0.001", "0")
	snap.Balances = []exchange.Balance{{Asset: "USDT", Wallet: "1000", Available: "1000"}}
	orders := map[string]OrderObservation{res.ClientOrderID: {Order: order, CheckedAt: now}}
	if _, err := lifecycle.RecordWholeSideReconciled(res, store, snap, orders, now); err != nil {
		t.Fatal(err)
	}

	// Simulate the crash window: Coordinator.Settle persists the sealed done
	// marker, but SETTLED lifecycle sidecar is not yet written.
	settleAt := time.Now().UTC()
	snap.AsOf = settleAt.Format(time.RFC3339Nano)
	orders[res.ClientOrderID] = OrderObservation{Order: order, CheckedAt: settleAt}
	if err := c.Settle(res, store, snap, orders, settleAt); err != nil {
		t.Fatal(err)
	}
	report, err := store.Audit(snap, orders, settleAt)
	if err != nil || !report.ReadyForAssessment {
		t.Fatalf("post-settle audit %+v %v", report, err)
	}
	basis := ReservationBasis{Snapshot: snap, Audit: report, Quote: Quote{Symbol: "BTCUSDT", Price: "100000", ReceivedAt: settleAt}}
	if _, _, err := c.reservePreview(makeIntent("life-nextentry1", Manual, "", Long, Enter, "0.001"), basis, settleAt); !errors.Is(err, ErrExecutionLifecyclePending) {
		t.Fatalf("crash window released same-side fence: %v", err)
	}

	// The sealed done marker may contain a newer fresh audit than the earlier
	// WHOLE_SIDE checkpoint. Because Coordinator.Settle itself re-ran the full
	// audit after that checkpoint, restart recovery may close only this exact
	// fsync gap without any network replay.
	recovered, err := lifecycle.RecoverSettled(res, time.Now().UTC())
	if err != nil || recovered.Stage != ExecutionSettled {
		t.Fatalf("crash-window recovery failed: %+v %v", recovered, err)
	}
}

func TestExecutionLifecycleRecoverSettledExactEvidence(t *testing.T) {
	root := t.TempDir()
	start := time.Now().UTC().Add(-2 * time.Second)
	intent := makeIntent("life-recover001", Manual, "", Long, Enter, "0.001")
	c, store, lifecycle, res := lifecycleReservation(t, root, intent, start)
	defer c.Close()
	exec := &lifecycleExecutor{id: 706}
	if _, err := lifecycle.DispatchOnce(context.Background(), res, exec, allow); err != nil {
		t.Fatal(err)
	}
	reader := &readerOrders{orders: map[string]exchange.OrderResult{}, trades: map[int64][]exchange.AccountTrade{}}
	rec, order := finalOrderForReservation(t, root, res, 706, "FILLED", "0.001")
	reader.orders[res.ClientOrderID] = order
	reader.trades[706] = []exchange.AccountTrade{receipt(rec, 70601, "0.001", "100000", "0.1", "0", time.Now().UTC().Add(-time.Second).UnixMilli())}
	now := time.Now().UTC()
	if _, err := lifecycle.RecordSignedObservation(res, OrderObservation{Order: order, CheckedAt: now}, now); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ImportDemoOrder(context.Background(), reader, DemoCredentials{APIKey: "demo-key", Secret: "secret"}, intent); err != nil {
		t.Fatal(err)
	}
	now = time.Now().UTC()
	if _, err := lifecycle.RecordFillsImported(res, store, OrderObservation{Order: order, CheckedAt: now}, now); err != nil {
		t.Fatal(err)
	}

	reconcileAt := time.Now().UTC()
	snap := accountSnapshot(reconcileAt, "0.001", "0")
	snap.Balances = []exchange.Balance{{Asset: "USDT", Wallet: "1000", Available: "1000"}}
	orders := map[string]OrderObservation{res.ClientOrderID: {Order: order, CheckedAt: reconcileAt}}
	if _, err := lifecycle.RecordWholeSideReconciled(res, store, snap, orders, reconcileAt); err != nil {
		t.Fatal(err)
	}
	// Use the exact same snapshot/time for Settle so the sealed done marker is
	// byte-equivalent to the WHOLE_SIDE evidence.
	if err := c.Settle(res, store, snap, orders, reconcileAt); err != nil {
		t.Fatal(err)
	}
	recovered, err := lifecycle.RecoverSettled(res, time.Now().UTC())
	if err != nil || recovered.Stage != ExecutionSettled {
		t.Fatalf("recover settled %+v %v", recovered, err)
	}
	basisReport, err := store.Audit(snap, orders, reconcileAt)
	if err != nil || !basisReport.ReadyForAssessment {
		t.Fatal(err)
	}
	basis := ReservationBasis{Snapshot: snap, Audit: basisReport, Quote: Quote{Symbol: "BTCUSDT", Price: "100000", ReceivedAt: reconcileAt}}
	if _, _, err := c.reservePreview(makeIntent("life-nextentry2", Manual, "", Long, Enter, "0.001"), basis, reconcileAt); err != nil {
		t.Fatalf("settled lifecycle kept side fenced: %v", err)
	}
}

func TestExecutionLifecycleTamperAndOrphanFailClosed(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC().Add(-time.Second)
	c, _, lifecycle, res := lifecycleReservation(t, root, makeIntent("life-tamper001", Manual, "", Long, Enter, "0.001"), now)
	defer c.Close()
	if _, err := lifecycle.MarkMayHaveSent(res, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	path := executionEventPath(filepath.Join(root, "trading", "execution_lifecycle"), res.ClientOrderID, 1)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw = []byte(strings.Replace(string(raw), "MAY_HAVE_SENT", "SIGNED_OBSERVED", 1))
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := lifecycle.Load(res); err == nil {
		t.Fatal("tampered lifecycle accepted")
	}
	if _, err := ReviewExecutionLifecycles(root); err == nil {
		t.Fatal("tampered lifecycle audit accepted")
	}
}

func TestExecutionLifecycleAuditEmptyIsReadOnly(t *testing.T) {
	root := t.TempDir()
	review, err := ReviewExecutionLifecycles(root)
	if err != nil || review.State != ExecutionAuditEmpty || review.TradingAuthorized || !review.ManualReviewRequired || review.Total != 0 {
		t.Fatalf("empty review %+v %v", review, err)
	}
	if _, err := os.Stat(filepath.Join(root, "trading")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("read-only audit created data: %v", err)
	}
}

func TestExecutionLifecycleConcurrentDispatchOnlyOneExecutorCall(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC().Add(-time.Second)
	c, _, lifecycle, res := lifecycleReservation(t, root, makeIntent("life-concur0001", Manual, "", Long, Enter, "0.001"), now)
	defer c.Close()
	exec := &lifecycleExecutor{id: 707}
	results := make(chan error, 20)
	for i := 0; i < 20; i++ {
		go func() {
			_, err := lifecycle.DispatchOnce(context.Background(), res, exec, allow)
			results <- err
		}()
	}
	successes := 0
	for i := 0; i < 20; i++ {
		if err := <-results; err == nil {
			successes++
		}
	}
	if successes != 1 || exec.calls != 1 {
		t.Fatalf("successes=%d executor calls=%d", successes, exec.calls)
	}
}

func TestExecutionLifecycleBlocksCoordinatorSettleBeforeWholeReconcile(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC().Add(-time.Second)
	c, store, lifecycle, res := lifecycleReservation(t, root, makeIntent("life-settleblk1", Manual, "", Long, Enter, "0.001"), now)
	defer c.Close()
	if _, err := lifecycle.MarkMayHaveSent(res, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	snap := accountSnapshot(time.Now().UTC(), "0", "0")
	if err := c.Settle(res, store, snap, map[string]OrderObservation{}, time.Now().UTC()); !errors.Is(err, ErrExecutionStageOrder) {
		t.Fatalf("coordinator bypassed lifecycle: %v", err)
	}
}

func TestExecutionLifecycleMayHaveSentIsGlobalMutationFence(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC().Add(-time.Second)
	c, _, lifecycle, res := lifecycleReservation(t, root, makeIntent("life-globalfence", Manual, "", Long, Enter, "0.001"), now)
	defer c.Close()
	if _, err := lifecycle.MarkMayHaveSent(res, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	// Even the opposite Hedge side is blocked once a network side effect may
	// have occurred. This is a global account-uncertainty gate layered above
	// dev.3.12's side-local pooled-exit serialization.
	basis := cleanReservationBasis(time.Now().UTC())
	if _, _, err := c.reservePreview(makeIntent("life-shortnext1", Manual, "", Short, Enter, "0.001"), basis, time.Now().UTC()); !errors.Is(err, ErrExecutionLifecyclePending) {
		t.Fatalf("opposite-side mutation escaped ambiguous lifecycle: %v", err)
	}
}

func TestExecutionLifecycleOrphanArchiveBlocksCoordinatorStartup(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "trading", "execution_lifecycle")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	// A syntactically valid stage file without its root must not be silently
	// ignored at coordinator startup.
	name := "bt_0123456789abcdef0123456789ab.stage.000001.json"
	if err := os.WriteFile(filepath.Join(dir, name), []byte("{}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenCoordinator(root); err == nil || !strings.Contains(err.Error(), "lifecycle") {
		t.Fatalf("orphan lifecycle did not fail coordinator startup: %v", err)
	}
}

func TestExecutionLifecycleFinalizeNilDependenciesFailClosed(t *testing.T) {
	root := t.TempDir()
	start := time.Now().UTC().Add(-2 * time.Second)
	intent := makeIntent("life-nildeps001", Manual, "", Long, Enter, "0.001")
	c, store, lifecycle, res := lifecycleReservation(t, root, intent, start)
	defer c.Close()
	exec := &lifecycleExecutor{id: 708}
	if _, err := lifecycle.DispatchOnce(context.Background(), res, exec, allow); err != nil {
		t.Fatal(err)
	}
	reader := &readerOrders{orders: map[string]exchange.OrderResult{}, trades: map[int64][]exchange.AccountTrade{}}
	rec, order := finalOrderForReservation(t, root, res, 708, "FILLED", "0.001")
	reader.orders[res.ClientOrderID] = order
	reader.trades[708] = []exchange.AccountTrade{receipt(rec, 70801, "0.001", "100000", "0.1", "0", time.Now().UTC().Add(-time.Second).UnixMilli())}
	now := time.Now().UTC()
	if _, err := lifecycle.RecordSignedObservation(res, OrderObservation{Order: order, CheckedAt: now}, now); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ImportDemoOrder(context.Background(), reader, DemoCredentials{APIKey: "demo-key", Secret: "secret"}, intent); err != nil {
		t.Fatal(err)
	}
	now = time.Now().UTC()
	if _, err := lifecycle.RecordFillsImported(res, store, OrderObservation{Order: order, CheckedAt: now}, now); err != nil {
		t.Fatal(err)
	}
	now = time.Now().UTC()
	snap := accountSnapshot(now, "0.001", "0")
	orders := map[string]OrderObservation{res.ClientOrderID: {Order: order, CheckedAt: now}}
	if _, err := lifecycle.RecordWholeSideReconciled(res, store, snap, orders, now); err != nil {
		t.Fatal(err)
	}
	if _, err := lifecycle.Finalize(res, c, nil, snap, orders, time.Now().UTC()); err == nil || !strings.Contains(err.Error(), "동일 데이터 디렉터리") {
		t.Fatalf("nil ownership store was not fail-closed: %v", err)
	}
	if _, err := lifecycle.Finalize(res, nil, store, snap, orders, time.Now().UTC()); err == nil || !strings.Contains(err.Error(), "동일 데이터 디렉터리") {
		t.Fatalf("nil coordinator was not fail-closed: %v", err)
	}
	current, err := lifecycle.Load(res)
	if err != nil || current.Stage != ExecutionWholeReconciled {
		t.Fatalf("failed finalize advanced lifecycle: %+v %v", current, err)
	}
}

func TestExecutionLifecycleAuditPendingDoesNotMutate(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC().Add(-time.Second)
	c, _, lifecycle, res := lifecycleReservation(t, root, makeIntent("life-auditpend1", Manual, "", Long, Enter, "0.001"), now)
	defer c.Close()
	if _, err := lifecycle.MarkMayHaveSent(res, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	before, _, _, err := snapshotDir(filepath.Join(root, "trading", "execution_lifecycle"), validExecutionArchiveEntry)
	if err != nil {
		t.Fatal(err)
	}
	review, err := ReviewExecutionLifecycles(root)
	if err != nil {
		t.Fatal(err)
	}
	if review.State != ExecutionAuditPending || review.Total != 1 || review.Pending != 1 || review.Settled != 0 || review.TradingAuthorized || !review.ManualReviewRequired {
		t.Fatalf("unexpected pending audit: %+v", review)
	}
	after, _, _, err := snapshotDir(filepath.Join(root, "trading", "execution_lifecycle"), validExecutionArchiveEntry)
	if err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatalf("read-only audit mutated lifecycle tree: before=%q after=%q", before, after)
	}
}

func TestExecutionDispatchRunsFinalGuardAfterMayHaveSent(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC().Add(-time.Second)
	c, _, lifecycle, res := lifecycleReservation(t, root, makeIntent("life-finalguard1", Manual, "", Long, Enter, "0.001"), now)
	defer c.Close()
	exec := &lifecycleExecutor{id: 9901}
	checks := 0
	guard := func(ctx context.Context, i Intent) error {
		checks++
		current, err := lifecycle.Load(res)
		if err != nil {
			return err
		}
		if checks == 1 && current.Stage != ExecutionReserved {
			return errors.New("first guard not before MAY_HAVE_SENT")
		}
		if checks == 2 && current.Stage != ExecutionMayHaveSent {
			return errors.New("final guard not after MAY_HAVE_SENT")
		}
		return nil
	}
	if _, err := lifecycle.DispatchOnce(context.Background(), res, exec, guard); err != nil {
		t.Fatal(err)
	}
	if checks != 2 || exec.calls != 1 {
		t.Fatalf("expected pre-marker and final pre-POST guards, checks=%d submits=%d", checks, exec.calls)
	}
}

func TestExecutionEvidenceSnapshotIncludesRequestsReservationsAndFills(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC().Add(-time.Second)
	c, _, _, _ := lifecycleReservation(t, root, makeIntent("life-evidence001", Manual, "", Long, Enter, "0.001"), now)
	defer c.Close()
	before, err := snapshotExecutionEvidence(root)
	if err != nil {
		t.Fatal(err)
	}
	fillDir := filepath.Join(root, "trading", "fills")
	if err := os.MkdirAll(fillDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fillDir, "BTCUSDT_1.json"), []byte("{}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	after, err := snapshotExecutionEvidence(root)
	if err != nil {
		t.Fatal(err)
	}
	if before == after {
		t.Fatal("fill directory mutation was not included in lifecycle audit evidence digest")
	}
}
