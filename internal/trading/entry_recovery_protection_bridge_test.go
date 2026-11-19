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

type entryBridgeFixture struct {
	root        string
	coordinator *Coordinator
	ownership   *OwnershipStore
	lifecycle   *ExecutionLifecycleStore
	protection  *ProtectionSubmissionStore
	reservation Reservation
	reader      *readerOrders
	transport   *armingTransportAdapter
	executor    *lifecycleExecutor
}

func newEntryBridgeFixture(t *testing.T, side PositionSide, status, executed string) entryBridgeFixture {
	t.Helper()
	root := t.TempDir()
	intent := makeIntent("bridge-entry-test-001", Model, "test_model", side, Enter, "0.01")
	c, o, l, r := lifecycleReservation(t, root, intent, time.Now().UTC().Add(-3*time.Second))
	t.Cleanup(func() { _ = c.Close() })
	executor := &lifecycleExecutor{id: 93371, err: errors.New("ACK lost")}
	_, _ = l.DispatchOnce(context.Background(), r, executor, allow)
	rec, order := finalOrderForReservation(t, root, r, executor.id, status, executed)
	reader := &readerOrders{orders: map[string]exchange.OrderResult{r.ClientOrderID: order}, trades: map[int64][]exchange.AccountTrade{executor.id: {}}}
	if executed != "0" {
		reader.trades[executor.id] = []exchange.AccountTrade{receipt(rec, 9337101, executed, "100000", "0.1", "0", time.Now().UTC().Add(-time.Second).UnixMilli())}
	}
	p, err := OpenProtectionSubmissionStore(root)
	if err != nil {
		t.Fatal(err)
	}
	owner := OwnerPosition{Source: Model, ModelID: "test_model", Symbol: "BTCUSDT", Side: side, Quantity: "0.01", AverageEntry: "100000"}
	transport := &armingTransportAdapter{m: &armingMock{owner: owner, algos: map[int64]exchange.DemoAlgoResult{}}}
	return entryBridgeFixture{root, c, o, l, p, r, reader, transport, executor}
}

func (f entryBridgeFixture) run(ctx context.Context, t *testing.T, signed EntryProtectionSignedPreflightReader) (RecoveredEntryProtectionReview, error) {
	t.Helper()
	if signed == nil {
		signed = f.transport
	}
	now := time.Now().UTC()
	quote := Quote{Symbol: "BTCUSDT", Price: "100000", ReceivedAt: now}
	return RecoverAndPrepareEntryProtectionSupervised(ctx, f.reservation, f.coordinator, f.lifecycle,
		f.ownership, f.protection, f.reader, signed, DemoCredentials{"demo", "secret"},
		sizingRules(now), quote, quote)
}

func TestEntryRecoveryProtectionBridgeLongShortRestartNoOrderPost(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			f := newEntryBridgeFixture(t, side, "FILLED", "0.01")
			out, err := f.run(context.Background(), t, nil)
			if err != nil || out.State != EntryProtectionPreparedReview || out.Recovery.FinalStage != ExecutionFillsImported ||
				out.Recovery.ImportedFillCount != 1 || out.Plan.GroupID != "entry_"+f.reservation.ClientOrderID ||
				out.Owner.Source != Model || out.Owner.ModelID != "test_model" || out.Owner.Quantity != "0.01" ||
				out.Continuity.State != "ENTRY_IMPORTED_PROTECTION_UNSENT_REVIEW" ||
				out.Continuity.CheckpointState != "PREPARED_NEVER_DISPATCHED" || !out.Continuity.SignedSnapshotMatched ||
				out.TradingAuthorized || out.ProtectionSubmitAuthorized || out.SettlementAuthorized || !out.ManualReviewRequired ||
				out.RequireProtectionSubmission() == nil || f.transport.m.submitCalls != 0 || f.executor.calls != 1 {
				t.Fatalf("unsafe bridge: %+v %v submits=%d entry=%d", out, err, f.transport.m.submitCalls, f.executor.calls)
			}
			for _, leg := range []string{"tp", "sl"} {
				c, err := f.protection.Inspect(out.GroupID, leg)
				if err != nil || c.MayHaveSent || c.SignedObserved {
					t.Fatalf("unexpected attempted protection %s: %+v %v", leg, c, err)
				}
			}
			entries, _ := filepath.Glob(filepath.Join(f.root, "trading", "reservations_done", "*.json"))
			if len(entries) != 0 {
				t.Fatalf("bridge unexpectedly settled: %v", entries)
			}
			// Reopen every store after crash/restart, then rerun. The same
			// deterministic plan is reused and NO fills/POSTs are duplicated.
			freshLife, err := NewExecutionLifecycleStore(f.root)
			if err != nil {
				t.Fatal(err)
			}
			freshOwner, err := NewOwnershipStore(f.root)
			if err != nil {
				t.Fatal(err)
			}
			freshProtection, err := OpenProtectionSubmissionStore(f.root)
			if err != nil {
				t.Fatal(err)
			}
			f.lifecycle, f.ownership, f.protection = freshLife, freshOwner, freshProtection
			again, err := f.run(context.Background(), t, nil)
			if err != nil || again.State != EntryProtectionPreparedReview || again.Recovery.ImportedFillCount != 0 ||
				again.Plan.Digest != out.Plan.Digest || f.executor.calls != 1 || f.transport.m.submitCalls != 0 {
				t.Fatalf("restart or duplicate: %+v %v", again, err)
			}
			fills, err := f.ownership.readFills()
			if err != nil || len(fills) != 1 {
				t.Fatalf("duplicate fills: %v %v", fills, err)
			}
		})
	}
}

func TestEntryRecoveryProtectionBridgeNonTerminalNoPlan(t *testing.T) {
	for _, tc := range []struct{ status, qty string }{{"NEW", "0"}, {"PARTIALLY_FILLED", "0.005"}} {
		t.Run(tc.status, func(t *testing.T) {
			f := newEntryBridgeFixture(t, Long, tc.status, tc.qty)
			out, err := f.run(context.Background(), t, nil)
			if err != nil || out.State != EntryProtectionRecoveryPending || out.Recovery.FinalStage != ExecutionSignedObserved ||
				out.PendingReason == "" || out.Plan.GroupID != "" || f.transport.m.submitCalls != 0 {
				t.Fatalf("unexpected pending: %+v %v", out, err)
			}
			files, _ := filepath.Glob(filepath.Join(f.root, "trading", "protection_submissions", "*.json"))
			if len(files) != 0 {
				t.Fatal("prepared plan for nonterminal order", files)
			}
		})
	}
}

func TestEntryRecoveryProtectionBridgeRejectsPartialTerminalAndExternalClose(t *testing.T) {
	for _, tc := range []struct{ status, qty string }{{"CANCELED", "0.005"}, {"REJECTED", "0"}} {
		t.Run(tc.status, func(t *testing.T) {
			f := newEntryBridgeFixture(t, Long, tc.status, tc.qty)
			out, err := f.run(context.Background(), t, nil)
			if err == nil || out.State != EntryProtectionRecoveryBlocked || f.transport.m.submitCalls != 0 {
				t.Fatalf("partial terminal accepted: %+v %v", out, err)
			}
		})
	}
	f := newEntryBridgeFixture(t, Short, "FILLED", "0.01")
	// Signed exchange says the hedge side has ALREADY been externally closed:
	// recovery can persist fills, but it must never infer a live owner to arm.
	f.transport.m.owner.Side = Long
	out, err := f.run(context.Background(), t, nil)
	if err == nil || out.Plan.GroupID != "" || out.State != EntryProtectionRecoveryBlocked || f.transport.m.submitCalls != 0 {
		t.Fatalf("externally closed side accepted: %+v %v", out, err)
	}
}

func TestEntryRecoveryProtectionBridgeMissingSignedAndCrossDirectoryFailClosed(t *testing.T) {
	f := newEntryBridgeFixture(t, Long, "FILLED", "0.01")
	f.reader.fail = true
	out, err := f.run(context.Background(), t, nil)
	if err == nil || out.State != EntryProtectionRecoveryBlocked {
		t.Fatalf("signed query bypass: %+v %v", out, err)
	}
	state, err := f.lifecycle.Load(f.reservation)
	if err != nil || state.Stage != ExecutionMayHaveSent {
		t.Fatalf("unsafe transition after missing signed response: %+v %v", state, err)
	}
	f.reader.fail = false
	foreign, err := OpenProtectionSubmissionStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f.protection = foreign
	out, err = f.run(context.Background(), t, nil)
	if err == nil || !strings.Contains(err.Error(), "데이터 경로") || out.State != EntryProtectionRecoveryBlocked {
		t.Fatalf("foreign store accepted: %+v %v", out, err)
	}
}

// A second signed view may observe an external intervention after the local
// plan is durable. The plan is non-executable, and the review must fail closed.
type bridgeSecondSnapshotChanged struct {
	base  ProtectionArmingTransport
	reads int
}

func (b *bridgeSecondSnapshotChanged) FetchDemoExecutionPreflight(ctx context.Context, key, secret, sym string) (exchange.ExecutionPreflightSnapshot, error) {
	b.reads++
	s, err := b.base.FetchDemoExecutionPreflight(ctx, key, secret, sym)
	if err != nil {
		return s, err
	}
	if b.reads == 2 {
		s.OpenAlgos = []exchange.AlgoOrder{{AlgoID: 919}}
	}
	return s, nil
}
func (b *bridgeSecondSnapshotChanged) SubmitDemoProtection(context.Context, string, string, exchange.NewDemoProtection) (exchange.DemoAlgoResult, error) {
	panic("bridge MUST NEVER call an exchange POST")
}
func (b *bridgeSecondSnapshotChanged) QueryDemoAlgo(context.Context, string, string, int64) (exchange.DemoAlgoResult, error) {
	panic("bridge SHOULD NEVER query an Algo by unapproved ID")
}

func TestEntryRecoveryProtectionBridgeSecondSignedViewFailClosed(t *testing.T) {
	f := newEntryBridgeFixture(t, Long, "FILLED", "0.01")
	changed := &bridgeSecondSnapshotChanged{base: f.transport}
	out, err := f.run(context.Background(), t, changed)
	if err == nil || out.State != EntryProtectionRecoveryBlocked || changed.reads != 2 || f.transport.m.submitCalls != 0 {
		t.Fatalf("external modification during prepare accepted: %+v %v", out, err)
	}
	// A signed view mismatch does not erase the durable plan or permit a
	// second group/side to be silently created.
	if _, err := os.Stat(filepath.Join(f.root, "trading", "protection_submissions", out.GroupID+".json")); err != nil {
		t.Fatalf("missing deterministic forensic plan: %v", err)
	}
}

func TestEntryRecoveryProtectionBridgeInvalidQuoteAndCancelledContext(t *testing.T) {
	f := newEntryBridgeFixture(t, Long, "FILLED", "0.01")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	out, err := f.run(ctx, t, nil)
	if !errors.Is(err, context.Canceled) || out.State != EntryProtectionRecoveryBlocked {
		t.Fatalf("cancel ignored: %+v %v", out, err)
	}
	old := time.Now().UTC().Add(-time.Minute)
	quote := Quote{Symbol: "BTCUSDT", Price: "100000", ReceivedAt: old}
	out, err = RecoverAndPrepareEntryProtectionSupervised(context.Background(), f.reservation, f.coordinator, f.lifecycle,
		f.ownership, f.protection, f.reader, f.transport, DemoCredentials{"demo", "secret"},
		sizingRules(time.Now().UTC()), quote, quote)
	if err == nil || out.Plan.GroupID != "" || f.transport.m.submitCalls != 0 {
		t.Fatalf("old quote accepted: %+v %v", out, err)
	}
}
