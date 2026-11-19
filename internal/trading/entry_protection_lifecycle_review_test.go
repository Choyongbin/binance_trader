package trading

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

// A deliberately over-capable adapter: the lifecycle review must NEVER call
// its POST/DELETE methods even if a test caller implements both.
type entryLifecycleSignedMock struct {
	*armingTransportAdapter
	trades      map[int64][]exchange.AccountTrade
	getCalls    int
	tradeCalls  int
	submitCalls int
	cancelCalls int
	failGET     bool
	drift       bool
	zeroSide    bool
	foreignAlgo bool
	stale       bool
}

func (m *entryLifecycleSignedMock) QueryDemoAlgo(ctx context.Context, k, s string, id int64) (exchange.DemoAlgoResult, error) {
	m.getCalls++
	if m.failGET {
		return exchange.DemoAlgoResult{}, errors.New("signed algo GET timeout")
	}
	return m.armingTransportAdapter.QueryDemoAlgo(ctx, k, s, id)
}
func (m *entryLifecycleSignedMock) FetchDemoOrderTrades(_ context.Context, _, _, _ string, id int64) ([]exchange.AccountTrade, error) {
	m.tradeCalls++
	if m.failGET {
		return nil, errors.New("trade GET timeout")
	}
	return m.trades[id], nil
}
func (m *entryLifecycleSignedMock) FetchDemoExecutionPreflight(ctx context.Context, k, s, symbol string) (exchange.ExecutionPreflightSnapshot, error) {
	m.getCalls++
	if m.failGET {
		return exchange.ExecutionPreflightSnapshot{}, errors.New("account GET timeout")
	}
	snap, err := m.armingTransportAdapter.FetchDemoExecutionPreflight(ctx, k, s, symbol)
	if err != nil {
		return snap, err
	}
	if m.zeroSide {
		raw, _ := json.Marshal(snap.Portfolio)
		var obj map[string]any
		_ = json.Unmarshal(raw, &obj)
		positions, ok := obj["positions"].([]any)
		if ok {
			for _, p := range positions {
				if mp, ok := p.(map[string]any); ok {
					mp["positionAmt"] = "0"
				}
			}
		}
		updated, _ := json.Marshal(obj)
		_ = json.Unmarshal(updated, &snap.Portfolio)
	}
	if m.foreignAlgo {
		snap.OpenAlgos = append(snap.OpenAlgos, exchange.AlgoOrder{AlgoID: 88771, ClientAlgoID: "unmanaged_algo"})
	}
	if m.drift && m.getCalls > 1 {
		snap.OpenAlgos = append(snap.OpenAlgos, exchange.AlgoOrder{AlgoID: 88772, ClientAlgoID: "intervening_order"})
	}
	if m.stale {
		snap.FinishedAt = time.Now().Add(-time.Minute)
	}
	return snap, nil
}
func (m *entryLifecycleSignedMock) SubmitDemoProtection(_ context.Context, _, _ string, _ exchange.NewDemoProtection) (exchange.DemoAlgoResult, error) {
	m.submitCalls++
	return exchange.DemoAlgoResult{}, errors.New("POST forbidden")
}
func (m *entryLifecycleSignedMock) CancelDemoAlgoOnce(_ context.Context, _, _ string, _ int64) (exchange.DemoAlgoResult, error) {
	m.cancelCalls++
	return exchange.DemoAlgoResult{}, errors.New("DELETE forbidden")
}

type entryLifecycleFixture struct {
	entryBridgeFixture
	cancels *CancelIntentStore
	signed  *entryLifecycleSignedMock
	plan    ProtectionDispatchPlan
}

func newEntryLifecycleFixture(t *testing.T, side PositionSide) *entryLifecycleFixture {
	t.Helper()
	f := newEntryBridgeFixture(t, side, "FILLED", "0.01")
	out, err := f.run(context.Background(), t, nil)
	if err != nil || out.State != EntryProtectionPreparedReview {
		t.Fatalf("prepare %+v %v", out, err)
	}
	cancels, err := NewCancelIntentStore(f.root)
	if err != nil {
		t.Fatal(err)
	}
	signed := &entryLifecycleSignedMock{armingTransportAdapter: f.transport, trades: make(map[int64][]exchange.AccountTrade)}
	return &entryLifecycleFixture{entryBridgeFixture: f, cancels: cancels, signed: signed, plan: out.Plan}
}
func (f *entryLifecycleFixture) review(ctx context.Context) (EntryProtectionLifecycleReview, error) {
	return ReviewRecoveredEntryProtectionLifecycleSigned(ctx, f.reservation, f.coordinator, f.lifecycle, f.protection, f.cancels,
		f.signed, DemoCredentials{APIKey: "demo", Secret: "secret"})
}
func (f *entryLifecycleFixture) simulateLeg(t *testing.T, leg string, id int64) {
	t.Helper()
	inst := f.plan.TP
	if leg == "sl" {
		inst = f.plan.SL
	}
	if err := f.protection.MarkMayHaveSent(f.plan.GroupID, leg, true); err != nil {
		t.Fatal(err)
	}
	f.signed.armingTransportAdapter.m.algos[id] = algoFromInstruction(id, inst, "NEW")
	f.signed.armingTransportAdapter.m.order = append(f.signed.armingTransportAdapter.m.order, id)
	if err := f.protection.RecordObserved(context.Background(), f.plan.GroupID, leg, id, f.signed, "demo", "secret"); err != nil {
		t.Fatal(err)
	}
}
func (f *entryLifecycleFixture) armPair(t *testing.T) {
	t.Helper()
	f.simulateLeg(t, "tp", 2201)
	f.simulateLeg(t, "sl", 2202)
}
func (f *entryLifecycleFixture) trigger(t *testing.T, name string, qty string) {
	t.Helper()
	inst, id := f.plan.TP, int64(2201)
	if name == "sl" {
		inst, id = f.plan.SL, 2202
	}
	a := f.signed.armingTransportAdapter.m.algos[id]
	a.Status = "FINISHED"
	a.ActualOrderID = "91234"
	_ = json.Unmarshal([]byte(`"`+qty+`"`), &a.ActualQty)
	f.signed.armingTransportAdapter.m.algos[id] = a
	f.signed.armingTransportAdapter.m.order = []int64{}
	if name == "tp" {
		f.signed.armingTransportAdapter.m.order = []int64{2202}
	} else {
		f.signed.armingTransportAdapter.m.order = []int64{2201}
	}
	f.signed.trades[91234] = []exchange.AccountTrade{protectionTrade(91234, 55671, inst.Side, string(inst.PositionSide), qty, "100500", "5")}
	f.signed.zeroSide = qty == "0.01"
}
func assertReadOnlyLifecycle(t *testing.T, f *entryLifecycleFixture, out EntryProtectionLifecycleReview) {
	t.Helper()
	if out.TradingAuthorized || out.ProtectionSubmitAuthorized || out.CancelAuthorized || out.RetryAuthorized || out.SettlementAuthorized || !out.ManualReviewRequired || f.signed.submitCalls != 0 || f.signed.cancelCalls != 0 || f.transport.m.submitCalls != 0 {
		t.Fatalf("lifecycle granted unsafe permissions: %+v POST=%d DELETE=%d", out, f.signed.submitCalls, f.signed.cancelCalls)
	}
}
func TestEntryLifecyclePreparedAndLongShortRestartReadOnly(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			f := newEntryLifecycleFixture(t, side)
			out, err := f.review(context.Background())
			if err != nil || out.State != EntryLifecyclePreparedOnly || out.GroupID != f.plan.GroupID || out.Checkpoint.State != "PREPARED_NEVER_DISPATCHED" {
				t.Fatalf("prepared %+v %v", out, err)
			}
			assertReadOnlyLifecycle(t, f, out)
			// Reopen the stores (cold process) with no previous in-memory state.
			life, err := NewExecutionLifecycleStore(f.root)
			if err != nil {
				t.Fatal(err)
			}
			subs, err := OpenProtectionSubmissionStore(f.root)
			if err != nil {
				t.Fatal(err)
			}
			cancels, err := NewCancelIntentStore(f.root)
			if err != nil {
				t.Fatal(err)
			}
			f.lifecycle, f.protection, f.cancels = life, subs, cancels
			out, err = f.review(context.Background())
			if err != nil || out.State != EntryLifecyclePreparedOnly {
				t.Fatalf("restart %+v %v", out, err)
			}
			assertReadOnlyLifecycle(t, f, out)
		})
	}
}
func TestEntryLifecycleAmbiguousAndSingleLegNoReplay(t *testing.T) {
	f := newEntryLifecycleFixture(t, Long)
	if err := f.protection.MarkMayHaveSent(f.plan.GroupID, "tp", true); err != nil {
		t.Fatal(err)
	}
	out, err := f.review(context.Background())
	if err != nil || out.State != EntryLifecycleUnknownSubmission || out.RetryAuthorized {
		t.Fatalf("ambiguous %+v %v", out, err)
	}
	assertReadOnlyLifecycle(t, f, out)
	f.signed.armingTransportAdapter.m.algos[2201] = algoFromInstruction(2201, f.plan.TP, "NEW")
	f.signed.armingTransportAdapter.m.order = []int64{2201}
	if err := f.protection.RecordObserved(context.Background(), f.plan.GroupID, "tp", 2201, f.signed, "demo", "secret"); err != nil {
		t.Fatal(err)
	}
	out, err = f.review(context.Background())
	if err != nil || out.State != EntryLifecycleSingleLeg {
		t.Fatalf("single %+v %v", out, err)
	}
	assertReadOnlyLifecycle(t, f, out)
	// A second-leg MAY_HAVE_SENT is still unresolvable and must never retry.
	if err := f.protection.MarkMayHaveSent(f.plan.GroupID, "sl", true); err != nil {
		t.Fatal(err)
	}
	out, err = f.review(context.Background())
	if err != nil || out.State != EntryLifecycleUnknownSubmission {
		t.Fatalf("ambiguous SL %+v %v", out, err)
	}
	assertReadOnlyLifecycle(t, f, out)
}
func TestEntryLifecycleActivePairThenTriggerAndCancelReview(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			f := newEntryLifecycleFixture(t, side)
			f.armPair(t)
			out, err := f.review(context.Background())
			if err != nil || out.State != EntryLifecyclePairActive || out.ExitFills.TradeCount != 0 {
				t.Fatalf("active %+v %v", out, err)
			}
			assertReadOnlyLifecycle(t, f, out)
			f.trigger(t, "tp", "0.01")
			out, err = f.review(context.Background())
			if err != nil || out.State != EntryLifecycleCancelReview || out.ExitFills.TotalFilledQty != "0.01" {
				t.Fatalf("triggered %+v %v", out, err)
			}
			assertReadOnlyLifecycle(t, f, out)
			intent, err := f.cancels.Plan(f.plan.GroupID, 2202, f.plan.SL.ClientAlgoID)
			if err != nil {
				t.Fatal(err)
			}
			out, err = f.review(context.Background())
			if err != nil || out.State != EntryLifecycleCancelReview || !out.CancelIntentRecorded || out.CancelMayHaveBeenSent {
				t.Fatalf("intent-only %+v %v", out, err)
			}
			if err := f.cancels.MarkMayHaveSent(intent); err != nil {
				t.Fatal(err)
			}
			out, err = f.review(context.Background())
			if err != nil || out.State != EntryLifecycleCancelAmbiguous || !out.CancelMayHaveBeenSent {
				t.Fatalf("cancel ambiguous %+v %v", out, err)
			}
			assertReadOnlyLifecycle(t, f, out)
			// Simulate a separate process, not an in-process cache.
			refreshed, err := NewCancelIntentStore(f.root)
			if err != nil {
				t.Fatal(err)
			}
			f.cancels = refreshed
			out, err = f.review(context.Background())
			if err != nil || out.State != EntryLifecycleCancelAmbiguous {
				t.Fatalf("restart %+v %v", out, err)
			}
			assertReadOnlyLifecycle(t, f, out)
			a := f.signed.armingTransportAdapter.m.algos[2202]
			a.Status = "CANCELED"
			f.signed.armingTransportAdapter.m.algos[2202] = a
			f.signed.armingTransportAdapter.m.order = nil
			out, err = f.review(context.Background())
			if err != nil || out.State != EntryLifecycleClosedNotSettled || out.ExitFills.TotalFilledQty != "0.01" {
				t.Fatalf("closed %+v %v", out, err)
			}
			assertReadOnlyLifecycle(t, f, out)
		})
	}
}
func TestEntryLifecyclePartialAndDoubleTriggerHalt(t *testing.T) {
	f := newEntryLifecycleFixture(t, Long)
	f.armPair(t)
	f.trigger(t, "tp", "0.004")
	out, err := f.review(context.Background())
	if err != nil || out.State != EntryLifecyclePartialHalt {
		t.Fatalf("partial %+v %v", out, err)
	}
	assertReadOnlyLifecycle(t, f, out)
	a := f.signed.armingTransportAdapter.m.algos[2202]
	a.Status = "FINISHED"
	a.ActualOrderID = "95612"
	_ = json.Unmarshal([]byte(`"0.006"`), &a.ActualQty)
	f.signed.armingTransportAdapter.m.algos[2202] = a
	f.signed.armingTransportAdapter.m.order = nil
	f.signed.trades[95612] = []exchange.AccountTrade{protectionTrade(95612, 55672, f.plan.SL.Side, "LONG", "0.006", "99900", "-2")}
	out, err = f.review(context.Background())
	if err != nil || out.State != EntryLifecycleDoubleTriggerHalt {
		t.Fatalf("double %+v %v", out, err)
	}
	assertReadOnlyLifecycle(t, f, out)
}
func TestEntryLifecycleSignedAndLedgerFailuresBlock(t *testing.T) {
	cases := []struct {
		name   string
		change func(*testing.T, *entryLifecycleFixture)
	}{
		{"missing signed GET", func(_ *testing.T, f *entryLifecycleFixture) { f.signed.failGET = true }},
		{"stale account", func(_ *testing.T, f *entryLifecycleFixture) { f.signed.stale = true }},
		{"foreign order", func(_ *testing.T, f *entryLifecycleFixture) { f.signed.foreignAlgo = true }},
		{"account race", func(_ *testing.T, f *entryLifecycleFixture) { f.signed.drift = true }},
		{"wrong credential", func(_ *testing.T, f *entryLifecycleFixture) {
			a := f.signed.armingTransportAdapter.m.algos[2202]
			a.ClientAlgoID = "hijacked"
			f.signed.armingTransportAdapter.m.algos[2202] = a
		}},
		{"foreign store", func(t *testing.T, f *entryLifecycleFixture) {
			x, err := NewCancelIntentStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			f.cancels = x
		}},
		{"checkpoint damaged", func(t *testing.T, f *entryLifecycleFixture) {
			p := filepath.Join(f.root, "trading", "protection_submissions", f.plan.GroupID+".sl.observed.json")
			if err := os.WriteFile(p, []byte(`broken`), 0600); err != nil {
				t.Fatal(err)
			}
		}},
		{"wrong cancel target", func(t *testing.T, f *entryLifecycleFixture) {
			if _, err := f.cancels.Plan(f.plan.GroupID, 2201, f.plan.TP.ClientAlgoID); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			f := newEntryLifecycleFixture(t, Long)
			f.armPair(t)
			tt.change(t, f)
			out, err := f.review(context.Background())
			if err == nil || out.State != EntryLifecycleBlocked {
				t.Fatalf("accepted %s %+v %v", tt.name, out, err)
			}
			assertReadOnlyLifecycle(t, f, out)
		})
	}
}
func TestEntryLifecycleCancelContextAndNoUnexpectedLedgerWrites(t *testing.T) {
	f := newEntryLifecycleFixture(t, Short)
	c, cancel := context.WithCancel(context.Background())
	cancel()
	out, err := f.review(c)
	if !errors.Is(err, context.Canceled) || out.State != EntryLifecycleBlocked {
		t.Fatalf("context %+v %v", out, err)
	}
	before, err := os.ReadDir(filepath.Join(f.root, "trading", "algo_cancel_intents"))
	if err != nil {
		t.Fatal(err)
	}
	out, err = f.review(context.Background())
	if err != nil || out.State != EntryLifecyclePreparedOnly {
		t.Fatalf("read-only %+v %v", out, err)
	}
	after, err := os.ReadDir(filepath.Join(f.root, "trading", "algo_cancel_intents"))
	if err != nil {
		t.Fatal(err)
	}
	if len(before) != len(after) || f.signed.submitCalls != 0 || f.signed.cancelCalls != 0 {
		t.Fatalf("writes detected before=%d after=%d", len(before), len(after))
	}
	assertReadOnlyLifecycle(t, f, out)
}
