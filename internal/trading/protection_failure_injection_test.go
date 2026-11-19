package trading

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"binance_trader/internal/exchange"
)

type recoveryArmingMock struct {
	*armingMock
	queryCalls    int
	failQueryCall int
}

func (m *recoveryArmingMock) QueryDemoAlgo(ctx context.Context, k, s string, id int64) (exchange.DemoAlgoResult, error) {
	m.queryCalls++
	if m.failQueryCall > 0 && m.queryCalls == m.failQueryCall {
		return exchange.DemoAlgoResult{}, errors.New("injected signed readback loss")
	}
	return m.armingMock.QueryDemoAlgo(ctx, k, s, id)
}

type recoveryTransportAdapter struct{ m *recoveryArmingMock }

func (a *recoveryTransportAdapter) FetchDemoExecutionPreflight(ctx context.Context, k, s, symbol string) (exchange.ExecutionPreflightSnapshot, error) {
	snap := a.m.snapshot()
	raw := []byte(`{"canTrade":true,"totalWalletBalance":"1000","totalUnrealizedProfit":"0","totalMarginBalance":"1000","totalInitialMargin":"1","totalPositionInitialMargin":"1","totalOpenOrderInitialMargin":"0","availableBalance":"999","assets":[{"asset":"USDT","walletBalance":"1000","availableBalance":"999","marginBalance":"1000"}],"positions":[{"symbol":"BTCUSDT","positionSide":"LONG","positionAmt":"0.01","initialMargin":"1","isolated":true}]}`)
	if a.m.owner.Side == Short {
		raw = []byte(`{"canTrade":true,"totalWalletBalance":"1000","totalUnrealizedProfit":"0","totalMarginBalance":"1000","totalInitialMargin":"1","totalPositionInitialMargin":"1","totalOpenOrderInitialMargin":"0","availableBalance":"999","assets":[{"asset":"USDT","walletBalance":"1000","availableBalance":"999","marginBalance":"1000"}],"positions":[{"symbol":"BTCUSDT","positionSide":"SHORT","positionAmt":"-0.01","initialMargin":"1","isolated":true}]}`)
	}
	_ = json.Unmarshal(raw, &snap.Portfolio)
	return snap, nil
}
func (a *recoveryTransportAdapter) SubmitDemoProtection(ctx context.Context, k, s string, o exchange.NewDemoProtection) (exchange.DemoAlgoResult, error) {
	return a.m.SubmitDemoProtection(ctx, k, s, o)
}
func (a *recoveryTransportAdapter) QueryDemoAlgo(ctx context.Context, k, s string, id int64) (exchange.DemoAlgoResult, error) {
	return a.m.QueryDemoAlgo(ctx, k, s, id)
}

func testRecoveryCoordinator(t *testing.T, store *ProtectionSubmissionStore) *Coordinator {
	t.Helper()
	dataDir := filepath.Dir(filepath.Dir(store.dir))
	coord, err := OpenCoordinator(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = coord.Close() })
	return coord
}

func TestProtectionFailureInjectionTPAckThenReadbackLossRecoversWithoutReplay(t *testing.T) {
	store, plan, _, _ := testProtectionGroup(t, Long)
	owner := OwnerPosition{Source: Manual, Symbol: "BTCUSDT", Side: Long, Quantity: "0.01", AverageEntry: "100000"}
	base := &armingMock{owner: owner, plan: plan, algos: map[int64]exchange.DemoAlgoResult{}}
	m := &recoveryArmingMock{armingMock: base, failQueryCall: 1}
	transport := &recoveryTransportAdapter{m: m}
	coord := testRecoveryCoordinator(t, store)

	if _, err := ArmDemoProtection(context.Background(), plan.GroupID, owner, coord, store, transport, DemoCredentials{"k", "s"}); err == nil {
		t.Fatal("TP readback loss unexpectedly succeeded")
	}
	if m.submitCalls != 1 {
		t.Fatalf("expected exactly one TP POST, got %d", m.submitCalls)
	}
	tp, err := store.Inspect(plan.GroupID, "tp")
	if err != nil || !tp.MayHaveSent || tp.SignedObserved {
		t.Fatalf("unexpected TP durable state %+v err=%v", tp, err)
	}

	m.failQueryCall = 0
	before := m.submitCalls
	res, err := RecoverDemoProtectionArming(context.Background(), plan.GroupID, owner, coord, store, transport, DemoCredentials{"k", "s"})
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	if m.submitCalls != before {
		t.Fatalf("recovery replayed POST: before=%d after=%d", before, m.submitCalls)
	}
	if !res.TPObserved || res.Armed {
		t.Fatalf("expected TP observed but not fully armed: %+v", res)
	}
	// SL was never MAY_HAVE_SENT; recovery must not infer or submit it.
	sl, err := store.Inspect(plan.GroupID, "sl")
	if err != nil || sl.MayHaveSent || sl.SignedObserved {
		t.Fatalf("SL mutated by read-only recovery: %+v err=%v", sl, err)
	}
}

func TestProtectionFailureInjectionSLAckThenReadbackLossRecoversPairWithoutReplay(t *testing.T) {
	store, plan, _, _ := testProtectionGroup(t, Long)
	owner := OwnerPosition{Source: Manual, Symbol: "BTCUSDT", Side: Long, Quantity: "0.01", AverageEntry: "100000"}
	base := &armingMock{owner: owner, plan: plan, algos: map[int64]exchange.DemoAlgoResult{}}
	m := &recoveryArmingMock{armingMock: base, failQueryCall: 2}
	transport := &recoveryTransportAdapter{m: m}
	coord := testRecoveryCoordinator(t, store)

	if _, err := ArmDemoProtection(context.Background(), plan.GroupID, owner, coord, store, transport, DemoCredentials{"k", "s"}); err == nil {
		t.Fatal("SL readback loss unexpectedly succeeded")
	}
	if m.submitCalls != 2 {
		t.Fatalf("expected exactly TP+SL POST, got %d", m.submitCalls)
	}
	tp, _ := store.Inspect(plan.GroupID, "tp")
	sl, _ := store.Inspect(plan.GroupID, "sl")
	if !tp.SignedObserved || !sl.MayHaveSent || sl.SignedObserved {
		t.Fatalf("unexpected durable state tp=%+v sl=%+v", tp, sl)
	}

	m.failQueryCall = 0
	before := m.submitCalls
	res, err := RecoverDemoProtectionArming(context.Background(), plan.GroupID, owner, coord, store, transport, DemoCredentials{"k", "s"})
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	if m.submitCalls != before {
		t.Fatalf("recovery replayed protection POST: before=%d after=%d", before, m.submitCalls)
	}
	if !res.Armed || !res.TPObserved || !res.SLObserved || res.ManualReviewRequired {
		t.Fatalf("pair not safely recovered: %+v", res)
	}
}

func TestProtectionFailureInjectionMayHaveSentMissingOpenAlgoStaysManual(t *testing.T) {
	store, plan, _, _ := testProtectionGroup(t, Long)
	owner := OwnerPosition{Source: Manual, Symbol: "BTCUSDT", Side: Long, Quantity: "0.01", AverageEntry: "100000"}
	if err := store.markMayHaveSentLive(plan.GroupID, "tp"); err != nil {
		t.Fatal(err)
	}
	base := &armingMock{owner: owner, plan: plan, algos: map[int64]exchange.DemoAlgoResult{}}
	m := &recoveryArmingMock{armingMock: base}
	transport := &recoveryTransportAdapter{m: m}
	coord := testRecoveryCoordinator(t, store)

	res, err := RecoverDemoProtectionArming(context.Background(), plan.GroupID, owner, coord, store, transport, DemoCredentials{"k", "s"})
	if err == nil || !res.ManualReviewRequired || res.Armed || m.submitCalls != 0 {
		t.Fatalf("missing ambiguous TP was not fail-closed: res=%+v calls=%d err=%v", res, m.submitCalls, err)
	}
}

type driftingRecoveryTransport struct {
	*recoveryTransportAdapter
	fetches int
	onFetch func(int)
}

func (d *driftingRecoveryTransport) FetchDemoExecutionPreflight(ctx context.Context, k, s, symbol string) (exchange.ExecutionPreflightSnapshot, error) {
	d.fetches++
	if d.onFetch != nil {
		d.onFetch(d.fetches)
	}
	return d.recoveryTransportAdapter.FetchDemoExecutionPreflight(ctx, k, s, symbol)
}

// Once both protection legs have durable signed observations, a later state
// drift before the final arming bracket must never be papered over as ARMED.
// Recovery also must not replay either POST on a subsequent restart.
func TestProtectionFailureInjectionFinalBracketDriftNeverArmsOrReplays(t *testing.T) {
	store, plan, _, _ := testProtectionGroup(t, Long)
	owner := OwnerPosition{Source: Manual, Symbol: "BTCUSDT", Side: Long, Quantity: "0.01", AverageEntry: "100000"}
	base := &armingMock{owner: owner, plan: plan, algos: map[int64]exchange.DemoAlgoResult{}}
	m := &recoveryArmingMock{armingMock: base, failQueryCall: 2}
	baseAdapter := &recoveryTransportAdapter{m: m}
	coord := testRecoveryCoordinator(t, store)

	// Create the crash shape: TP observed, SL POST may have succeeded, SL signed
	// readback was lost. Two POSTs exist and must never be replayed.
	if _, err := ArmDemoProtection(context.Background(), plan.GroupID, owner, coord, store, baseAdapter, DemoCredentials{"k", "s"}); err == nil {
		t.Fatal("expected injected SL readback loss")
	}
	if m.submitCalls != 2 {
		t.Fatalf("unexpected initial protection POST count: %d", m.submitCalls)
	}

	m.failQueryCall = 0
	before := m.submitCalls
	drift := &driftingRecoveryTransport{recoveryTransportAdapter: baseAdapter}
	drift.onFetch = func(n int) {
		// Recovery fetch #1 finds both open legs and can recover the SL readback.
		// Before the final fresh bracket, simulate TP leaving the open-algo set.
		if n == 2 {
			for id, a := range m.algos {
				if a.ClientAlgoID == plan.TP.ClientAlgoID {
					delete(m.algos, id)
					break
				}
			}
		}
	}
	res, err := RecoverDemoProtectionArming(context.Background(), plan.GroupID, owner, coord, store, drift, DemoCredentials{"k", "s"})
	if err == nil || res.Armed || !res.ManualReviewRequired {
		t.Fatalf("final bracket drift was accepted as armed: res=%+v err=%v", res, err)
	}
	if m.submitCalls != before {
		t.Fatalf("recovery replayed protection POST after drift: before=%d after=%d", before, m.submitCalls)
	}

	// Another restart remains read-only; missing/terminal history cannot be
	// inferred from the open set and therefore stays manual.
	again, err := RecoverDemoProtectionArming(context.Background(), plan.GroupID, owner, coord, store, baseAdapter, DemoCredentials{"k", "s"})
	if err == nil || again.Armed || !again.ManualReviewRequired || m.submitCalls != before {
		t.Fatalf("restart after final-bracket drift was not fail-closed: %+v err=%v submits=%d", again, err, m.submitCalls)
	}
}
