package trading

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

type armingMock struct {
	owner       OwnerPosition
	plan        ProtectionDispatchPlan
	algos       map[int64]exchange.DemoAlgoResult
	order       []int64
	submitCalls int
	failSecond  bool
}

func (m *armingMock) snapshot() exchange.ExecutionPreflightSnapshot {
	algos := make([]exchange.AlgoOrder, 0, len(m.order))
	for _, id := range m.order {
		a := m.algos[id]
		algos = append(algos, exchange.AlgoOrder{AlgoID: a.AlgoID, ClientAlgoID: a.ClientAlgoID, Symbol: a.Symbol, Side: a.Side, PositionSide: a.PositionSide, OrderType: a.Type, AlgoStatus: a.Status, Quantity: string(a.Quantity), TriggerPrice: string(a.TriggerPrice), WorkingType: a.WorkingType})
	}
	now := time.Now().UTC()
	return exchange.ExecutionPreflightSnapshot{Environment: "demo", StartedAt: now.Add(-time.Millisecond), FinishedAt: now,
		Portfolio:  exchange.AccountPortfolio{},
		OpenOrders: []exchange.Order{}, OpenAlgos: algos,
		Configuration: exchange.TradingConfiguration{Environment: "demo", Symbol: "BTCUSDT", HedgeMode: true, MarginType: "ISOLATED", Leverage: 15, SettingsMatchV1: true}}
}

func (m *armingMock) FetchDemoExecutionPreflight(context.Context, string, string, string) (exchange.ExecutionPreflightSnapshot, error) {
	return m.snapshot(), nil
}
func (m *armingMock) SubmitDemoProtection(_ context.Context, _, _ string, o exchange.NewDemoProtection) (exchange.DemoAlgoResult, error) {
	m.submitCalls++
	if m.failSecond && m.submitCalls == 2 {
		return exchange.DemoAlgoResult{}, errors.New("timeout")
	}
	id := int64(900 + m.submitCalls)
	a := algoFromInstruction(id, mapInstruction(o), "NEW")
	m.algos[id] = a
	m.order = append(m.order, id)
	return a, nil
}
func mapInstruction(o exchange.NewDemoProtection) ProtectionInstruction {
	return ProtectionInstruction{ClientAlgoID: o.ClientAlgoID, Symbol: o.Symbol, Side: o.Side, PositionSide: PositionSide(o.PositionSide), AlgoType: "CONDITIONAL", OrderType: o.Type, Quantity: o.Quantity, TriggerPrice: o.TriggerPrice, WorkingType: o.WorkingType}
}
func (m *armingMock) QueryDemoAlgo(_ context.Context, _, _ string, id int64) (exchange.DemoAlgoResult, error) {
	a, ok := m.algos[id]
	if !ok {
		return exchange.DemoAlgoResult{}, errors.New("missing")
	}
	return a, nil
}

func TestArmDemoProtectionOneShotBothLegs(t *testing.T) {
	store, plan, _, _ := testProtectionGroup(t, Long)
	owner := OwnerPosition{Source: Manual, Symbol: "BTCUSDT", Side: Long, Quantity: "0.01", AverageEntry: "100000"}
	m := &armingMock{owner: owner, plan: plan, algos: map[int64]exchange.DemoAlgoResult{}}
	// build portfolio decimals via JSON round-trip because exchange decimal type is private
	res, err := armWithJSONSnapshot(context.Background(), plan, owner, store, m, false)
	if err != nil || !res.Armed || res.TPAlgoID == 0 || res.SLAlgoID == 0 || m.submitCalls != 2 {
		t.Fatalf("res=%+v calls=%d err=%v", res, m.submitCalls, err)
	}
}

func TestArmDemoProtectionSecondLegAmbiguousNeverRetries(t *testing.T) {
	store, plan, _, _ := testProtectionGroup(t, Long)
	owner := OwnerPosition{Source: Manual, Symbol: "BTCUSDT", Side: Long, Quantity: "0.01", AverageEntry: "100000"}
	m := &armingMock{owner: owner, plan: plan, algos: map[int64]exchange.DemoAlgoResult{}, failSecond: true}
	_, err := armWithJSONSnapshot(context.Background(), plan, owner, store, m, true)
	if err == nil || m.submitCalls != 2 {
		t.Fatalf("calls=%d err=%v", m.submitCalls, err)
	}
	state, inspectErr := store.Inspect(plan.GroupID, "sl")
	if inspectErr != nil || !state.MayHaveSent || state.SignedObserved {
		t.Fatalf("ambiguous SL state not preserved: %+v %v", state, inspectErr)
	}
	if err2 := store.markMayHaveSentLive(plan.GroupID, "sl"); !errors.Is(err2, ErrProtectionAttempted) {
		t.Fatalf("ambiguous SL replay allowed: %v", err2)
	}
}

// override FetchDemoExecutionPreflight using a small JSON helper implemented in exchange test style.
func armWithJSONSnapshot(ctx context.Context, plan ProtectionDispatchPlan, owner OwnerPosition, store *ProtectionSubmissionStore, m *armingMock, _ bool) (ProtectionArmingResult, error) {
	dataDir := filepath.Dir(filepath.Dir(store.dir))
	coord, err := OpenCoordinator(dataDir)
	if err != nil {
		return ProtectionArmingResult{}, err
	}
	defer coord.Close()
	return ArmDemoProtection(ctx, plan.GroupID, owner, coord, store, &armingTransportAdapter{m: m}, DemoCredentials{"k", "s"})
}

type armingTransportAdapter struct{ m *armingMock }

func (a *armingTransportAdapter) FetchDemoExecutionPreflight(ctx context.Context, k, symSecret, symbol string) (exchange.ExecutionPreflightSnapshot, error) {
	// Use JSON to populate private wsDecimal fields.
	snap := a.m.snapshot()
	raw := []byte(`{"canTrade":true,"totalWalletBalance":"1000","totalUnrealizedProfit":"0","totalMarginBalance":"1000","totalInitialMargin":"1","totalPositionInitialMargin":"1","totalOpenOrderInitialMargin":"0","availableBalance":"999","assets":[{"asset":"USDT","walletBalance":"1000","availableBalance":"999","marginBalance":"1000"}],"positions":[{"symbol":"BTCUSDT","positionSide":"LONG","positionAmt":"0.01","initialMargin":"1","isolated":true}]}`)
	if a.m.owner.Side == Short {
		raw = []byte(`{"canTrade":true,"totalWalletBalance":"1000","totalUnrealizedProfit":"0","totalMarginBalance":"1000","totalInitialMargin":"1","totalPositionInitialMargin":"1","totalOpenOrderInitialMargin":"0","availableBalance":"999","assets":[{"asset":"USDT","walletBalance":"1000","availableBalance":"999","marginBalance":"1000"}],"positions":[{"symbol":"BTCUSDT","positionSide":"SHORT","positionAmt":"-0.01","initialMargin":"1","isolated":true}]}`)
	}
	_ = json.Unmarshal(raw, &snap.Portfolio)
	return snap, nil
}
func (a *armingTransportAdapter) SubmitDemoProtection(ctx context.Context, k, s string, o exchange.NewDemoProtection) (exchange.DemoAlgoResult, error) {
	return a.m.SubmitDemoProtection(ctx, k, s, o)
}
func (a *armingTransportAdapter) QueryDemoAlgo(ctx context.Context, k, s string, id int64) (exchange.DemoAlgoResult, error) {
	return a.m.QueryDemoAlgo(ctx, k, s, id)
}
