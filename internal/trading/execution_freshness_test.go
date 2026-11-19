package trading

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

type preflightReaderFunc func(context.Context, string, string, string) (exchange.ExecutionPreflightSnapshot, error)

func (f preflightReaderFunc) FetchDemoExecutionPreflight(ctx context.Context, k, s, symbol string) (exchange.ExecutionPreflightSnapshot, error) {
	return f(ctx, k, s, symbol)
}

func signedReadySnapshot(now time.Time, canTrade bool) exchange.ExecutionPreflightSnapshot {
	return exchange.ExecutionPreflightSnapshot{
		Environment: "demo", StartedAt: now.Add(-50 * time.Millisecond), FinishedAt: now,
		Portfolio:     exchange.AccountPortfolio{CanTrade: canTrade},
		Configuration: exchange.TradingConfiguration{Environment: "demo", Symbol: "BTCUSDT", HedgeMode: true, MarginType: "ISOLATED", Leverage: 15, SettingsMatchV1: true},
		OpenOrders:    []exchange.Order{}, OpenAlgos: []exchange.AlgoOrder{},
	}
}

func TestSignedTradingReadinessGuardRefreshesCapabilityEveryCall(t *testing.T) {
	now := time.Now().UTC()
	calls := 0
	guard := NewSignedTradingReadinessGuard(
		func(context.Context) (TradingReadinessInput, time.Time, error) {
			in := readyInput(now)
			in.AccountCanTradeKnown = false
			return in, now, nil
		},
		preflightReaderFunc(func(context.Context, string, string, string) (exchange.ExecutionPreflightSnapshot, error) {
			calls++
			return signedReadySnapshot(now, calls == 1), nil
		}),
		func(context.Context) (DemoCredentials, error) { return DemoCredentials{"k", "s"}, nil }, "BTCUSDT")
	if err := guard(context.Background(), Intent{}); err != nil {
		t.Fatal(err)
	}
	if err := guard(context.Background(), Intent{}); err == nil || !strings.Contains(err.Error(), TradingReadinessAccountTradingDisabled) {
		t.Fatalf("second should fail canTrade: %v", err)
	}
	if calls != 2 {
		t.Fatalf("signed calls=%d", calls)
	}
}

func TestSignedTradingReadinessGuardFailsClosedOnSignedRead(t *testing.T) {
	now := time.Now().UTC()
	guard := NewSignedTradingReadinessGuard(func(context.Context) (TradingReadinessInput, time.Time, error) { return readyInput(now), now, nil }, preflightReaderFunc(func(context.Context, string, string, string) (exchange.ExecutionPreflightSnapshot, error) {
		return exchange.ExecutionPreflightSnapshot{}, errors.New("boom")
	}), func(context.Context) (DemoCredentials, error) { return DemoCredentials{"k", "s"}, nil }, "BTCUSDT")
	if err := guard(context.Background(), Intent{}); err == nil || !strings.Contains(err.Error(), "signed execution preflight") {
		t.Fatalf("unexpected %v", err)
	}
}
