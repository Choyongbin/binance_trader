package trading

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func readyInput(now time.Time) TradingReadinessInput {
	return TradingReadinessInput{
		Environment: "demo", MarketState: "live", MarketReady: true, MarketAge: time.Second, MaxMarketAge: 5 * time.Second,
		AccountStatus: "connected", AccountEnvironment: "demo", AccountAsOf: now.Add(-time.Second), MaxAccountAge: 20 * time.Second,
		AccountCanTradeKnown: true, AccountCanTrade: true,
		WalletVerified: true, WalletAssignedUSDT: "100",
		ExecutionAuditState: ExecutionAuditEmpty, ProtectionUnsettled: 0, ProtectionArmingState: ProtectionArmingEmpty,
	}
}

func TestTradingReadinessReadyAndAlwaysNonAuthorizing(t *testing.T) {
	now := time.Now().UTC()
	got, err := EvaluateTradingReadiness(readyInput(now), now)
	if err != nil || !got.Ready || got.State != TradingReadinessReady || got.TradingAuthorized || !got.FreshPreflightRequired || !got.FinalGuardRequired {
		t.Fatalf("unexpected readiness: %+v err=%v", got, err)
	}
}

func TestTradingReadinessFailsClosedOnAccountCapabilityAndPendingExecution(t *testing.T) {
	now := time.Now().UTC()
	in := readyInput(now)
	in.AccountCanTradeKnown = false
	in.ExecutionAuditState = ExecutionAuditPending
	got, err := EvaluateTradingReadiness(in, now)
	if err != nil || got.Ready || got.AccountReady || got.ExecutionReady || got.State != TradingReadinessAccountCapabilityUnknown {
		t.Fatalf("expected fail closed: %+v err=%v", got, err)
	}
	joined := strings.Join(got.Blockers, " ")
	if !strings.Contains(joined, "canTrade") || !strings.Contains(joined, "execution lifecycle") {
		t.Fatalf("missing blockers: %v", got.Blockers)
	}
}

func TestTradingReadinessFailsClosedOnStaleMarketAccountWalletProtectionAndLock(t *testing.T) {
	now := time.Now().UTC()
	in := readyInput(now)
	in.MarketAge = 6 * time.Second
	in.AccountAsOf = now.Add(-21 * time.Second)
	in.WalletAssignedUSDT = "0"
	in.ProtectionUnsettled = 1
	in.MutationGatePresent = true
	got, err := EvaluateTradingReadiness(in, now)
	if err != nil || got.Ready || got.MarketReady || got.AccountReady || got.WalletReady || got.ProtectionReady || got.MutationReady {
		t.Fatalf("expected blockers: %+v err=%v", got, err)
	}
}

func TestAssertTradingReadinessRebuildsEvidenceEachCall(t *testing.T) {
	now := time.Now().UTC()
	calls := 0
	build := func() (TradingReadinessInput, time.Time, error) {
		calls++
		in := readyInput(now)
		if calls == 2 {
			in.MarketAge = 10 * time.Second
		}
		return in, now, nil
	}
	if err := AssertTradingReadiness(nil, build); err != nil {
		t.Fatalf("first guard: %v", err)
	}
	if err := AssertTradingReadiness(nil, build); err == nil || !strings.Contains(err.Error(), TradingReadinessStaleMarket) {
		t.Fatalf("second guard should fail stale market: %v", err)
	}
	if calls != 2 {
		t.Fatalf("builder calls=%d", calls)
	}
	if err := AssertTradingReadiness(errors.New("ctx done"), build); err == nil || err.Error() != "ctx done" || calls != 2 {
		t.Fatalf("context must stop before rebuilding: %v calls=%d", err, calls)
	}
}

func TestNewTradingReadinessGuardMatchesDurableGuardContract(t *testing.T) {
	now := time.Now().UTC()
	calls := 0
	guard := NewTradingReadinessGuard(func(context.Context) (TradingReadinessInput, time.Time, error) {
		calls++
		in := readyInput(now)
		if calls == 2 {
			in.ProtectionUnsettled = 1
		}
		return in, now, nil
	})
	if err := guard(context.Background(), Intent{}); err != nil {
		t.Fatalf("first: %v", err)
	}
	if err := guard(context.Background(), Intent{}); err == nil || !strings.Contains(err.Error(), TradingReadinessProtectionRecovery) {
		t.Fatalf("second should block protection: %v", err)
	}
}
