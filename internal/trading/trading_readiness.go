package trading

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"time"
)

const (
	TradingReadinessReady                    = "READY"
	TradingReadinessWrongEnvironment         = "WRONG_ENVIRONMENT"
	TradingReadinessMarketNotReady           = "MARKET_NOT_READY"
	TradingReadinessStaleMarket              = "STALE_MARKET"
	TradingReadinessAccountDisconnected      = "ACCOUNT_DISCONNECTED"
	TradingReadinessStaleAccount             = "STALE_ACCOUNT"
	TradingReadinessAccountCapabilityUnknown = "ACCOUNT_CAPABILITY_UNVERIFIED"
	TradingReadinessAccountTradingDisabled   = "ACCOUNT_TRADING_DISABLED"
	TradingReadinessWalletUnverified         = "WALLET_UNVERIFIED"
	TradingReadinessWalletEmpty              = "WALLET_EMPTY"
	TradingReadinessMutationBlocked          = "MUTATION_IN_PROGRESS_OR_UNCERTAIN"
	TradingReadinessExecutionRecovery        = "EXECUTION_RECOVERY_REQUIRED"
	TradingReadinessProtectionRecovery       = "PROTECTION_RECONCILIATION_REQUIRED"
	TradingReadinessLocalEvidenceBlocked     = "LOCAL_EVIDENCE_BLOCKED"
)

// TradingReadinessInput is the single side-effect-free contract used to decide
// whether a new automated order MAY proceed to the next preflight stage. It is
// deliberately independent of HTTP/UI and of the exchange POST adapter.
//
// READY is not an order authorization token: the executor must build a fresh
// input again immediately before MAY_HAVE_SENT and again immediately before
// its side effect. Any unknown field is fail-closed.
type TradingReadinessInput struct {
	Environment             string
	MarketState             string
	MarketReady             bool
	MarketAge               time.Duration
	MaxMarketAge            time.Duration
	AccountStatus           string
	AccountEnvironment      string
	AccountAsOf             time.Time
	MaxAccountAge           time.Duration
	AccountCanTradeKnown    bool
	AccountCanTrade         bool
	WalletVerified          bool
	WalletAssignedUSDT      string
	MutationGatePresent     bool
	MutationGateUnknown     bool
	CoordinatorLockPresent  bool
	CoordinatorLockExpected bool
	ExecutionAuditState     string
	ProtectionUnsettled     int
	ProtectionArmingState   string
	LocalEvidenceError      string
}

type TradingReadiness struct {
	Schema                 int       `json:"schema"`
	CheckedAt              time.Time `json:"checked_at"`
	State                  string    `json:"state"`
	Ready                  bool      `json:"ready"`
	TradingAuthorized      bool      `json:"trading_authorized"`
	FreshPreflightRequired bool      `json:"fresh_preflight_required"`
	FinalGuardRequired     bool      `json:"final_guard_required"`
	MarketReady            bool      `json:"market_ready"`
	AccountReady           bool      `json:"account_ready"`
	WalletReady            bool      `json:"wallet_ready"`
	ExecutionReady         bool      `json:"execution_ready"`
	ProtectionReady        bool      `json:"protection_ready"`
	MutationReady          bool      `json:"mutation_ready"`
	Blockers               []string  `json:"blockers"`
}

func normalizeDuration(v, fallback time.Duration) time.Duration {
	if v <= 0 {
		return fallback
	}
	return v
}

func appendReadinessBlocker(r *TradingReadiness, state, msg string) {
	if r.State == TradingReadinessReady {
		r.State = state
	}
	r.Blockers = append(r.Blockers, msg)
}

// EvaluateTradingReadiness contains no IO and performs no mutation. Callers
// gather independent evidence and feed it here. This makes UI, server and the
// future executor guard share one decision model instead of duplicating rules.
func EvaluateTradingReadiness(in TradingReadinessInput, now time.Time) (TradingReadiness, error) {
	out := TradingReadiness{
		Schema: 1, CheckedAt: now.UTC(), State: TradingReadinessReady,
		FreshPreflightRequired: true, FinalGuardRequired: true,
		MarketReady: true, AccountReady: true, WalletReady: true,
		ExecutionReady: true, ProtectionReady: true, MutationReady: true,
		Blockers: []string{},
	}
	if now.IsZero() {
		return out, errors.New("readiness 검사 시각이 필요합니다")
	}
	if strings.TrimSpace(in.LocalEvidenceError) != "" {
		out.ExecutionReady, out.ProtectionReady, out.MutationReady = false, false, false
		appendReadinessBlocker(&out, TradingReadinessLocalEvidenceBlocked, "로컬 증적 검증 실패: "+in.LocalEvidenceError)
	}
	if in.Environment != "demo" {
		out.AccountReady = false
		appendReadinessBlocker(&out, TradingReadinessWrongEnvironment, "자동 주문 후보는 Demo 환경에서만 준비할 수 있습니다")
	}

	maxMarketAge := normalizeDuration(in.MaxMarketAge, 5*time.Second)
	if !in.MarketReady || in.MarketState != "live" {
		out.MarketReady = false
		appendReadinessBlocker(&out, TradingReadinessMarketNotReady, "Demo BTCUSDT 실시간 시장 스트림이 준비되지 않았습니다")
	} else if in.MarketAge < 0 || in.MarketAge > maxMarketAge {
		out.MarketReady = false
		appendReadinessBlocker(&out, TradingReadinessStaleMarket, "실시간 시장 데이터가 freshness 한도를 초과했습니다")
	}

	if in.AccountStatus != "connected" || in.AccountEnvironment != "demo" {
		out.AccountReady = false
		appendReadinessBlocker(&out, TradingReadinessAccountDisconnected, "인증된 Demo 계정 snapshot이 연결 상태가 아닙니다")
	} else {
		maxAccountAge := normalizeDuration(in.MaxAccountAge, 20*time.Second)
		if in.AccountAsOf.IsZero() || in.AccountAsOf.After(now.Add(time.Second)) || now.Sub(in.AccountAsOf) > maxAccountAge {
			out.AccountReady = false
			appendReadinessBlocker(&out, TradingReadinessStaleAccount, "서명 계정 snapshot이 freshness 한도를 초과했습니다")
		}
		if !in.AccountCanTradeKnown {
			out.AccountReady = false
			appendReadinessBlocker(&out, TradingReadinessAccountCapabilityUnknown, "Binance signed account canTrade capability가 아직 독립 검증되지 않았습니다")
		} else if !in.AccountCanTrade {
			out.AccountReady = false
			appendReadinessBlocker(&out, TradingReadinessAccountTradingDisabled, "Binance 계정이 trading 가능 상태가 아닙니다")
		}
	}

	if !in.WalletVerified {
		out.WalletReady = false
		appendReadinessBlocker(&out, TradingReadinessWalletUnverified, "자동매매 내부 배정 지갑이 현재 Demo 계정에 검증되지 않았습니다")
	} else {
		amount, ok := new(big.Rat).SetString(in.WalletAssignedUSDT)
		if !ok || amount.Sign() <= 0 {
			out.WalletReady = false
			appendReadinessBlocker(&out, TradingReadinessWalletEmpty, "자동매매 내부 배정액이 0이거나 유효하지 않습니다")
		}
	}

	if in.MutationGatePresent || in.MutationGateUnknown || (in.CoordinatorLockPresent && !in.CoordinatorLockExpected) {
		out.MutationReady = false
		appendReadinessBlocker(&out, TradingReadinessMutationBlocked, "지갑 mutation gate 또는 예상하지 않은 coordinator lock이 존재합니다")
	}

	switch in.ExecutionAuditState {
	case ExecutionAuditEmpty, ExecutionAuditClear:
	default:
		out.ExecutionReady = false
		appendReadinessBlocker(&out, TradingReadinessExecutionRecovery, "미종료 또는 검증 불가능한 주문 execution lifecycle이 있습니다")
	}

	if in.ProtectionUnsettled < 0 || in.ProtectionUnsettled > 0 || in.ProtectionArmingState == ProtectionArmingPooledConflict {
		out.ProtectionReady = false
		appendReadinessBlocker(&out, TradingReadinessProtectionRecovery, "미완료 또는 충돌 상태의 protection lifecycle이 있습니다")
	}

	out.Ready = out.MarketReady && out.AccountReady && out.WalletReady && out.ExecutionReady && out.ProtectionReady && out.MutationReady && len(out.Blockers) == 0
	// This readiness object only proves that preconditions are presently clean.
	// It is never itself a durable/exchange authorization token.
	out.TradingAuthorized = false
	if out.Ready {
		out.State = TradingReadinessReady
	}
	sort.Strings(out.Blockers)
	return out, nil
}

// AssertTradingReadiness is the common guard entry point for a future executor.
// Supplying a fresh builder on each invocation guarantees the dispatcher and
// sealed executor can call the same authoritative decision twice.
func AssertTradingReadiness(ctxErr error, build func() (TradingReadinessInput, time.Time, error)) error {
	if ctxErr != nil {
		return ctxErr
	}
	if build == nil {
		return errors.New("readiness evidence builder가 필요합니다")
	}
	in, now, err := build()
	if err != nil {
		return err
	}
	r, err := EvaluateTradingReadiness(in, now)
	if err != nil {
		return err
	}
	if !r.Ready {
		return fmt.Errorf("trading readiness 차단 (%s): %s", r.State, strings.Join(r.Blockers, "; "))
	}
	return nil
}

// NewTradingReadinessGuard adapts the authoritative readiness evaluator to the
// durable lifecycle guard signature. The Intent is deliberately not trusted
// as evidence; order-specific validation remains in the executor/preflight.
// Each invocation rebuilds all readiness evidence.
func NewTradingReadinessGuard(build func(context.Context) (TradingReadinessInput, time.Time, error)) func(context.Context, Intent) error {
	return func(ctx context.Context, _ Intent) error {
		if build == nil {
			return errors.New("readiness evidence builder가 필요합니다")
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		in, now, err := build(ctx)
		if err != nil {
			return err
		}
		r, err := EvaluateTradingReadiness(in, now)
		if err != nil {
			return err
		}
		if !r.Ready {
			return fmt.Errorf("trading readiness 차단 (%s): %s", r.State, strings.Join(r.Blockers, "; "))
		}
		return nil
	}
}

// ReviewProtectionReadiness exposes only the protection portion needed by the
// authoritative live readiness builder, without importing unrelated historical
// operator incidents into the live gate.
func ReviewProtectionReadiness(dataDir string) (unsettled int, armingState string, err error) {
	p, _, err := summarizeProtectionForOperator(dataDir, nil)
	if err != nil {
		return 0, "", err
	}
	return p.UnsettledGroupCount, p.ArmingState, nil
}
