package trading

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"binance_trader/internal/exchange"
)

type ExecutionPreflightReader interface {
	FetchDemoExecutionPreflight(context.Context, string, string, string) (exchange.ExecutionPreflightSnapshot, error)
}

// NewSignedTradingReadinessGuard upgrades the authoritative local readiness
// with an independent signed Demo account bracket on every invocation. It is
// safe to call both before MAY_HAVE_SENT and again from the sealed executor.
func NewSignedTradingReadinessGuard(
	buildLocal func(context.Context) (TradingReadinessInput, time.Time, error),
	reader ExecutionPreflightReader,
	credentials CredentialsProvider,
	symbol string,
) func(context.Context, Intent) error {
	return func(ctx context.Context, _ Intent) error {
		if buildLocal == nil || reader == nil || credentials == nil {
			return errors.New("signed readiness 필수 구성요소가 없습니다")
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		in, now, err := buildLocal(ctx)
		if err != nil {
			return err
		}
		creds, err := credentials(ctx)
		if err != nil {
			return err
		}
		if creds.APIKey == "" || creds.Secret == "" {
			return errors.New("Demo 키가 잠겨 있습니다")
		}
		signed, err := reader.FetchDemoExecutionPreflight(ctx, creds.APIKey, creds.Secret, symbol)
		if err != nil {
			return fmt.Errorf("signed execution preflight 실패: %w", err)
		}
		if signed.Environment != "demo" || signed.Configuration.Environment != "demo" || signed.Configuration.Symbol != symbol || !signed.Configuration.SettingsMatchV1 {
			return errors.New("Demo Hedge/격리/15배 실행 설정이 검증되지 않았습니다")
		}
		in.AccountStatus = "connected"
		in.AccountEnvironment = "demo"
		in.AccountAsOf = signed.FinishedAt
		in.AccountCanTradeKnown = true
		in.AccountCanTrade = signed.Portfolio.CanTrade
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

func nonZeroDecimal(v string) (bool, error) {
	q, ok := new(big.Rat).SetString(v)
	if !ok {
		return false, errors.New("계정 수량 형식 오류")
	}
	return q.Sign() != 0, nil
}

// ValidateFirstDemoMarketEntryFresh is the first-order milestone validator
// using the bracketed signed execution snapshot, including canTrade.
func ValidateFirstDemoMarketEntryFresh(intent Intent, plan PlannedOrder, rules exchange.SymbolRules, quote Quote, snap exchange.ExecutionPreflightSnapshot, sharePercent int, now time.Time) (EntrySizing, error) {
	canonical, err := intent.Plan()
	if err != nil {
		return EntrySizing{}, err
	}
	if canonical != plan || intent.Source != Manual || intent.Action != Enter || intent.OrderType != Market {
		return EntrySizing{}, errors.New("최초 Demo 수동 시장가 진입만 사전 점검할 수 있습니다")
	}
	if snap.Environment != "demo" || !snap.Portfolio.CanTrade {
		return EntrySizing{}, errors.New("Binance Demo 계정이 trading 가능 상태가 아닙니다")
	}
	if snap.FinishedAt.IsZero() || snap.FinishedAt.After(now.Add(time.Second)) || now.Sub(snap.FinishedAt) > 5*time.Second {
		return EntrySizing{}, errors.New("signed execution preflight가 오래되었습니다")
	}
	c := snap.Configuration
	if c.Environment != "demo" || c.Symbol != "BTCUSDT" || !c.SettingsMatchV1 || !c.HedgeMode || c.MarginType != "ISOLATED" || c.Leverage != 15 || c.AutoAddMargin {
		return EntrySizing{}, errors.New("Demo Hedge/격리/15배 설정이 검증되지 않았습니다")
	}
	if len(snap.OpenOrders) > 0 || len(snap.OpenAlgos) > 0 {
		return EntrySizing{}, errors.New("미체결 주문이 있습니다. 소유권 동기화 이전에는 추가 진입할 수 없습니다")
	}
	for _, p := range snap.Portfolio.Positions {
		nz, err := nonZeroDecimal(string(p.PositionAmt))
		if err != nil {
			return EntrySizing{}, err
		}
		if nz {
			return EntrySizing{}, errors.New("기존 계정 포지션이 있습니다. 소유권 동기화 이전에는 추가 진입할 수 없습니다")
		}
	}
	available := ""
	for _, a := range snap.Portfolio.Assets {
		if a.Asset == "USDT" {
			if available != "" {
				return EntrySizing{}, errors.New("USDT 자산 응답이 중복되었습니다")
			}
			available = string(a.AvailableBalance)
		}
	}
	if available == "" {
		return EntrySizing{}, errors.New("사용 가능 USDT 금액을 확인하지 못했습니다")
	}
	size, err := SizeMarketEntry(rules, EntryBudget{AvailableUSDT: available, SharePercent: sharePercent, Leverage: 15, FeeReserveBPS: 20, PriceBufferBPS: 100, Quote: quote, Now: now})
	if err != nil {
		return EntrySizing{}, err
	}
	actual, err := pos(plan.Quantity)
	if err != nil {
		return EntrySizing{}, err
	}
	maximum, err := pos(size.Quantity)
	if err != nil {
		return EntrySizing{}, err
	}
	if actual.Cmp(maximum) > 0 {
		return EntrySizing{}, fmt.Errorf("주문 수량이 신규 진입 예산을 초과합니다 (최대 %s BTC)", size.Quantity)
	}
	if err := ValidatePlannedOrder(plan, rules, quote.Price, now); err != nil {
		return EntrySizing{}, err
	}
	return size, nil
}
