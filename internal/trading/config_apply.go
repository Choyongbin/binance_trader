package trading

import (
	"context"
	"errors"
	"fmt"
	"time"

	"binance_trader/internal/exchange"
)

// ConfigBackend binds the credentials inside an adapter; the application
// must not pass secrets through the UI or persist them in this package.
// Not instantiated by the production web server in this release.
type ConfigBackend interface {
	Current(context.Context) (exchange.TradingConfiguration, error)
	SetIsolated(context.Context) error
	SetLeverage(context.Context, int) error
}

// ApplyDemoConfig is for an EXPLICIT, operator-confirmed setup flow.
// Only per-symbol margin and leverage are applied. Hedge Mode is global and
// must be changed manually outside this program, with all Futures products
// checked. This routine cannot eliminate concurrent external account edits;
// before wiring it to a live UI, account-wide reconciliation and server-side
// operator authorization are required.
func ApplyDemoConfig(ctx context.Context, backend ConfigBackend, account exchange.Snapshot, consent string) (exchange.TradingConfiguration, error) {
	if backend == nil {
		return exchange.TradingConfiguration{}, errors.New("데모 설정 어댑터가 없습니다")
	}
	if consent != "DEMO BTCUSDT 격리 15배 설정" {
		return exchange.TradingConfiguration{}, errors.New("명시적인 Demo 설정 변경 동의가 필요합니다")
	}
	before, err := backend.Current(ctx)
	if err != nil {
		return exchange.TradingConfiguration{}, err
	}
	plan, err := ValidateConfigPlan(before, account, time.Now().UTC())
	if err != nil {
		return exchange.TradingConfiguration{}, err
	}
	if len(plan.Reasons) != 0 {
		return exchange.TradingConfiguration{}, fmt.Errorf("설정 변경 중단: %s", plan.Reasons[0])
	}
	if !before.HedgeMode || before.AutoAddMargin {
		return exchange.TradingConfiguration{}, errors.New("설정 변경 전에 Hedge Mode 및 자동 증거금을 확인하세요")
	}
	if plan.NeedsIsolated {
		if err := backend.SetIsolated(ctx); err != nil {
			return exchange.TradingConfiguration{}, fmt.Errorf("격리 설정 결과 불확실: %w; 반드시 다시 조회하세요", err)
		}
		before, err = backend.Current(ctx)
		if err != nil {
			return exchange.TradingConfiguration{}, fmt.Errorf("격리 설정 후 확인 실패: %w", err)
		}
		if before.Environment != "demo" || before.Symbol != "BTCUSDT" || !before.HedgeMode || before.MarginType != "ISOLATED" {
			return exchange.TradingConfiguration{}, errors.New("격리 설정 변경 후 검증 실패. 추가 설정을 수행하지 않습니다")
		}
	}
	if before.Leverage != 15 {
		if err := backend.SetLeverage(ctx, 15); err != nil {
			return exchange.TradingConfiguration{}, fmt.Errorf("레버리지 변경 결과 불확실: %w; 반드시 다시 조회하세요", err)
		}
		before, err = backend.Current(ctx)
		if err != nil {
			return exchange.TradingConfiguration{}, fmt.Errorf("레버리지 변경 후 확인 실패: %w", err)
		}
	}
	if before.Environment != "demo" || before.Symbol != "BTCUSDT" || !before.HedgeMode || before.MarginType != "ISOLATED" || before.Leverage != 15 || before.AutoAddMargin {
		return exchange.TradingConfiguration{}, errors.New("최종 거래 설정이 예상과 다릅니다. 주문 실행을 허용하지 않습니다")
	}
	return before, nil
}
