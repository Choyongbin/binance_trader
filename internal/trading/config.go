package trading

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"binance_trader/internal/exchange"
)

// ConfigPlan has NO side effects; all values reflect the *live* Binance
// account rather than UI assumptions. Switching Hedge Mode is never automatic
// because it applies account-wide, including other Futures products.
type ConfigPlan struct {
	Config                 exchange.TradingConfiguration `json:"exchange_config"`
	NeedsManualHedgeSwitch bool                          `json:"needs_manual_hedge_switch"`
	NeedsIsolated          bool                          `json:"needs_isolated"`
	NeedsLeverage15        bool                          `json:"needs_leverage_15"`
	Blocked                bool                          `json:"blocked"`
	Reasons                []string                      `json:"reasons"`
}

// ValidateConfigPlan checks a fresh, complete account snapshot and exchange
// config before ANY settings mutation (which remains inaccessible via HTTP).
// Presence of unknown data is an error rather than proof there are no orders.
func ValidateConfigPlan(config exchange.TradingConfiguration, account exchange.Snapshot, now time.Time) (ConfigPlan, error) {
	if config.Environment != "demo" || config.Symbol != "BTCUSDT" || config.AsOf == "" {
		return ConfigPlan{}, errors.New("유효한 Demo BTCUSDT 설정 조회가 필요합니다")
	}
	configTime, err := time.Parse(time.RFC3339Nano, config.AsOf)
	if err != nil || configTime.After(now.Add(2*time.Second)) || now.Sub(configTime) > 10*time.Second {
		return ConfigPlan{}, errors.New("거래소 설정 조회가 오래되었거나 시각이 잘못되었습니다")
	}
	if config.MarginType != "ISOLATED" && config.MarginType != "CROSSED" {
		return ConfigPlan{}, errors.New("증거금 모드를 확인할 수 없습니다")
	}
	if config.Leverage < 1 || config.Leverage > 125 {
		return ConfigPlan{}, errors.New("거래소 레버리지 값을 확인할 수 없습니다")
	}
	plan := ConfigPlan{Config: config, NeedsManualHedgeSwitch: !config.HedgeMode, NeedsIsolated: config.MarginType != "ISOLATED", NeedsLeverage15: config.Leverage != 15, Reasons: []string{}}
	if account.Environment != "demo" || account.Status != "connected" || account.AsOf == "" || account.Balances == nil || account.Positions == nil || account.Orders == nil || account.AlgoOrders == nil {
		return ConfigPlan{}, errors.New("Demo 계정의 전체 조회 결과가 필요합니다")
	}
	accountTime, err := time.Parse(time.RFC3339, account.AsOf)
	if err != nil || accountTime.After(now.Add(2*time.Second)) || now.Sub(accountTime) > 10*time.Second {
		return ConfigPlan{}, errors.New("계정 조회 결과가 오래되었습니다")
	}
	if plan.NeedsManualHedgeSwitch {
		plan.Reasons = append(plan.Reasons, "Hedge Mode는 계정 전체 설정이므로 Binance Demo에서 직접 변경한 후 다시 조회해야 합니다")
	}
	if config.AutoAddMargin {
		plan.Reasons = append(plan.Reasons, "자동 격리 증거금 추가가 켜져 있습니다. 설정을 먼저 확인하세요")
	}
	if plan.NeedsIsolated || plan.NeedsLeverage15 {
		if len(account.Orders) > 0 || len(account.AlgoOrders) > 0 {
			plan.Reasons = append(plan.Reasons, "BTCUSDT 미체결 주문이 있어 자동 설정 변경을 허용하지 않습니다")
		}
		for _, p := range account.Positions {
			if p.Symbol != "BTCUSDT" {
				return ConfigPlan{}, errors.New("계정 응답에 예상하지 않은 종목이 있습니다")
			}
			value, err := strconv.ParseFloat(p.Qty, 64)
			if err != nil {
				return ConfigPlan{}, errors.New("포지션 수량이 잘못되어 설정을 진행할 수 없습니다")
			}
			if value != 0 {
				plan.Reasons = append(plan.Reasons, fmt.Sprintf("%s 기존 포지션이 있습니다", strings.ToUpper(p.Side)))
			}
		}
	}
	plan.Blocked = len(plan.Reasons) > 0 || plan.NeedsIsolated || plan.NeedsLeverage15 || plan.NeedsManualHedgeSwitch
	return plan, nil
}
