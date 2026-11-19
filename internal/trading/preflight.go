package trading

import (
	"errors"
	"fmt"
	"time"

	"binance_trader/internal/exchange"
)

// ValidateFirstDemoMarketEntry is a pure preflight for the first manual Demo
// market-entry milestone. It intentionally blocks ANY existing BTC position,
// open order, or algo order until a reconciled ownership ledger is available.
// It is NOT sufficient for multi-model order authorization; those remain locked.
func ValidateFirstDemoMarketEntry(
	intent Intent, plan PlannedOrder, rules exchange.SymbolRules, quote Quote,
	account exchange.Snapshot, config exchange.TradingConfiguration,
	sharePercent int, now time.Time,
) (EntrySizing, error) {
	canonical, err := intent.Plan()
	if err != nil {
		return EntrySizing{}, err
	}
	if canonical != plan || intent.Source != Manual || intent.Action != Enter || intent.OrderType != Market {
		return EntrySizing{}, errors.New("최초 Demo 수동 시장가 진입만 사전 점검할 수 있습니다")
	}
	if config.Environment != "demo" || config.Symbol != "BTCUSDT" || !config.HedgeMode || config.MarginType != "ISOLATED" || config.Leverage != 15 || config.AutoAddMargin {
		return EntrySizing{}, errors.New("Demo Hedge/격리/15배 설정이 검증되지 않았습니다")
	}
	cfgAt, err := time.Parse(time.RFC3339Nano, config.AsOf)
	if err != nil || cfgAt.After(now.Add(time.Second)) || now.Sub(cfgAt) > 10*time.Second {
		return EntrySizing{}, errors.New("거래소 설정이 오래되었습니다")
	}
	if account.Status != "connected" || account.Environment != "demo" || account.Balances == nil || account.Positions == nil || account.Orders == nil || account.AlgoOrders == nil {
		return EntrySizing{}, errors.New("완전한 Demo 계정 조회가 필요합니다")
	}
	acctAt, err := time.Parse(time.RFC3339Nano, account.AsOf)
	if err != nil || acctAt.After(now.Add(time.Second)) || now.Sub(acctAt) > 5*time.Second {
		return EntrySizing{}, errors.New("Demo 계정 조회가 오래되었습니다")
	}
	if len(account.Positions) > 0 || len(account.Orders) > 0 || len(account.AlgoOrders) > 0 {
		return EntrySizing{}, errors.New("기존 BTC 포지션 또는 주문이 있습니다. 소유권 동기화 이전에는 추가 진입할 수 없습니다")
	}
	available := ""
	for _, balance := range account.Balances {
		if balance.Asset == "USDT" {
			if available != "" {
				return EntrySizing{}, errors.New("USDT 자산 응답이 중복되었습니다")
			}
			available = balance.Available
		}
	}
	if available == "" {
		return EntrySizing{}, errors.New("사용 가능 USDT 금액을 확인하지 못했습니다")
	}
	size, err := SizeMarketEntry(rules, EntryBudget{
		AvailableUSDT: available, SharePercent: sharePercent, Leverage: 15,
		FeeReserveBPS: 20, PriceBufferBPS: 100, Quote: quote, Now: now,
	})
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
	validationPrice := quote.Price
	if plan.Type == Limit {
		validationPrice = plan.Price
	}
	if err := ValidatePlannedOrder(plan, rules, validationPrice, now); err != nil {
		return EntrySizing{}, err
	}
	return size, nil
}
