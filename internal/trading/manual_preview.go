package trading

// PreviewManualMarket computes an indicative order size, never an execution
// permit. It intentionally supports only the first flat-account Demo market
// entry. Existing BTC positions or ANY observed BTC orders block this preview
// until real ownership/lifecycle integration and signed account-wide recovery
// are validated. Manual margin is separate from the auto-allocation budget,
// but both ultimately share real exchange collateral.
import (
	"binance_trader/internal/exchange"
	"errors"
	"math/big"
	"time"
)

type ManualPreviewInput struct {
	Side       PositionSide `json:"side"`
	OrderType  OrderType    `json:"order_type"`
	Leverage   int          `json:"leverage"`
	MarginUSDT string       `json:"margin_usdt"`
	LimitPrice string       `json:"limit_price,omitempty"`
}
type ManualMarketPreview struct {
	State                  string `json:"state"` // ESTIMATE_ONLY
	QuantityBTC            string `json:"quantity_btc"`
	IndicativePriceUSDT    string `json:"indicative_price_usdt"`
	IndicativeNotionalUSDT string `json:"indicative_notional_usdt"`
	MarginBudgetUSDT       string `json:"margin_budget_usdt"`
	UnspentBudgetUSDT      string `json:"unspent_budget_usdt"`
	ExchangeLeverage       int    `json:"exchange_leverage"`
	Caveat                 string `json:"caveat"`
	CanPlaceOrder          bool   `json:"can_place_order"` // ALWAYS false
}

func PreviewManualMarket(in ManualPreviewInput, rules exchange.SymbolRules, quote Quote,
	account exchange.Snapshot, config exchange.TradingConfiguration, reservedAutoUSDT string, now time.Time) (ManualMarketPreview, error) {
	if in.Side != Long && in.Side != Short || (in.OrderType != Market && in.OrderType != Limit) {
		return ManualMarketPreview{}, errors.New("현재는 첫 번째 수동 시장가/지정가 진입만 서버 사전 점검합니다")
	}
	if in.OrderType == Market && in.LimitPrice != "" {
		return ManualMarketPreview{}, errors.New("시장가 주문에 지정가 가격을 넣을 수 없습니다")
	}
	if in.OrderType == Limit {
		if _, err := pos(in.LimitPrice); err != nil {
			return ManualMarketPreview{}, errors.New("지정가 입력 가격이 잘못되었습니다")
		}
	}
	if in.Leverage < 1 || in.Leverage > 125 {
		return ManualMarketPreview{}, errors.New("레버리지는 1~125 범위입니다")
	}
	margin, err := pos(in.MarginUSDT)
	if err != nil {
		return ManualMarketPreview{}, errors.New("양의 USDT 증거금이 필요합니다")
	}
	if config.Environment != "demo" || config.Symbol != "BTCUSDT" || !config.HedgeMode ||
		config.MarginType != "ISOLATED" || config.AutoAddMargin || config.Leverage != in.Leverage {
		return ManualMarketPreview{}, errors.New("Demo Hedge/격리 설정 또는 요청한 레버리지가 실제 거래소 설정과 다릅니다")
	}
	at, err := time.Parse(time.RFC3339Nano, config.AsOf)
	if err != nil || at.After(now.Add(time.Second)) || now.Sub(at) > 10*time.Second {
		return ManualMarketPreview{}, errors.New("Demo 거래소 설정을 새로 확인해야 합니다")
	}
	if account.Status != "connected" || account.Environment != "demo" || account.Balances == nil ||
		account.Positions == nil || account.Orders == nil || account.AlgoOrders == nil {
		return ManualMarketPreview{}, errors.New("완전한 최신 Demo 계정 데이터가 필요합니다")
	}
	acctAt, err := time.Parse(time.RFC3339Nano, account.AsOf)
	if err != nil || acctAt.After(now.Add(time.Second)) || now.Sub(acctAt) > 5*time.Second {
		return ManualMarketPreview{}, errors.New("Demo 계정 데이터가 5초 이상 오래되었습니다")
	}
	if len(account.Positions) != 0 || len(account.Orders) != 0 || len(account.AlgoOrders) != 0 {
		return ManualMarketPreview{}, errors.New("BTC 포지션 또는 미체결 주문이 있습니다. 독립 소유권 동기화 전 첫 진입 사전 점검 불가")
	}
	found := false
	var available *big.Rat
	for _, b := range account.Balances {
		if b.Asset != "USDT" {
			continue
		}
		if found {
			return ManualMarketPreview{}, errors.New("중복된 USDT 자산 응답")
		}
		found = true
		available, err = decimalRat(b.Available, true)
		if err != nil {
			return ManualMarketPreview{}, errors.New("USDT 가용 잔액 형식 오류")
		}
	}
	if !found {
		return ManualMarketPreview{}, errors.New("Demo 실제 가용 USDT가 없습니다")
	}
	reserved, err := decimalRat(reservedAutoUSDT, true)
	if err != nil {
		return ManualMarketPreview{}, errors.New("검증된 자동매매 전용 지갑 배정액이 없습니다")
	}
	// The first-entry diagnostic conservatively sequesters the ENTIRE local
	// auto wallet before calculating manual collateral. Real reservation-aware
	// free amounts require the later full-account ownership engine.
	manualFree := new(big.Rat).Sub(available, reserved)
	if manualFree.Sign() < 0 || margin.Cmp(manualFree) > 0 {
		return ManualMarketPreview{}, errors.New("자동매매 전용 지갑을 제외한 실제 사용 가능 USDT가 부족합니다")
	}
	if rules.Symbol != "BTCUSDT" || rules.Status != "TRADING" || rules.ContractType != "PERPETUAL" ||
		rules.QuoteAsset != "USDT" || rules.MarginAsset != "USDT" {
		return ManualMarketPreview{}, errors.New("지원하지 않는 종목 또는 거래소 필터")
	}
	budget := EntryBudget{AvailableUSDT: in.MarginUSDT, SharePercent: 100,
		Leverage: in.Leverage, FeeReserveBPS: 20, PriceBufferBPS: 100, Quote: quote, Now: now}
	var size EntrySizing
	if in.OrderType == Limit {
		size, err = SizeLimitEntry(rules, budget, in.LimitPrice)
	} else {
		size, err = SizeMarketEntry(rules, budget)
	}
	if err != nil {
		return ManualMarketPreview{}, err
	}
	return ManualMarketPreview{State: "ESTIMATE_ONLY", QuantityBTC: size.Quantity, IndicativePriceUSDT: size.ReferencePrice,
		IndicativeNotionalUSDT: size.ReferenceNotionalUSDT, MarginBudgetUSDT: size.AllocatedMarginUSDT,
		UnspentBudgetUSDT: size.ReservedHeadroomUSDT, ExchangeLeverage: in.Leverage,
		Caveat:        "시세·수수료·슬리피지·추가 자산 위험 변동과 거래소 체결 여부는 보장되지 않습니다. 이 결과는 주문 승인이나 TP/SL 보호를 의미하지 않습니다.",
		CanPlaceOrder: false}, nil
}
