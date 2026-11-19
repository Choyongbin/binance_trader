package trading

import (
	"errors"
	"fmt"
	"math/big"
	"regexp"
	"strings"
	"time"

	"binance_trader/internal/exchange"
)

// Use exact base-10 rationals throughout. Float-based rounding can exceed
// available collateral or violate LOT_SIZE near an exchange filter boundary.
var decRE = regexp.MustCompile(`^(?:0|[1-9][0-9]{0,11})(?:\.[0-9]{1,12})?$`)

func dec(s string) (*big.Rat, error) {
	if !decRE.MatchString(s) {
		return nil, fmt.Errorf("십진 값이 올바르지 않습니다: %q", s)
	}
	out, ok := new(big.Rat).SetString(s)
	if !ok {
		return nil, errors.New("십진 파싱 실패")
	}
	return out, nil
}
func pos(s string) (*big.Rat, error) {
	r, e := dec(s)
	if e != nil {
		return nil, e
	}
	if r.Sign() <= 0 {
		return nil, errors.New("0보다 큰 값이 필요합니다")
	}
	return r, nil
}
func ratMul(a, b *big.Rat) *big.Rat { return new(big.Rat).Mul(a, b) }
func ratDiv(a, b *big.Rat) *big.Rat { return new(big.Rat).Quo(a, b) }
func ratToString(r *big.Rat) string {
	s := r.FloatString(12)
	if strings.Contains(s, ".") {
		s = strings.TrimRight(strings.TrimRight(s, "0"), ".")
	}
	return s
}

// decimalStepInteger converts a decimal step to exact integer 10^-12 units.
func decimalStepInteger(s string) (*big.Int, error) {
	r, e := pos(s)
	if e != nil {
		return nil, e
	}
	scaled := ratMul(r, new(big.Rat).SetInt64(1000000000000))
	if !scaled.IsInt() {
		return nil, errors.New("12자리보다 정밀한 거래소 단위는 지원하지 않습니다")
	}
	return new(big.Int).Set(scaled.Num()), nil
}
func lcm(a, b *big.Int) *big.Int {
	g := new(big.Int).GCD(nil, nil, a, b)
	return new(big.Int).Mul(new(big.Int).Quo(new(big.Int).Set(a), g), b)
}
func unitToRat(u *big.Int) *big.Rat { return new(big.Rat).SetFrac(u, big.NewInt(1000000000000)) }
func floorToStep(amount *big.Rat, step *big.Int) *big.Rat {
	units := ratMul(amount, new(big.Rat).SetInt64(1000000000000))
	quot := new(big.Int).Quo(units.Num(), new(big.Int).Mul(units.Denom(), step))
	return unitToRat(quot.Mul(quot, step))
}
func onGrid(value *big.Rat, stepStr string) (bool, error) {
	step, e := dec(stepStr)
	if e != nil {
		return false, e
	}
	if step.Sign() == 0 {
		return true, nil
	}
	ratio := ratDiv(value, step)
	return ratio.IsInt(), nil
}
func between(v *big.Rat, minS, maxS string) (bool, error) {
	min, e := dec(minS)
	if e != nil {
		return false, e
	}
	max, e := dec(maxS)
	if e != nil {
		return false, e
	}
	return (min.Sign() == 0 || v.Cmp(min) >= 0) && (max.Sign() == 0 || v.Cmp(max) <= 0), nil
}

// Quote must come from the backend's live, validated BTCUSDT stream, not an
// untrusted browser price or an earlier candle. It is explicitly time bound.
type Quote struct {
	Symbol     string
	Price      string
	ReceivedAt time.Time
}
type EntryBudget struct {
	AvailableUSDT  string // fresh balance from an authenticated account read
	SharePercent   int    // 1..100; first validation model uses 10
	Leverage       int    // first model uses 15; actual exchange settings must agree
	FeeReserveBPS  int    // pessimistic *one-way* taker fee reserve, e.g. 20 = 0.20%
	PriceBufferBPS int    // size market orders with additional price buffer
	Quote          Quote
	Now            time.Time
}
type EntrySizing struct {
	Symbol                string `json:"symbol"`
	AvailableUSDT         string `json:"available_usdt"`
	AllocatedMarginUSDT   string `json:"allocated_margin_usdt"`
	ReservedHeadroomUSDT  string `json:"reserved_headroom_usdt"`
	Leverage              int    `json:"leverage"`
	Quantity              string `json:"quantity"`
	ReferencePrice        string `json:"reference_price"`
	ReferenceNotionalUSDT string `json:"reference_notional_usdt"`
}

// ValidatePlannedOrder validates *actual planned quantity* against both
// LOT_SIZE and, for MARKET orders, MARKET_LOT_SIZE, plus price tick/notional.
// ReferencePrice must be an independently obtained, fresh market quote for
// MARKET (the final market execution price is inherently not guaranteed).
func ValidatePlannedOrder(p PlannedOrder, r exchange.SymbolRules, referencePrice string, now time.Time) error {
	if p.Symbol != "BTCUSDT" || r.Symbol != p.Symbol || r.Status != "TRADING" || r.ContractType != "PERPETUAL" || r.MarginAsset != "USDT" || r.QuoteAsset != "USDT" {
		return errors.New("거래 가능 Demo BTCUSDT 규칙이 아닙니다")
	}
	if r.FetchedAt.IsZero() || r.FetchedAt.After(now.Add(time.Second)) || now.Sub(r.FetchedAt) > 60*time.Second {
		return errors.New("거래소 필터가 오래되었습니다. 주문 직전 재조회하세요")
	}
	if p.PositionSide != Long && p.PositionSide != Short {
		return errors.New("잘못된 포지션 방향")
	}
	if (p.PositionSide == Long && p.ExchangeSide != "BUY" && p.ExchangeSide != "SELL") || (p.PositionSide == Short && p.ExchangeSide != "BUY" && p.ExchangeSide != "SELL") {
		return errors.New("주문 방향 오류")
	}
	if !regexp.MustCompile(`^[.A-Z:/a-z0-9_-]{1,36}$`).MatchString(p.ClientOrderID) {
		return errors.New("클라이언트 주문 ID 형식 오류")
	}
	qty, e := pos(p.Quantity)
	if e != nil {
		return e
	}
	ok, e := between(qty, r.Lot.Min, r.Lot.Max)
	if e != nil || !ok {
		return errors.New("LOT_SIZE 최소/최대 수량 위반")
	}
	if ok, e = onGrid(qty, r.Lot.Step); e != nil || !ok {
		return errors.New("LOT_SIZE 수량 단위 위반")
	}
	switch p.Type {
	case Market:
		if p.Price != "" || p.TimeInForce != "" {
			return errors.New("시장가 주문에 지정가 전용 필드가 있습니다")
		}
		if ok, e = between(qty, r.MarketLot.Min, r.MarketLot.Max); e != nil || !ok {
			return errors.New("MARKET_LOT_SIZE 수량 범위 위반")
		}
		if ok, e = onGrid(qty, r.MarketLot.Step); e != nil || !ok {
			return errors.New("MARKET_LOT_SIZE 수량 단위 위반")
		}
	case Limit:
		if p.TimeInForce != "GTC" {
			return errors.New("지정가에는 명시적인 GTC가 필요합니다")
		}
		price, e := pos(p.Price)
		if e != nil {
			return e
		}
		if ok, e = between(price, r.Price.Min, r.Price.Max); e != nil || !ok {
			return errors.New("PRICE_FILTER 가격 범위 위반")
		}
		if ok, e = onGrid(price, r.Price.Tick); e != nil || !ok {
			return errors.New("PRICE_FILTER 호가 단위 위반")
		}
	default:
		return errors.New("시장가/지정가 이외 주문 비활성화")
	}
	// LIMIT notional must use the submitted price, independent of the
	// caller's market reference used for MARKET orders.
	if p.Type == Limit {
		referencePrice = p.Price
	}
	ref, e := pos(referencePrice)
	if e != nil {
		return e
	}
	notional := ratMul(qty, ref)
	min, e := pos(r.MinNotional)
	if e != nil {
		return e
	}
	if notional.Cmp(min) < 0 {
		return errors.New("최소 주문 명목가치에 미달합니다")
	}
	return nil
}

// SizeMarketEntry computes the largest *aligned* BTC quantity inside the
// requested initial-margin budget plus explicit fee and adverse-price buffers.
// This is a preliminary sizing quote, never an authorization to trade. Real
// placement will require separate fresh config/account/rule/preflight checks.
func SizeMarketEntry(r exchange.SymbolRules, b EntryBudget) (EntrySizing, error) {
	now := b.Now
	if b.Quote.Symbol != r.Symbol || b.Quote.Symbol != "BTCUSDT" || b.Quote.ReceivedAt.IsZero() || b.Quote.ReceivedAt.After(now.Add(time.Second)) || now.Sub(b.Quote.ReceivedAt) > 3*time.Second {
		return EntrySizing{}, errors.New("실시간 가격이 없거나 3초 이상 지연되었습니다")
	}
	if b.SharePercent < 1 || b.SharePercent > 100 || b.Leverage < 1 || b.Leverage > 125 || b.FeeReserveBPS < 1 || b.FeeReserveBPS > 100 || b.PriceBufferBPS < 0 || b.PriceBufferBPS > 500 {
		return EntrySizing{}, errors.New("투입 비율/레버리지/비용 완충값이 유효하지 않습니다")
	}
	price, e := pos(b.Quote.Price)
	if e != nil {
		return EntrySizing{}, e
	}
	available, e := pos(b.AvailableUSDT)
	if e != nil {
		return EntrySizing{}, e
	}
	budget := ratDiv(ratMul(available, big.NewRat(int64(b.SharePercent), 1)), big.NewRat(100, 1))
	lev := big.NewRat(int64(b.Leverage), 1)
	// On adverse movement, margin + fee reserve must stay within allocation.
	adverse := new(big.Rat).Add(big.NewRat(1, 1), big.NewRat(int64(b.PriceBufferBPS), 10000))
	costPerNotional := new(big.Rat).Add(ratDiv(big.NewRat(1, 1), lev), big.NewRat(int64(b.FeeReserveBPS), 10000))
	denom := ratMul(ratMul(price, adverse), costPerNotional)
	allowedQty := ratDiv(budget, denom)
	lotStep, e := decimalStepInteger(r.Lot.Step)
	if e != nil {
		return EntrySizing{}, e
	}
	marketStep, e := dec(r.MarketLot.Step)
	if e != nil {
		return EntrySizing{}, e
	}
	common := lotStep
	if marketStep.Sign() > 0 {
		marketUnits, err := decimalStepInteger(r.MarketLot.Step)
		if err != nil {
			return EntrySizing{}, err
		}
		common = lcm(common, marketUnits)
	}
	qty := floorToStep(allowedQty, common)
	if qty.Sign() == 0 {
		return EntrySizing{}, errors.New("예산으로 주문 가능한 최소 단위에 미달합니다")
	}
	check := PlannedOrder{ClientOrderID: "preview_1234", Symbol: b.Quote.Symbol, PositionSide: Long, ExchangeSide: "BUY", Type: Market, Quantity: ratToString(qty)}
	if err := ValidatePlannedOrder(check, r, b.Quote.Price, now); err != nil {
		return EntrySizing{}, err
	}
	used := ratMul(qty, denom)
	headroom := new(big.Rat).Sub(budget, used)
	if headroom.Sign() < 0 {
		return EntrySizing{}, errors.New("계산된 주문 금액이 예산을 초과했습니다")
	}
	return EntrySizing{Symbol: r.Symbol, AvailableUSDT: b.AvailableUSDT, AllocatedMarginUSDT: ratToString(budget), ReservedHeadroomUSDT: ratToString(headroom), Leverage: b.Leverage, Quantity: ratToString(qty), ReferencePrice: b.Quote.Price, ReferenceNotionalUSDT: ratToString(ratMul(qty, price))}, nil
}

// SizeLimitEntry uses the user's explicitly supplied *limit price* for the
// indicative size and enforces actual PRICE_FILTER and LOT_SIZE (not the
// MARKET_LOT_SIZE). It still demands a fresh independent WS quote to avoid
// offering a stale account/market preview. No real placement is authorized.
func SizeLimitEntry(r exchange.SymbolRules, b EntryBudget, limitPrice string) (EntrySizing, error) {
	now := b.Now
	if b.Quote.Symbol != "BTCUSDT" || b.Quote.Symbol != r.Symbol || b.Quote.ReceivedAt.IsZero() ||
		b.Quote.ReceivedAt.After(now.Add(time.Second)) || now.Sub(b.Quote.ReceivedAt) > 3*time.Second {
		return EntrySizing{}, errors.New("실시간 가격이 지연되었습니다")
	}
	if b.SharePercent < 1 || b.SharePercent > 100 || b.Leverage < 1 || b.Leverage > 125 || b.FeeReserveBPS < 1 || b.FeeReserveBPS > 100 || b.PriceBufferBPS < 0 || b.PriceBufferBPS > 500 {
		return EntrySizing{}, errors.New("투입 비율·비용 완충 설정 오류")
	}
	if _, err := pos(b.Quote.Price); err != nil {
		return EntrySizing{}, errors.New("실시간 가격 오류")
	}
	price, err := pos(limitPrice)
	if err != nil {
		return EntrySizing{}, err
	}
	available, err := pos(b.AvailableUSDT)
	if err != nil {
		return EntrySizing{}, err
	}
	allocated := ratDiv(ratMul(available, big.NewRat(int64(b.SharePercent), 1)), big.NewRat(100, 1))
	adverse := new(big.Rat).Add(big.NewRat(1, 1), big.NewRat(int64(b.PriceBufferBPS), 10000))
	cost := new(big.Rat).Add(ratDiv(big.NewRat(1, 1), big.NewRat(int64(b.Leverage), 1)), big.NewRat(int64(b.FeeReserveBPS), 10000))
	denom := ratMul(ratMul(price, adverse), cost)
	step, err := decimalStepInteger(r.Lot.Step)
	if err != nil {
		return EntrySizing{}, err
	}
	qty := floorToStep(ratDiv(allocated, denom), step)
	if qty.Sign() == 0 {
		return EntrySizing{}, errors.New("지정가 예산으로 최소 수량 주문 불가")
	}
	check := PlannedOrder{ClientOrderID: "preview_limit_123", Symbol: "BTCUSDT", PositionSide: Long, ExchangeSide: "BUY", Type: Limit,
		Quantity: ratToString(qty), Price: limitPrice, TimeInForce: "GTC"}
	if err := ValidatePlannedOrder(check, r, limitPrice, now); err != nil {
		return EntrySizing{}, err
	}
	used := ratMul(qty, denom)
	headroom := new(big.Rat).Sub(allocated, used)
	if headroom.Sign() < 0 {
		return EntrySizing{}, errors.New("지정가 예산 검증 오류")
	}
	return EntrySizing{Symbol: r.Symbol, AvailableUSDT: b.AvailableUSDT, AllocatedMarginUSDT: ratToString(allocated),
		ReservedHeadroomUSDT: ratToString(headroom), Leverage: b.Leverage, Quantity: ratToString(qty), ReferencePrice: limitPrice,
		ReferenceNotionalUSDT: ratToString(ratMul(qty, price))}, nil
}
