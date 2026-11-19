package exchange

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"regexp"
	"strings"
	"time"
)

// SymbolRules comes from the *selected Demo exchange* exchangeInfo response.
// Do not substitute quantityPrecision/pricePrecision for stepSize/tickSize.
// Rules are always re-fetched before an order is allowed in the later live UI.
// Other symbols can be added without changing the rule parser.
type SymbolRules struct {
	Symbol       string         `json:"symbol"`
	Status       string         `json:"status"`
	ContractType string         `json:"contract_type"`
	QuoteAsset   string         `json:"quote_asset"`
	MarginAsset  string         `json:"margin_asset"`
	FetchedAt    time.Time      `json:"fetched_at"`
	Lot          QuantityFilter `json:"lot_size"`
	MarketLot    QuantityFilter `json:"market_lot_size"`
	Price        PriceFilter    `json:"price_filter"`
	MinNotional  string         `json:"min_notional"`
}
type QuantityFilter struct {
	Min  string `json:"min_qty"`
	Max  string `json:"max_qty"`
	Step string `json:"step_size"`
}
type PriceFilter struct {
	Min  string `json:"min_price"`
	Max  string `json:"max_price"`
	Tick string `json:"tick_size"`
}

// Up to 12 fractional digits are enough for the current USD-M contracts and
// keep the integer quantizer bounded. Explicitly refuse unknown precision.
var ruleDecimal = regexp.MustCompile(`^(?:0|[1-9][0-9]{0,11})(?:\.[0-9]{1,12})?$`)

func ruleRat(s string) (*big.Rat, error) {
	if !ruleDecimal.MatchString(s) {
		return nil, fmt.Errorf("거래소 필터 값이 유효하지 않습니다: %q", s)
	}
	r, ok := new(big.Rat).SetString(s)
	if !ok {
		return nil, errors.New("잘못된 거래소 필터 수치")
	}
	return r, nil
}
func positiveRule(s string) error {
	r, e := ruleRat(s)
	if e != nil {
		return e
	}
	if r.Sign() <= 0 {
		return errors.New("필수 거래소 필터가 0입니다")
	}
	return nil
}
func nonnegativeRule(s string) error { _, e := ruleRat(s); return e }

// ParseSymbolRules validates required filters and never silently assumes a
// default lot size, tick, or minimum notional when exchangeInfo is incomplete.
func ParseSymbolRules(raw []byte, symbol string, at time.Time) (SymbolRules, error) {
	if !regexp.MustCompile(`^[A-Z0-9]{3,20}$`).MatchString(symbol) {
		return SymbolRules{}, errors.New("종목 형식이 잘못되었습니다")
	}
	var env struct {
		Symbols []struct {
			Symbol, Status, ContractType, QuoteAsset, MarginAsset string
			Filters                                               []struct {
				FilterType, MinQty, MaxQty, StepSize, MinPrice, MaxPrice, TickSize, Notional, MinNotional string
			}
		}
	}
	if err := json.Unmarshal(raw, &env); err != nil || env.Symbols == nil {
		return SymbolRules{}, errors.New("거래소 필터 응답 형식 오류")
	}
	var r SymbolRules
	found := 0
	for _, s := range env.Symbols {
		if s.Symbol != symbol {
			continue
		}
		found++
		if found > 1 {
			return SymbolRules{}, errors.New("거래소에 중복된 종목 정보가 있습니다")
		}
		if s.Status != "TRADING" || s.ContractType != "PERPETUAL" || s.QuoteAsset != "USDT" || s.MarginAsset != "USDT" {
			return SymbolRules{}, errors.New("USDT 무기한 거래 가능 종목이 아닙니다")
		}
		r = SymbolRules{Symbol: symbol, Status: s.Status, ContractType: s.ContractType, QuoteAsset: s.QuoteAsset, MarginAsset: s.MarginAsset, FetchedAt: at.UTC()}
		var lot, market, price, notional bool
		for _, f := range s.Filters {
			switch f.FilterType {
			case "LOT_SIZE":
				if lot {
					return SymbolRules{}, errors.New("중복 LOT_SIZE")
				}
				lot = true
				r.Lot = QuantityFilter{f.MinQty, f.MaxQty, f.StepSize}
			case "MARKET_LOT_SIZE":
				if market {
					return SymbolRules{}, errors.New("중복 MARKET_LOT_SIZE")
				}
				market = true
				r.MarketLot = QuantityFilter{f.MinQty, f.MaxQty, f.StepSize}
			case "PRICE_FILTER":
				if price {
					return SymbolRules{}, errors.New("중복 PRICE_FILTER")
				}
				price = true
				r.Price = PriceFilter{f.MinPrice, f.MaxPrice, f.TickSize}
			case "MIN_NOTIONAL", "NOTIONAL":
				value := f.Notional
				if f.FilterType == "NOTIONAL" {
					value = f.MinNotional
				}
				if err := positiveRule(value); err != nil {
					return SymbolRules{}, fmt.Errorf("최소 명목가치: %w", err)
				}
				if !notional {
					r.MinNotional = value
					notional = true
				} else {
					prev, _ := ruleRat(r.MinNotional)
					next, _ := ruleRat(value)
					if prev.Cmp(next) < 0 {
						r.MinNotional = value
					}
				}
			}
		}
		if !lot || !market || !price || !notional {
			return SymbolRules{}, errors.New("필수 LOT_SIZE/MARKET_LOT_SIZE/PRICE_FILTER/MIN_NOTIONAL 필터가 누락되었습니다")
		}
		for _, value := range []string{r.Lot.Min, r.Lot.Max, r.Lot.Step, r.MarketLot.Min, r.MarketLot.Max, r.MarketLot.Step, r.Price.Min, r.Price.Max, r.Price.Tick} {
			if err := nonnegativeRule(value); err != nil {
				return SymbolRules{}, err
			}
		}
		if err := positiveRule(r.Lot.Min); err != nil {
			return SymbolRules{}, err
		}
		if err := positiveRule(r.Lot.Max); err != nil {
			return SymbolRules{}, err
		}
		if err := positiveRule(r.Lot.Step); err != nil {
			return SymbolRules{}, err
		}
		if err := positiveRule(r.Price.Tick); err != nil {
			return SymbolRules{}, err
		}
		if err := positiveRule(r.Price.Min); err != nil {
			return SymbolRules{}, err
		}
		if err := positiveRule(r.Price.Max); err != nil {
			return SymbolRules{}, err
		}
		min, _ := ruleRat(r.Lot.Min)
		max, _ := ruleRat(r.Lot.Max)
		if min.Cmp(max) > 0 {
			return SymbolRules{}, errors.New("최소 수량이 최대 수량보다 큽니다")
		}
		min, _ = ruleRat(r.Price.Min)
		max, _ = ruleRat(r.Price.Max)
		if min.Cmp(max) > 0 {
			return SymbolRules{}, errors.New("가격 필터의 상하한이 역전되었습니다")
		}
		// Binance documents 0 as a disabled MARKET_LOT_SIZE sub-filter.
		min, _ = ruleRat(r.MarketLot.Min)
		max, _ = ruleRat(r.MarketLot.Max)
		if max.Sign() > 0 && min.Cmp(max) > 0 {
			return SymbolRules{}, errors.New("시장가 수량 필터의 상하한이 역전되었습니다")
		}
	}
	if found == 0 {
		return SymbolRules{}, fmt.Errorf("거래소에 %s 정보가 없습니다", strings.ToUpper(symbol))
	}
	return r, nil
}

func (c *Client) FetchSymbolRules(ctx context.Context, symbol string) (SymbolRules, error) {
	if c.base != DemoBase && !c.localMock {
		return SymbolRules{}, errors.New("Demo 전용 거래소 필터 조회입니다")
	}
	raw, err := c.get(ctx, "/fapi/v1/exchangeInfo", nil, "")
	if err != nil {
		return SymbolRules{}, err
	}
	return ParseSymbolRules(raw, symbol, c.now())
}
