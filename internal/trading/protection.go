package trading

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math/big"
	"time"

	"binance_trader/internal/exchange"
)

// ProtectionPreview is a pure deterministic draft of model/manual owner-sized
// Binance USD-M Algo orders. NEVER submit these independently: Binance does
// not guarantee that sibling TP/SL orders cancel each other. The unified arming/settlement
// coordinators monitor a trigger, cancel/read back the sibling, and verify
// exchange positions before permitting ANY other exit or re-entry.
// Hedge Mode prohibits reduceOnly and closePosition=true would close OTHER
// models' same-side holdings; both fields are intentionally absent.
type ProtectionInstruction struct {
	ClientAlgoID string       `json:"client_algo_id"`
	Symbol       string       `json:"symbol"`
	Side         string       `json:"side"`
	PositionSide PositionSide `json:"position_side"`
	AlgoType     string       `json:"algo_type"`
	OrderType    string       `json:"order_type"`
	Quantity     string       `json:"quantity"`
	TriggerPrice string       `json:"trigger_price"`
	WorkingType  string       `json:"working_type"`
}
type ProtectionPreview struct {
	TP                    ProtectionInstruction `json:"tp"`
	SL                    ProtectionInstruction `json:"sl"`
	Leverage              int                   `json:"leverage"`
	TargetGrossROI        string                `json:"target_gross_roi"`
	DoesNotIncludeCosts   bool                  `json:"does_not_include_costs"`
	LiveSubmissionEnabled bool                  `json:"live_submission_enabled"`
}

// roundTick floors or ceils in exact rational arithmetic (positive prices).
func roundTick(x, tick *big.Rat, upward bool) *big.Rat {
	ratio := new(big.Rat).Quo(x, tick)
	q, rem := new(big.Int).QuoRem(ratio.Num(), ratio.Denom(), new(big.Int))
	if upward && rem.Sign() != 0 {
		q.Add(q, big.NewInt(1))
	}
	return new(big.Rat).Mul(new(big.Rat).SetInt(q), tick)
}
func protectionAlgoID(groupID string, owner OwnerPosition, kind string) string {
	sum := sha256.Sum256([]byte("DEMO|BTCUSDT|" + groupID + "|" + string(owner.Source) + "|" + owner.ModelID + "|" + string(owner.Side) + "|" + kind))
	return "btp_" + hex.EncodeToString(sum[:14])
}

func PreviewProtection(owner OwnerPosition, rules exchange.SymbolRules, contract Quote, mark Quote, groupID string, now time.Time) (ProtectionPreview, error) {
	if !requestIDPattern.MatchString(groupID) {
		return ProtectionPreview{}, errors.New("유효한 그룹 ID가 필요합니다")
	}
	if owner.Symbol != "BTCUSDT" || owner.Side != Long && owner.Side != Short || owner.Source != Manual && owner.Source != Model {
		return ProtectionPreview{}, errors.New("유효한 모델/수동 BTCUSDT 포지션이 필요합니다")
	}
	if owner.Source == Model && !modelIDPattern.MatchString(owner.ModelID) || owner.Source == Manual && owner.ModelID != "" {
		return ProtectionPreview{}, errors.New("포지션 소유자 ID 오류")
	}
	qty, err := pos(owner.Quantity)
	if err != nil {
		return ProtectionPreview{}, err
	}
	entry, err := pos(owner.AverageEntry)
	if err != nil {
		return ProtectionPreview{}, err
	}
	if rules.Symbol != "BTCUSDT" || rules.Status != "TRADING" || rules.Price.Tick == "" || rules.Lot.Step == "" || rules.FetchedAt.IsZero() || rules.FetchedAt.After(now.Add(time.Second)) || now.Sub(rules.FetchedAt) > 60*time.Second {
		return ProtectionPreview{}, errors.New("최신 거래소 가격/수량 필터가 필요합니다")
	}
	tick, err := pos(rules.Price.Tick)
	if err != nil {
		return ProtectionPreview{}, err
	}
	step, err := pos(rules.Lot.Step)
	if err != nil {
		return ProtectionPreview{}, err
	}
	if !new(big.Rat).Quo(qty, step).IsInt() {
		return ProtectionPreview{}, errors.New("TP/SL 수량이 거래소 수량 단위와 맞지 않습니다")
	}
	lotMin, err := pos(rules.Lot.Min)
	if err != nil {
		return ProtectionPreview{}, err
	}
	lotMax, err := pos(rules.Lot.Max)
	if err != nil {
		return ProtectionPreview{}, err
	}
	if qty.Cmp(lotMin) < 0 || qty.Cmp(lotMax) > 0 {
		return ProtectionPreview{}, errors.New("TP/SL 수량이 거래소 허용 범위를 벗어납니다")
	}
	minPrice, err := pos(rules.Price.Min)
	if err != nil {
		return ProtectionPreview{}, err
	}
	maxPrice, err := pos(rules.Price.Max)
	if err != nil {
		return ProtectionPreview{}, err
	}
	if contract.Symbol != "BTCUSDT" || mark.Symbol != "BTCUSDT" || contract.ReceivedAt.IsZero() || mark.ReceivedAt.IsZero() || contract.ReceivedAt.After(now.Add(time.Second)) || mark.ReceivedAt.After(now.Add(time.Second)) || now.Sub(contract.ReceivedAt) > 3*time.Second || now.Sub(mark.ReceivedAt) > 3*time.Second {
		return ProtectionPreview{}, errors.New("TP/SL 현재가 또는 마크 가격이 오래되었습니다")
	}
	cp, err := pos(contract.Price)
	if err != nil {
		return ProtectionPreview{}, err
	}
	mp, err := pos(mark.Price)
	if err != nil {
		return ProtectionPreview{}, err
	}
	// +/-5% initial margin ROI at 15x means delta = 1/300 of the entry
	// price. Neither funding nor execution fees nor slippage is included.
	d := big.NewRat(1, 300)
	upper := new(big.Rat).Mul(entry, new(big.Rat).Add(big.NewRat(1, 1), d))
	lower := new(big.Rat).Mul(entry, new(big.Rat).Sub(big.NewRat(1, 1), d))
	side := "SELL"
	tp := roundTick(upper, tick, true)
	sl := roundTick(lower, tick, true) // long SL earlier than unrounded threshold
	if owner.Side == Short {
		side = "BUY"
		tp = roundTick(lower, tick, false)
		sl = roundTick(upper, tick, false) // short SL earlier than unrounded threshold
	}
	if tp.Cmp(minPrice) < 0 || tp.Cmp(maxPrice) > 0 || sl.Cmp(minPrice) < 0 || sl.Cmp(maxPrice) > 0 {
		return ProtectionPreview{}, errors.New("TP/SL 가격이 거래소 필터 범위를 벗어납니다")
	}
	if owner.Side == Long && (tp.Cmp(cp) <= 0 || sl.Cmp(mp) >= 0) || owner.Side == Short && (tp.Cmp(cp) >= 0 || sl.Cmp(mp) <= 0) {
		return ProtectionPreview{}, errors.New("현재 시장 가격에서 즉시 발동될 수 있는 TP/SL입니다")
	}
	makeOrder := func(kind, typ, working string, price *big.Rat) ProtectionInstruction {
		return ProtectionInstruction{
			ClientAlgoID: protectionAlgoID(groupID, owner, kind), Symbol: "BTCUSDT", Side: side, PositionSide: owner.Side,
			AlgoType: "CONDITIONAL", OrderType: typ, Quantity: owner.Quantity, TriggerPrice: ratToString(price), WorkingType: working,
		}
	}
	return ProtectionPreview{TP: makeOrder("tp", "TAKE_PROFIT_MARKET", "CONTRACT_PRICE", tp),
		SL: makeOrder("sl", "STOP_MARKET", "MARK_PRICE", sl), Leverage: 15, TargetGrossROI: "0.05", DoesNotIncludeCosts: true, LiveSubmissionEnabled: false}, nil
}
