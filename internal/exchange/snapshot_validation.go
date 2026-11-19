package exchange

import (
	"errors"
	"fmt"
	"math/big"
)

// validateDisplaySnapshot runs before filtering zero positions and before
// publishing connected to the browser. Partial or malformed data is never OK.
func validateDisplaySnapshot(s Snapshot, symbol string) error {
	if s.Balances == nil || s.Positions == nil || s.Orders == nil || s.AlgoOrders == nil {
		return errors.New("계정 데이터 일부가 누락되었습니다")
	}
	seenAssets := map[string]bool{}
	seenPositions := map[string]bool{}
	usdt := false
	for _, b := range s.Balances {
		if b.Asset == "" || seenAssets[b.Asset] || !decimalNonNegative(b.Wallet, true) || !decimalNonNegative(b.Available, true) || (b.CrossUnPnl != "" && !decimalSigned(b.CrossUnPnl)) {
			return errors.New("계정 잔액 데이터가 잘못되었습니다")
		}
		wallet, _ := new(big.Rat).SetString(b.Wallet)
		available, _ := new(big.Rat).SetString(b.Available)
		// Positive cross-wallet unrealized PnL can legitimately increase
		// futures available balance above wallet balance. Reject unexplained
		// increases rather than falsely declaring a valid account corrupt.
		limit := new(big.Rat).Set(wallet)
		if b.CrossUnPnl != "" {
			cross, _ := new(big.Rat).SetString(b.CrossUnPnl)
			if cross.Sign() > 0 {
				limit.Add(limit, cross)
			}
		}
		if available.Cmp(limit) > 0 {
			return errors.New("계정 가용 잔액이 지갑·미실현 손익 합계를 초과합니다")
		}
		seenAssets[b.Asset] = true
		if b.Asset == "USDT" {
			usdt = true
		}
	}
	if !usdt {
		return errors.New("계정 USDT 잔액이 누락되었습니다")
	}
	for _, p := range s.Positions {
		if p.Symbol != symbol || (p.Side != "LONG" && p.Side != "SHORT" && p.Side != "BOTH") || seenPositions[p.Side] || !decimalSigned(p.Qty) {
			return errors.New("요청한 종목의 포지션 응답이 잘못되었습니다")
		}
		qty, _ := new(big.Rat).SetString(p.Qty)
		if (p.Side == "LONG" && qty.Sign() < 0) || (p.Side == "SHORT" && qty.Sign() > 0) || (p.Side == "BOTH" && qty.Sign() != 0) {
			return errors.New("포지션 수량과 방향이 일치하지 않습니다")
		}
		for _, amount := range []string{p.Entry, p.BreakEven, p.Mark, p.Liquidation, p.IsolatedMargin, p.Leverage} {
			if amount != "" && !decimalNonNegative(amount, true) {
				return errors.New("포지션 가격·증거금 필드 오류")
			}
		}
		if p.PnL != "" && !decimalSigned(p.PnL) {
			return errors.New("포지션 미실현 손익 필드 오류")
		}
		seenPositions[p.Side] = true
	}
	seenOrders := map[int64]bool{}
	for _, o := range s.Orders {
		if o.Symbol != symbol || o.OrderID <= 0 || seenOrders[o.OrderID] || !decimalNonNegative(o.OrigQty, true) || !decimalNonNegative(o.ExecutedQty, true) || !decimalNonNegative(o.Price, true) || (o.StopPrice != "" && !decimalNonNegative(o.StopPrice, true)) {
			return fmt.Errorf("%s 일반 주문 데이터 오류", symbol)
		}
		orig, _ := new(big.Rat).SetString(o.OrigQty)
		executed, _ := new(big.Rat).SetString(o.ExecutedQty)
		if executed.Cmp(orig) > 0 {
			return errors.New("체결 수량이 원래 수량을 초과합니다")
		}
		seenOrders[o.OrderID] = true
	}
	seenAlgos := map[int64]bool{}
	for _, a := range s.AlgoOrders {
		if a.Symbol != symbol || a.AlgoID <= 0 || seenAlgos[a.AlgoID] || !decimalNonNegative(a.Quantity, true) || !decimalNonNegative(a.TriggerPrice, true) {
			return errors.New("조건부 주문 데이터 오류")
		}
		seenAlgos[a.AlgoID] = true
	}
	return nil
}
