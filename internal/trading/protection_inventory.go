package trading

// Pure read-only proof of expected owner-specific paired protection inventory.
// This deliberately does NOT authorize further entry: separate exchange TP/SL
// orders are not atomic OCO, and an unexpected double-trigger can over-close
// the aggregated Hedge position until actual reconciliation.
import (
	"binance_trader/internal/exchange"
	"errors"
	"math/big"
	"time"
)

type ExpectedProtection struct {
	Source         Source       `json:"source"`
	ModelID        string       `json:"model_id,omitempty"`
	Side           PositionSide `json:"position_side"`
	Quantity       string       `json:"quantity"`
	TPClientAlgoID string       `json:"tp_client_algo_id"`
	SLClientAlgoID string       `json:"sl_client_algo_id"`
}
type ProtectionInventoryReview struct {
	Status            string   `json:"status"` // EMPTY / KNOWN_PROTECTED / BLOCKED
	Entries           int      `json:"entries"`
	TradingAuthorized bool     `json:"trading_authorized"` // ALWAYS FALSE
	Issues            []string `json:"issues"`
}

func ReviewProtectionInventory(snapshot exchange.Snapshot, owned []OwnerPosition, expected []ExpectedProtection, now time.Time) ProtectionInventoryReview {
	r := ProtectionInventoryReview{Status: "BLOCKED", Issues: []string{}}
	issue := func(s string) { r.Issues = append(r.Issues, s) }
	at, err := time.Parse(time.RFC3339Nano, snapshot.AsOf)
	if err != nil || at.After(now.Add(time.Second)) || now.Sub(at) > 5*time.Second || snapshot.Status != "connected" || snapshot.Environment != "demo" || snapshot.AlgoOrders == nil {
		issue("서명된 최신 Demo 조건부 주문 목록 필요")
		return r
	}
	if len(expected) == 0 && len(snapshot.AlgoOrders) == 0 && len(owned) == 0 {
		r.Status = "EMPTY"
		return r
	}
	if len(snapshot.AlgoOrders) != 2*len(expected) {
		issue("예상한 보호 주문 수와 실제 조건부 주문 수가 다릅니다")
		return r
	}
	ownerQty := map[string]*big.Rat{}
	for _, p := range owned {
		if (p.Source != Model && p.Source != Manual) || (p.Source == Model && !modelIDPattern.MatchString(p.ModelID)) || (p.Source == Manual && p.ModelID != "") || (p.Side != Long && p.Side != Short) {
			issue("알 수 없는 포지션 소유자 또는 방향")
			return r
		}
		if p.Symbol != "BTCUSDT" {
			issue("다른 종목 소유권 정보")
			return r
		}
		qty, e := decimalOwner(p.Quantity)
		if e != nil {
			issue("소유 수량 오류")
			return r
		}
		key := string(p.Source) + "|" + p.ModelID + "|" + string(p.Side)
		if ownerQty[key] != nil {
			issue("중복 소유자 포지션")
			return r
		}
		ownerQty[key] = qty
	}
	actual := map[string]exchange.AlgoOrder{}
	for _, a := range snapshot.AlgoOrders {
		if a.Symbol != "BTCUSDT" || a.ClientAlgoID == "" || a.AlgoID <= 0 || a.ClosePosition || a.Quantity == "" || a.AlgoStatus != "NEW" && a.AlgoStatus != "WORKING" {
			issue("외부 또는 확인 불가능한 조건부 주문")
			return r
		}
		if _, dup := actual[a.ClientAlgoID]; dup {
			issue("조건부 주문 ID 중복")
			return r
		}
		actual[a.ClientAlgoID] = a
	}
	if len(ownerQty) != len(expected) {
		issue("보호 주문을 찾을 수 없는 소유자 포지션이 있습니다")
		return r
	}
	ids := map[string]bool{}
	seenOwner := map[string]bool{}
	for _, plan := range expected {
		key := string(plan.Source) + "|" + plan.ModelID + "|" + string(plan.Side)
		ownedQ, ok := ownerQty[key]
		if seenOwner[key] {
			issue("한 소유자의 보호 주문이 중복되었습니다")
			return r
		}
		seenOwner[key] = true
		if !ok {
			issue("보호 대상 소유자 포지션 없음")
			return r
		}
		qty, e := decimalOwner(plan.Quantity)
		if e != nil || ownedQ.Cmp(qty) != 0 {
			issue("보호 수량과 소유 수량 불일치")
			return r
		}
		if plan.TPClientAlgoID == "" || plan.SLClientAlgoID == "" || plan.TPClientAlgoID == plan.SLClientAlgoID || ids[plan.TPClientAlgoID] || ids[plan.SLClientAlgoID] {
			issue("보호 주문 ID 중복")
			return r
		}
		ids[plan.TPClientAlgoID] = true
		ids[plan.SLClientAlgoID] = true
		tp, existsTP := actual[plan.TPClientAlgoID]
		sl, existsSL := actual[plan.SLClientAlgoID]
		if !existsTP || !existsSL {
			issue("독립 소유자의 보호 주문 누락")
			return r
		}
		for idx, a := range []exchange.AlgoOrder{tp, sl} {
			q, e := decimalOwner(a.Quantity)
			trigger, eTrigger := decimalOwner(a.TriggerPrice)
			if e != nil || eTrigger != nil || trigger.Sign() <= 0 || q.Cmp(qty) != 0 || PositionSide(a.PositionSide) != plan.Side || a.WorkingType != []string{"CONTRACT_PRICE", "MARK_PRICE"}[idx] {
				issue("보호 주문 수량·방향·발동 가격 종류 불일치")
				return r
			}
			if idx == 0 && a.OrderType != "TAKE_PROFIT_MARKET" || idx == 1 && a.OrderType != "STOP_MARKET" {
				issue("보호 주문 종류 불일치")
				return r
			}
			wantSide := "SELL"
			if plan.Side == Short {
				wantSide = "BUY"
			}
			if a.Side != wantSide {
				issue("보호 주문 방향 오류")
				return r
			}
		}
		r.Entries++
	}
	if len(ids) != len(actual) {
		issue("소유자 이외의 조건부 주문 감지")
		return r
	}
	r.Status = "KNOWN_PROTECTED"
	issue("서로 독립된 TP/SL은 원자적 OCO가 아닙니다. 동시 발동 방지 및 연속 계정 재동기화 전 신규 거래 잠금")
	return r
}
func decimalOwner(s string) (*big.Rat, error) {
	q, e := pos(s)
	if e != nil {
		return nil, errors.New("잘못된 수량")
	}
	return q, nil
}
