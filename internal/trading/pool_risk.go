package trading

// Independent READ-ONLY calculation of the worst-case requested close volume
// of Binance Hedge-mode TP/SL legs. This is NOT a price prediction: actual
// fills can be fewer, Binance can reject an oversized conditional order, and
// a TP and an SL are NOT an atomic OCO pair. It never approves a new order.
import (
	"binance_trader/internal/exchange"
	"math/big"
	"time"
)

type PooledSideExposure struct {
	Side                          PositionSide `json:"side"`
	SignedPositionBTC             string       `json:"signed_position_btc"`
	IndependentOwners             int          `json:"independent_owners"`
	ActiveTPBTC                   string       `json:"active_tp_btc"`
	ActiveSLBTC                   string       `json:"active_sl_btc"`
	TotalPossibleCloseRequestsBTC string       `json:"total_possible_close_requests_btc"`
	ExcessRequestsIfAllTriggerBTC string       `json:"excess_requests_if_all_trigger_btc"`
}

type PooledExitRisk struct {
	State                string               `json:"state"` // BLOCKED / EMPTY / NON_ATOMIC_EXIT_RISK
	ObservedAt           time.Time            `json:"observed_at"`
	Sides                []PooledSideExposure `json:"sides"`
	Blockers             []string             `json:"blockers"`
	TradingAuthorized    bool                 `json:"trading_authorized"`     // ALWAYS false
	ManualReviewRequired bool                 `json:"manual_review_required"` // ALWAYS true
}

// EvaluatePooledExitRisk requires an independently authenticated whole-account
// signed REST baseline AND a separately verified fill/ownership audit. A caller
// cannot turn this diagnostic into a live trading permission. If one TP or SL
// has disappeared after triggering, the complete-inventory precondition fails:
// a separate signed order/fill reconciliation is needed instead of inferring
// that disappearance means cancellation.
func EvaluatePooledExitRisk(b exchange.RecoveryBaseline, owned []OwnerPosition,
	expected []ExpectedProtection, audited bool, now time.Time) PooledExitRisk {
	r := PooledExitRisk{State: "BLOCKED", ObservedAt: b.FinishedAt, ManualReviewRequired: true,
		Blockers: []string{}, Sides: []PooledSideExposure{}}
	verdict := ReviewProtectionPortfolio(b, owned, expected, audited, now)
	if verdict.State == "BLOCKED" {
		r.Blockers = append(r.Blockers, verdict.Blockers...)
		return r
	}
	if verdict.State == "EMPTY" {
		r.State = "EMPTY"
		r.Blockers = append(r.Blockers, "빈 계정의 제한된 REST 관측일 뿐, 외부 거래나 스트림 연속성의 증거가 아닙니다")
		return r
	}
	if verdict.State != "OBSERVED_MATCH_NOT_OCO" {
		r.Blockers = append(r.Blockers, "알 수 없는 보호 주문 검증 결과")
		return r
	}
	owners := map[PositionSide]int{Long: 0, Short: 0}
	for _, p := range owned {
		owners[p.Side]++
	}
	for _, side := range []PositionSide{Long, Short} {
		qty, err := signedRat(verdict.LongQty)
		if side == Short {
			qty, err = signedRat(verdict.ShortQty)
		}
		if err != nil || qty.Sign() < 0 { // normally already checked by portfolio review
			r.Blockers = append(r.Blockers, "거래소 보호 수량 재검증 실패")
			return r
		}
		if qty.Sign() == 0 {
			continue
		}
		// Inventory review established exactly ONE owner-sized TP and ONE
		// owner-sized SL per owner, no foreign algos, no standard open orders,
		// and matching side quantities in this single bounded observation.
		closeQty := new(big.Rat).Mul(qty, big.NewRat(2, 1))
		excess := new(big.Rat).Sub(closeQty, qty)
		r.Sides = append(r.Sides, PooledSideExposure{Side: side,
			SignedPositionBTC: formatOwnershipDecimal(qty), IndependentOwners: owners[side],
			ActiveTPBTC: formatOwnershipDecimal(qty), ActiveSLBTC: formatOwnershipDecimal(qty),
			TotalPossibleCloseRequestsBTC: formatOwnershipDecimal(closeQty),
			ExcessRequestsIfAllTriggerBTC: formatOwnershipDecimal(excess)})
	}
	if len(r.Sides) == 0 {
		r.Blockers = append(r.Blockers, "소유 포지션 없이 활성 TP/SL 기록이 존재합니다")
		return r
	}
	r.State = "NON_ATOMIC_EXIT_RISK"
	r.Blockers = append(r.Blockers, "별도 TP/SL은 원자적 OCO가 아닙니다: 두 조건이 동시에 발동하면 요청된 청산 수량이 실제 포지션보다 클 수 있습니다")
	r.Blockers = append(r.Blockers, "자동 보호 주문 전송/후속 청산 불허: 독립 서명 조회, 취소 확인, 체결 및 전체 포지션 재대조 필요")
	return r
}
