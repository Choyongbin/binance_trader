package trading

// Conservative READ-ONLY review of one manually owned Binance Demo order.
// A valid order ACK or stream event is not a fill. Even a FILLED REST status
// does not release reserved collateral or authorize protecting a position:
// immutable imported trades, a separately verified ownership audit, fresh
// account-wide positions and user-stream continuity are still required.
import (
	"binance_trader/internal/exchange"
	"math/big"
	"time"
)

type SignedManualEvidence struct {
	Order                   exchange.OrderResult
	OrderReadAt             time.Time
	FromSignedGET           bool
	Trades                  []exchange.AccountTrade
	TradesFetchedAt         time.Time
	FullTradesFromSignedGET bool
}

type ManualOrderReview struct {
	State                           string `json:"state"` // BLOCKED / OPEN_UNFILLED / OPEN_PARTIAL / TERMINAL_PARTIAL / FILLED_MATCH / TERMINAL_EMPTY
	ExchangeStatus                  string `json:"exchange_status,omitempty"`
	PlannedBTC                      string `json:"planned_btc,omitempty"`
	FilledBTC                       string `json:"filled_btc,omitempty"`
	RemainingBTC                    string `json:"remaining_btc,omitempty"`
	ImportedFillsRequired           bool   `json:"imported_fills_required"`
	IndependentAccountAuditRequired bool   `json:"independent_account_audit_required"`
	MayReleaseReservation           bool   `json:"may_release_reservation"` // ALWAYS FALSE
	MayPlaceProtection              bool   `json:"may_place_protection"`    // ALWAYS FALSE
	Reason                          string `json:"reason"`
}

func ReviewManualOrder(record Record, e SignedManualEvidence, now time.Time) ManualOrderReview {
	r := ManualOrderReview{State: "BLOCKED", ImportedFillsRequired: true, IndependentAccountAuditRequired: true,
		Reason: "독립 서명 계정·체결 감사 전 안전하게 처리할 수 없습니다"}
	if record.Intent.Source != Manual || record.Intent.ModelID != "" {
		r.Reason = "수동 거래 출처가 아닌 주문"
		return r
	}
	plan, err := record.Intent.Plan()
	if err != nil || record.Plan != plan || record.IntentDigest == "" {
		r.Reason = "원본 주문 저널 불일치"
		return r
	}
	digest, _ := digestIntent(record.Intent)
	if digest != record.IntentDigest {
		r.Reason = "저널 요청 해시 불일치"
		return r
	}
	if !e.FromSignedGET || !e.FullTradesFromSignedGET || e.OrderReadAt.IsZero() || e.TradesFetchedAt.IsZero() ||
		e.OrderReadAt.After(now.Add(time.Second)) || e.TradesFetchedAt.After(now.Add(time.Second)) ||
		now.Sub(e.OrderReadAt) > 5*time.Second || now.Sub(e.TradesFetchedAt) > 5*time.Second {
		r.Reason = "최신 서명된 주문 상태와 해당 주문 전체 체결 조회 필요"
		return r
	}
	executed, err := verifyRemoteOrder(record, e.Order)
	if err != nil {
		r.Reason = "서명 주문 불일치: " + err.Error()
		return r
	}
	if record.ExchangeOrderID > 0 && record.ExchangeOrderID != e.Order.OrderID {
		r.Reason = "ACK/서명 주문 ID 충돌"
		return r
	}
	qty, err := pos(plan.Quantity)
	if err != nil {
		r.Reason = "원본 주문 수량 오류"
		return r
	}
	if len(e.Trades) >= 1000 {
		r.Reason = "체결 조회 1000건 포화: 전체 체결 여부 알 수 없음"
		return r
	}
	sum := new(big.Rat)
	seen := make(map[int64]bool)
	for _, fill := range e.Trades {
		if seen[fill.TradeID] || fill.OrderID != e.Order.OrderID {
			r.Reason = "중복되거나 다른 주문의 체결 내역"
			return r
		}
		seen[fill.TradeID] = true
		if fill.TimeMS > now.Add(time.Second).UnixMilli() {
			r.Reason = "현재 시각 이후의 체결 기록"
			return r
		}
		if err := validateFill(record, fill); err != nil {
			r.Reason = "서명 체결 오류: " + err.Error()
			return r
		}
		q, err := pos(fill.Quantity)
		if err != nil {
			r.Reason = "체결 수량 오류"
			return r
		}
		sum.Add(sum, q)
	}
	if sum.Cmp(executed) != 0 {
		r.Reason = "거래소 누적 체결 수량과 사용자 체결 내역 합계 불일치"
		return r
	}
	remain := new(big.Rat).Sub(qty, executed)
	if remain.Sign() < 0 {
		r.Reason = "원 주문 수량보다 더 많이 체결됨"
		return r
	}
	r.ExchangeStatus = e.Order.Status
	r.PlannedBTC = ratToString(qty)
	r.FilledBTC = ratToString(executed)
	r.RemainingBTC = ratToString(remain)
	switch e.Order.Status {
	case "NEW":
		if executed.Sign() != 0 {
			r.Reason = "NEW 상태인데 체결 기록 존재"
			return r
		}
		r.State = "OPEN_UNFILLED"
		r.Reason = "미체결 주문: 취소/대기 결정 후 독립 조회 필요"
	case "PARTIALLY_FILLED":
		if executed.Sign() == 0 || executed.Cmp(qty) >= 0 {
			r.Reason = "부분 체결 상태와 수량 모순"
			return r
		}
		r.State = "OPEN_PARTIAL"
		r.Reason = "부분 체결 중: 잔량을 해제하거나 전체 수량 TP/SL 등록 금지"
	case "FILLED":
		if executed.Cmp(qty) != 0 {
			r.Reason = "FILLED 상태인데 전체 수량 체결 불일치"
			return r
		}
		r.State = "FILLED_MATCH"
		r.Reason = "독립 체결 내역 합계 일치: 장부 반영·계정 전체 감사가 여전히 필요"
	case "CANCELED", "EXPIRED", "REJECTED", "EXPIRED_IN_MATCH":
		if executed.Sign() == 0 {
			r.State = "TERMINAL_EMPTY"
			r.Reason = "주문 종료·체결 없음: 다른 노출 확인 후 예약 해제 별도 검증 필요"
		} else {
			r.State = "TERMINAL_PARTIAL"
			r.Reason = "주문 종료되었지만 부분 체결 잔존: 실제 포지션/TP·SL 수량 분리 검사 필수"
		}
	default:
		r.Reason = "알 수 없는 주문 상태"
	}
	return r // no state authorizes a real order or collateral release
}
