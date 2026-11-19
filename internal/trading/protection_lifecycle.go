package trading

// Read-only, side-effect-free protection reconciliation proposal. This is NOT
// Binance OCO: on the same Hedge position side the exchange pools manual and
// model quantities. During the gap between one TP/SL triggering and cancelling
// its sibling, the sibling could also trigger. Such races must HALT all new
// trading and be resolved using actual signed fills/positions, never guesses.
import (
	"errors"
	"math/big"
	"time"
)

type ProtectionState string

const (
	ProtectionMonitoring    ProtectionState = "MONITORING_UNVERIFIED"
	ProtectionCancelSibling ProtectionState = "SIBLING_CANCEL_INTENT_REQUIRED"
	ProtectionClosed        ProtectionState = "CLOSED_AFTER_SIGNED_RECONCILIATION"
	ProtectionHalt          ProtectionState = "HALT_AND_RECONCILE"
)

type AlgoObservation struct {
	ClientID      string
	AlgoID        int64
	Status        string
	Symbol        string
	PositionSide  PositionSide
	Quantity      string
	ActualOrderID string
	ActualQty     string
	FromSignedGet bool
	ObservedAt    time.Time
}
type ProtectionAssessment struct {
	State         ProtectionState `json:"state"`
	SiblingAlgoID int64           `json:"sibling_algo_id,omitempty"`
	Reason        string          `json:"reason"`
}

// ProtectionEvidence is built from *independent* signed REST and imported
// fill accounting. These fields are NOT safe to accept from UI requests.
type ProtectionEvidence struct {
	TP, SL            AlgoObservation
	Owner             OwnerPosition
	RemainingOwnerQty string
	// Caller MUST independently verify this against exchange account positions,
	// all owned fills, commission and sibling status. Not a trading gate token.
	FullSignedOwnershipAudit bool
	SignedAccountAsOf        time.Time
}

func assessObs(o AlgoObservation, instruction ProtectionInstruction, now time.Time) error {
	if !o.FromSignedGet || o.AlgoID <= 0 || o.ClientID != instruction.ClientAlgoID || o.Symbol != instruction.Symbol || o.PositionSide != instruction.PositionSide || o.Quantity == "" ||
		o.ObservedAt.IsZero() || o.ObservedAt.After(now.Add(time.Second)) || now.Sub(o.ObservedAt) > 5*time.Second {
		return errors.New("서명된 최신 조건부 주문 확인 불완전")
	}
	a, e := pos(o.Quantity)
	if e != nil {
		return e
	}
	b, e := pos(instruction.Quantity)
	if e != nil {
		return e
	}
	if a.Cmp(b) != 0 {
		return errors.New("조건부 주문 수량 불일치")
	}
	return nil
}
func isActiveProtection(status string) bool    { return status == "NEW" || status == "WORKING" }
func isTriggeredProtection(status string) bool { return status == "TRIGGERED" || status == "FINISHED" }
func isCanceledProtection(status string) bool  { return status == "CANCELED" || status == "CANCELLED" }

// EvaluateProtection never performs a network request or returns permission to
// submit/cancel an order; it yields the next required operator/coordinator
// workflow. A stream notification alone must NEVER mark a leg canceled.
func EvaluateProtection(plan ProtectionPreview, e ProtectionEvidence, now time.Time) ProtectionAssessment {
	halt := func(why string) ProtectionAssessment { return ProtectionAssessment{State: ProtectionHalt, Reason: why} }
	if plan.LiveSubmissionEnabled || plan.TP.Symbol != "BTCUSDT" || plan.SL.Symbol != "BTCUSDT" || plan.TP.ClientAlgoID == "" || plan.TP.ClientAlgoID == plan.SL.ClientAlgoID ||
		plan.TP.PositionSide != plan.SL.PositionSide || plan.TP.Quantity != plan.SL.Quantity || e.Owner.Symbol != "BTCUSDT" || e.Owner.Side != plan.TP.PositionSide {
		return halt("TP/SL 계획과 소유자 정보가 일치하지 않습니다")
	}
	if e.TP.AlgoID <= 0 || e.SL.AlgoID <= 0 || e.TP.AlgoID == e.SL.AlgoID {
		return halt("TP/SL exchange Algo ID 중복 또는 누락")
	}
	if err := assessObs(e.TP, plan.TP, now); err != nil {
		return halt("TP 관측 불확실: " + err.Error())
	}
	if err := assessObs(e.SL, plan.SL, now); err != nil {
		return halt("SL 관측 불확실: " + err.Error())
	}
	if !e.SignedAccountAsOf.IsZero() && (e.SignedAccountAsOf.After(now.Add(time.Second)) || now.Sub(e.SignedAccountAsOf) > 5*time.Second) {
		return halt("계정 스냅샷 시간 초과")
	}
	owned, err := pos(e.Owner.Quantity)
	if err != nil {
		return halt("원본 소유 수량 오류")
	}
	remain, ok := new(big.Rat).SetString(e.RemainingOwnerQty)
	if !ok || remain.Sign() < 0 || remain.Cmp(owned) > 0 {
		return halt("독립 소유 수량 오류")
	}
	tp, sl := e.TP.Status, e.SL.Status
	if !validKnownProtectionStatus(tp) || !validKnownProtectionStatus(sl) {
		return halt("알 수 없는 보호 주문 상태")
	}
	// Never try a sibling cancellation while both might already be executing.
	if isTriggeredProtection(tp) && isTriggeredProtection(sl) {
		return halt("TP/SL 동시 발동 가능성: 주문과 실제 체결 전수 확인")
	}
	if (isTriggeredProtection(tp) && isActiveProtection(sl) || isTriggeredProtection(sl) && isActiveProtection(tp)) &&
		(!e.FullSignedOwnershipAudit || e.SignedAccountAsOf.IsZero()) {
		return halt("보호 주문 발동 후 최신 독립 체결·계정 검증 없이 형제 주문 취소 제안 불가")
	}
	if isTriggeredProtection(tp) && isActiveProtection(sl) {
		return ProtectionAssessment{State: ProtectionCancelSibling, SiblingAlgoID: e.SL.AlgoID, Reason: "TP 발동 관측: SL 단일 취소 인텐트 기록 및 서명 재조회 필요"}
	}
	if isTriggeredProtection(sl) && isActiveProtection(tp) {
		return ProtectionAssessment{State: ProtectionCancelSibling, SiblingAlgoID: e.TP.AlgoID, Reason: "SL 발동 관측: TP 단일 취소 인텐트 기록 및 서명 재조회 필요"}
	}
	if isTriggeredProtection(tp) && isCanceledProtection(sl) || isTriggeredProtection(sl) && isCanceledProtection(tp) {
		triggered := e.TP
		if isTriggeredProtection(sl) {
			triggered = e.SL
		}
		// ActualOrderID and imported trades are needed before claiming closure.
		actual, e2 := new(big.Rat).SetString(triggered.ActualQty)
		if !e.FullSignedOwnershipAudit || e.SignedAccountAsOf.IsZero() || triggered.ActualOrderID == "" || !e2 || actual.Sign() <= 0 || actual.Cmp(owned) != 0 || remain.Sign() != 0 {
			return halt("형제 주문 취소 관측: 실제 체결 및 소유 잔량 감사가 필요합니다")
		}
		return ProtectionAssessment{State: ProtectionClosed, Reason: "독립 서명 조회·체결·소유 수량 대조 결과가 모두 일치함"}
	}
	if isActiveProtection(tp) && isActiveProtection(sl) && remain.Cmp(owned) == 0 && e.FullSignedOwnershipAudit && !e.SignedAccountAsOf.IsZero() {
		return ProtectionAssessment{State: ProtectionMonitoring, Reason: "관측 시점에 두 보호 주문 활성; 실행 허가는 아님"}
	}
	return halt("보호 주문 누락·거부·불완전 체결 또는 잔량 불일치")
}
func validKnownProtectionStatus(s string) bool {
	switch s {
	case "NEW", "WORKING", "TRIGGERING", "TRIGGERED", "FINISHED", "CANCELED", "CANCELLED", "EXPIRED", "REJECTED":
		return true
	}
	return false
}
