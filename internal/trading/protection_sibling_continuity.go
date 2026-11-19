package trading

// Local, read-only continuity check between a triggered TP/SL pair and the
// durable one-shot sibling cancellation identity. It NEVER issues an exchange
// request and cannot prove that caller-supplied evidence is authentic. The
// caller must obtain fresh independent signed order/fill/account data.
import (
	"errors"
	"fmt"
	"os"
	"time"
)

type SiblingCancelContinuity struct {
	GroupID                 string   `json:"group_id"`
	State                   string   `json:"state"`
	SiblingAlgoID           int64    `json:"sibling_algo_id,omitempty"`
	IntentRecorded          bool     `json:"intent_recorded"`
	DispatchMayHaveBeenSent bool     `json:"dispatch_may_have_been_sent"`
	Blockers                []string `json:"blockers"`
	CancelAllowed           bool     `json:"cancel_allowed"`         // ALWAYS false
	ReplayAllowed           bool     `json:"replay_allowed"`         // ALWAYS false
	TradingAuthorized       bool     `json:"trading_authorized"`     // ALWAYS false
	FinanciallySettled      bool     `json:"financially_settled"`    // ALWAYS false
	ManualReviewRequired    bool     `json:"manual_review_required"` // ALWAYS true
}

// InspectSiblingCancelContinuity revalidates an immutable protection plan,
// checks both independent signed observations, then checks the durable cancel
// target before classifying a crash/restart. It never creates a cancel intent.
func InspectSiblingCancelContinuity(submissions *ProtectionSubmissionStore, cancels *CancelIntentStore, group string, evidence ProtectionEvidence, now time.Time) (SiblingCancelContinuity, error) {
	out := SiblingCancelContinuity{GroupID: group, State: "HALT", Blockers: []string{}, ManualReviewRequired: true}
	block := func(reason string, err error) (SiblingCancelContinuity, error) {
		out.Blockers = append(out.Blockers, reason)
		return out, err
	}
	if submissions == nil || cancels == nil || now.IsZero() {
		return block("저장소 또는 검사 시각 누락", errors.New("필수 구성요소 누락"))
	}
	plan, err := submissions.read(group)
	if err != nil {
		return block("원본 보호 계획 확인 실패", err)
	}
	if evidence.TP.AlgoID <= 0 || evidence.SL.AlgoID <= 0 || evidence.TP.AlgoID == evidence.SL.AlgoID {
		return block("서로 다른 TP/SL exchange Algo ID 필요", errors.New("동일 또는 무효 Algo ID"))
	}
	preview := ProtectionPreview{TP: plan.TP, SL: plan.SL}
	assessment := EvaluateProtection(preview, evidence, now)
	// Only a signed triggered+active pair or a triggered+cancelled pair can
	// establish a unique sibling identity. A stale or incomplete state HALTs.
	var expectedID int64
	var expectedClient string
	switch assessment.State {
	case ProtectionCancelSibling:
		expectedID = assessment.SiblingAlgoID
	case ProtectionClosed:
		if isCanceledProtection(evidence.TP.Status) {
			expectedID = evidence.TP.AlgoID
		} else if isCanceledProtection(evidence.SL.Status) {
			expectedID = evidence.SL.AlgoID
		}
	default:
		return block("서명 관측 및 소유권 감사가 취소 연속성 검사를 충족하지 못함: "+assessment.Reason, errors.New("보호주문 불확실"))
	}
	if expectedID == evidence.TP.AlgoID {
		expectedClient = plan.TP.ClientAlgoID
	} else if expectedID == evidence.SL.AlgoID {
		expectedClient = plan.SL.ClientAlgoID
	}
	if expectedID <= 0 || expectedClient == "" {
		return block("형제 보호 주문의 고유 식별자 불일치", errors.New("형제 식별 실패"))
	}
	out.SiblingAlgoID = expectedID
	intent, attempted, err := cancels.Inspect(group)
	if errors.Is(err, os.ErrNotExist) {
		out.State = "REQUIRES_OPERATOR_CANCEL_REVIEW"
		out.Blockers = append(out.Blockers, "취소 인텐트 없음: 독립 승인과 신규 signed 재검증 필요")
		return out, nil
	}
	if err != nil {
		return block("기존 취소 인텐트/체크포인트 손상 또는 읽기 실패", err)
	}
	out.IntentRecorded = true
	out.DispatchMayHaveBeenSent = attempted
	if intent.SiblingAlgoID != expectedID || intent.OwnerClientAlgoID != expectedClient || intent.GroupID != group {
		return block(fmt.Sprintf("기존 단발 취소 대상 불일치: group=%s", group), errors.New("취소 대상 충돌"))
	}
	if !attempted {
		out.State = "INTENT_ONLY_NOT_DISPATCHED"
		out.Blockers = append(out.Blockers, "취소 인텐트만 존재: 현재 실행 승인 불가")
	} else if assessment.State == ProtectionClosed {
		out.State = "OBSERVED_TERMINAL_NOT_SETTLED"
		out.Blockers = append(out.Blockers, "관측상 형제 종료: 독립 체결·정산 검증 전 완료 처리 금지")
	} else {
		out.State = "MAY_HAVE_SENT_RECHECK_REQUIRED"
		out.Blockers = append(out.Blockers, "과거 DELETE 전송 여부 불확실: 신규 DELETE 재전송 금지")
	}
	return out, nil
}
