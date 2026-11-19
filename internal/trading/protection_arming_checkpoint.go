package trading

// Read-only restart classifier for the local TP/SL arming checkpoints.
// It does not infer order presence from an old signed observation. Any
// MAY_HAVE_SENT event has to be reconciled with a new signed exchange view
// using the dedicated supervised recovery flow; it is NEVER a retry ticket.
import "errors"

type ProtectionArmingCheckpoint struct {
	GroupID                    string             `json:"group_id"`
	State                      string             `json:"state"`
	TP                         ProtectionLegState `json:"tp"`
	SL                         ProtectionLegState `json:"sl"`
	Blockers                   []string           `json:"blockers"`
	TradingAuthorized          bool               `json:"trading_authorized"`           // ALWAYS false
	ProtectionSubmitAuthorized bool               `json:"protection_submit_authorized"` // ALWAYS false
	RetryAuthorized            bool               `json:"retry_authorized"`             // ALWAYS false
	ManualReviewRequired       bool               `json:"manual_review_required"`       // ALWAYS true
}

// InspectProtectionArmingCheckpoint performs local reads only. It does not
// check fresh exchange order status and cannot assert that a protective pair
// is still armed (two signed-observed markers may now be stale).
func InspectProtectionArmingCheckpoint(store *ProtectionSubmissionStore, group string) (ProtectionArmingCheckpoint, error) {
	out := ProtectionArmingCheckpoint{GroupID: group, State: "BLOCKED", Blockers: []string{}, ManualReviewRequired: true}
	if store == nil {
		return out, errors.New("protection submission store 없음")
	}
	if _, err := store.read(group); err != nil {
		return out, err
	}
	tp, err := store.Inspect(group, "tp")
	if err != nil {
		return out, err
	}
	sl, err := store.Inspect(group, "sl")
	if err != nil {
		return out, err
	}
	out.TP, out.SL = tp, sl
	switch {
	case (tp.SignedObserved && !tp.MayHaveSent) || (sl.SignedObserved && !sl.MayHaveSent):
		out.Blockers = append(out.Blockers, "signed 관측과 MAY_HAVE_SENT 기록의 순서가 불가능합니다")
	case tp.SignedObserved && sl.SignedObserved && tp.ObservedAlgoID == sl.ObservedAlgoID:
		out.Blockers = append(out.Blockers, "서로 다른 TP/SL 레그가 같은 exchange Algo ID를 사용합니다")
	case sl.MayHaveSent && !tp.SignedObserved:
		out.Blockers = append(out.Blockers, "TP 서명 관측 없이 SL 전송 표식이 기록됐습니다")
	case !tp.MayHaveSent && !sl.MayHaveSent:
		out.State = "PREPARED_NEVER_DISPATCHED"
	case tp.MayHaveSent && !tp.SignedObserved:
		out.State = "TP_MAY_HAVE_SENT_UNRESOLVED"
		out.Blockers = append(out.Blockers, "TP 전송 여부 불명: 자동 재전송 금지")
	case tp.SignedObserved && !sl.MayHaveSent:
		out.State = "TP_OBSERVED_SL_NOT_DISPATCHED"
		out.Blockers = append(out.Blockers, "TP의 현재 상태를 최신 signed GET으로 다시 확인해야 합니다")
	case sl.MayHaveSent && !sl.SignedObserved:
		out.State = "SL_MAY_HAVE_SENT_UNRESOLVED"
		out.Blockers = append(out.Blockers, "SL 전송 여부 불명: 자동 재전송 금지")
	case tp.SignedObserved && sl.SignedObserved:
		out.State = "TWO_LEGS_PREVIOUSLY_OBSERVED_RECHECK_REQUIRED"
		out.Blockers = append(out.Blockers, "두 레그의 현재 활성 상태·실제 체결·계정 소유권 재검증 필요")
	default:
		out.Blockers = append(out.Blockers, "알 수 없는 보호주문 체크포인트 조합")
	}
	return out, nil
}
