package trading

// Offline-only, restartable wiring rehearsal for the existing one-shot
// protection POST / signed-only recovery / one-shot sibling DELETE functions.
// No production Binance transport can satisfy the private marker below.
// This is deliberately NOT a permission grant and is not registered with the
// HTTP server, scheduler, or DemoProtectionRuntimeCoordinator.

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
)

// Only an adapter implemented inside package trading (currently a test
// simulator) can satisfy this unexported marker; *exchange.Client cannot.
// Even an over-capable externally supplied transport must not be allowed to
// turn an operator's GET-only review into a live POST/DELETE.
type localOnlyProtectionRehearsalTransport interface {
	EntryProtectionLifecycleSignedReader
	ProtectionArmingTransport
	SupervisedSiblingCancelTransport
	localProtectionRehearsalOnly()
}

type EntryProtectionRehearsalAction string

const (
	EntryRehearsalArm     EntryProtectionRehearsalAction = "ARM_ONCE_OR_SIGNED_RECOVER"
	EntryRehearsalCancel  EntryProtectionRehearsalAction = "CANCEL_ONCE_OR_SIGNED_RECOVER"
	EntryRehearsalInspect EntryProtectionRehearsalAction = "INSPECT_ONLY"
)

type EntryProtectionRehearsalResult struct {
	GroupID              string                         `json:"group_id"`
	Action               EntryProtectionRehearsalAction `json:"action"`
	State                string                         `json:"state"`
	Before               EntryProtectionLifecycleReview `json:"before"`
	After                EntryProtectionLifecycleReview `json:"after"`
	Arming               ProtectionArmingResult         `json:"arming"`
	Cancel               SupervisedSiblingCancelResult  `json:"cancel"`
	TradingAuthorized    bool                           `json:"trading_authorized"`     // always false
	FinanciallySettled   bool                           `json:"financially_settled"`    // always false
	ManualReviewRequired bool                           `json:"manual_review_required"` // always true
	Reason               string                         `json:"reason"`
}

// RehearseEntryProtectionDispatch provides an OFFLINE test-only orchestration.
// Its action is explicitly chosen by a test. In particular:
//   - PREPARED may send exactly one TP then SL through the *simulated* transport.
//   - MAY_HAVE_SENT may only perform signed recovery, never a second POST/DELETE.
//   - Only a freshly classified triggered+active pair may reach a simulated
//     sibling DELETE, with caller-provided offline ownership evidence checked
//     against the fresh review. The caller's audit flag is not trustworthy for
//     production; hence the sealed local-only transport.
//   - The real exchange Client does not implement the sealed interface.
func RehearseEntryProtectionDispatch(ctx context.Context, action EntryProtectionRehearsalAction,
	r Reservation, coordinator *Coordinator, lifecycle *ExecutionLifecycleStore,
	protection *ProtectionSubmissionStore, cancels *CancelIntentStore,
	transport localOnlyProtectionRehearsalTransport, creds DemoCredentials,
	evidence *ProtectionEvidence,
) (EntryProtectionRehearsalResult, error) {
	result := EntryProtectionRehearsalResult{GroupID: entryProtectionGroupID(r), Action: action, State: "HALT", ManualReviewRequired: true}
	fail := func(why string, err error) (EntryProtectionRehearsalResult, error) {
		result.Reason = why
		return result, err
	}
	if ctx == nil {
		return fail("nil context", errors.New("rehearsal context 누락"))
	}
	if err := ctx.Err(); err != nil {
		return fail("context 종료", err)
	}
	// Reject unknown actions before even issuing signed GETs. Never interpret
	// a malformed supervisor command as a request to inspect/prepare state.
	switch action {
	case EntryRehearsalInspect, EntryRehearsalArm, EntryRehearsalCancel:
	default:
		return fail("지원되지 않는 모의 동작", errors.New("unsupported rehearsal action"))
	}
	if transport == nil || coordinator == nil || lifecycle == nil || protection == nil || cancels == nil || creds.APIKey == "" || creds.Secret == "" {
		return fail("필수 구성요소 누락", errors.New("rehearsal 구성요소 누락"))
	}
	if filepath.Clean(lifecycle.root) != filepath.Clean(coordinator.root) ||
		filepath.Clean(protection.dir) != filepath.Join(coordinator.root, "protection_submissions") ||
		filepath.Clean(cancels.dir) != filepath.Join(coordinator.root, "algo_cancel_intents") {
		return fail("외부 원장 혼용", errors.New("rehearsal ledger root mismatch"))
	}
	review := func() (EntryProtectionLifecycleReview, error) {
		return ReviewRecoveredEntryProtectionLifecycleSigned(ctx, r, coordinator, lifecycle, protection, cancels, transport, creds)
	}
	before, err := review()
	result.Before = before
	if err != nil {
		return fail("실행 전 signed lifecycle 검증 실패", err)
	}
	plan, err := protection.read(result.GroupID)
	if err != nil {
		return fail("durable plan 확인 실패", err)
	}
	owner := OwnerPosition{Source: r.Intent.Source, ModelID: r.Intent.ModelID, Side: r.Intent.PositionSide, Symbol: r.Intent.Symbol, Quantity: r.Intent.Quantity}
	switch action {
	case EntryRehearsalInspect:
		result.After = before
		result.State = before.State
		result.Reason = "로컬 모의 실행: 읽기 전용 확인"
		return result, nil
	case EntryRehearsalArm:
		switch before.State {
		case EntryLifecyclePreparedOnly:
			// ArmDemoProtection itself persists each irreversible MAY_HAVE_SENT
			// BEFORE a mock POST, with full signed preflights and signed readback.
			armed, e := ArmDemoProtection(ctx, result.GroupID, owner, coordinator, protection, transport, creds)
			result.Arming = armed
			if e != nil {
				return fail("모의 발행 결과 불확실: 재전송 금지, 체크포인트 유지", e)
			}
		case EntryLifecycleUnknownSubmission:
			// RecoverDemoProtectionArming uses signed account/Algo GET only.
			recovered, e := RecoverDemoProtectionArming(ctx, result.GroupID, owner, coordinator, protection, transport, creds)
			result.Arming = recovered
			if e != nil {
				return fail("모의 발행 MAY_HAVE_SENT 복구 불가: POST 재시도 금지", e)
			}
		default:
			// Single leg, active pair, canceled, partial and double-triggered
			// states never grant another protective POST.
			return fail("이 상태에서 모의 POST/재발행 금지", fmt.Errorf("arming state %s", before.State))
		}
	case EntryRehearsalCancel:
		if before.CancelMayHaveBeenSent && (before.State == EntryLifecycleCancelAmbiguous || before.State == EntryLifecycleClosedNotSettled) {
			// On restart, never replay DELETE; merely recover the prior
			// attempted cancel using a fresh signed GET.
			recovered, e := cancels.RecoverAttemptedCancel(ctx, transport, creds.APIKey, creds.Secret, result.GroupID)
			result.Cancel = SupervisedSiblingCancelResult{GroupID: result.GroupID, State: string(recovered.State), SiblingAlgoID: recovered.SiblingAlgoID, Reason: recovered.Reason, ManualReviewRequired: true}
			if e != nil {
				return fail("이전 DELETE 결과 signed 재조회 실패", e)
			}
		} else {
			if before.State != EntryLifecycleCancelReview || before.CancelMayHaveBeenSent || evidence == nil || !evidence.FullSignedOwnershipAudit {
				return fail("독립 감사 또는 취소 대상 확인 불충분", errors.New("sibling cancel review required"))
			}
			// Fail closed on a stale, forged or different-source evidence
			// bundle even inside the offline-only rehearsal.
			if !sameOfflineCancelReview(*evidence, before, plan, owner) {
				return fail("모의 감사 증거와 signed 검증 결과 불일치", errors.New("signed cancel evidence mismatch"))
			}
			canceled, e := SuperviseSiblingCancelOnce(ctx, coordinator, protection, cancels, transport, creds, result.GroupID, *evidence)
			result.Cancel = canceled
			if e != nil {
				return fail("모의 DELETE 불확실: 재시도 금지", e)
			}
		}
	default:
		return fail("알 수 없는 모의 작업", errors.New("unsupported rehearsal action"))
	}
	after, err := review()
	result.After = after
	if err != nil {
		return fail("실행 후 signed 재검증 실패", err)
	}
	result.State = after.State
	result.Reason = "모의 실행 결과만 관측됨; 실거래 권한·정산 권한 없음"
	return result, nil
}

func sameOfflineCancelReview(e ProtectionEvidence, review EntryProtectionLifecycleReview, plan ProtectionDispatchPlan, owner OwnerPosition) bool {
	if e.Owner.Symbol != owner.Symbol || e.Owner.Side != owner.Side || e.Owner.Source != owner.Source ||
		e.Owner.ModelID != owner.ModelID || !sameSignedDecimal(e.Owner.Quantity, owner.Quantity, true) ||
		e.SignedAccountAsOf.IsZero() || !e.FullSignedOwnershipAudit ||
		e.TP.AlgoID != review.Checkpoint.TP.ObservedAlgoID || e.SL.AlgoID != review.Checkpoint.SL.ObservedAlgoID ||
		e.TP.Status != review.TPStatus || e.SL.Status != review.SLStatus ||
		e.TP.ClientID != plan.TP.ClientAlgoID || e.SL.ClientID != plan.SL.ClientAlgoID ||
		e.TP.Symbol != plan.TP.Symbol || e.SL.Symbol != plan.SL.Symbol ||
		e.TP.PositionSide != plan.PositionSide || e.SL.PositionSide != plan.PositionSide ||
		!sameSignedDecimal(e.TP.Quantity, plan.TP.Quantity, true) || !sameSignedDecimal(e.SL.Quantity, plan.SL.Quantity, true) ||
		!sameSignedDecimal(e.RemainingOwnerQty, "0", false) ||
		!sameSignedDecimal(review.ExitFills.TotalFilledQty, owner.Quantity, true) {
		return false
	}
	return EvaluateProtection(ProtectionPreview{TP: plan.TP, SL: plan.SL}, e, timeNowUTC()).State == ProtectionCancelSibling
}

// Compile-time assurance that the production client is NOT a compatible
// implementation is supplied by the private method on the interface.
var _ EntryProtectionLifecycleSignedReader = (localOnlyProtectionRehearsalTransport)(nil)
var _ ProtectionArmingTransport = (localOnlyProtectionRehearsalTransport)(nil)
var _ SupervisedSiblingCancelTransport = (localOnlyProtectionRehearsalTransport)(nil)
