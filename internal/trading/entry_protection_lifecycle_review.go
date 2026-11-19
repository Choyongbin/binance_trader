package trading

// A GET-only continuation of the 3.117 signed-entry → durable TP/SL plan
// bridge. Two Binance conditional Algo legs are NOT an atomic OCO pair.
// This inspector does not issue POST/DELETE, mutate any ledger, release an
// owner reservation, decide financial settlement or authorize auto trading.
import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"binance_trader/internal/exchange"
)

// The interface deliberately excludes SubmitDemoProtection, CancelDemoAlgoOnce
// and any other exchange mutation method. Callers cannot use this workflow to
// create/cancel an exchange order, even when their adapter supports writes.
type EntryProtectionLifecycleSignedReader interface {
	EntryProtectionSignedPreflightReader
	SignedExitFillReader
}

const (
	EntryLifecyclePreparedOnly      = "PREPARED_UNSENT_OPERATOR_REVIEW"
	EntryLifecycleUnknownSubmission = "MAY_HAVE_SENT_NO_REPLAY"
	EntryLifecycleSingleLeg         = "ONE_LEG_OPERATOR_REVIEW"
	EntryLifecyclePairActive        = "PAIR_SIGNED_ACTIVE_NOT_OCO"
	EntryLifecycleCancelReview      = "TRIGGERED_SIBLING_CANCEL_REVIEW_ONLY"
	EntryLifecycleCancelAmbiguous   = "SIBLING_DELETE_MAY_HAVE_SENT_NO_REPLAY"
	EntryLifecycleClosedNotSettled  = "CLOSED_SIGNED_FILLS_NOT_SETTLED"
	EntryLifecyclePartialHalt       = "PARTIAL_EXIT_HALT"
	EntryLifecycleDoubleTriggerHalt = "DOUBLE_TRIGGER_HALT"
	EntryLifecycleBlocked           = "PROTECTION_LIFECYCLE_BLOCKED"
)

type EntryProtectionLifecycleReview struct {
	State                      string                     `json:"state"`
	ClientOrderID              string                     `json:"client_order_id"`
	GroupID                    string                     `json:"group_id"`
	Checkpoint                 ProtectionArmingCheckpoint `json:"checkpoint"`
	ExitFills                  SignedExitFillReview       `json:"exit_fills"`
	TPStatus                   string                     `json:"tp_status,omitempty"`
	SLStatus                   string                     `json:"sl_status,omitempty"`
	CancelIntentRecorded       bool                       `json:"cancel_intent_recorded"`
	CancelMayHaveBeenSent      bool                       `json:"cancel_may_have_been_sent"`
	Reason                     string                     `json:"reason"`
	TradingAuthorized          bool                       `json:"trading_authorized"`           // always false
	ProtectionSubmitAuthorized bool                       `json:"protection_submit_authorized"` // always false
	CancelAuthorized           bool                       `json:"cancel_authorized"`            // always false
	RetryAuthorized            bool                       `json:"retry_authorized"`             // always false
	SettlementAuthorized       bool                       `json:"settlement_authorized"`        // always false
	ManualReviewRequired       bool                       `json:"manual_review_required"`       // always true
}

func stableLifecycleSignedSnapshot(a, b exchange.ExecutionPreflightSnapshot) bool {
	return reflect.DeepEqual(a.Portfolio, b.Portfolio) &&
		reflect.DeepEqual(a.Configuration, b.Configuration) &&
		reflect.DeepEqual(a.OpenOrders, b.OpenOrders) &&
		reflect.DeepEqual(a.OpenAlgos, b.OpenAlgos)
}

// ReviewRecoveredEntryProtectionLifecycleSigned reconstructs the local
// PREPARED → MAY_HAVE_SENT → two observed legs → trigger → sibling cancellation
// incident state from *fresh* signed GETs. The only allowed outcome is a
// human-facing review. An old signed observation, even if locally durable,
// can never establish that an Algo order is currently open or closed.
//
// This method is deliberately not exposed to the product HTTP server and
// must not be called by the unattended trading loop.
func ReviewRecoveredEntryProtectionLifecycleSigned(ctx context.Context, r Reservation,
	coordinator *Coordinator, lifecycle *ExecutionLifecycleStore,
	protection *ProtectionSubmissionStore, cancels *CancelIntentStore,
	reader EntryProtectionLifecycleSignedReader, creds DemoCredentials,
) (EntryProtectionLifecycleReview, error) {
	out := EntryProtectionLifecycleReview{
		State: EntryLifecycleBlocked, ClientOrderID: r.ClientOrderID,
		GroupID: entryProtectionGroupID(r), ManualReviewRequired: true,
	}
	fail := func(reason string, err error) (EntryProtectionLifecycleReview, error) {
		out.State, out.Reason = EntryLifecycleBlocked, reason
		return out, err
	}
	if ctx == nil {
		return fail("context 없음", errors.New("protection lifecycle nil context"))
	}
	if err := ctx.Err(); err != nil {
		return fail("context 종료", err)
	}
	if coordinator == nil || lifecycle == nil || protection == nil || cancels == nil || reader == nil || creds.APIKey == "" || creds.Secret == "" {
		return fail("필수 구성요소/서명 인증 누락", errors.New("protection lifecycle signed GET 구성요소 누락"))
	}
	if filepath.Clean(lifecycle.root) != filepath.Clean(coordinator.root) ||
		filepath.Clean(protection.dir) != filepath.Join(coordinator.root, "protection_submissions") ||
		filepath.Clean(cancels.dir) != filepath.Join(coordinator.root, "algo_cancel_intents") {
		return fail("원장 데이터 경로 불일치", errors.New("protection lifecycle foreign store"))
	}
	if err := coordinator.ownsLock(); err != nil {
		return fail("coordinator 소유권 없음", err)
	}
	if r.Intent.Action != Enter || r.Intent.Symbol != "BTCUSDT" || r.ClientOrderID == "" ||
		!requestIDPattern.MatchString(out.GroupID) {
		return fail("지원되지 않는 진입 예약", errors.New("protection lifecycle entry invalid"))
	}
	state, err := lifecycle.Load(r)
	if err != nil || state.Stage != ExecutionFillsImported || validateHandoffLifecycleChain(state) != nil {
		return fail("서명 체결 진입 Lifecycle 무결성 검증 실패", errors.New("protection lifecycle incomplete entry"))
	}
	plan, err := protection.read(out.GroupID)
	if err != nil {
		return fail("보호 계획 누락/손상", err)
	}
	owner := OwnerPosition{Symbol: "BTCUSDT", Source: r.Intent.Source, ModelID: r.Intent.ModelID,
		Side: r.Intent.PositionSide, Quantity: r.Intent.Quantity}
	if plan.Source != owner.Source || plan.ModelID != owner.ModelID || plan.PositionSide != owner.Side {
		return fail("보호 계획 소유권 불일치", errors.New("protection lifecycle wrong owner"))
	}
	filled, err := positiveRat(state.Events[len(state.Events)-1].ExecutedQty)
	requested, e2 := positiveRat(r.Intent.Quantity)
	if err != nil || e2 != nil || filled.Cmp(requested) != 0 ||
		state.Events[len(state.Events)-1].ExchangeStatus != "FILLED" ||
		plan.TP.Quantity != owner.Quantity || plan.SL.Quantity != owner.Quantity {
		return fail("진입 체결과 보호 수량 불일치", errors.New("protection lifecycle fill mismatch"))
	}
	checkpoint, err := InspectProtectionArmingCheckpoint(protection, out.GroupID)
	out.Checkpoint = checkpoint
	if err != nil || checkpoint.State == "BLOCKED" {
		return fail("보호 전송 체크포인트 손상", errors.New("protection lifecycle checkpoint invalid"))
	}
	intent, attempted, cancelErr := cancels.Inspect(out.GroupID)
	if cancelErr != nil && !errors.Is(cancelErr, os.ErrNotExist) {
		return fail("취소 원장 손상", cancelErr)
	}
	out.CancelIntentRecorded, out.CancelMayHaveBeenSent = cancelErr == nil, attempted
	if cancelErr == nil && (!checkpoint.TP.SignedObserved || !checkpoint.SL.SignedObserved) {
		return fail("미관측 TP/SL에 대한 취소 인텐트 존재", errors.New("orphan cancellation"))
	}
	first, err := reader.FetchDemoExecutionPreflight(ctx, creds.APIKey, creds.Secret, "BTCUSDT")
	if err != nil {
		return fail("최초 signed account GET 실패", err)
	}
	// With no independent signed protection observation there must be no
	// exchange submission conclusion. A MAY_HAVE_SENT marker is irreversible.
	if checkpoint.State == "PREPARED_NEVER_DISPATCHED" {
		if out.CancelIntentRecorded {
			return fail("미전송 계획에 취소 인텐트 존재", errors.New("prepared plan cancellation"))
		}
		if err := validateProtectionPreflight(first, owner, map[int64]ProtectionInstruction{}); err != nil {
			return fail("미전송 보호 계획 signed 계정 불일치", err)
		}
	} else {
		// Validate common capability/freshness, without claiming the original
		// owner is still open after a signed exit fill.
		if err := validateLifecycleAccountEnvelope(first); err != nil {
			return fail("현재 보호 주문 계정 인증 증거 불충분", err)
		}
	}
	var tp, sl exchange.DemoAlgoResult
	for _, leg := range []struct {
		name string
		st   ProtectionLegState
		id   int64
		dst  *exchange.DemoAlgoResult
	}{
		{"tp", checkpoint.TP, checkpoint.TP.ObservedAlgoID, &tp},
		{"sl", checkpoint.SL, checkpoint.SL.ObservedAlgoID, &sl},
	} {
		if !leg.st.SignedObserved {
			continue
		}
		observed, err := reader.QueryDemoAlgo(ctx, creds.APIKey, creds.Secret, leg.id)
		if err != nil {
			return fail("보호 레그 signed GET 실패: "+leg.name, err)
		}
		if err := validateProtectionAlgo(plan, leg.name, leg.id, observed); err != nil {
			return fail("서명된 보호 주문과 원본 지시 불일치", err)
		}
		*leg.dst = observed
	}
	out.TPStatus, out.SLStatus = tp.Status, sl.Status
	if err := ctx.Err(); err != nil {
		return fail("signed 감사 도중 context 종료", err)
	}
	if checkpoint.TP.SignedObserved && checkpoint.SL.SignedObserved {
		// Read all actual trades before deriving exit/cancel status. This
		// routine independently re-GETs each leg around the trade lookup.
		exit, err := InspectSignedExitFills(ctx, protection, reader, creds, out.GroupID,
			checkpoint.TP.ObservedAlgoID, checkpoint.SL.ObservedAlgoID)
		if err != nil {
			return fail("signed 보호 청산 체결 감사 실패", err)
		}
		out.ExitFills = exit
	}
	second, err := reader.FetchDemoExecutionPreflight(ctx, creds.APIKey, creds.Secret, "BTCUSDT")
	if err != nil {
		return fail("최종 signed account GET 실패", err)
	}
	if err := validateLifecycleAccountEnvelope(first); err != nil {
		return fail("최초 signed 계정 관측이 감사 도중 만료됨", err)
	}
	if err := validateLifecycleAccountEnvelope(second); err != nil || !stableLifecycleSignedSnapshot(first, second) {
		return fail("조회 도중 계정/Algo 상태 변경 또는 서명 증거 불량", errors.New("signed account race or invalid capability"))
	}
	// A second local read rules out a checkpoint or plan being changed while
	// signed queries ran. This does NOT turn a read into a transaction.
	check2, err := InspectProtectionArmingCheckpoint(protection, out.GroupID)
	plan2, perr := protection.read(out.GroupID)
	entry2, lerr := lifecycle.Load(r)
	intent2, attempted2, cerr2 := cancels.Inspect(out.GroupID)
	if err != nil || perr != nil || lerr != nil || !reflect.DeepEqual(state, entry2) ||
		!reflect.DeepEqual(checkpoint, check2) || plan.Digest != plan2.Digest ||
		(cancelErr == nil) != (cerr2 == nil) || (cerr2 != nil && !errors.Is(cerr2, os.ErrNotExist)) ||
		(cancelErr == nil && (intent != intent2 || attempted != attempted2)) {
		return fail("signed 조회 중 로컬 원장/취소 체크포인트 변경", errors.New("local protection incident drift"))
	}
	if err := ctx.Err(); err != nil {
		return fail("최종 context 종료", err)
	}
	// Every active Algo in the full-account snapshot must belong to an
	// independently signed GET-verified observed leg. Unknown Algo = HALT.
	expectedOpen := map[int64]ProtectionInstruction{}
	for _, leg := range []struct {
		id     int64
		status string
		inst   ProtectionInstruction
	}{
		{checkpoint.TP.ObservedAlgoID, tp.Status, plan.TP},
		{checkpoint.SL.ObservedAlgoID, sl.Status, plan.SL},
	} {
		if isActiveProtection(leg.status) {
			expectedOpen[leg.id] = leg.inst
		}
	}
	// 3.119: A POST ACK may have been lost after the exchange accepted an
	// Algo, while the local leg remains MAY_HAVE_SENT without an Algo ID.
	// This is NOT an authorization to resubmit. Independently identify at
	// most one plan-exact signed open Algo per unobserved dispatched leg,
	// solely to permit the separate signed GET recovery path to run.
	if checkpoint.State == "TP_MAY_HAVE_SENT_UNRESOLVED" || checkpoint.State == "SL_MAY_HAVE_SENT_UNRESOLVED" {
		potential := []struct {
			name        string
			state       ProtectionLegState
			instruction ProtectionInstruction
		}{{"tp", checkpoint.TP, plan.TP}, {"sl", checkpoint.SL, plan.SL}}
		matched := map[string]bool{}
		for _, active := range first.OpenAlgos {
			if _, known := expectedOpen[active.AlgoID]; known {
				continue
			}
			identified := false
			for _, leg := range potential {
				if leg.state.SignedObserved || !leg.state.MayHaveSent ||
					active.ClientAlgoID != leg.instruction.ClientAlgoID {
					continue
				}
				if matched[leg.name] || !sameOpenProtectionAlgo(active, leg.instruction, active.AlgoID) {
					return fail("불확실한 레그와 open Algo 불일치/중복", errors.New("ambiguous open protection identity"))
				}
				fresh, getErr := reader.QueryDemoAlgo(ctx, creds.APIKey, creds.Secret, active.AlgoID)
				if getErr != nil {
					return fail("MAY_HAVE_SENT 레그 독립 signed GET 실패", getErr)
				}
				if err := validateProtectionAlgo(plan, leg.name, active.AlgoID, fresh); err != nil || !isActiveProtection(fresh.Status) {
					return fail("MAY_HAVE_SENT 레그 독립 signed identity/status 불일치", errors.New("unverified ambiguous Algo"))
				}
				matched[leg.name] = true
				expectedOpen[active.AlgoID] = leg.instruction
				identified = true
				break
			}
			if !identified {
				return fail("다른 소유자의 조건부 주문이 존재", errors.New("foreign live Algo"))
			}
		}
	}
	if len(first.OpenAlgos) != len(expectedOpen) {
		return fail("계정 전체의 미확인/누락 조건부 주문", errors.New("unexpected live algo"))
	}
	for _, active := range first.OpenAlgos {
		inst, ok := expectedOpen[active.AlgoID]
		if !ok || !sameOpenProtectionAlgo(active, inst, active.AlgoID) {
			return fail("open Algo identity mismatch", errors.New("unknown or altered open algo"))
		}
	}
	if len(first.OpenOrders) != 0 {
		return fail("다른 일반 미체결 주문이 존재", errors.New("unexpected normal open order"))
	}
	if out.CancelIntentRecorded && isActiveProtection(tp.Status) && isActiveProtection(sl.Status) {
		return fail("두 레그 모두 활성인데 이미 sibling 취소 인텐트 존재", errors.New("orphan or premature cancel intent"))
	}
	if out.CancelIntentRecorded && (intent.SiblingAlgoID != checkpoint.TP.ObservedAlgoID && intent.SiblingAlgoID != checkpoint.SL.ObservedAlgoID ||
		(intent.SiblingAlgoID == checkpoint.TP.ObservedAlgoID && intent.OwnerClientAlgoID != plan.TP.ClientAlgoID) ||
		(intent.SiblingAlgoID == checkpoint.SL.ObservedAlgoID && intent.OwnerClientAlgoID != plan.SL.ClientAlgoID)) {
		return fail("취소 대상이 TP/SL 원본과 불일치", errors.New("wrong sibling cancel target"))
	}
	switch checkpoint.State {
	case "PREPARED_NEVER_DISPATCHED":
		out.State, out.Reason = EntryLifecyclePreparedOnly, "서명 체결은 확인됐으나 TP/SL 실제 발행은 없음; 승인된 운영자 검토 필요"
	case "TP_MAY_HAVE_SENT_UNRESOLVED", "SL_MAY_HAVE_SENT_UNRESOLVED":
		out.State, out.Reason = EntryLifecycleUnknownSubmission, "발행 여부 불명: signed 전수 조회 및 단발 복구 필요, 자동 재전송 금지"
	case "TP_OBSERVED_SL_NOT_DISPATCHED":
		out.State, out.Reason = EntryLifecycleSingleLeg, "단일 레그만 서명 관측됨: 다른 레그 자동 제출 금지"
	case "TWO_LEGS_PREVIOUSLY_OBSERVED_RECHECK_REQUIRED":
		// This is a classifier, not an approval to dispatch a DELETE. A
		// complete *account-wide historical* ownership audit is still needed.
		if isTriggeredProtection(tp.Status) && isTriggeredProtection(sl.Status) {
			out.State, out.Reason = EntryLifecycleDoubleTriggerHalt, "TP/SL 이중 발동: 계정 전체 독립 재조정 필요"
			return out, nil
		}
		if isActiveProtection(tp.Status) && isActiveProtection(sl.Status) && out.ExitFills.TotalFilledQty == "0" && !out.CancelIntentRecorded {
			if err := validateProtectionPreflight(first, owner, expectedOpen); err != nil {
				return fail("활성 보호 레그 및 pooled-side 잔고 불일치", err)
			}
			out.State, out.Reason = EntryLifecyclePairActive, "두 레그 활성 관측: atomic OCO가 아니며 지속 모니터링·운영자 검토 필요"
			return out, nil
		}
		if out.ExitFills.State == "PARTIAL_OR_UNFILLED_MANUAL_RECONCILIATION" {
			out.State, out.Reason = EntryLifecyclePartialHalt, "부분/미확인 exit fill: 주문/소유권 재조정 전 취소·재전송 금지"
			return out, nil
		}
		if out.ExitFills.State != "EXIT_FILLS_MATCH_NOT_SETTLED" && out.ExitFills.State != "OPEN_LEG_MANUAL_RECONCILIATION" {
			return fail("보호 레그 signed exit 상태 미확정", errors.New("unreviewed protection exit"))
		}
		if err := exactZeroLifecycleSide(first.Portfolio, plan.PositionSide); err != nil {
			return fail("체결 이후 pooled Hedge-side 잔고 불일치", err)
		}
		triggerAndActive := isTriggeredProtection(tp.Status) && isActiveProtection(sl.Status) ||
			isTriggeredProtection(sl.Status) && isActiveProtection(tp.Status)
		triggerAndCanceled := isTriggeredProtection(tp.Status) && isCanceledProtection(sl.Status) ||
			isTriggeredProtection(sl.Status) && isCanceledProtection(tp.Status)
		if triggerAndActive {
			activeID := checkpoint.TP.ObservedAlgoID
			if isActiveProtection(sl.Status) {
				activeID = checkpoint.SL.ObservedAlgoID
			}
			if out.CancelIntentRecorded && intent.SiblingAlgoID != activeID {
				return fail("기존 취소 인텐트의 sibling ID가 활성 레그와 다름", errors.New("cancel target mismatch"))
			}
			if out.CancelMayHaveBeenSent {
				out.State, out.Reason = EntryLifecycleCancelAmbiguous, "기존 DELETE 결과 불확실: signed 재조회만, 재전송 금지"
			} else {
				out.State, out.Reason = EntryLifecycleCancelReview, "전체 계정 이력 및 독립 Ownership 확인 후 운영자 취소 검토 필요; 실행 권한 없음"
			}
			return out, nil
		}
		if triggerAndCanceled {
			cancelledID := checkpoint.TP.ObservedAlgoID
			if isCanceledProtection(sl.Status) {
				cancelledID = checkpoint.SL.ObservedAlgoID
			}
			if out.CancelIntentRecorded && intent.SiblingAlgoID != cancelledID {
				return fail("종료 sibling의 취소 대상 불일치", errors.New("cancel target mismatch"))
			}
			out.State, out.Reason = EntryLifecycleClosedNotSettled, "서명 체결·형제 취소 관측: 실제 금융 정산·원장 커밋·펀딩 귀속은 미완료"
			return out, nil
		}
		return fail("예상하지 않은 TP/SL terminal 상태 조합", errors.New("unhandled protection state"))
	default:
		return fail("알 수 없는 보호 체크포인트", fmt.Errorf("checkpoint=%s", checkpoint.State))
	}
	return out, nil
}

func validateLifecycleAccountEnvelope(s exchange.ExecutionPreflightSnapshot) error {
	now := time.Now().UTC()
	if s.Environment != "demo" || s.StartedAt.IsZero() || s.FinishedAt.Before(s.StartedAt) ||
		s.FinishedAt.Sub(s.StartedAt) > 5*time.Second || s.FinishedAt.After(now.Add(time.Second)) ||
		now.Sub(s.FinishedAt) > 5*time.Second || !s.Portfolio.CanTrade ||
		s.OpenOrders == nil || s.OpenAlgos == nil || exchange.ValidatePortfolioSnapshot(s.Portfolio) != nil {
		return errors.New("signed 보호 lifecycle account envelope 불완전")
	}
	c := s.Configuration
	if c.Environment != "demo" || c.Symbol != "BTCUSDT" || !c.SettingsMatchV1 || !c.HedgeMode || c.MarginType != "ISOLATED" || c.Leverage != 15 || c.AutoAddMargin {
		return errors.New("signed 보호 lifecycle capability 불일치")
	}
	return nil
}

// exactProtectionSideQty intentionally requires a *positive* arming balance.
// A signed full exit instead requires a true zero, not an absent/unknown
// quantity silently cast to a positive owner value.
func exactZeroLifecycleSide(p exchange.AccountPortfolio, side PositionSide) error {
	seen := false
	for _, position := range p.Positions {
		if position.Symbol != "BTCUSDT" || position.PositionSide != string(side) {
			continue
		}
		if seen {
			return errors.New("signed exit 동일 Hedge 방향 중복 포지션")
		}
		seen = true
		q, ok := new(big.Rat).SetString(string(position.PositionAmt))
		if !ok {
			return errors.New("signed exit position 숫자 오류")
		}
		// Never allow a positive and a negative row to cancel out to zero.
		if q.Sign() != 0 {
			return errors.New("signed exit 이후 pooled position이 0이 아님")
		}
	}
	return nil
}
