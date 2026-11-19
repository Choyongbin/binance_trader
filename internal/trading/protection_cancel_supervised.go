package trading

// Internal-only supervisor for a mockable, one-shot sibling-cancel experiment.
// This is NOT a live trading gate: the ownership audit in ProtectionEvidence
// is caller-supplied; future product integration must independently establish
// the complete signed fills, pooled-side position and a fresh account baseline.
// Never expose this entry point through HTTP or the automatic trading loop.
import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"binance_trader/internal/exchange"
)

type SupervisedSiblingCancelTransport interface {
	SignedProtectionReader
	AlgoCancelTransport
}

type SupervisedSiblingCancelResult struct {
	GroupID              string `json:"group_id"`
	State                string `json:"state"`
	SiblingAlgoID        int64  `json:"sibling_algo_id,omitempty"`
	DeleteAttempts       int    `json:"delete_attempts"`
	TradingAuthorized    bool   `json:"trading_authorized"`     // ALWAYS false
	FinanciallySettled   bool   `json:"financially_settled"`    // ALWAYS false
	ManualReviewRequired bool   `json:"manual_review_required"` // ALWAYS true
	Reason               string `json:"reason"`
}

func matchSignedAlgoToEvidence(o exchange.DemoAlgoResult, e AlgoObservation, instruction ProtectionInstruction) error {
	if o.AlgoID != e.AlgoID || o.ClientAlgoID != e.ClientID || o.Status != e.Status ||
		o.Symbol != instruction.Symbol || o.Side != instruction.Side || o.PositionSide != string(instruction.PositionSide) ||
		o.Type != instruction.OrderType || o.WorkingType != instruction.WorkingType || o.AlgoType != instruction.AlgoType ||
		o.ClosePosition || o.ReduceOnly || !sameSignedDecimal(string(o.Quantity), instruction.Quantity, true) ||
		!sameSignedDecimal(string(o.Quantity), e.Quantity, true) ||
		(e.ActualOrderID != "" && o.ActualOrderID != e.ActualOrderID) ||
		(e.ActualQty != "" && !sameSignedDecimal(string(o.ActualQty), e.ActualQty, false)) {
		return errors.New("독립 signed GET 결과가 보호 주문 계획/관측과 불일치")
	}
	return nil
}

// SuperviseSiblingCancelOnce is deliberately NOT wired into any server/daemon.
// The first call may issue at most one DELETE, only after durable MAY_HAVE_SENT.
// On restart with an existing dispatch it performs signed GET ONLY. The
// returned flags never grant additional trading or financial settlement.
func SuperviseSiblingCancelOnce(ctx context.Context, coordinator *Coordinator, submissions *ProtectionSubmissionStore,
	cancels *CancelIntentStore, transport SupervisedSiblingCancelTransport, creds DemoCredentials,
	group string, evidence ProtectionEvidence) (SupervisedSiblingCancelResult, error) {
	out := SupervisedSiblingCancelResult{GroupID: group, State: "HALT", ManualReviewRequired: true}
	if coordinator == nil || submissions == nil || cancels == nil || transport == nil || creds.APIKey == "" || creds.Secret == "" {
		return out, errors.New("감독형 취소 필수 구성요소 누락")
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if err := coordinator.ownsLock(); err != nil {
		return out, err
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}

	intent, attempted, inspectErr := cancels.Inspect(group)
	if inspectErr == nil && attempted {
		// The durable marker may already correspond to a DELETE. NEVER
		// evaluate another intent or issue a second DELETE after restart.
		recovered, err := cancels.RecoverAttemptedCancel(ctx, transport, creds.APIKey, creds.Secret, group)
		out.SiblingAlgoID = intent.SiblingAlgoID
		out.State = string(recovered.State)
		out.Reason = recovered.Reason
		return out, err
	}
	if inspectErr != nil && !errors.Is(inspectErr, os.ErrNotExist) {
		return out, inspectErr
	}

	now := time.Now().UTC()
	continuity, err := InspectSiblingCancelContinuity(submissions, cancels, group, evidence, now)
	if err != nil {
		return out, err
	}
	if continuity.State != "REQUIRES_OPERATOR_CANCEL_REVIEW" && continuity.State != "INTENT_ONLY_NOT_DISPATCHED" {
		return out, fmt.Errorf("취소 전제조건 불충족: %s", continuity.State)
	}
	plan, err := submissions.read(group)
	if err != nil {
		return out, err
	}
	// Fresh signed reads for both legs, not caller-provided status alone.
	for _, leg := range []struct {
		obs         AlgoObservation
		instruction ProtectionInstruction
	}{
		{evidence.TP, plan.TP}, {evidence.SL, plan.SL},
	} {
		o, err := transport.QueryDemoAlgo(ctx, creds.APIKey, creds.Secret, leg.obs.AlgoID)
		if err != nil {
			return out, fmt.Errorf("독립 보호 주문 signed GET 실패: %w", err)
		}
		if err := matchSignedAlgoToEvidence(o, leg.obs, leg.instruction); err != nil {
			return out, err
		}
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	// Re-evaluate before first durable intent. Older evidence cannot be
	// accepted after the signed round trip exceeds the freshness window.
	continuity, err = InspectSiblingCancelContinuity(submissions, cancels, group, evidence, time.Now().UTC())
	if err != nil {
		return out, err
	}
	if continuity.State != "REQUIRES_OPERATOR_CANCEL_REVIEW" && continuity.State != "INTENT_ONLY_NOT_DISPATCHED" {
		return out, errors.New("서명 재조회 이후 취소 인계 상태 변경")
	}
	clientID := plan.TP.ClientAlgoID
	if continuity.SiblingAlgoID == evidence.SL.AlgoID {
		clientID = plan.SL.ClientAlgoID
	}
	newIntent, err := cancels.Plan(group, continuity.SiblingAlgoID, clientID)
	if err != nil {
		return out, err
	}
	if inspectErr == nil && (intent.Digest != newIntent.Digest || attempted) {
		return out, errors.New("이미 기록된 취소 계획과 다른 대상")
	}
	out.SiblingAlgoID = newIntent.SiblingAlgoID
	// AttemptAlgoCancel persists MAY_HAVE_SENT before calling DELETE.
	// Any error (including ACK loss) becomes an irreversible no-replay state.
	out.DeleteAttempts = 1 // this invocation reached the one-shot attempt boundary
	_, err = cancels.AttemptAlgoCancel(ctx, transport, creds.APIKey, creds.Secret, newIntent)
	if err != nil {
		out.State = "MAY_HAVE_SENT_RECHECK_REQUIRED"
		out.Reason = "취소 결과 불명확: 재시작 시 signed GET만 허용"
		return out, err
	}
	recovered, err := cancels.RecoverAttemptedCancel(ctx, transport, creds.APIKey, creds.Secret, group)
	out.State, out.Reason = string(recovered.State), recovered.Reason
	return out, err
}
