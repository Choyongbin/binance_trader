package trading

// Product protection reconciliation control plane. This derives an explicit
// next step from ONE independently signed lifecycle observation. No returned
// field is a capability to issue an exchange mutation. The only live mutation
// entry points (DemoProtectionRuntimeCoordinator.Arm and
// CancelTriggeredSibling) remain hard-disabled until independently certified.

import (
	"context"
	"errors"
)

type ProductProtectionStepKind string

const (
	ProductStepHalt             ProductProtectionStepKind = "HALT_MANUAL_RECONCILIATION"
	ProductStepReviewArm        ProductProtectionStepKind = "REVIEW_ARM_NOT_AUTHORIZED"
	ProductStepRecoverPost      ProductProtectionStepKind = "SIGNED_RECOVER_POST_NO_REPLAY"
	ProductStepMonitorPair      ProductProtectionStepKind = "SIGNED_MONITOR_PAIR_NOT_OCO"
	ProductStepReviewCancel     ProductProtectionStepKind = "REVIEW_SIBLING_CANCEL_NOT_AUTHORIZED"
	ProductStepRecoverDelete    ProductProtectionStepKind = "SIGNED_RECOVER_DELETE_NO_REPLAY"
	ProductStepReviewSettlement ProductProtectionStepKind = "REVIEW_FINANCIAL_COMMIT_NOT_AUTHORIZED"
)

// ProductProtectionStep is an operator-facing reconciliation outcome, never
// an executable order intent. It can be regenerated after STOP or a process
// restart, but must always be recomputed from a fresh signed GET.
type ProductProtectionStep struct {
	ClientOrderID        string                    `json:"client_order_id"`
	GroupID              string                    `json:"group_id"`
	LifecycleState       string                    `json:"lifecycle_state"`
	NextStep             ProductProtectionStepKind `json:"next_step"`
	SignedRecoveryOnly   bool                      `json:"signed_recovery_only"`
	ManualReviewRequired bool                      `json:"manual_review_required"`
	TradingAuthorized    bool                      `json:"trading_authorized"`
	SubmitAuthorized     bool                      `json:"submit_authorized"`
	CancelAuthorized     bool                      `json:"cancel_authorized"`
	SettlementAuthorized bool                      `json:"settlement_authorized"`
}

func blockedProductProtectionStep(view EntryProtectionLifecycleReview) ProductProtectionStep {
	return ProductProtectionStep{
		ClientOrderID: view.ClientOrderID, GroupID: view.GroupID,
		LifecycleState: view.State, NextStep: ProductStepHalt, ManualReviewRequired: true,
	}
}

// classifyProductProtectionStep uses the exact shared policy used by the
// sealed simulator and product mutation reviews. A failed/ambiguous signed
// review is never converted into an ARM, DELETE, or settlement candidate.
func classifyProductProtectionStep(view EntryProtectionLifecycleReview) (ProductProtectionStep, error) {
	out := blockedProductProtectionStep(view)
	if !requestIDPattern.MatchString(view.ClientOrderID) ||
		view.GroupID != "entry_"+view.ClientOrderID ||
		!view.ManualReviewRequired {
		return out, errors.New("untrusted entry protection identity or claimed approval")
	}
	policy, err := classifyProtectionTransition(view)
	if err != nil {
		return out, err
	}
	out.SignedRecoveryOnly = policy.signedRecoveryOnly
	switch policy.kind {
	case protectionNeedArmReview:
		out.NextStep = ProductStepReviewArm
	case protectionRecoverPostOnly:
		out.NextStep = ProductStepRecoverPost
	case protectionMonitorPair:
		out.NextStep = ProductStepMonitorPair
	case protectionNeedCancelReview:
		out.NextStep = ProductStepReviewCancel
	case protectionRecoverDeleteOnly:
		out.NextStep = ProductStepRecoverDelete
	case protectionNeedSettlement:
		out.NextStep = ProductStepReviewSettlement
	default:
		return blockedProductProtectionStep(view), errors.New("unknown product protection transition")
	}
	return out, nil
}

// ReconcileEntryOnce is the non-mutating product orchestration entry point.
// It performs one complete signed review and derives exactly one next step.
// STOP does not revoke read access; a new process must inspect again. The
// method never invokes Arm, RecoverArming, CancelTriggeredSibling or Commit.
func (r *DemoProtectionRuntimeCoordinator) ReconcileEntryOnce(ctx context.Context, entry Reservation) (ProductProtectionStep, error) {
	blocked := blockedProductProtectionStep(EntryProtectionLifecycleReview{
		ClientOrderID: entry.ClientOrderID, GroupID: entryProtectionGroupID(entry), State: EntryLifecycleBlocked,
	})
	if r == nil || ctx == nil || entry.Intent.Action != Enter || entry.Intent.Symbol != "BTCUSDT" {
		return blocked, errors.New("invalid product protection orchestration input")
	}
	if err := ctx.Err(); err != nil {
		return blocked, err
	}
	view, err := r.InspectEntryProtection(ctx, entry)
	if err != nil {
		return blockedProductProtectionStep(view), err
	}
	return classifyProductProtectionStep(view)
}
