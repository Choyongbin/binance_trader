package trading

// This file defines the product-side approval REVIEW boundary, not an
// authorization capability. Real Binance Algo POST/DELETE remains disabled in
// DemoProtectionRuntimeCoordinator.Arm and CancelTriggeredSibling. A signed
// observation can identify a possible next operational step, but can never
// prove account-wide history, independent owner rights, atomic OCO behavior,
// or uninterrupted supervision.

import (
	"context"
	"errors"
	"fmt"
)

// ProductProtectionMutation is a requested future exchange side effect. No
// value of this type is an authorization to perform that side effect.
type ProductProtectionMutation string

const (
	ProductProtectionArm    ProductProtectionMutation = "ARM_TP_SL"
	ProductProtectionCancel ProductProtectionMutation = "CANCEL_SIBLING"
)

const (
	ProductMutationReviewBlocked  = "PRODUCT_MUTATION_BLOCKED"
	ProductMutationReviewIncident = "PRODUCT_MUTATION_INCIDENT"
	ProductMutationReviewReadOnly = "SIGNED_RECHECK_ONLY"
)

// ProductProtectionMutationReview is an operator-facing, GET-only decision.
// The false permission fields are intentional and must never be derived from
// the signed account snapshot or a caller's own claimed approval.
type ProductProtectionMutationReview struct {
	State                string                    `json:"state"`
	Action               ProductProtectionMutation `json:"action"`
	GroupID              string                    `json:"group_id,omitempty"`
	ClientOrderID        string                    `json:"client_order_id,omitempty"`
	LifecycleState       string                    `json:"lifecycle_state,omitempty"`
	NextStep             string                    `json:"next_step,omitempty"`
	SignedRecoveryOnly   bool                      `json:"signed_recovery_only"`
	Blockers             []string                  `json:"blockers"`
	ManualReviewRequired bool                      `json:"manual_review_required"`
	TradingAuthorized    bool                      `json:"trading_authorized"`
	SubmitAuthorized     bool                      `json:"submit_authorized"`
	CancelAuthorized     bool                      `json:"cancel_authorized"`
	SettlementAuthorized bool                      `json:"settlement_authorized"`
}

func newDeniedProductMutationReview(entry Reservation, action ProductProtectionMutation) ProductProtectionMutationReview {
	return ProductProtectionMutationReview{
		State: ProductMutationReviewBlocked, Action: action,
		ClientOrderID: entry.ClientOrderID, GroupID: entryProtectionGroupID(entry),
		Blockers: []string{"PRODUCT_ALGO_MUTATIONS_NOT_CERTIFIED"}, ManualReviewRequired: true,
	}
}

// ReviewProtectionMutation reads a freshly signed lifecycle for this EXACT
// durable entry, then explains why a product mutation is blocked. It cannot
// submit/cancel an order, write a settlement, or create an approval token.
// It is intentionally independent of the auto-run lease: STOP must not hide
// a position from read-only incident diagnosis.
//
// The result must not be used as a permission gate: Arm and
// CancelTriggeredSibling remain hard-disabled even if this method returns nil.
func (r *DemoProtectionRuntimeCoordinator) ReviewProtectionMutation(ctx context.Context,
	entry Reservation, action ProductProtectionMutation,
) (ProductProtectionMutationReview, error) {
	out := newDeniedProductMutationReview(entry, action)
	if action != ProductProtectionArm && action != ProductProtectionCancel {
		out.Blockers = append(out.Blockers, "UNKNOWN_MUTATION_ACTION")
		return out, fmt.Errorf("unsupported protection product mutation %q: %w", action, ErrProtectionApproval)
	}
	if ctx == nil {
		out.Blockers = append(out.Blockers, "MISSING_CONTEXT")
		return out, errors.New("product protection mutation review requires context")
	}
	if err := ctx.Err(); err != nil {
		out.Blockers = append(out.Blockers, "CONTEXT_CANCELED")
		return out, err
	}
	if entry.Intent.Action != Enter || entry.Intent.Symbol != "BTCUSDT" || entry.ClientOrderID == "" {
		out.Blockers = append(out.Blockers, "INVALID_ENTRY_IDENTITY")
		return out, fmt.Errorf("product mutation requires a durable BTCUSDT entry: %w", ErrProtectionApproval)
	}
	// InspectEntryProtection checks lock ownership, local sealed lifecycle,
	// both signed account reads, protective Algo identities and exit fills.
	signed, err := r.InspectEntryProtection(ctx, entry)
	out.LifecycleState = signed.State
	if err != nil || signed.State == EntryLifecycleBlocked ||
		signed.ClientOrderID != entry.ClientOrderID || signed.GroupID != out.GroupID ||
		!signed.ManualReviewRequired || signed.TradingAuthorized || signed.ProtectionSubmitAuthorized ||
		signed.CancelAuthorized || signed.RetryAuthorized || signed.SettlementAuthorized {
		out.State = ProductMutationReviewIncident
		out.Blockers = append(out.Blockers, "SIGNED_OWNER_OR_ORDER_REVIEW_FAILED")
		if err != nil {
			return out, fmt.Errorf("product mutation signed review failed: %w", err)
		}
		return out, fmt.Errorf("untrusted product signed review: %w", ErrProtectionApproval)
	}
	// The mock supervisor and the product review share ONE transition policy.
	// The decision is explanatory only: all authorization flags stay false.
	policy, policyErr := classifyProtectionTransition(signed)
	if policyErr != nil {
		out.State = ProductMutationReviewIncident
		out.Blockers = append(out.Blockers, "INCONSISTENT_PROTECTION_TRANSITION")
		// Known, already-quarantined incidents remain a normal GET-only
		// operator response, not a transport error. Contradictory journals or
		// future states must still report an error and remain blocked.
		switch signed.State {
		case EntryLifecycleSingleLeg, EntryLifecyclePartialHalt, EntryLifecycleDoubleTriggerHalt:
			return out, nil
		}
		return out, fmt.Errorf("unsafe signed protection transition: %w", policyErr)
	}
	out.NextStep = string(policy.kind)
	out.SignedRecoveryOnly = policy.signedRecoveryOnly
	switch action {
	case ProductProtectionArm:
		switch policy.kind {
		case protectionNeedArmReview:
			out.Blockers = append(out.Blockers, "TP_SL_SUBMISSION_APPROVAL_UNAVAILABLE", "NON_ATOMIC_OCO_NOT_DEMO_VERIFIED")
		case protectionRecoverPostOnly:
			out.State = ProductMutationReviewReadOnly
			out.Blockers = append(out.Blockers, "MAY_HAVE_SENT_SIGNED_GET_ONLY_NO_POST_RETRY")
		case protectionMonitorPair:
			out.Blockers = append(out.Blockers, "TWO_PROTECTION_LEGS_ALREADY_PRESENT_NO_REARM")
		default:
			out.State = ProductMutationReviewIncident
			out.Blockers = append(out.Blockers, "ENTRY_STATE_REQUIRES_MANUAL_RECONCILIATION_NO_POST")
		}
	case ProductProtectionCancel:
		switch policy.kind {
		case protectionNeedCancelReview:
			out.Blockers = append(out.Blockers,
				"INDEPENDENT_ACCOUNT_WIDE_OWNERSHIP_PROOF_MISSING",
				"SIBLING_CANCEL_E2E_CERTIFICATION_MISSING")
		case protectionRecoverDeleteOnly:
			out.State = ProductMutationReviewReadOnly
			out.Blockers = append(out.Blockers, "DELETE_MAY_HAVE_SENT_SIGNED_GET_ONLY_NO_REPLAY")
		case protectionNeedSettlement:
			out.Blockers = append(out.Blockers, "EXIT_FILLS_NOT_FINANCIALLY_SETTLED")
		default:
			out.State = ProductMutationReviewIncident
			out.Blockers = append(out.Blockers, "NO_UNAMBIGUOUS_SIBLING_CANCEL_TARGET")
		}
	}

	return out, nil
}
