package trading

// Shared, side-effect-free transition policy for the offline simulator and
// product's signed, read-only mutation review. A transition is a description
// of what evidence is missing, NEVER a permission or an exchange operation.
// The product coordinator's Arm/CancelTriggeredSibling methods remain locked.

import (
	"errors"
	"fmt"
)

type protectionTransitionKind string

const (
	protectionNeedArmReview     protectionTransitionKind = "NEED_ARM_APPROVAL_REVIEW"
	protectionRecoverPostOnly   protectionTransitionKind = "POST_MAY_HAVE_SENT_SIGNED_GET_ONLY"
	protectionMonitorPair       protectionTransitionKind = "MONITOR_PAIR_NOT_OCO"
	protectionNeedCancelReview  protectionTransitionKind = "NEED_INDEPENDENT_CANCEL_PROOF"
	protectionRecoverDeleteOnly protectionTransitionKind = "DELETE_MAY_HAVE_SENT_SIGNED_GET_ONLY"
	protectionNeedSettlement    protectionTransitionKind = "CLOSED_NEEDS_FINANCIAL_COMMIT"
)

type protectionTransitionPolicy struct {
	kind               protectionTransitionKind
	offlineAction      EntryProtectionRehearsalAction
	signedRecoveryOnly bool
	needsCancelProof   bool
	reason             string
}

// classifyProtectionTransition does not inspect the exchange; the CALLER must
// obtain fresh signed evidence with ReviewRecoveredEntryProtectionLifecycleSigned
// before invoking it. Unknown or internally contradictory states fail closed.
// The returned offlineAction may be used only by the sealed mock transport.
func classifyProtectionTransition(s EntryProtectionLifecycleReview) (protectionTransitionPolicy, error) {
	fail := func(reason string) (protectionTransitionPolicy, error) {
		return protectionTransitionPolicy{}, fmt.Errorf("protection transition not safe: %s", reason)
	}
	if s.TradingAuthorized || s.ProtectionSubmitAuthorized || s.CancelAuthorized ||
		s.RetryAuthorized || s.SettlementAuthorized {
		return fail("signed observation claimed an authorization")
	}
	// A DELETE marker cannot coexist with a first-time arm or a still-active
	// pair. Such a snapshot is stale or the local journals are inconsistent.
	if (s.State == EntryLifecyclePreparedOnly || s.State == EntryLifecyclePairActive ||
		s.State == EntryLifecycleUnknownSubmission) && (s.CancelIntentRecorded || s.CancelMayHaveBeenSent) {
		return fail("cancel journal contradicts protection state")
	}
	switch s.State {
	case EntryLifecyclePreparedOnly:
		return protectionTransitionPolicy{kind: protectionNeedArmReview, offlineAction: EntryRehearsalArm,
			reason: "mock first TP/SL attempt; product POST not approved"}, nil
	case EntryLifecycleUnknownSubmission:
		return protectionTransitionPolicy{kind: protectionRecoverPostOnly, offlineAction: EntryRehearsalArm,
			signedRecoveryOnly: true, reason: "ambiguous TP/SL POST: signed GET only; no replay"}, nil
	case EntryLifecyclePairActive:
		return protectionTransitionPolicy{kind: protectionMonitorPair, offlineAction: EntryRehearsalInspect,
			reason: "two active legs are not atomic OCO"}, nil
	case EntryLifecycleCancelReview:
		if s.CancelMayHaveBeenSent {
			return fail("DELETE already attempted but classified as unattempted")
		}
		return protectionTransitionPolicy{kind: protectionNeedCancelReview, offlineAction: EntryRehearsalCancel,
			needsCancelProof: true, reason: "mock sibling cancel requires independent signed ownership evidence"}, nil
	case EntryLifecycleCancelAmbiguous:
		if !s.CancelIntentRecorded || !s.CancelMayHaveBeenSent {
			return fail("ambiguous DELETE without a durable MAY_HAVE_SENT marker")
		}
		return protectionTransitionPolicy{kind: protectionRecoverDeleteOnly, offlineAction: EntryRehearsalCancel,
			signedRecoveryOnly: true, reason: "ambiguous DELETE: signed GET only; no replay"}, nil
	case EntryLifecycleClosedNotSettled:
		if s.CancelMayHaveBeenSent {
			if !s.CancelIntentRecorded {
				return fail("closed DELETE marker without cancel intent")
			}
			return protectionTransitionPolicy{kind: protectionRecoverDeleteOnly, offlineAction: EntryRehearsalCancel,
				signedRecoveryOnly: true, reason: "completed sibling DELETE: signed GET only"}, nil
		}
		return protectionTransitionPolicy{kind: protectionNeedSettlement, offlineAction: EntryRehearsalInspect,
			reason: "signed exit is not a financial COMMIT"}, nil
	case EntryLifecycleSingleLeg, EntryLifecyclePartialHalt,
		EntryLifecycleDoubleTriggerHalt, EntryLifecycleBlocked:
		return fail("protection incident state " + s.State)
	case "":
		return protectionTransitionPolicy{}, errors.New("protection transition missing signed state")
	default:
		return fail("unknown protection state " + s.State)
	}
}
