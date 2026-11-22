package trading

// This file is the OFFLINE transition driver for the already-sealed
// RehearseEntryProtectionDispatch harness. It does not extend product exchange
// permissions: the only transport accepted here implements the private
// localProtectionRehearsalOnly method and thus cannot be *exchange.Client.
// The production DemoProtectionRuntimeCoordinator continues to reject Arm and
// CancelTriggeredSibling with ErrProtectionApproval.

import (
	"context"
	"errors"
	"path/filepath"
)

const (
	OfflineSupervisorObserved      = "OBSERVED_NO_EXCHANGE_MUTATION"
	OfflineSupervisorSimulated     = "SIMULATED_MUTATION_RECHECK_REQUIRED"
	OfflineSupervisorSignedRecover = "SIGNED_ONLY_RECOVERY_NO_REPLAY"
	OfflineSupervisorHalted        = "INCIDENT_HALT_NO_REPLAY"
)

// OfflineProtectionSupervisorTick is ONE deterministic, restartable step. It
// first obtains a fresh signed lifecycle view and then dispatches exactly one
// state-dependent action via the existing mock-only one-shot engine. Each
// action independently re-reads signed evidence before any simulated write.
// A canceled/STOPped auto-run lease does not affect recovery inspections.
// Never loop over this function as if it were permission for unattended trading.
// The state is provisional until separately verified account-wide ownership
// and financial settlement; no result authorizes live trading.
type OfflineProtectionSupervisorTick struct {
	State                string                         `json:"state"`
	ChosenAction         EntryProtectionRehearsalAction `json:"chosen_action,omitempty"`
	Before               EntryProtectionLifecycleReview `json:"before"`
	Dispatch             EntryProtectionRehearsalResult `json:"dispatch"`
	Reason               string                         `json:"reason"`
	ManualReviewRequired bool                           `json:"manual_review_required"` // always true
	TradingAuthorized    bool                           `json:"trading_authorized"`     // always false
	SubmitAuthorized     bool                           `json:"submit_authorized"`      // always false
	CancelAuthorized     bool                           `json:"cancel_authorized"`      // always false
	SettlementAuthorized bool                           `json:"settlement_authorized"`  // always false
}

// nextOfflineSupervisorAction is deliberately private: a signed observation
// is not an operator authorization. The returned action is usable ONLY by
// RehearseEntryProtectionDispatch's sealed in-memory test transport.
func nextOfflineSupervisorAction(s EntryProtectionLifecycleReview, evidence *ProtectionEvidence) (EntryProtectionRehearsalAction, string, error) {
	policy, err := classifyProtectionTransition(s)
	if err != nil {
		return "", "signed state is unsafe: manual reconciliation required", err
	}
	if policy.needsCancelProof && (evidence == nil || !evidence.FullSignedOwnershipAudit) {
		// The absence of operator evidence MUST NOT create a DELETE candidate.
		return EntryRehearsalInspect, "independent signed ownership evidence required for any simulated DELETE", nil
	}
	return policy.offlineAction, policy.reason, nil
}

// rehearseEntryProtectionSupervisorCore makes at most ONE simulated action.
// Its only caller is the fenced offline entry point. ACK-loss recovery remains
// GET-only, and single-leg/double-trigger incidents remain halted.
func rehearseEntryProtectionSupervisorCore(ctx context.Context,
	r Reservation, coordinator *Coordinator, lifecycle *ExecutionLifecycleStore,
	protection *ProtectionSubmissionStore, cancels *CancelIntentStore,
	transport localOnlyProtectionRehearsalTransport, creds DemoCredentials,
	evidence *ProtectionEvidence,
) (OfflineProtectionSupervisorTick, error) {
	out := OfflineProtectionSupervisorTick{State: OfflineSupervisorHalted, ManualReviewRequired: true}
	if ctx == nil {
		return out, errors.New("offline protection supervisor nil context")
	}
	if transport == nil || coordinator == nil || lifecycle == nil || protection == nil || cancels == nil {
		return out, errors.New("offline protection supervisor missing sealed components")
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	before, err := ReviewRecoveredEntryProtectionLifecycleSigned(ctx, r, coordinator, lifecycle, protection, cancels, transport, creds)
	out.Before = before
	if err != nil {
		out.Reason = "initial signed review failed: no simulated mutation"
		return out, err
	}
	action, reason, err := nextOfflineSupervisorAction(before, evidence)
	out.Reason = reason
	if err != nil {
		return out, err
	}
	out.ChosenAction = action
	// The existing one-shot engine re-reads the state before any simulated
	// POST/DELETE and persists MAY_HAVE_SENT BEFORE touching the simulator.
	dispatch, err := RehearseEntryProtectionDispatch(ctx, action, r, coordinator, lifecycle, protection, cancels, transport, creds, evidence)
	out.Dispatch = dispatch
	if err != nil {
		out.State = OfflineSupervisorHalted
		out.Reason = reason + "; action failed or state changed: no automatic retry"
		return out, err
	}
	switch action {
	case EntryRehearsalInspect:
		out.State = OfflineSupervisorObserved
	case EntryRehearsalArm:
		if before.State == EntryLifecycleUnknownSubmission {
			out.State = OfflineSupervisorSignedRecover
		} else {
			out.State = OfflineSupervisorSimulated
		}
	case EntryRehearsalCancel:
		if before.CancelMayHaveBeenSent {
			out.State = OfflineSupervisorSignedRecover
		} else {
			out.State = OfflineSupervisorSimulated
		}
	default:
		return out, errors.New("unexpected offline transition")
	}
	return out, nil
}

// RehearseEntryProtectionSupervisorTick is the public OFFLINE entry point.
// It always constructs the crash-safe supervisor decision fence, rather than
// relying on the caller to remember a separate one-shot reservation. Real
// exchange transports remain excluded by the private local-only interface.
func RehearseEntryProtectionSupervisorTick(ctx context.Context,
	r Reservation, coordinator *Coordinator, lifecycle *ExecutionLifecycleStore,
	protection *ProtectionSubmissionStore, cancels *CancelIntentStore,
	transport localOnlyProtectionRehearsalTransport, creds DemoCredentials,
	evidence *ProtectionEvidence,
) (OfflineProtectionSupervisorTick, error) {
	if ctx == nil || coordinator == nil || lifecycle == nil || protection == nil || cancels == nil || transport == nil {
		return OfflineProtectionSupervisorTick{State: OfflineSupervisorHalted, ManualReviewRequired: true}, errors.New("offline supervisor dependencies absent")
	}
	if err := ctx.Err(); err != nil {
		return OfflineProtectionSupervisorTick{State: OfflineSupervisorHalted, ManualReviewRequired: true}, err
	}
	fences, err := OpenOfflineSupervisorFenceStore(filepath.Dir(coordinator.root))
	if err != nil {
		return OfflineProtectionSupervisorTick{State: OfflineSupervisorHalted, ManualReviewRequired: true}, err
	}
	return RehearseFencedProtectionSupervisorTick(ctx, r, coordinator, lifecycle, protection,
		cancels, fences, transport, creds, evidence)
}
