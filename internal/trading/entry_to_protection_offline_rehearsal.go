package trading

// An end-to-end, test-simulator-only assembly of the already implemented
// signed entry recovery and TP/SL lifecycle. The transport is SEALED by an
// unexported method in localOnlyProtectionRehearsalTransport: the real Binance
// client cannot implement it, and no product server, scheduler or live auto
// runtime invokes this function. No execution/settlement approval is created.
//
// An existing durable protection plan is NEVER re-prepared after an exit: a
// changed/flat account at that point must be handled through signed lifecycle
// observation, not through a fictitious new entry/TP/SL pair.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"binance_trader/internal/exchange"
)

const (
	OfflineEntryLifecyclePending    = "ENTRY_NOT_TERMINAL_NO_PROTECTION"
	OfflineEntryLifecycleUnprepared = "ENTRY_UNPREPARED_READ_ONLY_REVIEW"
	OfflineEntryLifecycleBlocked    = "OFFLINE_LIFECYCLE_BLOCKED"
)

type EntryToProtectionOfflineResult struct {
	GroupID              string                          `json:"group_id"`
	Action               EntryProtectionRehearsalAction  `json:"action"`
	State                string                          `json:"state"`
	PreparedThisCall     bool                            `json:"prepared_this_call"`
	Preparation          *RecoveredEntryProtectionReview `json:"preparation,omitempty"`
	Dispatch             *EntryProtectionRehearsalResult `json:"dispatch,omitempty"`
	Reason               string                          `json:"reason"`
	TradingAuthorized    bool                            `json:"trading_authorized"`     // always false
	ProtectionAuthorized bool                            `json:"protection_authorized"`  // always false
	SettlementAuthorized bool                            `json:"settlement_authorized"`  // always false
	ManualReviewRequired bool                            `json:"manual_review_required"` // always true
}

// RehearseRecoveredEntryToProtectionOffline is a product-shaped but STRICTLY
// offline assembly. A genuine exchange.Client cannot satisfy transport.
//
// For a new entry, the only possible local writes are signed fill recovery and
// durable PREPARED plan; its simulator-only action can then exercise the
// existing arming/cancellation state machine. For an already prepared entry,
// no new plan is made and the signed lifecycle re-check alone selects the
// appropriate restart/incident behavior. Cancellation never prepares a plan.
//
// Both the entry reader and simulator transport are expected to represent
// the SAME fake exchange snapshot; production admission remains forbidden.
func RehearseRecoveredEntryToProtectionOffline(
	ctx context.Context, action EntryProtectionRehearsalAction,
	r Reservation, coordinator *Coordinator, lifecycle *ExecutionLifecycleStore,
	ownership *OwnershipStore, protection *ProtectionSubmissionStore,
	cancels *CancelIntentStore, entryReader DemoTradeReader,
	transport localOnlyProtectionRehearsalTransport, creds DemoCredentials,
	rules exchange.SymbolRules, contract, mark Quote,
	evidence *ProtectionEvidence,
) (EntryToProtectionOfflineResult, error) {
	out := EntryToProtectionOfflineResult{
		GroupID: entryProtectionGroupID(r), Action: action,
		State: OfflineEntryLifecycleBlocked, ManualReviewRequired: true,
	}
	fail := func(reason string, err error) (EntryToProtectionOfflineResult, error) {
		out.State, out.Reason = OfflineEntryLifecycleBlocked, reason
		return out, err
	}
	if ctx == nil {
		return fail("nil context", errors.New("offline lifecycle context missing"))
	}
	if err := ctx.Err(); err != nil {
		return fail("context canceled", err)
	}
	if action != EntryRehearsalInspect && action != EntryRehearsalArm && action != EntryRehearsalCancel {
		return fail("unsupported action", errors.New("invalid offline lifecycle action"))
	}
	if coordinator == nil || lifecycle == nil || ownership == nil || protection == nil || cancels == nil || entryReader == nil || transport == nil {
		return fail("missing ledger or simulator", errors.New("offline lifecycle components missing"))
	}
	if creds.APIKey == "" || creds.Secret == "" || r.Intent.Action != Enter || r.Intent.Symbol != "BTCUSDT" {
		return fail("invalid signed entry", errors.New("offline lifecycle invalid entry/credentials"))
	}
	if filepath.Clean(coordinator.root) != filepath.Clean(lifecycle.root) ||
		filepath.Clean(ownership.journalDir) != filepath.Join(coordinator.root, "requests") ||
		filepath.Clean(ownership.fillDir) != filepath.Join(coordinator.root, "fills") ||
		filepath.Clean(protection.dir) != filepath.Join(coordinator.root, "protection_submissions") ||
		filepath.Clean(cancels.dir) != filepath.Join(coordinator.root, "algo_cancel_intents") {
		return fail("cross-directory evidence", errors.New("offline lifecycle mixed ledger roots"))
	}
	if err := coordinator.ownsLock(); err != nil {
		return fail("coordinator lock missing", err)
	}

	// A corrupt, tampered or partially recorded plan is never interpreted as
	// "not yet prepared". os.IsNotExist is the ONLY preparation trigger.
	_, err := protection.read(out.GroupID)
	if errors.Is(err, os.ErrNotExist) {
		// INSPECT_ONLY is an actual read-only operation, including before
		// the first plan exists. Do not recover fills or create a plan here:
		// those operations persist local ledgers and must require a distinct
		// explicit offline ARM action. No state/position is inferred from a
		// missing plan and no network mutation is attempted.
		if action == EntryRehearsalInspect {
			out.State = OfflineEntryLifecycleUnprepared
			out.Reason = "보호 계획 없음: signed 진입 복구/계획 저장을 수행하지 않은 읽기 전용 관측; 운영자 검토 필요"
			return out, nil
		}
		if action == EntryRehearsalCancel {
			return fail("cancel before protection", errors.New("cannot cancel an unprepared entry"))
		}
		prepared, prepareErr := RecoverAndPrepareEntryProtectionSupervised(
			ctx, r, coordinator, lifecycle, ownership, protection,
			entryReader, transport, creds, rules, contract, mark)
		out.Preparation = &prepared
		if prepareErr != nil {
			return fail("signed entry recovery or protection preparation failed", prepareErr)
		}
		if prepared.State == EntryProtectionRecoveryPending {
			out.State = OfflineEntryLifecyclePending
			out.Reason = prepared.PendingReason
			return out, nil
		}
		if prepared.State != EntryProtectionPreparedReview || prepared.Plan.GroupID != out.GroupID {
			return fail("unexpected entry preparation state", fmt.Errorf("state=%s", prepared.State))
		}
		out.PreparedThisCall = true
	} else if err != nil {
		return fail("tampered protection plan", err)
	}

	// The existing signed reader and checkpoint machine is the ONLY route
	// forward. In particular, it never falls back to re-POSTing a leg after
	// MAY_HAVE_SENT, and never assumes a canceled sibling is settled.
	dispatched, dispatchErr := RehearseEntryProtectionDispatch(ctx, action, r,
		coordinator, lifecycle, protection, cancels, transport, creds, evidence)
	out.Dispatch = &dispatched
	if dispatchErr != nil {
		return fail("protection simulator incident or signed verification failed", dispatchErr)
	}
	out.State = dispatched.State
	out.Reason = dispatched.Reason
	return out, nil
}
