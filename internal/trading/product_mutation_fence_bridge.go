package trading

// This bridge prepares a local MAY_HAVE_SENT fence for a FUTURE supervised
// Binance Demo TP/SL product transport. It does not perform or approve POST,
// DELETE, order placement, sibling cancellation or financial COMMIT. Nothing
// calls it from the HTTP/strategy execution path until independently reviewed.
// In particular, a SHA-256 evidence identifier is not signed ownership proof.
import (
	"context"
	"errors"
	"time"

	"binance_trader/internal/protectionfence"
)

// RecordProductProtectionMutationFence requires the existing coordinator
// lease and reopens the immutable saved plan. This only establishes a durable
// no-replay fence, NEVER permission to attempt the fenced mutation. A future
// orchestrator MUST first obtain independently verified signed evidence and
// account-wide ownership, and must treat every result as non-authorizing.
func (r *DemoProtectionRuntimeCoordinator) RecordProductProtectionMutationFence(
	ctx context.Context, plan ProtectionDispatchPlan, op protectionfence.Operation,
	targetClientAlgoID string, targetAlgoID int64, accountFingerprintSHA256, evidenceSHA256 string,
) (protectionfence.Fence, error) {
	blocked := protectionfence.Fence{}
	if r == nil || ctx == nil || r.coordinator == nil || r.submissions == nil {
		return blocked, ErrProtectionApproval
	}
	if err := ctx.Err(); err != nil {
		return blocked, err
	}
	if err := r.coordinator.ownsLock(); err != nil {
		return blocked, err
	}
	if err := plan.validate(); err != nil {
		return blocked, err
	}
	saved, err := r.submissions.read(plan.GroupID)
	if err != nil {
		return blocked, err
	}
	if saved.Digest != plan.Digest || saved.TP != plan.TP || saved.SL != plan.SL ||
		saved.Source != plan.Source || saved.ModelID != plan.ModelID || saved.PositionSide != plan.PositionSide {
		return blocked, errors.New("immutable protection plan and product ledger mismatch")
	}
	switch op {
	case protectionfence.SubmitTP:
		if targetClientAlgoID != plan.TP.ClientAlgoID || targetAlgoID != 0 {
			return blocked, errors.New("TP mutation target disagrees with immutable plan")
		}
	case protectionfence.SubmitSL:
		if targetClientAlgoID != plan.SL.ClientAlgoID || targetAlgoID != 0 {
			return blocked, errors.New("SL mutation target disagrees with immutable plan")
		}
	case protectionfence.CancelSibling:
		if targetAlgoID <= 0 || (targetClientAlgoID != plan.TP.ClientAlgoID && targetClientAlgoID != plan.SL.ClientAlgoID) {
			return blocked, errors.New("sibling cancel target disagrees with immutable plan")
		}
	default:
		return blocked, errors.New("unknown protection mutation")
	}
	intent := protectionfence.Intent{
		Environment: "demo", GroupID: plan.GroupID, Operation: op,
		Symbol: plan.TP.Symbol, PositionSide: string(plan.PositionSide),
		QuantityBTC:        plan.TP.Quantity,
		TargetClientAlgoID: targetClientAlgoID, TargetAlgoID: targetAlgoID,
		PlanSHA256: plan.Digest, AccountFingerprintSHA256: accountFingerprintSHA256,
		EvidenceSHA256: evidenceSHA256,
	}
	store, err := protectionfence.Open(r.dataDir)
	if err != nil {
		return blocked, err
	}
	return store.SealBeforePossibleSend(intent, time.Now().UTC())
}
