package trading

import (
	"context"
	"errors"
)

// InspectEntryProtection is the model runtime's operator-facing recovery
// observation. Unlike RunOnce it does not depend on a running auto lease: STOP
// must only halt NEW entries, not hide an already-open position. Inspection
// makes no POST/DELETE and cannot authorize trading or financial settlement.
// It is not a periodic autonomous supervisor and is not exposed over HTTP.
func (r *AutoExecutionRuntime) InspectEntryProtection(ctx context.Context, entry Reservation) (EntryProtectionLifecycleReview, error) {
	if r == nil || r.protection == nil {
		return blockedRuntimeProtectionReview(entry), errors.New("single-model product protection inspector is unavailable")
	}
	return r.protection.InspectEntryProtection(ctx, entry)
}

// InspectEntryProtection uses the SAME persisted coordinator/ownership scope
// as every selected model, so an isolated per-model inspector cannot silently
// claim ownership of a pooled BTCUSDT Hedge-side position.
func (r *MultiModelAutoRuntime) InspectEntryProtection(ctx context.Context, entry Reservation) (EntryProtectionLifecycleReview, error) {
	if r == nil || r.protection == nil {
		return blockedRuntimeProtectionReview(entry), errors.New("multi-model product protection inspector is unavailable")
	}
	return r.protection.InspectEntryProtection(ctx, entry)
}

func blockedRuntimeProtectionReview(entry Reservation) EntryProtectionLifecycleReview {
	return EntryProtectionLifecycleReview{
		State: EntryLifecycleBlocked, ClientOrderID: entry.ClientOrderID,
		ManualReviewRequired: true, Reason: "operator signed review only; runtime unavailable",
	}
}
