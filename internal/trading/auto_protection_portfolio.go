package trading

import (
	"context"
	"errors"
	"fmt"
)

// PendingProtectionInventory is a bounded, read-only inventory of local durable
// entry reservations. It is NOT an account-wide Binance reconciliation, an
// always-on watcher, an OCO guarantee or a trading permission. A new signed
// review must be requested for every observation. No result enables POST or
// DELETE, including when every individual entry has two active legs.
//
// A stopped auto lease does not prevent reading existing exposure. An unknown
// local order, a manual/shared-side mutation or a signed GET error fails the
// entire inventory closed rather than silently skipping the suspect item.
type PendingProtectionInventory struct {
	State                string                           `json:"state"`
	Entries              []EntryProtectionLifecycleReview `json:"entries"`
	Steps                []ProductProtectionStep          `json:"next_steps"`
	InspectedEntries     int                              `json:"inspected_entries"`
	IgnoredSettled       int                              `json:"ignored_settled"`
	Blockers             []string                         `json:"blockers"`
	Incidents            []ProtectionIncident             `json:"incidents"`
	ManualReviewRequired bool                             `json:"manual_review_required"`
	TradingAuthorized    bool                             `json:"trading_authorized"`    // always false
	SubmitAuthorized     bool                             `json:"submit_authorized"`     // always false
	CancelAuthorized     bool                             `json:"cancel_authorized"`     // always false
	SettlementAuthorized bool                             `json:"settlement_authorized"` // always false
	ContinuousMonitoring bool                             `json:"continuous_monitoring"` // always false
}

const (
	PendingProtectionInspectionBlocked  = "PORTFOLIO_REVIEW_BLOCKED"
	PendingProtectionInspectionObserved = "LOCAL_ENTRY_REVIEWS_OBSERVED_NOT_APPROVED"
	PendingProtectionInspectionEmpty    = "NO_PENDING_LOCAL_ENTRIES_ACCOUNT_UNVERIFIED"
	maxProtectionInventory              = 128
)

// ProtectionIncident is a non-authorizing, signed-observation diagnostic.
// Incidents describe an unsafe or unresolved protection state; they are never
// permission to retry POST, issue DELETE, close a pooled position, or settle.
// No credentials or signed account payloads are included in this report.
type ProtectionIncident struct {
	ClientOrderID        string `json:"client_order_id,omitempty"`
	GroupID              string `json:"group_id,omitempty"`
	LifecycleState       string `json:"lifecycle_state,omitempty"`
	Code                 string `json:"code"`
	Reason               string `json:"reason"`
	ManualReviewRequired bool   `json:"manual_review_required"`
}

// protectionIncidentForReview prevents the portfolio watcher from treating an
// unprotected entry, a pending sibling DELETE, a partial exit or an unsettled
// close as a healthy "observed" position. Only a strictly observed ACTIVE pair
// has no immediate incident; even that is not OCO or trading authorization.
func protectionIncidentForReview(review EntryProtectionLifecycleReview) (ProtectionIncident, bool) {
	code := ""
	switch review.State {
	case EntryLifecyclePairActive:
		return ProtectionIncident{}, false
	case EntryLifecyclePreparedOnly:
		code = "ENTRY_TP_SL_NOT_SUBMITTED"
	case EntryLifecycleUnknownSubmission:
		code = "PROTECTION_POST_MAY_HAVE_SENT"
	case EntryLifecycleSingleLeg:
		code = "SINGLE_PROTECTION_LEG"
	case EntryLifecycleCancelReview:
		code = "TRIGGERED_SIBLING_STILL_ACTIVE"
	case EntryLifecycleCancelAmbiguous:
		code = "SIBLING_DELETE_MAY_HAVE_SENT"
	case EntryLifecycleClosedNotSettled:
		code = "CLOSED_FINANCIAL_SETTLEMENT_PENDING"
	case EntryLifecyclePartialHalt:
		code = "PARTIAL_EXIT_UNRECONCILED"
	case EntryLifecycleDoubleTriggerHalt:
		code = "TP_SL_DOUBLE_TRIGGER"
	case EntryLifecycleBlocked:
		code = "SIGNED_PROTECTION_REVIEW_BLOCKED"
	default:
		code = "UNKNOWN_PROTECTION_LIFECYCLE_STATE"
	}
	return ProtectionIncident{
		ClientOrderID: review.ClientOrderID, GroupID: review.GroupID,
		LifecycleState: review.State, Code: code, Reason: review.Reason,
		ManualReviewRequired: true,
	}, true
}

// InspectOutstandingProtections lists all locally unfinished reservations,
// including manual and EXIT entries. It only GETs signed account/order/fill
// evidence for model ENTRY reservations. Other outstanding reservations block
// the aggregate, because pooled Binance Hedge-side quantities may have been
// changed outside this model's ownership scope.
//
// The lock serializes only the local directory scan. Signed queries across
// entries are NOT an atomic account snapshot; the report remains non-authorizing.
func (r *DemoProtectionRuntimeCoordinator) InspectOutstandingProtections(ctx context.Context) (PendingProtectionInventory, error) {
	out := PendingProtectionInventory{
		State:   PendingProtectionInspectionBlocked,
		Entries: []EntryProtectionLifecycleReview{}, Steps: []ProductProtectionStep{},
		Blockers: []string{}, Incidents: []ProtectionIncident{},
		ManualReviewRequired: true,
	}
	fail := func(reason string, err error) (PendingProtectionInventory, error) {
		out.State = PendingProtectionInspectionBlocked
		out.Blockers = append(out.Blockers, reason)
		return out, err
	}
	if ctx == nil {
		return fail("nil context", errors.New("protection inventory requires context"))
	}
	if err := ctx.Err(); err != nil {
		return fail("context ended", err)
	}
	if r == nil || r.coordinator == nil || r.lifecycle == nil || r.submissions == nil || r.cancels == nil || r.reader == nil || r.credentials == nil {
		return fail("missing product protection components", errors.New("protection inventory not configured"))
	}
	if err := r.coordinator.ownsLock(); err != nil {
		return fail("coordinator lock not owned", err)
	}
	r.coordinator.mu.Lock()
	reservations, err := r.coordinator.scan()
	r.coordinator.mu.Unlock()
	if err != nil {
		return fail("durable reservation scan failed", err)
	}
	// A restart may leave a protection plan or DELETE intent even when its
	// reservation is missing. Cross-check ALL local artifacts before treating
	// an apparently empty or fully active inventory as coherent.
	artifacts, artifactErr := r.auditLocalProtectionArtifacts(ctx, reservations)
	out.Incidents = append(out.Incidents, artifacts...)
	for _, item := range artifacts {
		out.Blockers = append(out.Blockers, fmt.Sprintf("%s: %s", item.GroupID, item.Code))
	}
	if artifactErr != nil {
		return fail("protection/cancel journal scan is incomplete or corrupt", artifactErr)
	}
	// Bound the number of signed reviews, not historical settled records:
	// long-running installations may have many already-settled entries.
	pendingCount := 0
	for _, entry := range reservations {
		if err := ctx.Err(); err != nil {
			return fail("context ended while scanning reservations", err)
		}
		if r.coordinator.isDone(entry) {
			life, loadErr := r.lifecycle.Load(entry)
			if loadErr == nil && life.Stage == ExecutionSettled {
				out.IgnoredSettled++
				continue
			}
			out.Blockers = append(out.Blockers, fmt.Sprintf("%s: done marker without verified settled lifecycle", entry.ClientOrderID))
			out.Incidents = append(out.Incidents, ProtectionIncident{
				ClientOrderID: entry.ClientOrderID, Code: "INVALID_LOCAL_SETTLEMENT_MARKER", ManualReviewRequired: true,
			})
			continue
		}
		pendingCount++
		if pendingCount > maxProtectionInventory {
			return fail("too many unfinished reservations for bounded review", errors.New("pending protection review limit exceeded"))
		}
		if entry.Intent.Source != Model || entry.Intent.Action != Enter {
			out.Blockers = append(out.Blockers, fmt.Sprintf("%s: outstanding non-model-entry reservation", entry.ClientOrderID))
			out.Incidents = append(out.Incidents, ProtectionIncident{
				ClientOrderID: entry.ClientOrderID, Code: "OUTSTANDING_UNOWNED_OR_EXIT_ORDER", ManualReviewRequired: true,
			})
			continue
		}
		review, inspectErr := r.InspectEntryProtection(ctx, entry)
		out.Entries = append(out.Entries, review)
		out.InspectedEntries++
		// Derive the product control-plane step from the SAME signed review.
		// Do not perform a second GET whose result may contradict this pass.
		step, stepErr := classifyProductProtectionStep(review)
		out.Steps = append(out.Steps, step)
		// Known incidents already have a precise, stable code below. Do not
		// duplicate them with a generic transition incident.
		_, knownIncident := protectionIncidentForReview(review)
		if stepErr != nil && inspectErr == nil && !knownIncident {
			out.Blockers = append(out.Blockers, fmt.Sprintf("%s: unsafe product transition", entry.ClientOrderID))
			out.Incidents = append(out.Incidents, ProtectionIncident{
				ClientOrderID: entry.ClientOrderID, GroupID: review.GroupID,
				LifecycleState: review.State, Code: "UNSAFE_PRODUCT_PROTECTION_TRANSITION",
				ManualReviewRequired: true,
			})
		}
		if incident, unsafe := protectionIncidentForReview(review); unsafe {
			out.Incidents = append(out.Incidents, incident)
			out.Blockers = append(out.Blockers, fmt.Sprintf("%s: %s (%s)", entry.ClientOrderID, incident.Code, review.State))
		}
		if inspectErr != nil && review.State != EntryLifecycleBlocked {
			out.Blockers = append(out.Blockers, fmt.Sprintf("%s: signed lifecycle review failed: %v", entry.ClientOrderID, inspectErr))
		}
		// An individual inspector that errors despite claiming an active pair is
		// also unsafe; never let the caller treat a partial account scan as clear.
		if inspectErr != nil && review.State == EntryLifecyclePairActive {
			out.Incidents = append(out.Incidents, ProtectionIncident{ClientOrderID: entry.ClientOrderID,
				GroupID: review.GroupID, LifecycleState: review.State,
				Code: "INCONSISTENT_SIGNED_REVIEW_ERROR", ManualReviewRequired: true})
		}
		// Even observed ACTIVE legs are two independent Binance Algo orders,
		// not an atomic OCO pair and never automatic-trading approval.
	}
	if len(out.Blockers) > 0 {
		return out, fmt.Errorf("protection portfolio has %d unresolved reservation(s)", len(out.Blockers))
	}
	if out.InspectedEntries == 0 {
		out.State = PendingProtectionInspectionEmpty
	} else {
		out.State = PendingProtectionInspectionObserved
	}
	return out, nil
}

// The single- and multi-model runtimes use one shared protection coordinator;
// these methods deliberately remain callable after STOP (lease revoked).
func (r *AutoExecutionRuntime) InspectOutstandingProtections(ctx context.Context) (PendingProtectionInventory, error) {
	if r == nil || r.protection == nil {
		return PendingProtectionInventory{State: PendingProtectionInspectionBlocked, ManualReviewRequired: true,
			Blockers: []string{"single-model inspector unavailable"}}, errors.New("product protection inspector unavailable")
	}
	return r.protection.InspectOutstandingProtections(ctx)
}

func (r *MultiModelAutoRuntime) InspectOutstandingProtections(ctx context.Context) (PendingProtectionInventory, error) {
	if r == nil || r.protection == nil {
		return PendingProtectionInventory{State: PendingProtectionInspectionBlocked, ManualReviewRequired: true,
			Blockers: []string{"multi-model inspector unavailable"}}, errors.New("product protection inspector unavailable")
	}
	return r.protection.InspectOutstandingProtections(ctx)
}
