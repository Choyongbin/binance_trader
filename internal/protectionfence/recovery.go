package protectionfence

// Read-only reconciliation of an immutable MAY_HAVE_SENT fence against two
// sequential authenticated observations. This package has NO exchange mutation
// interface, and a matching observation is NOT a no-replay fence release,
// ownership proof, OCO guarantee, or financial settlement authority.
//
// The caller implementing SignedAlgoReader is responsible for using fresh
// authenticated Binance Demo REST GETs. These local structures are not
// cryptographic signatures; tests and forged callers can populate them. They
// MUST NEVER be treated as an authorization token by any trading code.
import (
	"context"
	"errors"
	"fmt"
	"time"
)

type RecoveryState string

const (
	RecoveryBlocked            RecoveryState = "BLOCKED_MANUAL_REVIEW"
	RecoveryAbsentUnproven     RecoveryState = "NOT_FOUND_NOT_PROOF_NO_REPLAY"
	RecoverySubmitObserved     RecoveryState = "SUBMIT_OBSERVED_ONE_LEG_NOT_OCO"
	RecoveryCancelStillActive  RecoveryState = "CANCEL_TARGET_STILL_ACTIVE_NO_RETRY"
	RecoveryCancelObserved     RecoveryState = "CANCEL_OBSERVED_NOT_SETTLED"
	RecoveryTerminalNeedsAudit RecoveryState = "TERMINAL_ALGO_REQUIRES_SIGNED_FILL_AND_ACCOUNT_AUDIT"
)

// SignedAlgoObservation represents one result of the product's authenticated
// query. Found=false must still be authenticated as an attempted bounded
// lookup; absence within a bounded query is NEVER proof of non-submission.
type SignedAlgoObservation struct {
	ObservedAt               time.Time
	AccountFingerprintSHA256 string
	Found                    bool
	ClientAlgoID             string
	AlgoID                   int64
	Symbol                   string
	PositionSide             string
	QuantityBTC              string
	Status                   string
}

type SignedAlgoReader interface {
	ReadSignedAlgo(context.Context, Intent) (SignedAlgoObservation, error)
}

type RecoveryReview struct {
	State                RecoveryState `json:"state"`
	GroupID              string        `json:"group_id"`
	Operation            Operation     `json:"operation"`
	TargetClientAlgoID   string        `json:"target_client_algo_id"`
	FirstObservedAt      time.Time     `json:"first_observed_at,omitempty"`
	SecondObservedAt     time.Time     `json:"second_observed_at,omitempty"`
	Reasons              []string      `json:"reasons"`
	ManualReviewRequired bool          `json:"manual_review_required"`
	TradingAuthorized    bool          `json:"trading_authorized"`
	SubmitAuthorized     bool          `json:"submit_authorized"`
	CancelAuthorized     bool          `json:"cancel_authorized"`
	RetryAuthorized      bool          `json:"retry_authorized"`
	SettlementAuthorized bool          `json:"settlement_authorized"`
}

func recoveryBlocked(reason string) RecoveryReview {
	return RecoveryReview{State: RecoveryBlocked, Reasons: []string{reason}, ManualReviewRequired: true}
}

func validateSignedObservation(intent Intent, o SignedAlgoObservation, now time.Time) error {
	if o.AccountFingerprintSHA256 != intent.AccountFingerprintSHA256 ||
		o.ObservedAt.IsZero() || o.ObservedAt.After(now.Add(time.Second)) ||
		now.Sub(o.ObservedAt) > 5*time.Second {
		return errors.New("account identity or signed observation freshness invalid")
	}
	if !o.Found {
		if o.ClientAlgoID != "" || o.AlgoID != 0 || o.Symbol != "" ||
			o.PositionSide != "" || o.QuantityBTC != "" || o.Status != "" {
			return errors.New("absent observation contains conflicting data")
		}
		return nil
	}
	if o.ClientAlgoID != intent.TargetClientAlgoID || o.AlgoID <= 0 ||
		o.Symbol != intent.Symbol || o.PositionSide != intent.PositionSide ||
		!positiveDecimal.MatchString(o.QuantityBTC) ||
		// Decimal formatting may differ between persisted plans and API JSON.
		// String equality is deliberately stricter than numeric equality here;
		// do not silently accept a changed or rounded quantity.
		o.QuantityBTC != intent.QuantityBTC ||
		(intent.Operation == CancelSibling && o.AlgoID != intent.TargetAlgoID) {
		return errors.New("signed Algo identity, quantity or cancellation target mismatch")
	}
	switch o.Status {
	case "NEW", "WORKING", "TRIGGERING", "TRIGGERED", "FINISHED", "CANCELED", "CANCELLED", "EXPIRED", "REJECTED":
		return nil
	default:
		return errors.New("unknown signed Algo status")
	}
}

func observationsEqual(a, b SignedAlgoObservation) bool {
	return a.AccountFingerprintSHA256 == b.AccountFingerprintSHA256 &&
		a.Found == b.Found && a.ClientAlgoID == b.ClientAlgoID &&
		a.AlgoID == b.AlgoID && a.Symbol == b.Symbol &&
		a.PositionSide == b.PositionSide && a.QuantityBTC == b.QuantityBTC &&
		a.Status == b.Status
}

// InspectRecoveryOnce reads only. It never clears, renames or deletes the
// original fence, and never calls POST/DELETE. After two observations it scans
// the immutable fence again to detect concurrent corruption or replacement.
// A positive observation only describes the exact target at these moments;
// account-wide ownership, partial fills, pooled Hedge exposure and an OCO
// cancellation guarantee remain unproven in ALL resulting states.
func (s *Store) InspectRecoveryOnce(ctx context.Context, reader SignedAlgoReader) (RecoveryReview, error) {
	out := recoveryBlocked("read-only signed recovery has not been verified")
	if ctx == nil || s == nil || reader == nil {
		return out, errors.New("signed recovery dependencies missing")
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	fences, err := s.InspectAll()
	if err != nil {
		return out, fmt.Errorf("pre-read fence integrity: %w", err)
	}
	if len(fences) != 1 {
		return out, errors.New("exactly one immutable unresolved mutation is required")
	}
	fence := fences[0]
	out.GroupID, out.Operation, out.TargetClientAlgoID = fence.Intent.GroupID, fence.Intent.Operation, fence.Intent.TargetClientAlgoID
	first, err := reader.ReadSignedAlgo(ctx, fence.Intent)
	if err != nil {
		return out, fmt.Errorf("first signed GET: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	if err := validateSignedObservation(fence.Intent, first, time.Now().UTC()); err != nil {
		return out, fmt.Errorf("first signed GET: %w", err)
	}
	second, err := reader.ReadSignedAlgo(ctx, fence.Intent)
	if err != nil {
		return out, fmt.Errorf("second signed GET: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	now := time.Now().UTC()
	if err := validateSignedObservation(fence.Intent, second, now); err != nil {
		return out, fmt.Errorf("second signed GET: %w", err)
	}
	// Both observations must still be fresh at the time of the final
	// decision, not merely when each independent GET returned.
	if err := validateSignedObservation(fence.Intent, first, now); err != nil {
		return out, fmt.Errorf("first signed GET expired while rechecking: %w", err)
	}
	if second.ObservedAt.Before(first.ObservedAt) || !observationsEqual(first, second) {
		return out, errors.New("independent signed observations disagree or regress")
	}
	after, err := s.InspectAll()
	if err != nil || len(after) != 1 || after[0] != fence {
		return out, errors.New("fence changed, corrupt or missing during signed read")
	}
	out.FirstObservedAt, out.SecondObservedAt = first.ObservedAt, second.ObservedAt
	// Every branch below is descriptive only; never a release or a ticket.
	switch {
	case !second.Found:
		out.State = RecoveryAbsentUnproven
		out.Reasons = []string{"bounded signed lookup did not find target; request may have been sent or expired: NO retry"}
	case second.Status == "NEW" || second.Status == "WORKING":
		if fence.Intent.Operation == CancelSibling {
			out.State = RecoveryCancelStillActive
			out.Reasons = []string{"cancel target remains active; DELETE outcome uncertain, NO retry"}
		} else {
			out.State = RecoverySubmitObserved
			out.Reasons = []string{"one protection leg observed; sibling, OCO and full account safety unproven"}
		}
	case second.Status == "CANCELED" || second.Status == "CANCELLED":
		if fence.Intent.Operation == CancelSibling {
			out.State = RecoveryCancelObserved
			out.Reasons = []string{"cancel status observed twice; fills, remaining position and financial settlement unverified"}
		} else {
			out.State = RecoveryTerminalNeedsAudit
			out.Reasons = []string{"submitted leg now canceled; independent exit/owner audit required"}
		}
	default:
		out.State = RecoveryTerminalNeedsAudit
		out.Reasons = []string{"conditional order triggering, executed, rejected or expired; signed fills and account-wide position audit required"}
	}
	return out, nil
}
