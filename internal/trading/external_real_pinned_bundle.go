package trading

// An opt-in, OFFLINE-only consistency boundary for the *existing* real
// execution evidence. Ownership and lifecycle readers must observe exactly
// the same caller-pinned source digest. This is NOT a financial COMMIT: the
// existing writers do not use a transactional generation/lock, and the digest
// covers only the four execution directories, not Binance history, funding,
// account PnL, or other accounting stores. Stop writers and use a data copy.
import (
	"context"
	"errors"
	"fmt"
)

const ExternalRealPinnedReadOnly = "REAL_EXECUTION_PINNED_READ_ONLY_NOT_SETTLED"

// ExternalRealPinnedBundle holds two independent real-ledger projections of
// the same observed filesystem version. Neither component is a permission to
// settle or trade, even if the lifecycle audit happens to say ALL_SETTLED.
type ExternalRealPinnedBundle struct {
	Schema               int                           `json:"schema"`
	Stage                string                        `json:"stage"`
	ClientOrderID        string                        `json:"client_order_id"`
	EvidenceSHA256       string                        `json:"real_evidence_sha256"`
	Ownership            ExternalRealLedgerObservation `json:"ownership_observation"`
	Lifecycle            ExecutionLifecycleAudit       `json:"lifecycle_audit"`
	ManualReviewRequired bool                          `json:"manual_review_required"`
	PostingApplied       bool                          `json:"posting_applied"`
	SettlementAuthorized bool                          `json:"settlement_authorized"`
	TradingAuthorized    bool                          `json:"trading_authorized"`
}

// ReadPinnedRealLedgerBundle is a non-mutating opt-in real reader. The caller
// MUST pin a baseline first (InspectRealLedgerObservation); there is no
// implicit refresh on source drift. Repeated reads can use the same pin and
// will fail closed after a change. No directory is created.
func ReadPinnedRealLedgerBundle(ctx context.Context, dataDir, clientOrderID, pinnedSHA256 string) (ExternalRealPinnedBundle, error) {
	return readPinnedRealLedgerBundle(ctx, dataDir, clientOrderID, pinnedSHA256, nil)
}

// betweenReads is a test seam used only to deterministically trigger an
// intervening filesystem mutation. Public callers cannot bypass validation.
func readPinnedRealLedgerBundle(ctx context.Context, dataDir, clientOrderID, pinnedSHA256 string, betweenReads func() error) (ExternalRealPinnedBundle, error) {
	var empty ExternalRealPinnedBundle
	if ctx == nil || dataDir == "" || !externalReviewID.MatchString(clientOrderID) || !externalReviewHash.MatchString(pinnedSHA256) {
		return empty, errors.New("valid context, data path, client ID and pinned execution digest required")
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	before, err := snapshotExecutionEvidence(dataDir)
	if err != nil {
		return empty, fmt.Errorf("pinned real snapshot: %w", err)
	}
	if before != pinnedSHA256 {
		return empty, errors.New("stale or foreign real execution version")
	}

	owner, err := InspectRealLedgerObservation(dataDir, clientOrderID)
	if err != nil {
		return empty, fmt.Errorf("pinned ownership observation: %w", err)
	}
	if owner.EvidenceSHA256 != pinnedSHA256 {
		return empty, errors.New("ownership observation has a different real execution version")
	}
	if betweenReads != nil {
		if err := betweenReads(); err != nil {
			return empty, err
		}
	}
	audit, err := ReviewExecutionLifecycles(dataDir)
	if err != nil {
		return empty, fmt.Errorf("pinned lifecycle audit: %w", err)
	}
	if audit.TradingAuthorized || !audit.ManualReviewRequired || audit.Total != len(audit.Items) {
		return empty, errors.New("real lifecycle audit is not a non-authorizing complete view")
	}
	stage := ExternalRealLifecycleAbsent
	for _, item := range audit.Items {
		if item.ClientOrderID == clientOrderID {
			if stage != ExternalRealLifecycleAbsent {
				return empty, errors.New("duplicate real lifecycle client ID")
			}
			stage = string(item.Stage)
		}
	}
	if audit.State != owner.LifecycleAuditState || stage != owner.LifecycleStage ||
		owner.PostingApplied || owner.SettlementAuthorized || owner.TradingAuthorized || !owner.ManualReviewRequired {
		return empty, errors.New("ownership/lifecycle projections disagree or claim settlement authority")
	}
	after, err := snapshotExecutionEvidence(dataDir)
	if err != nil || after != pinnedSHA256 {
		return empty, errors.New("real execution version changed during pinned read")
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	return ExternalRealPinnedBundle{
		Schema: 1, Stage: ExternalRealPinnedReadOnly, ClientOrderID: clientOrderID,
		EvidenceSHA256: pinnedSHA256, Ownership: owner, Lifecycle: audit,
		ManualReviewRequired: true,
	}, nil
}
