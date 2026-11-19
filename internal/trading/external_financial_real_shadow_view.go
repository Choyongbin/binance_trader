package trading

// Read-only comparison of the REAL, immutable execution/ownership evidence and
// the completely separate SHADOW publication. The shadow publish marker is NOT
// a live financial commit. No existing live accounting reader is redirected to
// the shadow store, and neither view authorizes settlement or orders.
//
// The real evidence is pinned before and after both component reads. This is a
// consistency check for an OFFLINE, stopped data directory; it is NOT a global
// transactional snapshot against concurrently running writers, nor does it
// authenticate Binance history or determine external-close ownership.
import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"path/filepath"
	"reflect"
)

const ExternalRealShadowSeparatedNotPosted = "REAL_LEDGER_SEPARATE_FROM_SHADOW_NOT_POSTED"
const ExternalRealLifecycleAbsent = "NO_LOCAL_LIFECYCLE_RECORD"

type ExternalRealLedgerObservation struct {
	Schema               int    `json:"schema"`
	ClientOrderID        string `json:"client_order_id"`
	EvidenceSHA256       string `json:"real_evidence_sha256"`
	JournalCount         int    `json:"real_journal_count"`
	StoredFillCount      int    `json:"real_stored_fill_count"`
	EntryFillCount       int    `json:"real_entry_fill_count"`
	EntryQuantityBTC     string `json:"real_entry_quantity_btc"`
	LifecycleAuditState  string `json:"real_lifecycle_audit_state"`
	LifecycleStage       string `json:"real_lifecycle_stage"`
	ManualReviewRequired bool   `json:"manual_review_required"`
	PostingApplied       bool   `json:"posting_applied"`
	SettlementAuthorized bool   `json:"settlement_authorized"`
	TradingAuthorized    bool   `json:"trading_authorized"`
}

type ExternalRealShadowSeparatedView struct {
	Schema               int                           `json:"schema"`
	Stage                string                        `json:"stage"`
	ClientOrderID        string                        `json:"client_order_id"`
	Real                 ExternalRealLedgerObservation `json:"real"`
	Shadow               ExternalAtomicLedgerPair      `json:"shadow_proposal_not_posted"`
	ShadowPublishSHA256  string                        `json:"shadow_publish_sha256"`
	PostingApplied       bool                          `json:"posting_applied"`
	SettlementAuthorized bool                          `json:"settlement_authorized"`
	TradingAuthorized    bool                          `json:"trading_authorized"`
}

// InspectRealLedgerObservation never invokes constructors that create trading
// folders. It uses the real request/fill parsers, the actual lifecycle auditor,
// and a four-directory source fingerprint to reject torn or changed evidence.
// It does not equate a missing local entry with a settled external close.
func InspectRealLedgerObservation(dataDir, clientOrderID string) (ExternalRealLedgerObservation, error) {
	var empty ExternalRealLedgerObservation
	if dataDir == "" || !externalReviewID.MatchString(clientOrderID) {
		return empty, errors.New("valid data directory and event ID required for real ledger observation")
	}
	before, err := snapshotExecutionEvidence(dataDir)
	if err != nil {
		return empty, fmt.Errorf("real evidence snapshot: %w", err)
	}
	owner := &OwnershipStore{
		journalDir: filepath.Join(dataDir, "trading", "requests"),
		fillDir:    filepath.Join(dataDir, "trading", "fills"),
	}
	journals, err := owner.ScanJournal()
	if err != nil {
		return empty, fmt.Errorf("real ownership journal: %w", err)
	}
	// Refuse to claim an entry belongs to an unknown or mismatched order.
	found := false
	for _, record := range journals {
		if record.Plan.ClientOrderID != clientOrderID {
			continue
		}
		if found || record.Intent.Action != Enter {
			return empty, errors.New("real client ID is ambiguous or is not an entry")
		}
		found = true
	}
	if !found {
		return empty, errors.New("real entry request is missing")
	}
	fills, err := owner.readFills()
	if err != nil {
		return empty, fmt.Errorf("real ownership fills: %w", err)
	}
	quantity := new(big.Rat)
	count := 0
	for _, fill := range fills {
		if fill.ClientOrderID != clientOrderID {
			continue
		}
		qty, err := positiveRat(fill.Trade.Quantity)
		if err != nil {
			return empty, fmt.Errorf("invalid real entry fill quantity: %w", err)
		}
		quantity.Add(quantity, qty)
		count++
	}
	lifecycleAudit, err := ReviewExecutionLifecycles(dataDir)
	if err != nil {
		return empty, fmt.Errorf("real lifecycle audit: %w", err)
	}
	stage := ExternalRealLifecycleAbsent
	for _, item := range lifecycleAudit.Items {
		if item.ClientOrderID == clientOrderID {
			stage = string(item.Stage)
			break
		}
	}
	after, err := snapshotExecutionEvidence(dataDir)
	if err != nil || before != after {
		return empty, errors.New("real execution/ownership evidence changed during observation")
	}
	return ExternalRealLedgerObservation{
		Schema: 1, ClientOrderID: clientOrderID, EvidenceSHA256: before,
		JournalCount: len(journals), StoredFillCount: len(fills), EntryFillCount: count,
		EntryQuantityBTC:    formatOwnershipDecimal(quantity),
		LifecycleAuditState: lifecycleAudit.State, LifecycleStage: stage,
		ManualReviewRequired: true,
	}, nil
}

// ReadSeparatedRealAndShadow requires two independently pinned tokens:
// the live four-directory fingerprint and the shadow source's SEAL. An empty,
// stale, foreign or altered token fails closed. Each layer is re-read before
// any result is returned; no SHADOW_PUBLISHED state is reinterpreted as real.
func (s *ExternalShadowCommitStore) ReadSeparatedRealAndShadow(ctx context.Context,
	archive PackageAEntryArchive, pinnedSealSHA256, pinnedRealSHA256 string,
) (ExternalRealShadowSeparatedView, error) {
	return s.readSeparatedRealAndShadow(ctx, archive, pinnedSealSHA256, pinnedRealSHA256, nil)
}

// readShadow is an internal seam for deterministic mutation-in-the-middle
// tests; public calls always use the production shadow reader.
func (s *ExternalShadowCommitStore) readSeparatedRealAndShadow(ctx context.Context,
	archive PackageAEntryArchive, pinnedSealSHA256, pinnedRealSHA256 string,
	readShadow func() (ExternalAtomicLedgerPair, error),
) (ExternalRealShadowSeparatedView, error) {
	var empty ExternalRealShadowSeparatedView
	if s == nil || !externalReviewHash.MatchString(pinnedSealSHA256) ||
		!externalReviewHash.MatchString(pinnedRealSHA256) || ctx == nil {
		return empty, errors.New("two pinned evidence digests and context are required")
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	realProjectionBefore, err := ReadExternalLedgerSnapshot(ctx, s.dataDir, s.id, ExternalLedgerSourceReal, ExternalLedgerReadPin{RealEvidenceSHA256: pinnedRealSHA256}, nil)
	if err != nil {
		return empty, err
	}
	baselineBefore := *realProjectionBefore.Real
	realBefore := baselineBefore.Ownership
	if err != nil {
		return empty, err
	}
	if realBefore.EvidenceSHA256 != pinnedRealSHA256 {
		return empty, errors.New("stale real ledger evidence pin")
	}
	publish, err := s.Inspect(ctx, archive, pinnedSealSHA256)
	if err != nil {
		return empty, err
	}
	if !publish.ShadowPublicationVisible || publish.Stage != ExternalShadowCommitPublished ||
		!externalReviewHash.MatchString(publish.ShadowPublishSHA256) ||
		publish.PostingApplied || publish.TradingAuthorized || publish.SettlementAuthorized {
		return empty, errors.New("unpublished or authorizing shadow publication")
	}
	if readShadow == nil {
		readShadow = func() (ExternalAtomicLedgerPair, error) {
			port, err := NewExternalShadowReadPort(s, archive)
			if err != nil {
				return ExternalAtomicLedgerPair{}, err
			}
			projection, err := port.ReadPinned(ctx, ExternalLedgerReadPin{
				ShadowSealSHA256: pinnedSealSHA256, ShadowPublicationSHA256: publish.ShadowPublishSHA256,
			})
			if err != nil {
				return ExternalAtomicLedgerPair{}, err
			}
			return *projection.Shadow, nil
		}
	}
	shadow, err := readShadow()
	if err != nil {
		return empty, err
	}
	if shadow.Stage != ExternalAtomicPairViewNotPosted || shadow.ClientOrderID != s.id ||
		shadow.CommitSHA256 != pinnedSealSHA256 || shadow.Lifecycle.CommitSHA256 != pinnedSealSHA256 ||
		shadow.Ownership.CommitSHA256 != pinnedSealSHA256 || shadow.PostingApplied ||
		shadow.Lifecycle.Applied || shadow.Ownership.Applied ||
		shadow.TradingAuthorized || shadow.SettlementAuthorized {
		return empty, errors.New("shadow publication is inconsistent or implies financial posting")
	}
	realProjectionAfter, err := ReadExternalLedgerSnapshot(ctx, s.dataDir, s.id, ExternalLedgerSourceReal, ExternalLedgerReadPin{RealEvidenceSHA256: pinnedRealSHA256}, nil)
	if err != nil {
		return empty, errors.New("real ledger changed while comparing against shadow")
	}
	baselineAfter := *realProjectionAfter.Real
	realAfter := baselineAfter.Ownership
	if err != nil || realBefore != realAfter || baselineBefore.EvidenceSHA256 != baselineAfter.EvidenceSHA256 || !equalPinnedLifecycleAudit(baselineBefore.Lifecycle, baselineAfter.Lifecycle) {
		return empty, errors.New("real ledger changed while comparing against shadow")
	}
	publishAgain, err := s.Inspect(ctx, archive, pinnedSealSHA256)
	if err != nil || publish != publishAgain {
		return empty, errors.New("shadow publication changed while comparing against real ledger")
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	return ExternalRealShadowSeparatedView{
		Schema: 1, Stage: ExternalRealShadowSeparatedNotPosted, ClientOrderID: s.id,
		Real: realBefore, Shadow: shadow, ShadowPublishSHA256: publish.ShadowPublishSHA256,
	}, nil
}

// The lifecycle audit includes slices; compare every item, not only counters.
func equalPinnedLifecycleAudit(a, b ExecutionLifecycleAudit) bool { return reflect.DeepEqual(a, b) }
