package trading

// Cross-store, event-scoped read-only consistency check. This is an inspection
// of immutable NON-POSTED candidates, NOT a financial commit or Binance proof.
// It intentionally rejects all legacy constructors: a legacy single-event
// journal must never be silently paired with a new per-event atomic seal.
import (
	"context"
	"errors"
	"fmt"
)

const ExternalScopedContinuityNotPosted = "SCOPED_SETTLEMENT_CONTINUITY_VERIFIED_NOT_POSTED"

// ExternalScopedSettlementContinuity only includes checksummed, locally staged
// projections. No property here establishes complete account history, funding,
// ownership attribution, or operator authorization to settle or trade.
type ExternalScopedSettlementContinuity struct {
	Schema                int                      `json:"schema"`
	Stage                 string                   `json:"stage"`
	ClientOrderID         string                   `json:"client_order_id"`
	TransactionKey        string                   `json:"transaction_key"`
	ModelID               string                   `json:"model_id"`
	PositionSide          PositionSide             `json:"position_side"`
	JournalPreparedSHA256 string                   `json:"journal_prepared_sha256"`
	JournalFencedSHA256   string                   `json:"journal_fenced_sha256"`
	EvidenceMatrixSHA256  string                   `json:"evidence_matrix_sha256"`
	LocalEvidenceSHA256   string                   `json:"local_evidence_sha256"`
	PinnedCommitSHA256    string                   `json:"pinned_commit_sha256"`
	LedgerPair            ExternalAtomicLedgerPair `json:"ledger_pair"`
	BlockerCodes          []string                 `json:"blocker_codes"`
	PostingApplied        bool                     `json:"posting_applied"`
	SettlementAuthorized  bool                     `json:"settlement_authorized"`
	TradingAuthorized     bool                     `json:"trading_authorized"`
	DigestSHA256          string                   `json:"digest_sha256"`
}

func (r ExternalScopedSettlementContinuity) digest() string {
	r.DigestSHA256 = ""
	return externalCloseHash(r)
}

// InspectScopedSettlementContinuity independently revalidates scoped journal
// stages, the final atomic seal, the common transaction key, source hashes,
// and the two proposed ledger components against an explicitly pinned seal.
// If anything changes between reads, it fails closed without writing files.
func (s *ExternalAtomicRehearsalStore) InspectScopedSettlementContinuity(
	ctx context.Context, id string, archive PackageAEntryArchive, pinnedCommitSHA256 string,
) (ExternalScopedSettlementContinuity, error) {
	var empty ExternalScopedSettlementContinuity
	if s == nil || s.journal == nil || s.scopedClientID != id ||
		s.journal.scopedClientID != id || !externalReviewID.MatchString(id) ||
		!externalReviewHash.MatchString(pinnedCommitSHA256) {
		return empty, errors.New("a matching event-scoped atomic store, event journal and pinned seal are required")
	}
	matrix, err := s.journal.InspectEvidenceMatrix(ctx, id, archive)
	if err != nil {
		return empty, fmt.Errorf("scoped evidence matrix: %w", err)
	}
	if matrix.JournalStage != ExternalRecoveryFencedOnly || matrix.PostingAuthorized ||
		matrix.SettlementAuthorized || matrix.TradingAuthorized || matrix.LifecycleChanged || matrix.OwnershipChanged {
		return empty, errors.New("scoped journal has not reached a safe non-posted FENCED boundary")
	}
	prepared, fenced, err := s.journal.Load(ctx, id, archive)
	if err != nil || fenced == nil {
		return empty, errors.New("event journal PREPARED/FENCED checkpoints missing or invalid")
	}
	records, err := s.expected(ctx, id, archive)
	if err != nil {
		return empty, fmt.Errorf("linked atomic evidence: %w", err)
	}
	if records[0].FencedJournalSHA256 != fenced.Checksum ||
		records[0].DualLedgerSHA256 != fenced.DualLedgerVerifiedSHA256 ||
		records[0].LocalEvidenceSHA256 != fenced.LocalEvidenceSHA256 ||
		matrix.PreparedChecksum != prepared.Checksum || matrix.FencedChecksum != fenced.Checksum ||
		matrix.TransactionKey != fenced.TransactionKey ||
		records[0].TransactionKey != fenced.TransactionKey ||
		records[0].ClientOrderID != id || records[0].ModelID != fenced.ModelID ||
		records[0].PositionSide != fenced.PositionSide {
		return empty, errors.New("event journal and atomic candidate disagree on incident, owner or immutable evidence")
	}
	pair, err := s.ReadLedgerPair(ctx, id, archive, pinnedCommitSHA256)
	if err != nil {
		return empty, fmt.Errorf("event atomic pair: %w", err)
	}
	if pair.CommitSHA256 != records[3].Checksum || pair.TransactionKey != fenced.TransactionKey ||
		pair.LocalEvidenceSHA256 != fenced.LocalEvidenceSHA256 ||
		pair.ClientOrderID != id || pair.ModelID != fenced.ModelID || pair.PositionSide != fenced.PositionSide ||
		pair.Lifecycle.CommitSHA256 != pair.CommitSHA256 || pair.Ownership.CommitSHA256 != pair.CommitSHA256 ||
		pair.Lifecycle.TransactionKey != pair.TransactionKey || pair.Ownership.TransactionKey != pair.TransactionKey ||
		pair.PostingApplied || pair.SettlementAuthorized || pair.TradingAuthorized ||
		pair.Lifecycle.Applied || pair.Ownership.Applied {
		return empty, errors.New("two ledger candidates do not share the pinned non-posting commit")
	}
	// Detect source drift across the composite read; both stores independently
	// verify the original files, while this boundary also pins their join.
	again, err := s.journal.InspectEvidenceMatrix(ctx, id, archive)
	if err != nil || again.DigestSHA256 != matrix.DigestSHA256 ||
		again.FencedChecksum != matrix.FencedChecksum || again.PreparedChecksum != matrix.PreparedChecksum {
		return empty, errors.New("settlement journal evidence changed during scoped continuity inspection")
	}
	fresh, err := s.expected(ctx, id, archive)
	if err != nil || fresh != records {
		return empty, errors.New("atomic source changed during scoped continuity inspection")
	}
	result := ExternalScopedSettlementContinuity{
		Schema: 1, Stage: ExternalScopedContinuityNotPosted,
		ClientOrderID: id, TransactionKey: pair.TransactionKey,
		ModelID: pair.ModelID, PositionSide: pair.PositionSide,
		JournalPreparedSHA256: prepared.Checksum, JournalFencedSHA256: fenced.Checksum,
		EvidenceMatrixSHA256: matrix.DigestSHA256,
		LocalEvidenceSHA256:  pair.LocalEvidenceSHA256,
		PinnedCommitSHA256:   pinnedCommitSHA256, LedgerPair: pair,
		BlockerCodes: append([]string(nil), matrix.BlockerCodes...),
	}
	result.DigestSHA256 = result.digest()
	return result, nil
}
