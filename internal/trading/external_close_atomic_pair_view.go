package trading

// Read-only, token-pinned cross-ledger candidate view. This is NOT a posting
// protocol and MUST NOT be used as a replacement for the real lifecycle,
// ownership, or accounting journals. The seal's checksum is merely a common
// consistency boundary for two *proposed* changes, never trade authorization.
import (
	"context"
	"errors"
)

const ExternalAtomicPairViewNotPosted = "ATOMIC_PAIR_VIEW_SEALED_NOT_POSTED"

type ExternalAtomicLedgerPart struct {
	Schema              int    `json:"schema"`
	Kind                string `json:"kind"`
	CommitSHA256        string `json:"commit_sha256"`
	TransactionKey      string `json:"transaction_key"`
	LocalEvidenceSHA256 string `json:"local_evidence_sha256"`
	Proposal            string `json:"proposal"`
	ExitBTC             string `json:"projected_exit_btc,omitempty"`
	RemainingBTC        string `json:"projected_remaining_btc,omitempty"`
	Applied             bool   `json:"applied"`
}

// The pair is returned only after every upstream checksum and the final seal
// pass revalidation. All monetary and lifecycle fields are NON-POSTED previews.
// No individual part can be read without the exact same caller-pinned seal.
type ExternalAtomicLedgerPair struct {
	Schema               int                      `json:"schema"`
	Stage                string                   `json:"stage"`
	ClientOrderID        string                   `json:"client_order_id"`
	ModelID              string                   `json:"model_id"`
	PositionSide         PositionSide             `json:"position_side"`
	CommitSHA256         string                   `json:"commit_sha256"`
	TransactionKey       string                   `json:"transaction_key"`
	LocalEvidenceSHA256  string                   `json:"local_evidence_sha256"`
	Lifecycle            ExternalAtomicLedgerPart `json:"lifecycle"`
	Ownership            ExternalAtomicLedgerPart `json:"ownership"`
	PostingApplied       bool                     `json:"posting_applied"`
	SettlementAuthorized bool                     `json:"settlement_authorized"`
	TradingAuthorized    bool                     `json:"trading_authorized"`
}

// ReadLedgerPair requires a seal checksum obtained from a prior successful
// Inspect call. It rejects a missing/stale token, partial checkpoints and any
// source drift rather than silently merging different source revisions.
func (s *ExternalAtomicRehearsalStore) ReadLedgerPair(ctx context.Context, id string, archive PackageAEntryArchive, pinnedCommitSHA256 string) (ExternalAtomicLedgerPair, error) {
	if s == nil || !externalReviewHash.MatchString(pinnedCommitSHA256) {
		return ExternalAtomicLedgerPair{}, errors.New("valid pinned rehearsal seal SHA-256 is required")
	}
	records, err := s.expected(ctx, id, archive)
	if err != nil {
		return ExternalAtomicLedgerPair{}, err
	}
	if records[3].Checksum != pinnedCommitSHA256 {
		return ExternalAtomicLedgerPair{}, errors.New("stale or foreign cross-ledger seal token")
	}
	state, index, err := s.inspectExpected(records)
	if err != nil {
		return ExternalAtomicLedgerPair{}, err
	}
	if index != len(records)-1 || state.Stage != ExternalAtomicRehearsalSealed || !state.AtomicProjectionVisible ||
		state.PostingApplied || state.LifecycleChanged || state.OwnershipChanged || state.SettlementAuthorized || state.TradingAuthorized ||
		state.LifecycleProposal != records[1].LifecycleProposal || state.OwnershipProposal != records[2].OwnershipProposal ||
		state.ProjectedExitBTC != records[2].ProjectedExitBTC || state.ProjectedRemainingBTC != records[2].ProjectedRemainingBTC {
		return ExternalAtomicLedgerPair{}, errors.New("cross-ledger candidate not fully sealed or unsafe")
	}
	// Re-read all source records after checking seal; the last boundary does not
	// authorize use of data modified during the inspection itself.
	fresh, err := s.expected(ctx, id, archive)
	if err != nil || fresh != records {
		return ExternalAtomicLedgerPair{}, errors.New("cross-ledger evidence changed during candidate read")
	}
	common := ExternalAtomicLedgerPart{Schema: 1, CommitSHA256: pinnedCommitSHA256,
		TransactionKey: records[0].TransactionKey, LocalEvidenceSHA256: records[0].LocalEvidenceSHA256}
	lifecycle := common
	lifecycle.Kind = "LIFECYCLE_PROPOSAL_NOT_POSTED"
	lifecycle.Proposal = records[1].LifecycleProposal
	ownership := common
	ownership.Kind = "OWNERSHIP_PROPOSAL_NOT_POSTED"
	ownership.Proposal = records[2].OwnershipProposal
	ownership.ExitBTC = records[2].ProjectedExitBTC
	ownership.RemainingBTC = records[2].ProjectedRemainingBTC
	if lifecycle.Proposal == "" || ownership.Proposal == "" || ownership.ExitBTC == "" || ownership.RemainingBTC == "" {
		return ExternalAtomicLedgerPair{}, errors.New("incomplete cross-ledger candidate")
	}
	return ExternalAtomicLedgerPair{Schema: 1, Stage: ExternalAtomicPairViewNotPosted,
		ClientOrderID: id, ModelID: records[0].ModelID, PositionSide: records[0].PositionSide,
		CommitSHA256: pinnedCommitSHA256, TransactionKey: records[0].TransactionKey,
		LocalEvidenceSHA256: records[0].LocalEvidenceSHA256,
		Lifecycle:           lifecycle, Ownership: ownership}, nil
}

// Both component readers revalidate the entire same-token pair; no component
// is exposed from a partly written or changed transaction.
func (s *ExternalAtomicRehearsalStore) ReadLifecycleCandidate(ctx context.Context, id string, archive PackageAEntryArchive, pinnedCommitSHA256 string) (ExternalAtomicLedgerPart, error) {
	pair, err := s.ReadLedgerPair(ctx, id, archive, pinnedCommitSHA256)
	if err != nil {
		return ExternalAtomicLedgerPart{}, err
	}
	return pair.Lifecycle, nil
}

func (s *ExternalAtomicRehearsalStore) ReadOwnershipCandidate(ctx context.Context, id string, archive PackageAEntryArchive, pinnedCommitSHA256 string) (ExternalAtomicLedgerPart, error) {
	pair, err := s.ReadLedgerPair(ctx, id, archive, pinnedCommitSHA256)
	if err != nil {
		return ExternalAtomicLedgerPart{}, err
	}
	return pair.Ownership, nil
}
