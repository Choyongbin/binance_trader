package trading

// Financial-commit preflight for an event-scoped, sealed NON-POSTED candidate.
//
// This is an intentionally non-authorizing boundary. Local SHA-256 checkpoints
// and archived signed responses do not prove a complete Binance account
// history, funding allocation, or current ownership. Do not turn this report
// into a posting API without independent signed attestation, operator approval,
// and an actual crash-recoverable atomic ledger implementation.
import (
	"context"
	"errors"
	"fmt"
)

const ExternalFinancialCommitBlocked = "FINANCIAL_COMMIT_BLOCKED_INDEPENDENT_EVIDENCE_REQUIRED"

// ExternalFinancialCommitPreflight binds the two *proposed* ledgers, scoped
// journal and outstanding financial prerequisites to one pinned candidate.
// All authority flags are deliberately false and are not caller-configurable.
// The returned digest protects against accidental changes, not against an
// adversary with filesystem write access or forged exchange proof.
type ExternalFinancialCommitPreflight struct {
	Schema                  int          `json:"schema"`
	Stage                   string       `json:"stage"`
	ClientOrderID           string       `json:"client_order_id"`
	ModelID                 string       `json:"model_id"`
	PositionSide            PositionSide `json:"position_side"`
	TransactionKey          string       `json:"transaction_key"`
	PinnedCommitSHA256      string       `json:"pinned_commit_sha256"`
	JournalFencedSHA256     string       `json:"journal_fenced_sha256"`
	EvidenceMatrixSHA256    string       `json:"evidence_matrix_sha256"`
	ContinuitySHA256        string       `json:"continuity_sha256"`
	LifecycleCandidate      string       `json:"lifecycle_candidate"`
	OwnershipCandidate      string       `json:"ownership_candidate"`
	OutstandingRequirements []string     `json:"outstanding_requirements"`
	PostingApplied          bool         `json:"posting_applied"`
	LifecycleChanged        bool         `json:"lifecycle_changed"`
	OwnershipChanged        bool         `json:"ownership_changed"`
	SettlementAuthorized    bool         `json:"settlement_authorized"`
	TradingAuthorized       bool         `json:"trading_authorized"`
	DigestSHA256            string       `json:"digest_sha256"`
}

func (r ExternalFinancialCommitPreflight) digest() string {
	r.DigestSHA256 = ""
	return externalCloseHash(r)
}

// InspectFinancialCommitPreflight has no filesystem writes, network access, or
// caller-supplied boolean claiming complete signed proof. It is usable only
// with a linked event-scoped store and the caller-pinned SEAL SHA-256.
// Malformed, missing, stale, or cross-event evidence is an error. A valid
// local candidate is a BLOCKED report, never permission to post a ledger.
func (s *ExternalAtomicRehearsalStore) InspectFinancialCommitPreflight(
	ctx context.Context, id string, archive PackageAEntryArchive, pinnedCommitSHA256 string,
) (ExternalFinancialCommitPreflight, error) {
	var empty ExternalFinancialCommitPreflight
	continuity, err := s.InspectScopedSettlementContinuity(ctx, id, archive, pinnedCommitSHA256)
	if err != nil {
		return empty, fmt.Errorf("commit preflight scoped continuity: %w", err)
	}
	matrix, err := s.journal.InspectEvidenceMatrix(ctx, id, archive)
	if err != nil {
		return empty, fmt.Errorf("commit preflight evidence matrix: %w", err)
	}
	if err := ValidateExternalSettlementEvidenceMatrix(matrix); err != nil {
		return empty, err
	}
	if continuity.Stage != ExternalScopedContinuityNotPosted ||
		continuity.DigestSHA256 != continuity.digest() ||
		continuity.ClientOrderID != id || matrix.ClientOrderID != id ||
		continuity.TransactionKey != matrix.TransactionKey ||
		continuity.EvidenceMatrixSHA256 != matrix.DigestSHA256 ||
		continuity.JournalFencedSHA256 != matrix.FencedChecksum ||
		continuity.PinnedCommitSHA256 != pinnedCommitSHA256 ||
		continuity.LedgerPair.CommitSHA256 != pinnedCommitSHA256 ||
		continuity.LedgerPair.Lifecycle.CommitSHA256 != pinnedCommitSHA256 ||
		continuity.LedgerPair.Ownership.CommitSHA256 != pinnedCommitSHA256 ||
		continuity.LedgerPair.Lifecycle.Applied || continuity.LedgerPair.Ownership.Applied ||
		continuity.PostingApplied || continuity.SettlementAuthorized || continuity.TradingAuthorized ||
		matrix.PostingAuthorized || matrix.SettlementAuthorized || matrix.TradingAuthorized ||
		matrix.LifecycleChanged || matrix.OwnershipChanged {
		return empty, errors.New("inconsistent or authorizing financial-commit preflight evidence")
	}
	// These are HARD prerequisites. This offline path has no independently
	// authenticated way to establish any of them. A new implementation must
	// introduce signed source validation, not merely flip these bits.
	required := [...]string{
		"FRESH_SIGNED_ACCOUNT_AND_ORDERS",
		"COMPLETE_RELEVANT_TRADE_HISTORY",
		"MODEL_FUNDING_AND_FEES",
		"EXTERNAL_EXIT_OWNERSHIP",
		"ATOMIC_CROSS_LEDGER_POSTING",
		"INDEPENDENT_SETTLEMENT_APPROVAL",
	}
	for _, code := range required {
		found := false
		for _, c := range matrix.Checks {
			if c.Code == code {
				if c.Evidenced || !includesJournalBlocker(matrix.BlockerCodes, code) ||
					!includesJournalBlocker(continuity.BlockerCodes, code) {
					return empty, fmt.Errorf("financial-commit prerequisite cannot be waived: %s", code)
				}
				found = true
			}
		}
		if !found {
			return empty, fmt.Errorf("financial-commit prerequisite missing: %s", code)
		}
	}
	if !includesJournalBlocker(matrix.BlockerCodes, "HTTP_423_ORDER_LOCK_REQUIRED") ||
		!includesJournalBlocker(continuity.BlockerCodes, "HTTP_423_ORDER_LOCK_REQUIRED") ||
		continuity.LedgerPair.Lifecycle.Proposal == "" || continuity.LedgerPair.Ownership.Proposal == "" {
		return empty, errors.New("commit gate lacks mandatory lock or complete candidate")
	}
	// Recheck the original continuity again, after the matrix read. The gate
	// never trusts a single cached snapshot across separate store readers.
	fresh, err := s.InspectScopedSettlementContinuity(ctx, id, archive, pinnedCommitSHA256)
	if err != nil || fresh.DigestSHA256 != continuity.DigestSHA256 {
		return empty, errors.New("commit preflight source changed during inspection")
	}
	result := ExternalFinancialCommitPreflight{
		Schema: 1, Stage: ExternalFinancialCommitBlocked, ClientOrderID: id,
		ModelID: continuity.ModelID, PositionSide: continuity.PositionSide,
		TransactionKey: continuity.TransactionKey, PinnedCommitSHA256: pinnedCommitSHA256,
		JournalFencedSHA256:  continuity.JournalFencedSHA256,
		EvidenceMatrixSHA256: matrix.DigestSHA256, ContinuitySHA256: continuity.DigestSHA256,
		LifecycleCandidate:      continuity.LedgerPair.Lifecycle.Proposal,
		OwnershipCandidate:      continuity.LedgerPair.Ownership.Proposal,
		OutstandingRequirements: append([]string(nil), matrix.BlockerCodes...),
	}
	result.DigestSHA256 = result.digest()
	return result, nil
}
