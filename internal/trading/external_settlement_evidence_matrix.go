package trading

// SettlementEvidenceMatrix exposes the actual locally verified evidence and
// missing authorization prerequisites without accepting caller-supplied proof
// booleans. It does not contact Binance, create a journal or post settlement.
import (
	"context"
	"errors"
	"fmt"
)

const ExternalSettlementMatrixBlocked = "EXTERNAL_CLOSE_SETTLEMENT_EVIDENCE_INCOMPLETE_NOT_POSTED"

type SettlementEvidenceCheck struct {
	Code      string `json:"code"`
	Evidenced bool   `json:"evidenced"`
	Detail    string `json:"detail"`
}

type ExternalSettlementEvidenceMatrix struct {
	Schema               int                       `json:"schema"`
	Stage                string                    `json:"stage"`
	ClientOrderID        string                    `json:"client_order_id"`
	TransactionKey       string                    `json:"transaction_key"`
	JournalStage         string                    `json:"journal_stage"`
	ProposalSHA256       string                    `json:"proposal_sha256"`
	PreparedChecksum     string                    `json:"prepared_checksum,omitempty"`
	FencedChecksum       string                    `json:"fenced_checksum,omitempty"`
	EntryTradeIDs        []int64                   `json:"entry_trade_ids"`
	ExitTradeIDs         []int64                   `json:"exit_trade_ids"`
	MissingEntryTradeIDs []int64                   `json:"missing_entry_trade_ids"`
	Checks               []SettlementEvidenceCheck `json:"checks"`
	BlockerCodes         []string                  `json:"blocker_codes"`
	PostingAuthorized    bool                      `json:"posting_authorized"`
	SettlementAuthorized bool                      `json:"settlement_authorized"`
	TradingAuthorized    bool                      `json:"trading_authorized"`
	LifecycleChanged     bool                      `json:"lifecycle_changed"`
	OwnershipChanged     bool                      `json:"ownership_changed"`
	DigestSHA256         string                    `json:"digest_sha256"`
}

func (m ExternalSettlementEvidenceMatrix) digest() string {
	m.DigestSHA256 = ""
	return externalCloseHash(m)
}

// InspectEvidenceMatrix rechecks upstream proposal, posting gate and entire
// dual-ledger/recovery journal before producing a non-authorizing report.
// Missing or inconsistent local files fail closed; a complete local chain
// never stands in for fresh signed exchange evidence or independent approval.
func (s *ExternalSettlementJournalStore) InspectEvidenceMatrix(ctx context.Context, id string, archive PackageAEntryArchive) (ExternalSettlementEvidenceMatrix, error) {
	if s == nil || s.dual == nil {
		return ExternalSettlementEvidenceMatrix{}, errors.New("nil external settlement journal")
	}
	proposal, err := s.dual.shadow.intents.replay.proposal.Load(id)
	if err != nil {
		return ExternalSettlementEvidenceMatrix{}, fmt.Errorf("settlement proposal: %w", err)
	}
	gate, err := PreviewExternalClosePosting(proposal)
	if err != nil {
		return ExternalSettlementEvidenceMatrix{}, err
	}
	if err := ValidateExternalClosePostingGate(gate); err != nil {
		return ExternalSettlementEvidenceMatrix{}, err
	}
	inspection, err := s.InspectRecovery(ctx, id, archive)
	if err != nil {
		return ExternalSettlementEvidenceMatrix{}, fmt.Errorf("settlement restart inspection: %w", err)
	}
	if inspection.ClientOrderID != proposal.ClientOrderID ||
		len(inspection.EntryTradeIDs) != len(proposal.EntryTradeIDs) ||
		len(inspection.ExitTradeIDs) != len(proposal.ExitTradeIDs) {
		return ExternalSettlementEvidenceMatrix{}, errors.New("settlement proposal and recovery trade identities disagree")
	}
	for i, v := range inspection.EntryTradeIDs {
		if v != proposal.EntryTradeIDs[i] {
			return ExternalSettlementEvidenceMatrix{}, errors.New("entry trade identity mismatch")
		}
	}
	for i, v := range inspection.ExitTradeIDs {
		if v != proposal.ExitTradeIDs[i] {
			return ExternalSettlementEvidenceMatrix{}, errors.New("exit trade identity mismatch")
		}
	}
	checks := []SettlementEvidenceCheck{
		{"LOCAL_BOUNDED_PROPOSAL", true, "Locally sealed review and proposal revalidated; not Binance-signed archival proof"},
		{"LOCAL_RESTART_JOURNAL", true, "Local restart boundary inspected (including checkpoint absence); never a posted settlement"},
		{"ENTRY_FILLS_IMPORTED", len(inspection.MissingEntryTradeIDs) == 0, "Only archived entry fills checked against local immutable journal"},
		{"FRESH_SIGNED_ACCOUNT_AND_ORDERS", false, "No live independently refreshed signed account/order/algo snapshot in this offline inspection"},
		{"COMPLETE_RELEVANT_TRADE_HISTORY", false, "Bounded history cannot establish full related account history"},
		{"MODEL_FUNDING_AND_FEES", false, "Independent per-model funding, fee asset and net profit attribution absent"},
		{"EXTERNAL_EXIT_OWNERSHIP", false, "Operator review-only acknowledgment does not prove pooled-side ownership"},
		{"ATOMIC_CROSS_LEDGER_POSTING", false, "No authorized crash-recoverable cross-ledger financial posting implementation"},
		{"INDEPENDENT_SETTLEMENT_APPROVAL", false, "No separate authenticated operator approval for financial settlement"},
	}
	m := ExternalSettlementEvidenceMatrix{Schema: 1, Stage: ExternalSettlementMatrixBlocked,
		ClientOrderID: id, TransactionKey: inspection.TransactionKey, JournalStage: inspection.Stage,
		ProposalSHA256: proposal.Checksum, PreparedChecksum: inspection.PreparedChecksum,
		FencedChecksum:       inspection.FencedChecksum,
		EntryTradeIDs:        append([]int64{}, inspection.EntryTradeIDs...),
		ExitTradeIDs:         append([]int64{}, inspection.ExitTradeIDs...),
		MissingEntryTradeIDs: append([]int64{}, inspection.MissingEntryTradeIDs...),
		Checks:               checks, BlockerCodes: append([]string{}, inspection.BlockerCodes...),
	}
	for _, c := range checks {
		if !c.Evidenced {
			m.BlockerCodes = append(m.BlockerCodes, c.Code)
		}
	}
	m.DigestSHA256 = m.digest()
	return m, ValidateExternalSettlementEvidenceMatrix(m)
}

func ValidateExternalSettlementEvidenceMatrix(m ExternalSettlementEvidenceMatrix) error {
	if m.Schema != 1 || m.Stage != ExternalSettlementMatrixBlocked || !externalReviewID.MatchString(m.ClientOrderID) ||
		!externalReviewHash.MatchString(m.TransactionKey) || !externalReviewHash.MatchString(m.ProposalSHA256) ||
		len(m.EntryTradeIDs) == 0 || len(m.ExitTradeIDs) == 0 || len(m.Checks) != 9 || len(m.BlockerCodes) < 6 ||
		m.PostingAuthorized || m.SettlementAuthorized || m.TradingAuthorized || m.LifecycleChanged || m.OwnershipChanged ||
		m.DigestSHA256 != m.digest() {
		return errors.New("invalid or authorizing settlement evidence matrix")
	}
	if m.JournalStage != ExternalRecoveryPrerequisitesOnly && m.JournalStage != ExternalRecoveryPreparedOnly && m.JournalStage != ExternalRecoveryFencedOnly {
		return errors.New("invalid settlement restart stage")
	}
	if (m.JournalStage == ExternalRecoveryPrerequisitesOnly && (m.PreparedChecksum != "" || m.FencedChecksum != "")) ||
		(m.JournalStage == ExternalRecoveryPreparedOnly && (!externalReviewHash.MatchString(m.PreparedChecksum) || m.FencedChecksum != "")) ||
		(m.JournalStage == ExternalRecoveryFencedOnly && (!externalReviewHash.MatchString(m.PreparedChecksum) || !externalReviewHash.MatchString(m.FencedChecksum))) {
		return errors.New("invalid restart checkpoint linkage")
	}
	codes := []string{"LOCAL_BOUNDED_PROPOSAL", "LOCAL_RESTART_JOURNAL", "ENTRY_FILLS_IMPORTED", "FRESH_SIGNED_ACCOUNT_AND_ORDERS", "COMPLETE_RELEVANT_TRADE_HISTORY", "MODEL_FUNDING_AND_FEES", "EXTERNAL_EXIT_OWNERSHIP", "ATOMIC_CROSS_LEDGER_POSTING", "INDEPENDENT_SETTLEMENT_APPROVAL"}
	for i, c := range m.Checks {
		if c.Code != codes[i] || c.Detail == "" || (i != 2 && c.Evidenced != (i < 2)) || (i == 2 && c.Evidenced != (len(m.MissingEntryTradeIDs) == 0)) {
			return errors.New("settlement evidence checks invalid or forged")
		}
		if !c.Evidenced && !includesJournalBlocker(m.BlockerCodes, c.Code) {
			return errors.New("missing settlement blocker")
		}
	}
	if !includesJournalBlocker(m.BlockerCodes, "HTTP_423_ORDER_LOCK_REQUIRED") {
		return errors.New("HTTP order lock blocker missing")
	}
	return nil
}
