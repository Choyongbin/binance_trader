package trading

// This offline inspection is NOT a posting protocol. It revalidates the
// immutable cross-ledger dependencies and reports the precise restart boundary.
// In particular a FENCED record does not mean a fill, ownership, or financial
// ledger has been posted. Never interpret this report as trading permission.
import (
	"context"
	"errors"
	"os"
)

const (
	ExternalRecoveryPrerequisitesOnly = "PREREQUISITES_VERIFIED_JOURNAL_ABSENT_NOT_POSTED"
	ExternalRecoveryPreparedOnly      = "PREPARED_NOT_POSTED_RESTART_INSPECTION"
	ExternalRecoveryFencedOnly        = "FENCED_NOT_POSTED_RESTART_INSPECTION"
)

type ExternalSettlementRecoveryInspection struct {
	Schema               int      `json:"schema"`
	Stage                string   `json:"stage"`
	ClientOrderID        string   `json:"client_order_id"`
	TransactionKey       string   `json:"transaction_key"`
	PreparedChecksum     string   `json:"prepared_checksum,omitempty"`
	FencedChecksum       string   `json:"fenced_checksum,omitempty"`
	EntryTradeIDs        []int64  `json:"entry_trade_ids"`
	ExitTradeIDs         []int64  `json:"exit_trade_ids"`
	MissingEntryTradeIDs []int64  `json:"missing_entry_trade_ids"`
	BlockerCodes         []string `json:"blocker_codes"`
	NextStep             string   `json:"next_step"`
	PostingAuthorized    bool     `json:"posting_authorized"`
	SettlementAuthorized bool     `json:"settlement_authorized"`
	TradingAuthorized    bool     `json:"trading_authorized"`
	LifecycleChanged     bool     `json:"lifecycle_changed"`
	OwnershipChanged     bool     `json:"ownership_changed"`
}

// InspectRecovery does not create directories or call Binance. If the journal
// is incomplete, unexpectedly populated, tampered, or its upstream evidence
// drifts, it returns an error rather than proposing recovery by deletion.
func (s *ExternalSettlementJournalStore) InspectRecovery(ctx context.Context, id string, archive PackageAEntryArchive) (ExternalSettlementRecoveryInspection, error) {
	if s == nil {
		return ExternalSettlementRecoveryInspection{}, errors.New("nil settlement journal")
	}
	expected, err := s.expected(ctx, id, archive)
	if err != nil {
		return ExternalSettlementRecoveryInspection{}, err
	}
	r := ExternalSettlementRecoveryInspection{
		Schema: 1, Stage: ExternalRecoveryPrerequisitesOnly,
		ClientOrderID: expected.ClientOrderID, TransactionKey: expected.TransactionKey,
		EntryTradeIDs:        append([]int64(nil), expected.EntryTradeIDs...),
		ExitTradeIDs:         append([]int64(nil), expected.ExitTradeIDs...),
		MissingEntryTradeIDs: append([]int64(nil), expected.MissingEntryTradeIDs...),
		BlockerCodes:         append([]string(nil), expected.BlockerCodes...),
		NextStep:             "Independent signed history, funding allocation and operator-authorized atomic posting remain required; do not remove original journals.",
	}
	if err := s.checkDir(false); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return r, nil
		}
		return ExternalSettlementRecoveryInspection{}, err
	}
	root, fenced, err := s.Load(ctx, id, archive)
	if err != nil {
		return ExternalSettlementRecoveryInspection{}, err
	}
	r.PreparedChecksum = root.Checksum
	r.Stage = ExternalRecoveryPreparedOnly
	if fenced != nil {
		r.Stage = ExternalRecoveryFencedOnly
		r.FencedChecksum = fenced.Checksum
	}
	return r, nil
}
