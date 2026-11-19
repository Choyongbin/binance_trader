package trading

// Package A phase 1: deterministic, strictly offline PREPARATION of owned entry
// fill recovery. No method here imports or writes a fill, advances a lifecycle,
// or treats previously exported signed GET JSON as fresh exchange evidence.
// The bounded archive remains a LOCAL copy, not an exchange signature.
import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"

	"binance_trader/internal/exchange"
)

const PackageAEntryRecoveryPlanStage = "ENTRY_FILL_RECOVERY_PREVIEW_NOT_APPLIED"

type PackageAEntryRecoveryCandidate struct {
	TradeID         int64  `json:"trade_id"`
	JournalBasename string `json:"proposed_journal_basename"`
	// The exact copied remote trade is needed for later independent matching;
	// never replay this into the original ledger from an archive alone.
	ArchivedSignedTrade      exchange.AccountTrade `json:"archived_signed_trade"`
	ProposedStoredFillSHA256 string                `json:"proposed_stored_fill_sha256"`
}

type PackageAEntryRecoveryPlan struct {
	Schema                     int                              `json:"schema"`
	Stage                      string                           `json:"stage"`
	ClientOrderID              string                           `json:"client_order_id"`
	ModelID                    string                           `json:"model_id"`
	PositionSide               PositionSide                     `json:"position_side"`
	OrderID                    int64                            `json:"entry_order_id"`
	ArchiveSHA256              string                           `json:"source_archive_sha256"`
	SignedEvidenceSHA256       string                           `json:"archived_signed_evidence_sha256"`
	LocalEvidenceSHA256        string                           `json:"original_local_evidence_sha256"`
	LifecycleStage             string                           `json:"local_lifecycle_stage"`
	AlreadyImportedIDs         []int64                          `json:"already_imported_ids"`
	Candidates                 []PackageAEntryRecoveryCandidate `json:"missing_fill_candidates"`
	BlockerCodes               []string                         `json:"blocker_codes"`
	FullHistoryVerified        bool                             `json:"full_history_verified"`
	FundingAttributionVerified bool                             `json:"funding_attribution_verified"`
	RecoveryApplied            bool                             `json:"recovery_applied"`
	LifecycleChanged           bool                             `json:"lifecycle_changed"`
	OwnershipChanged           bool                             `json:"ownership_changed"`
	SettlementAuthorized       bool                             `json:"settlement_authorized"`
	TradingAuthorized          bool                             `json:"trading_authorized"`
	Checksum                   string                           `json:"local_sha256_checksum"`
}

func (p PackageAEntryRecoveryPlan) digest() string {
	p.Checksum = ""
	return externalCloseHash(p)
}

// PlanPackageAEntryRecovery requires the original immutable entry request and
// exact signed-order/fill archive to match the CURRENT local originals, before
// and after preparing the manifest. A stale plan is never independently valid.
func PlanPackageAEntryRecovery(ctx context.Context, dataDir string, archive PackageAEntryArchive) (PackageAEntryRecoveryPlan, error) {
	if err := ctx.Err(); err != nil {
		return PackageAEntryRecoveryPlan{}, err
	}
	if err := VerifyPackageAEntryArchive(ctx, dataDir, archive); err != nil {
		return PackageAEntryRecoveryPlan{}, fmt.Errorf("entry archive preflight: %w", err)
	}
	before, err := snapshotExecutionEvidence(dataDir)
	if err != nil {
		return PackageAEntryRecoveryPlan{}, err
	}
	if archive.Gap.OriginalEvidenceSHA256 != before {
		return PackageAEntryRecoveryPlan{}, errors.New("original entry evidence has drifted")
	}
	audit, err := ReviewExecutionLifecycles(dataDir)
	if err != nil {
		return PackageAEntryRecoveryPlan{}, err
	}
	if audit.TradingAuthorized || audit.State == ExecutionAuditBlocked {
		return PackageAEntryRecoveryPlan{}, errors.New("unsafe or inconsistent lifecycle audit")
	}
	p := PackageAEntryRecoveryPlan{
		Schema: 1, Stage: PackageAEntryRecoveryPlanStage, ClientOrderID: archive.Gap.ClientOrderID,
		ModelID: archive.Gap.ModelID, PositionSide: archive.Gap.PositionSide, OrderID: archive.Order.OrderID,
		ArchiveSHA256: archive.Checksum, SignedEvidenceSHA256: archive.Gap.SignedEvidenceSHA256,
		LocalEvidenceSHA256: before, LifecycleStage: "MISSING_LIFECYCLE_BLOCKED",
		AlreadyImportedIDs: append([]int64{}, archive.Gap.AlreadyImportedIDs...),
		Candidates:         []PackageAEntryRecoveryCandidate{},
		BlockerCodes:       []string{"FRESH_SIGNED_RECHECK_REQUIRED", "FULL_ACCOUNT_HISTORY_UNVERIFIED", "MODEL_FUNDING_ATTRIBUTION_UNVERIFIED", "ATOMIC_LEDGER_POSTING_UNIMPLEMENTED", "HTTP_423_ORDER_LOCK_REQUIRED"},
	}
	count := 0
	for _, item := range audit.Items {
		if item.ClientOrderID != p.ClientOrderID {
			continue
		}
		count++
		if item.Action != Enter || item.PositionSide != p.PositionSide || item.Stage == ExecutionSettled {
			return PackageAEntryRecoveryPlan{}, errors.New("entry lifecycle conflicts with signed entry identity or is settled")
		}
		p.LifecycleStage = string(item.Stage)
	}
	if count > 1 {
		return PackageAEntryRecoveryPlan{}, errors.New("duplicate entry lifecycle")
	}
	if count == 0 {
		p.BlockerCodes = append(p.BlockerCodes, "MISSING_ENTRY_LIFECYCLE")
	} else if p.LifecycleStage != string(ExecutionMayHaveSent) && p.LifecycleStage != string(ExecutionSignedObserved) {
		p.BlockerCodes = append(p.BlockerCodes, "UNEXPECTED_ENTRY_LIFECYCLE_STAGE")
	}

	signed := make(map[int64]exchange.AccountTrade, len(archive.Fills))
	for _, trade := range archive.Fills {
		if trade.TradeID <= 0 || trade.OrderID != p.OrderID || trade.Symbol != "BTCUSDT" {
			return PackageAEntryRecoveryPlan{}, errors.New("invalid archived signed fill identity")
		}
		if _, exists := signed[trade.TradeID]; exists {
			return PackageAEntryRecoveryPlan{}, errors.New("duplicate archived signed fill")
		}
		signed[trade.TradeID] = trade
	}
	// Scan ALL existing fill files, not just the expected filenames. Reject a
	// second fill owned by this entry but not in the archive or a colliding
	// signed trade ID belonging to a different order. Never mask drift.
	fills, err := (&OwnershipStore{fillDir: filepath.Join(dataDir, "trading", "fills")}).readFills()
	if err != nil {
		return PackageAEntryRecoveryPlan{}, err
	}
	for _, local := range fills {
		signedTrade, known := signed[local.Trade.TradeID]
		if local.ClientOrderID == p.ClientOrderID {
			if !known || !equalCloseTrade(local.Trade, signedTrade) {
				return PackageAEntryRecoveryPlan{}, errors.New("unrecognized/conflicting locally owned entry fill")
			}
		} else if known && local.Trade.Symbol == "BTCUSDT" {
			return PackageAEntryRecoveryPlan{}, errors.New("signed entry fill trade ID belongs to another local owner")
		}
	}
	for _, trade := range archive.Fills {
		present, err := checkLocalSignedFill(dataDir, p.ClientOrderID, trade)
		if err != nil {
			return PackageAEntryRecoveryPlan{}, err
		}
		if present {
			continue
		}
		proposed := StoredFill{Schema: 1, ClientOrderID: p.ClientOrderID, Trade: trade}
		proposed.seal()
		p.Candidates = append(p.Candidates, PackageAEntryRecoveryCandidate{
			TradeID:                  trade.TradeID,
			JournalBasename:          fmt.Sprintf("BTCUSDT_%d.json", trade.TradeID),
			ArchivedSignedTrade:      trade,
			ProposedStoredFillSHA256: proposed.Checksum,
		})
	}
	sort.Slice(p.Candidates, func(i, j int) bool { return p.Candidates[i].TradeID < p.Candidates[j].TradeID })
	sort.Slice(p.AlreadyImportedIDs, func(i, j int) bool { return p.AlreadyImportedIDs[i] < p.AlreadyImportedIDs[j] })
	if len(p.Candidates) != len(archive.Gap.MissingLocalTradeIDs) {
		return PackageAEntryRecoveryPlan{}, errors.New("missing fill candidate count differs from signed gap report")
	}
	for i, c := range p.Candidates {
		if c.TradeID != archive.Gap.MissingLocalTradeIDs[i] {
			return PackageAEntryRecoveryPlan{}, errors.New("missing fill identity changed since signed gap report")
		}
	}
	if len(p.Candidates) > 0 {
		p.BlockerCodes = append(p.BlockerCodes, "OWNED_ENTRY_FILLS_NOT_IMPORTED")
	}
	after, err := snapshotExecutionEvidence(dataDir)
	if err != nil || before != after {
		return PackageAEntryRecoveryPlan{}, errors.New("local evidence changed during recovery planning")
	}
	p.Checksum = p.digest()
	return p, nil
}

// A stored plan cannot be replayed or trusted after the source changes.
// Build it again from the original immutable archive and current local files.
func VerifyPackageAEntryRecoveryPlan(ctx context.Context, dataDir string, archive PackageAEntryArchive, plan PackageAEntryRecoveryPlan) error {
	if plan.Schema != 1 || plan.Stage != PackageAEntryRecoveryPlanStage || plan.Checksum == "" || plan.Checksum != plan.digest() ||
		plan.RecoveryApplied || plan.LifecycleChanged || plan.OwnershipChanged || plan.FullHistoryVerified || plan.FundingAttributionVerified || plan.SettlementAuthorized || plan.TradingAuthorized {
		return errors.New("entry recovery plan is corrupt or claims financial authorization")
	}
	rebuilt, err := PlanPackageAEntryRecovery(ctx, dataDir, archive)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(rebuilt, plan) {
		return errors.New("entry recovery plan conflicts with current original evidence")
	}
	return nil
}
