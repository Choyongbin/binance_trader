package trading

// Offline, read-only financial amount reconciliation for a single reviewed
// external close.  A captured Binance signed GET response is NOT itself an
// exchange-signed archival document.  This check cannot prove a complete
// account history, assign pooled-side funding, post a ledger or authorize a
// trade.  It only verifies the bounded entry/exit amounts and explicitly
// preserves their unallocated funding amount.
import (
	"context"
	"errors"
	"fmt"
	"math/big"
)

const ExternalBoundedFinancialReconciledNotPosted = "BOUNDED_FINANCIAL_AMOUNTS_RECONCILED_NOT_POSTED"

// BoundedFinancialReconciliation is a fingerprinted *observation*, not a
// payable balance or permission to settle. The funding amount deliberately
// has no model allocation. The net excludes ALL funding and is provisional.
type BoundedFinancialReconciliation struct {
	Schema                      int          `json:"schema"`
	Stage                       string       `json:"stage"`
	ClientOrderID               string       `json:"client_order_id"`
	ModelID                     string       `json:"model_id"`
	PositionSide                PositionSide `json:"position_side"`
	ProposalSHA256              string       `json:"proposal_sha256"`
	IncidentArchiveSHA256       string       `json:"incident_archive_sha256"`
	RealEvidenceSHA256          string       `json:"real_evidence_sha256"`
	EntryTradeIDs               []int64      `json:"entry_trade_ids"`
	ExitTradeIDs                []int64      `json:"exit_trade_ids"`
	EntryQuantityBTC            string       `json:"entry_quantity_btc"`
	ExitQuantityBTC             string       `json:"exit_quantity_btc"`
	EntryCommissionUSDT         string       `json:"entry_commission_usdt"`
	ExitCommissionUSDT          string       `json:"exit_commission_usdt"`
	ExchangeExitRealizedPnLUSDT string       `json:"exchange_exit_realized_pnl_usdt"`
	ProvisionalNetExFundingUSDT string       `json:"provisional_net_ex_funding_usdt"`
	UnallocatedFundingUSDT      string       `json:"unallocated_bounded_funding_usdt"`
	FundingRowCount             int          `json:"bounded_funding_rows"`
	MissingLocalEntryTradeIDs   []int64      `json:"missing_local_entry_trade_ids"`
	BlockerCodes                []string     `json:"blocker_codes"`
	PostingApplied              bool         `json:"posting_applied"`
	LifecycleChanged            bool         `json:"lifecycle_changed"`
	OwnershipChanged            bool         `json:"ownership_changed"`
	SettlementAuthorized        bool         `json:"settlement_authorized"`
	TradingAuthorized           bool         `json:"trading_authorized"`
	DigestSHA256                string       `json:"digest_sha256"`
}

func (r BoundedFinancialReconciliation) digest() string {
	r.DigestSHA256 = ""
	return externalCloseHash(r)
}

// InspectExternalCloseBoundedFinancials binds the persisted single-incident
// review proposal to the archived signed GET payload and the immutable local
// request/fill/lifecycle evidence. No network or filesystem writes occur.
// A successful return means amounts reconcile *within the bounded window*;
// every returned authority flag is false, even when all local numbers agree.
func InspectExternalCloseBoundedFinancials(ctx context.Context, dataDir, clientID string, incident PackageAIncidentArchive) (BoundedFinancialReconciliation, error) {
	var empty BoundedFinancialReconciliation
	if ctx == nil || dataDir == "" || !externalReviewID.MatchString(clientID) {
		return empty, errors.New("context, data directory and entry ID required")
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	if incident.ClientOrderID != clientID {
		return empty, errors.New("financial incident ID mismatch")
	}
	originalBefore, err := snapshotExecutionEvidence(dataDir)
	if err != nil {
		return empty, fmt.Errorf("original evidence: %w", err)
	}
	if err := VerifyPackageAIncidentArchive(ctx, dataDir, incident); err != nil {
		return empty, fmt.Errorf("bounded incident archive: %w", err)
	}
	store, err := NewExternalCloseSettlementProposalStore(dataDir)
	if err != nil {
		return empty, err
	}
	proposal, err := store.Load(clientID)
	if err != nil {
		return empty, fmt.Errorf("settlement proposal: %w", err)
	}
	if proposal.LocalEvidenceSHA256 != originalBefore || originalBefore != incident.OriginalEvidenceSHA256 ||
		proposal.EntryOrderID != incident.Close.EntryOrderID || proposal.ExitOrderID != incident.Close.ExitOrderID ||
		!equalExternalTradeIDs(proposal.EntryTradeIDs, incident.Close.EntryTradeIDs) ||
		!equalExternalTradeIDs(proposal.ExitTradeIDs, incident.Close.ExitTradeIDs) {
		return empty, errors.New("proposed allocation and bounded source do not describe the same incident")
	}
	if err := checkSignedFinancials(proposal, incident.Close); err != nil {
		return empty, fmt.Errorf("signed bounded amounts: %w", err)
	}
	entryQuantity, exitQuantity := new(big.Rat), new(big.Rat)
	entryCommission, exitCommission, realized := new(big.Rat), new(big.Rat), new(big.Rat)
	for _, t := range incident.Close.EntryFills {
		qty, _ := positiveRat(t.Quantity) // validated by VerifyPackageAIncidentArchive + checkSignedFinancials
		fee, _ := signedRat(t.Commission)
		entryQuantity.Add(entryQuantity, qty)
		entryCommission.Add(entryCommission, fee)
	}
	for _, t := range incident.Close.ExitFills {
		qty, _ := positiveRat(t.Quantity)
		fee, _ := signedRat(t.Commission)
		pnl, _ := signedRat(t.RealizedPnL)
		exitQuantity.Add(exitQuantity, qty)
		exitCommission.Add(exitCommission, fee)
		realized.Add(realized, pnl)
	}
	net := new(big.Rat).Sub(new(big.Rat).Sub(new(big.Rat).Set(realized), entryCommission), exitCommission)
	funding, err := signedRat(incident.Close.BoundedFundingNetUSDT)
	if err != nil || entryQuantity.Cmp(exitQuantity) != 0 ||
		formatOwnershipDecimal(net) != proposal.ProvisionalNetExcludingFunding ||
		formatOwnershipDecimal(entryCommission) != proposal.EntryCommission ||
		formatOwnershipDecimal(exitCommission) != proposal.ExitCommission ||
		formatOwnershipDecimal(realized) != proposal.SignedRealizedPnL {
		return empty, errors.New("bounded financial totals differ from original proposal")
	}
	codes := []string{
		"COMPLETE_RELEVANT_TRADE_HISTORY", "MODEL_FUNDING_AND_FEES",
		"EXTERNAL_EXIT_OWNERSHIP", "ATOMIC_CROSS_LEDGER_POSTING",
		"INDEPENDENT_SETTLEMENT_APPROVAL", "HTTP_423_ORDER_LOCK_REQUIRED",
	}
	missing := append([]int64{}, incident.Entry.Gap.MissingLocalTradeIDs...)
	if len(missing) > 0 {
		codes = append(codes, "MISSING_LOCAL_ENTRY_FILL")
	}
	result := BoundedFinancialReconciliation{
		Schema: 1, Stage: ExternalBoundedFinancialReconciledNotPosted,
		ClientOrderID: clientID, ModelID: proposal.ModelID, PositionSide: proposal.PositionSide,
		ProposalSHA256: proposal.Checksum, IncidentArchiveSHA256: incident.Checksum,
		RealEvidenceSHA256: originalBefore,
		EntryTradeIDs:      append([]int64{}, proposal.EntryTradeIDs...),
		ExitTradeIDs:       append([]int64{}, proposal.ExitTradeIDs...),
		EntryQuantityBTC:   formatOwnershipDecimal(entryQuantity), ExitQuantityBTC: formatOwnershipDecimal(exitQuantity),
		EntryCommissionUSDT: formatOwnershipDecimal(entryCommission), ExitCommissionUSDT: formatOwnershipDecimal(exitCommission),
		ExchangeExitRealizedPnLUSDT: formatOwnershipDecimal(realized),
		ProvisionalNetExFundingUSDT: formatOwnershipDecimal(net), UnallocatedFundingUSDT: formatOwnershipDecimal(funding),
		FundingRowCount: len(incident.Close.FundingRows), MissingLocalEntryTradeIDs: missing, BlockerCodes: codes,
	}
	result.DigestSHA256 = result.digest()
	// Reject modifications that happened during the several source reads.
	finalProposal, err := store.Load(clientID)
	if err != nil || finalProposal.Checksum != proposal.Checksum {
		return empty, errors.New("settlement proposal changed during financial inspection")
	}
	originalAfter, err := snapshotExecutionEvidence(dataDir)
	if err != nil || originalAfter != originalBefore {
		return empty, errors.New("real source changed during financial inspection")
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	return result, nil
}
