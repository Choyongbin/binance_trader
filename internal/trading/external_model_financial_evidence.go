package trading

// dev.3.126 offline fee/funding evidence contract.  The captured Demo signed
// GET response was signed as an API REQUEST, not as an exchange-certified
// archival document.  This package NEVER authorizes model PnL, applies a
// financial ledger change, or allocates pooled Hedge funding.
import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sort"
)

const ExternalModelFinancialEvidenceOnly = "BOUNDED_MODEL_FEE_TRACE_FUNDING_UNALLOCATED_NOT_POSTED"

type ExternalModelFeeTrace struct {
	TradeID      int64  `json:"trade_id"`
	OrderID      int64  `json:"order_id"`
	Leg          string `json:"leg"`
	QuantityBTC  string `json:"quantity_btc"`
	FeeAsset     string `json:"fee_asset"`
	FeeUSDT      string `json:"fee_usdt"`
	RealizedUSDT string `json:"exchange_realized_pnl_usdt"`
	SourceSHA256 string `json:"source_sha256"`
}

type ExternalUnallocatedFundingTrace struct {
	TranID       int64  `json:"tran_id"`
	TimeMS       int64  `json:"time_ms"`
	Asset        string `json:"asset"`
	IncomeUSDT   string `json:"account_income_usdt"`
	SourceSHA256 string `json:"source_sha256"`
}

// Every total is an observation about the BOUNDED captured event, NOT a
// definitive model balance. Null model funding is intentional: it is not zero.
type ExternalModelFinancialEvidence struct {
	Schema                    int                               `json:"schema"`
	Stage                     string                            `json:"stage"`
	ClientOrderID             string                            `json:"client_order_id"`
	ModelID                   string                            `json:"model_id"`
	PositionSide              PositionSide                      `json:"position_side"`
	BoundedReportSHA256       string                            `json:"bounded_report_sha256"`
	IncidentArchiveSHA256     string                            `json:"incident_archive_sha256"`
	OriginalEvidenceSHA256    string                            `json:"original_evidence_sha256"`
	HistoryScope              string                            `json:"history_scope"`
	FeeRows                   []ExternalModelFeeTrace           `json:"bounded_trade_fee_rows"`
	FundingRows               []ExternalUnallocatedFundingTrace `json:"account_funding_rows_unallocated"`
	EntryCommissionUSDT       string                            `json:"bounded_entry_commission_usdt"`
	ExitCommissionUSDT        string                            `json:"bounded_exit_commission_usdt"`
	TotalCommissionUSDT       string                            `json:"bounded_total_commission_usdt"`
	ExchangeExitRealizedUSDT  string                            `json:"bounded_exchange_exit_realized_usdt"`
	ProvisionalExFundingUSDT  string                            `json:"provisional_net_excluding_funding_usdt"`
	BoundedAccountFundingUSDT string                            `json:"bounded_account_funding_unallocated_usdt"`
	ModelFundingUSDT          *string                           `json:"model_funding_usdt"`
	ModelFinalNetUSDT         *string                           `json:"model_final_net_usdt"`
	BlockerCodes              []string                          `json:"blocker_codes"`
	CompleteHistoryProven     bool                              `json:"complete_history_proven"`
	ModelOwnershipProven      bool                              `json:"model_ownership_proven"`
	FundingAllocated          bool                              `json:"funding_allocated"`
	PostingApplied            bool                              `json:"posting_applied"`
	SettlementAuthorized      bool                              `json:"settlement_authorized"`
	TradingAuthorized         bool                              `json:"trading_authorized"`
	DigestSHA256              string                            `json:"digest_sha256"`
}

func (r ExternalModelFinancialEvidence) digest() string {
	r.DigestSHA256 = ""
	return externalCloseHash(r)
}

// ValidateExternalModelFinancialEvidence detects accidentally or maliciously
// inconsistent local JSON reports. It does NOT authenticate the report's
// origin; its SHA-256 is an integrity fingerprint, not a digital signature.
func ValidateExternalModelFinancialEvidence(r ExternalModelFinancialEvidence) error {
	if r.Schema != 1 || r.Stage != ExternalModelFinancialEvidenceOnly ||
		!externalReviewID.MatchString(r.ClientOrderID) || r.ModelID == "" ||
		(r.PositionSide != Long && r.PositionSide != Short) ||
		!externalReviewHash.MatchString(r.BoundedReportSHA256) ||
		!externalReviewHash.MatchString(r.IncidentArchiveSHA256) ||
		!externalReviewHash.MatchString(r.OriginalEvidenceSHA256) ||
		r.HistoryScope != "ENTRY_FIRST_FILL_TO_EXIT_LAST_FILL_ONLY" ||
		len(r.FeeRows) < 2 || len(r.BlockerCodes) != 7 ||
		r.ModelFundingUSDT != nil || r.ModelFinalNetUSDT != nil ||
		r.CompleteHistoryProven || r.ModelOwnershipProven || r.FundingAllocated ||
		r.PostingApplied || r.SettlementAuthorized || r.TradingAuthorized ||
		r.DigestSHA256 != r.digest() {
		return errors.New("untrusted or authorizing model financial report")
	}
	for i, code := range modelFinancialBlockers() {
		if r.BlockerCodes[i] != code {
			return errors.New("mandatory model finance blockers altered")
		}
	}
	entry, exit, realized, funding := new(big.Rat), new(big.Rat), new(big.Rat), new(big.Rat)
	lastTradeID := int64(0)
	entries, exits := 0, 0
	for _, row := range r.FeeRows {
		if row.TradeID <= lastTradeID || row.OrderID <= 0 || row.FeeAsset != "USDT" ||
			!externalReviewHash.MatchString(row.SourceSHA256) {
			return errors.New("fee-trace identity or order invalid")
		}
		lastTradeID = row.TradeID
		fee, fe := signedRat(row.FeeUSDT)
		pnl, pe := signedRat(row.RealizedUSDT)
		qty, qe := positiveRat(row.QuantityBTC)
		if fe != nil || pe != nil || qe != nil || fee.Sign() < 0 ||
			row.FeeUSDT != formatOwnershipDecimal(fee) ||
			row.RealizedUSDT != formatOwnershipDecimal(pnl) ||
			row.QuantityBTC != formatOwnershipDecimal(qty) {
			return errors.New("non-canonical fee-trace amount")
		}
		switch row.Leg {
		case "ENTRY":
			if pnl.Sign() != 0 {
				return errors.New("entry realized PnL must be zero")
			}
			entry.Add(entry, fee)
			entries++
		case "EXIT":
			exit.Add(exit, fee)
			realized.Add(realized, pnl)
			exits++
		default:
			return errors.New("unknown fee-trace leg")
		}
	}
	if entries == 0 || exits == 0 {
		return errors.New("both entry and exit trades required")
	}
	lastFundingID := int64(0)
	for _, row := range r.FundingRows {
		if row.TranID <= lastFundingID || row.TimeMS <= 0 || row.Asset != "USDT" ||
			!externalReviewHash.MatchString(row.SourceSHA256) {
			return errors.New("duplicate or invalid funding trace")
		}
		lastFundingID = row.TranID
		amount, err := signedRat(row.IncomeUSDT)
		if err != nil || row.IncomeUSDT != formatOwnershipDecimal(amount) {
			return errors.New("non-canonical funding amount")
		}
		funding.Add(funding, amount)
	}
	fees := new(big.Rat).Add(new(big.Rat).Set(entry), exit)
	net := new(big.Rat).Sub(new(big.Rat).Set(realized), fees)
	if formatOwnershipDecimal(entry) != r.EntryCommissionUSDT ||
		formatOwnershipDecimal(exit) != r.ExitCommissionUSDT ||
		formatOwnershipDecimal(fees) != r.TotalCommissionUSDT ||
		formatOwnershipDecimal(realized) != r.ExchangeExitRealizedUSDT ||
		formatOwnershipDecimal(net) != r.ProvisionalExFundingUSDT ||
		formatOwnershipDecimal(funding) != r.BoundedAccountFundingUSDT {
		return errors.New("model financial report amounts fail exact fee/funding invariants")
	}
	return nil
}

// A textual 12-place rendering is not an exact monetary allocation when
// additional nonzero digits would be silently rounded. Refuse such evidence
// instead of letting a rounded display conceal an unmatched exchange fee.
func ensureExactModelFinancialDecimal(v *big.Rat) error {
	display := formatOwnershipDecimal(v)
	decoded, err := signedRat(display)
	if err != nil || decoded.Cmp(v) != 0 {
		return errors.New("financial evidence needs precision beyond 12 decimal places")
	}
	return nil
}

func modelFinancialBlockers() []string {
	return []string{"MODEL_FUNDING_OWNER_NOT_PROVEN", "PRE_ENTRY_ACCOUNT_HISTORY_NOT_PROVEN",
		"CAPTURED_SIGNED_GET_NOT_EXCHANGE_ATTESTATION", "MODEL_OWNERSHIP_NOT_PROVEN",
		"REAL_FINANCIAL_COMMIT_NOT_IMPLEMENTED", "INDEPENDENT_SETTLEMENT_APPROVAL_REQUIRED", "HTTP_423_ORDER_LOCK_REQUIRED"}
}

// InspectExternalModelFinancialEvidence binds each individual commission row,
// funding transaction, and exact rational sum to the *already independently*
// reverified original journals and Package A archive.  Nothing is written.
func InspectExternalModelFinancialEvidence(ctx context.Context, dataDir, clientID string, incident PackageAIncidentArchive) (ExternalModelFinancialEvidence, error) {
	var zero ExternalModelFinancialEvidence
	bounded, err := InspectExternalCloseBoundedFinancials(ctx, dataDir, clientID, incident)
	if err != nil {
		return zero, err
	}
	if bounded.DigestSHA256 != bounded.digest() || bounded.Stage != ExternalBoundedFinancialReconciledNotPosted ||
		bounded.PostingApplied || bounded.SettlementAuthorized || bounded.TradingAuthorized ||
		bounded.OwnershipChanged || bounded.LifecycleChanged ||
		bounded.IncidentArchiveSHA256 != incident.Checksum {
		return zero, errors.New("bounded financial inspection does not meet non-posting evidence contract")
	}
	e := incident.Close
	if e.HistoryScope != "ENTRY_FIRST_FILL_TO_EXIT_LAST_FILL_ONLY" || e.HistoricalCoverageComplete || e.FundingVerified {
		return zero, errors.New("untrusted funding or account history scope")
	}
	feeRows := make([]ExternalModelFeeTrace, 0, len(e.EntryFills)+len(e.ExitFills))
	fundingRows := make([]ExternalUnallocatedFundingTrace, 0, len(e.FundingRows))
	entryFee, exitFee, gross, funding := new(big.Rat), new(big.Rat), new(big.Rat), new(big.Rat)
	seenTrades := make(map[int64]bool, len(e.EntryFills)+len(e.ExitFills))
	trace := func(leg string, tradeID, orderID int64, qty, feeAsset, fee, realized string, source any) error {
		if tradeID <= 0 || orderID <= 0 || seenTrades[tradeID] || feeAsset != "USDT" {
			return errors.New("ambiguous trade identity or non-USDT fee")
		}
		seenTrades[tradeID] = true
		q, qe := positiveRat(qty)
		f, fe := signedRat(fee)
		p, pe := signedRat(realized)
		if qe != nil || fe != nil || pe != nil || f.Sign() < 0 || (leg == "ENTRY" && p.Sign() != 0) {
			return errors.New("invalid per-fill financial amount")
		}
		for _, amount := range []*big.Rat{q, f, p} {
			if err := ensureExactModelFinancialDecimal(amount); err != nil {
				return err
			}
		}
		if leg == "ENTRY" {
			entryFee.Add(entryFee, f)
		} else {
			exitFee.Add(exitFee, f)
			gross.Add(gross, p)
		}
		feeRows = append(feeRows, ExternalModelFeeTrace{
			TradeID: tradeID, OrderID: orderID, Leg: leg, QuantityBTC: formatOwnershipDecimal(q),
			FeeAsset: feeAsset, FeeUSDT: formatOwnershipDecimal(f), RealizedUSDT: formatOwnershipDecimal(p), SourceSHA256: externalCloseHash(source),
		})
		return nil
	}
	for _, t := range e.EntryFills {
		if err := trace("ENTRY", t.TradeID, t.OrderID, t.Quantity, t.CommissionAsset, t.Commission, t.RealizedPnL, t); err != nil {
			return zero, err
		}
	}
	for _, t := range e.ExitFills {
		if err := trace("EXIT", t.TradeID, t.OrderID, t.Quantity, t.CommissionAsset, t.Commission, t.RealizedPnL, t); err != nil {
			return zero, err
		}
	}
	sort.Slice(feeRows, func(i, j int) bool { return feeRows[i].TradeID < feeRows[j].TradeID })
	seenFunding := map[int64]bool{}
	for _, row := range e.FundingRows {
		if row.TranID <= 0 || seenFunding[row.TranID] || row.Symbol != "BTCUSDT" || row.IncomeType != "FUNDING_FEE" ||
			row.Asset != "USDT" || row.TimeMS < e.HistoryFromMS || row.TimeMS > e.HistoryToMS {
			return zero, errors.New("ambiguous account funding row")
		}
		seenFunding[row.TranID] = true
		amount, err := signedRat(row.Income)
		if err != nil {
			return zero, err
		}
		if err := ensureExactModelFinancialDecimal(amount); err != nil {
			return zero, err
		}
		funding.Add(funding, amount)
		fundingRows = append(fundingRows, ExternalUnallocatedFundingTrace{TranID: row.TranID, TimeMS: row.TimeMS,
			Asset: row.Asset, IncomeUSDT: formatOwnershipDecimal(amount), SourceSHA256: externalCloseHash(row)})
	}
	sort.Slice(fundingRows, func(i, j int) bool { return fundingRows[i].TranID < fundingRows[j].TranID })
	fees := new(big.Rat).Add(new(big.Rat).Set(entryFee), exitFee)
	net := new(big.Rat).Sub(new(big.Rat).Set(gross), fees)
	if len(feeRows) != len(bounded.EntryTradeIDs)+len(bounded.ExitTradeIDs) ||
		len(fundingRows) != bounded.FundingRowCount ||
		formatOwnershipDecimal(entryFee) != bounded.EntryCommissionUSDT ||
		formatOwnershipDecimal(exitFee) != bounded.ExitCommissionUSDT ||
		formatOwnershipDecimal(gross) != bounded.ExchangeExitRealizedPnLUSDT ||
		formatOwnershipDecimal(net) != bounded.ProvisionalNetExFundingUSDT ||
		formatOwnershipDecimal(funding) != bounded.UnallocatedFundingUSDT {
		return zero, fmt.Errorf("per-row financial trace conflicts with bounded original amounts")
	}
	result := ExternalModelFinancialEvidence{
		Schema: 1, Stage: ExternalModelFinancialEvidenceOnly, ClientOrderID: clientID, ModelID: bounded.ModelID,
		PositionSide: bounded.PositionSide, BoundedReportSHA256: bounded.DigestSHA256,
		IncidentArchiveSHA256: incident.Checksum, OriginalEvidenceSHA256: bounded.RealEvidenceSHA256,
		HistoryScope: e.HistoryScope, FeeRows: feeRows, FundingRows: fundingRows,
		EntryCommissionUSDT: formatOwnershipDecimal(entryFee), ExitCommissionUSDT: formatOwnershipDecimal(exitFee),
		TotalCommissionUSDT: formatOwnershipDecimal(fees), ExchangeExitRealizedUSDT: formatOwnershipDecimal(gross),
		ProvisionalExFundingUSDT: formatOwnershipDecimal(net), BoundedAccountFundingUSDT: formatOwnershipDecimal(funding),
		BlockerCodes: modelFinancialBlockers(),
	}
	result.DigestSHA256 = result.digest()
	if err := ValidateExternalModelFinancialEvidence(result); err != nil {
		return zero, err
	}
	// Guard against a rewrite between this trace and its independently
	// verified source. This is a best-effort consistency check on STOPPED data,
	// not an atomic multi-file read transaction.
	after, err := InspectExternalCloseBoundedFinancials(ctx, dataDir, clientID, incident)
	if err != nil || after.DigestSHA256 != bounded.DigestSHA256 {
		return zero, errors.New("original evidence drifted during model financial trace")
	}
	return result, nil
}

// Unposted assertions can be cross-checked as CANDIDATES only. In particular
// even a claim of zero model funding is forbidden: zero is not equivalent to
// an independently proven allocation. The caller cannot set authority flags.
type ExternalModelFinancialAssertion struct {
	ClientOrderID            string  `json:"client_order_id"`
	ModelID                  string  `json:"model_id"`
	EvidenceSHA256           string  `json:"evidence_sha256"`
	EntryCommissionUSDT      string  `json:"entry_commission_usdt"`
	ExitCommissionUSDT       string  `json:"exit_commission_usdt"`
	ExchangeRealizedUSDT     string  `json:"exchange_realized_pnl_usdt"`
	ProvisionalExFundingUSDT string  `json:"provisional_net_excluding_funding_usdt"`
	ModelFundingUSDT         *string `json:"model_funding_usdt"`
	ModelFinalNetUSDT        *string `json:"model_final_net_usdt"`
	PostingApplied           bool    `json:"posting_applied"`
	SettlementAuthorized     bool    `json:"settlement_authorized"`
	TradingAuthorized        bool    `json:"trading_authorized"`
}

func ValidateExternalModelFinancialAssertion(r ExternalModelFinancialEvidence, a ExternalModelFinancialAssertion) error {
	if ValidateExternalModelFinancialEvidence(r) != nil ||
		a.ClientOrderID != r.ClientOrderID || a.ModelID != r.ModelID || a.EvidenceSHA256 != r.DigestSHA256 ||
		a.ModelFundingUSDT != nil || a.ModelFinalNetUSDT != nil ||
		a.PostingApplied || a.SettlementAuthorized || a.TradingAuthorized {
		return errors.New("untrusted or authorizing model financial assertion")
	}
	if a.EntryCommissionUSDT != r.EntryCommissionUSDT || a.ExitCommissionUSDT != r.ExitCommissionUSDT ||
		a.ExchangeRealizedUSDT != r.ExchangeExitRealizedUSDT || a.ProvisionalExFundingUSDT != r.ProvisionalExFundingUSDT {
		return errors.New("unposted model financial assertion conflicts with signed bounded trade rows")
	}
	return nil
}
