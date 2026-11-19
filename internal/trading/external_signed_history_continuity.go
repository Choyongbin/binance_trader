package trading

// Offline BTCUSDT signed-GET capture continuity contract (dev.3.127).
// A local JSON manifest, even if self-hashed, is NOT an exchange-signed proof
// of historical completeness. This review detects missing/overlapping windows,
// truncated responses and disagreements with the independently rechecked
// Package A incident. ALL authority flags remain false. No network or writes.
import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"binance_trader/internal/exchange"
)

const (
	ExternalSignedHistoryScope = "BTCUSDT_ENTRY_FIRST_FILL_TO_EXIT_LAST_FILL_ONLY"
	ExternalSignedHistoryStage = "BOUNDED_WINDOW_SEQUENCE_MATCHED_NOT_ACCOUNT_ATTESTED"
	externalHistoryPageLimit   = 1000
	externalHistoryMaxWindows  = 64
)

type ExternalSignedHistoryWindow struct {
	FromMS int64 `json:"from_ms"`
	ToMS   int64 `json:"to_ms"`
	// Non-nil arrays are required. [] is a valid empty *captured response*.
	// null is an absent response and must never be accepted as no trades.
	Trades            []exchange.AccountTrade  `json:"trade_response_rows"`
	Funding           []exchange.FundingIncome `json:"funding_response_rows"`
	TradePageLimit    int                      `json:"trade_page_limit"`
	FundingPageLimit  int                      `json:"funding_page_limit"`
	TradeRowsSHA256   string                   `json:"trade_rows_sha256"`
	FundingRowsSHA256 string                   `json:"funding_rows_sha256"`
}

// Captured windows are supplied by an offline operator. A SHA-256 checksum
// detects accidental drift but cannot establish their remote origin, the
// authenticity of a signed GET, or the absence of earlier/later account trades.
type ExternalSignedHistoryManifest struct {
	Schema                int                           `json:"schema"`
	ClientOrderID         string                        `json:"client_order_id"`
	IncidentArchiveSHA256 string                        `json:"incident_archive_sha256"`
	Scope                 string                        `json:"scope"`
	Windows               []ExternalSignedHistoryWindow `json:"windows"`
	Checksum              string                        `json:"checksum"`
}

func (m ExternalSignedHistoryManifest) digest() string {
	m.Checksum = ""
	return externalCloseHash(m)
}

// Intentionally NOT an approval token. A matched source interval does not
// imply lifetime coverage, other-symbol coverage, or signed response origin.
type ExternalSignedHistoryContinuity struct {
	Schema                           int      `json:"schema"`
	Stage                            string   `json:"stage"`
	ClientOrderID                    string   `json:"client_order_id"`
	ModelID                          string   `json:"model_id"`
	Scope                            string   `json:"scope"`
	IncidentArchiveSHA256            string   `json:"incident_archive_sha256"`
	ModelEvidenceSHA256              string   `json:"model_evidence_sha256"`
	ManifestSHA256                   string   `json:"manifest_sha256"`
	FromMS                           int64    `json:"from_ms"`
	ToMS                             int64    `json:"to_ms"`
	WindowCount                      int      `json:"window_count"`
	TradeCount                       int      `json:"trade_count"`
	FundingCount                     int      `json:"funding_count"`
	ObservedWindowSequenceContiguous bool     `json:"observed_window_sequence_contiguous"`
	CapturedRowsMatchIncident        bool     `json:"captured_rows_match_incident"`
	ExchangeSourceAttested           bool     `json:"exchange_source_attested"`
	WholeAccountHistoryProven        bool     `json:"whole_account_history_proven"`
	OtherSymbolsCovered              bool     `json:"other_symbols_covered"`
	PrivateStreamContinuityProven    bool     `json:"private_stream_continuity_proven"`
	ModelFundingAllocated            bool     `json:"model_funding_allocated"`
	PostingApplied                   bool     `json:"posting_applied"`
	SettlementAuthorized             bool     `json:"settlement_authorized"`
	TradingAuthorized                bool     `json:"trading_authorized"`
	BlockerCodes                     []string `json:"blocker_codes"`
	DigestSHA256                     string   `json:"digest_sha256"`
}

func (r ExternalSignedHistoryContinuity) digest() string {
	r.DigestSHA256 = ""
	return externalCloseHash(r)
}
func externalHistoryBlockers() []string {
	return []string{
		"CAPTURED_GET_NOT_EXCHANGE_ATTESTATION", "BEFORE_ENTRY_AND_AFTER_EXIT_HISTORY_UNKNOWN",
		"OTHER_SYMBOLS_AND_ORDERS_NOT_COVERED", "PRIVATE_STREAM_CONTINUITY_UNVERIFIED",
		"MODEL_OWNERSHIP_AND_FUNDING_UNVERIFIED", "FINANCIAL_COMMIT_AND_INDEPENDENT_APPROVAL_MISSING",
		"HTTP_423_ORDER_LOCK_REQUIRED",
	}
}

// InspectExternalSignedHistoryContinuity rechecks the original local fills,
// incident, and financial evidence before comparing all provided windows.
// Every adjacent pair must have exact millisecond continuity; duplicate IDs,
// missing responses, at-limit pages, malformed values and omitted rows fail.
func InspectExternalSignedHistoryContinuity(ctx context.Context, dataDir, clientID string, incident PackageAIncidentArchive, manifest ExternalSignedHistoryManifest) (ExternalSignedHistoryContinuity, error) {
	var zero ExternalSignedHistoryContinuity
	if ctx == nil || dataDir == "" || !externalReviewID.MatchString(clientID) {
		return zero, errors.New("data directory, valid client ID and context required")
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if manifest.Schema != 1 || manifest.ClientOrderID != clientID || manifest.IncidentArchiveSHA256 != incident.Checksum ||
		!externalReviewHash.MatchString(manifest.IncidentArchiveSHA256) || manifest.Scope != ExternalSignedHistoryScope ||
		manifest.Checksum != manifest.digest() || len(manifest.Windows) == 0 || len(manifest.Windows) > externalHistoryMaxWindows {
		return zero, errors.New("untrusted history manifest identity, checksum, scope or window count")
	}
	model, err := InspectExternalModelFinancialEvidence(ctx, dataDir, clientID, incident)
	if err != nil {
		return zero, fmt.Errorf("original bounded financial evidence: %w", err)
	}
	if err := ValidateExternalModelFinancialEvidence(model); err != nil {
		return zero, err
	}
	if model.IncidentArchiveSHA256 != manifest.IncidentArchiveSHA256 || model.HistoryScope != "ENTRY_FIRST_FILL_TO_EXIT_LAST_FILL_ONLY" ||
		model.CompleteHistoryProven || model.SettlementAuthorized || model.TradingAuthorized {
		return zero, errors.New("financial source scope or authority invalid")
	}
	from, to := incident.Close.HistoryFromMS, incident.Close.HistoryToMS
	if from <= 0 || to < from || to-from > signedCloseMaxSpan.Milliseconds() {
		return zero, errors.New("incident history bounds absent or exceed signed reader range")
	}
	trades := make(map[int64]exchange.AccountTrade)
	funding := make(map[int64]exchange.FundingIncome)
	next := from
	for i, window := range manifest.Windows {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		if window.FromMS != next || window.ToMS < window.FromMS || window.ToMS > to ||
			window.ToMS-window.FromMS > int64((24*time.Hour)/time.Millisecond) ||
			window.Trades == nil || window.Funding == nil ||
			window.TradePageLimit != externalHistoryPageLimit || window.FundingPageLimit != externalHistoryPageLimit ||
			len(window.Trades) >= externalHistoryPageLimit || len(window.Funding) >= externalHistoryPageLimit ||
			!externalReviewHash.MatchString(window.TradeRowsSHA256) ||
			!externalReviewHash.MatchString(window.FundingRowsSHA256) ||
			window.TradeRowsSHA256 != externalCloseHash(window.Trades) ||
			window.FundingRowsSHA256 != externalCloseHash(window.Funding) {
			return zero, fmt.Errorf("missing/overlapping/truncated/untrusted signed history window %d", i)
		}
		for _, row := range window.Trades {
			if row.Symbol != "BTCUSDT" || row.OrderID <= 0 || row.TradeID <= 0 || row.TimeMS < window.FromMS || row.TimeMS > window.ToMS ||
				(row.Side != "BUY" && row.Side != "SELL") || (row.PositionSide != "LONG" && row.PositionSide != "SHORT") ||
				row.CommissionAsset != "USDT" {
				return zero, fmt.Errorf("invalid trade in history window %d", i)
			}
			if _, err := positiveRat(row.Price); err != nil {
				return zero, fmt.Errorf("invalid trade price in window %d: %w", i, err)
			}
			if _, err := positiveRat(row.Quantity); err != nil {
				return zero, fmt.Errorf("invalid trade quantity in window %d: %w", i, err)
			}
			fee, err := signedRat(row.Commission)
			if err != nil || fee.Sign() < 0 {
				return zero, fmt.Errorf("invalid trade fee in window %d", i)
			}
			if _, err := signedRat(row.RealizedPnL); err != nil {
				return zero, fmt.Errorf("invalid trade PnL in window %d: %w", i, err)
			}
			if _, ok := trades[row.TradeID]; ok {
				return zero, errors.New("trade ID duplicated across windows")
			}
			trades[row.TradeID] = row
		}
		for _, row := range window.Funding {
			if row.Symbol != "BTCUSDT" || row.IncomeType != "FUNDING_FEE" || row.Asset != "USDT" ||
				row.TranID <= 0 || row.TimeMS < window.FromMS || row.TimeMS > window.ToMS {
				return zero, fmt.Errorf("invalid funding row in window %d", i)
			}
			if _, err := signedRat(row.Income); err != nil {
				return zero, fmt.Errorf("invalid funding amount in window %d: %w", i, err)
			}
			if _, ok := funding[row.TranID]; ok {
				return zero, errors.New("funding transaction duplicated across windows")
			}
			funding[row.TranID] = row
		}
		if window.ToMS == to {
			if i != len(manifest.Windows)-1 {
				return zero, errors.New("history windows continue past incident end")
			}
		} else {
			next = window.ToMS + 1 // safe: to-from bounded above, and ToMS < to
		}
	}
	if manifest.Windows[len(manifest.Windows)-1].ToMS != to {
		return zero, errors.New("history window coverage ends before incident exit")
	}
	expectedTrades := make(map[int64]exchange.AccountTrade)
	expectedFunding := make(map[int64]exchange.FundingIncome)
	for _, row := range incident.Close.HistoricalTrades {
		if _, ok := expectedTrades[row.TradeID]; ok {
			return zero, errors.New("duplicate archived trade ID")
		}
		expectedTrades[row.TradeID] = row
	}
	for _, row := range incident.Close.FundingRows {
		if _, ok := expectedFunding[row.TranID]; ok {
			return zero, errors.New("duplicate archived funding ID")
		}
		expectedFunding[row.TranID] = row
	}
	if !reflect.DeepEqual(trades, expectedTrades) || !reflect.DeepEqual(funding, expectedFunding) {
		return zero, errors.New("captured history differs from independently reviewed incident rows")
	}
	// Stable summary only: the supplied manifest and current archive may have
	// matching captured rows, but they cannot prove unseen or expired records.
	result := ExternalSignedHistoryContinuity{
		Schema: 1, Stage: ExternalSignedHistoryStage, ClientOrderID: clientID, ModelID: model.ModelID,
		Scope: ExternalSignedHistoryScope, IncidentArchiveSHA256: incident.Checksum,
		ModelEvidenceSHA256: model.DigestSHA256, ManifestSHA256: manifest.Checksum,
		FromMS: from, ToMS: to, WindowCount: len(manifest.Windows), TradeCount: len(trades), FundingCount: len(funding),
		ObservedWindowSequenceContiguous: true, CapturedRowsMatchIncident: true,
		BlockerCodes: externalHistoryBlockers(),
	}
	result.DigestSHA256 = result.digest()
	// Verify the original disk contents a second time, not just the user's
	// self-hashed window manifest, to catch mid-read journal/proposal mutation.
	modelAgain, err := InspectExternalModelFinancialEvidence(ctx, dataDir, clientID, incident)
	if err != nil || !reflect.DeepEqual(model, modelAgain) {
		return zero, errors.New("source evidence changed while comparing signed windows")
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if err := ValidateExternalSignedHistoryContinuity(result); err != nil {
		return zero, err
	}
	return result, nil
}

// Validate is a local consistency check, NOT authentication or an approval.
func ValidateExternalSignedHistoryContinuity(r ExternalSignedHistoryContinuity) error {
	if r.Schema != 1 || r.Stage != ExternalSignedHistoryStage ||
		!externalReviewID.MatchString(r.ClientOrderID) || r.ModelID == "" || r.Scope != ExternalSignedHistoryScope ||
		!externalReviewHash.MatchString(r.IncidentArchiveSHA256) || !externalReviewHash.MatchString(r.ModelEvidenceSHA256) ||
		!externalReviewHash.MatchString(r.ManifestSHA256) || r.FromMS <= 0 || r.ToMS < r.FromMS ||
		r.WindowCount < 1 || r.WindowCount > externalHistoryMaxWindows || r.TradeCount < 2 || r.FundingCount < 0 ||
		!r.ObservedWindowSequenceContiguous || !r.CapturedRowsMatchIncident || r.ExchangeSourceAttested ||
		r.WholeAccountHistoryProven || r.OtherSymbolsCovered || r.PrivateStreamContinuityProven ||
		r.ModelFundingAllocated || r.PostingApplied || r.SettlementAuthorized || r.TradingAuthorized ||
		!reflect.DeepEqual(r.BlockerCodes, externalHistoryBlockers()) || r.DigestSHA256 != r.digest() {
		return errors.New("invalid or authorizing history continuity report")
	}
	return nil
}
