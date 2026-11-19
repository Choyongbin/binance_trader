package trading

// Read-only entry-fill gap evidence for legacy MAY_HAVE_SENT incidents whose
// signed trade exists but original local fills are absent. This runs BEFORE
// full external-close proposal staging; it does not require review receipts.
// It NEVER calls ImportDemoOrder, settles or mutates the original journals.
import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"path/filepath"
	"sort"

	"binance_trader/internal/exchange"
)

type PackageAEntryGapReport struct {
	Schema                 int          `json:"schema"`
	Status                 string       `json:"status"`
	ClientOrderID          string       `json:"client_order_id"`
	ModelID                string       `json:"model_id"`
	PositionSide           PositionSide `json:"position_side"`
	SignedOrderID          int64        `json:"signed_order_id"`
	SignedOrderStatus      string       `json:"signed_order_status"`
	SignedTradeIDs         []int64      `json:"signed_trade_ids"`
	AlreadyImportedIDs     []int64      `json:"already_imported_ids"`
	MissingLocalTradeIDs   []int64      `json:"missing_local_trade_ids"`
	OriginalEvidenceSHA256 string       `json:"original_evidence_sha256"`
	SignedEvidenceSHA256   string       `json:"signed_evidence_sha256"`
	Blockers               []string     `json:"blockers"`
	FillJournalChanged     bool         `json:"fill_journal_changed"`
	LifecycleChanged       bool         `json:"lifecycle_changed"`
	SettlementAuthorized   bool         `json:"settlement_authorized"`
	TradingAuthorized      bool         `json:"trading_authorized"`
}

func AuditPackageAEntryGap(ctx context.Context, dataDir, id string, reader DemoTradeReader, creds DemoCredentials) (PackageAEntryGapReport, error) {
	if reader == nil || creds.APIKey == "" || creds.Secret == "" || !externalReviewID.MatchString(id) {
		return PackageAEntryGapReport{}, errors.New("signed Demo reader, credentials and valid order ID required")
	}
	reviews, err := NewExternalCloseReviewLedger(dataDir)
	if err != nil {
		return PackageAEntryGapReport{}, err
	}
	if err = reviews.ensureOffline(); err != nil {
		return PackageAEntryGapReport{}, err
	}
	start, err := snapshotExecutionEvidence(dataDir)
	if err != nil {
		return PackageAEntryGapReport{}, err
	}
	rec, err := readRecord(filepath.Join(dataDir, "trading", "requests", id+".json"))
	if err != nil {
		return PackageAEntryGapReport{}, err
	}
	plan, e1 := rec.Intent.Plan()
	hash, e2 := digestIntent(rec.Intent)
	if e1 != nil || e2 != nil || rec.IntentDigest != hash || rec.Plan != plan || rec.Plan.ClientOrderID != id || rec.Plan.Symbol != "BTCUSDT" || rec.Intent.Action != Enter {
		return PackageAEntryGapReport{}, errors.New("invalid original immutable entry journal")
	}
	first, err := reader.QueryDemoOrder(ctx, creds.APIKey, creds.Secret, "BTCUSDT", id)
	if err != nil {
		return PackageAEntryGapReport{}, err
	}
	want, err := verifyRemoteOrder(rec, first)
	if err != nil || first.Status != "FILLED" {
		return PackageAEntryGapReport{}, errors.New("signed entry must be a fully filled owned order")
	}
	fills, err := reader.FetchDemoOrderTrades(ctx, creds.APIKey, creds.Secret, "BTCUSDT", first.OrderID)
	if err != nil {
		return PackageAEntryGapReport{}, err
	}
	second, err := reader.QueryDemoOrder(ctx, creds.APIKey, creds.Secret, "BTCUSDT", id)
	if err != nil {
		return PackageAEntryGapReport{}, err
	}
	if first != second {
		return PackageAEntryGapReport{}, errors.New("signed order changed during independent double read")
	}
	sum := new(big.Rat)
	seen := map[int64]bool{}
	imported, missing, all := []int64{}, []int64{}, []int64{}
	for _, t := range fills {
		if t.OrderID != first.OrderID || seen[t.TradeID] || t.CommissionAsset != "USDT" {
			return PackageAEntryGapReport{}, errors.New("duplicate/mismatched signed entry trade")
		}
		if err := validateFill(rec, t); err != nil {
			return PackageAEntryGapReport{}, err
		}
		price, e1 := positiveRat(t.Price)
		fee, e2 := signedRat(t.Commission)
		pnl, e3 := signedRat(t.RealizedPnL)
		if e1 != nil || price.Sign() <= 0 || e2 != nil || e3 != nil || fee.Sign() < 0 || pnl.Sign() != 0 {
			return PackageAEntryGapReport{}, errors.New("signed entry contains invalid financial values")
		}
		seen[t.TradeID] = true
		q, _ := positiveRat(t.Quantity)
		sum.Add(sum, q)
		all = append(all, t.TradeID)
		present, err := checkLocalSignedFill(dataDir, id, t)
		if err != nil {
			return PackageAEntryGapReport{}, err
		}
		if present {
			imported = append(imported, t.TradeID)
		} else {
			missing = append(missing, t.TradeID)
		}
	}
	if len(all) == 0 || sum.Cmp(want) != 0 {
		return PackageAEntryGapReport{}, errors.New("signed entry fills do not sum to executed quantity")
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	sort.Slice(imported, func(i, j int) bool { return imported[i] < imported[j] })
	sort.Slice(missing, func(i, j int) bool { return missing[i] < missing[j] })
	end, err := snapshotExecutionEvidence(dataDir)
	if err != nil || start != end {
		return PackageAEntryGapReport{}, fmt.Errorf("original evidence changed during read-only audit: %v", err)
	}
	blockers := []string{"signed entry gap inspection does not import trades or resolve MAY_HAVE_SENT", "external/manual close and pooled Hedge side remain unreconciled", "account-wide history and funding allocation not proven", "operator-approved atomic settlement not implemented"}
	if len(missing) > 0 {
		blockers = append(blockers, "missing original fills require separate idempotent owned-order recovery")
	}
	return PackageAEntryGapReport{Schema: 1, Status: "SIGNED_ENTRY_GAP_REVIEW_ONLY", ClientOrderID: id, ModelID: rec.Intent.ModelID, PositionSide: rec.Plan.PositionSide, SignedOrderID: first.OrderID, SignedOrderStatus: first.Status, SignedTradeIDs: all, AlreadyImportedIDs: imported, MissingLocalTradeIDs: missing, OriginalEvidenceSHA256: start, SignedEvidenceSHA256: externalCloseHash(struct {
		Order exchange.OrderResult
		Fills []exchange.AccountTrade
	}{first, fills}), Blockers: blockers}, nil
}
