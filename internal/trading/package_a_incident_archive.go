package trading

// This incident archive captures BOTH the locally owned entry and the
// externally closed side's bounded signed-GET evidence in ONE verifiable,
// non-posting document. It does not attest the complete account lifetime or
// assign pooled Hedge funding to any model. Binance signs REQUESTS, not this
// file; SHA-256 detects accidental/local changes, not a malicious forger.
import (
	"binance_trader/internal/exchange"
	"context"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"sort"
	"time"
)

const PackageAIncidentArchived = "ENTRY_EXIT_BOUNDED_EVIDENCE_ARCHIVED_NOT_SETTLED"

func packageAIncidentBlockers() []string {
	return []string{
		"no proof of complete pre-entry account history",
		"pooled Hedge funding has no independently verified model attribution",
		"original lifecycle and ownership posting not implemented or authorized",
		"captured signed GET responses are not exchange-signed archival documents",
	}
}

type PackageAIncidentArchive struct {
	Schema                 int                  `json:"schema"`
	Stage                  string               `json:"stage"`
	CapturedAtUTC          string               `json:"captured_at_utc"`
	ClientOrderID          string               `json:"client_order_id"`
	Entry                  PackageAEntryArchive `json:"entry"`
	Close                  SignedCloseEvidence  `json:"bounded_signed_close"`
	OriginalEvidenceSHA256 string               `json:"original_evidence_sha256"`
	Checksum               string               `json:"local_sha256_checksum"`
	Scope                  string               `json:"scope"`
	Blockers               []string             `json:"blockers"`
	LifecycleChanged       bool                 `json:"lifecycle_changed"`
	OwnershipChanged       bool                 `json:"ownership_changed"`
	SettlementAuthorized   bool                 `json:"settlement_authorized"`
	TradingAuthorized      bool                 `json:"trading_authorized"`
}

func (a PackageAIncidentArchive) digest() string { a.Checksum = ""; return externalCloseHash(a) }

// CapturePackageAIncidentArchive ONLY accepts an independently-supplied exit
// order ID; it never guesses an order from an account-wide trading history.
// Both readers are GET-only interfaces. Data files are NEVER opened for write.
func CapturePackageAIncidentArchive(ctx context.Context, dataDir, clientID string, exitOrderID int64, entryReader DemoTradeReader, closeReader SignedCloseEvidenceReader, creds DemoCredentials, now time.Time) (PackageAIncidentArchive, error) {
	if ctx.Err() != nil {
		return PackageAIncidentArchive{}, ctx.Err()
	}
	if !externalReviewID.MatchString(clientID) || exitOrderID <= 0 || entryReader == nil || closeReader == nil || creds.APIKey == "" || creds.Secret == "" || now.IsZero() {
		return PackageAIncidentArchive{}, errors.New("valid entry/exit identities and signed Demo readers required")
	}
	before, err := snapshotExecutionEvidence(dataDir)
	if err != nil {
		return PackageAIncidentArchive{}, err
	}
	entry, err := CapturePackageAEntryArchive(ctx, dataDir, clientID, entryReader, creds, now)
	if err != nil {
		return PackageAIncidentArchive{}, fmt.Errorf("entry archive: %w", err)
	}
	closeEvidence, err := closeReader.RefreshExternalCloseEvidence(ctx, entry.Order.OrderID, exitOrderID)
	if err != nil {
		return PackageAIncidentArchive{}, fmt.Errorf("signed exit audit: %w", err)
	}
	a := PackageAIncidentArchive{
		Schema: 1, Stage: PackageAIncidentArchived, CapturedAtUTC: now.UTC().Format(time.RFC3339Nano),
		ClientOrderID: clientID, Entry: entry, Close: closeEvidence, OriginalEvidenceSHA256: before,
		Scope:    "SINGLE_ENTRY_TO_EXIT_BOUNDED_NOT_FULL_ACCOUNT_HISTORY",
		Blockers: packageAIncidentBlockers(),
	}
	a.Checksum = a.digest()
	if err = VerifyPackageAIncidentArchive(ctx, dataDir, a); err != nil {
		return PackageAIncidentArchive{}, err
	}
	after, err := snapshotExecutionEvidence(dataDir)
	if err != nil || before != after {
		return PackageAIncidentArchive{}, errors.New("original financial evidence changed during incident archive")
	}
	return a, nil
}

func VerifyPackageAIncidentArchive(ctx context.Context, dataDir string, a PackageAIncidentArchive) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.Schema != 1 || a.Stage != PackageAIncidentArchived || !externalReviewID.MatchString(a.ClientOrderID) || a.ClientOrderID != a.Entry.Gap.ClientOrderID ||
		a.Scope != "SINGLE_ENTRY_TO_EXIT_BOUNDED_NOT_FULL_ACCOUNT_HISTORY" || !reflect.DeepEqual(a.Blockers, packageAIncidentBlockers()) ||
		a.LifecycleChanged || a.OwnershipChanged || a.SettlementAuthorized || a.TradingAuthorized ||
		!externalReviewHash.MatchString(a.OriginalEvidenceSHA256) || a.Checksum != a.digest() {
		return errors.New("invalid, tampered or authorizing incident archive")
	}
	at, err := time.Parse(time.RFC3339Nano, a.CapturedAtUTC)
	if err != nil || at.IsZero() {
		return errors.New("invalid archive capture clock")
	}
	if err = VerifyPackageAEntryArchive(ctx, dataDir, a.Entry); err != nil {
		return fmt.Errorf("entry archive invalid: %w", err)
	}
	current, err := snapshotExecutionEvidence(dataDir)
	if err != nil {
		return err
	}
	if current != a.OriginalEvidenceSHA256 || a.Entry.Gap.OriginalEvidenceSHA256 != current {
		return errors.New("original trading evidence changed")
	}
	e := a.Close
	if e.EntryOrder != a.Entry.Order || e.EntryOrder.OrderID != e.EntryOrderID || e.ExitOrder.OrderID != e.ExitOrderID ||
		!validSignedCloseOrders(e.EntryOrder, e.ExitOrder) || e.EntryOrderID <= 0 || e.ExitOrderID <= 0 ||
		!e.AccountFlat || e.HistoricalCoverageComplete || e.FundingVerified || e.ObservedAt.IsZero() ||
		e.ObservedAt.After(at.Add(5*time.Second)) || at.Sub(e.ObservedAt) > 2*time.Minute ||
		e.HistoryScope != "ENTRY_FIRST_FILL_TO_EXIT_LAST_FILL_ONLY" ||
		e.HistoryFromMS <= 0 || e.HistoryToMS < e.HistoryFromMS || e.TradeWindowsRead <= 0 ||
		e.FundingWindowsRead != e.TradeWindowsRead || e.BoundedTradeRows <= 0 {
		return errors.New("signed close identity, coverage, timing or safety flags invalid")
	}
	entryFills := append([]exchange.AccountTrade(nil), e.EntryFills...)
	archivedFills := append([]exchange.AccountTrade(nil), a.Entry.Fills...)
	sort.Slice(entryFills, func(i, j int) bool { return entryFills[i].TradeID < entryFills[j].TradeID })
	sort.Slice(archivedFills, func(i, j int) bool { return archivedFills[i].TradeID < archivedFills[j].TradeID })
	if !reflect.DeepEqual(entryFills, archivedFills) {
		return errors.New("signed entry fills disagree between independently captured reports")
	}
	inQty, inIDs, inFirst, inLast, err := checkSignedCloseFills(e.EntryFills, e.EntryOrder)
	if err != nil {
		return err
	}
	outQty, outIDs, outFirst, outLast, err := checkSignedCloseFills(e.ExitFills, e.ExitOrder)
	if err != nil {
		return err
	}
	if inQty.Cmp(outQty) != 0 || inLast >= outFirst || inFirst != e.HistoryFromMS || outLast != e.HistoryToMS ||
		!equalSignedIDs(inIDs, e.EntryTradeIDs) || !equalSignedIDs(outIDs, e.ExitTradeIDs) ||
		!equalSignedIDs(inIDs, a.Entry.Gap.SignedTradeIDs) {
		return errors.New("signed entry/exit chronology, quantity or trade IDs mismatch")
	}
	expected := make(map[int64]exchange.AccountTrade, len(e.EntryFills)+len(e.ExitFills))
	for _, t := range append(append([]exchange.AccountTrade(nil), e.EntryFills...), e.ExitFills...) {
		if _, ok := expected[t.TradeID]; ok {
			return errors.New("trade ID reused across entry and exit")
		}
		expected[t.TradeID] = t
	}
	if len(e.HistoricalTrades) != len(expected) || e.BoundedTradeRows != len(expected) {
		return errors.New("historical signed trades not completely archived")
	}
	seen := map[int64]bool{}
	for _, t := range e.HistoricalTrades {
		v, ok := expected[t.TradeID]
		if !ok || !equalCloseTrade(v, t) || seen[t.TradeID] || t.TimeMS < e.HistoryFromMS || t.TimeMS > e.HistoryToMS {
			return errors.New("bounded history does not match signed fills")
		}
		seen[t.TradeID] = true
	}
	if e.BoundedFundingRows != len(e.FundingRows) {
		return errors.New("funding row count mismatch")
	}
	net := new(big.Rat)
	fundingIDs := map[int64]bool{}
	for _, r := range e.FundingRows {
		if r.Symbol != "BTCUSDT" || r.IncomeType != "FUNDING_FEE" || r.Asset != "USDT" || r.TranID <= 0 || fundingIDs[r.TranID] || r.TimeMS < e.HistoryFromMS || r.TimeMS > e.HistoryToMS {
			return errors.New("funding signed row identity invalid")
		}
		fundingIDs[r.TranID] = true
		n, err := signedRat(r.Income)
		if err != nil {
			return err
		}
		net.Add(net, n)
	}
	if e.BoundedFundingNetUSDT != formatOwnershipDecimal(net) {
		return errors.New("bounded funding sum mismatch")
	}
	// SignedCloseEvidence.FundingRows uses json:",omitempty". A successful
	// bounded signed GET with zero funding rows computes its detail digest with
	// an empty (non-nil) slice, but JSON omits that field on disk. Unmarshalling
	// the archive then yields nil, whose JSON representation is `null` rather
	// than `[]`. Restore the *original fingerprint input* only for a verified
	// zero-row case; never rewrite the archive or relax the digest comparison.
	fundingForDigest := e.FundingRows
	if fundingForDigest == nil && e.BoundedFundingRows == 0 {
		fundingForDigest = []exchange.FundingIncome{}
	}
	detail := struct {
		EntryOrder, ExitOrder          exchange.OrderResult
		EntryFills, ExitFills, History []exchange.AccountTrade
		Funding                        []exchange.FundingIncome
	}{e.EntryOrder, e.ExitOrder, e.EntryFills, e.ExitFills, e.HistoricalTrades, fundingForDigest}
	if !externalReviewHash.MatchString(e.SignedDetailSHA256) || e.SignedDetailSHA256 != externalCloseHash(detail) {
		return errors.New("signed GET detail fingerprint does not match archived contents")
	}
	return nil
}
