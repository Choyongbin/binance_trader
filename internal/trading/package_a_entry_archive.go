package trading

// This archive preserves the bounded Binance Demo order/fill evidence captured
// from signed GET calls while it is still available. Binance REST signatures
// authenticate requests, NOT the exported JSON: the archive's SHA-256 is only
// a local corruption check, never a cryptographic exchange attestation.
// No method in this file can post, import, settle, cancel or authorize orders.
import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"binance_trader/internal/exchange"
)

const entryArchiveStage = "SIGNED_ENTRY_EVIDENCE_ARCHIVED_REVIEW_ONLY"

type PackageAEntryArchive struct {
	Schema               int                     `json:"schema"`
	Stage                string                  `json:"stage"`
	CapturedAtUTC        string                  `json:"captured_at_utc"`
	Scope                string                  `json:"scope"`
	Order                exchange.OrderResult    `json:"signed_order_snapshot"`
	Fills                []exchange.AccountTrade `json:"signed_entry_fills_snapshot"`
	Gap                  PackageAEntryGapReport  `json:"original_entry_gap_audit"`
	Checksum             string                  `json:"local_sha256_checksum"`
	SettlementAuthorized bool                    `json:"settlement_authorized"`
	TradingAuthorized    bool                    `json:"trading_authorized"`
}

func (a PackageAEntryArchive) digest() string {
	a.Checksum = ""
	return externalCloseHash(a)
}

// CapturePackageAEntryArchive re-queries a signed Demo order twice AFTER a
// separate source-journal audit, and requires both reads and detailed fills
// to match the original audit fingerprint. The original journals are read-only.
func CapturePackageAEntryArchive(ctx context.Context, dataDir, clientID string, reader DemoTradeReader, creds DemoCredentials, now time.Time) (PackageAEntryArchive, error) {
	if reader == nil || !externalReviewID.MatchString(clientID) || now.IsZero() {
		return PackageAEntryArchive{}, errors.New("valid entry ID, clock and signed Demo reader required")
	}
	gap, err := AuditPackageAEntryGap(ctx, dataDir, clientID, reader, creds)
	if err != nil {
		return PackageAEntryArchive{}, err
	}
	first, err := reader.QueryDemoOrder(ctx, creds.APIKey, creds.Secret, "BTCUSDT", clientID)
	if err != nil {
		return PackageAEntryArchive{}, err
	}
	fills, err := reader.FetchDemoOrderTrades(ctx, creds.APIKey, creds.Secret, "BTCUSDT", first.OrderID)
	if err != nil {
		return PackageAEntryArchive{}, err
	}
	second, err := reader.QueryDemoOrder(ctx, creds.APIKey, creds.Secret, "BTCUSDT", clientID)
	if err != nil {
		return PackageAEntryArchive{}, err
	}
	if first != second || first.Status != "FILLED" || first.OrderID != gap.SignedOrderID || first.ClientOrderID != clientID {
		return PackageAEntryArchive{}, errors.New("archive signed order changed between independent reads")
	}
	signedHash := externalCloseHash(struct {
		Order exchange.OrderResult
		Fills []exchange.AccountTrade
	}{first, fills})
	if signedHash != gap.SignedEvidenceSHA256 {
		return PackageAEntryArchive{}, errors.New("archive signed details differ from independent entry gap audit")
	}
	// Re-run original gap logic with an in-memory snapshot. Besides checking
	// order quantity and all financial details, this detects journal drift.
	replayed, err := AuditPackageAEntryGap(ctx, dataDir, clientID, entryArchiveReader{first, fills}, DemoCredentials{APIKey: "ARCHIVE_REPLAY", Secret: "ARCHIVE_REPLAY"})
	if err != nil || !reflect.DeepEqual(gap, replayed) {
		return PackageAEntryArchive{}, fmt.Errorf("archive original journal changed or signed evidence inconsistent: %v", err)
	}
	a := PackageAEntryArchive{Schema: 1, Stage: entryArchiveStage,
		CapturedAtUTC: now.UTC().Format(time.RFC3339Nano),
		Scope:         "SINGLE_ENTRY_ORDER_FILLS_ONLY_NO_EXIT_OR_ACCOUNT_HISTORY",
		Order:         first, Fills: append([]exchange.AccountTrade(nil), fills...), Gap: gap}
	a.Checksum = a.digest()
	return a, nil
}

type entryArchiveReader struct {
	order exchange.OrderResult
	fills []exchange.AccountTrade
}

func (r entryArchiveReader) QueryDemoOrder(_ context.Context, _, _, _, _ string) (exchange.OrderResult, error) {
	return r.order, nil
}
func (r entryArchiveReader) FetchDemoOrderTrades(_ context.Context, _, _, _ string, _ int64) ([]exchange.AccountTrade, error) {
	return append([]exchange.AccountTrade(nil), r.fills...), nil
}

// VerifyPackageAEntryArchive replays all entry checks against current source
// journals without credentials or network calls. A checksum-valid archive is
// NEVER fresh signed proof, lifetime-history proof or settlement authorization.
func VerifyPackageAEntryArchive(ctx context.Context, dataDir string, a PackageAEntryArchive) error {
	if a.Schema != 1 || a.Stage != entryArchiveStage ||
		a.Scope != "SINGLE_ENTRY_ORDER_FILLS_ONLY_NO_EXIT_OR_ACCOUNT_HISTORY" ||
		a.Checksum == "" || a.Checksum != a.digest() ||
		a.SettlementAuthorized || a.TradingAuthorized || a.Gap.TradingAuthorized || a.Gap.SettlementAuthorized ||
		a.Gap.Schema != 1 || a.Gap.Status != "SIGNED_ENTRY_GAP_REVIEW_ONLY" ||
		a.Gap.ClientOrderID != a.Order.ClientOrderID || a.Order.OrderID != a.Gap.SignedOrderID ||
		a.Order.Status != "FILLED" || a.Order.Symbol != "BTCUSDT" || len(a.Fills) == 0 {
		return errors.New("entry archive checksum/schema/authorization invalid")
	}
	at, err := time.Parse(time.RFC3339Nano, a.CapturedAtUTC)
	if err != nil || at.IsZero() {
		return errors.New("entry archive capture time invalid")
	}
	signedHash := externalCloseHash(struct {
		Order exchange.OrderResult
		Fills []exchange.AccountTrade
	}{a.Order, a.Fills})
	if signedHash != a.Gap.SignedEvidenceSHA256 {
		return errors.New("entry archive signed detail digest mismatch")
	}
	replayed, err := AuditPackageAEntryGap(ctx, dataDir, a.Gap.ClientOrderID, entryArchiveReader{a.Order, a.Fills}, DemoCredentials{APIKey: "ARCHIVE_REPLAY", Secret: "ARCHIVE_REPLAY"})
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(replayed, a.Gap) {
		return errors.New("entry archive does not match current immutable source journals")
	}
	return nil
}
