package main

// The discovery archive is an operator's READ-ONLY evidence bundle. A unique
// candidate may be used to CAPTURE another signed-GET snapshot, but it never
// establishes who placed the exit, full historical coverage, settlement or
// permission to trade. Only an explicit operator review can attribute trades.
import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"sort"
	"time"

	"binance_trader/internal/trading"
)

const discoveryArchiveStage = "DISCOVERED_EXIT_BOUNDED_EVIDENCE_ARCHIVED_REVIEW_ONLY"

type packageADiscoveryArchive struct {
	Schema                int                             `json:"schema"`
	Stage                 string                          `json:"stage"`
	ClientOrderID         string                          `json:"client_order_id"`
	Discovery             exitDiscoveryResult             `json:"signed_exit_discovery"`
	Incident              trading.PackageAIncidentArchive `json:"bounded_incident_archive"`
	OperatorExitConfirmed bool                            `json:"operator_exit_confirmed"`
	SettlementAuthorized  bool                            `json:"settlement_authorized"`
	TradingAuthorized     bool                            `json:"trading_authorized"`
	LifecycleChanged      bool                            `json:"lifecycle_changed"`
	OwnershipChanged      bool                            `json:"ownership_changed"`
	Checksum              string                          `json:"local_sha256_checksum"`
}

func (a packageADiscoveryArchive) digest() string {
	a.Checksum = ""
	payload, _ := json.Marshal(a)
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

// bundleIdentityCheck deliberately validates the correlation independently of
// its checksum, so recomputing a local checksum cannot make mismatched IDs,
// history, direction, or financial claims acceptable.
func bundleIdentityCheck(b packageADiscoveryArchive) error {
	d, inc := b.Discovery, b.Incident
	if b.Schema != 1 || b.Stage != discoveryArchiveStage || b.ClientOrderID == "" ||
		b.OperatorExitConfirmed || b.SettlementAuthorized || b.TradingAuthorized ||
		b.LifecycleChanged || b.OwnershipChanged || b.Checksum != b.digest() ||
		d.Status != "UNIQUE_EXIT_ORDER_ID_CANDIDATE_REVIEW_ONLY" || d.Schema != 1 ||
		!d.BoundedHistoryRead || !d.FreshFlatAccount || d.FullAccountHistoryVerified ||
		d.TradingAuthorized || d.SettlementAuthorized || d.OwnershipChanged || d.LifecycleChanged ||
		len(d.Candidates) != 1 || len(d.UnexpectedTradeIDs) != 0 ||
		len(d.Blockers) == 0 || inc.TradingAuthorized || inc.SettlementAuthorized ||
		inc.LifecycleChanged || inc.OwnershipChanged ||
		b.ClientOrderID != d.EntryClientOrderID || b.ClientOrderID != inc.ClientOrderID ||
		d.EntryOrderID != inc.Entry.Order.OrderID || d.EntryOrderID != inc.Close.EntryOrderID ||
		d.PositionSide != inc.Entry.Order.PositionSide || d.PositionSide != inc.Close.ExitOrder.PositionSide {
		return errors.New("discovery archive identity/authorization mismatch")
	}
	candidate := d.Candidates[0]
	if !candidate.SignedOrderConfirmed || candidate.OrderID <= 0 || candidate.OrderID != inc.Close.ExitOrderID ||
		candidate.OrderID != inc.Close.ExitOrder.OrderID || len(candidate.TradeIDs) == 0 ||
		len(inc.Close.ExitFills) != len(candidate.TradeIDs) {
		return errors.New("discovery candidate differs from independently signed archived exit")
	}
	expectedQty, ok := new(big.Rat).SetString(candidate.Quantity)
	if !ok || expectedQty.Sign() <= 0 {
		return errors.New("invalid discovery exit quantity")
	}
	sumQty := new(big.Rat)
	gotTradeIDs := make([]int64, 0, len(inc.Close.ExitFills))
	for _, t := range inc.Close.ExitFills {
		q, ok := new(big.Rat).SetString(t.Quantity)
		if !ok || q.Sign() <= 0 || t.OrderID != candidate.OrderID || t.TradeID <= 0 {
			return errors.New("invalid signed archived exit fill")
		}
		sumQty.Add(sumQty, q)
		gotTradeIDs = append(gotTradeIDs, t.TradeID)
	}
	sort.Slice(gotTradeIDs, func(i, j int) bool { return gotTradeIDs[i] < gotTradeIDs[j] })
	if sumQty.Cmp(expectedQty) != 0 || !reflect.DeepEqual(gotTradeIDs, candidate.TradeIDs) {
		return errors.New("exit quantity/trade identities disagree with discovery")
	}
	from, err1 := time.Parse(time.RFC3339Nano, d.HistoryFromUTC)
	to, err2 := time.Parse(time.RFC3339Nano, d.HistoryToUTC)
	if err1 != nil || err2 != nil || !to.After(from) || to.Sub(from) > maxExitDiscoverySpan ||
		inc.Close.HistoryFromMS < from.UnixMilli() || inc.Close.HistoryToMS > to.UnixMilli() {
		return errors.New("archived incident exceeds verified discovery history interval")
	}
	return nil
}

func makePackageADiscoveryArchive(d exitDiscoveryResult, incident trading.PackageAIncidentArchive) (packageADiscoveryArchive, error) {
	b := packageADiscoveryArchive{Schema: 1, Stage: discoveryArchiveStage,
		ClientOrderID: incident.ClientOrderID, Discovery: d, Incident: incident}
	b.Checksum = b.digest()
	return b, bundleIdentityCheck(b)
}

// The only capture path used by the command requires two independent GET-only
// audits. Never choose a candidate when there are two or more possible exits.
func capturePackageADiscoveryArchive(ctx context.Context, dataDir, clientID string,
	discoveryReader exitDiscoveryReader, entryReader trading.DemoTradeReader,
	closeReader trading.SignedCloseEvidenceReader, creds trading.DemoCredentials, now time.Time) (packageADiscoveryArchive, error) {
	entry, err := trading.CapturePackageAEntryArchive(ctx, dataDir, clientID, entryReader, creds, now)
	if err != nil {
		return packageADiscoveryArchive{}, fmt.Errorf("entry evidence: %w", err)
	}
	discovery := discoverExitOrder(ctx, discoveryReader, creds, entry, now)
	if discovery.Status != "UNIQUE_EXIT_ORDER_ID_CANDIDATE_REVIEW_ONLY" || len(discovery.Candidates) != 1 {
		return packageADiscoveryArchive{}, fmt.Errorf("external exit is not uniquely evidenced: %v", discovery.Blockers)
	}
	// A second entry capture and bounded entry/exit audit checks the exchange
	// anew; the first discovery cannot force an unrelated order into the report.
	incident, err := trading.CapturePackageAIncidentArchive(ctx, dataDir, clientID, discovery.Candidates[0].OrderID, entryReader, closeReader, creds, time.Now().UTC())
	if err != nil {
		return packageADiscoveryArchive{}, fmt.Errorf("second signed incident audit: %w", err)
	}
	if entry.Order != incident.Entry.Order || !reflect.DeepEqual(entry.Fills, incident.Entry.Fills) ||
		entry.Gap.OriginalEvidenceSHA256 != incident.Entry.Gap.OriginalEvidenceSHA256 ||
		entry.Gap.SignedEvidenceSHA256 != incident.Entry.Gap.SignedEvidenceSHA256 {
		return packageADiscoveryArchive{}, errors.New("entry signed evidence/local journal changed between discovery and incident archive")
	}
	bundle, err := makePackageADiscoveryArchive(discovery, incident)
	if err != nil {
		return packageADiscoveryArchive{}, err
	}
	if err = verifyPackageADiscoveryArchive(ctx, dataDir, bundle); err != nil {
		return packageADiscoveryArchive{}, err
	}
	return bundle, nil
}

func verifyPackageADiscoveryArchive(ctx context.Context, dataDir string, b packageADiscoveryArchive) error {
	if err := bundleIdentityCheck(b); err != nil {
		return err
	}
	// This verifies archive integrity against original local immutable data,
	// NOT account lifetime, attribution, funds or fresh signed credentials.
	return trading.VerifyPackageAIncidentArchive(ctx, dataDir, b.Incident)
}
