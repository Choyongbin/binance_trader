package trading

// End-to-end OFFLINE rehearsal of the manual LIMIT partial fill -> one-shot
// cancellation -> immutable fill import -> aggregate position audit -> draft
// owner-sized TP/SL. No real Binance order methods are used by this test.
import (
	"binance_trader/internal/exchange"
	"context"
	"testing"
	"time"
)

func TestMockManualLimitPartialFillCancelAuditAndProtectionDraft(t *testing.T) {
	store, reader, rec, dir := setupManualCancel(t)
	ctx := context.Background()
	// First observation: manual GTC order remains partially filled.
	reader.trades[rec.ExchangeOrderID] = []exchange.AccountTrade{receipt(rec, 551, "0.004", "100000", "0.10", "0", 1700000000551)}
	now := time.Now().UTC()
	partial := ReviewManualOrder(rec, SignedManualEvidence{Order: reader.orders[rec.Plan.ClientOrderID], OrderReadAt: now, FromSignedGET: true,
		Trades: reader.trades[rec.ExchangeOrderID], TradesFetchedAt: now, FullTradesFromSignedGET: true}, now)
	if partial.State != "OPEN_PARTIAL" || partial.RemainingBTC != "0.006" || partial.MayPlaceProtection || partial.MayReleaseReservation {
		t.Fatalf("unexpected partial %+v", partial)
	}
	cancelPlan, err := store.PlanVerified(ctx, reader, "demo", "secret", rec.Plan.ClientOrderID)
	if err != nil {
		t.Fatal(err)
	}
	transport := &mockManualCancel{reader: reader, status: "CANCELED", filled: "0.004"}
	if _, err := store.AttemptOnce(ctx, transport, "demo", "secret", cancelPlan); err != nil {
		t.Fatal(err)
	}
	// Mock the independent signed readback; a canceled LIMIT still owns
	// precisely the 0.004 BTC executed before cancellation.
	finished := reader.orders[rec.Plan.ClientOrderID]
	finished.Status = "CANCELED"
	reader.orders[rec.Plan.ClientOrderID] = finished
	now = time.Now().UTC()
	canceled := ReviewManualOrder(rec, SignedManualEvidence{Order: finished, OrderReadAt: now, FromSignedGET: true,
		Trades: reader.trades[rec.ExchangeOrderID], TradesFetchedAt: now, FullTradesFromSignedGET: true}, now)
	if canceled.State != "TERMINAL_PARTIAL" || canceled.FilledBTC != "0.004" || canceled.MayReleaseReservation || canceled.MayPlaceProtection {
		t.Fatalf("invalid canceled state %+v", canceled)
	}
	ledger, err := NewOwnershipStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	imported, err := ledger.ImportDemoOrder(ctx, reader, DemoCredentials{"demo", "secret"}, rec.Intent)
	if err != nil || imported != 1 {
		t.Fatalf("fill import %v %d", err, imported)
	}
	now = time.Now().UTC()
	audited, err := ledger.Audit(accountSnapshot(now, "0.004", "0"), observations(reader, now), now)
	if err != nil || !audited.ReadyForAssessment || audited.ObservedFills != 1 {
		t.Fatalf("ownership audit %+v %v", audited, err)
	}
	owner := owned(t, audited, Manual, "", Long)
	if owner.Quantity != "0.004" || owner.AverageEntry != "100000" {
		t.Fatalf("incorrect owner partial fill: %+v", owner)
	}
	quote := Quote{Symbol: "BTCUSDT", Price: "100000", ReceivedAt: now}
	draft, err := PreviewProtection(owner, sizingRules(now), quote, quote, "manual-draft-001", now)
	if err != nil || draft.TP.Quantity != "0.004" || draft.SL.Quantity != "0.004" {
		t.Fatalf("draft not owner-sized %+v %v", draft, err)
	}
	protection, err := OpenProtectionSubmissionStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = protection.Prepare("manual-draft-001", owner, draft); err != nil {
		t.Fatal(err)
	}
	if state, err := protection.Inspect("manual-draft-001", "tp"); err != nil || state.MayHaveSent || state.LiveAuthorized {
		t.Fatalf("unexpected simulated TP state %+v %v", state, err)
	}
}
