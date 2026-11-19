package trading

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"binance_trader/internal/exchange"
	"binance_trader/internal/wallet"
)

func cleanReservationBasis(now time.Time) ReservationBasis {
	return ReservationBasis{
		Snapshot: exchange.Snapshot{Environment: "demo", Status: "connected", AsOf: now.UTC().Format(time.RFC3339Nano),
			Balances: []exchange.Balance{{Asset: "USDT", Wallet: "1000", Available: "1000"}}, Positions: []exchange.Position{}, Orders: []exchange.Order{}, AlgoOrders: []exchange.AlgoOrder{}},
		Quote:                  Quote{Symbol: "BTCUSDT", Price: "100000", ReceivedAt: now},
		Audit:                  OwnershipReport{ReadyForAssessment: true, Owned: []OwnerPosition{}, Blockers: []string{}},
		AutoWalletAssignedUSDT: "1000", WalletVerified: true,
	}
}
func modelIntent(id, model string, side PositionSide, action Action, qty string) Intent {
	return Intent{RequestID: id, Source: Model, ModelID: model, Symbol: "BTCUSDT", PositionSide: side, Action: action, OrderType: Market, Quantity: qty}
}
func TestCoordinatorSingleWriterPersistentCrashLock(t *testing.T) {
	d := t.TempDir()
	a, err := OpenCoordinator(d)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenCoordinator(d); !errors.Is(err, ErrCoordinatorLocked) {
		t.Fatalf("second process allowed: %v", err)
	}
	now := time.Now().UTC()
	basis := cleanReservationBasis(now)
	i := modelIntent("model-A-req1", "A", Long, Enter, "0.005")
	r, _, err := a.reservePreview(i, basis, now)
	if err != nil {
		t.Fatal(err)
	}
	if r.MarginUSDT != "33.333333333334" {
		t.Fatalf("margin rounded down: %s", r.MarginUSDT)
	}
	if _, _, err := a.reservePreview(i, basis, now); err == nil {
		t.Fatal("duplicate reserved again")
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	b, err := OpenCoordinator(d)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.reservePreview(i, basis, now); err == nil {
		t.Fatal("duplicate after restart accepted")
	}
	// Crash simulation: no Close -> surviving atomic lock must block new owner.
	if _, err := OpenCoordinator(d); !errors.Is(err, ErrCoordinatorLocked) {
		t.Fatalf("crash lock auto-recovered: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}
func TestCoordinatorAccountCapAndPerEntry(t *testing.T) {
	c, err := OpenCoordinator(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	now := time.Now().UTC()
	b := cleanReservationBasis(now)
	if _, _, err := c.reservePreview(modelIntent("large-entry01", "A", Long, Enter, "0.016"), b, now); err == nil {
		t.Fatal("accepted > 10% per-model entry")
	}
	// Each .01 BTC consumes $66.667. At 10% of the remaining budget,
	// five entries pass; the sixth exceeds its shrinking per-entry cap.
	for n := 1; n <= 5; n++ {
		id := "model-entry" + string(rune('a'+n))
		_, _, err := c.reservePreview(modelIntent(id, "A", Long, Enter, "0.01"), b, now)
		if err != nil {
			t.Fatalf("reservation %d: %v", n, err)
		}
	}
	if _, _, err := c.reservePreview(modelIntent("model-entryZ", "A", Long, Enter, "0.01"), b, now); err == nil || !strings.Contains(err.Error(), "10%") {
		t.Fatalf("failed aggregate cap: %v", err)
	}
	// Manual entries share available USDT but are not counted against the
	// model-only assigned-budget cap; they receive independent reservations.
	if _, _, err := c.reservePreview(Intent{RequestID: "manual-req01", Source: Manual, Symbol: "BTCUSDT", PositionSide: Short, Action: Enter, OrderType: Market, Quantity: "0.01"}, b, now); err != nil {
		t.Fatalf("manual wrongly counted in model budget %v", err)
	}
}
func TestCoordinatorOwnerExitReservationAndCorruptionStop(t *testing.T) {
	d := t.TempDir()
	c, err := OpenCoordinator(d)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	b := cleanReservationBasis(now)
	b.Audit.Owned = []OwnerPosition{{Source: Model, ModelID: "A", Symbol: "BTCUSDT", Side: Long, Quantity: "0.010", AverageEntry: "100000"},
		{Source: Model, ModelID: "B", Symbol: "BTCUSDT", Side: Long, Quantity: "0.005", AverageEntry: "100000"}}
	first := modelIntent("model-A-exit1", "A", Long, Exit, "0.007")
	r, _, err := c.reservePreview(first, b, now)
	if err != nil {
		t.Fatal(err)
	}
	if r.MarginUSDT != "0" {
		t.Fatal("exit consumed collateral")
	}
	if _, _, err := c.reservePreview(modelIntent("model-A-exit2", "A", Long, Exit, "0.004"), b, now); err == nil {
		t.Fatal("over-reserved owner A")
	}
	if _, _, err := c.reservePreview(modelIntent("model-B-exit1", "B", Long, Exit, "0.003"), b, now); !errors.Is(err, ErrPooledSideExitInFlight) {
		t.Fatalf("same pooled LONG side was not serialized: %v", err)
	}
	if _, _, err := c.reservePreview(modelIntent("model-B-enter2", "B", Long, Enter, "0.001"), b, now); !errors.Is(err, ErrPooledSideExitInFlight) {
		t.Fatalf("same-side re-entry bypassed active exit fence: %v", err)
	}
	if _, _, err := c.reservePreview(modelIntent("model-A-exit3", "A", Short, Exit, "0.001"), b, now); err == nil {
		t.Fatal("short exit used long holdings")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	// Corrupt a real reservation file after graceful release. Opening a new
	// instance must lock/fail, not silently discard risk exposure.
	path := filepath.Join(d, "trading", "reservations", r.ClientOrderID+".json")
	if err := os.WriteFile(path, []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenCoordinator(d); err == nil {
		t.Fatal("corrupt exposure silently ignored")
	}
	if _, err := OpenCoordinator(d); !errors.Is(err, ErrCoordinatorLocked) {
		t.Fatal("corrupt startup lock not retained")
	}
}
func TestCoordinatorRequiresFreshSignedContext(t *testing.T) {
	c, err := OpenCoordinator(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	now := time.Now().UTC()
	b := cleanReservationBasis(now)
	i := modelIntent("fresh-entry1", "A", Long, Enter, "0.005")
	b.Quote.ReceivedAt = now.Add(-5 * time.Second)
	if _, _, err := c.reservePreview(i, b, now); err == nil {
		t.Fatal("stale price accepted")
	}
	b = cleanReservationBasis(now)
	b.Snapshot.Orders = []exchange.Order{{ClientOrderID: "external"}}
	if _, _, err := c.reservePreview(i, b, now); err == nil {
		t.Fatal("external open order accepted")
	}
	b = cleanReservationBasis(now)
	b.Audit.ReadyForAssessment = false
	if _, _, err := c.reservePreview(i, b, now); err == nil {
		t.Fatal("unaudited holdings accepted")
	}
	b = cleanReservationBasis(now)
	b.Snapshot.Balances = []exchange.Balance{{Asset: "USDT", Wallet: "1000", Available: "1001"}}
	if _, _, err := c.reservePreview(i, b, now); err == nil {
		t.Fatal("invalid balance accepted")
	}
}
func TestCoordinatorReleaseRequiresFullOwnershipAudit(t *testing.T) {
	e, store, reader := setupOwnership(t)
	// Engine+store root is private; derive it from the journal path.
	root := filepath.Dir(filepath.Dir(store.journalDir))
	c, err := OpenCoordinator(root)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	now := time.Now().UTC()
	basis := cleanReservationBasis(now)
	i := makeIntent("manual-test01", Manual, "", Long, Enter, "0.010")
	r, _, err := c.reservePreview(i, basis, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Settle(r, store, accountSnapshot(time.Now(), "0", "0"), nil, time.Now()); err == nil {
		t.Fatal("unsubmitted reservation released")
	}
	rec := sendAndImport(t, e, store, reader, i, 101, "0.010", "100000", "0.4", "0")
	// Terminal signed order + full imported fills + account qty match.
	when := time.Now().UTC()
	snap := accountSnapshot(when, "0.010", "0")
	obs := observations(reader, when)
	if err := c.Settle(r, store, snap, obs, when); err != nil {
		t.Fatal(err)
	}
	if err := c.Settle(r, store, snap, obs, when); err != nil {
		t.Fatal("idempotent settle", err)
	}
	// If a second reservation reuses the same ID, fail even after settlement.
	if _, _, err := c.reservePreview(rec.Intent, cleanReservationBasis(time.Now()), time.Now()); err == nil {
		t.Fatal("settled request ID reused")
	}
}

func TestPartialFillKeepsExitCapacityReservedUntilSignedCancelReadback(t *testing.T) {
	engine, store, reader := setupOwnership(t)
	dir := filepath.Dir(filepath.Dir(store.journalDir))
	c, err := OpenCoordinator(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	now := time.Now().UTC()
	i := makeIntent("partial-enter01", Manual, "", Long, Enter, "0.010")
	reservation, _, err := c.reservePreview(i, cleanReservationBasis(now), now)
	if err != nil {
		t.Fatal(err)
	}
	record, err := engine.Submit(context.Background(), i, allow)
	if err != nil {
		t.Fatal(err)
	}
	partial := orderFromRecord(record, "PARTIALLY_FILLED", "0.004")
	reader.orders[record.Plan.ClientOrderID] = partial
	reader.trades[partial.OrderID] = []exchange.AccountTrade{receipt(record, 200, "0.004", "100000", "0.20", "0", 1700000000200)}
	if n, err := store.ImportDemoOrder(context.Background(), reader, DemoCredentials{APIKey: "mock", Secret: "mock"}, i); err != nil || n != 1 {
		t.Fatalf("partial import %d %v", n, err)
	}
	now = time.Now().UTC()
	snap := accountSnapshot(now, "0.004", "0")
	obs := observations(reader, now)
	report, err := store.Audit(snap, obs, now)
	if err != nil || report.ReadyForAssessment {
		t.Fatalf("partial order incorrectly ready %+v %v", report, err)
	}
	if err := c.Settle(reservation, store, snap, obs, now); err == nil {
		t.Fatal("partial fill prematurely released")
	}
	// A verified cancellation terminates the unfilled .006. Only then may
	// the outstanding reservation be removed; .004 remains in the owner's lot.
	final := partial
	final.Status = "CANCELED"
	reader.orders[record.Plan.ClientOrderID] = final
	if n, err := store.ImportDemoOrder(context.Background(), reader, DemoCredentials{APIKey: "mock", Secret: "mock"}, i); err != nil || n != 0 {
		t.Fatalf("cancel idempotent import %d %v", n, err)
	}
	now = time.Now().UTC()
	snap = accountSnapshot(now, "0.004", "0")
	obs = observations(reader, now)
	report, err = store.Audit(snap, obs, now)
	if err != nil || !report.ReadyForAssessment {
		t.Fatalf("terminal partial audit %+v %v", report, err)
	}
	if err := c.Settle(reservation, store, snap, obs, now); err != nil {
		t.Fatal(err)
	}
	if qty := owned(t, report, Manual, "", Long).Quantity; qty != "0.004" {
		t.Fatalf("partial settled wrong owner quantity: %s", qty)
	}
}

func TestPublicReserveRecomputesAuditAndRequiresSameRoot(t *testing.T) {
	dir := t.TempDir()
	c, err := OpenCoordinator(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	store, err := NewOwnershipStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	b := cleanReservationBasis(now)
	i := modelIntent("checked-reserve1", "A", Long, Enter, "0.005")
	// No orders, no fills and empty actual position: independent audit is ready.
	if _, _, err := c.ReserveAudited(i, store, b.Snapshot, map[string]OrderObservation{}, b.Quote, now); err == nil {
		t.Fatal("old wallet-less API accepted model entry")
	}
	budget, _ := wallet.Open(dir)
	if _, err := budget.Change("modelbudget01", "ALLOCATE", "500", "1000", strings.Repeat("a", 64)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.ReserveAuditedWithWallet(i, store, budget, strings.Repeat("a", 64), b.Snapshot, map[string]OrderObservation{}, b.Quote, now); err != nil {
		t.Fatal(err)
	}
	wrong, err := NewOwnershipStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.ReserveAudited(modelIntent("checked-reserve2", "B", Long, Enter, "0.005"), wrong, b.Snapshot, nil, b.Quote, now); err == nil {
		t.Fatal("accepted unrelated journal")
	}
	b.Snapshot.Positions = []exchange.Position{{Symbol: "BTCUSDT", Side: "LONG", Qty: "0.005"}}
	if _, _, err := c.ReserveAudited(modelIntent("checked-reserve3", "C", Long, Enter, "0.005"), store, b.Snapshot, map[string]OrderObservation{}, b.Quote, now); err == nil {
		t.Fatal("accepted forged ready from mock basis")
	}
}
