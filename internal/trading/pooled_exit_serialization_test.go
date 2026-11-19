package trading

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

func TestPooledExitFenceBlocksSameSideAndSurvivesRestart(t *testing.T) {
	d := t.TempDir()
	c, err := OpenCoordinator(d)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	b := cleanReservationBasis(now)
	b.Audit.Owned = []OwnerPosition{
		{Source: Model, ModelID: "A", Symbol: "BTCUSDT", Side: Long, Quantity: "0.01", AverageEntry: "100000"},
		{Source: Model, ModelID: "B", Symbol: "BTCUSDT", Side: Long, Quantity: "0.02", AverageEntry: "100000"},
	}
	exitA := modelIntent("serial-exit-a01", "A", Long, Exit, "0.005")
	if _, _, err := c.reservePreview(exitA, b, now); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.reservePreview(modelIntent("serial-exit-b01", "B", Long, Exit, "0.005"), b, now); !errors.Is(err, ErrPooledSideExitInFlight) {
		t.Fatalf("second owner exit accepted: %v", err)
	}
	if _, _, err := c.reservePreview(modelIntent("serial-enter-b01", "B", Long, Enter, "0.001"), b, now); !errors.Is(err, ErrPooledSideExitInFlight) {
		t.Fatalf("same-side re-entry accepted: %v", err)
	}
	// The opposite Hedge side does not share Binance's LONG pool and can still
	// be staged by this local serialization layer. Other global risk gates remain.
	if _, _, err := c.reservePreview(modelIntent("serial-short-in01", "B", Short, Enter, "0.001"), b, now); err != nil {
		t.Fatalf("opposite side incorrectly fenced: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenCoordinator(d)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, _, err := reopened.reservePreview(modelIntent("serial-enter-b02", "B", Long, Enter, "0.001"), b, now); !errors.Is(err, ErrPooledSideExitInFlight) {
		t.Fatalf("restart lost durable exit fence: %v", err)
	}
}

func TestPooledExitCannotStartBehindPendingSameSideEntry(t *testing.T) {
	c, err := OpenCoordinator(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	now := time.Now().UTC()
	b := cleanReservationBasis(now)
	if _, _, err := c.reservePreview(modelIntent("pending-entry-a1", "A", Long, Enter, "0.001"), b, now); err != nil {
		t.Fatal(err)
	}
	b.Audit.Owned = []OwnerPosition{{Source: Model, ModelID: "B", Symbol: "BTCUSDT", Side: Long, Quantity: "0.01", AverageEntry: "100000"}}
	if _, _, err := c.reservePreview(modelIntent("blocked-exit-b1", "B", Long, Exit, "0.005"), b, now); !errors.Is(err, ErrPooledSideMutationPending) {
		t.Fatalf("exit interleaved with unsettled entry: %v", err)
	}
}

func TestPooledExitSettleRequiresAuditThenReleasesSameSideFence(t *testing.T) {
	engine, store, reader := setupOwnership(t)
	root := filepath.Dir(filepath.Dir(store.journalDir))
	// Establish two independent owners on one Binance LONG pool.
	entryA := makeIntent("ser-a-enter01", Model, "A", Long, Enter, "0.010")
	entryB := makeIntent("ser-b-enter01", Model, "B", Long, Enter, "0.020")
	sendAndImport(t, engine, store, reader, entryA, 301, "0.010", "100000", "0.1", "0")
	sendAndImport(t, engine, store, reader, entryB, 302, "0.020", "100000", "0.1", "0")

	c, err := OpenCoordinator(root)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	now := time.Now().UTC()
	beforeSnap := accountSnapshot(now, "0.030", "0")
	beforeSnap.Balances = []exchange.Balance{{Asset: "USDT", Wallet: "1000", Available: "1000"}}
	beforeAudit, err := store.Audit(beforeSnap, observations(reader, now), now)
	if err != nil || !beforeAudit.ReadyForAssessment {
		t.Fatalf("pre-exit ownership audit %+v %v", beforeAudit, err)
	}
	basis := ReservationBasis{Snapshot: beforeSnap, Audit: beforeAudit, Quote: Quote{Symbol: "BTCUSDT", Price: "100000", ReceivedAt: now}, AutoWalletAssignedUSDT: "1000", WalletVerified: true}
	exitA := makeIntent("ser-a-exit001", Model, "A", Long, Exit, "0.005")
	reservation, _, err := c.reservePreview(exitA, basis, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.reservePreview(makeIntent("ser-b-exit001", Model, "B", Long, Exit, "0.005"), basis, now); !errors.Is(err, ErrPooledSideExitInFlight) {
		t.Fatalf("second exit not fenced: %v", err)
	}

	// A local reservation is not enough. Settle must fail until the exit order
	// is actually signed-observed/imported and the aggregate LONG position agrees.
	if err := c.Settle(reservation, store, beforeSnap, observations(reader, now), now); err == nil {
		t.Fatal("unsubmitted exit released pooled-side fence")
	}
	sendAndImport(t, engine, store, reader, exitA, 303, "0.005", "101000", "0.1", "4.9")
	after := time.Now().UTC()
	afterSnap := accountSnapshot(after, "0.025", "0")
	afterSnap.Balances = []exchange.Balance{{Asset: "USDT", Wallet: "1000", Available: "1000"}}
	afterObs := observations(reader, after)
	afterAudit, err := store.Audit(afterSnap, afterObs, after)
	if err != nil || !afterAudit.ReadyForAssessment {
		t.Fatalf("post-exit audit %+v %v", afterAudit, err)
	}
	if err := c.Settle(reservation, store, afterSnap, afterObs, after); err != nil {
		t.Fatal(err)
	}

	// The v2 done marker is a checksum-sealed restart attestation of the audit.
	donePath := filepath.Join(root, "trading", "reservations", reservation.ClientOrderID+".done.json")
	raw, err := os.ReadFile(donePath)
	if err != nil {
		t.Fatal(err)
	}
	var done reservationDoneMarker
	if err := json.Unmarshal(raw, &done); err != nil || done.Schema != 2 || !done.OwnershipReady || done.Action != Exit || done.PositionSide != Long || done.OwnerRemainingQty != "0.005" || done.ExchangeSideQty != "0.025" || done.Checksum == "" || done.Checksum != done.digest() {
		t.Fatalf("invalid settlement attestation %+v err=%v", done, err)
	}

	// Only after Settle may the next owner on the pooled side reserve an exit.
	basis = ReservationBasis{Snapshot: afterSnap, Audit: afterAudit, Quote: Quote{Symbol: "BTCUSDT", Price: "101000", ReceivedAt: after}, AutoWalletAssignedUSDT: "1000", WalletVerified: true}
	if _, _, err := c.reservePreview(makeIntent("ser-b-exit002", Model, "B", Long, Exit, "0.005"), basis, after); err != nil {
		t.Fatalf("next serialized exit remained blocked after signed reconciliation: %v", err)
	}
}

func TestPooledExitReviewIsReadOnlyAndDetectsLegacyConflict(t *testing.T) {
	d := t.TempDir()
	c, err := OpenCoordinator(d)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	b := cleanReservationBasis(now)
	b.Audit.Owned = []OwnerPosition{{Source: Model, ModelID: "A", Symbol: "BTCUSDT", Side: Long, Quantity: "0.01", AverageEntry: "100000"}}
	if _, _, err := c.reservePreview(modelIntent("audit-exit-a01", "A", Long, Exit, "0.005"), b, now); err != nil {
		t.Fatal(err)
	}
	before, err := directoryRegularFileDigests(filepath.Join(d, "trading", "reservations"))
	if err != nil {
		t.Fatal(err)
	}
	r, err := ReviewPooledExitSerialization(d)
	if err != nil || r.State != PooledExitReviewFenced || r.ActiveReservations != 1 || len(r.Sides) != 1 || r.Sides[0].ActiveExits != 1 || r.TradingAuthorized || !r.ManualReviewRequired {
		t.Fatalf("unexpected read-only review %+v %v", r, err)
	}
	after, _ := directoryRegularFileDigests(filepath.Join(d, "trading", "reservations"))
	if strings.Join(before, "|") != strings.Join(after, "|") {
		t.Fatal("read-only pooled audit mutated reservations")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate an older release that had already persisted two active same-side
	// exits. The audit must preserve the evidence and classify, not repair it.
	reserveDir := filepath.Join(d, "trading", "reservations")
	legacy := modelIntent("legacy-exit-b01", "B", Long, Exit, "0.003")
	plan, _ := legacy.Plan()
	digest, _ := digestIntent(legacy)
	lr := Reservation{Schema: 1, ClientOrderID: plan.ClientOrderID, Intent: legacy, IntentDigest: digest, ReservedQuantity: legacy.Quantity, MarginUSDT: "0", CreatedAt: now.Format(time.RFC3339Nano)}
	lr.seal()
	buf, _ := json.MarshalIndent(lr, "", "  ")
	if err := os.WriteFile(filepath.Join(reserveDir, lr.ClientOrderID+".json"), append(buf, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	r, err = ReviewPooledExitSerialization(d)
	if err != nil || r.State != PooledExitReviewConflict || r.ActiveReservations != 2 || len(r.Sides) != 1 || r.Sides[0].ActiveExits != 2 {
		t.Fatalf("legacy concurrent exits not detected %+v %v", r, err)
	}
}

func TestReservationDoneV2TamperFailsClosed(t *testing.T) {
	engine, store, reader := setupOwnership(t)
	root := filepath.Dir(filepath.Dir(store.journalDir))
	c, err := OpenCoordinator(root)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	intent := makeIntent("tamper-enter01", Manual, "", Long, Enter, "0.001")
	res, _, err := c.reservePreview(intent, cleanReservationBasis(now), now)
	if err != nil {
		t.Fatal(err)
	}
	sendAndImport(t, engine, store, reader, intent, 401, "0.001", "100000", "0", "0")
	now = time.Now().UTC()
	snap := accountSnapshot(now, "0.001", "0")
	snap.Balances = []exchange.Balance{{Asset: "USDT", Wallet: "1000", Available: "1000"}}
	if err := c.Settle(res, store, snap, observations(reader, now), now); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "trading", "reservations", res.ClientOrderID+".done.json")
	raw, _ := os.ReadFile(path)
	raw = []byte(strings.Replace(string(raw), `"owner_remaining_qty": "0.001"`, `"owner_remaining_qty": "0.002"`, 1))
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenCoordinator(root); err == nil {
		t.Fatal("tampered settlement attestation did not fail closed")
	}
}

// directoryRegularFileDigests is test-only and deliberately metadata-light.
func directoryRegularFileDigests(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, err
		}
		out = append(out, e.Name()+":"+string(b))
	}
	return out, nil
}

func TestLegacySchema1DoneMarkerRemainsReadable(t *testing.T) {
	d := t.TempDir()
	c, err := OpenCoordinator(d)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	intent := modelIntent("legacy-done-e01", "A", Long, Enter, "0.001")
	res, _, err := c.reservePreview(intent, cleanReservationBasis(now), now)
	if err != nil {
		t.Fatal(err)
	}
	legacy := map[string]any{
		"schema":               1,
		"client_order_id":      res.ClientOrderID,
		"reservation_checksum": res.Checksum,
		"settled_at":           now.Format(time.RFC3339Nano),
	}
	raw, _ := json.MarshalIndent(legacy, "", "  ")
	path := filepath.Join(d, "trading", "reservations", res.ClientOrderID+".done.json")
	if err := os.WriteFile(path, append(raw, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenCoordinator(d)
	if err != nil {
		t.Fatalf("legacy schema-1 done marker became unreadable: %v", err)
	}
	defer reopened.Close()
	review, err := ReviewPooledExitSerialization(d)
	if err != nil || review.CompletedReservations != 1 || review.ActiveReservations != 0 {
		t.Fatalf("legacy done review %+v %v", review, err)
	}
}
