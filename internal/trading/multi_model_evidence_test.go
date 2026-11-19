package trading

import (
	"binance_trader/internal/exchange"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func cleanEvidenceTree(t *testing.T) string {
	t.Helper()
	d := t.TempDir()
	for _, name := range []string{"trading/reservations", "trading/requests", "trading/fills"} {
		if err := os.MkdirAll(filepath.Join(d, filepath.FromSlash(name)), 0700); err != nil {
			t.Fatal(err)
		}
	}
	return d
}
func assertEvidenceLocked(t *testing.T, s MultiModelEvidenceStatus) {
	t.Helper()
	if s.HistoricalTradesVerified || s.OwnershipVerified || s.RiskPreviewReady || s.TradingAuthorized || s.ExecutionEnabled {
		t.Fatalf("unapproved trading flags: %+v", s)
	}
}
func TestMultiModelEvidenceFlatSnapshotNeverAuthorizesHistory(t *testing.T) {
	now := time.Now().UTC()
	d := cleanEvidenceTree(t)
	s := InspectMultiModelEvidence(d, accountSnapshot(now, "0", "0"), now)
	assertEvidenceLocked(t, s)
	if s.State != "CURRENT_SNAPSHOT_CONSISTENT_HISTORY_UNVERIFIED" || !s.LocalSnapshotConsistent || !s.SignedAccountFresh || len(s.Blockers) != 0 || s.LocalOrders != 0 || s.ActiveReservations != 0 {
		t.Fatalf("clean snapshot: %+v", s)
	}
	// Read-only inspection creates no files or missing lifecycle dir.
	if _, err := os.Lstat(filepath.Join(d, "trading", "execution_lifecycle")); !os.IsNotExist(err) {
		t.Fatalf("unexpected audit write: %v", err)
	}
}
func TestMultiModelEvidenceSignedShortWithoutOwnerBlocks(t *testing.T) {
	now := time.Now().UTC()
	d := cleanEvidenceTree(t)
	s := InspectMultiModelEvidence(d, accountSnapshot(now, "0", "0.001"), now)
	assertEvidenceLocked(t, s)
	if s.LocalSnapshotConsistent || s.State != "BLOCKED_MANUAL_REVIEW_REQUIRED" || s.SignedShortQty != "0.001" || len(s.Blockers) == 0 {
		t.Fatalf("external SHORT must block: %+v", s)
	}
}
func TestMultiModelEvidenceStaleRealOrIncompleteBlocks(t *testing.T) {
	now := time.Now().UTC()
	d := cleanEvidenceTree(t)
	for _, tc := range []struct {
		name           string
		snapshotOffset time.Duration
		real           bool
		nilOrders      bool
	}{
		{name: "stale", snapshotOffset: -10 * time.Second}, {name: "future", snapshotOffset: 10 * time.Second}, {name: "real", real: true}, {name: "incomplete", nilOrders: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snap := accountSnapshot(now.Add(tc.snapshotOffset), "0", "0")
			if tc.real {
				snap.Environment = "real"
			}
			if tc.nilOrders {
				snap.Orders = nil
			}
			s := InspectMultiModelEvidence(d, snap, now)
			assertEvidenceLocked(t, s)
			if s.LocalSnapshotConsistent || s.State != "BLOCKED_MANUAL_REVIEW_REQUIRED" || len(s.Blockers) == 0 {
				t.Fatal(s)
			}
		})
	}
}
func TestMultiModelEvidenceLocalUnknownRecordAndLockBlocks(t *testing.T) {
	now := time.Now().UTC()
	d := cleanEvidenceTree(t)
	if err := os.WriteFile(filepath.Join(d, "trading", "requests", "unknown.json"), []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(d, "trading", "coordinator.active"), 0700); err != nil {
		t.Fatal(err)
	}
	s := InspectMultiModelEvidence(d, accountSnapshot(now, "0", "0"), now)
	assertEvidenceLocked(t, s)
	if s.LocalSnapshotConsistent || len(s.Blockers) == 0 {
		t.Fatal(s)
	}
	got := strings.Join(s.Blockers, " | ")
	if !strings.Contains(got, "잠금") || !strings.Contains(got, "저널") && !strings.Contains(got, "기록") {
		t.Fatal(got)
	}
}
func TestMultiModelEvidenceMissingOrSymlinkEvidenceBlockedWithoutCreation(t *testing.T) {
	now := time.Now().UTC()
	d := t.TempDir()
	s := InspectMultiModelEvidence(d, accountSnapshot(now, "0", "0"), now)
	assertEvidenceLocked(t, s)
	if s.LocalSnapshotConsistent || len(s.Blockers) == 0 {
		t.Fatal(s)
	}
	if _, err := os.Stat(filepath.Join(d, "trading")); !os.IsNotExist(err) {
		t.Fatalf("must not create data tree: %v", err)
	}
	d = cleanEvidenceTree(t)
	if err := os.Remove(filepath.Join(d, "trading", "fills")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(d, "trading", "fills")); err != nil {
		t.Skipf("symlink unsupported: %v", err)
	}
	s = InspectMultiModelEvidence(d, accountSnapshot(now, "0", "0"), now)
	assertEvidenceLocked(t, s)
	if s.LocalSnapshotConsistent || !strings.Contains(strings.Join(s.Blockers, " "), "안전하지") {
		t.Fatalf("symlink must block: %+v", s)
	}
}
func TestMultiModelEvidenceIndependentlySignedOrderRequired(t *testing.T) {
	now := time.Now().UTC()
	d := t.TempDir()
	e, err := NewEngine(d, &uniqueOrders{next: 300})
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewOwnershipStore(d)
	if err != nil {
		t.Fatal(err)
	}
	r := &readerOrders{orders: make(map[string]exchange.OrderResult), trades: make(map[int64][]exchange.AccountTrade)}
	sendAndImport(t, e, store, r, makeIntent("existing-short-entry", Model, "ema_rsi_macd_v1", Short, Enter, "0.001"), 42, "0.001", "80000", "0.03", "0")
	// Signed account is now flat due to manual close, but the local ownership
	// ledger still contains the SHORT. No separate signed order observation is
	// supplied and this function is forbidden from treating it as reconciled.
	s := InspectMultiModelEvidence(d, accountSnapshot(now, "0", "0"), now)
	assertEvidenceLocked(t, s)
	if s.LocalSnapshotConsistent || s.State != "BLOCKED_MANUAL_REVIEW_REQUIRED" || s.LocalOrders != 1 || s.LocalFills != 1 || len(s.ModelLocalExposures) != 1 || s.ModelLocalExposures[0].ModelID != "ema_rsi_macd_v1" {
		t.Fatalf("manual close false-positive: %+v", s)
	}
}
