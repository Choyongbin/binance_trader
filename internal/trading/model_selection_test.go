package trading

import (
	"binance_trader/internal/exchange"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func flatSignedModelSelectionSnapshot(now time.Time) exchange.Snapshot {
	return exchange.Snapshot{Environment: "demo", Status: "connected", AsOf: now.UTC().Format(time.RFC3339Nano), Balances: []exchange.Balance{{Asset: "USDT", Available: "1000"}}, Positions: []exchange.Position{}, Orders: []exchange.Order{}, AlgoOrders: []exchange.AlgoOrder{}}
}
func TestModelSelectionGuardFailClosed(t *testing.T) {
	now := time.Now().UTC()
	for _, tc := range []struct {
		name   string
		change func(*exchange.Snapshot)
	}{
		{"flat", func(*exchange.Snapshot) {}},
		{"open long", func(s *exchange.Snapshot) {
			s.Positions = []exchange.Position{{Symbol: "BTCUSDT", Side: "LONG", Qty: "0.001"}}
		}},
		{"open short", func(s *exchange.Snapshot) {
			s.Positions = []exchange.Position{{Symbol: "BTCUSDT", Side: "SHORT", Qty: "-0.001"}}
		}},
		{"ordinary order", func(s *exchange.Snapshot) { s.Orders = []exchange.Order{{Symbol: "BTCUSDT"}} }},
		{"algo order", func(s *exchange.Snapshot) { s.AlgoOrders = []exchange.AlgoOrder{{Symbol: "BTCUSDT"}} }},
		{"unknown positions", func(s *exchange.Snapshot) { s.Positions = nil }},
		{"stale", func(s *exchange.Snapshot) { s.AsOf = now.Add(-time.Minute).Format(time.RFC3339Nano) }},
		{"real", func(s *exchange.Snapshot) { s.Environment = "real" }},
		{"bad qty", func(s *exchange.Snapshot) {
			s.Positions = []exchange.Position{{Symbol: "BTCUSDT", Side: "SHORT", Qty: "not-a-number"}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			s := flatSignedModelSelectionSnapshot(now)
			tc.change(&s)
			err := VerifyModelSelectionSafe(dir, s, now)
			if tc.name == "flat" && err != nil {
				t.Fatalf("flat rejected: %v", err)
			}
			if tc.name != "flat" && err == nil {
				t.Fatal("unsafe change accepted")
			}
		})
	}
}
func TestModelSelectionGuardPendingLifecycleAndUntrustedFiles(t *testing.T) {
	now := time.Now().UTC()
	dir := t.TempDir()
	path := filepath.Join(dir, "trading", "execution_lifecycle")
	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatal(err)
	}
	// Any unexpected file must block, not be silently ignored.
	if err := os.WriteFile(filepath.Join(path, "unexpected.json"), []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := VerifyModelSelectionSafe(dir, flatSignedModelSelectionSnapshot(now), now); err == nil || !strings.Contains(err.Error(), "감사") {
		t.Fatalf("tampering accepted: %v", err)
	}
}

func TestModelSelectionGuardActiveCoordinator(t *testing.T) {
	dir := t.TempDir()
	now := time.Now().UTC()
	lock := filepath.Join(dir, "trading", "coordinator.active")
	if err := os.MkdirAll(lock, 0700); err != nil {
		t.Fatal(err)
	}
	if err := VerifyModelSelectionSafe(dir, flatSignedModelSelectionSnapshot(now), now); err == nil {
		t.Fatal("active coordinator accepted")
	}
}
func TestModelSelectionGuardUnexpectedReservation(t *testing.T) {
	dir := t.TempDir()
	now := time.Now().UTC()
	path := filepath.Join(dir, "trading", "reservations")
	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "unexpected.json"), []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := VerifyModelSelectionSafe(dir, flatSignedModelSelectionSnapshot(now), now); err == nil {
		t.Fatal("unexpected reservation accepted")
	}
}
