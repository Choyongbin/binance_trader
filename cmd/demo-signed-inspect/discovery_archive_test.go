package main

import (
	"context"
	"testing"
	"time"

	"binance_trader/internal/exchange"
	"binance_trader/internal/trading"
)

// These tests verify the correlation boundary. Nested incident archive source
// validation is covered by trading's independent LONG/SHORT journal fixtures.
func bundleFixture(t *testing.T, side string) packageADiscoveryArchive {
	t.Helper()
	now := time.Date(2026, 10, 22, 7, 0, 0, 0, time.UTC)
	entry, reader := makeExitEvidence(side, now)
	found := discoverExitOrder(context.Background(), reader, trading.DemoCredentials{APIKey: "MOCK", Secret: "MOCK"}, entry, now)
	if found.Status != "UNIQUE_EXIT_ORDER_ID_CANDIDATE_REVIEW_ONLY" {
		t.Fatalf("bad fixture: %+v", found)
	}
	close := trading.SignedCloseEvidence{EntryOrderID: entry.Order.OrderID, ExitOrderID: 200,
		EntryOrder: entry.Order, ExitOrder: reader.orders[200], EntryFills: entry.Fills,
		ExitFills:     []exchange.AccountTrade{reader.trades[1]},
		HistoryFromMS: entry.Fills[0].TimeMS, HistoryToMS: reader.trades[1].TimeMS}
	inc := trading.PackageAIncidentArchive{Schema: 1, ClientOrderID: entry.Order.ClientOrderID,
		Entry: entry, Close: close}
	b, err := makePackageADiscoveryArchive(found, inc)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func TestDiscoveryArchiveLinkageLongShort(t *testing.T) {
	for _, side := range []string{"SHORT", "LONG"} {
		t.Run(side, func(t *testing.T) {
			b := bundleFixture(t, side)
			if err := bundleIdentityCheck(b); err != nil {
				t.Fatal(err)
			}
			if b.TradingAuthorized || b.SettlementAuthorized || b.OperatorExitConfirmed || b.LifecycleChanged || b.OwnershipChanged {
				t.Fatal("unsafe archived settlement flags")
			}
		})
	}
}
func TestDiscoveryArchiveFailsClosedEvenWithRecomputedChecksum(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*packageADiscoveryArchive)
	}{
		{"second_candidate", func(b *packageADiscoveryArchive) {
			b.Discovery.Candidates = append(b.Discovery.Candidates, b.Discovery.Candidates[0])
		}},
		{"wrong_exit_order", func(b *packageADiscoveryArchive) { b.Discovery.Candidates[0].OrderID++ }},
		{"wrong_side", func(b *packageADiscoveryArchive) { b.Discovery.PositionSide = "LONG" }},
		{"wrong_trade", func(b *packageADiscoveryArchive) { b.Discovery.Candidates[0].TradeIDs[0]++ }},
		{"wrong_qty", func(b *packageADiscoveryArchive) { b.Discovery.Candidates[0].Quantity = "0.002" }},
		{"truncated_history", func(b *packageADiscoveryArchive) { b.Discovery.BoundedHistoryRead = false }},
		{"other_trade", func(b *packageADiscoveryArchive) { b.Discovery.UnexpectedTradeIDs = []int64{999} }},
		{"operator_confirmed", func(b *packageADiscoveryArchive) { b.OperatorExitConfirmed = true }},
		{"false_settlement", func(b *packageADiscoveryArchive) { b.SettlementAuthorized = true }},
		{"false_account_coverage", func(b *packageADiscoveryArchive) { b.Discovery.FullAccountHistoryVerified = true }},
		{"missing_blockers", func(b *packageADiscoveryArchive) { b.Discovery.Blockers = nil }},
		{"time_window", func(b *packageADiscoveryArchive) { b.Discovery.HistoryToUTC = "2026-10-21T00:00:00Z" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := bundleFixture(t, "SHORT")
			tc.mutate(&b)
			b.Checksum = b.digest()
			if err := bundleIdentityCheck(b); err == nil {
				t.Fatal("accepted unsafe candidate/archive correlation")
			}
		})
	}
}
func TestDiscoveryArchiveMissingCandidateDoesNotConstructBundle(t *testing.T) {
	now := time.Date(2026, 10, 22, 7, 0, 0, 0, time.UTC)
	entry, reader := makeExitEvidence("SHORT", now)
	reader.trades = reader.trades[:1]
	d := discoverExitOrder(context.Background(), reader, trading.DemoCredentials{APIKey: "key", Secret: "secret"}, entry, now)
	if _, err := makePackageADiscoveryArchive(d, trading.PackageAIncidentArchive{ClientOrderID: entry.Order.ClientOrderID}); err == nil {
		t.Fatal("accepted missing exit")
	}
}
