package trading

import (
	"binance_trader/internal/exchange"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func incidentFixture(t *testing.T, side PositionSide) (string, string, *ledgerMock, SignedCloseEvidence) {
	t.Helper()
	data, m, _ := externalCloseFixture(t, side)
	records, err := readJournalForEntryGapTest(data)
	if err != nil || len(records) != 1 {
		t.Fatal(err)
	}
	id := records[0].Plan.ClientOrderID
	entry := m.readerOrders.orders[id]
	exit := m.externalOrders[899]
	closeFills := []exchange.AccountTrade{m.history[1]}
	entryFills := []exchange.AccountTrade{m.history[0]}
	details := struct {
		EntryOrder, ExitOrder          exchange.OrderResult
		EntryFills, ExitFills, History []exchange.AccountTrade
		Funding                        []exchange.FundingIncome
	}{entry, exit, entryFills, closeFills, append([]exchange.AccountTrade(nil), m.history...), []exchange.FundingIncome{}}
	ev := SignedCloseEvidence{
		EntryOrder: entry, ExitOrder: exit, HistoricalTrades: append([]exchange.AccountTrade(nil), m.history...),
		EntryFills: entryFills, ExitFills: closeFills, FundingRows: []exchange.FundingIncome{},
		EntryOrderID: entry.OrderID, ExitOrderID: 899, EntryTradeIDs: []int64{901}, ExitTradeIDs: []int64{902},
		AccountFlat: true, ObservedAt: time.Now().UTC(), SignedDetailSHA256: externalCloseHash(details),
		BoundedFundingRows: 0, BoundedFundingNetUSDT: "0", HistoryFromMS: entryFills[0].TimeMS,
		HistoryToMS: closeFills[0].TimeMS, BoundedTradeRows: 2, TradeWindowsRead: 1, FundingWindowsRead: 1,
		HistoryScope: "ENTRY_FIRST_FILL_TO_EXIT_LAST_FILL_ONLY",
	}
	return data, id, m, ev
}
func TestPackageAIncidentArchiveLongShortAndTamper(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			data, id, m, ev := incidentFixture(t, side)
			start, err := snapshotExecutionEvidence(data)
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC()
			a, err := CapturePackageAIncidentArchive(context.Background(), data, id, 899, m, mockCloseReader{v: ev}, DemoCredentials{APIKey: "key", Secret: "secret"}, now)
			if err != nil {
				t.Fatal(err)
			}
			if a.TradingAuthorized || a.SettlementAuthorized || len(a.Close.HistoricalTrades) != 2 {
				t.Fatal("unsafe archive")
			}
			if err = VerifyPackageAIncidentArchive(context.Background(), data, a); err != nil {
				t.Fatal(err)
			}
			next, _ := snapshotExecutionEvidence(data)
			if start != next {
				t.Fatal("modified original ledger")
			}
			bad := a
			bad.Close.FundingVerified = true
			bad.Checksum = bad.digest()
			if VerifyPackageAIncidentArchive(context.Background(), data, bad) == nil {
				t.Fatal("false funding verified accepted")
			}
			bad = a
			bad.Close.HistoricalTrades = bad.Close.HistoricalTrades[:1]
			bad.Checksum = bad.digest()
			if VerifyPackageAIncidentArchive(context.Background(), data, bad) == nil {
				t.Fatal("missing signed history accepted")
			}
			bad = a
			bad.Close.ExitFills = append([]exchange.AccountTrade(nil), a.Close.ExitFills...)
			bad.Close.ExitFills[0].Commission = "999"
			bad.Checksum = bad.digest()
			if VerifyPackageAIncidentArchive(context.Background(), data, bad) == nil {
				t.Fatal("fee tampering accepted")
			}
			bad = a
			bad.TradingAuthorized = true
			bad.Checksum = bad.digest()
			if VerifyPackageAIncidentArchive(context.Background(), data, bad) == nil {
				t.Fatal("forged trading authorization accepted")
			}
			if err = os.Remove(filepath.Join(data, "trading", "fills", "BTCUSDT_901.json")); err != nil {
				t.Fatal(err)
			}
			if VerifyPackageAIncidentArchive(context.Background(), data, a) == nil {
				t.Fatal("local drift accepted")
			}
		})
	}
}
func TestPackageAIncidentArchiveMissingLocalEntryReadOnly(t *testing.T) {
	data, id, m, ev := incidentFixture(t, Short)
	if err := os.Remove(filepath.Join(data, "trading", "fills", "BTCUSDT_901.json")); err != nil {
		t.Fatal(err)
	}
	before, _ := snapshotExecutionEvidence(data)
	a, err := CapturePackageAIncidentArchive(context.Background(), data, id, 899, m, mockCloseReader{v: ev}, DemoCredentials{APIKey: "key", Secret: "secret"}, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if len(a.Entry.Gap.MissingLocalTradeIDs) != 1 || a.Entry.Gap.MissingLocalTradeIDs[0] != 901 {
		t.Fatal("missing local fill not recorded")
	}
	after, _ := snapshotExecutionEvidence(data)
	if before != after {
		t.Fatal("read-only capture changed source")
	}
	if err = VerifyPackageAIncidentArchive(context.Background(), data, a); err != nil {
		t.Fatal(err)
	}
}
func TestPackageAIncidentArchiveRejectsUnverifiedSnapshot(t *testing.T) {
	for _, kind := range []string{"extra history", "not flat", "wrong order", "wrong history scope", "stale", "funding mismatch"} {
		t.Run(kind, func(t *testing.T) {
			data, id, m, ev := incidentFixture(t, Long)
			switch kind {
			case "extra history":
				ev.HistoricalTrades = append(ev.HistoricalTrades, ev.HistoricalTrades[0])
			case "not flat":
				ev.AccountFlat = false
			case "wrong order":
				ev.ExitOrder.Side = "BUY"
			case "wrong history scope":
				ev.HistoryScope = "LIFETIME"
			case "stale":
				ev.ObservedAt = time.Now().Add(-time.Hour)
			case "funding mismatch":
				ev.BoundedFundingNetUSDT = "0.1"
			}
			if _, err := CapturePackageAIncidentArchive(context.Background(), data, id, 899, m, mockCloseReader{v: ev}, DemoCredentials{APIKey: "key", Secret: "secret"}, time.Now().UTC()); err == nil {
				t.Fatal("accepted unsafe signed evidence")
			}
		})
	}
}

// Regression: dev.3.88 captured valid non-nil empty funding rows, omitted them
// from the archived JSON, and then rejected the exact same signed detail after
// reading the file back. The original immutable archive must remain readable.
func TestDev389EmptyFundingArchiveRoundTripAndIntegrity(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			data, id, m, ev := incidentFixture(t, side)
			if ev.FundingRows == nil || len(ev.FundingRows) != 0 {
				t.Fatal("fixture must use non-nil zero funding")
			}
			archive, err := CapturePackageAIncidentArchive(context.Background(), data, id, 899, m, mockCloseReader{v: ev}, DemoCredentials{APIKey: "key", Secret: "secret"}, time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}
			raw, err := json.MarshalIndent(archive, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			var reread PackageAIncidentArchive
			if err := json.Unmarshal(raw, &reread); err != nil {
				t.Fatal(err)
			}
			if reread.Close.FundingRows != nil {
				t.Fatal("test did not reproduce omitted empty funding")
			}
			if err := VerifyPackageAIncidentArchive(context.Background(), data, reread); err != nil {
				t.Fatalf("unaltered saved archive rejected: %v", err)
			}
			bad := reread
			bad.Close.SignedDetailSHA256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			bad.Checksum = bad.digest()
			if err := VerifyPackageAIncidentArchive(context.Background(), data, bad); err == nil {
				t.Fatal("invalid independent signed detail digest accepted")
			}
			bad = reread
			bad.Close.BoundedFundingRows = 1
			bad.Checksum = bad.digest()
			if err := VerifyPackageAIncidentArchive(context.Background(), data, bad); err == nil {
				t.Fatal("incorrect funding row count accepted")
			}
			bad = reread
			bad.Close.EntryFills = append([]exchange.AccountTrade(nil), reread.Close.EntryFills...)
			bad.Close.EntryFills[0].Commission = "0.777"
			bad.Checksum = bad.digest()
			if err := VerifyPackageAIncidentArchive(context.Background(), data, bad); err == nil {
				t.Fatal("tampered fee accepted")
			}
		})
	}
}
