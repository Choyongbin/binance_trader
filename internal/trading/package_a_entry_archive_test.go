package trading

import (
	"binance_trader/internal/exchange"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPackageAEntryArchiveCaptureVerifyAndTamper(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			data, fixture, _ := externalCloseFixture(t, side)
			recs, err := readJournalForEntryGapTest(data)
			if err != nil || len(recs) != 1 {
				t.Fatalf("journal %v %v", recs, err)
			}
			id := recs[0].Plan.ClientOrderID
			before, err := snapshotExecutionEvidence(data)
			if err != nil {
				t.Fatal(err)
			}
			archived, err := CapturePackageAEntryArchive(context.Background(), data, id, fixture.readerOrders, DemoCredentials{APIKey: "mock", Secret: "mock"}, time.Now().UTC())
			if err != nil {
				t.Fatalf("capture: %v", err)
			}
			if archived.SettlementAuthorized || archived.TradingAuthorized || archived.Checksum == "" || archived.Order.ClientOrderID != id || len(archived.Fills) != 1 {
				t.Fatalf("unsafe/archive invalid %+v", archived)
			}
			if err = VerifyPackageAEntryArchive(context.Background(), data, archived); err != nil {
				t.Fatalf("verify: %v", err)
			}
			after, err := snapshotExecutionEvidence(data)
			if err != nil || before != after {
				t.Fatalf("capture/verify mutated source: %v", err)
			}
			modified := archived
			modified.Fills = append([]exchange.AccountTrade(nil), archived.Fills...)
			modified.Fills[0].Price = "999999"
			if err = VerifyPackageAEntryArchive(context.Background(), data, modified); err == nil {
				t.Fatal("tampered archive accepted")
			}
			modified = archived
			modified.TradingAuthorized = true
			modified.Checksum = modified.digest()
			if err = VerifyPackageAEntryArchive(context.Background(), data, modified); err == nil {
				t.Fatal("trading authorization laundering accepted")
			}
			if err = os.Remove(filepath.Join(data, "trading", "fills", "BTCUSDT_901.json")); err != nil {
				t.Fatal(err)
			}
			if err = VerifyPackageAEntryArchive(context.Background(), data, archived); err == nil {
				t.Fatal("local journal drift accepted")
			}
		})
	}
}

func TestPackageAEntryArchiveMissingFillIsReadOnly(t *testing.T) {
	data, fixture, _ := externalCloseFixture(t, Short)
	recs, err := readJournalForEntryGapTest(data)
	if err != nil {
		t.Fatal(err)
	}
	id := recs[0].Plan.ClientOrderID
	if err = os.Remove(filepath.Join(data, "trading", "fills", "BTCUSDT_901.json")); err != nil {
		t.Fatal(err)
	}
	prior, err := snapshotExecutionEvidence(data)
	if err != nil {
		t.Fatal(err)
	}
	a, err := CapturePackageAEntryArchive(context.Background(), data, id, fixture.readerOrders, DemoCredentials{APIKey: "mock", Secret: "mock"}, time.Now())
	if err != nil || len(a.Gap.MissingLocalTradeIDs) != 1 || a.Gap.MissingLocalTradeIDs[0] != 901 {
		t.Fatalf("missing fill archive: %+v %v", a.Gap, err)
	}
	if err = VerifyPackageAEntryArchive(context.Background(), data, a); err != nil {
		t.Fatal(err)
	}
	next, err := snapshotExecutionEvidence(data)
	if err != nil || prior != next {
		t.Fatal("original journal mutated")
	}
}
