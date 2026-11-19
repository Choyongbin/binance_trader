package trading

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestPackageAEntryGapDoesNotMutateMissingOriginalFills(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			dir, m, _ := externalCloseFixture(t, side)
			records, e := readJournalForEntryGapTest(dir)
			if e != nil || len(records) != 1 {
				t.Fatalf("journal %+v %v", records, e)
			}
			id := records[0].Plan.ClientOrderID
			original, _ := snapshotExecutionEvidence(dir)
			r, e := AuditPackageAEntryGap(context.Background(), dir, id, m.readerOrders, DemoCredentials{APIKey: "mock", Secret: "mock"})
			if e != nil || len(r.AlreadyImportedIDs) != 1 || len(r.MissingLocalTradeIDs) != 0 || r.TradingAuthorized || r.SettlementAuthorized {
				t.Fatalf("full entry %+v %v", r, e)
			}
			after, _ := snapshotExecutionEvidence(dir)
			if after != original {
				t.Fatal("source evidence modified")
			}
			if e := os.Remove(filepath.Join(dir, "trading", "fills", "BTCUSDT_901.json")); e != nil {
				t.Fatal(e)
			}
			before, _ := snapshotExecutionEvidence(dir)
			r, e = AuditPackageAEntryGap(context.Background(), dir, id, m.readerOrders, DemoCredentials{APIKey: "mock", Secret: "mock"})
			if e != nil || len(r.MissingLocalTradeIDs) != 1 || r.MissingLocalTradeIDs[0] != 901 || len(r.AlreadyImportedIDs) != 0 || r.TradingAuthorized {
				t.Fatalf("missing entry %+v %v", r, e)
			}
			after, _ = snapshotExecutionEvidence(dir)
			if after != before {
				t.Fatal("gap audit wrote original ledger")
			}
			m.readerOrders.trades[r.SignedOrderID][0].Quantity = "0.002"
			if _, e = AuditPackageAEntryGap(context.Background(), dir, id, m.readerOrders, DemoCredentials{APIKey: "mock", Secret: "mock"}); e == nil {
				t.Fatal("signed quantity drift accepted")
			}
		})
	}
}
func readJournalForEntryGapTest(dir string) ([]Record, error) {
	s, e := NewOwnershipStore(dir)
	if e != nil {
		return nil, e
	}
	return s.ScanJournal()
}
