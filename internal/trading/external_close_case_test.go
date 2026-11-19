package trading

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

func externalCloseFixture(t *testing.T, side PositionSide) (string, *ledgerMock, exchange.AccountTrade) {
	t.Helper()
	if side == Short {
		d, m, entry := makeLedgerTest(t)
		m.snapshot = accountSnapshot(time.Now().UTC(), "0", "0")
		addCloseToLedgerFixture(m, entry, side)
		return d, m, entry
	}
	d := t.TempDir()
	engine, err := NewEngine(d, &uniqueOrders{next: 700})
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewOwnershipStore(d)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(d, "trading", "reservations"), 0700); err != nil {
		t.Fatal(err)
	}
	intent := makeIntent("ledger-review-long", Model, "ema_rsi_macd_v1", Long, Enter, "0.001")
	rec, err := engine.Submit(context.Background(), intent, allow)
	if err != nil {
		t.Fatal(err)
	}
	order := orderFromRecord(rec, "FILLED", "0.001")
	entry := receipt(rec, 901, "0.001", "80000", "0.03", "0", time.Now().UTC().Add(-30*time.Minute).UnixMilli())
	remote := &readerOrders{orders: map[string]exchange.OrderResult{rec.Plan.ClientOrderID: order}, trades: map[int64][]exchange.AccountTrade{order.OrderID: {entry}}}
	if n, err := store.ImportDemoOrder(context.Background(), remote, DemoCredentials{APIKey: "mockkey", Secret: "mocksecret"}, intent); err != nil || n != 1 {
		t.Fatalf("import long: %d %v", n, err)
	}
	m := &ledgerMock{readerOrders: remote, history: []exchange.AccountTrade{entry}, snapshot: accountSnapshot(time.Now().UTC(), "0", "0")}
	addCloseToLedgerFixture(m, entry, side)
	return d, m, entry
}

func addCloseToLedgerFixture(m *ledgerMock, entry exchange.AccountTrade, side PositionSide) {
	direction := "BUY"
	if side == Long {
		direction = "SELL"
	}
	close := exchange.AccountTrade{Symbol: "BTCUSDT", TradeID: 902, OrderID: 899, Side: direction, PositionSide: string(side), Quantity: "0.001", Price: "79900", Commission: "0.025", CommissionAsset: "USDT", RealizedPnL: "1.25", TimeMS: entry.TimeMS + 120000}
	m.history = append(m.history, close)
	m.externalOrders = map[int64]exchange.OrderResult{899: {Symbol: "BTCUSDT", OrderID: 899, Side: direction, PositionSide: string(side), Status: "FILLED", ExecutedQty: "0.001", OriginalQty: "0.001"}}
}

func TestExternalCloseForensicCandidatesLongShortReadOnly(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			d, m, _ := externalCloseFixture(t, side)
			before, err := snapshotExecutionEvidence(d)
			if err != nil {
				t.Fatal(err)
			}
			r := reviewFixture(d, m)
			after, err := snapshotExecutionEvidence(d)
			if err != nil {
				t.Fatal(err)
			}
			if before != after || len(r.ExternalCloseCases) != 1 {
				t.Fatalf("evidence mutation or missing case: %+v", r)
			}
			c := r.ExternalCloseCases[0]
			if c.Status != ExternalCloseCandidate || c.ExitOrderID != 899 || c.EntryOrderID != 701 || c.PositionSide != side || c.EntryQuantity != "0.001" || c.ExitQuantity != "0.001" || c.SignedRealizedPnL != "1.25" || c.SignedExitCommission != "0.025" || c.CommissionAsset != "USDT" || !c.LocalEntryFillsMatched || !c.HistoricalCoverageLimited || c.TradingAuthorized || c.LocalLifecycleChanged || c.LocalOwnershipChanged || r.TradingAuthorized || r.State != "BLOCKED_REVIEW_REQUIRED" {
				t.Fatalf("invalid candidate: %+v report %+v", c, r)
			}
		})
	}
}

func TestExternalCloseCandidateWithMissingLocalFillNeverSettles(t *testing.T) {
	d, m, _ := externalCloseFixture(t, Short)
	entries, err := os.ReadDir(filepath.Join(d, "trading", "fills"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("fills %v %v", entries, err)
	}
	if err := os.Remove(filepath.Join(d, "trading", "fills", entries[0].Name())); err != nil {
		t.Fatal(err)
	}
	r := reviewFixture(d, m)
	if len(r.ExternalCloseCases) != 1 {
		t.Fatal(r)
	}
	c := r.ExternalCloseCases[0]
	if c.Status != ExternalCloseCandidate || c.LocalEntryFillsMatched || r.LocalFillsMatchSigned || r.TradingAuthorized || !strings.Contains(strings.Join(c.Blockers, " "), "locally imported") {
		t.Fatalf("missing local evidence must remain blocked: %+v / %+v", c, r)
	}
}

func TestExternalCloseRejectsAmbiguityAndContradictions(t *testing.T) {
	for _, scenario := range []string{"wrong-close-side", "wrong-position-side", "partial-close", "over-close", "other-external", "nonflat-signed", "stale-signed", "history-incomplete", "external-unverified", "negative-fee", "different-fee-asset", "close-before-entry", "entry-not-filled", "external-second-drift", "local-changed-during-review"} {
		t.Run(scenario, func(t *testing.T) {
			d, m, entry := externalCloseFixture(t, Short)
			switch scenario {
			case "wrong-close-side":
				m.history[1].Side = "SELL"
				v := m.externalOrders[899]
				v.Side = "SELL"
				m.externalOrders[899] = v
			case "wrong-position-side":
				m.history[1].PositionSide = "LONG"
				v := m.externalOrders[899]
				v.PositionSide = "LONG"
				m.externalOrders[899] = v
			case "partial-close":
				m.history[1].Quantity = "0.0005"
				v := m.externalOrders[899]
				v.ExecutedQty = "0.0005"
				v.OriginalQty = "0.0005"
				m.externalOrders[899] = v
			case "over-close":
				m.history[1].Quantity = "0.002"
				v := m.externalOrders[899]
				v.ExecutedQty = "0.002"
				v.OriginalQty = "0.002"
				m.externalOrders[899] = v
			case "other-external":
				m.history = append(m.history, exchange.AccountTrade{Symbol: "BTCUSDT", TradeID: 903, OrderID: 999, Side: "BUY", PositionSide: "SHORT", Quantity: "0.0001", Price: "80000", Commission: "0.01", CommissionAsset: "USDT", RealizedPnL: "0", TimeMS: entry.TimeMS + 130000})
			case "nonflat-signed":
				m.snapshot = accountSnapshot(time.Now().UTC(), "0", "0.001")
			case "stale-signed":
				m.snapshot.AsOf = time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)
			case "history-incomplete":
				m.historyErr = context.Canceled
			case "external-unverified":
				delete(m.externalOrders, 899)
			case "negative-fee":
				m.history[1].Commission = "-0.02"
			case "different-fee-asset":
				m.history[1].CommissionAsset = "BNB"
			case "close-before-entry":
				m.history[1].TimeMS = entry.TimeMS - 5000
			case "external-second-drift":
				m.externalSecondDrift = true
			case "local-changed-during-review":
				m.tamperDir = d
			case "entry-not-filled":
				for id, order := range m.readerOrders.orders {
					order.Status = "PARTIALLY_FILLED"
					m.readerOrders.orders[id] = order
				}
			}
			r := reviewFixture(d, m)
			if len(r.ExternalCloseCases) != 1 || r.ExternalCloseCases[0].Status != ExternalCloseAmbiguous || r.TradingAuthorized || r.ExternalCloseCases[0].TradingAuthorized {
				t.Fatalf("unsafe scenario %s got candidate: %+v", scenario, r)
			}
		})
	}
}
