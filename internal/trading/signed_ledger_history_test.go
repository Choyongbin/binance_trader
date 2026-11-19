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
)

type ledgerMock struct {
	*readerOrders
	history             []exchange.AccountTrade
	snapshot            exchange.Snapshot
	historyErr          error
	tamperDir           string
	historyReads        int
	externalOrders      map[int64]exchange.OrderResult
	externalReadCount   int
	externalSecondDrift bool
	calls               int
	drift               bool
}

func (m *ledgerMock) QueryDemoOrder(ctx context.Context, key, secret, symbol, id string) (exchange.OrderResult, error) {
	m.calls++
	o, e := m.readerOrders.QueryDemoOrder(ctx, key, secret, symbol, id)
	if m.drift && m.calls%2 == 0 {
		o.Status = "PARTIALLY_FILLED"
	}
	return o, e
}
func (m *ledgerMock) QueryDemoOrderByID(_ context.Context, _, _, _ string, id int64) (exchange.OrderResult, error) {
	if v, ok := m.externalOrders[id]; ok {
		m.externalReadCount++
		if m.externalSecondDrift && m.externalReadCount%2 == 0 {
			v.PositionSide = "LONG"
		}
		return v, nil
	}
	return exchange.OrderResult{}, errors.New("not found")
}
func (m *ledgerMock) FetchDemoTradeWindow(_ context.Context, _, _, _ string, start, end int64) ([]exchange.AccountTrade, error) {
	m.historyReads++
	if m.historyErr != nil {
		return nil, m.historyErr
	}
	ret := []exchange.AccountTrade{}
	for _, t := range m.history {
		if t.TimeMS >= start && t.TimeMS <= end {
			ret = append(ret, t)
		}
	}
	return ret, nil
}
func (m *ledgerMock) Fetch(_ context.Context, _, _ string) (exchange.Snapshot, error) {
	if m.tamperDir != "" {
		_ = os.WriteFile(filepath.Join(m.tamperDir, "trading", "fills", "corruption.txt"), []byte("tamper"), 0600)
	}
	return m.snapshot, nil
}
func makeLedgerTest(t *testing.T) (string, *ledgerMock, exchange.AccountTrade) {
	t.Helper()
	d := t.TempDir()
	engine, e := NewEngine(d, &uniqueOrders{next: 700})
	if e != nil {
		t.Fatal(e)
	}
	store, e := NewOwnershipStore(d)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.MkdirAll(filepath.Join(d, "trading", "reservations"), 0700); e != nil {
		t.Fatal(e)
	}
	intent := makeIntent("ledger-review-short", Model, "ema_rsi_macd_v1", Short, Enter, "0.001")
	rec, e := engine.Submit(context.Background(), intent, allow)
	if e != nil {
		t.Fatal(e)
	}
	order := orderFromRecord(rec, "FILLED", "0.001")
	trade := receipt(rec, 901, "0.001", "80000", "0.03", "0", time.Now().UTC().Add(-30*time.Minute).UnixMilli())
	original := &readerOrders{orders: map[string]exchange.OrderResult{rec.Plan.ClientOrderID: order}, trades: map[int64][]exchange.AccountTrade{order.OrderID: {trade}}}
	count, e := store.ImportDemoOrder(context.Background(), original, DemoCredentials{APIKey: "mockkey", Secret: "mocksecret"}, intent)
	if e != nil || count != 1 {
		t.Fatalf("import: %d %v", count, e)
	}
	m := &ledgerMock{readerOrders: original, history: []exchange.AccountTrade{trade}, snapshot: accountSnapshot(time.Now().UTC(), "0", "0.001")}
	return d, m, trade
}
func reviewFixture(d string, m *ledgerMock) SignedLedgerHistoryReport {
	return ReviewSignedLedgerHistory(context.Background(), d, m, DemoCredentials{APIKey: "mockkey", Secret: "mocksecret"}, time.Now().UTC())
}
func TestSignedLedgerFullOrderAndWindowReadOnly(t *testing.T) {
	d, m, _ := makeLedgerTest(t)
	before, err := snapshotExecutionEvidence(d)
	if err != nil {
		t.Fatal(err)
	}
	r := reviewFixture(d, m)
	after, err := snapshotExecutionEvidence(d)
	if err != nil {
		t.Fatal(err)
	}
	if before != after || !r.LocalFillsMatchSigned || !r.HistoryWindowComplete || len(r.Orders) != 1 || !r.Orders[0].ExactFillsMatched || len(r.UnattributedTrades) != 0 || m.calls != 2 || m.historyReads != 1 || r.TradingAuthorized || r.LocalLifecycleChanged {
		t.Fatalf("signed audit mismatch: %+v, calls %d/%d", r, m.calls, m.historyReads)
	}
}
func TestSignedLedgerDetectsManualCloseWithoutAttributingModel(t *testing.T) {
	d, m, entry := makeLedgerTest(t)
	close := exchange.AccountTrade{Symbol: "BTCUSDT", TradeID: 902, OrderID: 899, Side: "BUY", PositionSide: "SHORT", Quantity: "0.001", Price: "80100", Commission: "0.03", CommissionAsset: "USDT", RealizedPnL: "-0.1", TimeMS: entry.TimeMS + 120000}
	m.history = append(m.history, close)
	m.externalOrders = map[int64]exchange.OrderResult{899: {Symbol: "BTCUSDT", OrderID: 899, Side: "BUY", PositionSide: "SHORT", Status: "FILLED", ExecutedQty: "0.001", OriginalQty: "0.001"}}
	m.snapshot = accountSnapshot(time.Now().UTC(), "0", "0")
	r := reviewFixture(d, m)
	if len(r.UnattributedTrades) != 1 || r.UnattributedTrades[0].TradeID != 902 || len(r.ExternalOrders) != 1 || !r.ExternalOrders[0].VerifiedOrderIdentity || !r.LocalFillsMatchSigned || !r.HistoryWindowComplete || r.State != "BLOCKED_REVIEW_REQUIRED" || r.TradingAuthorized {
		t.Fatalf("must leave external close unowned: %+v", r)
	}
}
func TestSignedLedgerBlocksMissingOrAlteredLocalFill(t *testing.T) {
	for _, tc := range []string{"remote-price-drift", "remote-fee-drift", "missing-history", "quantity-mismatch", "snapshot-stale", "local-tamper", "order-drift"} {
		t.Run(tc, func(t *testing.T) {
			d, m, _ := makeLedgerTest(t)
			switch tc {
			case "remote-price-drift":
				m.readerOrders.trades[701][0].Price = "90000"
			case "remote-fee-drift":
				m.readerOrders.trades[701][0].Commission = "0.7"
			case "missing-history":
				m.historyErr = errors.New("history incomplete")
			case "quantity-mismatch":
				x := m.readerOrders.orders
				for key, v := range x {
					v.ExecutedQty = "0.002"
					x[key] = v
				}
			case "snapshot-stale":
				m.snapshot.AsOf = time.Now().UTC().Add(-30 * time.Second).Format(time.RFC3339Nano)
			case "local-tamper":
				m.tamperDir = d
			case "order-drift":
				m.drift = true
			}
			r := reviewFixture(d, m)
			if r.State != "BLOCKED_REVIEW_REQUIRED" || r.TradingAuthorized || r.LocalLifecycleChanged {
				t.Fatalf("fail-closed %s: %+v", tc, r)
			}
			if tc == "remote-price-drift" || tc == "remote-fee-drift" || tc == "quantity-mismatch" || tc == "order-drift" {
				if r.LocalFillsMatchSigned {
					t.Fatalf("false local match: %+v", r)
				}
			}
			if tc == "missing-history" && r.HistoryWindowComplete {
				t.Fatalf("false complete history: %+v", r)
			}
			if tc == "local-tamper" && !strings.Contains(strings.Join(r.Blockers, " "), "changed during") {
				t.Fatalf("missed TOCTOU: %+v", r)
			}
		})
	}
}
func TestSignedLedgerBlocksLockAndSymlinkBeforeNetwork(t *testing.T) {
	d, m, _ := makeLedgerTest(t)
	if err := os.Mkdir(filepath.Join(d, "trading", "coordinator.active"), 0700); err != nil {
		t.Fatal(err)
	}
	r := reviewFixture(d, m)
	if m.calls != 0 || r.State != "BLOCKED_REVIEW_REQUIRED" || r.TradingAuthorized {
		t.Fatal(r)
	}
	if err := os.Remove(filepath.Join(d, "trading", "coordinator.active")); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(d, "trading", "reservations")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), path); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	r = reviewFixture(d, m)
	if m.calls != 0 || r.State != "BLOCKED_REVIEW_REQUIRED" {
		t.Fatal(r)
	}
}
func TestEqualSignedFillUsesExactDecimals(t *testing.T) {
	a := exchange.AccountTrade{Symbol: "BTCUSDT", TradeID: 1, OrderID: 2, Side: "SELL", PositionSide: "SHORT", Price: "80000.00", Quantity: "0.001", Commission: "0.0000100", CommissionAsset: "USDT", RealizedPnL: "0", TimeMS: 123}
	b := a
	b.Quantity = "0.0010"
	b.Commission = "0.00001"
	if !equalSignedFill(a, b) {
		t.Fatal("equivalent decimal values falsely differ")
	}
	b.RealizedPnL = "-1"
	if equalSignedFill(a, b) {
		t.Fatal("different PnL falsely equal")
	}
}

func TestSignedLedgerExternalOrderNotFoundNeverProvesClose(t *testing.T) {
	d, m, entry := makeLedgerTest(t)
	close := exchange.AccountTrade{Symbol: "BTCUSDT", TradeID: 904, OrderID: 905, Side: "BUY", PositionSide: "SHORT", Quantity: "0.001", Price: "80001", Commission: "0.03", CommissionAsset: "USDT", RealizedPnL: "-1", TimeMS: entry.TimeMS + 2000}
	m.history = append(m.history, close)
	r := reviewFixture(d, m)
	if r.State != "BLOCKED_REVIEW_REQUIRED" || len(r.ExternalOrders) != 1 || r.ExternalOrders[0].VerifiedOrderIdentity || r.TradingAuthorized {
		t.Fatalf("external order not proven: %+v", r)
	}
}
func TestSignedLedgerAgeLimitBlocksHistory(t *testing.T) {
	d, m, _ := makeLedgerTest(t)
	now := time.Now().UTC().Add(16 * 24 * time.Hour)
	r := ReviewSignedLedgerHistory(context.Background(), d, m, DemoCredentials{APIKey: "mockkey", Secret: "mocksecret"}, now)
	if r.HistoryWindowComplete || r.TradingAuthorized || r.State != "BLOCKED_REVIEW_REQUIRED" || m.historyReads != 0 {
		t.Fatalf("history 14-day bound bypass: %+v", r)
	}
}
