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

type uniqueOrders struct{ next int64 }

func (u *uniqueOrders) Submit(_ context.Context, p PlannedOrder) (Submission, error) {
	u.next++
	return Submission{ClientOrderID: p.ClientOrderID, OrderID: u.next, Status: "NEW"}, nil
}

type readerOrders struct {
	orders map[string]exchange.OrderResult
	trades map[int64][]exchange.AccountTrade
	mutate bool
	fail   bool
}

func (r *readerOrders) QueryDemoOrder(_ context.Context, _, _, _, cid string) (exchange.OrderResult, error) {
	if r.fail {
		return exchange.OrderResult{}, errors.New("missing authenticated order")
	}
	o := r.orders[cid]
	if r.mutate {
		r.mutate = false
		o.Status = "PARTIALLY_FILLED"
	}
	return o, nil
}
func (r *readerOrders) FetchDemoOrderTrades(_ context.Context, _, _, _ string, id int64) ([]exchange.AccountTrade, error) {
	return r.trades[id], nil
}
func orderFromRecord(r Record, status, filled string) exchange.OrderResult {
	return exchange.OrderResult{Symbol: r.Plan.Symbol, ClientOrderID: r.Plan.ClientOrderID, OrderID: r.ExchangeOrderID,
		Side: r.Plan.ExchangeSide, PositionSide: string(r.Plan.PositionSide), Type: string(r.Plan.Type),
		Status: status, OriginalQty: r.Plan.Quantity, ExecutedQty: filled}
}
func receipt(rec Record, id int64, qty, price, fee, exPnl string, timestamp int64) exchange.AccountTrade {
	return exchange.AccountTrade{Symbol: "BTCUSDT", OrderID: rec.ExchangeOrderID, TradeID: id, Quantity: qty, Price: price,
		Side: rec.Plan.ExchangeSide, PositionSide: string(rec.Plan.PositionSide), Commission: fee, CommissionAsset: "USDT", RealizedPnL: exPnl, TimeMS: timestamp}
}
func accountSnapshot(now time.Time, long, short string) exchange.Snapshot {
	positions := []exchange.Position{}
	if long != "0" {
		positions = append(positions, exchange.Position{Symbol: "BTCUSDT", Side: "LONG", Qty: long})
	}
	if short != "0" {
		positions = append(positions, exchange.Position{Symbol: "BTCUSDT", Side: "SHORT", Qty: "-" + short})
	}
	return exchange.Snapshot{Environment: "demo", Status: "connected", AsOf: now.UTC().Format(time.RFC3339Nano),
		Balances: []exchange.Balance{}, Positions: positions, Orders: []exchange.Order{}, AlgoOrders: []exchange.AlgoOrder{}}
}
func makeIntent(id string, src Source, model string, side PositionSide, action Action, qty string) Intent {
	return Intent{RequestID: id, Source: src, ModelID: model, Symbol: "BTCUSDT", PositionSide: side, Action: action, OrderType: Market, Quantity: qty}
}
func setupOwnership(t *testing.T) (*Engine, *OwnershipStore, *readerOrders) {
	t.Helper()
	d := t.TempDir()
	engine, err := NewEngine(d, &uniqueOrders{next: 100})
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewOwnershipStore(d)
	if err != nil {
		t.Fatal(err)
	}
	return engine, store, &readerOrders{orders: make(map[string]exchange.OrderResult), trades: make(map[int64][]exchange.AccountTrade)}
}
func sendAndImport(t *testing.T, e *Engine, s *OwnershipStore, r *readerOrders, i Intent, tradeID int64, qty, price, fee, exPnl string) Record {
	t.Helper()
	rec, err := e.Submit(context.Background(), i, allow)
	if err != nil {
		t.Fatal(err)
	}
	o := orderFromRecord(rec, "FILLED", qty)
	r.orders[rec.Plan.ClientOrderID] = o
	r.trades[o.OrderID] = []exchange.AccountTrade{receipt(rec, tradeID, qty, price, fee, exPnl, 1700000000000+tradeID)}
	n, err := s.ImportDemoOrder(context.Background(), r, DemoCredentials{APIKey: "demo-key", Secret: "test-secret"}, i)
	if err != nil || n != 1 {
		t.Fatalf("import id %d: %d %v", tradeID, n, err)
	}
	return rec
}
func observations(r *readerOrders, now time.Time) map[string]OrderObservation {
	out := map[string]OrderObservation{}
	for id, o := range r.orders {
		out[id] = OrderObservation{Order: o, CheckedAt: now}
	}
	return out
}
func owned(t *testing.T, report OwnershipReport, source Source, model string, side PositionSide) OwnerPosition {
	t.Helper()
	for _, p := range report.Owned {
		if p.Source == source && p.ModelID == model && p.Side == side {
			return p
		}
	}
	t.Fatalf("missing %s/%s/%s: %+v", source, model, side, report)
	return OwnerPosition{}
}
func TestMultiOwnerHedgePartialExitReplayAndAudit(t *testing.T) {
	engine, store, reader := setupOwnership(t)
	sendAndImport(t, engine, store, reader, makeIntent("model-A-enter1", Model, "A", Long, Enter, "0.010"), 1, "0.010", "100000", "0.4", "0")
	sendAndImport(t, engine, store, reader, makeIntent("model-B-enter1", Model, "B", Long, Enter, "0.020"), 2, "0.020", "110000", "0.5", "0")
	sendAndImport(t, engine, store, reader, makeIntent("manual-enter01", Manual, "", Long, Enter, "0.005"), 3, "0.005", "105000", "0.3", "0")
	sendAndImport(t, engine, store, reader, makeIntent("model-C-short1", Model, "C", Short, Enter, "0.003"), 4, "0.003", "107000", "0.2", "0")
	sendAndImport(t, engine, store, reader, makeIntent("model-A-exit01", Model, "A", Long, Exit, "0.005"), 5, "0.005", "102000", "0.2", "9.5")
	now := time.Now().UTC()
	report, err := store.Audit(accountSnapshot(now, "0.030", "0.003"), observations(reader, now), now)
	if err != nil || !report.ReadyForAssessment {
		t.Fatalf("report %+v %v", report, err)
	}
	a := owned(t, report, Model, "A", Long)
	if a.Quantity != "0.005" || a.AverageEntry != "100000" || a.VirtualGrossRealizedUSDT != "10" || a.ExchangeGrossRealizedUSDT != "9.5" || a.VirtualAfterUSDTFeesExFunding != "9.4" {
		t.Fatalf("model A %+v", a)
	}
	if b := owned(t, report, Model, "B", Long); b.Quantity != "0.02" || b.AverageEntry != "110000" {
		t.Fatal(b)
	}
	if manual := owned(t, report, Manual, "", Long); manual.Quantity != "0.005" || manual.AverageEntry != "105000" {
		t.Fatal(manual)
	}
	if c := owned(t, report, Model, "C", Short); c.Quantity != "0.003" || c.AverageEntry != "107000" {
		t.Fatal(c)
	}
	// Re-reading a complete signed history after restart must not add fills.
	recreated, err := NewOwnershipStore(filepath.Dir(filepath.Dir(store.journalDir)))
	if err != nil {
		t.Fatal(err)
	}
	i := makeIntent("model-A-enter1", Model, "A", Long, Enter, "0.010")
	n, err := recreated.ImportDemoOrder(context.Background(), reader, DemoCredentials{"demo-key", "secret"}, i)
	if err != nil || n != 0 {
		t.Fatalf("duplicate imported: n=%d err=%v", n, err)
	}
	after, err := recreated.Audit(accountSnapshot(now, "0.030", "0.003"), observations(reader, now), now)
	if err != nil || !after.ReadyForAssessment || after.ObservedFills != 5 {
		t.Fatalf("restart audit: %+v %v", after, err)
	}
	// An external Binance LONG trade is not silently attributed to a model.
	mismatch, err := store.Audit(accountSnapshot(now, "0.031", "0.003"), observations(reader, now), now)
	if err != nil || mismatch.ReadyForAssessment || !strings.Contains(strings.Join(mismatch.Blockers, " "), "LONG") {
		t.Fatalf("mismatch %+v %v", mismatch, err)
	}
	stale, err := store.Audit(accountSnapshot(now.Add(-10*time.Second), "0.030", "0.003"), observations(reader, now.Add(-10*time.Second)), now)
	if err != nil || stale.ReadyForAssessment {
		t.Fatalf("stale accepted %+v %v", stale, err)
	}
}
func TestUnknownJournalAndMissingTradeAndUnauthorizedExitAreBlocked(t *testing.T) {
	engine, store, reader := setupOwnership(t)
	i := makeIntent("owner-unknown01", Manual, "", Long, Enter, "0.01")
	p, _ := i.Plan()
	hash, _ := digestIntent(i)
	rec, created, err := engine.ensureNew(p, i, hash)
	if err != nil || !created {
		t.Fatal(err)
	}
	reader.orders[p.ClientOrderID] = exchange.OrderResult{Symbol: "BTCUSDT", ClientOrderID: p.ClientOrderID, OrderID: 55,
		Side: p.ExchangeSide, PositionSide: "LONG", Type: "MARKET", Status: "FILLED", OriginalQty: "0.01", ExecutedQty: "0.01"}
	now := time.Now().UTC()
	report, err := store.Audit(accountSnapshot(now, "0.01", "0"), observations(reader, now), now)
	if err != nil || report.ReadyForAssessment || len(report.Blockers) < 2 {
		t.Fatalf("unknown accepted: %+v %v", report, err)
	}
	// Even if signed fills are read, a PREPARED reservation remains blocked.
	reader.trades[55] = []exchange.AccountTrade{{Symbol: "BTCUSDT", OrderID: 55, TradeID: 55, Quantity: "0.01", Price: "100000", Side: "BUY", PositionSide: "LONG", Commission: "0.01", CommissionAsset: "USDT", RealizedPnL: "0", TimeMS: 1700000000555}}
	n, err := store.ImportDemoOrder(context.Background(), reader, DemoCredentials{"demo-key", "secret"}, i)
	if n != 1 || err != nil {
		t.Fatalf("import prepared %+v %d %v", rec, n, err)
	}
	report, err = store.Audit(accountSnapshot(now, "0.01", "0"), observations(reader, now), now)
	if err != nil || report.ReadyForAssessment {
		t.Fatalf("prepared accepted after fill: %+v %v", report, err)
	}
	exit := makeIntent("unknown-exit01", Manual, "", Short, Exit, "0.01")
	other, err := engine.Submit(context.Background(), exit, allow)
	if err != nil {
		t.Fatal(err)
	}
	reader.orders[other.Plan.ClientOrderID] = orderFromRecord(other, "FILLED", "0.01")
	reader.trades[other.ExchangeOrderID] = []exchange.AccountTrade{receipt(other, 80, "0.01", "100100", "0", "0", 1700000000900)}
	n, err = store.ImportDemoOrder(context.Background(), reader, DemoCredentials{"demo-key", "secret"}, exit)
	if err == nil || n != 0 || !strings.Contains(err.Error(), "사전 검증") {
		t.Fatalf("unowned EXIT accepted: %d %v", n, err)
	}
}
func TestImportRejectsChangingOrdersAndTamperedTradesAndOrphanFiles(t *testing.T) {
	engine, store, reader := setupOwnership(t)
	i := makeIntent("manual-owner001", Manual, "", Long, Enter, "0.01")
	rec, err := engine.Submit(context.Background(), i, allow)
	if err != nil {
		t.Fatal(err)
	}
	reader.orders[rec.Plan.ClientOrderID] = orderFromRecord(rec, "FILLED", "0.01")
	reader.trades[rec.ExchangeOrderID] = []exchange.AccountTrade{receipt(rec, 42, "0.009", "100000", "0", "0", 1700000000042)}
	_, err = store.ImportDemoOrder(context.Background(), reader, DemoCredentials{"k", "s"}, i)
	if err == nil || !strings.Contains(err.Error(), "누적") {
		t.Fatalf("partial fill imported: %v", err)
	}
	reader.trades[rec.ExchangeOrderID][0].Quantity = "0.01"
	reader.mutate = true
	_, err = store.ImportDemoOrder(context.Background(), reader, DemoCredentials{"k", "s"}, i)
	if err == nil || !strings.Contains(err.Error(), "조회 도중") {
		t.Fatalf("changing result accepted %v", err)
	}
	n, err := store.ImportDemoOrder(context.Background(), reader, DemoCredentials{"k", "s"}, i)
	if err != nil || n != 1 {
		t.Fatalf("normal import failed %d %v", n, err)
	}
	reader.trades[rec.ExchangeOrderID][0].Price = "99000"
	_, err = store.ImportDemoOrder(context.Background(), reader, DemoCredentials{"k", "s"}, i)
	if err == nil || !strings.Contains(err.Error(), "충돌") {
		t.Fatalf("tampered history accepted %v", err)
	}
	if err := os.WriteFile(filepath.Join(store.fillDir, "BTCUSDT_99.json"), []byte(`{"schema":1}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Audit(accountSnapshot(time.Now(), "0.01", "0"), nil, time.Now()); err == nil {
		t.Fatal("corrupt fill did not fail closed")
	}
}
func TestIncompleteAccountUnknownOpenOrderAndMixedFees(t *testing.T) {
	engine, store, reader := setupOwnership(t)
	i := makeIntent("owner-nonusdt1", Model, "fee", Long, Enter, "0.01")
	rec, err := engine.Submit(context.Background(), i, allow)
	if err != nil {
		t.Fatal(err)
	}
	reader.orders[rec.Plan.ClientOrderID] = orderFromRecord(rec, "FILLED", "0.01")
	trade := receipt(rec, 123, "0.01", "100000", "0.01", "0", 1700000000123)
	trade.CommissionAsset = "BNB"
	reader.trades[rec.ExchangeOrderID] = []exchange.AccountTrade{trade}
	if n, err := store.ImportDemoOrder(context.Background(), reader, DemoCredentials{"key", "secret"}, i); err != nil || n != 1 {
		t.Fatalf("non-USDT import %d %v", n, err)
	}
	now := time.Now()
	rep, err := store.Audit(accountSnapshot(now, "0.01", "0"), observations(reader, now), now)
	if err != nil || !rep.ReadyForAssessment || !owned(t, rep, Model, "fee", Long).OtherFeeAssetPresent {
		t.Fatalf("other fee representation %+v %v", rep, err)
	}
	invalid := accountSnapshot(now, "0.01", "0")
	invalid.AlgoOrders = append(invalid.AlgoOrders, exchange.AlgoOrder{Symbol: "BTCUSDT", AlgoID: 4321})
	invalid.Orders = append(invalid.Orders, exchange.Order{Symbol: "BTCUSDT", ClientOrderID: "external", OrderID: 5})
	report, err := store.Audit(invalid, observations(reader, now), now)
	if err != nil || report.ReadyForAssessment {
		t.Fatalf("external orders accepted: %+v %v", report, err)
	}
	// A trade event whose order ID was changed after persistence is rejected
	// even if the record has an ambiguous (zero) ExchangeOrderID.
	order := reader.orders[rec.Plan.ClientOrderID]
	order.OrderID = rec.ExchangeOrderID + 1
	obs := observations(reader, now)
	obs[rec.Plan.ClientOrderID] = OrderObservation{Order: order, CheckedAt: now}
	report, err = store.Audit(accountSnapshot(now, "0.01", "0"), obs, now)
	if err != nil || report.ReadyForAssessment {
		t.Fatalf("mismatched order ID accepted %+v %v", report, err)
	}
}

func TestSameSideSeparateOwnersNoCrossCloseAndWeightedAverage(t *testing.T) {
	engine, store, r := setupOwnership(t)
	sendAndImport(t, engine, store, r, makeIntent("model-A-average1", Model, "A", Long, Enter, "0.010"), 10, "0.010", "100000", "0", "0")
	sendAndImport(t, engine, store, r, makeIntent("model-A-average2", Model, "A", Long, Enter, "0.010"), 11, "0.010", "101000", "0", "0")
	sendAndImport(t, engine, store, r, makeIntent("model-B-average1", Model, "B", Long, Enter, "0.020"), 12, "0.020", "99000", "0", "0")
	now := time.Now()
	report, err := store.Audit(accountSnapshot(now, "0.04", "0"), observations(r, now), now)
	if err != nil || !report.ReadyForAssessment {
		t.Fatalf("initial audit: %+v %v", report, err)
	}
	if a := owned(t, report, Model, "A", Long); a.AverageEntry != "100500" {
		t.Fatalf("weighted average %+v", a)
	}
	ex := makeIntent("model-A-overexit", Model, "A", Long, Exit, "0.025")
	rec, err := engine.Submit(context.Background(), ex, allow)
	if err != nil {
		t.Fatal(err)
	}
	r.orders[rec.Plan.ClientOrderID] = orderFromRecord(rec, "FILLED", "0.025")
	r.trades[rec.ExchangeOrderID] = []exchange.AccountTrade{receipt(rec, 13, "0.025", "101000", "0", "0", 1700000000013)}
	n, err := store.ImportDemoOrder(context.Background(), r, DemoCredentials{"key", "secret"}, ex)
	if err == nil || n != 0 || !strings.Contains(err.Error(), "미보유") {
		t.Fatalf("model A could close model B: %d %v", n, err)
	}
	if _, err := os.Stat(filepath.Join(store.fillDir, "BTCUSDT_13.json")); !os.IsNotExist(err) {
		t.Fatal("rejected fill was saved", err)
	}
	// An unimported ACK is also a blocker, not a reason to silently ignore it.
	report, err = store.Audit(accountSnapshot(now, "0.04", "0"), observations(r, now), now)
	if err != nil || report.ReadyForAssessment {
		t.Fatalf("unimported fill accepted: %+v %v", report, err)
	}
}
func TestConcurrentImportIdempotentAndChecksumDetectsMutation(t *testing.T) {
	engine, store, r := setupOwnership(t)
	i := makeIntent("manual-concur01", Manual, "", Short, Enter, "0.01")
	rec, err := engine.Submit(context.Background(), i, allow)
	if err != nil {
		t.Fatal(err)
	}
	r.orders[rec.Plan.ClientOrderID] = orderFromRecord(rec, "FILLED", "0.01")
	r.trades[rec.ExchangeOrderID] = []exchange.AccountTrade{receipt(rec, 320, "0.01", "99000", "0.3", "0", 1700000000320)}
	ch := make(chan error, 12)
	result := make(chan int, 12)
	for k := 0; k < 12; k++ {
		go func() {
			n, err := store.ImportDemoOrder(context.Background(), r, DemoCredentials{"key", "secret"}, i)
			ch <- err
			result <- n
		}()
	}
	sum := 0
	for k := 0; k < 12; k++ {
		if err := <-ch; err != nil {
			t.Error(err)
		}
		sum += <-result
	}
	if sum != 1 {
		t.Fatalf("imported %d unique executions", sum)
	}
	path := filepath.Join(store.fillDir, "BTCUSDT_320.json")
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	altered := strings.Replace(string(body), `"price": "99000"`, `"price": "98000"`, 1)
	if altered == string(body) {
		t.Fatal("test failed to modify fill")
	}
	if err := os.WriteFile(path, []byte(altered), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Audit(accountSnapshot(time.Now(), "0", "0.01"), observations(r, time.Now()), time.Now()); err == nil || !strings.Contains(err.Error(), "체크섬") {
		t.Fatalf("tampering not detected: %v", err)
	}
}
