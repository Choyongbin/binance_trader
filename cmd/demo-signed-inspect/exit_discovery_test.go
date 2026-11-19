package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"binance_trader/internal/exchange"
	"binance_trader/internal/trading"
)

type fakeExitDiscovery struct {
	trades      []exchange.AccountTrade
	orders      map[int64]exchange.OrderResult
	stale       bool
	duplicate   bool
	failHistory bool
	changeOrder bool
	accountOpen bool
	observed    time.Time
	calls       map[int64]int
}

func (f *fakeExitDiscovery) FetchDemoTradeWindow(_ context.Context, _, _, _ string, start, end int64) ([]exchange.AccountTrade, error) {
	if f.failHistory {
		return nil, errors.New("history not available")
	}
	rows := []exchange.AccountTrade{}
	for _, v := range f.trades {
		if v.TimeMS >= start && v.TimeMS <= end {
			rows = append(rows, v)
		}
	}
	if f.duplicate && len(rows) > 0 {
		rows = append(rows, rows[0])
	}
	return rows, nil
}
func (f *fakeExitDiscovery) QueryDemoOrderByID(_ context.Context, _, _, _ string, id int64) (exchange.OrderResult, error) {
	if f.calls == nil {
		f.calls = map[int64]int{}
	}
	f.calls[id]++
	r, ok := f.orders[id]
	if !ok {
		return r, errors.New("not found")
	}
	if f.changeOrder && f.calls[id] > 1 {
		r.Status = "PARTIALLY_FILLED"
	}
	return r, nil
}
func (f *fakeExitDiscovery) Fetch(_ context.Context, _, _ string) (exchange.Snapshot, error) {
	t := f.observed
	if f.stale {
		t = t.Add(-10 * time.Minute)
	}
	snap := exchange.Snapshot{Status: "connected", Environment: "demo", AsOf: t.UTC().Format(time.RFC3339Nano), Balances: []exchange.Balance{}, Positions: []exchange.Position{}, Orders: []exchange.Order{}, AlgoOrders: []exchange.AlgoOrder{}}
	if f.accountOpen {
		snap.Positions = []exchange.Position{{Symbol: "BTCUSDT", Side: "SHORT", Qty: "-0.001"}}
	}
	return snap, nil
}
func makeExitEvidence(side string, now time.Time) (trading.PackageAEntryArchive, *fakeExitDiscovery) {
	enter, close := "BUY", "SELL"
	if side == "SHORT" {
		enter, close = "SELL", "BUY"
	}
	entry := exchange.OrderResult{Symbol: "BTCUSDT", ClientOrderID: "bt_test", OrderID: 100, Side: enter, PositionSide: side, Type: "MARKET", Status: "FILLED", OriginalQty: "0.001", ExecutedQty: "0.001", AveragePrice: "81000"}
	exit := exchange.OrderResult{Symbol: "BTCUSDT", ClientOrderID: "external", OrderID: 200, Side: close, PositionSide: side, Type: "MARKET", Status: "FILLED", OriginalQty: "0.001", ExecutedQty: "0.001", AveragePrice: "80900"}
	a := exchange.AccountTrade{Symbol: "BTCUSDT", TradeID: 11, OrderID: 100, Side: enter, PositionSide: side, Price: "81000", Quantity: "0.001", Commission: "0.03", CommissionAsset: "USDT", RealizedPnL: "0", TimeMS: now.Add(-2 * time.Hour).UnixMilli()}
	b := exchange.AccountTrade{Symbol: "BTCUSDT", TradeID: 12, OrderID: 200, Side: close, PositionSide: side, Price: "80900", Quantity: "0.001", Commission: "0.03", CommissionAsset: "USDT", RealizedPnL: "0.1", TimeMS: now.Add(-time.Hour).UnixMilli()}
	arc := trading.PackageAEntryArchive{Order: entry, Fills: []exchange.AccountTrade{a}}
	fake := &fakeExitDiscovery{trades: []exchange.AccountTrade{a, b}, orders: map[int64]exchange.OrderResult{200: exit}, observed: now}
	return arc, fake
}
func TestExitDiscoveryLongShortAndAlwaysBlockedForPosting(t *testing.T) {
	now := time.Date(2026, 10, 22, 7, 0, 0, 0, time.UTC)
	for _, side := range []string{"SHORT", "LONG"} {
		t.Run(side, func(t *testing.T) {
			a, f := makeExitEvidence(side, now)
			got := discoverExitOrder(context.Background(), f, trading.DemoCredentials{APIKey: "MOCK", Secret: "MOCK"}, a, now)
			if got.Status != "UNIQUE_EXIT_ORDER_ID_CANDIDATE_REVIEW_ONLY" || len(got.Candidates) != 1 || got.Candidates[0].OrderID != 200 || !got.Candidates[0].SignedOrderConfirmed || !got.BoundedHistoryRead || !got.FreshFlatAccount || got.FullAccountHistoryVerified || got.TradingAuthorized || got.SettlementAuthorized || got.OwnershipChanged || got.LifecycleChanged {
				t.Fatalf("unexpected result: %+v", got)
			}
			if f.calls[200] != 2 {
				t.Fatalf("signed order must be independently checked twice")
			}
		})
	}
}
func TestExitDiscoveryFailClosed(t *testing.T) {
	now := time.Date(2026, 10, 22, 7, 0, 0, 0, time.UTC)
	cases := []struct {
		name  string
		alter func(*trading.PackageAEntryArchive, *fakeExitDiscovery)
	}{
		{"no-exit", func(a *trading.PackageAEntryArchive, f *fakeExitDiscovery) { f.trades = f.trades[:1] }},
		{"second-exit", func(a *trading.PackageAEntryArchive, f *fakeExitDiscovery) {
			v := f.trades[1]
			v.TradeID = 13
			v.OrderID = 201
			v.TimeMS++
			f.trades = append(f.trades, v)
			e := f.orders[200]
			e.OrderID = 201
			f.orders[201] = e
		}},
		{"unexpected-trade", func(a *trading.PackageAEntryArchive, f *fakeExitDiscovery) {
			v := f.trades[1]
			v.TradeID = 13
			v.OrderID = 201
			v.Side = "SELL"
			f.trades = append(f.trades, v)
		}},
		{"partial-exit", func(a *trading.PackageAEntryArchive, f *fakeExitDiscovery) {
			f.trades[1].Quantity = "0.0005"
			f.orders[200] = exchange.OrderResult{Symbol: "BTCUSDT", OrderID: 200, Side: "BUY", PositionSide: "SHORT", Type: "MARKET", Status: "FILLED", ExecutedQty: "0.0005"}
		}},
		{"history-unavailable", func(a *trading.PackageAEntryArchive, f *fakeExitDiscovery) { f.failHistory = true }},
		{"duplicate", func(a *trading.PackageAEntryArchive, f *fakeExitDiscovery) { f.duplicate = true }},
		{"order-changed", func(a *trading.PackageAEntryArchive, f *fakeExitDiscovery) { f.changeOrder = true }},
		{"stale-account", func(a *trading.PackageAEntryArchive, f *fakeExitDiscovery) { f.stale = true }},
		{"open-position", func(a *trading.PackageAEntryArchive, f *fakeExitDiscovery) { f.accountOpen = true }},
		{"missing-entry", func(a *trading.PackageAEntryArchive, f *fakeExitDiscovery) { f.trades = f.trades[1:] }},
		{"corrupt-entry", func(a *trading.PackageAEntryArchive, f *fakeExitDiscovery) { a.Fills[0].Commission = "-5" }},
		{"too-old", func(a *trading.PackageAEntryArchive, f *fakeExitDiscovery) {
			a.Fills[0].TimeMS = now.Add(-15 * 24 * time.Hour).UnixMilli()
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, f := makeExitEvidence("SHORT", now)
			tc.alter(&a, f)
			got := discoverExitOrder(context.Background(), f, trading.DemoCredentials{APIKey: "MOCK", Secret: "MOCK"}, a, now)
			if got.Status == "UNIQUE_EXIT_ORDER_ID_CANDIDATE_REVIEW_ONLY" || got.TradingAuthorized || got.SettlementAuthorized || got.FullAccountHistoryVerified || len(got.Blockers) == 0 {
				t.Fatalf("unsafe classification: %+v", got)
			}
		})
	}
}
