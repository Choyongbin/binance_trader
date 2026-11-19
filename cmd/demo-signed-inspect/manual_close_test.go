package main

import (
	"binance_trader/internal/exchange"
	"binance_trader/internal/trading"
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

type manualReviewMock struct {
	all        []exchange.AccountTrade
	exit       exchange.OrderResult
	snapshot   exchange.Snapshot
	historyErr error
	fetches    int
}

func (m *manualReviewMock) FetchDemoTradeWindow(_ context.Context, _, _, _ string, start, end int64) ([]exchange.AccountTrade, error) {
	m.fetches++
	if m.historyErr != nil {
		return nil, m.historyErr
	}
	out := []exchange.AccountTrade{}
	for _, v := range m.all {
		if v.TimeMS >= start && v.TimeMS <= end {
			out = append(out, v)
		}
	}
	return out, nil
}
func (m *manualReviewMock) QueryDemoOrderByID(_ context.Context, _, _, _ string, id int64) (exchange.OrderResult, error) {
	if m.exit.OrderID != id {
		return exchange.OrderResult{}, errors.New("not found")
	}
	return m.exit, nil
}
func (m *manualReviewMock) Fetch(_ context.Context, _, _ string) (exchange.Snapshot, error) {
	return m.snapshot, nil
}
func manualTrade(id, order int64, side, qty, pnl string, ts int64) exchange.AccountTrade {
	return exchange.AccountTrade{Symbol: "BTCUSDT", TradeID: id, OrderID: order, Side: side, PositionSide: "SHORT", Price: "81760.90", Quantity: qty, Commission: "0.01", CommissionAsset: "USDT", RealizedPnL: pnl, TimeMS: ts}
}
func sampleManualClose(t *testing.T) (exchange.OrderResult, []exchange.AccountTrade, *manualReviewMock) {
	t.Helper()
	entry := exchange.OrderResult{Symbol: "BTCUSDT", ClientOrderID: incidentClientID, OrderID: 112, Side: "SELL", PositionSide: "SHORT", Type: "MARKET", Status: "FILLED", OriginalQty: "0.001", ExecutedQty: "0.001"}
	at := time.Now().UTC().Add(-3 * time.Minute).UnixMilli()
	f1 := manualTrade(10, 112, "SELL", "0.001", "0", at)
	f2 := manualTrade(11, 113, "BUY", "0.001", "-0.34", at+60000)
	m := &manualReviewMock{all: []exchange.AccountTrade{f1, f2}, exit: exchange.OrderResult{Symbol: "BTCUSDT", OrderID: 113, Side: "BUY", PositionSide: "SHORT", Status: "FILLED", OriginalQty: "0.001", ExecutedQty: "0.001"}, snapshot: exchange.Snapshot{Status: "connected", Environment: "demo", AsOf: time.Now().UTC().Format(time.RFC3339)}}
	return entry, []exchange.AccountTrade{f1}, m
}
func TestManualCloseSignedHistoryCandidateReadOnly(t *testing.T) {
	entry, fills, m := sampleManualClose(t)
	r := reviewManualClose(context.Background(), m, trading.DemoCredentials{APIKey: "test", Secret: "test"}, entry, fills, time.Now().UTC())
	if r.Status != "SIGNED_EXTERNAL_CLOSE_CANDIDATE_REVIEW_ONLY" || !r.HistoryComplete || !r.FreshBTCUSDTFlat || r.TradingAuthorized || r.LocalLifecycleChanged || r.ExitOrderID != 113 || r.ExitQuantity != "0.00100000" || r.ExitRealizedPnL != "-0.34000000" || m.fetches < 1 {
		t.Fatalf("bad candidate: %+v", r)
	}
}
func TestManualCloseAllAmbiguitiesBlocked(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*exchange.OrderResult, *[]exchange.AccountTrade, *manualReviewMock)
	}{
		{"no_exit", func(_ *exchange.OrderResult, _ *[]exchange.AccountTrade, m *manualReviewMock) { m.all = m.all[:1] }},
		{"partial_close", func(_ *exchange.OrderResult, _ *[]exchange.AccountTrade, m *manualReviewMock) {
			m.all[1].Quantity = "0.0005"
		}},
		{"two_close_orders", func(_ *exchange.OrderResult, _ *[]exchange.AccountTrade, m *manualReviewMock) {
			m.all[1].Quantity = "0.0005"
			m.all = append(m.all, manualTrade(12, 114, "BUY", "0.0005", "-0.1", m.all[1].TimeMS+1))
		}},
		{"other_trade", func(_ *exchange.OrderResult, _ *[]exchange.AccountTrade, m *manualReviewMock) {
			m.all = append(m.all, manualTrade(12, 114, "SELL", "0.002", "0", m.all[1].TimeMS+1))
		}},
		{"duplicate_trade", func(_ *exchange.OrderResult, _ *[]exchange.AccountTrade, m *manualReviewMock) {
			m.all = append(m.all, m.all[1])
		}},
		{"snapshot_open_position", func(_ *exchange.OrderResult, _ *[]exchange.AccountTrade, m *manualReviewMock) {
			m.snapshot.Positions = []exchange.Position{{Symbol: "BTCUSDT", Side: "SHORT", Qty: "-0.001"}}
		}},
		{"snapshot_open_order", func(_ *exchange.OrderResult, _ *[]exchange.AccountTrade, m *manualReviewMock) {
			m.snapshot.Orders = []exchange.Order{{Symbol: "BTCUSDT"}}
		}},
		{"snapshot_stale", func(_ *exchange.OrderResult, _ *[]exchange.AccountTrade, m *manualReviewMock) {
			m.snapshot.AsOf = time.Now().UTC().Add(-10 * time.Minute).Format(time.RFC3339)
		}},
		{"exit_order_mismatch", func(_ *exchange.OrderResult, _ *[]exchange.AccountTrade, m *manualReviewMock) { m.exit.Status = "NEW" }},
		{"history_error", func(_ *exchange.OrderResult, _ *[]exchange.AccountTrade, m *manualReviewMock) {
			m.historyErr = errors.New("incomplete")
		}},
		{"entry_missing", func(_ *exchange.OrderResult, _ *[]exchange.AccountTrade, m *manualReviewMock) { m.all = m.all[1:] }},
		{"entry_fill_mismatch", func(_ *exchange.OrderResult, p *[]exchange.AccountTrade, _ *manualReviewMock) {
			(*p)[0].Quantity = "0.0005"
		}},
		{"old_history", func(_ *exchange.OrderResult, p *[]exchange.AccountTrade, m *manualReviewMock) {
			(*p)[0].TimeMS = time.Now().Add(-15 * 24 * time.Hour).UnixMilli()
			m.all[0].TimeMS = (*p)[0].TimeMS
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entry, fs, m := sampleManualClose(t)
			tc.mutate(&entry, &fs, m)
			result := reviewManualClose(context.Background(), m, trading.DemoCredentials{APIKey: "test", Secret: "test"}, entry, fs, time.Now().UTC())
			if result.Status == "SIGNED_EXTERNAL_CLOSE_CANDIDATE_REVIEW_ONLY" || result.TradingAuthorized || result.LocalLifecycleChanged || len(result.Blockers) < 2 {
				t.Fatalf("unsafe attribution %+v", result)
			}
		})
	}
}
func TestManualCloseRequiresEntrySignedProof(t *testing.T) {
	entry, fs, m := sampleManualClose(t)
	entry.Status = "PARTIALLY_FILLED"
	r := reviewManualClose(context.Background(), m, trading.DemoCredentials{}, entry, fs, time.Now())
	if r.HistoryComplete || r.TradingAuthorized || !strings.Contains(strings.Join(r.Blockers, " "), "entry") {
		t.Fatal(r)
	}
}
