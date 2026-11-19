package trading

import (
	"binance_trader/internal/exchange"
	"testing"
	"time"

	"binance_trader/internal/model"
)

func TestMarketSignalPreviewNonExecutingAndFailClosed(t *testing.T) {
	reg, ids, market, _, _, _, now := previewFixture(t, model.ActionEnterLong, model.ActionEnterShort)
	// A custom evaluator cannot bypass required genuine candle provenance.
	candle := now.UnixMilli() / 60000 * 60000
	bars := make([]exchange.Candle, 180)
	for i := range bars {
		start := candle - int64(179-i)*60000
		v := 100.0 + float64(i)*0.1
		bars[i] = exchange.Candle{OpenTime: start, CloseTime: start + 59999, Open: v, Close: v, High: v + 1, Low: v - 1, Volume: 1}
	}
	market.MarketSnapshot = exchange.MarketSnapshot{Symbol: "BTCUSDT", Source: "BINANCE_FUTURES_DEMO_WEBSOCKET", Interval: "1m", Candles: bars}
	market.LiveMarketStatus = exchange.LiveMarketStatus{State: "live", Ready: true, StreamSymbol: "BTCUSDT", StreamInterval: "1m", LastEventMs: now.UnixMilli(), LastReceivedAt: now.Format(time.RFC3339Nano)}
	result, err := PreviewMarketSignals(reg, ids, market, now)
	if err != nil || result.State != "MARKET_SIGNALS_ONLY" || !(!result.TradingAuthorized && !result.ExecutionEnabled) || len(result.Signals) != 2 || result.Signals[0].Direction != "LONG" || result.Signals[1].Direction != "SHORT" || result.Assumption != "HYPOTHETICAL_FLAT_NO_ACCOUNT_OR_OWNER_ATTESTATION" {
		t.Fatalf("market-only preview: %+v %v", result, err)
	}
	for _, bad := range [][]string{nil, {ids[0], ids[0]}, {ids[0], "unregistered"}} {
		if r, e := PreviewMarketSignals(reg, bad, market, now); e == nil || r.TradingAuthorized || r.ExecutionEnabled || len(r.Signals) != 0 {
			t.Fatalf("accepted invalid selection: %+v %v", r, e)
		}
	}
	if r, e := PreviewMarketSignals(reg, ids, market, time.Time{}); e == nil || len(r.Signals) != 0 {
		t.Fatalf("accepted zero time %+v %v", r, e)
	}
}

func TestMarketSignalInvalidSourceAlwaysBlockedEvenForStubStrategies(t *testing.T) {
	reg, ids, m, _, _, _, now := previewFixture(t, model.ActionEnterLong, model.ActionEnterShort)
	r, e := PreviewMarketSignals(reg, ids, m, now)
	if e == nil || r.State != "BLOCKED" || len(r.Signals) != 0 || r.TradingAuthorized || r.ExecutionEnabled {
		t.Fatalf("unvalidated fake market reached strategies: %+v %v", r, e)
	}
}
