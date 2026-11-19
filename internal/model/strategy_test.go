package model

import (
	"math"
	"strings"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

const currentCandle int64 = 1800000000000

func TestThreeConditionsANDAndHedge(t *testing.T) {
	long := SignalInput{EMA7: 105, EMA25: 110, EMA99: 100, RSI14: 55, MACD: 2, Signal: 1}
	s := OwnerState{Verified: true}
	got := EvaluateIndicators(long, s, currentCandle)
	if got.Action != ActionEnterLong || !got.LongEntry || got.TradingAuthorized {
		t.Fatalf("long signal: %+v", got)
	}
	long.RSI14 = 50
	if got = EvaluateIndicators(long, s, currentCandle); got.Action != ActionHold || got.LongEntry {
		t.Fatalf("strict rsi threshold: %+v", got)
	}
	long.RSI14 = 51
	long.MACD = long.Signal
	if got = EvaluateIndicators(long, s, currentCandle); got.Action != ActionHold {
		t.Fatalf("strict MACD: %+v", got)
	}
	long.MACD = 2
	long.EMA25 = long.EMA99
	if got = EvaluateIndicators(long, s, currentCandle); got.Action != ActionHold {
		t.Fatalf("strict EMA: %+v", got)
	}
	long.EMA25 = 110
	s.ShortOpen = true // still can enter long in hedge mode
	if got = EvaluateIndicators(long, s, currentCandle); got.Action != ActionEnterLong {
		t.Fatalf("hedge: %+v", got)
	}
	s.LongOpen = true
	if got = EvaluateIndicators(long, s, currentCandle); got.Action != ActionHold {
		t.Fatalf("duplicate side: %+v", got)
	}
}

func TestExitPriorityAndSameMinuteNoReentry(t *testing.T) {
	short := SignalInput{EMA7: 90, EMA25: 100, EMA99: 110, RSI14: 42, MACD: -2, Signal: -1}
	s := OwnerState{Verified: true, LongOpen: true, LastEntryCandleMS: currentCandle}
	got := EvaluateIndicators(short, s, currentCandle)
	if got.Action != ActionExitLong || !got.LongExit || got.TradingAuthorized {
		t.Fatalf("exit ignored: %+v", got)
	}
	s.LongOpen = false
	if got = EvaluateIndicators(short, s, currentCandle); got.Action != ActionHold || !strings.Contains(got.Reason, "재진입") {
		t.Fatalf("re-enter same minute: %+v", got)
	}
	s.LastEntryCandleMS = currentCandle - 60000
	if got = EvaluateIndicators(short, s, currentCandle); got.Action != ActionEnterShort {
		t.Fatalf("new minute: %+v", got)
	}
	s.ShortOpen = true
	reverse := SignalInput{EMA7: 120, EMA25: 110, EMA99: 100, RSI14: 55, MACD: 2, Signal: 1}
	if got = EvaluateIndicators(reverse, s, currentCandle); got.Action != ActionExitShort {
		t.Fatalf("short exit priority: %+v", got)
	}
}

func TestBlocksInvalidOwnerOrData(t *testing.T) {
	in := SignalInput{EMA7: 110, EMA25: 108, EMA99: 100, RSI14: 51, MACD: 1, Signal: 0}
	for _, tt := range []struct {
		name  string
		in    SignalInput
		owner OwnerState
		at    int64
	}{
		{"unverified", in, OwnerState{}, currentCandle},
		{"unrounded time", in, OwnerState{Verified: true}, currentCandle + 1},
		{"future history", in, OwnerState{Verified: true, LastEntryCandleMS: currentCandle + 60000}, currentCandle},
		{"rsi NaN", SignalInput{110, 108, 100, math.NaN(), 1, 0}, OwnerState{Verified: true}, currentCandle},
		{"inf MACD", SignalInput{110, 108, 100, 51, math.Inf(1), 0}, OwnerState{Verified: true}, currentCandle},
	} {
		got := EvaluateIndicators(tt.in, tt.owner, tt.at)
		if got.Status != StatusBlocked || got.Action != ActionHold || got.TradingAuthorized {
			t.Fatalf("%s %+v", tt.name, got)
		}
	}
}

func validLive(now time.Time) exchange.LiveMarketSnapshot {
	// The final in-progress minute stays current, 179 previous closed 1m bars.
	min := now.UnixMilli() / 60000 * 60000
	candles := make([]exchange.Candle, 180)
	for i := range candles {
		start := min - int64(179-i)*60000
		v := 100.0 + float64(i)*0.1
		candles[i] = exchange.Candle{OpenTime: start, CloseTime: start + 59999, Open: v, High: v + 1, Low: v - 1, Close: v, Volume: 1}
	}
	return exchange.LiveMarketSnapshot{MarketSnapshot: exchange.MarketSnapshot{Symbol: "BTCUSDT", Source: "BINANCE_FUTURES_DEMO_WEBSOCKET", Interval: "1m", Candles: candles}, LiveMarketStatus: exchange.LiveMarketStatus{Ready: true, State: "live", StreamSymbol: "BTCUSDT", StreamInterval: "1m", LastEventMs: now.UnixMilli(), LastReceivedAt: now.UTC().Format(time.RFC3339Nano)}}
}
func TestEvaluateLiveRequiresFreshCompleteInProgressCandle(t *testing.T) {
	now := time.UnixMilli(currentCandle + 15000).UTC()
	live := validLive(now)
	owner := OwnerState{Verified: true}
	got, err := EvaluateLive(live, owner, now)
	if err != nil || got.Status != StatusReady || got.TradingAuthorized || got.CandleOpenMS != currentCandle {
		t.Fatalf("valid live: %+v %v", got, err)
	}
	variants := []struct {
		name   string
		mutate func(*exchange.LiveMarketSnapshot)
	}{
		{"stale recv", func(m *exchange.LiveMarketSnapshot) {
			m.LastReceivedAt = now.Add(-10 * time.Second).Format(time.RFC3339Nano)
		}},
		{"stale exchange event", func(m *exchange.LiveMarketSnapshot) { m.LastEventMs = now.Add(-10 * time.Second).UnixMilli() }},
		{"previous bar", func(m *exchange.LiveMarketSnapshot) {
			m.Candles[len(m.Candles)-1].OpenTime -= 60000
			m.Candles[len(m.Candles)-1].CloseTime -= 60000
		}},
		{"missing bar", func(m *exchange.LiveMarketSnapshot) {
			m.Candles[100].OpenTime -= 60000
			m.Candles[100].CloseTime -= 60000
		}},
		{"wrong symbol", func(m *exchange.LiveMarketSnapshot) { m.Symbol = "ETHUSDT" }},
		{"wrong source", func(m *exchange.LiveMarketSnapshot) { m.Source = "REST_CACHE" }},
		{"not ready", func(m *exchange.LiveMarketSnapshot) { m.Ready = false }},
		{"broken price", func(m *exchange.LiveMarketSnapshot) { m.Candles[20].Close = math.NaN() }},
	}
	for _, tt := range variants {
		m := validLive(now)
		tt.mutate(&m)
		decision, err := EvaluateLive(m, owner, now)
		if err == nil || decision.TradingAuthorized || decision.Status != StatusBlocked {
			t.Fatalf("%s %+v %v", tt.name, decision, err)
		}
	}
	_, err = EvaluateLive(live, OwnerState{Verified: false}, now)
	if err == nil {
		t.Fatal("unverified ownership must block")
	}
}
