package model

import (
	"math"
	"testing"
	"time"
)

// Both strategies share validation, ownership invariants and the same exit
// rules. Only their RSI entry thresholds and durable IDs may differ.
func TestTwoBuiltinStrategiesProduceDifferentSignals(t *testing.T) {
	owner := OwnerState{Verified: true}
	long := SignalInput{EMA7: 105, EMA25: 110, EMA99: 100, RSI14: 55, MACD: 2, Signal: 1}
	base := EvaluateIndicators(long, owner, currentCandle)
	strict := EvaluateStrictIndicators(long, owner, currentCandle)
	if base.ModelID != TestStrategyID || base.Action != ActionEnterLong || strict.ModelID != StrictStrategyID || strict.Action != ActionHold || strict.LongEntry || strict.TradingAuthorized {
		t.Fatalf("unexpected model distinction: base=%+v strict=%+v", base, strict)
	}
	long.RSI14 = 60
	if got := EvaluateStrictIndicators(long, owner, currentCandle); got.Action != ActionHold {
		t.Fatalf("strict >60 inclusive boundary: %+v", got)
	}
	long.RSI14 = 61
	if got := EvaluateStrictIndicators(long, owner, currentCandle); got.Action != ActionEnterLong || !got.LongEntry {
		t.Fatalf("strict long: %+v", got)
	}

	short := SignalInput{EMA7: 95, EMA25: 90, EMA99: 100, RSI14: 45, MACD: -2, Signal: -1}
	if got := EvaluateIndicators(short, owner, currentCandle); got.Action != ActionEnterShort {
		t.Fatalf("base short: %+v", got)
	}
	if got := EvaluateStrictIndicators(short, owner, currentCandle); got.Action != ActionHold {
		t.Fatalf("strict short hold: %+v", got)
	}
	short.RSI14 = 40
	if got := EvaluateStrictIndicators(short, owner, currentCandle); got.Action != ActionHold {
		t.Fatalf("strict <40 inclusive boundary: %+v", got)
	}
	short.RSI14 = 39
	if got := EvaluateStrictIndicators(short, owner, currentCandle); got.Action != ActionEnterShort {
		t.Fatalf("strict short: %+v", got)
	}

	// Positions still exit using identical conditions, even when a new
	// strict entry would not be permitted in the same candle.
	shortOwner := OwnerState{Verified: true, ShortOpen: true, LastEntryCandleMS: currentCandle}
	exitShort := SignalInput{EMA7: 115, EMA25: 110, EMA99: 100, RSI14: 55, MACD: 2, Signal: 1}
	for _, fn := range []func(SignalInput, OwnerState, int64) Decision{EvaluateIndicators, EvaluateStrictIndicators} {
		if got := fn(exitShort, shortOwner, currentCandle); got.Action != ActionExitShort {
			t.Fatalf("short exit: %+v", got)
		}
	}
	longOwner := OwnerState{Verified: true, LongOpen: true, LastEntryCandleMS: currentCandle}
	exitLong := SignalInput{EMA7: 90, EMA25: 100, EMA99: 110, RSI14: 45, MACD: -2, Signal: -1}
	for _, fn := range []func(SignalInput, OwnerState, int64) Decision{EvaluateIndicators, EvaluateStrictIndicators} {
		if got := fn(exitLong, longOwner, currentCandle); got.Action != ActionExitLong {
			t.Fatalf("long exit: %+v", got)
		}
	}
	for _, fn := range []func(SignalInput, OwnerState, int64) Decision{EvaluateIndicators, EvaluateStrictIndicators} {
		if got := fn(long, OwnerState{}, currentCandle); got.Status != StatusBlocked || got.Action != ActionHold {
			t.Fatalf("unverified accepted: %+v", got)
		}
		long.RSI14 = math.NaN()
		if got := fn(long, owner, currentCandle); got.Status != StatusBlocked {
			t.Fatalf("NaN accepted: %+v", got)
		}
		long.RSI14 = 61
	}
}

func TestTwoBuiltinStrategiesUseSameLiveValidation(t *testing.T) {
	now := time.UnixMilli(currentCandle + 15000).UTC()
	m := validLive(now)
	registry := DefaultRegistry()
	if got := registry.IDs(); len(got) != 2 || got[0] != StrictStrategyID || got[1] != TestStrategyID {
		t.Fatalf("unexpected default registry: %v", got)
	}
	for _, id := range []string{TestStrategyID, StrictStrategyID} {
		s, err := registry.Resolve(id)
		if err != nil {
			t.Fatal(err)
		}
		got, err := s.EvaluateLive(m, OwnerState{Verified: true}, now)
		if err != nil || got.ModelID != id || got.Status != StatusReady || got.TradingAuthorized {
			t.Fatalf("%s: %+v %v", id, got, err)
		}
		stale := validLive(now)
		stale.LastEventMs = now.Add(-15 * time.Second).UnixMilli()
		got, err = s.EvaluateLive(stale, OwnerState{Verified: true}, now)
		if err == nil || got.ModelID != id || got.Status != StatusBlocked || got.TradingAuthorized {
			t.Fatalf("%s stale: %+v %v", id, got, err)
		}
	}
}
