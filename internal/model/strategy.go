// Package model contains pure, non-executing Go trading signal calculations.
// All results are candidates only: a future execution engine MUST verify
// durable owner positions, available allocated margin, exchange state and
// protective-order lifecycle before it can create an actual Demo order.
package model

import (
	"errors"
	"math"
	"time"

	"binance_trader/internal/exchange"
	"binance_trader/internal/indicators"
)

const (
	StatusBlocked    = "BLOCKED"
	StatusReady      = "CANDIDATE_ONLY"
	ActionHold       = "HOLD"
	ActionEnterLong  = "ENTER_LONG"
	ActionEnterShort = "ENTER_SHORT"
	ActionExitLong   = "EXIT_LONG"
	ActionExitShort  = "EXIT_SHORT"
	// Separate runtime configuration will own this bound in a future phase.
	maxPreviewDataAge = 5 * time.Second
)

type OwnerState struct {
	// Only a separately verified and durably recovered owner state is valid.
	Verified  bool `json:"verified"`
	LongOpen  bool `json:"long_open"`
	ShortOpen bool `json:"short_open"`
	// Persisted by the future order engine after a reservation, not by this
	// calculator.  Prevents an open/close/re-enter loop in the same candle.
	LastEntryCandleMS int64 `json:"last_entry_candle_ms"`
}

type SignalInput struct {
	EMA7   float64
	EMA25  float64
	EMA99  float64
	RSI14  float64
	MACD   float64
	Signal float64
}

type Decision struct {
	ModelID      string `json:"model_id"`
	Status       string `json:"status"`
	Action       string `json:"action"`
	Reason       string `json:"reason"`
	CandleOpenMS int64  `json:"candle_open_ms"`
	MarketAgeMS  int64  `json:"market_age_ms,omitempty"`
	LongEntry    bool   `json:"long_entry_condition"`
	ShortEntry   bool   `json:"short_entry_condition"`
	LongExit     bool   `json:"long_exit_condition"`
	ShortExit    bool   `json:"short_exit_condition"`
	// Never convert a signal into a live trading permission in this package.
	TradingAuthorized bool `json:"trading_authorized"`
}

const TestStrategyID = "ema_rsi_macd_v1"

// StrictStrategyID is a new durable identity. It never reinterprets past
// ema_rsi_macd_v1 trades or existing Client Order IDs.
const StrictStrategyID = "ema_rsi_macd_strict_v1"

func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

// EvaluateIndicators implements the user-defined simple comparisons, NOT
// crossing events. All three comparisons are AND conditions. Exit signals
// take priority; at most one entry candidate per owner per minute is returned.
// Existing LONG and SHORT owner positions are independent (hedge mode).
func EvaluateIndicators(in SignalInput, owner OwnerState, candleOpenMS int64) Decision {
	return evaluateIndicatorsWithThresholds(in, owner, candleOpenMS, TestStrategyID, 50, 50)
}

// EvaluateStrictIndicators uses a separate strategy identity and conservative
// RSI entry thresholds. The established exit rules remain unchanged.
func EvaluateStrictIndicators(in SignalInput, owner OwnerState, candleOpenMS int64) Decision {
	return evaluateIndicatorsWithThresholds(in, owner, candleOpenMS, StrictStrategyID, 60, 40)
}

func evaluateIndicatorsWithThresholds(in SignalInput, owner OwnerState, candleOpenMS int64, strategyID string, longEntryRSI, shortEntryRSI float64) Decision {
	out := Decision{ModelID: strategyID, Status: StatusBlocked, Action: ActionHold, Reason: "지표 또는 포지션 검증 대기", CandleOpenMS: candleOpenMS}
	if !owner.Verified || candleOpenMS <= 0 || candleOpenMS%60000 != 0 || owner.LastEntryCandleMS > candleOpenMS || owner.LastEntryCandleMS < 0 {
		return out
	}
	for _, v := range []float64{in.EMA7, in.EMA25, in.EMA99, in.RSI14, in.MACD, in.Signal} {
		if !finite(v) {
			return out
		}
	}
	if in.EMA7 <= 0 || in.EMA25 <= 0 || in.EMA99 <= 0 || in.RSI14 < 0 || in.RSI14 > 100 {
		return out
	}
	out.LongEntry = in.EMA25 > in.EMA99 && in.RSI14 > longEntryRSI && in.MACD > in.Signal
	out.ShortEntry = in.EMA25 < in.EMA99 && in.RSI14 < shortEntryRSI && in.MACD < in.Signal
	out.LongExit = in.EMA7 < in.EMA25 && in.RSI14 < 50 && in.MACD < in.Signal
	out.ShortExit = in.EMA7 > in.EMA25 && in.RSI14 > 50 && in.MACD > in.Signal
	out.Status = StatusReady
	out.Reason = "조건 불충족 또는 현재 포지션 유지"
	// An exit must never be inhibited by a same-minute entry restriction.
	if owner.LongOpen && out.LongExit {
		out.Action, out.Reason = ActionExitLong, "LONG 세 청산 조건 모두 충족"
	} else if owner.ShortOpen && out.ShortExit {
		out.Action, out.Reason = ActionExitShort, "SHORT 세 청산 조건 모두 충족"
	} else if owner.LastEntryCandleMS == candleOpenMS {
		out.Reason = "동일 모델의 동일 1분봉 재진입 차단"
	} else if !owner.LongOpen && out.LongEntry {
		out.Action, out.Reason = ActionEnterLong, "LONG 세 진입 조건 모두 충족"
	} else if !owner.ShortOpen && out.ShortEntry {
		out.Action, out.Reason = ActionEnterShort, "SHORT 세 진입 조건 모두 충족"
	}
	return out
}

func latest(s []*float64, n int) (float64, bool) {
	if len(s) != n || n == 0 || s[n-1] == nil || !finite(*s[n-1]) {
		return 0, false
	}
	return *s[n-1], true
}

// EvaluateLive requires a validated backend WS snapshot with the in-progress
// current one-minute candle. A web UI, a cached REST response or a closed
// previous candle can never cause an eligible action.
func EvaluateLive(m exchange.LiveMarketSnapshot, owner OwnerState, now time.Time) (Decision, error) {
	return evaluateLiveWithStrategy(m, owner, now, TestStrategyID, EvaluateIndicators)
}

// EvaluateStrictLive shares the exact same candle freshness/continuity checks.
func EvaluateStrictLive(m exchange.LiveMarketSnapshot, owner OwnerState, now time.Time) (Decision, error) {
	return evaluateLiveWithStrategy(m, owner, now, StrictStrategyID, EvaluateStrictIndicators)
}

func evaluateLiveWithStrategy(m exchange.LiveMarketSnapshot, owner OwnerState, now time.Time, strategyID string,
	evaluate func(SignalInput, OwnerState, int64) Decision) (Decision, error) {
	blocked := Decision{ModelID: strategyID, Status: StatusBlocked, Action: ActionHold, Reason: "실시간 캔들 검증 대기"}
	if now.IsZero() || !m.Ready || m.State != "live" || m.Symbol != "BTCUSDT" || m.Source != "BINANCE_FUTURES_DEMO_WEBSOCKET" || m.Interval != "1m" || m.StreamSymbol != "BTCUSDT" || m.StreamInterval != "1m" || len(m.Candles) < 120 || len(m.Candles) > 10000 {
		return blocked, errors.New("실시간 BTCUSDT 1분봉 데이터가 준비되지 않았습니다")
	}
	received, err := time.Parse(time.RFC3339Nano, m.LastReceivedAt)
	if err != nil || now.Before(received.Add(-2*time.Second)) || now.Sub(received) > maxPreviewDataAge || m.LastEventMs <= 0 || now.Sub(time.UnixMilli(m.LastEventMs)) > maxPreviewDataAge || time.UnixMilli(m.LastEventMs).After(now.Add(2*time.Second)) {
		return blocked, errors.New("현재 시장 WebSocket 데이터의 수신/이벤트 시각이 유효하지 않습니다")
	}
	closes := make([]float64, len(m.Candles))
	for i, c := range m.Candles {
		if c.OpenTime%60000 != 0 || c.CloseTime != c.OpenTime+59999 || c.Close <= 0 || !finite(c.Close) ||
			!finite(c.Open) || !finite(c.High) || !finite(c.Low) || !finite(c.Volume) || c.Open <= 0 || c.Volume < 0 ||
			c.High < math.Max(c.Open, c.Close) || c.Low > math.Min(c.Open, c.Close) || c.Low > c.High ||
			i > 0 && c.OpenTime-m.Candles[i-1].OpenTime != 60000 {
			return blocked, errors.New("1분봉 연속성 및 OHLC 검증 실패")
		}
		closes[i] = c.Close
	}
	lastOpen := m.Candles[len(m.Candles)-1].OpenTime
	if lastOpen != now.UnixMilli()/60000*60000 {
		return blocked, errors.New("현재 진행 중인 1분봉이 아닙니다")
	}
	computed, err := indicators.Calculate(closes)
	if err != nil {
		return blocked, err
	}
	n := len(closes)
	e7, a := latest(computed.EMA7, n)
	e25, b := latest(computed.EMA25, n)
	e99, c := latest(computed.EMA99, n)
	rsi, d := latest(computed.RSI, n)
	macd, e := latest(computed.MACD, n)
	signal, f := latest(computed.Signal, n)
	if !a || !b || !c || !d || !e || !f {
		return blocked, errors.New("보조지표 초기화가 완료되지 않았습니다")
	}
	out := evaluate(SignalInput{e7, e25, e99, rsi, macd, signal}, owner, lastOpen)
	out.MarketAgeMS = now.Sub(received).Milliseconds()
	if out.Status != StatusReady {
		return out, errors.New("검증된 소유 포지션 또는 모델 상태가 아닙니다")
	}
	return out, nil
}
