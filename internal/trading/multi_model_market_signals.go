package trading

import (
	"errors"
	"fmt"
	"time"

	"binance_trader/internal/exchange"
	"binance_trader/internal/model"
)

// MarketSignal is an INFORMATIONAL calculation for a hypothetical FLAT owner.
// It does not assert actual exchange positions or permission to execute.
// No intent, quantity, collateral, or order client ID is supplied.
type MarketSignal struct {
	ModelID   string `json:"model_id"`
	Direction string `json:"direction"`
	Reason    string `json:"reason"`
	State     string `json:"state"`
}

type MarketSignalCycle struct {
	State             string         `json:"state"`
	Assumption        string         `json:"assumption"`
	CandleOpenMS      int64          `json:"candle_open_ms"`
	EvaluatedAt       string         `json:"evaluated_at"`
	Signals           []MarketSignal `json:"signals"`
	TradingAuthorized bool           `json:"trading_authorized"`
	ExecutionEnabled  bool           `json:"execution_enabled"`
}

// PreviewMarketSignals evaluates real, validated Binance public WS candles for
// every selected strategy, assuming *hypothetical* zero owner positions ONLY
// for a chart signal comparison. Account/ownership evidence MUST be supplied
// separately to PreviewMultiModelSignals before any risk preview is possible.
// A failed/invalid strategy blocks the ENTIRE cycle (no partial signals).
// This function is pure and does not use the order executor or file system.
func PreviewMarketSignals(registry *model.Registry, ids []string, market exchange.LiveMarketSnapshot, now time.Time) (MarketSignalCycle, error) {
	out := MarketSignalCycle{State: "BLOCKED", Assumption: "HYPOTHETICAL_FLAT_NO_ACCOUNT_OR_OWNER_ATTESTATION", Signals: []MarketSignal{}, TradingAuthorized: false, ExecutionEnabled: false}
	if registry == nil || len(ids) < 2 || len(ids) > 16 || now.IsZero() {
		return out, errors.New("다중 모델 시장 신호: 2~16개의 등록 모델과 평가 시각이 필요합니다")
	}
	seen := map[string]bool{}
	for _, id := range ids {
		if seen[id] {
			return out, fmt.Errorf("다중 모델 시장 신호: 중복 ID %s", id)
		}
		seen[id] = true
		if _, err := registry.Resolve(id); err != nil {
			return out, err
		}
	}
	// Independently validate the demo public WS snapshot with the builtin
	// strict candle/continuity validator BEFORE running any registered strategy.
	// A future plugin model must not be able to bypass the market provenance
	// checks merely by returning an apparently eligible signal.
	if _, err := model.EvaluateLive(market, model.OwnerState{Verified: true}, now); err != nil {
		return out, errors.New("다중 모델 시장 신호: 실시간 Demo 1분봉 연속성/신선도 검증 실패")
	}
	result := make([]MarketSignal, 0, len(ids))
	var candle int64
	for _, id := range ids {
		strategy, _ := registry.Resolve(id)
		decision, err := strategy.EvaluateLive(market, model.OwnerState{Verified: true}, now)
		if err != nil || decision.ModelID != id || decision.Status != model.StatusReady || decision.TradingAuthorized || decision.CandleOpenMS <= 0 || decision.CandleOpenMS%60000 != 0 || decision.CandleOpenMS != market.Candles[len(market.Candles)-1].OpenTime {
			return out, fmt.Errorf("다중 모델 시장 신호: %s의 공개 시세 평가 실패 (거래는 잠금 유지)", id)
		}
		if candle != 0 && candle != decision.CandleOpenMS {
			return out, errors.New("모델 간 캔들 시각 불일치")
		}
		candle = decision.CandleOpenMS
		direction := "HOLD"
		switch decision.Action {
		case model.ActionEnterLong:
			direction = "LONG"
		case model.ActionEnterShort:
			direction = "SHORT"
		case model.ActionHold:
		default:
			return out, errors.New("가상 FLAT 계정에 부적절한 모델 액션")
		}
		result = append(result, MarketSignal{ModelID: id, Direction: direction, Reason: decision.Reason, State: "INDICATIVE_ONLY"})
	}
	out.State = "MARKET_SIGNALS_ONLY"
	out.CandleOpenMS = candle
	out.EvaluatedAt = now.UTC().Format(time.RFC3339Nano)
	out.Signals = result
	return out, nil
}
