// Package indicators computes 1-minute candle indicators in Go to avoid
// running a trading signal in the browser. The current in-progress candle is
// deliberately included and recomputed on each new market snapshot.
// This is a pure library; no automatic trading route consumes it yet.
package indicators

import (
	"errors"
	"math"
)

type Series struct {
	EMA7      []*float64 `json:"ema7"`
	EMA25     []*float64 `json:"ema25"`
	EMA99     []*float64 `json:"ema99"`
	SMA25     []*float64 `json:"sma25"`
	SMA99     []*float64 `json:"sma99"`
	RSI       []*float64 `json:"rsi"`
	MACD      []*float64 `json:"macd"`
	Signal    []*float64 `json:"signal"`
	Histogram []*float64 `json:"hist"`
}

func f(v float64) *float64 { c := v; return &c }
func EMA(input []*float64, period int) []*float64 {
	out := make([]*float64, len(input))
	if period <= 0 {
		return out
	}
	k := 2.0 / (float64(period) + 1)
	sum, seen, last := 0.0, 0, 0.0
	hasLast := false
	for i, v := range input {
		if v == nil || math.IsNaN(*v) || math.IsInf(*v, 0) {
			continue
		}
		if !hasLast {
			sum += *v
			seen++
			if seen == period {
				last = sum / float64(period)
				out[i] = f(last)
				hasLast = true
			}
		} else {
			last = (*v)*k + last*(1-k)
			out[i] = f(last)
		}
	}
	return out
}
func SMA(input []float64, period int) []*float64 {
	out := make([]*float64, len(input))
	if period <= 0 {
		return out
	}
	sum := 0.0
	for i, v := range input {
		sum += v
		if i >= period {
			sum -= input[i-period]
		}
		if i >= period-1 {
			out[i] = f(sum / float64(period))
		}
	}
	return out
}
func WilderRSI(close []float64, period int) []*float64 {
	out := make([]*float64, len(close))
	if period <= 0 || len(close) <= period {
		return out
	}
	gains, losses := 0.0, 0.0
	render := func(g, l float64) float64 {
		if g == 0 && l == 0 {
			return 50
		}
		if l == 0 {
			return 100
		}
		return 100 - 100/(1+g/l)
	}
	for i := 1; i <= period; i++ {
		d := close[i] - close[i-1]
		gains += math.Max(d, 0)
		losses += math.Max(-d, 0)
	}
	gains /= float64(period)
	losses /= float64(period)
	out[period] = f(render(gains, losses))
	for i := period + 1; i < len(close); i++ {
		d := close[i] - close[i-1]
		gains = (gains*float64(period-1) + math.Max(d, 0)) / float64(period)
		losses = (losses*float64(period-1) + math.Max(-d, 0)) / float64(period)
		out[i] = f(render(gains, losses))
	}
	return out
}

// Calculate requires a bounded, time-ordered, fresh 1m close sequence from a
// separate validated market data service. This function does not grant any
// eligibility by itself; missing warmup means null values.
func Calculate(close []float64) (Series, error) {
	if len(close) < 1 || len(close) > 10000 {
		return Series{}, errors.New("1분봉 종가 개수 오류")
	}
	for _, v := range close {
		if v <= 0 || math.IsNaN(v) || math.IsInf(v, 0) {
			return Series{}, errors.New("유효하지 않은 1분봉 종가")
		}
	}
	ptrs := make([]*float64, len(close))
	for i, v := range close {
		ptrs[i] = f(v)
	}
	e12, e26 := EMA(ptrs, 12), EMA(ptrs, 26)
	macd := make([]*float64, len(close))
	for i := range close {
		if e12[i] != nil && e26[i] != nil {
			macd[i] = f(*e12[i] - *e26[i])
		}
	}
	signal := EMA(macd, 9)
	hist := make([]*float64, len(close))
	for i := range close {
		if macd[i] != nil && signal[i] != nil {
			hist[i] = f(*macd[i] - *signal[i])
		}
	}
	return Series{EMA7: EMA(ptrs, 7), EMA25: EMA(ptrs, 25), EMA99: EMA(ptrs, 99), SMA25: SMA(close, 25), SMA99: SMA(close, 99), RSI: WilderRSI(close, 14), MACD: macd, Signal: signal, Histogram: hist}, nil
}
