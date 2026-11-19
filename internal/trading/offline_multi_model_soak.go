package trading

// Strictly OFFLINE multi-minute audit of already supplied synthetic evidence.
// No Binance transport, executor, order journal or financial-commit capability
// is exposed here. A fresh WS candle after a missed minute is NOT a proof that
// the intervening market/account events were observed. The gap is latched until
// the caller builds a new, separately reconciled offline scenario.

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"binance_trader/internal/exchange"
)

const maxOfflineSoakSamples = 10080 // up to seven simulated 1-minute days

type OfflineMultiModelSoakSample struct {
	Evidence   PreviewCycleEvidence
	Portfolio  exchange.RecoveryBaseline
	MaxPercent int
}

type OfflineMultiModelSoakStep struct {
	Index                 int    `json:"index"`
	CandleOpenMS          int64  `json:"candle_open_ms"`
	State                 string `json:"state"`
	Reason                string `json:"reason,omitempty"`
	ProvisionalMarginUSDT string `json:"provisional_margin_usdt,omitempty"`
	CandidateCount        int    `json:"candidate_count"`
	TradingAuthorized     bool   `json:"trading_authorized"`
	ExecutionEnabled      bool   `json:"execution_enabled"`
}

type OfflineMultiModelSoakReport struct {
	State                                string                      `json:"state"`
	Samples                              int                         `json:"samples"`
	Reviewed                             int                         `json:"reviewed"`
	FreshCandles                         int                         `json:"fresh_candles"`
	Duplicates                           int                         `json:"duplicates"`
	StopBlocked                          int                         `json:"stop_blocked"`
	EquityCapBlocked                     int                         `json:"equity_cap_blocked"`
	EvidenceRejected                     int                         `json:"evidence_rejected"`
	GapBlocked                           int                         `json:"gap_blocked"`
	ProvisionalCandidates                int                         `json:"provisional_candidates"`
	MaxProvisionalMarginUSDT             string                      `json:"max_provisional_margin_usdt"`
	GapRecoveryRequiresIndependentReview bool                        `json:"gap_recovery_requires_independent_review"`
	TradingAuthorized                    bool                        `json:"trading_authorized"` // always false
	ExecutionEnabled                     bool                        `json:"execution_enabled"`  // always false
	Steps                                []OfflineMultiModelSoakStep `json:"steps"`
}

// AuditOfflineMultiModelSoak evaluates an immutable, caller-supplied trace.
// Ordinary invalid market/account evidence becomes a rejected sample, not a
// simulated successful order. A missing/reversed candle latches a gap for the
// remainder of this trace; no auto-resync is attempted. STOP never closes a
// position and never produces a candidate, even on an old/gapped market.
// Results are a rehearsal report, NOT a claim of realtime duration or trading.
func AuditOfflineMultiModelSoak(ctx context.Context, scheduler *ReadOnlyMultiModelScheduler,
	control *AutoRunControl, samples []OfflineMultiModelSoakSample) (OfflineMultiModelSoakReport, error) {
	out := OfflineMultiModelSoakReport{State: "OFFLINE_READ_ONLY", Samples: len(samples),
		MaxProvisionalMarginUSDT: "0", Steps: make([]OfflineMultiModelSoakStep, 0, len(samples))}
	if ctx == nil || scheduler == nil || control == nil || len(samples) == 0 || len(samples) > maxOfflineSoakSamples {
		return out, errors.New("오프라인 soak 검증 인자 또는 샘플 수(1~10080) 오류")
	}
	var lastAccepted int64
	gapLatched := false
	maximum := new(big.Rat)
	for index, sample := range samples {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		candle := int64(0)
		if candles := sample.Evidence.Market.Candles; len(candles) > 0 {
			candle = candles[len(candles)-1].OpenTime
		}
		step := OfflineMultiModelSoakStep{Index: index, CandleOpenMS: candle}
		// Do not classify a STOPPED cycle as a missing-market failure. The
		// underlying risk review must itself re-check the generation.
		run, err := control.Status()
		if err != nil {
			step.State, step.Reason = "EVIDENCE_REJECTED", "RUN/STOP evidence unreadable: "+err.Error()
			out.EvidenceRejected++
			out.Steps = append(out.Steps, step)
			continue
		}
		if run.State == AutoRunRunning && !run.RestartBlock {
			if !gapLatched && lastAccepted != 0 && candle != lastAccepted && (candle < lastAccepted || candle-lastAccepted != 60000) {
				gapLatched = true
				out.GapRecoveryRequiresIndependentReview = true
			}
			if gapLatched {
				step.State, step.Reason = "GAP_REQUIRES_RECONCILIATION", "missing/reversed 1m candle; no automatic resume"
				out.GapBlocked++
				out.Steps = append(out.Steps, step)
				continue
			}
		}
		got, err := scheduler.ReviewOfflineMultiModelRiskCycle(ctx, control, sample.Evidence, sample.Portfolio, sample.MaxPercent)
		if err != nil {
			step.State, step.Reason = "EVIDENCE_REJECTED", err.Error()
			out.EvidenceRejected++
			out.Steps = append(out.Steps, step)
			continue
		}
		if got.TradingAuthorized || got.ExecutionEnabled || (got.Cycle != nil && (got.Cycle.TradingAuthorized || got.Cycle.ExecutionEnabled || got.Cycle.Preview.TradingAuthorized || got.Cycle.Preview.ExecutionEnabled)) {
			return out, errors.New("오프라인 위험 결과가 실제 거래 권한을 주장합니다")
		}
		step.State = got.State
		out.Reviewed++
		if got.State == "NEW_ENTRIES_BLOCKED_STOPPED" || got.State == "NEW_ENTRIES_BLOCKED_RESTART_BLOCKED" || got.State == "STOP_OR_GENERATION_CHANGED" {
			out.StopBlocked++
		}
		if got.Cycle != nil {
			if got.Cycle.CandleOpenMS != candle {
				return out, fmt.Errorf("sample %d: candle identity changed during review", index)
			}
			if got.Cycle.DuplicateSuppressed {
				out.Duplicates++
			} else {
				out.FreshCandles++
				lastAccepted = candle
			}
			step.ProvisionalMarginUSDT = got.SelectedProvisionalMarginUSDT
			amount, err := decimalRat(got.SelectedProvisionalMarginUSDT, true)
			if err != nil {
				return out, fmt.Errorf("sample %d: malformed provisional margin: %w", index, err)
			}
			if amount.Cmp(maximum) > 0 {
				maximum.Set(amount)
			}
			for _, candidate := range got.Cycle.Preview.Candidates {
				if candidate.TradingAuthorized {
					return out, errors.New("candidate asserted trading authority")
				}
				switch candidate.State {
				case "PREVIEW_ONLY":
					step.CandidateCount++
					out.ProvisionalCandidates++
				case "BLOCKED_EQUITY_CAP":
					out.EquityCapBlocked++
				}
			}
		}
		out.Steps = append(out.Steps, step)
	}
	out.MaxProvisionalMarginUSDT = ratToString(maximum)
	if out.GapBlocked > 0 || out.EvidenceRejected > 0 {
		out.State = "OFFLINE_BLOCKED_EVIDENCE_OR_GAP"
	}
	return out, nil
}
