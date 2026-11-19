package trading

import (
	"errors"
	"fmt"
	"math/big"
	"sort"
	"time"

	"binance_trader/internal/exchange"
	"binance_trader/internal/model"
	walletpkg "binance_trader/internal/wallet"
)

// ModelExposurePreview describes audited per-model use of a SHARED Binance
// account allocation. The amounts are not isolated exchange balances.
type ModelExposurePreview struct {
	ModelID                string `json:"model_id"`
	OwnedMarginUSDT        string `json:"owned_margin_usdt"`
	PendingEntryMarginUSDT string `json:"pending_entry_margin_usdt"`
}

// ModelCandidatePreview is a non-executing signal with a provisional budget.
// No entry in this structure is permission to submit or cancel an order.
type ModelCandidatePreview struct {
	ModelID               string         `json:"model_id"`
	Decision              model.Decision `json:"decision"`
	State                 string         `json:"state"`
	Reason                string         `json:"reason,omitempty"`
	PositionSide          PositionSide   `json:"position_side,omitempty"`
	EstimatedQuantity     string         `json:"estimated_quantity,omitempty"`
	ProvisionalMarginUSDT string         `json:"provisional_margin_usdt,omitempty"`
	TradingAuthorized     bool           `json:"trading_authorized"`
}

// MultiModelPreview is deliberately not connected to DemoExecutor or the
// controller. It lets the runtime safely reason about independent model
// ownership but shared account budget and pooled hedge-side positions.
type MultiModelPreview struct {
	Candidates           []ModelCandidatePreview `json:"candidates"`
	Exposures            []ModelExposurePreview  `json:"exposures"`
	GlobalBudget         walletpkg.Assessment    `json:"global_budget"`
	ProvisionalTotalUSDT string                  `json:"provisional_total_usdt"`
	TradingAuthorized    bool                    `json:"trading_authorized"`
	ExecutionEnabled     bool                    `json:"execution_enabled"`
}

// PreviewMultiModelSignals takes already reconciled ownership and fresh signed
// account evidence provided by a trusted internal caller. It neither performs
// private queries nor writes any reservations. Calls with unverifiable evidence
// fail closed. Selection priority is the caller-provided ID order. A selected
// model can see its own lot, but a same-side shared Binance hedge position is
// not granted to another model. Exit signals are *informational*, never
// execution-ready, because TP/SL pooled-side serialization is not proven.
func PreviewMultiModelSignals(registry *model.Registry, selected []string, market exchange.LiveMarketSnapshot,
	signed exchange.Snapshot, report OwnershipReport, reservations []Reservation,
	rules exchange.SymbolRules, assignedUSDT string, now time.Time) (MultiModelPreview, error) {
	out := MultiModelPreview{Candidates: []ModelCandidatePreview{}, Exposures: []ModelExposurePreview{}, ProvisionalTotalUSDT: "0"}
	if registry == nil || len(selected) == 0 || len(selected) > 16 || now.IsZero() || !report.ReadyForAssessment || len(report.Blockers) != 0 {
		return out, errors.New("다중 모델 미리보기: 모델 선택 또는 검증된 소유권 증거가 없습니다")
	}
	if signed.Environment != "demo" || signed.Status != "connected" || signed.Balances == nil || signed.Positions == nil || signed.Orders == nil || signed.AlgoOrders == nil || len(signed.Orders) != 0 || len(signed.AlgoOrders) != 0 {
		return out, errors.New("다중 모델 미리보기: 완전한 빈 미체결 주문 목록이 포함된 signed Demo 계정이 필요합니다")
	}
	asOf, err := time.Parse(time.RFC3339Nano, signed.AsOf)
	if err != nil || asOf.After(now.Add(time.Second)) || now.Sub(asOf) > 5*time.Second {
		return out, errors.New("다중 모델 미리보기: signed 계정 시각이 유효하지 않습니다")
	}
	quote, err := quoteFromLive(market)
	if err != nil || quote.ReceivedAt.After(now.Add(time.Second)) || now.Sub(quote.ReceivedAt) > 3*time.Second || !market.Ready || market.State != "live" || market.Source != "BINANCE_FUTURES_DEMO_WEBSOCKET" || market.Interval != "1m" {
		return out, errors.New("다중 모델 미리보기: fresh Demo 1분봉 WS가 필요합니다")
	}
	available, err := accountAvailableUSDT(signed)
	if err != nil {
		return out, err
	}
	current, err := autoModelBudget(report, reservations, assignedUSDT, available)
	if err != nil || !current.Fits {
		return out, errors.New("다중 모델 미리보기: 공유 자동매매 배정 지갑 예산을 검증할 수 없습니다")
	}
	out.GlobalBudget = current

	// Resolve every strategy BEFORE evaluation; duplicate IDs or incomplete
	// registries must not be represented as a partially evaluated portfolio.
	strategies := make([]model.Strategy, 0, len(selected))
	seen := map[string]bool{}
	for _, id := range selected {
		if seen[id] {
			return out, fmt.Errorf("다중 모델 미리보기: 중복 모델 ID %q", id)
		}
		seen[id] = true
		s, err := registry.Resolve(id)
		if err != nil {
			return out, err
		}
		strategies = append(strategies, s)
	}
	// This is an analysis of *all* ledger owners, not merely selected models.
	// Otherwise deselecting a model would make its collateral disappear.
	type exposure struct{ owned, pending *big.Rat }
	byID := map[string]*exposure{}
	add := func(id string) *exposure {
		if byID[id] == nil {
			byID[id] = &exposure{owned: new(big.Rat), pending: new(big.Rat)}
		}
		return byID[id]
	}
	occupied := map[PositionSide]bool{}
	expectedBySide := map[PositionSide]*big.Rat{Long: new(big.Rat), Short: new(big.Rat)}
	for _, p := range report.Owned {
		if p.Symbol != "BTCUSDT" || (p.Side != Long && p.Side != Short) {
			return out, errors.New("다중 모델 미리보기: 알 수 없는 소유 포지션")
		}
		q, err := decimalRat(p.Quantity, true)
		if err != nil || q.Sign() < 0 {
			return out, errors.New("다중 모델 미리보기: 소유 수량 오류")
		}
		if q.Sign() == 0 {
			continue
		}
		expectedBySide[p.Side].Add(expectedBySide[p.Side], q)
		occupied[p.Side] = true
		if p.Source == Model {
			avg, err := positiveRat(p.AverageEntry)
			if err != nil || p.ModelID == "" {
				return out, errors.New("다중 모델 미리보기: 모델 소유 증거 오류")
			}
			add(p.ModelID).owned.Add(add(p.ModelID).owned, ratDiv(ratMul(q, avg), big.NewRat(15, 1)))
		}
	}
	actualBySide := map[PositionSide]*big.Rat{Long: new(big.Rat), Short: new(big.Rat)}
	for _, p := range signed.Positions {
		side := PositionSide(p.Side)
		if p.Symbol != "BTCUSDT" || (side != Long && side != Short) {
			return out, errors.New("다중 모델 미리보기: 알 수 없는 signed 포지션")
		}
		qty, err := signedRat(p.Qty)
		if err != nil {
			return out, errors.New("다중 모델 미리보기: signed 포지션 수량 오류")
		}
		if side == Short {
			qty.Neg(qty)
		}
		if qty.Sign() < 0 {
			return out, errors.New("다중 모델 미리보기: signed 포지션 방향 불일치")
		}
		actualBySide[side].Add(actualBySide[side], qty)
	}
	for _, side := range []PositionSide{Long, Short} {
		if expectedBySide[side].Cmp(actualBySide[side]) != 0 {
			return out, fmt.Errorf("다중 모델 미리보기: %s 로컬 소유수량과 signed 포지션 불일치", side)
		}
	}
	for _, r := range reservations {
		if r.Intent.Symbol != "BTCUSDT" || (r.Intent.PositionSide != Long && r.Intent.PositionSide != Short) {
			return out, errors.New("다중 모델 미리보기: 예약 증거 오류")
		}
		// Caller must provide only the ACTIVE reservations. Unknown/finished
		// reservation state is not inferred by this pure planner.
		occupied[r.Intent.PositionSide] = true
		if r.Intent.Source == Model && r.Intent.Action == Enter {
			margin, err := decMargin(r.MarginUSDT)
			if err != nil || margin.Sign() < 0 || r.Intent.ModelID == "" {
				return out, errors.New("다중 모델 미리보기: 예약 증거금 오류")
			}
			add(r.Intent.ModelID).pending.Add(add(r.Intent.ModelID).pending, margin)
		}
	}
	// Include selected IDs even when exposure is zero. Separate model owners
	// share ONE overall account budget, which is never duplicated per model.
	ids := append([]string(nil), selected...)
	extras := []string{}
	for id := range byID {
		if !seen[id] {
			extras = append(extras, id)
		}
	}
	sort.Strings(extras)
	ids = append(ids, extras...)
	// Sort only the exposure display, not signal priority.
	for _, id := range ids {
		e := add(id)
		out.Exposures = append(out.Exposures, ModelExposurePreview{ModelID: id,
			OwnedMarginUSDT: ceilingDecimal12(e.owned), PendingEntryMarginUSDT: ceilingDecimal12(e.pending)})
	}

	availableRat, _ := decimalRat(available, true)
	remaining := new(big.Rat)
	remaining.SetString(current.FreeBudgetUSDT)
	provisional := new(big.Rat)
	for i, s := range strategies {
		id := selected[i]
		candidate := ModelCandidatePreview{ModelID: id, State: "BLOCKED", TradingAuthorized: false}
		owner, quantities, err := ownerStateForModel(report, reservations, id)
		if err != nil {
			candidate.Reason = err.Error()
			out.Candidates = append(out.Candidates, candidate)
			continue
		}
		decision, err := s.EvaluateLive(market, owner, now)
		candidate.Decision = decision
		if err != nil {
			candidate.Reason = err.Error()
			out.Candidates = append(out.Candidates, candidate)
			continue
		}
		if decision.ModelID != id || decision.TradingAuthorized || decision.Status != model.StatusReady ||
			decision.CandleOpenMS <= 0 || decision.CandleOpenMS%60000 != 0 {
			candidate.Reason = "모델 신호 식별자 또는 준비 상태가 유효하지 않습니다"
			out.Candidates = append(out.Candidates, candidate)
			continue
		}
		switch decision.Action {
		case model.ActionHold:
			candidate.State = "HOLD"
			candidate.Reason = "진입/청산 신호 없음"
		case model.ActionEnterLong, model.ActionEnterShort:
			side := Long
			if decision.Action == model.ActionEnterShort {
				side = Short
			}
			candidate.PositionSide = side
			if occupied[side] {
				candidate.Reason = "동일 Hedge 방향의 기존 소유 포지션, 예약 또는 앞선 모델 신호와 충돌"
				break
			}
			capBalance := new(big.Rat).Sub(availableRat, provisional)
			budget := new(big.Rat).Set(remaining)
			if capBalance.Cmp(budget) < 0 {
				budget.Set(capBalance)
			}
			if budget.Sign() <= 0 {
				candidate.Reason = "공유 지갑 또는 실제 가용 잔고 부족"
				break
			}
			size, err := SizeMarketEntry(rules, EntryBudget{AvailableUSDT: ratToString(budget), SharePercent: 10,
				Leverage: 15, FeeReserveBPS: 20, PriceBufferBPS: 100, Quote: quote, Now: now})
			if err != nil {
				candidate.Reason = "공유 예산/거래소 수량 규칙 불충족: " + err.Error()
				break
			}
			margin, err := decimalRat(size.AllocatedMarginUSDT, true)
			if err != nil || margin.Sign() <= 0 || margin.Cmp(remaining) > 0 || margin.Cmp(capBalance) > 0 {
				candidate.Reason = "공유 증거금 계산 불일치"
				break
			}
			// Conservatively reserve the entire 10% planned allocation, not
			// only the estimated used margin. No real reservation is written.
			provisional.Add(provisional, margin)
			remaining.Sub(remaining, margin)
			occupied[side] = true
			candidate.State = "PREVIEW_ONLY"
			candidate.EstimatedQuantity = size.Quantity
			candidate.ProvisionalMarginUSDT = size.AllocatedMarginUSDT
			candidate.Reason = "모의 공유 예산 배분; 실제 주문 불허"
		case model.ActionExitLong, model.ActionExitShort:
			side := Long
			if decision.Action == model.ActionExitShort {
				side = Short
			}
			candidate.PositionSide = side
			if occupied[side] && quantities[side] == "0" {
				candidate.Reason = "모델 소유 수량 없음"
				break
			}
			if quantities[side] == "0" {
				candidate.Reason = "청산할 모델 소유 포지션 없음"
				break
			}
			candidate.State = "EXIT_REVIEW_ONLY"
			candidate.Reason = "Hedge pooled-side EXIT/보호주문 직렬화 미검증; 주문 불허"
		default:
			candidate.Reason = "알 수 없는 모델 액션"
		}
		out.Candidates = append(out.Candidates, candidate)
	}
	out.ProvisionalTotalUSDT = ratToString(provisional)
	return out, nil
}
