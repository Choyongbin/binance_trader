package trading

// Offline, read-only integration of the multi-model preview, persisted STOP
// state, and a conservative equity cap. This is NOT a live order preflight:
// the two signed reads are not an atomic transaction, and neither current
// positions nor a bounded REST history can prove complete ownership.

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"binance_trader/internal/exchange"
)

// MultiModelRiskCycle is a diagnostic, never an authorization. A BLOCKED
// candidate must never be interpreted as a request to close an existing lot.
// MaxPercent is capped at 30 and can be reduced only for this offline review.
// All amounts are decimal strings calculated with exact rational arithmetic.
type MultiModelRiskCycle struct {
	State                         string        `json:"state"`
	RunState                      string        `json:"run_state"`
	RunGeneration                 uint64        `json:"run_generation"`
	EquityUSDT                    string        `json:"equity_usdt,omitempty"`
	MaxPercent                    int           `json:"max_percent"`
	EquityCapUSDT                 string        `json:"equity_cap_usdt,omitempty"`
	ExistingModelMarginUSDT       string        `json:"existing_model_margin_usdt,omitempty"`
	PendingModelMarginUSDT        string        `json:"pending_model_margin_usdt,omitempty"`
	SelectedProvisionalMarginUSDT string        `json:"selected_provisional_margin_usdt,omitempty"`
	RemainingEquityCapUSDT        string        `json:"remaining_equity_cap_usdt,omitempty"`
	ExistingPositionCount         int           `json:"existing_position_count"`
	ActiveReservationCount        int           `json:"active_reservation_count"`
	Cycle                         *PreviewCycle `json:"cycle,omitempty"`
	Blockers                      []string      `json:"blockers"`
	ExistingPositionsUnchanged    bool          `json:"existing_positions_unchanged"`
	TradingAuthorized             bool          `json:"trading_authorized"` // ALWAYS false
	ExecutionEnabled              bool          `json:"execution_enabled"`  // ALWAYS false
}

// checkRiskPortfolio insists on a fresh, bounded Demo signed observation.
// Identical wallet/available balances across separate endpoints is a strict
// offline consistency check, not a proof of an atomic or complete account view.
func checkRiskPortfolio(in PreviewCycleEvidence, b exchange.RecoveryBaseline) (*big.Rat, error) {
	if in.Now.IsZero() || b.Environment != "demo" || b.StartedAt.IsZero() || b.FinishedAt.Before(b.StartedAt) ||
		b.FinishedAt.After(in.Now.Add(time.Second)) || in.Now.Sub(b.StartedAt) > 5*time.Second ||
		b.OpenOrders == nil || b.OpenAlgos == nil || len(b.OpenOrders) != 0 || len(b.OpenAlgos) != 0 {
		return nil, errors.New("전체 계정 signed 조회 범위·시각·미체결 주문 증거가 불완전합니다")
	}
	p := b.Portfolio
	if !p.CanTrade || p.MultiAssetsMargin || p.Assets == nil || p.Positions == nil {
		return nil, errors.New("Demo 단일 USDT 자산·포지션 증거가 불완전합니다")
	}
	wallet, e1 := decimalRat(string(p.TotalWalletBalance), true)
	unreal, e2 := signedRat(string(p.TotalUnrealizedProfit))
	equity, e3 := decimalRat(string(p.TotalMarginBalance), true)
	initial, e4 := decimalRat(string(p.TotalInitialMargin), true)
	positionInitial, e5 := decimalRat(string(p.TotalPositionInitialMargin), true)
	openInitial, e6 := decimalRat(string(p.TotalOpenOrderInitialMargin), true)
	available, e7 := decimalRat(string(p.AvailableBalance), true)
	if e1 != nil || e2 != nil || e3 != nil || e4 != nil || e5 != nil || e6 != nil || e7 != nil ||
		equity.Sign() <= 0 || available.Cmp(equity) > 0 || initial.Cmp(equity) > 0 || new(big.Rat).Add(wallet, unreal).Cmp(equity) != 0 ||
		openInitial.Sign() != 0 || new(big.Rat).Add(positionInitial, openInitial).Cmp(initial) != 0 {
		return nil, errors.New("전체 계정 Equity·초기 증거금 불변식 불일치")
	}
	seen := map[string]bool{}
	var usdtWallet, usdtAvailable, usdtMargin *big.Rat
	for _, asset := range p.Assets {
		if seen[asset.Asset] || asset.Asset == "" {
			return nil, errors.New("계정 자산 중복")
		}
		seen[asset.Asset] = true
		w, err1 := decimalRat(string(asset.WalletBalance), true)
		a, err2 := decimalRat(string(asset.AvailableBalance), true)
		m, err3 := decimalRat(string(asset.MarginBalance), true)
		if err1 != nil || err2 != nil || err3 != nil {
			return nil, errors.New("계정 자산 숫자 오류")
		}
		if asset.Asset == "USDT" {
			usdtWallet, usdtAvailable, usdtMargin = w, a, m
		} else if w.Sign() != 0 || a.Sign() != 0 || m.Sign() != 0 {
			return nil, errors.New("USDT 외 계정 자산 노출")
		}
	}
	if usdtWallet == nil || usdtWallet.Cmp(wallet) != 0 || usdtAvailable.Cmp(available) != 0 || usdtMargin.Cmp(equity) != 0 {
		return nil, errors.New("USDT 자산과 전체 계정 총계 불일치")
	}
	snapWallet, snapAvailable := (*big.Rat)(nil), (*big.Rat)(nil)
	for _, asset := range in.Signed.Balances {
		if asset.Asset != "USDT" {
			continue
		}
		if snapWallet != nil {
			return nil, errors.New("signed USDT 잔액 중복")
		}
		snapWallet, e1 = decimalRat(asset.Wallet, true)
		snapAvailable, e2 = decimalRat(asset.Available, true)
		if e1 != nil || e2 != nil {
			return nil, errors.New("signed USDT 잔액 오류")
		}
	}
	if snapWallet == nil || snapWallet.Cmp(wallet) != 0 || snapAvailable.Cmp(available) != 0 {
		return nil, errors.New("signed 계정과 전체 자산 조회 불일치")
	}
	seenPositions := map[string]bool{}
	portfolioSides := map[string]*big.Rat{"LONG": new(big.Rat), "SHORT": new(big.Rat)}
	portfolioMarginSum := new(big.Rat)
	for _, pos := range p.Positions {
		k := pos.Symbol + ":" + pos.PositionSide
		if pos.Symbol == "" || seenPositions[k] || (pos.PositionSide != "LONG" && pos.PositionSide != "SHORT" && pos.PositionSide != "BOTH") {
			return nil, errors.New("포지션 식별자 중복·오류")
		}
		seenPositions[k] = true
		q, err := signedRat(string(pos.PositionAmt))
		if err != nil {
			return nil, errors.New("포지션 수량 오류")
		}
		if q.Sign() != 0 && (!pos.Isolated || pos.Symbol != "BTCUSDT" || pos.PositionSide == "BOTH") {
			return nil, errors.New("BTC 외 또는 one-way 계정 포지션 노출")
		}
		if pos.Symbol == "BTCUSDT" && pos.PositionSide != "BOTH" {
			if pos.PositionSide == "SHORT" {
				q.Neg(q)
			}
			if q.Sign() < 0 {
				return nil, errors.New("포지션 Hedge 수량 부호 오류")
			}
			portfolioSides[pos.PositionSide].Add(portfolioSides[pos.PositionSide], q)
		}
		m, err := decimalRat(string(pos.InitialMargin), true)
		if err != nil {
			return nil, errors.New("포지션 증거금 오류")
		}
		portfolioMarginSum.Add(portfolioMarginSum, m)
	}
	if portfolioMarginSum.Cmp(positionInitial) != 0 {
		return nil, errors.New("전체 포지션 초기 증거금 합계 불일치")
	}
	signedSides := map[string]*big.Rat{"LONG": new(big.Rat), "SHORT": new(big.Rat)}
	for _, pos := range in.Signed.Positions {
		if pos.Symbol != "BTCUSDT" || (pos.Side != "LONG" && pos.Side != "SHORT") {
			return nil, errors.New("signed 포지션 종목·방향 오류")
		}
		q, err := signedRat(pos.Qty)
		if err != nil {
			return nil, errors.New("signed 포지션 수량 오류")
		}
		if pos.Side == "SHORT" {
			q.Neg(q)
		}
		if q.Sign() < 0 {
			return nil, errors.New("signed 포지션 수량 부호 오류")
		}
		signedSides[pos.Side].Add(signedSides[pos.Side], q)
	}
	for _, side := range []string{"LONG", "SHORT"} {
		if signedSides[side].Cmp(portfolioSides[side]) != 0 {
			return nil, errors.New("signed 포지션과 전체 계정 Hedge 수량 불일치")
		}
	}

	return equity, nil
}

// ReviewOfflineMultiModelRiskCycle performs NO account/API calls and NO disk
// writes. It re-reads the durable STOP generation before and after the pure
// preview and suppresses ALL candidates on STOP, restart or generation change.
// It cannot guarantee safety of a future POST; that requires a separate
// exchange-backed final guard with its own durable ordering protocol.
func (s *ReadOnlyMultiModelScheduler) ReviewOfflineMultiModelRiskCycle(ctx context.Context, control *AutoRunControl,
	in PreviewCycleEvidence, signedPortfolio exchange.RecoveryBaseline, capPercent int) (MultiModelRiskCycle, error) {
	out := MultiModelRiskCycle{State: "BLOCKED", MaxPercent: capPercent, Blockers: []string{}, ExistingPositionsUnchanged: true,
		ExistingPositionCount: len(in.Ownership.Owned), ActiveReservationCount: len(in.ActiveReservations)}
	if s == nil || control == nil || ctx == nil || capPercent < 1 || capPercent > 30 {
		out.Blockers = append(out.Blockers, "scheduler, RUN 제어 또는 1~30%의 위험 한도가 필요합니다")
		return out, errors.New("오프라인 위험 검증 인자 오류")
	}
	if err := ctx.Err(); err != nil {
		out.Blockers = append(out.Blockers, err.Error())
		return out, err
	}
	before, err := control.Status()
	if err != nil {
		out.Blockers = append(out.Blockers, "RUN/STOP 기록 손상")
		return out, err
	}
	out.RunState, out.RunGeneration = before.State, before.Generation
	if before.State != AutoRunRunning || before.RestartBlock {
		out.State = "NEW_ENTRIES_BLOCKED_" + before.State
		out.Blockers = append(out.Blockers, "STOP/비정상 재시작 이후 신규 진입 평가 금지; 기존 포지션·보호주문은 변경하지 않음")
		return out, nil
	}
	equity, err := checkRiskPortfolio(in, signedPortfolio)
	if err != nil {
		out.Blockers = append(out.Blockers, err.Error())
		return out, err
	}
	cap := new(big.Rat).Mul(equity, big.NewRat(int64(capPercent), 100))
	out.EquityUSDT = ratToString(equity)
	out.EquityCapUSDT = ratToString(cap)
	cycle, err := s.Cycle(ctx, in)
	if err != nil {
		out.Blockers = append(out.Blockers, err.Error())
		return out, err
	}
	if cycle.TradingAuthorized || cycle.ExecutionEnabled || cycle.Preview.TradingAuthorized || cycle.Preview.ExecutionEnabled {
		return out, errors.New("read-only preview가 거래 승인을 주장합니다")
	}
	after, err := control.Status()
	if err != nil {
		out.Blockers = append(out.Blockers, "후속 STOP 상태 조회 실패")
		return out, err
	}
	if after.State != before.State || after.Generation != before.Generation || after.RestartBlock {
		out.RunState, out.RunGeneration = after.State, after.Generation
		out.State = "STOP_OR_GENERATION_CHANGED"
		out.Blockers = append(out.Blockers, "평가 중 STOP/새 세대 감지; 모든 신규 진입 후보 폐기")
		return out, nil
	}
	owned, e1 := decimalRat(cycle.Preview.GlobalBudget.OwnedUSDT, true)
	pending, e2 := decimalRat(cycle.Preview.GlobalBudget.PendingUSDT, true)
	if e1 != nil || e2 != nil {
		return out, errors.New("모델 원장 예산 숫자 검증 실패")
	}
	out.ExistingModelMarginUSDT, out.PendingModelMarginUSDT = ratToString(owned), ratToString(pending)
	used := new(big.Rat).Add(owned, pending)
	projected := new(big.Rat)
	for i := range cycle.Preview.Candidates {
		c := &cycle.Preview.Candidates[i]
		if c.State != "PREVIEW_ONLY" {
			continue
		}
		margin, e := decimalRat(c.ProvisionalMarginUSDT, true)
		if e != nil || margin.Sign() <= 0 {
			return out, fmt.Errorf("모델 %s의 신규 예산 오류", c.ModelID)
		}
		next := new(big.Rat).Add(used, new(big.Rat).Add(projected, margin))
		if next.Cmp(cap) > 0 {
			c.State, c.ProvisionalMarginUSDT = "BLOCKED_EQUITY_CAP", ""
			c.Reason = "전체 계정 Equity 대비 자동 모델 총 증거금 한도 초과: 기존 포지션은 유지"
			out.Blockers = append(out.Blockers, "모델 "+c.ModelID+" 신규 진입은 Equity 한도 초과로 제외")
			continue
		}
		projected.Add(projected, margin)
	}
	overCap := used.Cmp(cap) > 0
	if overCap {
		out.Blockers = append(out.Blockers, "기존 자동 증거금이 Equity 한도를 초과함; 포지션 강제 종료 금지")
	}
	left := new(big.Rat).Sub(cap, new(big.Rat).Add(used, projected))
	if left.Sign() < 0 {
		left.SetInt64(0)
	}
	out.RemainingEquityCapUSDT = ratToString(left)
	out.SelectedProvisionalMarginUSDT = ratToString(projected)
	cycle.Preview.ProvisionalTotalUSDT = ratToString(projected)
	// The scheduler's duplicate-candle suppression retains no fresh entries.
	if overCap {
		out.State = "EXISTING_MARGIN_OVER_CAP_NO_NEW_ENTRY"
	} else if cycle.DuplicateSuppressed {
		out.State = "DUPLICATE_CANDLE_NO_NEW_ENTRY"
	} else {
		out.State = "BOUNDED_PREVIEW_NOT_AUTHORIZED"
	}
	out.Cycle = &cycle
	return out, nil
}
