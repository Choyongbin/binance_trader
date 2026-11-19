package trading

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"binance_trader/internal/exchange"
)

// ProtectionArmingTransport is the sealed Demo-only boundary required for
// actually creating conditional TP/SL orders. It deliberately excludes cancel
// methods so arming can never turn into an automatic sibling-cancel workflow.
type ProtectionArmingTransport interface {
	FetchDemoExecutionPreflight(context.Context, string, string, string) (exchange.ExecutionPreflightSnapshot, error)
	SubmitDemoProtection(context.Context, string, string, exchange.NewDemoProtection) (exchange.DemoAlgoResult, error)
	QueryDemoAlgo(context.Context, string, string, int64) (exchange.DemoAlgoResult, error)
}

type ProtectionArmingResult struct {
	GroupID              string `json:"group_id"`
	TPAlgoID             int64  `json:"tp_algo_id,omitempty"`
	SLAlgoID             int64  `json:"sl_algo_id,omitempty"`
	TPObserved           bool   `json:"tp_observed"`
	SLObserved           bool   `json:"sl_observed"`
	Armed                bool   `json:"armed"`
	TradingAuthorized    bool   `json:"trading_authorized"` // always false
	ManualReviewRequired bool   `json:"manual_review_required"`
}

func protectionInstructionRequest(i ProtectionInstruction) exchange.NewDemoProtection {
	return exchange.NewDemoProtection{ClientAlgoID: i.ClientAlgoID, Symbol: i.Symbol, Side: i.Side,
		PositionSide: string(i.PositionSide), Type: i.OrderType, Quantity: i.Quantity,
		TriggerPrice: i.TriggerPrice, WorkingType: i.WorkingType}
}

func exactProtectionSideQty(p exchange.AccountPortfolio, side PositionSide, want string) error {
	target, err := pos(want)
	if err != nil {
		return err
	}
	got := new(big.Rat)
	for _, x := range p.Positions {
		if x.Symbol != "BTCUSDT" || x.PositionSide != string(side) {
			continue
		}
		q, ok := new(big.Rat).SetString(string(x.PositionAmt))
		if !ok {
			return errors.New("signed protection position 수량 오류")
		}
		q.Abs(q)
		got.Add(got, q)
	}
	if got.Cmp(target) != 0 {
		return fmt.Errorf("보호 주문 owner 수량과 exchange pooled-side 수량 불일치: exchange=%s owner=%s", got.RatString(), want)
	}
	return nil
}

func sameOpenProtectionAlgo(a exchange.AlgoOrder, i ProtectionInstruction, id int64) bool {
	if a.AlgoID != id || a.ClientAlgoID != i.ClientAlgoID || a.Symbol != i.Symbol || a.Side != i.Side ||
		a.PositionSide != string(i.PositionSide) || a.OrderType != i.OrderType || a.WorkingType != i.WorkingType || a.ClosePosition {
		return false
	}
	aq, ok1 := new(big.Rat).SetString(a.Quantity)
	iq, ok2 := new(big.Rat).SetString(i.Quantity)
	ap, ok3 := new(big.Rat).SetString(a.TriggerPrice)
	ip, ok4 := new(big.Rat).SetString(i.TriggerPrice)
	return ok1 && ok2 && ok3 && ok4 && aq.Cmp(iq) == 0 && ap.Cmp(ip) == 0 && (a.AlgoStatus == "NEW" || a.AlgoStatus == "WORKING")
}

func validateProtectionPreflight(s exchange.ExecutionPreflightSnapshot, owner OwnerPosition, expected map[int64]ProtectionInstruction) error {
	now := time.Now().UTC()
	if s.Environment != "demo" || !s.Portfolio.CanTrade || s.StartedAt.IsZero() || s.FinishedAt.IsZero() ||
		s.StartedAt.After(s.FinishedAt) || s.FinishedAt.Sub(s.StartedAt) > 5*time.Second ||
		s.FinishedAt.After(now.Add(time.Second)) || now.Sub(s.FinishedAt) > 5*time.Second ||
		s.OpenOrders == nil || s.OpenAlgos == nil || exchange.ValidatePortfolioSnapshot(s.Portfolio) != nil {
		return errors.New("최신 Binance Demo protection capability를 확인할 수 없습니다")
	}
	c := s.Configuration
	if c.Environment != "demo" || c.Symbol != "BTCUSDT" || !c.SettingsMatchV1 || !c.HedgeMode || c.MarginType != "ISOLATED" || c.Leverage != 15 || c.AutoAddMargin {
		return errors.New("Demo Hedge/ISOLATED/15x protection 설정이 실행 계약과 다릅니다")
	}
	if len(s.OpenOrders) != 0 {
		return errors.New("normal open order가 있어 protection arming을 중단합니다")
	}
	if err := exactProtectionSideQty(s.Portfolio, owner.Side, owner.Quantity); err != nil {
		return err
	}
	if len(s.OpenAlgos) != len(expected) {
		return errors.New("예상하지 않은 conditional algo order가 있습니다")
	}
	seen := map[int64]bool{}
	for _, a := range s.OpenAlgos {
		inst, ok := expected[a.AlgoID]
		if !ok || !sameOpenProtectionAlgo(a, inst, a.AlgoID) {
			return errors.New("기존 conditional algo order가 protection 계획과 다릅니다")
		}
		seen[a.AlgoID] = true
	}
	if len(seen) != len(expected) {
		return errors.New("예상 protection algo order가 누락되었습니다")
	}
	return nil
}

func (s *ProtectionSubmissionStore) markMayHaveSentLive(group, leg string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.inspectLocked(group, leg)
	if err != nil {
		return err
	}
	if st.MayHaveSent {
		return ErrProtectionAttempted
	}
	if leg == "sl" {
		first, err := s.inspectLocked(group, "tp")
		if err != nil {
			return err
		}
		if !first.SignedObserved || first.ObservedAt.IsZero() || time.Since(first.ObservedAt) > 5*time.Second {
			return errors.New("SL 실제 전송 직전에 TP signed observation이 필요합니다")
		}
	}
	p, err := s.read(group)
	if err != nil {
		return err
	}
	inst := p.TP
	if leg == "sl" {
		inst = p.SL
	}
	raw := []byte(fmt.Sprintf(`{"plan_digest":%q,"client_algo_id":%q,"state":"MAY_HAVE_SENT"}`, p.Digest, inst.ClientAlgoID))
	attempt, _, _ := s.legPaths(group, leg)
	if err := exclusiveDurable(attempt, raw); err != nil {
		return ErrProtectionAttempted
	}
	return nil
}

// ArmDemoProtection performs a one-shot TP then one-shot SL arming sequence.
// Every POST is preceded by durable MAY_HAVE_SENT and a full signed preflight;
// every ACK is followed by an independent signed GET. Ambiguous outcomes are
// never retried. It is intentionally not wired to HTTP until Demo E2E passes.
func ArmDemoProtection(ctx context.Context, group string, owner OwnerPosition, coordinator *Coordinator, store *ProtectionSubmissionStore,
	transport ProtectionArmingTransport, creds DemoCredentials) (ProtectionArmingResult, error) {
	out := ProtectionArmingResult{GroupID: group, ManualReviewRequired: true}
	if coordinator == nil || store == nil || transport == nil || creds.APIKey == "" || creds.Secret == "" {
		return out, errors.New("protection arming 필수 구성요소가 없습니다")
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if err := coordinator.ownsLock(); err != nil {
		return out, fmt.Errorf("protection arming coordinator 소유권 실패: %w", err)
	}
	plan, err := store.read(group)
	if err != nil {
		return out, err
	}
	if owner.Symbol != "BTCUSDT" || owner.Side != plan.PositionSide || owner.Quantity != plan.TP.Quantity || owner.Quantity != plan.SL.Quantity || owner.Source != plan.Source || owner.ModelID != plan.ModelID {
		return out, errors.New("protection arming owner와 durable plan이 일치하지 않습니다")
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	pre, err := transport.FetchDemoExecutionPreflight(ctx, creds.APIKey, creds.Secret, "BTCUSDT")
	if err != nil {
		return out, err
	}
	if err := validateProtectionPreflight(pre, owner, map[int64]ProtectionInstruction{}); err != nil {
		return out, err
	}

	if err := store.markMayHaveSentLive(group, "tp"); err != nil {
		return out, err
	}
	if err := ctx.Err(); err != nil {
		return out, fmt.Errorf("TP 전송 전 context 취소: MAY_HAVE_SENT 보존, 재전송 금지: %w", err)
	}
	tpAck, err := transport.SubmitDemoProtection(ctx, creds.APIKey, creds.Secret, protectionInstructionRequest(plan.TP))
	if err != nil {
		return out, fmt.Errorf("TP 전송 결과 불명확: 자동 재전송 금지: %w", err)
	}
	if err := store.RecordObserved(ctx, group, "tp", tpAck.AlgoID, transport, creds.APIKey, creds.Secret); err != nil {
		return out, fmt.Errorf("TP signed readback 실패: 자동 재전송 금지: %w", err)
	}
	out.TPAlgoID, out.TPObserved = tpAck.AlgoID, true

	pre2, err := transport.FetchDemoExecutionPreflight(ctx, creds.APIKey, creds.Secret, "BTCUSDT")
	if err != nil {
		return out, err
	}
	if err := validateProtectionPreflight(pre2, owner, map[int64]ProtectionInstruction{tpAck.AlgoID: plan.TP}); err != nil {
		return out, err
	}
	if err := store.markMayHaveSentLive(group, "sl"); err != nil {
		return out, err
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	slAck, err := transport.SubmitDemoProtection(ctx, creds.APIKey, creds.Secret, protectionInstructionRequest(plan.SL))
	if err != nil {
		return out, fmt.Errorf("SL 전송 결과 불명확: TP 자동 취소/SL 재전송 금지: %w", err)
	}
	if err := store.RecordObserved(ctx, group, "sl", slAck.AlgoID, transport, creds.APIKey, creds.Secret); err != nil {
		return out, fmt.Errorf("SL signed readback 실패: 자동 재전송 금지: %w", err)
	}
	out.SLAlgoID, out.SLObserved = slAck.AlgoID, true
	// Two independently submitted Algo legs must never share an exchange ID.
	// An impossible/contradictory signed reply is an incident, not an armed pair.
	if tpAck.AlgoID <= 0 || slAck.AlgoID <= 0 || tpAck.AlgoID == slAck.AlgoID {
		return out, errors.New("TP/SL 독립 Algo ID가 유효하지 않거나 충돌합니다: 재전송 금지, signed 감사 필요")
	}

	final, err := transport.FetchDemoExecutionPreflight(ctx, creds.APIKey, creds.Secret, "BTCUSDT")
	if err != nil {
		return out, err
	}
	if err := validateProtectionPreflight(final, owner, map[int64]ProtectionInstruction{tpAck.AlgoID: plan.TP, slAck.AlgoID: plan.SL}); err != nil {
		return out, err
	}
	out.Armed = true
	out.ManualReviewRequired = false
	return out, nil
}

// RecoverDemoProtectionArming is a READ-ONLY recovery path for an arming
// attempt whose POST may have succeeded but whose signed readback was lost.
// It never submits or cancels an order. A pending MAY_HAVE_SENT leg is promoted
// to SIGNED_ACTIVE_OBSERVED only when a fresh full-account preflight contains
// exactly one open algo whose immutable clientAlgoId and complete instruction
// match the durable plan. Missing/terminal/ambiguous evidence remains manual.
func RecoverDemoProtectionArming(ctx context.Context, group string, owner OwnerPosition, coordinator *Coordinator, store *ProtectionSubmissionStore,
	transport ProtectionArmingTransport, creds DemoCredentials) (ProtectionArmingResult, error) {
	out := ProtectionArmingResult{GroupID: group, ManualReviewRequired: true}
	if coordinator == nil || store == nil || transport == nil || creds.APIKey == "" || creds.Secret == "" {
		return out, errors.New("protection arming recovery 필수 구성요소가 없습니다")
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if err := coordinator.ownsLock(); err != nil {
		return out, fmt.Errorf("protection arming recovery coordinator 소유권 실패: %w", err)
	}
	plan, err := store.read(group)
	if err != nil {
		return out, err
	}
	if owner.Symbol != "BTCUSDT" || owner.Side != plan.PositionSide || owner.Quantity != plan.TP.Quantity || owner.Quantity != plan.SL.Quantity || owner.Source != plan.Source || owner.ModelID != plan.ModelID {
		return out, errors.New("protection arming recovery owner와 durable plan이 일치하지 않습니다")
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}

	type legInfo struct {
		name string
		inst ProtectionInstruction
		st   ProtectionLegState
	}
	legs := []legInfo{{name: "tp", inst: plan.TP}, {name: "sl", inst: plan.SL}}
	for i := range legs {
		st, err := store.Inspect(group, legs[i].name)
		if err != nil {
			return out, err
		}
		legs[i].st = st
	}
	if legs[0].st.SignedObserved && legs[1].st.SignedObserved &&
		legs[0].st.ObservedAlgoID == legs[1].st.ObservedAlgoID {
		return out, errors.New("재시작 복구: TP/SL 영속 관측의 Algo ID 중복, 독립 검토 필요")
	}

	pre, err := transport.FetchDemoExecutionPreflight(ctx, creds.APIKey, creds.Secret, "BTCUSDT")
	if err != nil {
		return out, err
	}
	expected := make(map[int64]ProtectionInstruction)
	for _, leg := range legs {
		if leg.st.SignedObserved {
			expected[leg.st.ObservedAlgoID] = leg.inst
		}
	}
	// Recover only MAY_HAVE_SENT legs. A merely prepared leg was never made
	// ambiguous and must not be inferred or submitted by a recovery routine.
	for i := range legs {
		leg := &legs[i]
		if leg.st.SignedObserved || !leg.st.MayHaveSent {
			continue
		}
		var match *exchange.AlgoOrder
		for j := range pre.OpenAlgos {
			a := &pre.OpenAlgos[j]
			if a.ClientAlgoID != leg.inst.ClientAlgoID {
				continue
			}
			if match != nil {
				return out, fmt.Errorf("%s clientAlgoId에 둘 이상의 open algo가 존재합니다", leg.name)
			}
			if !sameOpenProtectionAlgo(*a, leg.inst, a.AlgoID) {
				return out, fmt.Errorf("%s MAY_HAVE_SENT open algo가 durable plan과 다릅니다", leg.name)
			}
			copy := *a
			match = &copy
		}
		if match == nil {
			return out, fmt.Errorf("%s MAY_HAVE_SENT 결과를 open algo에서 재식별할 수 없습니다: terminal/history signed 검증 필요", leg.name)
		}
		expected[match.AlgoID] = leg.inst
	}
	if err := validateProtectionPreflight(pre, owner, expected); err != nil {
		return out, err
	}

	for i := range legs {
		leg := &legs[i]
		if leg.st.SignedObserved || !leg.st.MayHaveSent {
			continue
		}
		var algoID int64
		for id, inst := range expected {
			if inst.ClientAlgoID == leg.inst.ClientAlgoID {
				algoID = id
				break
			}
		}
		if algoID <= 0 {
			return out, fmt.Errorf("%s recovery algoId 누락", leg.name)
		}
		if err := store.RecordObserved(ctx, group, leg.name, algoID, transport, creds.APIKey, creds.Secret); err != nil {
			return out, fmt.Errorf("%s recovery signed readback 실패: %w", leg.name, err)
		}
		if leg.name == "tp" {
			out.TPAlgoID, out.TPObserved = algoID, true
		} else {
			out.SLAlgoID, out.SLObserved = algoID, true
		}
	}
	// Populate already-observed legs too.
	for _, leg := range legs {
		if leg.st.SignedObserved {
			if leg.name == "tp" {
				out.TPAlgoID, out.TPObserved = leg.st.ObservedAlgoID, true
			} else {
				out.SLAlgoID, out.SLObserved = leg.st.ObservedAlgoID, true
			}
		}
	}

	// Final fresh bracket after writing recovery observations. Only a complete
	// two-leg active pair may be called armed; otherwise recovery stays manual.
	final, err := transport.FetchDemoExecutionPreflight(ctx, creds.APIKey, creds.Secret, "BTCUSDT")
	if err != nil {
		return out, err
	}
	finalExpected := make(map[int64]ProtectionInstruction)
	tp, err := store.Inspect(group, "tp")
	if err != nil {
		return out, err
	}
	sl, err := store.Inspect(group, "sl")
	if err != nil {
		return out, err
	}
	if tp.SignedObserved {
		finalExpected[tp.ObservedAlgoID] = plan.TP
	}
	if sl.SignedObserved {
		if tp.SignedObserved && tp.ObservedAlgoID == sl.ObservedAlgoID {
			return out, errors.New("복구 후 TP/SL Algo ID 충돌: pair armed 주장 불가")
		}
		finalExpected[sl.ObservedAlgoID] = plan.SL
	}
	if err := validateProtectionPreflight(final, owner, finalExpected); err != nil {
		return out, err
	}
	if tp.SignedObserved && sl.SignedObserved {
		out.TPAlgoID, out.TPObserved = tp.ObservedAlgoID, true
		out.SLAlgoID, out.SLObserved = sl.ObservedAlgoID, true
		out.Armed = true
		out.ManualReviewRequired = false
	}
	return out, nil
}
