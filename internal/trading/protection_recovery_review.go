package trading

// Protection recovery is a side-effect-free incident classifier. It never
// submits/cancels orders, writes original ledgers, grants trading permission,
// or treats two independent Binance Algo legs as OCO. Inputs MUST originate
// from separately authenticated Demo signed GETs; the caller must validate
// provenance and complete-history coverage outside this bounded review.
import (
	"binance_trader/internal/exchange"
	"fmt"
	"math/big"
	"reflect"
	"sort"
	"strconv"
	"time"
)

type ProtectionRecoveryState string

const (
	RecoveryReviewBlocked      ProtectionRecoveryState = "BLOCKED_SIGNED_RECONCILIATION_REQUIRED"
	RecoveryReviewMonitoring   ProtectionRecoveryState = "OBSERVED_PAIR_ACTIVE_NOT_OCO"
	RecoveryReviewCancelReview ProtectionRecoveryState = "TRIGGERED_SIBLING_REQUIRES_CANCEL_REVIEW"
	RecoveryReviewPartial      ProtectionRecoveryState = "PARTIAL_FILL_HALT"
	RecoveryReviewDouble       ProtectionRecoveryState = "DOUBLE_TRIGGER_HALT"
	RecoveryReviewOrphan       ProtectionRecoveryState = "ORPHAN_OR_MISSING_LEG_HALT"
	RecoveryReviewClosed       ProtectionRecoveryState = "OBSERVED_CLOSED_NOT_POSTED"
)

type ProtectionRecoveryLeg struct {
	GroupID     string          `json:"group_id"`
	Kind        string          `json:"kind"` // tp / sl
	Observation AlgoObservation `json:"observation"`
	// Independently signed trades from the actualOrderId, NOT websocket events.
	Trades []exchange.AccountTrade `json:"trades"`
}

type ProtectionRecoveryGroup struct {
	GroupID         string                  `json:"group_id"`
	State           ProtectionRecoveryState `json:"state"`
	OriginalQty     string                  `json:"original_qty"`
	ObservedExitQty string                  `json:"observed_exit_qty"`
	RemainingQty    string                  `json:"remaining_qty"`
	Blockers        []string                `json:"blockers"`
}

type ProtectionRecoveryReview struct {
	State                string                    `json:"state"` // REVIEW_ONLY / HALT
	Groups               []ProtectionRecoveryGroup `json:"groups"`
	UnexpectedAlgoIDs    []int64                   `json:"unexpected_algo_ids,omitempty"`
	Blockers             []string                  `json:"blockers"`
	TradingAuthorized    bool                      `json:"trading_authorized"`     // ALWAYS false
	OrderDispatchAllowed bool                      `json:"order_dispatch_allowed"` // ALWAYS false
	CancelReplayAllowed  bool                      `json:"cancel_replay_allowed"`  // ALWAYS false
	FinanciallySettled   bool                      `json:"financially_settled"`    // ALWAYS false
	ManualReviewRequired bool                      `json:"manual_review_required"` // ALWAYS true
}

// ReviewDemoProtectionRecovery reviews a whole account bracket AND both legs
// for every outstanding local protection group. SignedAccountAsOf is not an
// exchange transaction; external races remain possible. All states are
// advisory, including OBSERVED_CLOSED_NOT_POSTED.
func ReviewDemoProtectionRecovery(b exchange.RecoveryBaseline, plans []ProtectionDispatchPlan, owners []OwnerPosition,
	legs []ProtectionRecoveryLeg, ownershipAuditVerified bool, now time.Time) ProtectionRecoveryReview {
	r := ProtectionRecoveryReview{State: "HALT", Groups: []ProtectionRecoveryGroup{}, Blockers: []string{}, ManualReviewRequired: true}
	block := func(why string) { r.Blockers = append(r.Blockers, why) }
	if !ownershipAuditVerified {
		block("독립 signed 계정·소유자 전수 감사 미완료")
	}
	if b.Environment != "demo" || b.Coverage != "BOUNDED_HISTORY_ONLY_NOT_CONTINUOUS" || b.StartedAt.IsZero() || b.FinishedAt.Before(b.StartedAt) ||
		b.FinishedAt.After(now.Add(time.Second)) || now.Sub(b.FinishedAt) > 5*time.Second || b.FinishedAt.Sub(b.StartedAt) > 30*time.Second ||
		b.WindowStartMS <= 0 || b.WindowEndMS != b.StartedAt.UnixMilli() || b.WindowEndMS <= b.WindowStartMS ||
		b.OpenAlgos == nil || b.OpenOrders == nil || b.RecentBTCTrades == nil || b.RecentBTCAlgos == nil || b.RecentBTCOrders == nil || b.RecentIncome == nil ||
		exchange.ValidatePortfolioSnapshot(b.Portfolio) != nil {
		block("최신이고 완전한 bounded Demo signed account bracket 필요")
	}
	if len(b.OpenOrders) != 0 || b.Portfolio.MultiAssetsMargin || !b.Portfolio.CanTrade {
		block("외부 미체결 일반 주문·다중 자산·거래 불가 계정")
	}
	for _, asset := range b.Portfolio.Assets {
		if asset.Asset != "USDT" {
			qty, err := signedRat(string(asset.MarginBalance))
			if err != nil || qty.Sign() != 0 {
				block("USDT 이외의 계정 증거금은 지원하지 않습니다")
				break
			}
		}
	}
	if len(plans) == 0 && (len(owners) != 0 || len(legs) != 0 || len(b.OpenAlgos) != 0) {
		block("소유권/관측은 있지만 durable 보호 계획이 없습니다")
	}
	if len(plans) == 0 {
		block("보호 계획 없는 빈 계정도 거래 허가는 아닙니다")
	}
	if len(r.Blockers) != 0 {
		return r
	}

	ownerByKey := make(map[string]OwnerPosition)
	ownerQty := map[PositionSide]*big.Rat{Long: new(big.Rat), Short: new(big.Rat)}
	for _, owner := range owners {
		if owner.Symbol != "BTCUSDT" || (owner.Side != Long && owner.Side != Short) ||
			(owner.Source != Manual && owner.Source != Model) || (owner.Source == Model && !modelIDPattern.MatchString(owner.ModelID)) ||
			(owner.Source == Manual && owner.ModelID != "") {
			block("지원하지 않는 소유자/심볼 정보")
			return r
		}
		key := recoveryOwnerKey(owner.Source, owner.ModelID, owner.Side)
		qty, err := pos(owner.Quantity)
		if err != nil || ownerByKey[key].Symbol != "" {
			block("소유권 중복 또는 수량 오류")
			return r
		}
		ownerByKey[key] = owner
		ownerQty[owner.Side].Add(ownerQty[owner.Side], qty)
	}
	// The exchange provides ONE pooled position for each Hedge side. More
	// than one independent protection group per side is a hard stop.
	sideCounts := map[PositionSide]int{}
	planByGroup := make(map[string]ProtectionDispatchPlan)
	allIDs := map[string]bool{}
	for _, p := range plans {
		if err := p.validate(); err != nil {
			block("보호 계획 체크섬/내용 손상")
			return r
		}
		if _, dup := planByGroup[p.GroupID]; dup {
			block("보호 그룹 중복")
			return r
		}
		if allIDs[p.TP.ClientAlgoID] || allIDs[p.SL.ClientAlgoID] {
			block("clientAlgoId 중복")
			return r
		}
		allIDs[p.TP.ClientAlgoID], allIDs[p.SL.ClientAlgoID] = true, true
		planByGroup[p.GroupID] = p
		sideCounts[p.PositionSide]++
		key := recoveryOwnerKey(p.Source, p.ModelID, p.PositionSide)
		o, ok := ownerByKey[key]
		a, e1 := pos(o.Quantity)
		z, e2 := pos(p.TP.Quantity)
		if !ok || e1 != nil || e2 != nil || a.Cmp(z) != 0 {
			block("보호 계획과 소유권 수량 불일치")
			return r
		}
	}
	if len(owners) != len(plans) {
		block("소유자/보호 그룹 개수 불일치")
		return r
	}
	if sideCounts[Long] > 1 || sideCounts[Short] > 1 {
		block("동일 Hedge 방향 복수 그룹의 독립 TP/SL 발동 경합")
		return r
	}
	observedSide := map[PositionSide]*big.Rat{Long: new(big.Rat), Short: new(big.Rat)}
	seenSide := map[PositionSide]bool{}
	for _, p := range b.Portfolio.Positions {
		q, ok := new(big.Rat).SetString(string(p.PositionAmt))
		if !ok {
			block("계정 포지션 숫자 오류")
			return r
		}
		if p.Symbol != "BTCUSDT" || p.PositionSide == "BOTH" {
			if q.Sign() != 0 {
				block("다른 종목 또는 BOTH 포지션 활성")
				return r
			}
			continue
		}
		side := PositionSide(p.PositionSide)
		if side != Long && side != Short || seenSide[side] || (side == Long && q.Sign() < 0) || (side == Short && q.Sign() > 0) {
			block("Hedge 포지션 중복/부호 오류")
			return r
		}
		seenSide[side] = true
		observedSide[side].Abs(q)
	}
	for _, side := range []PositionSide{Long, Short} {
		if sideCounts[side] == 0 && observedSide[side].Sign() != 0 {
			block("보호 계획 없는 반대방향 포지션 발견")
			return r
		}
	}
	byLeg := make(map[string]ProtectionRecoveryLeg)
	for _, l := range legs {
		if l.Kind != "tp" && l.Kind != "sl" {
			block("알 수 없는 레그 종류")
			return r
		}
		if _, ok := planByGroup[l.GroupID]; !ok {
			block("원본 계획 없는 고아 레그")
			return r
		}
		key := l.GroupID + "|" + l.Kind
		if _, dup := byLeg[key]; dup {
			block("레그 관측 중복")
			return r
		}
		byLeg[key] = l
	}
	// The full open-algo inventory must have exactly one identity and must
	// never contain an unmanaged order, even on another symbol.
	openByID := map[int64]exchange.AlgoOrder{}
	for _, a := range b.OpenAlgos {
		if a.AlgoID <= 0 || openByID[a.AlgoID].AlgoID != 0 {
			block("전체 계정 Algo ID 중복/오류")
			return r
		}
		openByID[a.AlgoID] = a
	}
	recognizedOpen := map[int64]bool{}
	consumedTrades := map[int64]bool{}
	for _, p := range plans {
		group := ProtectionRecoveryGroup{GroupID: p.GroupID, State: RecoveryReviewBlocked, OriginalQty: p.TP.Quantity, ObservedExitQty: "0", RemainingQty: p.TP.Quantity, Blockers: []string{}}
		bleg, hasTP := byLeg[p.GroupID+"|tp"]
		sleg, hasSL := byLeg[p.GroupID+"|sl"]
		if !hasTP || !hasSL {
			group.State = RecoveryReviewOrphan
			group.Blockers = append(group.Blockers, "TP/SL signed 개별 관측이 누락되었습니다")
			r.Groups = append(r.Groups, group)
			continue
		}
		invalid := false
		total := new(big.Rat)
		original, _ := pos(p.TP.Quantity)
		triggered := 0
		active := 0
		canceled := 0
		partial := false
		for _, l := range []ProtectionRecoveryLeg{bleg, sleg} {
			inst := p.TP
			if l.Kind == "sl" {
				inst = p.SL
			}
			o := l.Observation
			if err := assessObs(o, inst, now); err != nil || o.ObservedAt.Before(b.StartedAt.Add(-5*time.Second)) ||
				o.ObservedAt.After(b.FinishedAt.Add(time.Second)) || !validKnownProtectionStatus(o.Status) {
				invalid = true
				group.Blockers = append(group.Blockers, "signed 레그 관측 시간/ID/수량/상태 오류")
				continue
			}
			// An observed active leg must exist with the same immutable
			// instruction in the current full open inventory.
			if isActiveProtection(o.Status) {
				active++
				a, ok := openByID[o.AlgoID]
				if !ok || !sameOpenProtectionAlgo(a, inst, o.AlgoID) || recognizedOpen[o.AlgoID] {
					invalid = true
					group.Blockers = append(group.Blockers, "활성 레그 누락/변조/중복 (orphan)")
				} else {
					recognizedOpen[o.AlgoID] = true
				}
			} else if _, stillOpen := openByID[o.AlgoID]; stillOpen {
				invalid = true
				group.Blockers = append(group.Blockers, "종료 레그가 open inventory에 남아 있습니다")
			}
			if isTriggeredProtection(o.Status) {
				triggered++
			}
			if isCanceledProtection(o.Status) {
				canceled++
			}
			if o.Status == "TRIGGERING" || o.Status == "EXPIRED" || o.Status == "REJECTED" {
				invalid = true
				group.Blockers = append(group.Blockers, "중간/거부된 보호주문은 수동 조회가 필요합니다")
			}
			execQty := new(big.Rat)
			actualQty := new(big.Rat)
			if o.ActualQty != "" {
				var err error
				actualQty, err = signedRat(o.ActualQty)
				if err != nil {
					actualQty = new(big.Rat)
					invalid = true
					group.Blockers = append(group.Blockers, "actualQty 숫자 오류")
				} else if actualQty.Sign() < 0 {
					invalid = true
					group.Blockers = append(group.Blockers, "actualQty 숫자 오류")
				}
			}
			if len(l.Trades) > 0 && (o.ActualOrderID == "" || o.ActualOrderID == "0") {
				invalid = true
				group.Blockers = append(group.Blockers, "체결이 있으나 actualOrderId 누락")
			}
			for _, tr := range l.Trades {
				id, err := strconv.ParseInt(o.ActualOrderID, 10, 64)
				wantSide := "SELL"
				if p.PositionSide == Short {
					wantSide = "BUY"
				}
				if err != nil || id <= 0 || tr.OrderID != id || tr.TradeID <= 0 || tr.Symbol != "BTCUSDT" ||
					tr.PositionSide != string(p.PositionSide) || tr.Side != wantSide || tr.CommissionAsset == "" || tr.TimeMS <= 0 || consumedTrades[tr.TradeID] {
					invalid = true
					group.Blockers = append(group.Blockers, "중복/외부/방향 불일치 체결")
					continue
				}
				q, er := pos(tr.Quantity)
				price, pe := pos(tr.Price)
				_, ce := signedRat(tr.Commission)
				_, re := signedRat(tr.RealizedPnL)
				if er != nil || pe != nil || ce != nil || re != nil || price.Sign() <= 0 {
					invalid = true
					group.Blockers = append(group.Blockers, "체결 가격/수량/손익 오류")
					continue
				}
				// The bounded account trade response must contain precisely
				// the signed order readback trade details.
				present := false
				for _, fromAccount := range b.RecentBTCTrades {
					if reflect.DeepEqual(tr, fromAccount) {
						present = true
						break
					}
				}
				if !present {
					invalid = true
					group.Blockers = append(group.Blockers, "전체 계정 bounded 체결 증거 누락")
					continue
				}
				consumedTrades[tr.TradeID] = true
				execQty.Add(execQty, q)
			}
			if execQty.Cmp(actualQty) != 0 || (execQty.Sign() > 0 && !isTriggeredProtection(o.Status)) {
				invalid = true
				group.Blockers = append(group.Blockers, "signed actualQty와 개별 체결 합계 불일치")
			}
			total.Add(total, execQty)
			if actualQty.Sign() > 0 && actualQty.Cmp(original) < 0 {
				partial = true
			}
		}
		group.ObservedExitQty = formatOwnershipDecimal(total)
		remaining := new(big.Rat).Sub(original, total)
		group.RemainingQty = formatOwnershipDecimal(remaining)
		if total.Cmp(original) > 0 || remaining.Sign() < 0 {
			invalid = true
			group.Blockers = append(group.Blockers, "보호 주문 과다 청산 가능성")
		}
		// UNKNOWN open Algos are checked after all group observations.
		switch {
		case invalid:
			group.State = RecoveryReviewOrphan
		case triggered == 2:
			group.State = RecoveryReviewDouble
			group.Blockers = append(group.Blockers, "TP/SL 이중 발동: 자동 취소 및 재시도 금지")
		case partial || total.Sign() > 0 && total.Cmp(original) < 0:
			group.State = RecoveryReviewPartial
			group.Blockers = append(group.Blockers, "부분 체결/잔량 발생: 형제 주문 자동 재전송 금지")
		case active == 2 && triggered == 0 && canceled == 0 && total.Sign() == 0:
			group.State = RecoveryReviewMonitoring
		case triggered == 1 && active == 1 && total.Cmp(original) == 0:
			group.State = RecoveryReviewCancelReview
			group.Blockers = append(group.Blockers, "형제 레그 취소는 별도 signed 재조회와 단일 취소 인텐트 필요")
		case triggered == 1 && canceled == 1 && total.Cmp(original) == 0:
			group.State = RecoveryReviewClosed
			group.Blockers = append(group.Blockers, "bounded 관측상 청산: 금융 정산·펀딩 검증 완료 아님")
		default:
			group.State = RecoveryReviewOrphan
			group.Blockers = append(group.Blockers, "레그 누락/종료/상태 전이 불명확")
		}
		// Side quantity is reconciled only from known bounded trades; any
		// external actor or missing history forces HALT.
		if group.State == RecoveryReviewMonitoring || group.State == RecoveryReviewCancelReview || group.State == RecoveryReviewClosed {
			want := new(big.Rat).Sub(ownerQty[p.PositionSide], total)
			if observedSide[p.PositionSide].Cmp(want) != 0 {
				group.State = RecoveryReviewBlocked
				group.Blockers = append(group.Blockers, "pooled-side 포지션과 복구 잔량 불일치")
			}
		}
		r.Groups = append(r.Groups, group)
	}
	for id := range openByID {
		if !recognizedOpen[id] {
			r.UnexpectedAlgoIDs = append(r.UnexpectedAlgoIDs, id)
			block("원본 보호 계획에 없는 활성 조건부 주문")
		}
	}
	sort.Slice(r.UnexpectedAlgoIDs, func(i, j int) bool { return r.UnexpectedAlgoIDs[i] < r.UnexpectedAlgoIDs[j] })
	// Even an unmatched entry trade can change the pooled-side quantity.
	// Absence from a *bounded* window does not establish full history.
	accountTradeIDs := map[int64]bool{}
	for _, tr := range b.RecentBTCTrades {
		if tr.TradeID <= 0 || accountTradeIDs[tr.TradeID] {
			block("bounded signed 체결 ID 중복/손상")
		}
		accountTradeIDs[tr.TradeID] = true
		if !consumedTrades[tr.TradeID] {
			block(fmt.Sprintf("복구 그룹 외 체결 발견 (trade_id=%d): pooled 소유권 재검증", tr.TradeID))
		}
	}
	for _, g := range r.Groups {
		if g.State != RecoveryReviewMonitoring && g.State != RecoveryReviewCancelReview && g.State != RecoveryReviewClosed {
			block("보호 그룹 복구 수동 확인 필요: " + g.GroupID + " " + string(g.State))
		}
	}
	if len(r.Blockers) == 0 {
		r.State = "REVIEW_ONLY"
	}
	return r
}
func recoveryOwnerKey(src Source, modelID string, side PositionSide) string {
	return string(src) + "|" + modelID + "|" + string(side)
}
