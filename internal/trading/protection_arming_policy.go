package trading

// This file reviews the LOCAL pre-dispatch protection plan set. Binance Hedge
// mode pools all owners on one LONG or SHORT position side, so two owner-sized
// TP/SL pairs on the same side are not independent risk containers. This
// review therefore blocks staging a second owner pair on a pooled side. It is
// deliberately stricter than the historical read-only inventory diagnostics.
// Nothing here enables live submission or trading.

import (
	"errors"
	"math/big"
	"sort"
)

const (
	ProtectionArmingEmpty               = "EMPTY"
	ProtectionArmingSingleOwnerPairOnly = "SINGLE_OWNER_PER_SIDE_PAIR_NOT_OCO"
	ProtectionArmingPooledConflict      = "POOLED_SIDE_CONFLICT_BLOCKED"
)

// ErrPooledProtectionConflict is returned by the durable submission store when
// more than one distinct protection group would exist on the same Hedge side.
// Old releases may already contain such plans; those archives are retained but
// are fail-closed by AuditAll/Prepare until separately reconciled.
var ErrPooledProtectionConflict = errors.New("동일 Hedge 방향에 둘 이상의 독립 TP/SL 그룹이 존재합니다: pooled 포지션 과청산 경합 때문에 추가 보호 계획을 만들 수 없습니다")

type ProtectionArmingSideReview struct {
	Side                           PositionSide `json:"side"`
	OwnerGroups                    int          `json:"owner_groups"`
	OwnedBTC                       string       `json:"owned_btc"`
	IndependentConditionalLegs     int          `json:"independent_conditional_legs"`
	WorstCaseRequestedCloseBTC     string       `json:"worst_case_requested_close_btc"`
	ExcessIfAllIndependentLegsFire string       `json:"excess_if_all_independent_legs_fire_btc"`
}

type ProtectionArmingReview struct {
	State                     string                       `json:"state"`
	Sides                     []ProtectionArmingSideReview `json:"sides"`
	PlanCount                 int                          `json:"plan_count"`
	SameSideMultiOwnerPresent bool                         `json:"same_side_multi_owner_present"`
	PerOwnerAlgoArmingAllowed bool                         `json:"per_owner_algo_arming_allowed"` // ALWAYS false until live coordinator exists
	TradingAuthorized         bool                         `json:"trading_authorized"`            // ALWAYS false
	ManualReviewRequired      bool                         `json:"manual_review_required"`        // ALWAYS true
	Blockers                  []string                     `json:"blockers"`
}

// ReviewProtectionArmingPlans validates immutable local plans and quantifies
// side-wide close request exposure. Even a single owner pair remains two
// independent non-OCO orders, so live arming is never authorized here. The
// additional hard rule is that a second distinct group on the same Hedge side
// is a pooled-owner conflict and must not be staged.
func ReviewProtectionArmingPlans(plans []ProtectionDispatchPlan) ProtectionArmingReview {
	r := ProtectionArmingReview{State: ProtectionArmingEmpty, Sides: []ProtectionArmingSideReview{}, Blockers: []string{}, ManualReviewRequired: true}
	if len(plans) == 0 {
		r.Blockers = append(r.Blockers, "보호 계획이 없습니다. 빈 로컬 계획 집합은 거래 허가가 아닙니다")
		return r
	}

	type sideAccum struct {
		groups int
		qty    *big.Rat
	}
	bySide := map[PositionSide]*sideAccum{Long: {qty: new(big.Rat)}, Short: {qty: new(big.Rat)}}
	groups := make(map[string]bool)
	owners := make(map[string]bool)
	clientIDs := make(map[string]bool)

	for _, p := range plans {
		if err := p.validate(); err != nil {
			r.State = ProtectionArmingPooledConflict
			r.Blockers = append(r.Blockers, "보호 계획 무결성 검증 실패")
			return r
		}
		if groups[p.GroupID] {
			r.State = ProtectionArmingPooledConflict
			r.Blockers = append(r.Blockers, "보호 그룹 ID 중복")
			return r
		}
		groups[p.GroupID] = true
		ownerKey := string(p.Source) + "|" + p.ModelID + "|" + string(p.PositionSide)
		if owners[ownerKey] {
			r.State = ProtectionArmingPooledConflict
			r.Blockers = append(r.Blockers, "동일 소유자/방향에 복수 보호 그룹이 존재합니다")
			return r
		}
		owners[ownerKey] = true
		for _, id := range []string{p.TP.ClientAlgoID, p.SL.ClientAlgoID} {
			if clientIDs[id] {
				r.State = ProtectionArmingPooledConflict
				r.Blockers = append(r.Blockers, "보호 주문 clientAlgoId가 서로 다른 그룹에서 중복되었습니다")
				return r
			}
			clientIDs[id] = true
		}
		qty, err := pos(p.TP.Quantity)
		if err != nil || qty.Sign() <= 0 {
			r.State = ProtectionArmingPooledConflict
			r.Blockers = append(r.Blockers, "보호 계획 수량 오류")
			return r
		}
		acc := bySide[p.PositionSide]
		if acc == nil {
			r.State = ProtectionArmingPooledConflict
			r.Blockers = append(r.Blockers, "지원하지 않는 Hedge 방향")
			return r
		}
		acc.groups++
		acc.qty.Add(acc.qty, qty)
	}

	r.PlanCount = len(plans)
	for _, side := range []PositionSide{Long, Short} {
		acc := bySide[side]
		if acc.groups == 0 {
			continue
		}
		// Every owner pair consists of two independent close requests. If all
		// legs fired before any sibling cancellation/readback completed, the
		// requested total would be 2x the owned side quantity.
		worst := new(big.Rat).Mul(new(big.Rat).Set(acc.qty), big.NewRat(2, 1))
		excess := new(big.Rat).Sub(new(big.Rat).Set(worst), acc.qty)
		r.Sides = append(r.Sides, ProtectionArmingSideReview{
			Side: side, OwnerGroups: acc.groups, OwnedBTC: formatOwnershipDecimal(acc.qty),
			IndependentConditionalLegs:     acc.groups * 2,
			WorstCaseRequestedCloseBTC:     formatOwnershipDecimal(worst),
			ExcessIfAllIndependentLegsFire: formatOwnershipDecimal(excess),
		})
		if acc.groups > 1 {
			r.SameSideMultiOwnerPresent = true
		}
	}
	sort.Slice(r.Sides, func(i, j int) bool { return r.Sides[i].Side < r.Sides[j].Side })
	if r.SameSideMultiOwnerPresent {
		r.State = ProtectionArmingPooledConflict
		r.Blockers = append(r.Blockers, "동일 Hedge 방향 복수 소유자의 개별 TP/SL 계획을 함께 준비할 수 없습니다")
		r.Blockers = append(r.Blockers, "향후 pooled-side 단일 직렬 청산 coordinator와 독립 서명 재대조가 필요합니다")
		return r
	}
	r.State = ProtectionArmingSingleOwnerPairOnly
	r.Blockers = append(r.Blockers, "방향별 소유자는 하나뿐이지만 TP와 SL은 여전히 독립 주문이며 원자적 OCO가 아닙니다")
	r.Blockers = append(r.Blockers, "실제 전송은 전체 계정 감사·형제 취소·체결 재대조 coordinator 구현 전 금지됩니다")
	return r
}
