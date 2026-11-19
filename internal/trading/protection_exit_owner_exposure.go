package trading

// Read-only, fail-closed comparison of a signed exit-fill review, immutable
// protection ownership plan, historical local ownership snapshot and a bounded
// account-wide REST observation. This does not attribute individual exchange
// fills to owners: Binance Hedge positions are pooled by side. Caller-provided
// baseline and ownership data are NOT authenticated by this function; no
// settlement or trading permission is ever generated.
import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"binance_trader/internal/exchange"
)

type ExitOwnerExposureReview struct {
	GroupID                 string       `json:"group_id"`
	State                   string       `json:"state"`
	PositionSide            PositionSide `json:"position_side"`
	PlannedOwnerQty         string       `json:"planned_owner_qty"`
	SignedExitQty           string       `json:"signed_exit_qty"`
	OtherOwnerQty           string       `json:"other_owner_qty"`
	ObservedExchangeSideQty string       `json:"observed_exchange_side_qty"`
	SameSideOwners          int          `json:"same_side_owners"`
	Blockers                []string     `json:"blockers"`
	TradingAuthorized       bool         `json:"trading_authorized"`     // ALWAYS false
	FinanciallySettled      bool         `json:"financially_settled"`    // ALWAYS false
	ManualReviewRequired    bool         `json:"manual_review_required"` // ALWAYS true
}

// ReviewExitOwnerExposure is a historical consistency check, NOT a financial
// posting gate. owned must represent an independently verified pre-exit local
// ownership snapshot; the caller must attest provenance independently.
// Current portfolio balance is not proof of the absence of intervening trades.
func ReviewExitOwnerExposure(plan ProtectionDispatchPlan, exit SignedExitFillReview, owned []OwnerPosition, baseline exchange.RecoveryBaseline, now time.Time) (ExitOwnerExposureReview, error) {
	out := ExitOwnerExposureReview{GroupID: plan.GroupID, State: "HALT", PositionSide: plan.PositionSide,
		ManualReviewRequired: true, PlannedOwnerQty: "0", SignedExitQty: "0", OtherOwnerQty: "0", ObservedExchangeSideQty: "0"}
	fail := func(reason string) (ExitOwnerExposureReview, error) {
		out.Blockers = append(out.Blockers, reason)
		return out, errors.New(reason)
	}
	if err := plan.validate(); err != nil {
		return fail("보호 계획 검증 실패: " + err.Error())
	}
	planned, err := positiveRat(plan.TP.Quantity)
	if err != nil {
		return fail("보호 계획 수량 오류")
	}
	out.PlannedOwnerQty = formatOwnershipDecimal(planned)
	if exit.GroupID != plan.GroupID || exit.TradingAuthorized || exit.FinanciallySettled || !exit.ManualReviewRequired ||
		exit.State != "EXIT_FILLS_MATCH_NOT_SETTLED" || exit.TradeCount <= 0 {
		return fail("검증된 단일 종료 체결 상태가 아닙니다")
	}
	signed, err := positiveRat(exit.TotalFilledQty)
	if err != nil || signed.Cmp(planned) != 0 {
		return fail("종료 체결과 보호 수량 불일치")
	}
	out.SignedExitQty = formatOwnershipDecimal(signed)
	tp, err := decimalRat(exit.TPFilledQty, true)
	if err != nil {
		return fail("TP 체결 수량 오류")
	}
	sl, err := decimalRat(exit.SLFilledQty, true)
	if err != nil {
		return fail("SL 체결 수량 오류")
	}
	if new(big.Rat).Add(tp, sl).Cmp(signed) != 0 || (tp.Sign() > 0 && sl.Sign() > 0) {
		return fail("TP/SL 동시 체결 또는 합계 불일치")
	}
	if baseline.Environment != "demo" || baseline.Coverage != "BOUNDED_HISTORY_ONLY_NOT_CONTINUOUS" ||
		baseline.StartedAt.IsZero() || baseline.FinishedAt.Before(baseline.StartedAt) ||
		baseline.FinishedAt.After(now.Add(time.Second)) || now.Sub(baseline.FinishedAt) > 5*time.Second ||
		baseline.FinishedAt.Sub(baseline.StartedAt) > 30*time.Second ||
		baseline.WindowStartMS <= 0 || baseline.WindowEndMS != baseline.StartedAt.UnixMilli() || baseline.WindowStartMS >= baseline.WindowEndMS ||
		baseline.OpenOrders == nil || baseline.OpenAlgos == nil || baseline.RecentBTCOrders == nil ||
		baseline.RecentBTCAlgos == nil || baseline.RecentBTCTrades == nil || baseline.RecentIncome == nil {
		return fail("불완전하거나 오래된 전체 Demo signed REST 관측")
	}
	if err := exchange.ValidatePortfolioSnapshot(baseline.Portfolio); err != nil {
		return fail("계정 포지션·자산 증거 오류")
	}
	if len(baseline.OpenOrders) != 0 || len(baseline.OpenAlgos) != 0 {
		return fail("취소되지 않은 계정 주문이 존재")
	}
	if len(owned) == 0 {
		return fail("독립적으로 검증된 소유자 정보 없음")
	}
	other := new(big.Rat)
	targetFound := false
	unique := make(map[string]bool)
	for _, p := range owned {
		if p.Symbol != "BTCUSDT" || (p.Side != Long && p.Side != Short) ||
			(p.Source != Model && p.Source != Manual) || (p.Source == Model && !modelIDPattern.MatchString(p.ModelID)) ||
			(p.Source == Manual && p.ModelID != "") {
			return fail("비정상 소유자 기록")
		}
		key := string(p.Source) + "|" + p.ModelID + "|" + string(p.Side)
		if unique[key] {
			return fail("소유자 기록 중복")
		}
		unique[key] = true
		qty, err := positiveRat(p.Quantity)
		if err != nil {
			return fail("소유자 수량 오류")
		}
		if p.Side != plan.PositionSide {
			continue
		}
		out.SameSideOwners++
		if p.Source == plan.Source && p.ModelID == plan.ModelID {
			targetFound = true
			if qty.Cmp(planned) != 0 {
				return fail("보호계획과 해당 소유자 수량 불일치")
			}
		} else {
			other.Add(other, qty)
		}
	}
	if !targetFound {
		return fail("보호계획의 소유자가 로컬 원장에 없음")
	}
	out.OtherOwnerQty = formatOwnershipDecimal(other)
	observed := new(big.Rat)
	foundSide := false
	for _, p := range baseline.Portfolio.Positions {
		qty, err := signedRat(string(p.PositionAmt))
		if err != nil {
			return fail("거래소 포지션 숫자 오류")
		}
		if p.Symbol != "BTCUSDT" {
			if qty.Sign() != 0 {
				return fail("BTC 외 종목 잔량 존재")
			}
			continue
		}
		if p.PositionSide == "BOTH" {
			if qty.Sign() != 0 {
				return fail("One-way 포지션이 존재")
			}
			continue
		}
		if p.PositionSide == string(plan.PositionSide) {
			if foundSide {
				return fail("중복된 동일 방향 거래소 포지션")
			}
			foundSide = true
			if (plan.PositionSide == Long && qty.Sign() < 0) || (plan.PositionSide == Short && qty.Sign() > 0) {
				return fail("거래소 포지션 부호 오류")
			}
			observed.Abs(qty)
		}
	}
	out.ObservedExchangeSideQty = formatOwnershipDecimal(observed)
	if other.Sign() > 0 {
		out.State = "POOLED_SIDE_AMBIGUOUS_NOT_SETTLED"
		out.Blockers = []string{fmt.Sprintf("동일 Hedge 방향에 다른 소유자 %s BTC: 체결의 실제 귀속은 전체 이력 없이는 불명", out.OtherOwnerQty)}
		return out, nil
	}
	if observed.Sign() != 0 {
		out.State = "EXCHANGE_SIDE_NOT_FLAT_NOT_SETTLED"
		out.Blockers = []string{"서명 관측상 동일 방향 잔량 존재: 이력 및 미정리 체결 확인 필요"}
		return out, nil
	}
	out.State = "SINGLE_OWNER_FLAT_OBSERVED_NOT_SETTLED"
	out.Blockers = []string{"한 시점의 Flat 관측일 뿐 주문 경합·누락 이력·펀딩·정산은 검증되지 않음"}
	return out, nil
}

// InspectSignedExitOwnerExposure obtains signed TP/SL evidence through the
// existing reader instead of trusting a supplied exit-review object. The
// baseline and local owner records STILL require separate provenance checks.
func InspectSignedExitOwnerExposure(ctx context.Context, submissions *ProtectionSubmissionStore,
	reader SignedExitFillReader, creds DemoCredentials, group string, tpID, slID int64,
	owned []OwnerPosition, baseline exchange.RecoveryBaseline, now time.Time) (ExitOwnerExposureReview, error) {
	closed := ExitOwnerExposureReview{GroupID: group, State: "HALT", ManualReviewRequired: true,
		Blockers: []string{"signed 체결 검증에 실패했습니다"}}
	if submissions == nil || reader == nil {
		return closed, errors.New("보호주문 signed 조회기 누락")
	}
	plan, err := submissions.read(group)
	if err != nil {
		return closed, err
	}
	exit, err := InspectSignedExitFills(ctx, submissions, reader, creds, group, tpID, slID)
	if err != nil {
		return closed, err
	}
	return ReviewExitOwnerExposure(plan, exit, owned, baseline, now)
}
