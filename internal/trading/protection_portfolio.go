package trading

// This is a READ-ONLY review of an entire authenticated REST observation.
// Passing means the *observed* open Algo inventory and current exchange side
// quantities agree with the imported ownership ledger at one bounded moment.
// It never permits placement: two independent stop/TP legs are NOT OCO, and
// even immediately following this review the account may already have changed.
import (
	"binance_trader/internal/exchange"
	"fmt"
	"math/big"
	"time"
)

type ProtectionPortfolioReview struct {
	State                string    `json:"state"` // BLOCKED / EMPTY / OBSERVED_MATCH_NOT_OCO
	ObservedAt           time.Time `json:"observed_at"`
	OwnerCount           int       `json:"owner_count"`
	ActiveAlgoCount      int       `json:"active_algo_count"`
	LongQty              string    `json:"long_qty"`
	ShortQty             string    `json:"short_qty"`
	Blockers             []string  `json:"blockers"`
	TradingAuthorized    bool      `json:"trading_authorized"`     // ALWAYS false
	ManualReviewRequired bool      `json:"manual_review_required"` // ALWAYS true
}

// ReviewProtectionPortfolio uses the *account-wide*, signed and bracketed REST
// baseline, never the display snapshot (which queries only BTCUSDT). All
// balances, positions, open standard and Algo orders must have been returned;
// nil slices are treated as incomplete. A verified ownership audit is a
// prerequisite supplied by the caller, not inferred from matching quantities.
// Future trading requires a fresh live stream epoch and a separate safety
// policy; this function deliberately cannot return an authorization token.
func ReviewProtectionPortfolio(b exchange.RecoveryBaseline, owned []OwnerPosition, expected []ExpectedProtection, ownershipAuditVerified bool, now time.Time) ProtectionPortfolioReview {
	r := ProtectionPortfolioReview{State: "BLOCKED", ObservedAt: b.FinishedAt,
		ManualReviewRequired: true, Blockers: []string{}, LongQty: "0", ShortQty: "0"}
	block := func(reason string) ProtectionPortfolioReview {
		r.Blockers = append(r.Blockers, reason)
		return r
	}
	if !ownershipAuditVerified {
		return block("독립적인 실제 체결·소유권 검증이 필요합니다")
	}
	if b.Environment != "demo" || b.Coverage != "BOUNDED_HISTORY_ONLY_NOT_CONTINUOUS" ||
		b.StartedAt.IsZero() || b.FinishedAt.Before(b.StartedAt) || b.WindowStartMS <= 0 ||
		b.WindowEndMS != b.StartedAt.UnixMilli() || b.WindowStartMS >= b.WindowEndMS ||
		b.FinishedAt.Sub(b.StartedAt) > 30*time.Second || b.FinishedAt.After(now.Add(time.Second)) ||
		now.Sub(b.FinishedAt) > 5*time.Second || b.OpenOrders == nil || b.OpenAlgos == nil ||
		b.RecentBTCOrders == nil || b.RecentBTCAlgos == nil || b.RecentBTCTrades == nil || b.RecentIncome == nil {
		return block("완전하고 최신의 Demo 전체 계정 REST 진단 기록이 필요합니다")
	}
	if err := exchange.ValidatePortfolioSnapshot(b.Portfolio); err != nil {
		return block("전체 계정 자산/포지션 정보 오류")
	}
	if !b.Portfolio.CanTrade || b.Portfolio.MultiAssetsMargin {
		return block("거래 불가 또는 다중 자산 증거금 계정은 지원하지 않습니다")
	}
	// Reject foreign standard orders, even if they are on BTCUSDT: a limit
	// exit can execute while an Algo protection is also triggering.
	if len(b.OpenOrders) != 0 {
		return block("미체결 일반 주문이 존재합니다: 동시 청산 가능성 조사 필요")
	}
	// Any non-USDT collateral allocation needs its own full margin model.
	for _, a := range b.Portfolio.Assets {
		if a.Asset != "USDT" && a.MarginBalance != "0" && a.MarginBalance != "0.0" && a.MarginBalance != "0.00000000" {
			v, err := signedRat(string(a.MarginBalance))
			if err != nil || v.Sign() != 0 {
				return block("다른 자산의 증거금이 존재합니다")
			}
		}
	}
	exchangeSides := map[PositionSide]*big.Rat{Long: new(big.Rat), Short: new(big.Rat)}
	observedSides := map[PositionSide]bool{}
	for _, p := range b.Portfolio.Positions {
		qty, err := signedRat(string(p.PositionAmt))
		if err != nil {
			return block("거래소 포지션 수량 오류")
		}
		if p.Symbol != "BTCUSDT" {
			if qty.Sign() != 0 {
				return block("BTC 이외 종목의 포지션이 존재합니다")
			}
			continue
		}
		if p.PositionSide == "BOTH" {
			if qty.Sign() != 0 {
				return block("단방향 BTC 포지션이 존재합니다")
			}
			continue
		}
		side := PositionSide(p.PositionSide)
		if observedSides[side] {
			return block("거래소 BTC 방향별 포지션이 중복되었습니다")
		}
		observedSides[side] = true
		if (side == Long && qty.Sign() < 0) || (side == Short && qty.Sign() > 0) {
			return block("거래소 Hedge 포지션 부호가 잘못되었습니다")
		}
		exchangeSides[side].Abs(qty)
	}
	ownedSides := map[PositionSide]*big.Rat{Long: new(big.Rat), Short: new(big.Rat)}
	ownerIDs := map[string]bool{}
	for _, p := range owned {
		if p.Symbol != "BTCUSDT" || (p.Source != Model && p.Source != Manual) ||
			(p.Source == Model && !modelIDPattern.MatchString(p.ModelID)) ||
			(p.Source == Manual && p.ModelID != "") || (p.Side != Long && p.Side != Short) {
			return block("알 수 없는 소유자 정보")
		}
		key := string(p.Source) + "|" + p.ModelID + "|" + string(p.Side)
		if ownerIDs[key] {
			return block("동일 소유자의 포지션이 중복되었습니다")
		}
		ownerIDs[key] = true
		qty, err := pos(p.Quantity)
		if err != nil {
			return block("소유자 수량 오류")
		}
		ownedSides[p.Side].Add(ownedSides[p.Side], qty)
	}
	r.OwnerCount = len(owned)
	r.LongQty = formatOwnershipDecimal(ownedSides[Long])
	r.ShortQty = formatOwnershipDecimal(ownedSides[Short])
	if ownedSides[Long].Cmp(exchangeSides[Long]) != 0 || ownedSides[Short].Cmp(exchangeSides[Short]) != 0 {
		return block(fmt.Sprintf("거래소/소유자 포지션 불일치 (LONG %s / SHORT %s BTC는 소유 장부 기준)", r.LongQty, r.ShortQty))
	}
	inventory := exchange.Snapshot{Status: "connected", Environment: "demo", AsOf: b.FinishedAt.Format(time.RFC3339Nano), AlgoOrders: b.OpenAlgos}
	observed := ReviewProtectionInventory(inventory, owned, expected, now)
	if observed.Status == "BLOCKED" {
		return block("조건부 주문 소유권/수량 검증 실패: " + observed.Issues[0])
	}
	r.ActiveAlgoCount = len(b.OpenAlgos)
	if observed.Status == "EMPTY" {
		r.State = "EMPTY"
		r.Blockers = append(r.Blockers, "현재 빈 계정의 과거 5초 관측일 뿐, 외부 주문/스트림 단절이 없다는 증거는 아닙니다")
	} else {
		r.State = "OBSERVED_MATCH_NOT_OCO"
		r.Blockers = append(r.Blockers, "동일 방향의 여러 소유자 TP/SL은 독립 주문이며 원자적 OCO가 아닙니다. 실제 거래 활성화 금지")
	}
	return r
}
