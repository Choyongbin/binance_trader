package trading

// Independent, read-only signed reconstruction of both conditional exit legs.
// This is NOT ownership reconciliation: pooled Hedge-side positions, historical
// manual closes and funding must be audited separately before any posting.
import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strconv"

	"binance_trader/internal/exchange"
)

type SignedExitFillReader interface {
	SignedProtectionReader
	FetchDemoOrderTrades(context.Context, string, string, string, int64) ([]exchange.AccountTrade, error)
}

type SignedExitFillReview struct {
	GroupID              string `json:"group_id"`
	State                string `json:"state"`
	TPFilledQty          string `json:"tp_filled_qty"`
	SLFilledQty          string `json:"sl_filled_qty"`
	TotalFilledQty       string `json:"total_filled_qty"`
	OwnerPlannedQty      string `json:"owner_planned_qty"`
	TradeCount           int    `json:"trade_count"`
	TradingAuthorized    bool   `json:"trading_authorized"`     // ALWAYS false
	FinanciallySettled   bool   `json:"financially_settled"`    // ALWAYS false
	ManualReviewRequired bool   `json:"manual_review_required"` // ALWAYS true
	Reason               string `json:"reason"`
}

// InspectSignedExitFills never writes to any ledger or submits/cancels orders.
// Re-reads both Algo legs before and after trade lookup; missing, duplicated,
// unstable, or overfilled signed evidence fails closed. Even exact totals do
// NOT establish the owner, funding attribution or current exchange position.
func InspectSignedExitFills(ctx context.Context, submissions *ProtectionSubmissionStore, reader SignedExitFillReader,
	creds DemoCredentials, group string, tpID, slID int64) (SignedExitFillReview, error) {
	out := SignedExitFillReview{GroupID: group, State: "HALT", TPFilledQty: "0", SLFilledQty: "0", TotalFilledQty: "0", ManualReviewRequired: true}
	if submissions == nil || reader == nil || creds.APIKey == "" || creds.Secret == "" || group == "" || tpID <= 0 || slID <= 0 || tpID == slID {
		return out, errors.New("signed exit 검증 입력 누락 또는 중복 Algo ID")
	}
	plan, err := submissions.read(group)
	if err != nil {
		return out, err
	}
	owner, err := positiveRat(plan.TP.Quantity)
	if err != nil {
		return out, err
	}
	slQty, err := positiveRat(plan.SL.Quantity)
	if err != nil || owner.Cmp(slQty) != 0 {
		return out, errors.New("보호 레그 소유 수량 불일치")
	}
	out.OwnerPlannedQty = formatOwnershipDecimal(owner)
	seen := make(map[int64]bool)
	total := new(big.Rat)
	statuses := make([]string, 0, 2)
	orders := make(map[int64]bool)
	for _, leg := range []struct {
		name string
		id   int64
	}{{"tp", tpID}, {"sl", slID}} {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		first, err := reader.QueryDemoAlgo(ctx, creds.APIKey, creds.Secret, leg.id)
		if err != nil {
			return out, fmt.Errorf("%s 최초 signed GET: %w", leg.name, err)
		}
		if err := validateProtectionAlgo(plan, leg.name, leg.id, first); err != nil {
			return out, err
		}
		if first.Status == "REJECTED" {
			return out, errors.New("REJECTED 보호주문")
		}
		actual, err := decimalRat(string(first.ActualQty), true)
		if err != nil {
			return out, err
		}
		sum := new(big.Rat)
		if actual.Sign() > 0 {
			orderID, err := strconv.ParseInt(first.ActualOrderID, 10, 64)
			if err != nil || orderID <= 0 || orders[orderID] {
				return out, errors.New("보호 레그 actual order ID 누락 또는 중복")
			}
			orders[orderID] = true
			trades, err := reader.FetchDemoOrderTrades(ctx, creds.APIKey, creds.Secret, "BTCUSDT", orderID)
			if err != nil {
				return out, fmt.Errorf("%s signed trades: %w", leg.name, err)
			}
			for _, trade := range trades {
				if seen[trade.TradeID] {
					return out, errors.New("보호 레그 간 trade ID 중복")
				}
				if err := validateProtectionTrade(plan, leg.name, first, trade); err != nil {
					return out, err
				}
				seen[trade.TradeID] = true
				qty, _ := positiveRat(trade.Quantity)
				sum.Add(sum, qty)
			}
			if sum.Cmp(actual) != 0 {
				return out, errors.New("signed actualQty와 실제 체결 합계 불일치")
			}
		} else if first.ActualOrderID != "" && first.ActualOrderID != "0" {
			// An order id may exist without a fill; it cannot prove a close.
			if _, err := strconv.ParseInt(first.ActualOrderID, 10, 64); err != nil {
				return out, err
			}
		}
		second, err := reader.QueryDemoAlgo(ctx, creds.APIKey, creds.Secret, leg.id)
		if err != nil {
			return out, fmt.Errorf("%s 재검증 signed GET: %w", leg.name, err)
		}
		if err := validateProtectionAlgo(plan, leg.name, leg.id, second); err != nil {
			return out, err
		}
		if first.Status != second.Status || first.ActualOrderID != second.ActualOrderID || string(first.ActualQty) != string(second.ActualQty) {
			return out, errors.New("체결 조회 중 보호 레그 상태 변경")
		}
		statuses = append(statuses, second.Status)
		total.Add(total, sum)
		if total.Cmp(owner) > 0 {
			return out, errors.New("TP/SL 총 체결량이 소유 수량 초과: 과청산 위험")
		}
		if leg.name == "tp" {
			out.TPFilledQty = formatOwnershipDecimal(sum)
		} else {
			out.SLFilledQty = formatOwnershipDecimal(sum)
		}
	}
	out.TotalFilledQty = formatOwnershipDecimal(total)
	out.TradeCount = len(seen)
	if statuses[0] == "FINISHED" && statuses[1] == "FINISHED" {
		out.State = "DOUBLE_TRIGGER_MANUAL_RECONCILIATION"
		out.Reason = "TP/SL 동시 종료: 계정 전체 체결·잔량 조사 필요"
	} else if total.Cmp(owner) < 0 {
		out.State = "PARTIAL_OR_UNFILLED_MANUAL_RECONCILIATION"
		out.Reason = "소유 수량에 비해 exit fill 부족: 잔량과 기존 보호주문 재조회 필요"
	} else if !terminalProtectionEvidence(statuses[0]) || !terminalProtectionEvidence(statuses[1]) {
		out.State = "OPEN_LEG_MANUAL_RECONCILIATION"
		out.Reason = "서명 체결 합계만 일치하며 조건부 주문이 종료되지 않음"
	} else {
		out.State = "EXIT_FILLS_MATCH_NOT_SETTLED"
		out.Reason = "두 레그의 signed 체결 수량만 일치: Ownership·계정·펀딩 원자적 정산은 미검증"
	}
	return out, nil
}
