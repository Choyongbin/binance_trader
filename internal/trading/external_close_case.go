package trading

// External close cases are FORENSIC, READ-ONLY explanations of potentially
// matching externally submitted closes. They MUST NOT mutate ownership,
// lifecycle, reservations, protection, PnL, or trading permissions. A candidate
// does not establish manual intent, funding completeness, or pre-window history.
import (
	"math/big"
	"time"

	"binance_trader/internal/exchange"
)

const (
	ExternalCloseCandidate = "SIGNED_EXTERNAL_CLOSE_CANDIDATE_REVIEW_ONLY"
	ExternalCloseAmbiguous = "INSUFFICIENT_OR_AMBIGUOUS_EVIDENCE"
)

type ExternalCloseRecoveryCase struct {
	Status                    string       `json:"status"`
	EntryClientOrderID        string       `json:"entry_client_order_id"`
	ModelID                   string       `json:"model_id,omitempty"`
	PositionSide              PositionSide `json:"position_side"`
	EntryOrderID              int64        `json:"signed_entry_order_id,omitempty"`
	ExitOrderID               int64        `json:"signed_external_exit_order_id,omitempty"`
	EntryQuantity             string       `json:"signed_entry_quantity,omitempty"`
	ExitQuantity              string       `json:"signed_exit_quantity,omitempty"`
	SignedRealizedPnL         string       `json:"signed_exit_realized_pnl,omitempty"`
	SignedExitCommission      string       `json:"signed_exit_commission,omitempty"`
	CommissionAsset           string       `json:"commission_asset,omitempty"`
	LocalEntryFillsMatched    bool         `json:"local_entry_fills_matched"`
	HistoricalCoverageLimited bool         `json:"historical_coverage_limited"`
	Blockers                  []string     `json:"blockers"`
	ManualReviewRequired      bool         `json:"manual_review_required"`
	LocalLifecycleChanged     bool         `json:"local_lifecycle_changed"`
	LocalOwnershipChanged     bool         `json:"local_ownership_changed"`
	TradingAuthorized         bool         `json:"trading_authorized"`
}

func buildExternalCloseCases(out SignedLedgerHistoryReport, records []Record, remoteTrades map[int64]exchange.AccountTrade, externalByOrder map[int64][]exchange.AccountTrade, snapshot exchange.Snapshot, now time.Time) []ExternalCloseRecoveryCase {
	cases := make([]ExternalCloseRecoveryCase, 0)
	for _, rec := range records {
		if rec.Intent.Action != Enter {
			continue
		}
		c := ExternalCloseRecoveryCase{
			Status:             ExternalCloseAmbiguous,
			EntryClientOrderID: rec.Plan.ClientOrderID,
			ModelID:            rec.Intent.ModelID, PositionSide: rec.Plan.PositionSide,
			HistoricalCoverageLimited: true,
			ManualReviewRequired:      true,
			Blockers:                  []string{"외부 청산은 모델 소유권/실현손익 확정이 아닙니다. MAY_HAVE_SENT, 미종료 예약 및 거래 잠금을 유지하세요."},
		}
		fail := func(msg string) { c.Blockers = append(c.Blockers, msg) }
		// Binance Hedge mode pools the same side. More than one owned order
		// cannot be safely correlated with one external close by quantity alone.
		if len(records) != 1 || len(out.Orders) != 1 {
			fail("multiple owned orders; pooled-side allocation is ambiguous")
			cases = append(cases, c)
			continue
		}
		entry := out.Orders[0]
		c.EntryOrderID, c.EntryQuantity, c.LocalEntryFillsMatched = entry.SignedOrderID, entry.SignedQty, entry.ExactFillsMatched
		if !out.HistoryWindowComplete || !out.SignedAccountFresh || snapshot.Environment != "demo" || snapshot.Status != "connected" || snapshot.Balances == nil || snapshot.Positions == nil || snapshot.Orders == nil || snapshot.AlgoOrders == nil {
			fail("complete signed history and fresh Demo account snapshot required")
			cases = append(cases, c)
			continue
		}
		if len(snapshot.Orders) != 0 || len(snapshot.AlgoOrders) != 0 {
			fail("open standard/algo orders remain")
			cases = append(cases, c)
			continue
		}
		if t, e := time.Parse(time.RFC3339Nano, snapshot.AsOf); e != nil || t.After(now.Add(time.Second)) || now.Sub(t) > 5*time.Second {
			fail("signed account snapshot stale or invalid")
			cases = append(cases, c)
			continue
		}
		longQty, shortQty, err := signedSideQuantities(snapshot)
		if err != nil || longQty != "0" || shortQty != "0" {
			fail("signed BTCUSDT account is not flat or its position data is invalid")
			cases = append(cases, c)
			continue
		}
		if entry.SignedOrderID <= 0 || entry.Status != "FILLED" || entry.SignedTradeCount == 0 {
			fail("entry is not a fully signed FILLED order with verified trade count")
			cases = append(cases, c)
			continue
		}
		// Missing imported LOCAL fills are allowed ONLY as an explicit
		// human-review blocker; the signed entry must be independently sound.
		if len(entry.Blockers) > 0 && !(len(entry.Blockers) == 1 && entry.Blockers[0] == "signed fills do not match immutable local fill records exactly") {
			fail("signed entry has unrelated verification blockers")
			cases = append(cases, c)
			continue
		}
		expected, err := positiveRat(entry.SignedQty)
		if err != nil {
			fail("invalid entry signed quantity")
			cases = append(cases, c)
			continue
		}
		planned, err := positiveRat(rec.Plan.Quantity)
		if err != nil || planned.Cmp(expected) != 0 {
			fail("signed entry quantity differs from immutable planned quantity")
			cases = append(cases, c)
			continue
		}
		if len(externalByOrder) != 1 || len(out.ExternalOrders) != 1 || len(out.UnattributedTrades) == 0 {
			fail("exactly one external signed order required; other external trades are ambiguous")
			cases = append(cases, c)
			continue
		}
		var exitID int64
		var exitFills []exchange.AccountTrade
		for id, fills := range externalByOrder {
			exitID, exitFills = id, fills
		}
		c.ExitOrderID = exitID
		external := out.ExternalOrders[0]
		if exitID <= 0 || external.OrderID != exitID || !external.VerifiedOrderIdentity || external.Status != "FILLED" || len(exitFills) != external.TradeCount {
			fail("external order has not passed independent signed orderId verification")
			cases = append(cases, c)
			continue
		}
		entrySide, closeSide := "", ""
		switch rec.Plan.PositionSide {
		case Long:
			entrySide, closeSide = "BUY", "SELL"
		case Short:
			entrySide, closeSide = "SELL", "BUY"
		default:
			fail("unsupported hedge side")
			cases = append(cases, c)
			continue
		}
		if rec.Plan.ExchangeSide != entrySide || external.Side != closeSide || external.PositionSide != string(rec.Plan.PositionSide) {
			fail("entry/exit side is not a hedge-side close pair")
			cases = append(cases, c)
			continue
		}
		entryCount := 0
		latestEntryTime := int64(0)
		for _, trade := range remoteTrades {
			if trade.OrderID != entry.SignedOrderID || trade.Side != entrySide || trade.PositionSide != string(rec.Plan.PositionSide) {
				fail("unexpected signed owned trade in history")
				break
			}
			entryCount++
			if trade.TimeMS > latestEntryTime {
				latestEntryTime = trade.TimeMS
			}
		}
		if entryCount != entry.SignedTradeCount || latestEntryTime <= 0 {
			fail("signed entry trade set incomplete")
			cases = append(cases, c)
			continue
		}
		qty, pnl, fee := new(big.Rat), new(big.Rat), new(big.Rat)
		valid := true
		for _, trade := range exitFills {
			q, qe := positiveRat(trade.Quantity)
			p, pe := signedRat(trade.RealizedPnL)
			f, fe := signedRat(trade.Commission)
			if trade.OrderID != exitID || trade.Symbol != "BTCUSDT" || trade.Side != closeSide || trade.PositionSide != string(rec.Plan.PositionSide) || trade.TimeMS < latestEntryTime || qe != nil || pe != nil || fe != nil || f.Sign() < 0 || trade.CommissionAsset != "USDT" {
				valid = false
				break
			}
			qty.Add(qty, q)
			pnl.Add(pnl, p)
			fee.Add(fee, f)
		}
		if !valid || qty.Cmp(expected) != 0 {
			fail("external signed fills invalid, out of chronology, or not exact full close")
			cases = append(cases, c)
			continue
		}
		c.ExitQuantity, c.SignedRealizedPnL, c.SignedExitCommission, c.CommissionAsset = formatOwnershipDecimal(qty), formatOwnershipDecimal(pnl), formatOwnershipDecimal(fee), "USDT"
		c.Status = ExternalCloseCandidate
		if !c.LocalEntryFillsMatched {
			fail("signed entry exists but locally imported fills are missing or inconsistent; separate immutable recovery required")
		}
		fail("pre-window account activity, funding and operator intent remain unverified; no ledger/lifecycle transition")
		cases = append(cases, c)
	}
	return cases
}
