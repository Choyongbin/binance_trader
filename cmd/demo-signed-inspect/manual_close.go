package main

// Forensic correlation of a human/external close to an existing signed entry.
// This module does NOT write local ledgers or infer model ownership. A match
// is a human-review CANDIDATE, never SETTLED and never trading permission.
import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"time"

	"binance_trader/internal/exchange"
	"binance_trader/internal/trading"
)

const maxManualCloseReviewWindow = 14 * 24 * time.Hour

type manualCloseReview struct {
	Status                string   `json:"status"`
	EntryOrderID          int64    `json:"entry_order_id"`
	EntryFillCount        int      `json:"entry_fill_count"`
	EntryQuantity         string   `json:"entry_quantity,omitempty"`
	ExitOrderID           int64    `json:"candidate_exit_order_id,omitempty"`
	ExitTradeCount        int      `json:"candidate_exit_trade_count,omitempty"`
	ExitQuantity          string   `json:"candidate_exit_quantity,omitempty"`
	ExitRealizedPnL       string   `json:"candidate_exit_realized_pnl,omitempty"`
	ExitTradeIDs          []int64  `json:"candidate_exit_trade_ids,omitempty"`
	HistoryStartUTC       string   `json:"history_start_utc,omitempty"`
	HistoryEndUTC         string   `json:"history_end_utc,omitempty"`
	HistoryComplete       bool     `json:"history_complete"`
	FreshBTCUSDTFlat      bool     `json:"fresh_btcusdt_flat"`
	TradingAuthorized     bool     `json:"trading_authorized"`      // NEVER true
	LocalLifecycleChanged bool     `json:"local_lifecycle_changed"` // NEVER true
	Blockers              []string `json:"blockers"`
}

type manualCloseReader interface {
	FetchDemoTradeWindow(context.Context, string, string, string, int64, int64) ([]exchange.AccountTrade, error)
	QueryDemoOrderByID(context.Context, string, string, string, int64) (exchange.OrderResult, error)
	Fetch(context.Context, string, string) (exchange.Snapshot, error)
}

func quantityRat(v string) (*big.Rat, error) {
	if v == "" || v[0] == '-' {
		return nil, errors.New("invalid decimal")
	}
	x, ok := new(big.Rat).SetString(v)
	if !ok || x.Sign() <= 0 {
		return nil, errors.New("invalid positive decimal")
	}
	return x, nil
}
func anyRat(v string) (*big.Rat, error) {
	x, ok := new(big.Rat).SetString(v)
	if !ok {
		return nil, errors.New("invalid decimal")
	}
	return x, nil
}
func formatRat(x *big.Rat) string { return x.FloatString(8) }
func blockedManualReview(r manualCloseReview, reason string) manualCloseReview {
	r.Status = "REVIEW_INCOMPLETE_OR_AMBIGUOUS"
	r.Blockers = append(r.Blockers, reason)
	return r
}

// reviewManualClose fetches a COMPLETE bounded trade-history interval before
// attributing any external/manual close. A full 1000-row page is rejected by
// the exchange adapter; no automatic paging or missing trades are assumed.
func reviewManualClose(ctx context.Context, reader manualCloseReader, creds trading.DemoCredentials, entry exchange.OrderResult, entryFills []exchange.AccountTrade, checkedAt time.Time) manualCloseReview {
	r := manualCloseReview{Status: "REVIEW_INCOMPLETE_OR_AMBIGUOUS", EntryOrderID: entry.OrderID, Blockers: []string{"외부 청산의 모델 소유권은 미확정입니다. MAY_HAVE_SENT/거래 잠금 유지; signed 조회 보고서로만 사용하세요."}}
	if reader == nil || entry.Symbol != "BTCUSDT" || entry.Side != "SELL" || entry.PositionSide != "SHORT" || entry.Type != "MARKET" || entry.Status != "FILLED" || entry.OrderID <= 0 || len(entryFills) == 0 {
		return blockedManualReview(r, "signed SHORT entry order/fills not complete")
	}
	expected, err := quantityRat(entry.ExecutedQty)
	if err != nil {
		return blockedManualReview(r, "invalid signed entry executed quantity")
	}
	var minMS int64
	sum := new(big.Rat)
	entryIDs := map[int64]bool{}
	for _, f := range entryFills {
		if f.Symbol != "BTCUSDT" || f.OrderID != entry.OrderID || f.TradeID <= 0 || entryIDs[f.TradeID] || f.Side != "SELL" || f.PositionSide != "SHORT" || f.TimeMS <= 0 {
			return blockedManualReview(r, "entry fills inconsistent or duplicate")
		}
		entryIDs[f.TradeID] = true
		q, e := quantityRat(f.Quantity)
		if e != nil {
			return blockedManualReview(r, "entry fill quantity invalid")
		}
		sum.Add(sum, q)
		if minMS == 0 || f.TimeMS < minMS {
			minMS = f.TimeMS
		}
	}
	if sum.Cmp(expected) != 0 {
		return blockedManualReview(r, "entry fill sum differs from signed order")
	}
	r.EntryFillCount = len(entryFills)
	r.EntryQuantity = formatRat(sum)
	start := time.UnixMilli(minMS).UTC().Add(-time.Minute)
	end := checkedAt.UTC()
	if !end.After(start) || end.Sub(start) > maxManualCloseReviewWindow {
		return blockedManualReview(r, "history horizon exceeds supported 14 days or clock is invalid; historical coverage NOT proven")
	}
	r.HistoryStartUTC = start.Format(time.RFC3339Nano)
	r.HistoryEndUTC = end.Format(time.RFC3339Nano)
	all := make([]exchange.AccountTrade, 0)
	seen := map[int64]bool{}
	for from := start.UnixMilli(); from <= end.UnixMilli(); {
		to := from + int64((24*time.Hour)/time.Millisecond) - 1
		if to > end.UnixMilli() {
			to = end.UnixMilli()
		}
		trades, e := reader.FetchDemoTradeWindow(ctx, creds.APIKey, creds.Secret, "BTCUSDT", from, to)
		if e != nil {
			return blockedManualReview(r, "signed trade history GET incomplete; no close inference: "+e.Error())
		}
		for _, t := range trades {
			if seen[t.TradeID] || t.TradeID <= 0 || t.TimeMS < from || t.TimeMS > to {
				return blockedManualReview(r, "duplicate/out-of-window trade in signed history")
			}
			seen[t.TradeID] = true
			all = append(all, t)
		}
		if to == end.UnixMilli() {
			break
		}
		from = to + 1
	}
	r.HistoryComplete = true
	sort.Slice(all, func(i, j int) bool {
		if all[i].TimeMS == all[j].TimeMS {
			return all[i].TradeID < all[j].TradeID
		}
		return all[i].TimeMS < all[j].TimeMS
	})
	exitOrders := map[int64][]exchange.AccountTrade{}
	for _, t := range all {
		if t.Symbol != "BTCUSDT" {
			return blockedManualReview(r, "unexpected symbol in account trade history")
		}
		if t.OrderID == entry.OrderID && t.Side == "SELL" && t.PositionSide == "SHORT" {
			if !entryIDs[t.TradeID] {
				return blockedManualReview(r, "original entry trade mismatch: some fills missing from entry order query")
			}
			delete(entryIDs, t.TradeID)
			continue
		}
		if t.PositionSide != "SHORT" || t.Side != "BUY" || t.TimeMS < minMS {
			return blockedManualReview(r, "unexpected external BTCUSDT trade; attribution is ambiguous")
		}
		exitOrders[t.OrderID] = append(exitOrders[t.OrderID], t)
	}
	if len(entryIDs) != 0 {
		return blockedManualReview(r, "original entry fills are missing from bounded signed history")
	}
	if len(exitOrders) != 1 {
		return blockedManualReview(r, fmt.Sprintf("expected one external SHORT BUY order, found %d; no ownership attribution", len(exitOrders)))
	}
	for orderID, fills := range exitOrders {
		r.ExitOrderID = orderID
		r.ExitTradeCount = len(fills)
		exitQty, realized := new(big.Rat), new(big.Rat)
		for _, t := range fills {
			q, e := quantityRat(t.Quantity)
			if e != nil {
				return blockedManualReview(r, "close fill quantity invalid")
			}
			pnl, e := anyRat(t.RealizedPnL)
			if e != nil {
				return blockedManualReview(r, "close realized pnl invalid")
			}
			exitQty.Add(exitQty, q)
			realized.Add(realized, pnl)
			r.ExitTradeIDs = append(r.ExitTradeIDs, t.TradeID)
		}
		sort.Slice(r.ExitTradeIDs, func(i, j int) bool { return r.ExitTradeIDs[i] < r.ExitTradeIDs[j] })
		r.ExitQuantity = formatRat(exitQty)
		r.ExitRealizedPnL = formatRat(realized)
		if exitQty.Cmp(expected) != 0 {
			return blockedManualReview(r, "close signed quantity does not match entry")
		}
		remote, e := reader.QueryDemoOrderByID(ctx, creds.APIKey, creds.Secret, "BTCUSDT", orderID)
		if e != nil {
			return blockedManualReview(r, "cannot verify external close order by signed orderId: "+e.Error())
		}
		executed, e := quantityRat(remote.ExecutedQty)
		if e != nil || remote.OrderID != orderID || remote.Symbol != "BTCUSDT" || remote.PositionSide != "SHORT" || remote.Side != "BUY" || remote.Status != "FILLED" || executed.Cmp(exitQty) != 0 {
			return blockedManualReview(r, "signed external close order identity/status/quantity mismatch")
		}
	}
	fresh, e := reader.Fetch(ctx, creds.APIKey, creds.Secret)
	if e != nil {
		return blockedManualReview(r, "fresh signed account GET failed: "+e.Error())
	}
	asOf, e := time.Parse(time.RFC3339, fresh.AsOf)
	if e != nil || asOf.Before(time.Now().UTC().Add(-2*time.Minute)) || fresh.Environment != "demo" || fresh.Status != "connected" {
		return blockedManualReview(r, "signed account snapshot stale or invalid")
	}
	if len(fresh.Positions) != 0 || len(fresh.Orders) != 0 || len(fresh.AlgoOrders) != 0 {
		return blockedManualReview(r, "account is not flat or has open/algo orders")
	}
	r.FreshBTCUSDTFlat = true
	r.Status = "SIGNED_EXTERNAL_CLOSE_CANDIDATE_REVIEW_ONLY"
	// Never change r.TradingAuthorized / LocalLifecycleChanged from false.
	return r
}
