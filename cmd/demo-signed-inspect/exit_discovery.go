package main

// Exit-order discovery is an OPERATOR AID, not an attribution or settlement
// procedure. Even a unique matching order ID must be independently reviewed
// before running incident-archive with an explicitly supplied exit order ID.
// All exchange methods used here are Demo signed GETs.
import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"time"

	"binance_trader/internal/exchange"
	"binance_trader/internal/trading"
)

const maxExitDiscoverySpan = 14 * 24 * time.Hour

type exitDiscoveryReader interface {
	FetchDemoTradeWindow(context.Context, string, string, string, int64, int64) ([]exchange.AccountTrade, error)
	QueryDemoOrderByID(context.Context, string, string, string, int64) (exchange.OrderResult, error)
	Fetch(context.Context, string, string) (exchange.Snapshot, error)
}

type exitDiscoveryCandidate struct {
	OrderID              int64    `json:"order_id"`
	TradeIDs             []int64  `json:"signed_trade_ids"`
	Quantity             string   `json:"quantity"`
	SignedOrderConfirmed bool     `json:"signed_order_confirmed"`
	Notes                []string `json:"notes,omitempty"`
}

type exitDiscoveryResult struct {
	Schema                     int                      `json:"schema"`
	Status                     string                   `json:"status"`
	EntryClientOrderID         string                   `json:"entry_client_order_id"`
	EntryOrderID               int64                    `json:"entry_order_id"`
	PositionSide               string                   `json:"position_side"`
	HistoryFromUTC             string                   `json:"bounded_history_from_utc,omitempty"`
	HistoryToUTC               string                   `json:"bounded_history_to_utc,omitempty"`
	BoundedHistoryRead         bool                     `json:"bounded_history_read"`
	FullAccountHistoryVerified bool                     `json:"full_account_history_verified"` // ALWAYS false
	FreshFlatAccount           bool                     `json:"fresh_flat_account"`
	Candidates                 []exitDiscoveryCandidate `json:"external_exit_order_candidates"`
	UnexpectedTradeIDs         []int64                  `json:"unattributed_or_other_trade_ids,omitempty"`
	Blockers                   []string                 `json:"blockers"`
	SettlementAuthorized       bool                     `json:"settlement_authorized"` // ALWAYS false
	TradingAuthorized          bool                     `json:"trading_authorized"`    // ALWAYS false
	LifecycleChanged           bool                     `json:"lifecycle_changed"`     // ALWAYS false
	OwnershipChanged           bool                     `json:"ownership_changed"`     // ALWAYS false
}

func blockedExitDiscovery(r exitDiscoveryResult, reason string) exitDiscoveryResult {
	r.Status = "INCOMPLETE_OR_AMBIGUOUS_REVIEW_ONLY"
	r.Blockers = append(r.Blockers, reason)
	return r
}

func discoverExitOrder(ctx context.Context, reader exitDiscoveryReader, creds trading.DemoCredentials, entry trading.PackageAEntryArchive, now time.Time) exitDiscoveryResult {
	r := exitDiscoveryResult{Schema: 1, Status: "INCOMPLETE_OR_AMBIGUOUS_REVIEW_ONLY", EntryClientOrderID: entry.Order.ClientOrderID,
		EntryOrderID: entry.Order.OrderID, PositionSide: entry.Order.PositionSide, Candidates: []exitDiscoveryCandidate{},
		Blockers: []string{"order IDs are review candidates only; no independent manual intent or pooled Hedge ownership attribution", "full account history, funding attribution and original MAY_HAVE_SENT remain unresolved"}}
	if reader == nil || creds.APIKey == "" || creds.Secret == "" || now.IsZero() || ctx.Err() != nil {
		return blockedExitDiscovery(r, "reader/credentials/clock/context invalid")
	}
	e := entry.Order
	if e.Symbol != "BTCUSDT" || e.Status != "FILLED" || e.Type != "MARKET" || e.OrderID <= 0 ||
		(e.PositionSide != "LONG" && e.PositionSide != "SHORT") || len(entry.Fills) == 0 {
		return blockedExitDiscovery(r, "entry signed order is not a complete BTCUSDT MARKET entry")
	}
	enterSide, exitSide := "BUY", "SELL"
	if e.PositionSide == "SHORT" {
		enterSide, exitSide = "SELL", "BUY"
	}
	if e.Side != enterSide {
		return blockedExitDiscovery(r, "entry side mismatched")
	}
	entryQty := new(big.Rat)
	seenEntry := make(map[int64]exchange.AccountTrade)
	firstMS, lastMS := int64(0), int64(0)
	for _, t := range entry.Fills {
		qty, qtyErr := quantityRat(t.Quantity)
		_, priceErr := quantityRat(t.Price)
		fee, feeErr := anyRat(t.Commission)
		pnl, pnlErr := anyRat(t.RealizedPnL)
		if qtyErr != nil || priceErr != nil || feeErr != nil || pnlErr != nil || fee.Sign() < 0 || pnl.Sign() != 0 ||
			t.CommissionAsset != "USDT" || t.TradeID <= 0 || t.TimeMS <= 0 || t.Symbol != "BTCUSDT" || t.OrderID != e.OrderID ||
			t.Side != enterSide || t.PositionSide != e.PositionSide {
			return blockedExitDiscovery(r, "invalid signed entry fill")
		}
		if _, exists := seenEntry[t.TradeID]; exists {
			return blockedExitDiscovery(r, "duplicate entry fill ID")
		}
		seenEntry[t.TradeID] = t
		entryQty.Add(entryQty, qty)
		if firstMS == 0 || t.TimeMS < firstMS {
			firstMS = t.TimeMS
		}
		if t.TimeMS > lastMS {
			lastMS = t.TimeMS
		}
	}
	expected, err := quantityRat(e.ExecutedQty)
	if err != nil || expected.Cmp(entryQty) != 0 {
		return blockedExitDiscovery(r, "entry executed quantity mismatch")
	}
	from := time.UnixMilli(firstMS).UTC().Add(-time.Minute)
	to := now.UTC()
	if !to.After(from) || to.Sub(from) > maxExitDiscoverySpan || time.UnixMilli(lastMS).After(to) {
		return blockedExitDiscovery(r, "signed history exceeds 14-day bounded window; do not infer missing orders")
	}
	r.HistoryFromUTC, r.HistoryToUTC = from.Format(time.RFC3339Nano), to.Format(time.RFC3339Nano)
	observedEntry := map[int64]bool{}
	candidateFills := map[int64][]exchange.AccountTrade{}
	seen := map[int64]bool{}
	for start := from.UnixMilli(); start <= to.UnixMilli(); {
		end := start + (24 * time.Hour).Milliseconds() - 1
		if end > to.UnixMilli() {
			end = to.UnixMilli()
		}
		rows, err := reader.FetchDemoTradeWindow(ctx, creds.APIKey, creds.Secret, "BTCUSDT", start, end)
		if err != nil {
			return blockedExitDiscovery(r, "signed BTCUSDT trade history missing/partial: "+err.Error())
		}
		if len(rows) >= 1000 {
			return blockedExitDiscovery(r, "signed history possibly truncated (1000 rows)")
		}
		for _, t := range rows {
			if t.TradeID <= 0 || t.OrderID <= 0 || t.TimeMS < start || t.TimeMS > end || seen[t.TradeID] || t.Symbol != "BTCUSDT" {
				return blockedExitDiscovery(r, "duplicate or malformed signed history")
			}
			seen[t.TradeID] = true
			if stored, ok := seenEntry[t.TradeID]; ok {
				if stored != t {
					return blockedExitDiscovery(r, "signed history entry differs from independent order fills")
				}
				observedEntry[t.TradeID] = true
			} else if t.PositionSide == e.PositionSide && t.Side == exitSide && t.TimeMS > lastMS && t.OrderID != e.OrderID {
				candidateFills[t.OrderID] = append(candidateFills[t.OrderID], t)
			} else {
				r.UnexpectedTradeIDs = append(r.UnexpectedTradeIDs, t.TradeID)
			}
		}
		if end == to.UnixMilli() {
			break
		}
		start = end + 1
	}
	if len(observedEntry) != len(seenEntry) {
		return blockedExitDiscovery(r, "signed entry fills absent from bounded history; retention/truncation suspected")
	}
	r.BoundedHistoryRead = true
	ids := make([]int64, 0, len(candidateFills))
	for id := range candidateFills {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	if len(ids) > 32 {
		return blockedExitDiscovery(r, "too many possible exits for bounded review")
	}
	for _, id := range ids {
		fills := candidateFills[id]
		c := exitDiscoveryCandidate{OrderID: id, TradeIDs: []int64{}}
		qty := new(big.Rat)
		valid := true
		for _, t := range fills {
			amount, err1 := quantityRat(t.Quantity)
			_, err2 := quantityRat(t.Price)
			fee, err3 := anyRat(t.Commission)
			_, err4 := anyRat(t.RealizedPnL)
			if err1 != nil || err2 != nil || err3 != nil || err4 != nil || fee.Sign() < 0 || t.CommissionAsset != "USDT" {
				valid = false
				break
			}
			qty.Add(qty, amount)
			c.TradeIDs = append(c.TradeIDs, t.TradeID)
		}
		sort.Slice(c.TradeIDs, func(i, j int) bool { return c.TradeIDs[i] < c.TradeIDs[j] })
		c.Quantity = qty.FloatString(12)
		if !valid {
			c.Notes = append(c.Notes, "invalid signed fill finance fields")
			r.Candidates = append(r.Candidates, c)
			continue
		}
		first, err1 := reader.QueryDemoOrderByID(ctx, creds.APIKey, creds.Secret, "BTCUSDT", id)
		second, err2 := reader.QueryDemoOrderByID(ctx, creds.APIKey, creds.Secret, "BTCUSDT", id)
		signedQty, qtyErr := quantityRat(first.ExecutedQty)
		if err1 != nil || err2 != nil || first != second || qtyErr != nil || signedQty.Cmp(qty) != 0 ||
			first.OrderID != id || first.Symbol != "BTCUSDT" || first.PositionSide != e.PositionSide || first.Side != exitSide ||
			first.Status != "FILLED" || (first.Type != "MARKET" && first.Type != "LIMIT") {
			c.Notes = append(c.Notes, "double signed order query incomplete/mismatched")
		} else {
			c.SignedOrderConfirmed = true
		}
		r.Candidates = append(r.Candidates, c)
	}
	sort.Slice(r.UnexpectedTradeIDs, func(i, j int) bool { return r.UnexpectedTradeIDs[i] < r.UnexpectedTradeIDs[j] })
	snap, err := reader.Fetch(ctx, creds.APIKey, creds.Secret)
	if err != nil {
		return blockedExitDiscovery(r, "fresh signed account query failed")
	}
	asOf, err := time.Parse(time.RFC3339Nano, snap.AsOf)
	if err != nil || asOf.After(now.Add(5*time.Second)) || now.Sub(asOf) > 2*time.Minute || snap.Environment != "demo" || snap.Status != "connected" || snap.Balances == nil || snap.Positions == nil || snap.Orders == nil || snap.AlgoOrders == nil {
		return blockedExitDiscovery(r, "signed account snapshot missing/stale/incorrect environment")
	}
	if len(snap.Positions) != 0 || len(snap.Orders) != 0 || len(snap.AlgoOrders) != 0 {
		return blockedExitDiscovery(r, "signed account not flat or open/algo orders exist")
	}
	r.FreshFlatAccount = true
	if len(r.Candidates) != 1 || len(r.UnexpectedTradeIDs) != 0 || !r.Candidates[0].SignedOrderConfirmed || qtyCmp(r.Candidates[0].Quantity, expected) != 0 {
		return blockedExitDiscovery(r, fmt.Sprintf("candidate count %d, unexpected trade count %d, or signed quantity/identity mismatch; operator must review", len(r.Candidates), len(r.UnexpectedTradeIDs)))
	}
	r.Status = "UNIQUE_EXIT_ORDER_ID_CANDIDATE_REVIEW_ONLY"
	return r
}

func qtyCmp(s string, expected *big.Rat) int {
	q, ok := new(big.Rat).SetString(s)
	if !ok {
		return -1
	}
	return q.Cmp(expected)
}
