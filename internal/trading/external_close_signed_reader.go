package trading

// DemoSignedCloseReader binds the dev.3.71 evidence interface to real Binance
// Demo signed GETs. It never submits, cancels, settles or journals an order.
// Fresh, flat account and bounded replay evidence are still NOT authorization
// to settle pooled Hedge ownership: historical and funding completeness are
// deliberately unproven, and every caller stays fail-closed.
import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"time"

	"binance_trader/internal/exchange"
)

const signedCloseMaxSpan = 14 * 24 * time.Hour

type DemoSignedCloseReader struct {
	Client      *exchange.Client
	Key, Secret string
	Now         func() time.Time
}

func (r DemoSignedCloseReader) RefreshExternalCloseEvidence(ctx context.Context, entryID, exitID int64) (SignedCloseEvidence, error) {
	if r.Client == nil || r.Now == nil || r.Key == "" || r.Secret == "" || entryID <= 0 || exitID <= 0 || entryID == exitID {
		return SignedCloseEvidence{}, errors.New("invalid signed close reader or order identity")
	}
	if err := ctx.Err(); err != nil {
		return SignedCloseEvidence{}, err
	}
	initial := r.Now().UTC()
	entry1, err := r.Client.QueryDemoOrderByID(ctx, r.Key, r.Secret, "BTCUSDT", entryID)
	if err != nil {
		return SignedCloseEvidence{}, err
	}
	exit1, err := r.Client.QueryDemoOrderByID(ctx, r.Key, r.Secret, "BTCUSDT", exitID)
	if err != nil {
		return SignedCloseEvidence{}, err
	}
	if !validSignedCloseOrders(entry1, exit1) {
		return SignedCloseEvidence{}, errors.New("entry/exit signed order identity or quantity invalid")
	}
	entryFills, err := r.Client.FetchDemoOrderTrades(ctx, r.Key, r.Secret, "BTCUSDT", entryID)
	if err != nil {
		return SignedCloseEvidence{}, err
	}
	exitFills, err := r.Client.FetchDemoOrderTrades(ctx, r.Key, r.Secret, "BTCUSDT", exitID)
	if err != nil {
		return SignedCloseEvidence{}, err
	}
	if len(entryFills) == 0 || len(exitFills) == 0 {
		return SignedCloseEvidence{}, errors.New("signed order fills missing")
	}
	entryQty, entryIDs, earliest, entryLast, err := checkSignedCloseFills(entryFills, entry1)
	if err != nil {
		return SignedCloseEvidence{}, err
	}
	exitQty, exitIDs, exMin, exMax, err := checkSignedCloseFills(exitFills, exit1)
	if err != nil {
		return SignedCloseEvidence{}, err
	}
	if entryLast >= exMin || entryQty.Cmp(exitQty) != 0 || entryQty.Sign() <= 0 {
		return SignedCloseEvidence{}, errors.New("signed entry/exit fill order or quantity mismatch")
	}
	latest := exMax
	if initial.Sub(time.UnixMilli(earliest)) > signedCloseMaxSpan || latest-earliest > signedCloseMaxSpan.Milliseconds() || time.UnixMilli(latest).After(initial.Add(5*time.Second)) {
		return SignedCloseEvidence{}, errors.New("historical retention/coverage not sufficient for bounded audit")
	}
	// Inclusive windows with disjoint millisecond boundaries. This covers the
	// entry-to-exit interval only, NOT the full account's pre-entry history.
	perOrder := make(map[int64]exchange.AccountTrade)
	for _, t := range append(append([]exchange.AccountTrade(nil), entryFills...), exitFills...) {
		if _, exists := perOrder[t.TradeID]; exists {
			return SignedCloseEvidence{}, errors.New("trade ID reused across orders")
		}
		perOrder[t.TradeID] = t
	}
	seen := make(map[int64]bool)
	historical := make([]exchange.AccountTrade, 0, len(perOrder))
	funding := make([]exchange.FundingIncome, 0)
	seenFundingIDs := make(map[int64]bool)
	fundingNet := new(big.Rat)
	tradeWindowCount, fundingWindowCount := 0, 0
	for start := earliest; start <= latest; {
		end := start + int64((24*time.Hour)/time.Millisecond) - 1
		if end < start || end > latest {
			end = latest
		}
		rows, err := r.Client.FetchDemoTradeWindow(ctx, r.Key, r.Secret, "BTCUSDT", start, end)
		tradeWindowCount++
		if err != nil {
			return SignedCloseEvidence{}, fmt.Errorf("bounded trade history: %w", err)
		}
		for _, t := range rows {
			expected, ok := perOrder[t.TradeID]
			if !ok || !equalCloseTrade(expected, t) || seen[t.TradeID] {
				return SignedCloseEvidence{}, errors.New("unattributed/changed/duplicate historical trade")
			}
			seen[t.TradeID] = true
			historical = append(historical, t)
		}
		// Even if fees are summed, the exchange may pool both directions and
		// models on the same side. Funding rows cannot prove model attribution.
		fundingRows, err := r.Client.FetchDemoFundingWindow(ctx, r.Key, r.Secret, "BTCUSDT", start, end)
		fundingWindowCount++
		if err != nil {
			return SignedCloseEvidence{}, fmt.Errorf("bounded funding audit: %w", err)
		}
		for _, row := range fundingRows {
			if seenFundingIDs[row.TranID] {
				return SignedCloseEvidence{}, errors.New("funding transaction ID repeated across windows")
			}
			seenFundingIDs[row.TranID] = true
			amount, e := signedRat(row.Income)
			if e != nil {
				return SignedCloseEvidence{}, e
			}
			fundingNet.Add(fundingNet, amount)
			funding = append(funding, row)
		}
		if end == latest {
			break
		}
		start = end + 1
	}
	if len(seen) != len(perOrder) {
		return SignedCloseEvidence{}, errors.New("historical trade coverage does not match order-level fills")
	}
	entry2, err := r.Client.QueryDemoOrderByID(ctx, r.Key, r.Secret, "BTCUSDT", entryID)
	if err != nil {
		return SignedCloseEvidence{}, err
	}
	exit2, err := r.Client.QueryDemoOrderByID(ctx, r.Key, r.Secret, "BTCUSDT", exitID)
	if err != nil {
		return SignedCloseEvidence{}, err
	}
	if entry1 != entry2 || exit1 != exit2 {
		return SignedCloseEvidence{}, errors.New("signed order changed while fetching history")
	}
	snap, err := r.Client.Fetch(ctx, r.Key, r.Secret)
	if err != nil {
		return SignedCloseEvidence{}, err
	}
	now := r.Now().UTC()
	at, err := time.Parse(time.RFC3339Nano, snap.AsOf)
	if err != nil || at.After(now.Add(5*time.Second)) || now.Sub(at) > 5*time.Second ||
		snap.Environment != "demo" || snap.Status != "connected" || snap.Positions == nil || snap.Orders == nil || snap.AlgoOrders == nil || snap.Balances == nil || len(snap.Orders) != 0 || len(snap.AlgoOrders) != 0 {
		return SignedCloseEvidence{}, errors.New("signed Demo account is stale, disconnected or contains open orders")
	}
	longQty, shortQty, err := signedSideQuantities(snap)
	if err != nil || longQty != "0" || shortQty != "0" {
		return SignedCloseEvidence{}, errors.New("signed account is not flat")
	}
	// TRUE is intentionally never claimed here: this endpoint range cannot
	// prove pre-entry history, a complete funding allocation or other models.
	sort.Slice(entryFills, func(i, j int) bool { return entryFills[i].TradeID < entryFills[j].TradeID })
	sort.Slice(exitFills, func(i, j int) bool { return exitFills[i].TradeID < exitFills[j].TradeID })
	sort.Slice(historical, func(i, j int) bool { return historical[i].TradeID < historical[j].TradeID })
	sort.Slice(funding, func(i, j int) bool { return funding[i].TranID < funding[j].TranID })
	details := struct {
		EntryOrder, ExitOrder          exchange.OrderResult
		EntryFills, ExitFills, History []exchange.AccountTrade
		Funding                        []exchange.FundingIncome
	}{entry1, exit1, entryFills, exitFills, historical, funding}
	return SignedCloseEvidence{EntryOrder: entry1, ExitOrder: exit1, HistoricalTrades: historical, EntryFills: entryFills, ExitFills: exitFills, FundingRows: funding, SignedDetailSHA256: externalCloseHash(details), BoundedFundingRows: len(funding), BoundedFundingNetUSDT: formatOwnershipDecimal(fundingNet), EntryOrderID: entryID, ExitOrderID: exitID,
		EntryTradeIDs: entryIDs, ExitTradeIDs: exitIDs, AccountFlat: true,
		HistoryFromMS: earliest, HistoryToMS: latest, BoundedTradeRows: len(historical),
		TradeWindowsRead: tradeWindowCount, FundingWindowsRead: fundingWindowCount,
		HistoryScope:               "ENTRY_FIRST_FILL_TO_EXIT_LAST_FILL_ONLY",
		HistoricalCoverageComplete: false, FundingVerified: false,
		ObservedAt: now}, nil
}

func validSignedCloseOrders(entry, exit exchange.OrderResult) bool {
	if entry.Symbol != "BTCUSDT" || exit.Symbol != "BTCUSDT" || entry.OrderID <= 0 || exit.OrderID <= 0 || entry.OrderID == exit.OrderID ||
		entry.Status != "FILLED" || exit.Status != "FILLED" || entry.PositionSide != exit.PositionSide {
		return false
	}
	if entry.PositionSide == "SHORT" {
		if entry.Side != "SELL" || exit.Side != "BUY" {
			return false
		}
	} else if entry.PositionSide == "LONG" {
		if entry.Side != "BUY" || exit.Side != "SELL" {
			return false
		}
	} else {
		return false
	}
	a, ae := positiveRat(entry.ExecutedQty)
	b, be := positiveRat(exit.ExecutedQty)
	origA, oa := positiveRat(entry.OriginalQty)
	origB, ob := positiveRat(exit.OriginalQty)
	return ae == nil && be == nil && oa == nil && ob == nil && a.Cmp(b) == 0 && a.Cmp(origA) == 0 && b.Cmp(origB) == 0
}
func checkSignedCloseFills(fills []exchange.AccountTrade, order exchange.OrderResult) (*big.Rat, []int64, int64, int64, error) {
	total := new(big.Rat)
	ids := make([]int64, 0, len(fills))
	earliest, latest := int64(1<<63-1), int64(0)
	for _, t := range fills {
		if t.Symbol != order.Symbol || t.OrderID != order.OrderID || t.PositionSide != order.PositionSide || t.Side != order.Side || t.CommissionAsset != "USDT" || t.TimeMS <= 0 {
			return nil, nil, 0, 0, errors.New("fill identity or fee asset invalid")
		}
		q, e := positiveRat(t.Quantity)
		price, priceErr := positiveRat(t.Price)
		fee, f := signedRat(t.Commission)
		pnl, p := signedRat(t.RealizedPnL)
		if e != nil || priceErr != nil || price.Sign() <= 0 || f != nil || p != nil || fee.Sign() < 0 || (order.Side == "SELL" && order.PositionSide == "SHORT" || order.Side == "BUY" && order.PositionSide == "LONG") && pnl.Sign() != 0 {
			return nil, nil, 0, 0, errors.New("fill value invalid")
		}
		total.Add(total, q)
		ids = append(ids, t.TradeID)
		if t.TimeMS < earliest {
			earliest = t.TimeMS
		}
		if t.TimeMS > latest {
			latest = t.TimeMS
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for i, v := range ids {
		if v <= 0 || i > 0 && v == ids[i-1] {
			return nil, nil, 0, 0, errors.New("duplicate fill ID")
		}
	}
	expected, err := positiveRat(order.ExecutedQty)
	if err != nil || expected.Cmp(total) != 0 {
		return nil, nil, 0, 0, errors.New("order quantity and signed fills mismatch")
	}
	return total, ids, earliest, latest, nil
}
func equalCloseTrade(a, b exchange.AccountTrade) bool {
	return a.TradeID == b.TradeID && a.OrderID == b.OrderID && a.Symbol == b.Symbol && a.Side == b.Side && a.PositionSide == b.PositionSide && a.Price == b.Price && a.Quantity == b.Quantity && a.Commission == b.Commission && a.CommissionAsset == b.CommissionAsset && a.RealizedPnL == b.RealizedPnL && a.TimeMS == b.TimeMS
}
