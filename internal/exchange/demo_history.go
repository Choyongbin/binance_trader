package exchange

// Explicit bounded, signed READ-ONLY BTCUSDT account trade history used to
// identify EXTERNAL closes. This is never a source of trading authorization.
// Do not use fromId together with startTime/endTime (Binance forbids it).
import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// FetchDemoTradeWindow returns all signed trades in an inclusive <=24h window.
// A full 1000 row page is ambiguous and rejected: a truncated list MUST NOT
// be mistaken for evidence that there were no intervening account trades.
func (c *Client) FetchDemoTradeWindow(ctx context.Context, key, secret, symbol string, startMS, endMS int64) ([]AccountTrade, error) {
	if err := c.demoOnly(); err != nil {
		return nil, err
	}
	if symbol != "BTCUSDT" || strings.TrimSpace(key) == "" || secret == "" || startMS <= 0 || endMS < startMS || endMS-startMS > int64((24*time.Hour)/time.Millisecond) {
		return nil, errors.New("Demo signed history requires an explicit <=24h BTCUSDT window")
	}
	offset, err := c.serverOffset(ctx)
	if err != nil {
		return nil, err
	}
	var trades []AccountTrade
	err = c.signedGet(ctx, "/fapi/v1/userTrades", url.Values{"symbol": {symbol}, "startTime": {strconv.FormatInt(startMS, 10)}, "endTime": {strconv.FormatInt(endMS, 10)}, "limit": {"1000"}}, key, secret, offset, &trades)
	if err != nil {
		return nil, err
	}
	if trades == nil || len(trades) >= 1000 {
		return nil, errors.New("signed account trade history missing or possibly truncated; manual review required")
	}
	seen := map[int64]bool{}
	for _, t := range trades {
		if t.Symbol != symbol || t.OrderID <= 0 || t.TradeID <= 0 || seen[t.TradeID] || t.TimeMS < startMS || t.TimeMS > endMS || (t.Side != "BUY" && t.Side != "SELL") || (t.PositionSide != "SHORT" && t.PositionSide != "LONG") || !decimalNonNegative(t.Price, false) || !decimalNonNegative(t.Quantity, false) || !decimalSigned(t.Commission) || !decimalSigned(t.RealizedPnL) || t.CommissionAsset == "" {
			return nil, fmt.Errorf("signed history invalid/incomplete in window (%d..%d)", startMS, endMS)
		}
		seen[t.TradeID] = true
	}
	return trades, nil
}

// QueryDemoOrderByID independently checks an external/manual close without
// learning or guessing its clientOrderID. Signed GET only; never POST/DELETE.
func (c *Client) QueryDemoOrderByID(ctx context.Context, key, secret, symbol string, orderID int64) (OrderResult, error) {
	if err := c.demoOnly(); err != nil {
		return OrderResult{}, err
	}
	if symbol != "BTCUSDT" || orderID <= 0 || strings.TrimSpace(key) == "" || secret == "" {
		return OrderResult{}, errors.New("Demo orderId query requires BTCUSDT and positive orderId")
	}
	raw, status, err := c.demoSignedRequest(ctx, "GET", "/fapi/v1/order", url.Values{"symbol": {symbol}, "orderId": {strconv.FormatInt(orderID, 10)}}, key, secret)
	if err != nil {
		return OrderResult{}, err
	}
	if status != 200 {
		return OrderResult{}, statusError(raw, status, true)
	}
	var result OrderResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return OrderResult{}, err
	}
	if result.Symbol != symbol || result.OrderID != orderID || !orderStatusAllowed(result.Status) {
		return OrderResult{}, errors.New("signed orderId response inconsistent")
	}
	return result, nil
}
