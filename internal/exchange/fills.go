package exchange

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// AccountTrade is a signed USER_DATA execution, never an order acknowledgement.
// RealizedPnL is the EXCHANGE account's accounting result; a model's virtual
// realized PnL is computed separately because positions are pooled by side.
type AccountTrade struct {
	Symbol          string `json:"symbol"`
	TradeID         int64  `json:"id"`
	OrderID         int64  `json:"orderId"`
	Side            string `json:"side"`
	PositionSide    string `json:"positionSide"`
	Price           string `json:"price"`
	Quantity        string `json:"qty"`
	Commission      string `json:"commission"`
	CommissionAsset string `json:"commissionAsset"`
	RealizedPnL     string `json:"realizedPnl"`
	TimeMS          int64  `json:"time"`
}

// UnmarshalJSON preserves exact decimal text, accepting both official quoted
// decimals and numeric JSON seen in some Demo market data responses. Avoid
// float64 conversion for financial values.
func (t *AccountTrade) UnmarshalJSON(raw []byte) error {
	var x struct {
		Symbol          string    `json:"symbol"`
		TradeID         int64     `json:"id"`
		OrderID         int64     `json:"orderId"`
		Side            string    `json:"side"`
		PositionSide    string    `json:"positionSide"`
		Price           wsDecimal `json:"price"`
		Quantity        wsDecimal `json:"qty"`
		Commission      wsDecimal `json:"commission"`
		CommissionAsset string    `json:"commissionAsset"`
		RealizedPnL     wsDecimal `json:"realizedPnl"`
		TimeMS          int64     `json:"time"`
	}
	if err := json.Unmarshal(raw, &x); err != nil {
		return err
	}
	*t = AccountTrade{Symbol: x.Symbol, TradeID: x.TradeID, OrderID: x.OrderID, Side: x.Side,
		PositionSide: x.PositionSide, Price: string(x.Price), Quantity: string(x.Quantity),
		Commission: string(x.Commission), CommissionAsset: x.CommissionAsset,
		RealizedPnL: string(x.RealizedPnL), TimeMS: x.TimeMS}
	return nil
}

// FetchDemoOrderTrades uses a signed READ-ONLY endpoint with orderId filtering.
// A full 1000-row response is deliberately rejected instead of assuming that
// pagination/trade-history retention have returned an exhaustive set.
// IMPORTANT: use a verified current order result and compare executedQty with
// these actual fills before interpreting them as complete.
func (c *Client) FetchDemoOrderTrades(ctx context.Context, key, secret, symbol string, orderID int64) ([]AccountTrade, error) {
	if err := c.demoOnly(); err != nil {
		return nil, err
	}
	if symbol != "BTCUSDT" || orderID <= 0 || strings.TrimSpace(key) == "" || secret == "" {
		return nil, errors.New("Demo BTCUSDT 주문 ID와 인증 정보가 필요합니다")
	}
	offset, err := c.serverOffset(ctx)
	if err != nil {
		return nil, err
	}
	var trades []AccountTrade
	err = c.signedGet(ctx, "/fapi/v1/userTrades", url.Values{
		"symbol": {symbol}, "orderId": {strconv.FormatInt(orderID, 10)}, "limit": {"1000"},
	}, key, secret, offset, &trades)
	if err != nil {
		return nil, err
	}
	if trades == nil {
		return nil, errors.New("체결 목록 응답이 누락되었습니다")
	}
	if len(trades) >= 1000 {
		return nil, errors.New("주문 체결 1000건 상한: 완전한 목록을 확인하기 전 장부에 반영할 수 없습니다")
	}
	seen := make(map[int64]bool)
	for _, t := range trades {
		if t.Symbol != symbol || t.OrderID != orderID || t.TradeID <= 0 || seen[t.TradeID] || t.TimeMS <= 0 ||
			(t.Side != "BUY" && t.Side != "SELL") || (t.PositionSide != "LONG" && t.PositionSide != "SHORT") ||
			!decimalNonNegative(t.Price, false) || !decimalNonNegative(t.Quantity, false) ||
			!decimalSigned(t.Commission) || !decimalSigned(t.RealizedPnL) || t.CommissionAsset == "" {
			return nil, fmt.Errorf("거래소 체결 내역이 불완전하거나 중복되었습니다 (주문 %d)", orderID)
		}
		seen[t.TradeID] = true
	}
	return trades, nil
}

// Exchange decimal strings are bounded to avoid overflow-prone or malformed
// payloads before they reach the exact-rational accounting layer.
func decimalNonNegative(s string, allowZero bool) bool {
	if len(s) == 0 || len(s) > 64 {
		return false
	}
	dot := false
	digits, nonzero := 0, false
	for _, c := range s {
		switch {
		case c == '.' && !dot:
			dot = true
		case c >= '0' && c <= '9':
			digits++
			if c != '0' {
				nonzero = true
			}
		default:
			return false
		}
	}
	return digits > 0 && s[0] != '.' && s[len(s)-1] != '.' && (allowZero || nonzero)
}
func decimalSigned(s string) bool {
	if strings.HasPrefix(s, "-") {
		s = s[1:]
	}
	return decimalNonNegative(s, true)
}
