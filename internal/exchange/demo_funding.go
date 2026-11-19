package exchange

// Bounded signed USD-M funding-income reads for forensic reconciliation only.
// An empty response proves only that Binance returned no funding in this
// specific time window; it NEVER proves lifetime funding completeness or model
// attribution in a pooled Hedge position.
import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type FundingIncome struct {
	Symbol     string `json:"symbol"`
	IncomeType string `json:"incomeType"`
	Income     string `json:"income"`
	Asset      string `json:"asset"`
	TimeMS     int64  `json:"time"`
	TranID     int64  `json:"tranId"`
}

func (c *Client) FetchDemoFundingWindow(ctx context.Context, key, secret, symbol string, startMS, endMS int64) ([]FundingIncome, error) {
	if err := c.demoOnly(); err != nil {
		return nil, err
	}
	if symbol != "BTCUSDT" || strings.TrimSpace(key) == "" || secret == "" || startMS <= 0 || endMS < startMS || endMS-startMS > int64(24*time.Hour/time.Millisecond) {
		return nil, errors.New("funding signed audit requires an explicit <=24h BTCUSDT window")
	}
	offset, err := c.serverOffset(ctx)
	if err != nil {
		return nil, err
	}
	var rows []FundingIncome
	if err := c.signedGet(ctx, "/fapi/v1/income", url.Values{
		"symbol": {symbol}, "incomeType": {"FUNDING_FEE"},
		"startTime": {strconv.FormatInt(startMS, 10)}, "endTime": {strconv.FormatInt(endMS, 10)},
		"limit": {"1000"},
	}, key, secret, offset, &rows); err != nil {
		return nil, err
	}
	if rows == nil || len(rows) >= 1000 {
		return nil, errors.New("funding rows absent or possibly truncated")
	}
	seen := make(map[int64]bool, len(rows))
	for _, r := range rows {
		if r.Symbol != symbol || r.IncomeType != "FUNDING_FEE" || r.Asset != "USDT" || r.TranID <= 0 || seen[r.TranID] || r.TimeMS < startMS || r.TimeMS > endMS || !decimalSigned(r.Income) {
			return nil, fmt.Errorf("funding record is invalid or duplicated in %d..%d", startMS, endMS)
		}
		seen[r.TranID] = true
	}
	return rows, nil
}
