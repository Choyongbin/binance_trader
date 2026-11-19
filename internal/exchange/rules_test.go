package exchange

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const btcRuleFixture = `{"symbols":[{"symbol":"ETHUSDT","status":"TRADING","contractType":"PERPETUAL","quoteAsset":"USDT","marginAsset":"USDT"},{"symbol":"BTCUSDT","status":"TRADING","contractType":"PERPETUAL","quoteAsset":"USDT","marginAsset":"USDT","filters":[{"filterType":"PRICE_FILTER","minPrice":"0.10","maxPrice":"1000000","tickSize":"0.10"},{"filterType":"LOT_SIZE","minQty":"0.001","maxQty":"100","stepSize":"0.001"},{"filterType":"MARKET_LOT_SIZE","minQty":"0.001","maxQty":"20","stepSize":"0.002"},{"filterType":"MIN_NOTIONAL","notional":"100"}]}]}`

func TestFetchSymbolRulesAndStrictFailures(t *testing.T) {
	now := time.Now().UTC()
	good, err := ParseSymbolRules([]byte(btcRuleFixture), "BTCUSDT", now)
	if err != nil || good.Lot.Step != "0.001" || good.MarketLot.Step != "0.002" || good.Price.Tick != "0.10" || good.MinNotional != "100" {
		t.Fatalf("bad rule decode %+v %v", good, err)
	}
	h := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/fapi/v1/exchangeInfo" || req.Method != "GET" {
			t.Errorf("unexpected request %v", req.URL)
			w.WriteHeader(404)
			return
		}
		w.Write([]byte(btcRuleFixture))
	}))
	defer h.Close()
	c := NewWithBase(h.URL, h.Client())
	result, err := c.FetchSymbolRules(context.Background(), "BTCUSDT")
	if err != nil || result.MinNotional != "100" {
		t.Fatalf("mock exchange info %+v %v", result, err)
	}
	failures := []string{
		strings.Replace(btcRuleFixture, `"stepSize":"0.001"`, `"stepSize":"0"`, 1),
		strings.Replace(btcRuleFixture, `"tickSize":"0.10"`, `"tickSize":"NaN"`, 1),
		strings.Replace(btcRuleFixture, `"notional":"100"`, `"notional":""`, 1),
		strings.Replace(btcRuleFixture, `"MARKET_LOT_SIZE"`, `"UNSUPPORTED_LOT"`, 1),
		strings.Replace(btcRuleFixture, `"contractType":"PERPETUAL","quoteAsset":"USDT","marginAsset":"USDT","filters"`, `"contractType":"NEXT_QUARTER","quoteAsset":"USDT","marginAsset":"USDT","filters"`, 1),
	}
	for _, b := range failures {
		if _, err := ParseSymbolRules([]byte(b), "BTCUSDT", now); err == nil {
			t.Fatalf("accepted invalid filters %s", b)
		}
	}
	if _, err := ParseSymbolRules([]byte(btcRuleFixture), "DOGEUSDT", now); err == nil {
		t.Fatal("unknown symbol accepted")
	}
	if _, err := ParseSymbolRules([]byte(btcRuleFixture), "BTCUSDT", time.Time{}); err != nil {
		t.Fatal(err)
	}
}
