package exchange

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSignedDemoTradesByOrderAndMalformedResponses(t *testing.T) {
	output := `[{"symbol":"BTCUSDT","id":51,"orderId":934,"side":"BUY","positionSide":"LONG","price":"100000.00","qty":"0.005","commission":"0.05","commissionAsset":"USDT","realizedPnl":"0","time":1790526000000}]`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/fapi/v1/time":
			fmt.Fprintf(w, `{"serverTime":%d}`, time.Now().UnixMilli())
		case "/fapi/v1/userTrades":
			if r.Method != "GET" || r.Header.Get("X-MBX-APIKEY") != "mock-key" {
				t.Error("missing signed GET key")
			}
			values := r.URL.Query()
			if values.Get("symbol") != "BTCUSDT" || values.Get("orderId") != "934" || values.Get("limit") != "1000" {
				t.Error("wrong userTrades params")
			}
			sig := values.Get("signature")
			values.Del("signature")
			mac := hmac.New(sha256.New, []byte("mock-secret"))
			mac.Write([]byte(values.Encode()))
			if sig != hex.EncodeToString(mac.Sum(nil)) {
				t.Error("invalid HMAC")
			}
			fmt.Fprint(w, output)
		default:
			t.Error("unexpected path " + r.URL.Path)
		}
	}))
	defer srv.Close()
	c := NewWithBase(srv.URL, srv.Client())
	rows, err := c.FetchDemoOrderTrades(context.Background(), "mock-key", "mock-secret", "BTCUSDT", 934)
	if err != nil || len(rows) != 1 || rows[0].TradeID != 51 {
		t.Fatalf("trades %+v %v", rows, err)
	}
	if _, err := c.FetchDemoOrderTrades(context.Background(), "mock-key", "mock-secret", "BTCUSDT", 0); err == nil {
		t.Fatal("accepted zero ID")
	}
	if _, err := NewWithBase(RealBase, srv.Client()).FetchDemoOrderTrades(context.Background(), "mock-key", "mock-secret", "BTCUSDT", 934); err == nil {
		t.Fatal("real endpoint accepted")
	}
	for _, payload := range []string{
		`[{}]`, `[{"symbol":"BTCUSDT","id":51,"orderId":934,"side":"BUY","positionSide":"LONG","price":"NaN","qty":"0.01","commission":"0","commissionAsset":"USDT","realizedPnl":"0","time":99}]`,
		`[ {"symbol":"BTCUSDT","id":51,"orderId":934,"side":"BUY","positionSide":"LONG","price":"10","qty":"0.01","commission":"0","commissionAsset":"USDT","realizedPnl":"0","time":99}, {"symbol":"BTCUSDT","id":51,"orderId":934,"side":"BUY","positionSide":"LONG","price":"10","qty":"0.01","commission":"0","commissionAsset":"USDT","realizedPnl":"0","time":99}]`,
	} {
		output = payload
		if _, err := c.FetchDemoOrderTrades(context.Background(), "mock-key", "mock-secret", "BTCUSDT", 934); err == nil {
			t.Fatalf("accepted invalid trade data: %s", payload)
		}
	}
}
func TestDecimalValidation(t *testing.T) {
	for _, s := range []string{"NaN", "+1", "0.1.2", "1e3", ".", ".1", "1.", "-1", "", "123%"} {
		if decimalNonNegative(s, true) {
			t.Fatalf("accepted %s", s)
		}
	}
	if !decimalNonNegative("0.000000000000000000", true) || decimalNonNegative("0", false) || !decimalSigned("-0.01") {
		t.Fatal("decimal sign check")
	}
}

func TestTradesAcceptStringOrNumberWithoutFloatConversion(t *testing.T) {
	for _, raw := range []string{
		`{"symbol":"BTCUSDT","id":8,"orderId":99,"side":"SELL","positionSide":"SHORT","price":78000.12345678,"qty":0.010,"commission":0.00045,"commissionAsset":"USDT","realizedPnl":-0.12345678,"time":1790526000000}`,
		`{"symbol":"BTCUSDT","id":8,"orderId":99,"side":"SELL","positionSide":"SHORT","price":"78000.12345678","qty":"0.01","commission":"0.00045","commissionAsset":"USDT","realizedPnl":"-0.12345678","time":1790526000000}`,
	} {
		var item AccountTrade
		if err := json.Unmarshal([]byte(raw), &item); err != nil {
			t.Fatal(err)
		}
		if item.Price != "78000.12345678" || item.RealizedPnL != "-0.12345678" || item.Commission != "0.00045" {
			t.Fatal(item)
		}
	}
}
