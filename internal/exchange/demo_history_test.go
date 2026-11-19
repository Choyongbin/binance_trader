package exchange

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestDemoSignedHistoryWindowAndOrderIDGET(t *testing.T) {
	const start int64 = 1792000000000
	payload := fmt.Sprintf(`[ {"symbol":"BTCUSDT","id":71,"orderId":113,"side":"BUY","positionSide":"SHORT","price":"81761","qty":"0.001","commission":"0.01","commissionAsset":"USDT","realizedPnl":"-0.3","time":%d} ]`, start+1234)
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != "GET" {
			t.Error("non GET request detected")
		}
		switch r.URL.Path {
		case "/fapi/v1/time":
			fmt.Fprintf(w, `{"serverTime":%d}`, time.Now().UnixMilli())
		case "/fapi/v1/userTrades":
			q := r.URL.Query()
			if q.Get("symbol") != "BTCUSDT" || q.Get("startTime") != strconv.FormatInt(start, 10) || q.Get("endTime") != strconv.FormatInt(start+5000, 10) || q.Get("limit") != "1000" || q.Get("orderId") != "" || q.Get("fromId") != "" || q.Get("signature") == "" || r.Header.Get("X-MBX-APIKEY") != "test" {
				t.Error("bad bounded signed GET", q)
			}
			fmt.Fprint(w, payload)
		case "/fapi/v1/order":
			q := r.URL.Query()
			if q.Get("orderId") != "113" || q.Get("origClientOrderId") != "" || q.Get("signature") == "" {
				t.Error("bad orderId request", q)
			}
			fmt.Fprint(w, `{"symbol":"BTCUSDT","orderId":113,"clientOrderId":"web_manual","side":"BUY","positionSide":"SHORT","status":"FILLED","origQty":"0.001","executedQty":"0.001","type":"MARKET"}`)
		default:
			t.Error("unexpected request", r.URL.Path)
		}
	}))
	defer srv.Close()
	c := NewWithBase(srv.URL, srv.Client())
	rows, err := c.FetchDemoTradeWindow(context.Background(), "test", "secret", "BTCUSDT", start, start+5000)
	if err != nil || len(rows) != 1 || rows[0].TradeID != 71 {
		t.Fatalf("signed window %+v %v", rows, err)
	}
	order, err := c.QueryDemoOrderByID(context.Background(), "test", "secret", "BTCUSDT", 113)
	if err != nil || order.OrderID != 113 || calls < 4 {
		t.Fatalf("signed order %+v %v calls=%d", order, err, calls)
	}
	payload = "null"
	if _, err := c.FetchDemoTradeWindow(context.Background(), "test", "secret", "BTCUSDT", start, start+5000); err == nil {
		t.Fatal("null history accepted")
	}
	payload = "[" + strings.TrimRight(strings.Repeat(`{"symbol":"BTCUSDT","id":71,"orderId":113,"side":"BUY","positionSide":"SHORT","price":"81761","qty":"0.001","commission":"0.01","commissionAsset":"USDT","realizedPnl":"-0.3","time":1792000001234},`, 1000), ",") + "]"
	if _, err := c.FetchDemoTradeWindow(context.Background(), "test", "secret", "BTCUSDT", start, start+5000); err == nil {
		t.Fatal("1000 row truncated history accepted")
	}
	for _, p := range []struct{ start, end int64 }{{0, 1}, {start + 100, start}, {start, start + int64(25*time.Hour/time.Millisecond)}} {
		if _, err := c.FetchDemoTradeWindow(context.Background(), "test", "secret", "BTCUSDT", p.start, p.end); err == nil {
			t.Fatalf("invalid interval accepted %+v", p)
		}
	}
	if _, err := NewWithBase(RealBase, srv.Client()).FetchDemoTradeWindow(context.Background(), "test", "secret", "BTCUSDT", start, start+5000); err == nil {
		t.Fatal("mainnet allowed")
	}
}
