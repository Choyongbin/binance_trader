package exchange

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestFetchDemoFundingWindowStrict(t *testing.T) {
	now := time.Now().UnixMilli()
	mode := "normal"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Error("non read-only request")
		}
		switch r.URL.Path {
		case "/fapi/v1/time":
			fmt.Fprintf(w, `{"serverTime":%d}`, time.Now().UnixMilli())
		case "/fapi/v1/income":
			q := r.URL.Query()
			if q.Get("incomeType") != "FUNDING_FEE" || q.Get("symbol") != "BTCUSDT" || q.Get("limit") != "1000" || q.Get("signature") == "" || r.Header.Get("X-MBX-APIKEY") != "key" {
				t.Error("unsigned or unbounded funding GET")
			}
			switch mode {
			case "normal":
				fmt.Fprintf(w, `[{"symbol":"BTCUSDT","incomeType":"FUNDING_FEE","income":"-0.0001","asset":"USDT","time":%d,"tranId":456}]`, now)
			case "null":
				fmt.Fprint(w, "null")
			case "wrong":
				fmt.Fprintf(w, `[{"symbol":"ETHUSDT","incomeType":"FUNDING_FEE","income":"-0.0001","asset":"USDT","time":%d,"tranId":456}]`, now)
			case "dup":
				fmt.Fprintf(w, `[{"symbol":"BTCUSDT","incomeType":"FUNDING_FEE","income":"0.01","asset":"USDT","time":%d,"tranId":456},{"symbol":"BTCUSDT","incomeType":"FUNDING_FEE","income":"0.01","asset":"USDT","time":%d,"tranId":456}]`, now, now)
			case "malformed":
				fmt.Fprintf(w, `[{"symbol":"BTCUSDT","incomeType":"FUNDING_FEE","income":"NaN","asset":"USDT","time":%d,"tranId":456}]`, now)
			}
		default:
			t.Error("unexpected path", r.URL.Path)
		}
	}))
	defer srv.Close()
	c := NewWithBase(srv.URL, srv.Client())
	rows, err := c.FetchDemoFundingWindow(context.Background(), "key", "secret", "BTCUSDT", now-1000, now+1000)
	if err != nil || len(rows) != 1 || rows[0].Income != "-0.0001" {
		t.Fatalf("funding %+v %v", rows, err)
	}
	for _, v := range []string{"null", "wrong", "dup", "malformed"} {
		mode = v
		if _, err := c.FetchDemoFundingWindow(context.Background(), "key", "secret", "BTCUSDT", now-1000, now+1000); err == nil {
			t.Errorf("accepted %s", v)
		}
	}
	for _, bounds := range [][2]int64{{0, 1000}, {now + 1, now}, {now, now + int64(25*time.Hour/time.Millisecond)}} {
		if _, err := c.FetchDemoFundingWindow(context.Background(), "key", "secret", "BTCUSDT", bounds[0], bounds[1]); err == nil {
			t.Error("unsafe time bounds")
		}
	}
}
