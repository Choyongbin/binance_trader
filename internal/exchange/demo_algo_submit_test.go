package exchange

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func testProtectionOrder() NewDemoProtection {
	return NewDemoProtection{ClientAlgoID: "btp_test_limit_long", Symbol: "BTCUSDT", Side: "SELL", PositionSide: "LONG", Type: "TAKE_PROFIT_MARKET", Quantity: "0.010", TriggerPrice: "100333.4", WorkingType: "CONTRACT_PRICE"}
}
func TestSubmitDemoProtectionExactSignedPOSTNoUnsafeFields(t *testing.T) {
	var post atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/fapi/v1/time" {
			fmt.Fprintf(w, `{"serverTime":%d}`, time.Now().UnixMilli())
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != "/fapi/v1/algoOrder" {
			t.Errorf("unexpected method/path %s %s", r.Method, r.URL.Path)
			w.WriteHeader(404)
			return
		}
		post.Add(1)
		data, _ := io.ReadAll(r.Body)
		v, e := url.ParseQuery(string(data))
		if e != nil {
			t.Error(e)
		}
		for _, k := range []string{"closePosition", "reduceOnly", "price"} {
			if v.Has(k) {
				t.Errorf("unexpected unsafe field %s", k)
			}
		}
		if v.Get("algoType") != "CONDITIONAL" || v.Get("workingType") != "CONTRACT_PRICE" || v.Get("triggerPrice") != "100333.4" || v.Get("quantity") != "0.010" || v.Get("clientAlgoId") != "btp_test_limit_long" || v.Get("signature") == "" || r.Header.Get("X-MBX-APIKEY") != "demo-key" {
			t.Errorf("invalid submitted payload %v", v)
		}
		fmt.Fprint(w, `{"algoId":121,"clientAlgoId":"btp_test_limit_long","algoType":"CONDITIONAL","orderType":"TAKE_PROFIT_MARKET","symbol":"BTCUSDT","side":"SELL","positionSide":"LONG","quantity":0.01,"triggerPrice":100333.40,"workingType":"CONTRACT_PRICE","closePosition":false,"reduceOnly":false,"algoStatus":"NEW"}`)
	}))
	defer srv.Close()
	c := NewWithBase(srv.URL, srv.Client())
	ack, err := c.SubmitDemoProtection(context.Background(), "demo-key", "secret", testProtectionOrder())
	if err != nil || ack.AlgoID != 121 || post.Load() != 1 {
		t.Fatalf("unexpected ack %+v %v posts:%d", ack, err, post.Load())
	}
}
func TestSubmitDemoProtectionBlocksWrongRequestsWithoutNetwork(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { n.Add(1); w.WriteHeader(500) }))
	defer srv.Close()
	c := NewWithBase(srv.URL, srv.Client())
	cases := []func(*NewDemoProtection){
		func(o *NewDemoProtection) { o.Symbol = "ETHUSDT" },
		func(o *NewDemoProtection) { o.Side = "BUY" },
		func(o *NewDemoProtection) { o.Quantity = "NaN" },
		func(o *NewDemoProtection) { o.Quantity = "0" },
		func(o *NewDemoProtection) { o.TriggerPrice = "0" },
		func(o *NewDemoProtection) { o.TriggerPrice = "1e9" },
		func(o *NewDemoProtection) { o.ClientAlgoID = "no spaces accepted" },
		func(o *NewDemoProtection) { o.Type = "STOP_MARKET" },
		func(o *NewDemoProtection) { o.WorkingType = "MARK_PRICE" },
	}
	for idx, fn := range cases {
		order := testProtectionOrder()
		fn(&order)
		if _, err := c.SubmitDemoProtection(context.Background(), "key", "secret", order); err == nil {
			t.Fatalf("invalid order %d accepted", idx)
		}
	}
	if n.Load() != 0 {
		t.Fatalf("invalid payload reached exchange %d", n.Load())
	}
	if _, err := NewWithBase(RealBase, &http.Client{}).SubmitDemoProtection(context.Background(), "key", "secret", testProtectionOrder()); err == nil {
		t.Fatal("live REAL unlocked")
	}
}
func TestSubmitDemoProtectionMalformedAckNeverResend(t *testing.T) {
	for _, fragment := range []string{`"quantity":"0.03"`, `"triggerPrice":"100333.5"`, `"clientAlgoId":"other"`, `"reduceOnly":true`, `"positionSide":"SHORT"`} {
		t.Run(fragment, func(t *testing.T) {
			var posts atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/fapi/v1/time" {
					fmt.Fprintf(w, `{"serverTime":%d}`, time.Now().UnixMilli())
					return
				}
				posts.Add(1)
				msg := `{"algoId":121,"clientAlgoId":"btp_test_limit_long","algoType":"CONDITIONAL","orderType":"TAKE_PROFIT_MARKET","symbol":"BTCUSDT","side":"SELL","positionSide":"LONG","quantity":"0.01","triggerPrice":"100333.4","workingType":"CONTRACT_PRICE","closePosition":false,"reduceOnly":false,"algoStatus":"NEW"}`
				bits := strings.SplitN(fragment, ":", 2)
				for _, key := range []string{`"quantity"`, `"triggerPrice"`, `"clientAlgoId"`, `"reduceOnly"`, `"positionSide"`} {
					if bits[0] == key {
						importRE := strings.Index(msg, key+":")
						if importRE >= 0 {
							start := importRE + len(key) + 1
							end := start
							for end < len(msg) && msg[end] != ',' && msg[end] != '}' {
								end++
							}
							msg = msg[:start] + bits[1] + msg[end:]
							break
						}
					}
				}
				fmt.Fprint(w, msg)
			}))
			defer srv.Close()
			c := NewWithBase(srv.URL, srv.Client())
			if _, err := c.SubmitDemoProtection(context.Background(), "key", "secret", testProtectionOrder()); err == nil {
				t.Fatal("bad ack accepted")
			}
			if posts.Load() != 1 {
				t.Fatalf("auto retried POST: %d", posts.Load())
			}
		})
	}
}
