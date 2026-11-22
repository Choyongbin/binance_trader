package exchange

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func demoAlgoReadFixture(t *testing.T, reply func(http.ResponseWriter, *http.Request)) (*Client, *atomic.Int32) {
	t.Helper()
	var mutations atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/fapi/v1/time" {
			fmt.Fprintf(w, `{"serverTime":%d}`, time.Now().UnixMilli())
			return
		}
		if r.Method != http.MethodGet {
			mutations.Add(1)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Path != "/fapi/v1/algoOrder" || r.Header.Get("X-MBX-APIKEY") != "demo-key" ||
			r.URL.Query().Get("clientAlgoId") != "safe_tp_001" || r.URL.Query().Has("algoId") ||
			r.URL.Query().Get("timestamp") == "" || r.URL.Query().Get("recvWindow") != "5000" {
			t.Errorf("unexpected signed request: %s", r.URL)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		q := r.URL.Query()
		supplied := q.Get("signature")
		q.Del("signature")
		if supplied == "" || supplied != signatureHex(q.Encode(), "demo-secret") {
			t.Errorf("signed GET HMAC mismatch")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		reply(w, r)
	}))
	t.Cleanup(srv.Close)
	return NewWithBase(srv.URL, srv.Client()), &mutations
}

func validClientAlgoResponse() string {
	return `{"algoId":932,"clientAlgoId":"safe_tp_001","algoStatus":"NEW",` +
		`"algoType":"CONDITIONAL","symbol":"BTCUSDT","side":"SELL","positionSide":"LONG",` +
		`"quantity":"0.010","triggerPrice":"101000.0","orderType":"TAKE_PROFIT_MARKET",` +
		`"workingType":"CONTRACT_PRICE","closePosition":false,"reduceOnly":false}`
}

func TestQueryDemoAlgoByClientIDUsesOnlySignedGet(t *testing.T) {
	var gets atomic.Int32
	c, mutations := demoAlgoReadFixture(t, func(w http.ResponseWriter, _ *http.Request) {
		gets.Add(1)
		fmt.Fprint(w, validClientAlgoResponse())
	})
	for i := 0; i < 2; i++ {
		got, err := c.QueryDemoAlgoByClientID(context.Background(), "demo-key", "demo-secret", "safe_tp_001")
		if err != nil || got.AlgoID != 932 || got.ClientAlgoID != "safe_tp_001" {
			t.Fatalf("signed GET result: %+v err=%v", got, err)
		}
	}
	if gets.Load() != 2 || mutations.Load() != 0 {
		t.Fatalf("GETs=%d mutations=%d", gets.Load(), mutations.Load())
	}
}

func TestQueryDemoAlgoByClientIDNotFoundNeverReissues(t *testing.T) {
	c, mutations := demoAlgoReadFixture(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"code":-2013,"msg":"Order does not exist"}`)
	})
	_, err := c.QueryDemoAlgoByClientID(context.Background(), "demo-key", "demo-secret", "safe_tp_001")
	if !errors.Is(err, ErrOrderNotFound) || mutations.Load() != 0 {
		t.Fatalf("missing Algo must remain inconclusive without mutation: %v %d", err, mutations.Load())
	}
}

func TestQueryDemoAlgoByClientIDFailClosed(t *testing.T) {
	for _, tc := range []struct{ name, body string }{
		{"wrong client id", strings.Replace(validClientAlgoResponse(), "safe_tp_001", "foreign_tp", 1)},
		{"unexpected algo type", strings.Replace(validClientAlgoResponse(), "CONDITIONAL", "OTHER", 1)},
		{"wrong position mode", strings.Replace(validClientAlgoResponse(), `"LONG"`, `"BOTH"`, 1)},
		{"close position enabled", strings.Replace(validClientAlgoResponse(), `"closePosition":false`, `"closePosition":true`, 1)},
		{"invalid decimal", strings.Replace(validClientAlgoResponse(), `"0.010"`, `"0.000"`, 1)},
		{"missing closePosition flag", strings.Replace(validClientAlgoResponse(), `,"closePosition":false`, "", 1)},
		{"missing reduceOnly flag", strings.Replace(validClientAlgoResponse(), `,"reduceOnly":false`, "", 1)},
		{"malformed JSON", `{"algoId":`},
		{"unknown status", strings.Replace(validClientAlgoResponse(), `"NEW"`, `"UNKNOWN"`, 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, mutations := demoAlgoReadFixture(t, func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, tc.body) })
			if _, err := c.QueryDemoAlgoByClientID(context.Background(), "demo-key", "demo-secret", "safe_tp_001"); err == nil || mutations.Load() != 0 {
				t.Fatalf("unsafe acceptance: err=%v mutations=%d", err, mutations.Load())
			}
		})
	}
}

func TestQueryDemoAlgoByClientIDRejectsRealAndInvalidInputBeforeNetwork(t *testing.T) {
	for _, id := range []string{"", "../../unsafe", strings.Repeat("x", 37)} {
		c := NewDemoClient()
		if _, err := c.QueryDemoAlgoByClientID(context.Background(), "demo-key", "demo-secret", id); err == nil {
			t.Errorf("accepted invalid clientAlgoId %q", id)
		}
	}
	c := NewWithBase(RealBase, &http.Client{})
	if _, err := c.QueryDemoAlgoByClientID(context.Background(), "demo-key", "demo-secret", "safe_tp_001"); err == nil {
		t.Fatal("real environment must remain blocked")
	}
}
