package exchange

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSignedDemoAccountSnapshot(t *testing.T) {
	calls := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls[r.URL.Path]++
		if r.Method != "GET" {
			t.Errorf("unexpected non-read endpoint %s", r.Method)
			w.WriteHeader(405)
			return
		}
		switch r.URL.Path {
		case "/fapi/v1/time":
			fmt.Fprintf(w, `{"serverTime":%d}`, time.Now().UnixMilli())
			return
		case "/fapi/v3/balance", "/fapi/v3/positionRisk", "/fapi/v1/openOrders", "/fapi/v1/openAlgoOrders":
			if r.Header.Get("X-MBX-APIKEY") != "local-test-key" {
				t.Error("api key absent")
			}
			params := r.URL.Query()
			got := params.Get("signature")
			params.Del("signature")
			m := hmac.New(sha256.New, []byte("local-test-secret"))
			m.Write([]byte(params.Encode()))
			if got != hex.EncodeToString(m.Sum(nil)) {
				t.Error("bad Binance query signature")
			}
			if params.Get("recvWindow") != "5000" {
				t.Error("recvWindow missing")
			}
			if r.URL.Path != "/fapi/v3/balance" && params.Get("symbol") != "BTCUSDT" {
				t.Error("missing required symbol")
			}
			switch r.URL.Path {
			case "/fapi/v3/balance":
				fmt.Fprint(w, `[{"asset":"USDT","balance":"1000.1","availableBalance":"900.5"}]`)
			case "/fapi/v3/positionRisk":
				fmt.Fprint(w, `[{"symbol":"BTCUSDT","positionSide":"LONG","positionAmt":"0","entryPrice":"0"},{"symbol":"BTCUSDT","positionSide":"SHORT","positionAmt":"-0.2","entryPrice":"120000","breakEvenPrice":"119800","unRealizedProfit":"2.5"}]`)
			default:
				fmt.Fprint(w, `[]`)
			}
			return
		default:
			t.Error("unknown path", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer srv.Close()
	client := NewWithBase(srv.URL, srv.Client())
	result, err := client.Fetch(context.Background(), "local-test-key", "local-test-secret")
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "connected" || len(result.Balances) != 1 || len(result.Positions) != 1 || result.Positions[0].BreakEven != "119800" {
		t.Fatalf("unexpected snapshot %+v", result)
	}
	if result.Orders == nil || result.AlgoOrders == nil {
		t.Fatal("empty lists must be known, not null")
	}
	for _, path := range []string{"/fapi/v1/time", "/fapi/v3/balance", "/fapi/v3/positionRisk", "/fapi/v1/openOrders", "/fapi/v1/openAlgoOrders"} {
		if calls[path] != 1 {
			t.Errorf("endpoint %s called %d", path, calls[path])
		}
	}
}
func TestFailClosedOnPartialAccountFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/fapi/v1/time":
			fmt.Fprintf(w, `{"serverTime":%d}`, time.Now().UnixMilli())
		case "/fapi/v3/balance":
			fmt.Fprint(w, `[{"asset":"USDT","balance":"100"}]`)
		case "/fapi/v3/positionRisk":
			fmt.Fprint(w, `[]`)
		case "/fapi/v1/openOrders":
			http.Error(w, `{"code":-2015,"msg":"bad key"}`, 401)
		default:
			t.Error("unexpected endpoint", r.URL.Path)
		}
	}))
	defer srv.Close()
	result, err := NewWithBase(srv.URL, srv.Client()).Fetch(context.Background(), "any", "any")
	if err == nil || result.Status != "" || result.Balances != nil {
		t.Fatalf("returned partial data: %+v %v", result, err)
	}
	if strings.Contains(err.Error(), "any") {
		t.Fatal("secret leakage")
	}
}
