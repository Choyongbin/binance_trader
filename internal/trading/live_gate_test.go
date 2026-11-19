package trading

import (
	"binance_trader/internal/exchange"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestLivePreflightMockQueriesNeverPosts(t *testing.T) {
	var posts, queries atomic.Int32
	var mismatch atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			posts.Add(1)
			w.WriteHeader(500)
			return
		}
		queries.Add(1)
		switch r.URL.Path {
		case "/fapi/v1/time":
			fmt.Fprintf(w, `{"serverTime":%d}`, time.Now().UnixMilli())
		case "/fapi/v3/account":
			if mismatch.Load() {
				w.Write([]byte(`{"totalWalletBalance":"5000.0713","totalUnrealizedProfit":"0","totalMarginBalance":"5000.0713","totalInitialMargin":"0","totalPositionInitialMargin":"0","totalOpenOrderInitialMargin":"0","availableBalance":"5000.0713","assets":[{"asset":"USDT","walletBalance":"5000.0713","availableBalance":"5000.0713","marginBalance":"5000.0713"}],"positions":[{"symbol":"BTCUSDT","positionSide":"LONG","positionAmt":"0.01","initialMargin":"0","isolated":true}]}`))
			} else {
				w.Write([]byte(`{"totalWalletBalance":"5000.0713","totalUnrealizedProfit":"0","totalMarginBalance":"5000.0713","totalInitialMargin":"0","totalPositionInitialMargin":"0","totalOpenOrderInitialMargin":"0","availableBalance":"5000.0713","assets":[{"asset":"USDT","walletBalance":"5000.0713","availableBalance":"5000.0713","marginBalance":"5000.0713"}],"positions":[]}`))
			}
		case "/fapi/v1/accountConfig":
			w.Write([]byte(`{"canTrade":true,"multiAssetsMargin":false,"dualSidePosition":true}`))
		case "/fapi/v3/balance":
			w.Write([]byte(`[{"asset":"USDT","balance":"5000.0713","availableBalance":"5000.0713"}]`))
		case "/fapi/v3/positionRisk":
			if mismatch.Load() {
				w.Write([]byte(`[{"symbol":"BTCUSDT","positionSide":"LONG","positionAmt":"0.01"}]`))
			} else {
				w.Write([]byte(`[]`))
			}
		case "/fapi/v1/openOrders", "/fapi/v1/openAlgoOrders":
			w.Write([]byte(`[]`))
		case "/fapi/v1/positionSide/dual":
			w.Write([]byte(`{"dualSidePosition":true}`))
		case "/fapi/v1/symbolConfig":
			w.Write([]byte(`[{"symbol":"BTCUSDT","marginType":"ISOLATED","leverage":15,"isAutoAddMargin":false}]`))
		default:
			t.Errorf("unexpected endpoint %s", r.URL)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	c := exchange.NewWithBase(server.URL, server.Client())
	pIntent := sampleIntent()
	pIntent.Quantity = "0.004"
	plan, _ := pIntent.Plan()
	gate, err := NewFirstDemoLiveGate(c, pIntent, 10)
	if err != nil {
		t.Fatal(err)
	}
	rules := sizingRules(time.Now().UTC())
	q := Quote{Symbol: "BTCUSDT", Price: "84510.6", ReceivedAt: time.Now().UTC()}
	if err := gate(context.Background(), plan, rules, q, DemoCredentials{"k", "s"}); err != nil {
		t.Fatal(err)
	}
	if queries.Load() < 8 || posts.Load() != 0 {
		t.Fatalf("read-only gate queries=%d posts=%d", queries.Load(), posts.Load())
	}
	mismatch.Store(true)
	if err := gate(context.Background(), plan, rules, q, DemoCredentials{"k", "s"}); err == nil || !strings.Contains(err.Error(), "포지션") {
		t.Fatalf("existing position not blocked: %v", err)
	}
	if posts.Load() != 0 {
		t.Fatal("preflight made a POST")
	}
	bad := plan
	bad.Quantity = "0.005"
	if err := gate(context.Background(), bad, rules, q, DemoCredentials{"k", "s"}); err == nil {
		t.Fatal("mutated plan accepted")
	}
	model := pIntent
	model.Source = Model
	model.ModelID = "a"
	if _, err := NewFirstDemoLiveGate(c, model, 10); err == nil {
		t.Fatal("model gate exposed before ledger")
	}
}
