package trading

import (
	"binance_trader/internal/exchange"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

const exchangeInfoTest = `{"symbols":[{"symbol":"BTCUSDT","status":"TRADING","contractType":"PERPETUAL","quoteAsset":"USDT","marginAsset":"USDT","filters":[{"filterType":"PRICE_FILTER","minPrice":"0.1","maxPrice":"1000000","tickSize":"0.1"},{"filterType":"LOT_SIZE","minQty":"0.001","maxQty":"100","stepSize":"0.001"},{"filterType":"MARKET_LOT_SIZE","minQty":"0.001","maxQty":"50","stepSize":"0.001"},{"filterType":"MIN_NOTIONAL","notional":"100"}]}]}`

func TestDemoExecutorMockGateJournalAndReadOnlyReconciliation(t *testing.T) {
	var submissions, queries, gateCalls atomic.Int32
	var broken atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/fapi/v1/exchangeInfo":
			w.Write([]byte(exchangeInfoTest))
		case "/fapi/v1/time":
			fmt.Fprintf(w, `{"serverTime":%d}`, time.Now().UnixMilli())
		case "/fapi/v1/order":
			if r.Method == http.MethodPost {
				submissions.Add(1)
				if broken.Load() {
					w.WriteHeader(500)
					w.Write([]byte(`{"code":-1007}`))
					return
				}
				fmt.Fprintf(w, `{"symbol":"BTCUSDT","clientOrderId":"%s","orderId":121,"status":"NEW","side":"BUY","positionSide":"LONG","type":"MARKET"}`, r.FormValue("newClientOrderId"))
			} else {
				queries.Add(1)
				fmt.Fprintf(w, `{"symbol":"BTCUSDT","clientOrderId":"%s","orderId":121,"status":"FILLED","side":"BUY","positionSide":"LONG","type":"MARKET"}`, r.FormValue("origClientOrderId"))
			}
		default:
			t.Errorf("unexpected endpoint %s", r.URL)
		}
	}))
	defer server.Close()
	client := exchange.NewWithBase(server.URL, server.Client())
	gateBlocked := false
	quote := func(context.Context, string) (Quote, error) {
		return Quote{Symbol: "BTCUSDT", Price: "84510.6", ReceivedAt: time.Now().UTC()}, nil
	}
	creds := func(context.Context) (DemoCredentials, error) { return DemoCredentials{"key", "secret"}, nil }
	exec, err := NewDemoExecutor(client, creds, quote, func(_ context.Context, p PlannedOrder, r exchange.SymbolRules, q Quote, creds DemoCredentials) error {
		gateCalls.Add(1)
		if r.Symbol != "BTCUSDT" || q.Symbol != "BTCUSDT" {
			return errors.New("bad input")
		}
		if gateBlocked {
			return errors.New("stale account")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewDemoExecutor(nil, creds, quote, func(context.Context, PlannedOrder, exchange.SymbolRules, Quote, DemoCredentials) error { return nil }); err == nil {
		t.Fatal("nil client")
	}
	if _, err := NewDemoExecutor(client, creds, nil, func(context.Context, PlannedOrder, exchange.SymbolRules, Quote, DemoCredentials) error { return nil }); err == nil {
		t.Fatal("nil quote")
	}
	if _, err := NewDemoExecutor(client, creds, quote, nil); err == nil {
		t.Fatal("nil guard")
	}
	intent := sampleIntent()
	intent.Quantity = "0.004"
	plan, _ := intent.Plan()
	gateBlocked = true
	if _, err := exec.Submit(context.Background(), plan); err == nil || submissions.Load() != 0 {
		t.Fatal("blocked gate still posted")
	}
	gateBlocked = false
	engine, err := NewEngine(t.TempDir(), exec)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := engine.Submit(context.Background(), intent, allow)
	if err != nil || rec.State != Acknowledged || submissions.Load() != 1 || gateCalls.Load() != 2 {
		t.Fatalf("bad mock live execution %+v %v posts=%d", rec, err, submissions.Load())
	}
	rec, err = engine.Submit(context.Background(), intent, allow)
	if err != nil || rec.State != Acknowledged || submissions.Load() != 1 {
		t.Fatalf("duplicate posted %+v %v", rec, err)
	}
	rec, err = engine.Reconcile(context.Background(), intent, exec)
	if err != nil || rec.State != Reconciled || rec.ExchangeStatus != "FILLED" || queries.Load() != 1 || submissions.Load() != 1 {
		t.Fatalf("bad reconciliation %+v %v", rec, err)
	}
	// Separate unknown request: simulated HTTP 500 must not be retried.
	broken.Store(true)
	next := intent
	next.RequestID = "manual-other-0002"
	rec, err = engine.Submit(context.Background(), next, allow)
	if err == nil || rec.State != Unknown || submissions.Load() != 2 {
		t.Fatalf("uncertain send mishandled %+v %v", rec, err)
	}
	_, err = engine.Submit(context.Background(), next, allow)
	if !errors.Is(err, ErrNeedsReconciliation) || submissions.Load() != 2 {
		t.Fatal("unknown order was retransmitted")
	}
	broken.Store(false)
	rec, err = engine.Reconcile(context.Background(), next, exec)
	if err != nil || rec.ExchangeStatus != "FILLED" || submissions.Load() != 2 {
		t.Fatalf("reconcile wrong %+v %v", rec, err)
	}
	// No hidden live Demo call: every request went to local httptest server.
}
