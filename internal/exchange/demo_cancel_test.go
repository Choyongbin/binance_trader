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

func TestCancelDemoSignedDeleteAndTerminalReadback(t *testing.T) {
	var deletes, reads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/fapi/v1/time":
			fmt.Fprintf(w, `{"serverTime":%d}`, time.Now().UnixMilli())
		case "/fapi/v1/order":
			if r.Header.Get("X-MBX-APIKEY") != "mock-demo" {
				t.Error("unexpected credential")
			}
			if r.Method == http.MethodDelete {
				deletes.Add(1)
				if r.URL.Query().Get("signature") == "" || r.URL.Query().Get("origClientOrderId") != "bt_cancel001" {
					t.Error("DELETE must be signed query")
				}
				body, _ := io.ReadAll(r.Body)
				if len(body) != 0 {
					t.Error("DELETE should not put signed data in body")
				}
				fmt.Fprint(w, `{"symbol":"BTCUSDT","clientOrderId":"bt_cancel001","orderId":19,"status":"CANCELED","side":"BUY","positionSide":"LONG","type":"LIMIT","origQty":"0.02","executedQty":"0.007"}`)
			} else if r.Method == http.MethodGet {
				reads.Add(1)
				fmt.Fprint(w, `{"symbol":"BTCUSDT","clientOrderId":"bt_cancel001","orderId":19,"status":"CANCELED","side":"BUY","positionSide":"LONG","type":"LIMIT","origQty":"0.02","executedQty":"0.007"}`)
			} else {
				t.Errorf("unexpected %s", r.Method)
			}
		default:
			t.Errorf("unknown route %s", r.URL.Path)
		}
	}))
	defer server.Close()
	client := NewWithBase(server.URL, server.Client())
	result, err := client.CancelDemoOrder(context.Background(), "mock-demo", "secret", "BTCUSDT", "bt_cancel001")
	if err != nil || result.Status != "CANCELED" || result.ExecutedQty != "0.007" || deletes.Load() != 1 || reads.Load() != 1 {
		t.Fatalf("cancel %+v err=%v counts=%d/%d", result, err, deletes.Load(), reads.Load())
	}
	if _, err := NewWithBase(RealBase, &http.Client{}).CancelDemoOrder(context.Background(), "key", "secret", "BTCUSDT", "bt_cancel001"); err == nil {
		t.Fatal("real cancel enabled")
	}
	if _, err := client.CancelDemoOrder(context.Background(), "key", "secret", "ETHUSDT", "bt_cancel001"); err == nil {
		t.Fatal("other symbol accepted")
	}
}
func TestCancelFailureNeverRetries(t *testing.T) {
	var deletes atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/fapi/v1/time" {
			fmt.Fprintf(w, `{"serverTime":%d}`, time.Now().UnixMilli())
			return
		}
		if r.Method == http.MethodDelete {
			deletes.Add(1)
			w.Header().Set("Location", "https://fapi.binance.com/fapi/v1/order")
			w.WriteHeader(http.StatusTemporaryRedirect)
			return
		}
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"code":-2013}`)
	}))
	defer srv.Close()
	client := NewWithBase(srv.URL, srv.Client())
	if _, err := client.CancelDemoOrder(context.Background(), "mock", "secret", "BTCUSDT", "bt_cancel001"); err == nil || deletes.Load() != 1 {
		t.Fatalf("unexpected retry or success %v %d", err, deletes.Load())
	}
	if _, err := client.QueryDemoOrder(context.Background(), "mock", "secret", "BTCUSDT", "bt_cancel001"); err != ErrOrderNotFound {
		t.Fatalf("not-found should stay ambiguous %v", err)
	}
	if deletes.Load() != 1 {
		t.Fatal("Query caused a delete")
	}
}
func TestCancelIncompleteReadbackFailsClosed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/fapi/v1/time" {
			fmt.Fprintf(w, `{"serverTime":%d}`, time.Now().UnixMilli())
			return
		}
		if r.Method == http.MethodDelete {
			fmt.Fprint(w, `{"symbol":"BTCUSDT","clientOrderId":"bt_cancel001","orderId":19,"status":"CANCELED","side":"BUY","positionSide":"LONG","type":"LIMIT"}`)
			return
		}
		fmt.Fprint(w, `{"symbol":"BTCUSDT","clientOrderId":"bt_cancel001","orderId":19,"status":"CANCELED","side":"BUY","positionSide":"LONG","type":"LIMIT"}`)
	}))
	defer srv.Close()
	cli := NewWithBase(srv.URL, srv.Client())
	_, err := cli.CancelDemoOrder(context.Background(), "key", "secret", "BTCUSDT", "bt_cancel001")
	if err == nil || !strings.Contains(err.Error(), "조회 불일치") {
		t.Fatalf("accepted incomplete order %+v", err)
	}
}

// keep explicit use of url.Values for strict query validations in future tests
var _ url.Values
