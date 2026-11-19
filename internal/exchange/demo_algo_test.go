package exchange

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestAlgoSignedReadCancelAndNoReissue(t *testing.T) {
	var gets, deletes atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/fapi/v1/time" {
			fmt.Fprintf(w, `{"serverTime":%d}`, time.Now().UnixMilli())
			return
		}
		if r.URL.Path != "/fapi/v1/algoOrder" || r.Header.Get("X-MBX-APIKEY") != "demo-only" || r.URL.Query().Get("signature") == "" || r.URL.Query().Get("algoId") != "100" {
			t.Errorf("bad query %s %s", r.URL, r.Method)
			w.WriteHeader(400)
			return
		}
		switch r.Method {
		case "GET":
			gets.Add(1)
			fmt.Fprint(w, `{"algoId":100,"clientAlgoId":"btp_test","algoStatus":"CANCELED","symbol":"BTCUSDT","side":"SELL","positionSide":"LONG","quantity":"0.01","orderType":"STOP_MARKET"}`)
		case "DELETE":
			deletes.Add(1)
			fmt.Fprint(w, `{"algoId":100,"code":200,"msg":"success"}`)
		default:
			t.Error("unexpected method")
		}
	}))
	defer srv.Close()
	c := NewWithBase(srv.URL, srv.Client())
	a, err := c.CancelDemoAlgoOnce(context.Background(), "demo-only", "secret", 100)
	if err != nil || a.Status != "CANCELED" || gets.Load() != 1 || deletes.Load() != 1 {
		t.Fatalf("cancel %+v %v %d/%d", a, err, gets.Load(), deletes.Load())
	}
	if _, err := NewWithBase(RealBase, &http.Client{}).CancelDemoAlgoOnce(context.Background(), "demo-only", "secret", 100); err == nil {
		t.Fatal("real cancel unlocked")
	}
}
func TestAlgoUnexpectedGetNeverCancels(t *testing.T) {
	var deletes atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/fapi/v1/time" {
			fmt.Fprintf(w, `{"serverTime":%d}`, time.Now().UnixMilli())
			return
		}
		if r.Method == http.MethodDelete {
			deletes.Add(1)
		}
		w.WriteHeader(404)
		fmt.Fprint(w, `{"code":-2013}`)
	}))
	defer srv.Close()
	c := NewWithBase(srv.URL, srv.Client())
	if _, err := c.QueryDemoAlgo(context.Background(), "demo", "secret", 100); err == nil {
		t.Fatal("missing order incorrectly confirmed")
	}
	if deletes.Load() != 0 {
		t.Fatal("query attempted cancel")
	}
}
