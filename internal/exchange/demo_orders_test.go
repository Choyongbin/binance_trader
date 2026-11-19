package exchange

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
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

func TestDemoSignedOrderSerializationReadbackAndNoRedirect(t *testing.T) {
	var posts, queries atomic.Int32
	cli := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/fapi/v1/time":
			fmt.Fprintf(w, `{"serverTime":%d}`, time.Now().UnixMilli())
		case "/fapi/v1/order":
			if r.Header.Get("X-MBX-APIKEY") != "test-key" {
				t.Error("missing API key")
			}
			var v url.Values
			if r.Method == http.MethodPost {
				posts.Add(1)
				if r.Header.Get("Content-Type") != "application/x-www-form-urlencoded" {
					t.Error("wrong Content-Type")
				}
				raw, _ := io.ReadAll(r.Body)
				v, _ = url.ParseQuery(string(raw))
				if v.Get("reduceOnly") != "" || v.Get("closePosition") != "" || v.Get("stopPrice") != "" {
					t.Error("forbidden Hedge flags")
				}
				if v.Get("newOrderRespType") != "ACK" || v.Get("newClientOrderId") != "bt_test001" {
					t.Error("wrong id/ACK")
				}
			} else if r.Method == http.MethodGet {
				queries.Add(1)
				v = r.URL.Query()
				if v.Get("origClientOrderId") != "bt_test001" {
					t.Error("no client ID")
				}
			}
			sig := v.Get("signature")
			v.Del("signature")
			mac := hmac.New(sha256.New, []byte("test-secret"))
			mac.Write([]byte(v.Encode()))
			if sig != hex.EncodeToString(mac.Sum(nil)) {
				t.Errorf("bad signature: query %s", v.Encode())
			}
			if v.Get("symbol") != "BTCUSDT" {
				t.Error("wrong symbol")
			}
			fmt.Fprint(w, `{"symbol":"BTCUSDT","clientOrderId":"bt_test001","orderId":1234,"status":"NEW","side":"BUY","positionSide":"LONG","type":"MARKET","origQty":"0.004","executedQty":"0"}`)
		default:
			t.Errorf("unexpected endpoint %s", r.URL)
			w.WriteHeader(404)
		}
	}))
	defer cli.Close()
	c := NewWithBase(cli.URL, cli.Client())
	req := NewOrderRequest{Symbol: "BTCUSDT", Side: "BUY", PositionSide: "LONG", Type: "MARKET", Quantity: "0.004", ClientOrderID: "bt_test001"}
	ack, err := c.SubmitDemoOrder(context.Background(), "test-key", "test-secret", req)
	if err != nil || ack.ClientOrderID != req.ClientOrderID || ack.OrderID != 1234 || posts.Load() != 1 {
		t.Fatalf("mock submit %+v %v", ack, err)
	}
	order, err := c.QueryDemoOrder(context.Background(), "test-key", "test-secret", "BTCUSDT", "bt_test001")
	if err != nil || order.Status != "NEW" || queries.Load() != 1 {
		t.Fatalf("mock query %+v %v", order, err)
	}
	// Direct call MUST refuse real URL with no network requests.
	if _, err := NewWithBase(RealBase, &http.Client{}).SubmitDemoOrder(context.Background(), "x", "x", req); err == nil {
		t.Fatal("real submission enabled")
	}
}
func TestDemoOrderAmbiguityAndNotFoundNeverRetries(t *testing.T) {
	var posts atomic.Int32
	cli := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/fapi/v1/time":
			fmt.Fprintf(w, `{"serverTime":%d}`, time.Now().UnixMilli())
		case "/fapi/v1/order":
			if r.Method == http.MethodPost {
				posts.Add(1)
				w.Header().Set("Location", "https://fapi.binance.com/fapi/v1/order")
				w.WriteHeader(http.StatusTemporaryRedirect)
				return
			}
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, `{"code":-2013,"msg":"Order does not exist."}`)
		}
	}))
	defer cli.Close()
	c := NewWithBase(cli.URL, cli.Client())
	req := NewOrderRequest{Symbol: "BTCUSDT", Side: "SELL", PositionSide: "SHORT", Type: "LIMIT", Price: "84200.1", TimeInForce: "GTC", Quantity: "0.004", ClientOrderID: "bt_test001"}
	_, err := c.SubmitDemoOrder(context.Background(), "test-key", "test-secret", req)
	if err == nil || posts.Load() != 1 {
		t.Fatalf("redirect was followed or accepted %v %d", err, posts.Load())
	}
	_, err = c.QueryDemoOrder(context.Background(), "test-key", "test-secret", "BTCUSDT", "bt_test001")
	if err != ErrOrderNotFound {
		t.Fatalf("not found must have distinct error: %v", err)
	}
	if posts.Load() != 1 {
		t.Fatal("query made a POST")
	}
	for _, broken := range []NewOrderRequest{
		{Symbol: "ETHUSDT", Side: "BUY", PositionSide: "LONG", Type: "MARKET", Quantity: "0.01", ClientOrderID: "bt_test001"},
		{Symbol: "BTCUSDT", Side: "BUY", PositionSide: "BOTH", Type: "MARKET", Quantity: "0.01", ClientOrderID: "bt_test001"},
		{Symbol: "BTCUSDT", Side: "BUY", PositionSide: "LONG", Type: "MARKET", Quantity: "NaN", ClientOrderID: "bt_test001"},
		{Symbol: "BTCUSDT", Side: "BUY", PositionSide: "LONG", Type: "MARKET", Quantity: "0.01", Price: "1", ClientOrderID: "bt_test001"},
		{Symbol: "BTCUSDT", Side: "BUY", PositionSide: "LONG", Type: "LIMIT", Quantity: "0.01", Price: "1", ClientOrderID: "bt_test001"},
		{Symbol: "BTCUSDT", Side: "BUY", PositionSide: "LONG", Type: "STOP_MARKET", Quantity: "0.01", ClientOrderID: "bt_test001"},
	} {
		if _, err := c.SubmitDemoOrder(context.Background(), "test-key", "test-secret", broken); err == nil {
			t.Errorf("accepted bad %+v", broken)
		}
	}
	if posts.Load() != 1 {
		t.Fatal("invalid submission reached mock exchange")
	}
}
func TestDemoOrderMalformedAckIsUnknown(t *testing.T) {
	cli := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/fapi/v1/time" {
			fmt.Fprintf(w, `{"serverTime":%d}`, time.Now().UnixMilli())
			return
		}
		if strings.HasSuffix(r.URL.Path, "/order") {
			fmt.Fprint(w, `{"symbol":"BTCUSDT","clientOrderId":"other","orderId":42,"status":"FILLED","side":"BUY","positionSide":"LONG","type":"MARKET"}`)
		}
	}))
	defer cli.Close()
	c := NewWithBase(cli.URL, cli.Client())
	_, err := c.SubmitDemoOrder(context.Background(), "k", "s", NewOrderRequest{Symbol: "BTCUSDT", Side: "BUY", PositionSide: "LONG", Type: "MARKET", Quantity: "0.004", ClientOrderID: "bt_test001"})
	if err == nil || !strings.Contains(err.Error(), "조회 전 재전송 금지") {
		t.Fatalf("unreliable ACK accepted %v", err)
	}
}

func TestDemoOrderSignatureUsesPercentEncodedPayload(t *testing.T) {
	const secret = "test-secret"
	const clientID = "bt:test/01"
	var posts atomic.Int32
	cli := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/fapi/v1/time":
			fmt.Fprintf(w, `{"serverTime":%d}`, time.Now().UnixMilli())
		case "/fapi/v1/order":
			if r.Method != http.MethodPost {
				t.Fatalf("unexpected method %s", r.Method)
			}
			posts.Add(1)
			raw, _ := io.ReadAll(r.Body)
			body := string(raw)
			if !strings.Contains(body, "newClientOrderId=bt%3Atest%2F01") {
				t.Fatalf("client ID was not percent encoded before signing: %s", body)
			}
			values, err := url.ParseQuery(body)
			if err != nil {
				t.Fatalf("parse body: %v", err)
			}
			sig := values.Get("signature")
			values.Del("signature")
			canonical := values.Encode()
			mac := hmac.New(sha256.New, []byte(secret))
			mac.Write([]byte(canonical))
			if sig != hex.EncodeToString(mac.Sum(nil)) {
				t.Fatalf("signature did not cover percent-encoded canonical payload: %s", canonical)
			}
			fmt.Fprintf(w, `{"symbol":"BTCUSDT","clientOrderId":"%s","orderId":1234,"status":"NEW","side":"BUY","positionSide":"LONG","type":"MARKET","origQty":"0.004","executedQty":"0"}`, clientID)
		default:
			t.Fatalf("unexpected endpoint %s", r.URL)
		}
	}))
	defer cli.Close()

	c := NewWithBase(cli.URL, cli.Client())
	_, err := c.SubmitDemoOrder(context.Background(), "test-key", secret, NewOrderRequest{
		Symbol:        "BTCUSDT",
		Side:          "BUY",
		PositionSide:  "LONG",
		Type:          "MARKET",
		Quantity:      "0.004",
		ClientOrderID: clientID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if posts.Load() != 1 {
		t.Fatalf("expected one POST, got %d", posts.Load())
	}
}
