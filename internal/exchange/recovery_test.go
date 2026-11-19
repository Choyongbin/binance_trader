package exchange

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

var recoveryTestAccount = `{"totalWalletBalance":"1000.0","totalUnrealizedProfit":"0","totalMarginBalance":"1000.0","totalInitialMargin":"0","totalPositionInitialMargin":"0","totalOpenOrderInitialMargin":"0","availableBalance":"1000","assets":[{"asset":"USDT","walletBalance":"1000.0","availableBalance":"1000","marginBalance":"1000"}],"positions":[]}`

type recoveryFixture struct {
	t                *testing.T
	mu               sync.Mutex
	calls            map[string]int
	now              time.Time
	accountFirst     string
	accountLast      string
	payloads         map[string]string
	captured         map[string]url.Values
	openSecond       string
	invalidSignature bool
	server           *httptest.Server
	client           *Client
}

func newRecoveryFixture(t *testing.T) *recoveryFixture {
	t.Helper()
	f := &recoveryFixture{t: t, calls: map[string]int{}, now: time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC), accountFirst: recoveryTestAccount, accountLast: recoveryTestAccount,
		captured: map[string]url.Values{}, payloads: map[string]string{"/fapi/v1/accountConfig": `{"canTrade":true,"multiAssetsMargin":false,"dualSidePosition":true}`, "/fapi/v1/openOrders": "[]", "/fapi/v1/openAlgoOrders": "[]", "/fapi/v1/allOrders": "[]", "/fapi/v1/allAlgoOrders": "[]", "/fapi/v1/userTrades": "[]", "/fapi/v1/income": "[]"}}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.calls[r.URL.Path]++
		n := f.calls[r.URL.Path]
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/fapi/v1/time" {
			fmt.Fprintf(w, `{"serverTime":%d}`, f.now.UnixMilli())
			return
		}
		if r.Method != "GET" {
			f.t.Errorf("write call in recovery: %s", r.Method)
			http.Error(w, "wrong", 400)
			return
		}
		v := r.URL.Query()
		f.mu.Lock()
		f.captured[r.URL.Path] = v
		f.mu.Unlock()
		sig := v.Get("signature")
		v.Del("signature")
		mac := hmac.New(sha256.New, []byte("secret"))
		mac.Write([]byte(v.Encode()))
		if v.Get("timestamp") == "" || !hmac.Equal([]byte(sig), []byte(hex.EncodeToString(mac.Sum(nil)))) || r.Header.Get("X-MBX-APIKEY") != "key" {
			f.invalidSignature = true
			http.Error(w, "bad signature", 401)
			return
		}
		if r.URL.Path == "/fapi/v3/account" {
			if n == 1 {
				fmt.Fprint(w, f.accountFirst)
			} else {
				fmt.Fprint(w, f.accountLast)
			}
			return
		}
		if r.URL.Path == "/fapi/v1/openOrders" && n == 2 && f.openSecond != "" {
			fmt.Fprint(w, f.openSecond)
			return
		}
		if r.URL.Path == "/fapi/v1/income" && v.Get("page") == "2" {
			if raw, ok := f.payloads["income-page2"]; ok {
				fmt.Fprint(w, raw)
			} else {
				fmt.Fprint(w, "[]")
			}
			return
		}
		raw, ok := f.payloads[r.URL.Path]
		if !ok {
			http.Error(w, "unexpected endpoint", 404)
			return
		}
		fmt.Fprint(w, raw)
	}))
	f.client = NewWithBase(f.server.URL, f.server.Client())
	f.client.now = func() time.Time { return f.now }
	t.Cleanup(f.server.Close)
	return f
}
func (f *recoveryFixture) run() (RecoveryBaseline, error) {
	return f.client.FetchDemoRecoveryBaseline(context.Background(), "key", "secret", f.now.Add(-time.Hour).UnixMilli())
}
func TestRecoveryFullWindowSignedRead(t *testing.T) {
	f := newRecoveryFixture(t)
	b, err := f.run()
	if err != nil {
		t.Fatal(err)
	}
	if b.Coverage != "BOUNDED_HISTORY_ONLY_NOT_CONTINUOUS" || b.Environment != "demo" || len(b.OpenOrders) != 0 || f.invalidSignature {
		t.Fatalf("bad baseline %+v", b)
	}
	for _, path := range []string{"/fapi/v3/account", "/fapi/v1/accountConfig", "/fapi/v1/openOrders", "/fapi/v1/openAlgoOrders"} {
		if f.calls[path] != 2 {
			t.Errorf("missing bracket for %s: %d", path, f.calls[path])
		}
	}
	for _, path := range []string{"/fapi/v1/allOrders", "/fapi/v1/allAlgoOrders", "/fapi/v1/userTrades", "/fapi/v1/income"} {
		if f.calls[path] != 1 {
			t.Errorf("missing history %s: %d", path, f.calls[path])
		}
	}
}
func TestRecoveryRejectsMovedAccountAndMalformedLists(t *testing.T) {
	t.Run("wallet changed", func(t *testing.T) {
		f := newRecoveryFixture(t)
		f.accountLast = strings.ReplaceAll(recoveryTestAccount, "1000.0", "999.0")
		if _, err := f.run(); err == nil || !strings.Contains(err.Error(), "변경") {
			t.Fatalf("expected reject %v", err)
		}
	})
	t.Run("null open list", func(t *testing.T) {
		f := newRecoveryFixture(t)
		f.payloads["/fapi/v1/openAlgoOrders"] = "null"
		if _, err := f.run(); err == nil {
			t.Fatal("accepted missing list")
		}
	})
	t.Run("one corrupt position", func(t *testing.T) {
		f := newRecoveryFixture(t)
		f.accountFirst = strings.Replace(recoveryTestAccount, `"positions":[]`, `"positions":[{"symbol":"BTCUSDT","positionSide":"LONG","positionAmt":"oops","initialMargin":"0"}]`, 1)
		if _, err := f.run(); err == nil {
			t.Fatal("accepted corrupt position")
		}
	})
	t.Run("invalid window", func(t *testing.T) {
		f := newRecoveryFixture(t)
		if _, err := f.client.FetchDemoRecoveryBaseline(context.Background(), "key", "secret", f.now.Add(-7*24*time.Hour).UnixMilli()); err == nil {
			t.Fatal("accepted retention gap")
		}
	})
}
func TestRecoveryRejectsTruncatedHistoryAndDuplicateIncome(t *testing.T) {
	t.Run("saturated trades", func(t *testing.T) {
		f := newRecoveryFixture(t)
		items := make([]AccountTrade, 1000)
		for i := range items {
			items[i] = AccountTrade{Symbol: "BTCUSDT", TradeID: int64(i + 1), TimeMS: f.now.Add(-time.Minute).UnixMilli()}
		}
		b, _ := json.Marshal(items)
		f.payloads["/fapi/v1/userTrades"] = string(b)
		if _, err := f.run(); err == nil || !strings.Contains(err.Error(), "1000") {
			t.Fatalf("unexpected %v", err)
		}
	})
	t.Run("pagination", func(t *testing.T) {
		f := newRecoveryFixture(t)
		items := make([]IncomeRecord, 1000)
		for i := range items {
			items[i] = IncomeRecord{Type: "FUNDING_FEE", Asset: "USDT", Income: "-0.02", TransactionID: int64(i + 1), TimeMS: f.now.Add(-time.Minute).UnixMilli()}
		}
		b, _ := json.Marshal(items)
		f.payloads["/fapi/v1/income"] = string(b)
		f.payloads["income-page2"] = `[{"incomeType":"FUNDING_FEE","asset":"USDT","income":"-0.01","tranId":1001,"time":` + fmt.Sprint(f.now.Add(-time.Minute).UnixMilli()) + `}]`
		baseline, err := f.run()
		if err != nil || len(baseline.RecentIncome) != 1001 || f.calls["/fapi/v1/income"] != 2 {
			t.Fatalf("paging %v %d", err, len(baseline.RecentIncome))
		}
	})
	t.Run("duplicate income", func(t *testing.T) {
		f := newRecoveryFixture(t)
		x := IncomeRecord{Type: "FUNDING_FEE", Asset: "USDT", Income: "-0.02", TransactionID: 17, TimeMS: f.now.Add(-time.Minute).UnixMilli()}
		b, _ := json.Marshal([]IncomeRecord{x, x})
		f.payloads["/fapi/v1/income"] = string(b)
		if _, err := f.run(); err == nil {
			t.Fatal("accepted duplicated history")
		}
	})
}
func TestRecoveryRedirectNeverForwardsCredentials(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("redirected with signed credentials") }))
	defer remote.Close()
	f := newRecoveryFixture(t)
	// Dedicated dummy server: a redirect should never be followed.
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/fapi/v1/time" {
			fmt.Fprint(w, `{"serverTime":1790553600000}`)
			return
		}
		http.Redirect(w, r, remote.URL, http.StatusFound)
	}))
	defer redirect.Close()
	f.client = NewWithBase(redirect.URL, redirect.Client())
	f.client.now = func() time.Time { return f.now }
	_, err := f.run()
	if err == nil {
		t.Fatal("accepted redirect")
	}
}
func TestRecoverySignedURLContainsWindow(t *testing.T) {
	f := newRecoveryFixture(t)
	_, err := f.run()
	if err != nil {
		t.Fatal(err)
	}
	from := fmt.Sprint(f.now.Add(-time.Hour).UnixMilli())
	until := fmt.Sprint(f.now.UnixMilli())
	for _, path := range []string{"/fapi/v1/allOrders", "/fapi/v1/allAlgoOrders", "/fapi/v1/userTrades", "/fapi/v1/income"} {
		v := f.captured[path]
		if v.Get("startTime") != from || v.Get("endTime") != until || v.Get("limit") != "1000" {
			t.Errorf("missing window %s %v", path, v)
		}
		if path == "/fapi/v1/income" {
			if v.Get("symbol") != "" {
				t.Fatal("global income was filtered")
			}
		} else if v.Get("symbol") != "BTCUSDT" {
			t.Errorf("unscoped historical symbol %s", path)
		}
	}
}
func TestRecoveryDetectsInterleavedOpenOrderChange(t *testing.T) {
	f := newRecoveryFixture(t)
	f.payloads["/fapi/v1/openOrders"] = `[{"orderId":17,"symbol":"BTCUSDT","clientOrderId":"other","status":"NEW","origQty":"1","executedQty":"0","price":"100","type":"LIMIT","side":"BUY","positionSide":"LONG"}]`
	f.openSecond = `[{"orderId":17,"symbol":"BTCUSDT","clientOrderId":"other","status":"NEW","origQty":"1","executedQty":"0","price":"101","type":"LIMIT","side":"BUY","positionSide":"LONG"}]`
	if _, err := f.run(); err == nil || !strings.Contains(err.Error(), "변경") {
		t.Fatalf("order price change was accepted %v", err)
	}
}
func TestRecoveryRejectsInvalidTrades(t *testing.T) {
	f := newRecoveryFixture(t)
	f.payloads["/fapi/v1/userTrades"] = `[{"symbol":"BTCUSDT","orderId":1,"id":1,"side":"BUY","positionSide":"LONG","price":"NaN","qty":"1","commission":"0","realizedPnl":"0","commissionAsset":"USDT","time":` + fmt.Sprint(f.now.Add(-time.Minute).UnixMilli()) + `}]`
	if _, err := f.run(); err == nil {
		t.Fatal("malformed trade accepted")
	}
}
