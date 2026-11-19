package exchange

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func verifySignature(t *testing.T, r *http.Request, params url.Values) {
	t.Helper()
	if r.Header.Get("X-MBX-APIKEY") != "test-key" {
		t.Error("API key missing")
	}
	sig := params.Get("signature")
	params.Del("signature")
	if params.Get("recvWindow") != "5000" || params.Get("timestamp") == "" {
		t.Error("timing parameters missing")
	}
	mac := hmac.New(sha256.New, []byte("test-secret"))
	_, _ = mac.Write([]byte(params.Encode()))
	if sig != hex.EncodeToString(mac.Sum(nil)) {
		t.Errorf("bad signature on %s %s", r.Method, r.URL.Path)
	}
}
func TestConfigReadOnlySignedAndFailClosed(t *testing.T) {
	calls := map[string]int{}
	mode := `{"dualSidePosition":true}`
	symbols := `[{"symbol":"BTCUSDT","marginType":"ISOLATED","leverage":15,"isAutoAddMargin":false}]`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls[r.URL.Path]++
		if r.Method != "GET" {
			t.Error("unexpected write from inspector")
			w.WriteHeader(500)
			return
		}
		if r.URL.Path != "/fapi/v1/time" {
			verifySignature(t, r, r.URL.Query())
		}
		switch r.URL.Path {
		case "/fapi/v1/time":
			fmt.Fprintf(w, `{"serverTime":%d}`, time.Now().UnixMilli())
		case "/fapi/v1/positionSide/dual":
			fmt.Fprint(w, mode)
		case "/fapi/v1/symbolConfig":
			if r.URL.Query().Get("symbol") != "BTCUSDT" {
				t.Error("wrong symbol")
			}
			fmt.Fprint(w, symbols)
		default:
			t.Error("unexpected path", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer srv.Close()
	c := NewWithBase(srv.URL, srv.Client())
	conf, err := c.InspectTradingConfiguration(context.Background(), "test-key", "test-secret", "BTCUSDT")
	if err != nil || !conf.SettingsMatchV1 || !conf.HedgeMode || conf.Leverage != 15 || len(conf.Warnings) != 0 {
		t.Fatalf("config: %+v, err: %v", conf, err)
	}
	if calls["/fapi/v1/positionSide/dual"] != 1 || calls["/fapi/v1/symbolConfig"] != 1 {
		t.Fatal("bad read count")
	}
	// Missing mode should NEVER be interpreted as one-way or hedge.
	mode = `{}`
	if got, err := c.InspectTradingConfiguration(context.Background(), "test-key", "test-secret", "BTCUSDT"); err == nil || got.Symbol != "" {
		t.Fatalf("missing mode accepted: %+v %v", got, err)
	}
	mode = `{"dualSidePosition":false}`
	conf, err = c.InspectTradingConfiguration(context.Background(), "test-key", "test-secret", "BTCUSDT")
	if err != nil || conf.SettingsMatchV1 || conf.HedgeMode || len(conf.Warnings) == 0 {
		t.Fatalf("one-way incorrectly accepted: %+v %v", conf, err)
	}
	symbols = `[]`
	if _, err = c.InspectTradingConfiguration(context.Background(), "test-key", "test-secret", "BTCUSDT"); err == nil {
		t.Fatal("missing symbol config accepted")
	}
	if _, err = c.InspectTradingConfiguration(context.Background(), "test-key", "test-secret", "ETHUSDT"); err == nil {
		t.Fatal("unsupported symbol accepted")
	}
}
func TestSettingsPostSignedMockOnly(t *testing.T) {
	count := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/fapi/v1/time" {
			fmt.Fprintf(w, `{"serverTime":%d}`, time.Now().UnixMilli())
			return
		}
		if r.Method != "POST" || r.Header.Get("Content-Type") != "application/x-www-form-urlencoded" {
			t.Error("settings must use signed form POST")
			return
		}
		if r.URL.RawQuery != "" {
			t.Error("settings POST should use body, not URL")
		}
		if err := r.ParseForm(); err != nil {
			t.Error(err)
			return
		}
		verifySignature(t, r, r.PostForm)
		count++
		switch r.URL.Path {
		case "/fapi/v1/positionSide/dual":
			if r.PostForm.Get("dualSidePosition") != "true" {
				t.Error("wrong hedge config")
			}
			fmt.Fprint(w, `{"code":200,"msg":"success"}`)
		case "/fapi/v1/marginType":
			if r.PostForm.Get("symbol") != "BTCUSDT" || r.PostForm.Get("marginType") != "ISOLATED" {
				t.Error("wrong margin config")
			}
			fmt.Fprint(w, `{"code":200,"msg":"success"}`)
		case "/fapi/v1/leverage":
			if r.PostForm.Get("symbol") != "BTCUSDT" || r.PostForm.Get("leverage") != "15" {
				t.Error("wrong leverage")
			}
			fmt.Fprint(w, `{"symbol":"BTCUSDT","leverage":15,"maxNotionalValue":"500000"}`)
		default:
			t.Error("unexpected path", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer srv.Close()
	c := NewWithBase(srv.URL, srv.Client())
	if err := c.SetHedgeModeForDemo(context.Background(), "test-key", "test-secret", true); err != nil {
		t.Fatal(err)
	}
	if err := c.SetIsolatedForDemo(context.Background(), "test-key", "test-secret"); err != nil {
		t.Fatal(err)
	}
	if err := c.SetLeverageForDemo(context.Background(), "test-key", "test-secret", 15); err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatal("unexpected writes", count)
	}
	if err := c.SetLeverageForDemo(context.Background(), "test-key", "test-secret", 999); err == nil {
		t.Fatal("invalid leverage accepted")
	}
	// The production base URL is categorically blocked, even if a caller
	// accidentally constructs a Client via NewWithBase rather than NewDemoClient.
	real := NewWithBase(RealBase, &http.Client{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := real.SetIsolatedForDemo(ctx, "test-key", "test-secret"); err == nil || !strings.Contains(err.Error(), "비활성화") {
		t.Fatal("real config write not blocked", err)
	}
	unsafe := NewWithBase("https://other.example.invalid", &http.Client{})
	if err := unsafe.SetLeverageForDemo(ctx, "test-key", "test-secret", 15); err == nil {
		t.Fatal("arbitrary internet host permitted")
	}
}
