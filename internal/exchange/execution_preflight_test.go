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

func TestFetchDemoExecutionPreflightStableAndChanging(t *testing.T) {
	var accountReads atomic.Int32
	var capabilityReads atomic.Int32
	var change atomic.Bool
	var capabilityChange atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/fapi/v1/time":
			fmt.Fprintf(w, `{"serverTime":%d}`, time.Now().UnixMilli())
		case "/fapi/v3/account":
			n := accountReads.Add(1)
			pos := `[]`
			if change.Load() && n%2 == 0 {
				pos = `[{"symbol":"BTCUSDT","positionSide":"LONG","positionAmt":"0.01","initialMargin":"0","isolated":true}]`
			}
			// V3 intentionally omits canTrade/multiAssetsMargin: Binance moved
			// account configuration fields to /fapi/v1/accountConfig.
			fmt.Fprintf(w, `{"totalWalletBalance":"1000","totalUnrealizedProfit":"0","totalMarginBalance":"1000","totalInitialMargin":"0","totalPositionInitialMargin":"0","totalOpenOrderInitialMargin":"0","availableBalance":"1000","assets":[{"asset":"USDT","walletBalance":"1000","availableBalance":"1000","marginBalance":"1000"}],"positions":%s}`, pos)
		case "/fapi/v1/accountConfig":
			n := capabilityReads.Add(1)
			canTrade := true
			if capabilityChange.Load() && n%2 == 0 {
				canTrade = false
			}
			fmt.Fprintf(w, `{"canTrade":%t,"multiAssetsMargin":false,"dualSidePosition":true}`, canTrade)
		case "/fapi/v1/openOrders", "/fapi/v1/openAlgoOrders":
			w.Write([]byte(`[]`))
		case "/fapi/v1/positionSide/dual":
			w.Write([]byte(`{"dualSidePosition":true}`))
		case "/fapi/v1/symbolConfig":
			w.Write([]byte(`[{"symbol":"BTCUSDT","marginType":"ISOLATED","leverage":15,"isAutoAddMargin":false}]`))
		default:
			t.Fatalf("unexpected %s", r.URL.Path)
		}
	}))
	defer srv.Close()
	c := NewWithBase(srv.URL, srv.Client())
	got, err := c.FetchDemoExecutionPreflight(context.Background(), "k", "s", "BTCUSDT")
	if err != nil || !got.Portfolio.CanTrade || !got.Configuration.SettingsMatchV1 || !got.FinishedAt.After(got.StartedAt.Add(-time.Nanosecond)) {
		t.Fatalf("stable preflight %+v err=%v", got, err)
	}
	accountReads.Store(0)
	capabilityReads.Store(0)
	change.Store(true)
	if _, err := c.FetchDemoExecutionPreflight(context.Background(), "k", "s", "BTCUSDT"); err == nil {
		t.Fatal("changing account accepted")
	}
	change.Store(false)
	accountReads.Store(0)
	capabilityReads.Store(0)
	capabilityChange.Store(true)
	if _, err := c.FetchDemoExecutionPreflight(context.Background(), "k", "s", "BTCUSDT"); err == nil {
		t.Fatal("changing account capability accepted")
	}
}

func TestFetchDemoExecutionPreflightRejectsMissingMovedCapabilityFields(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/fapi/v1/time":
			fmt.Fprintf(w, `{"serverTime":%d}`, time.Now().UnixMilli())
		case "/fapi/v3/account":
			w.Write([]byte(`{"totalWalletBalance":"1000","totalUnrealizedProfit":"0","totalMarginBalance":"1000","totalInitialMargin":"0","totalPositionInitialMargin":"0","totalOpenOrderInitialMargin":"0","availableBalance":"1000","assets":[{"asset":"USDT","walletBalance":"1000","availableBalance":"1000","marginBalance":"1000"}],"positions":[]}`))
		case "/fapi/v1/accountConfig":
			w.Write([]byte(`{"multiAssetsMargin":false,"dualSidePosition":true}`)) // canTrade missing
		default:
			w.Write([]byte(`[]`))
		}
	}))
	defer srv.Close()
	c := NewWithBase(srv.URL, srv.Client())
	if _, err := c.FetchDemoExecutionPreflight(context.Background(), "k", "s", "BTCUSDT"); err == nil {
		t.Fatal("missing accountConfig canTrade accepted")
	}
}
