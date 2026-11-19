package trading

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

func TestDemoSignedCloseReaderShortLongAndFailClosed(t *testing.T) {
	for _, side := range []string{"SHORT", "LONG"} {
		t.Run(side, func(t *testing.T) {
			enter, leave := "SELL", "BUY"
			if side == "LONG" {
				enter, leave = "BUY", "SELL"
			}
			now := time.Now().UTC()
			ts1, ts2 := now.Add(-time.Hour).UnixMilli(), now.Add(-time.Hour+time.Minute).UnixMilli()
			first := fmt.Sprintf(`{"symbol":"BTCUSDT","id":11,"orderId":101,"side":"%s","positionSide":"%s","price":"80000","qty":"0.001","commission":"0.01","commissionAsset":"USDT","realizedPnl":"0","time":%d}`, enter, side, ts1)
			second := fmt.Sprintf(`{"symbol":"BTCUSDT","id":12,"orderId":102,"side":"%s","positionSide":"%s","price":"80100","qty":"0.001","commission":"0.01","commissionAsset":"USDT","realizedPnl":"0.1","time":%d}`, leave, side, ts2)
			var mu sync.Mutex
			mode := "normal"
			seen := map[string]int{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				if r.Method != "GET" {
					t.Error("mutation request", r.Method)
				}
				seen[r.URL.Path]++
				if r.URL.Path != "/fapi/v1/time" && (r.Header.Get("X-MBX-APIKEY") != "key" || r.URL.Query().Get("signature") == "") {
					t.Errorf("unsigned request: %s", r.URL.Path)
				}
				switch r.URL.Path {
				case "/fapi/v1/time":
					fmt.Fprintf(w, `{"serverTime":%d}`, time.Now().UnixMilli())
				case "/fapi/v1/order":
					id := r.URL.Query().Get("orderId")
					direction := enter
					if id == "102" {
						direction = leave
					}
					if id != "101" && id != "102" {
						t.Error("unknown order")
					}
					if mode == "changed_order" && id == "102" && seen[r.URL.Path] >= 4 {
						direction = enter
					}
					fmt.Fprintf(w, `{"symbol":"BTCUSDT","orderId":%s,"clientOrderId":"web","side":"%s","positionSide":"%s","status":"FILLED","origQty":"0.001","executedQty":"0.001","type":"MARKET"}`, id, direction, side)
				case "/fapi/v1/userTrades":
					if r.URL.Query().Get("orderId") == "101" {
						item := first
						if mode == "interleaved_fills" {
							item = strings.Replace(item, fmt.Sprintf(`"time":%d`, ts1), fmt.Sprintf(`"time":%d`, ts2+1), 1)
						}
						if mode == "zero_price" {
							item = strings.Replace(item, `"price":"80000"`, `"price":"0"`, 1)
						}
						fmt.Fprintf(w, "[%s]", item)
					} else if r.URL.Query().Get("orderId") == "102" {
						fmt.Fprintf(w, "[%s]", second)
					} else {
						if r.URL.Query().Get("startTime") == "" || r.URL.Query().Get("endTime") == "" {
							t.Error("unbounded history")
						}
						if mode == "extra_trade" {
							fmt.Fprintf(w, "[%s,%s,%s]", first, second, strings.ReplaceAll(second, `"id":12`, `"id":13`))
						} else if mode == "history_missing" {
							fmt.Fprint(w, "[]")
						} else {
							fmt.Fprintf(w, "[%s,%s]", first, second)
						}
					}
				case "/fapi/v1/income":
					if r.URL.Query().Get("incomeType") != "FUNDING_FEE" || r.URL.Query().Get("startTime") == "" {
						t.Error("invalid funding request")
					}
					if mode == "funding_ambiguous" {
						fmt.Fprint(w, "null")
					} else {
						fmt.Fprint(w, "[]")
					}
				case "/fapi/v3/balance":
					fmt.Fprint(w, `[{"asset":"USDT","balance":"5000","availableBalance":"5000"}]`)
				case "/fapi/v3/positionRisk":
					if mode == "nonflat" {
						fmt.Fprint(w, `[{"symbol":"BTCUSDT","positionSide":"SHORT","positionAmt":"-0.001"}]`)
					} else {
						fmt.Fprint(w, "[]")
					}
				case "/fapi/v1/openOrders", "/fapi/v1/openAlgoOrders":
					fmt.Fprint(w, "[]")
				default:
					t.Errorf("unexpected path %s", r.URL.Path)
					http.Error(w, "invalid", 404)
				}
			}))
			defer server.Close()
			reader := DemoSignedCloseReader{Client: exchange.NewWithBase(server.URL, server.Client()), Key: "key", Secret: "secret", Now: time.Now}
			got, err := reader.RefreshExternalCloseEvidence(context.Background(), 101, 102)
			if err != nil {
				t.Fatal(err)
			}
			if !got.AccountFlat || got.HistoricalCoverageComplete || got.FundingVerified || got.HistoryScope != "ENTRY_FIRST_FILL_TO_EXIT_LAST_FILL_ONLY" || got.HistoryFromMS != ts1 || got.HistoryToMS != ts2 || got.BoundedTradeRows != 2 || got.TradeWindowsRead != 1 || got.FundingWindowsRead != 1 || len(got.EntryTradeIDs) != 1 || got.EntryTradeIDs[0] != 11 || got.ExitTradeIDs[0] != 12 {
				t.Fatalf("unsafe or invalid evidence %+v", got)
			}
			for _, fail := range []string{"extra_trade", "history_missing", "funding_ambiguous", "nonflat", "changed_order", "interleaved_fills", "zero_price"} {
				mu.Lock()
				mode = fail
				seen = map[string]int{}
				mu.Unlock()
				if _, err := reader.RefreshExternalCloseEvidence(context.Background(), 101, 102); err == nil {
					t.Errorf("%s not blocked", fail)
				}
			}
			mu.Lock()
			mode = "normal"
			mu.Unlock()
			if _, err := reader.RefreshExternalCloseEvidence(context.Background(), 102, 101); err == nil {
				t.Fatal("reverse entry/exit accepted")
			}
			if _, err := exchange.NewWithBase(exchange.RealBase, server.Client()).FetchDemoFundingWindow(context.Background(), "key", "secret", "BTCUSDT", ts1, ts2); err == nil {
				t.Fatal("real signed funding allowed")
			}
		})
	}
}
