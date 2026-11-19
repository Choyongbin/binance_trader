package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"binance_trader/internal/exchange"
	"binance_trader/internal/trading"
)

type fakeAccountingWindow func(context.Context, string, string, int64) (exchange.RecoveryBaseline, error)

func (f fakeAccountingWindow) FetchDemoRecoveryBaseline(ctx context.Context, k, s string, start int64) (exchange.RecoveryBaseline, error) {
	return f(ctx, k, s, start)
}
func minimalSignedWindow(t *testing.T, start int64) exchange.RecoveryBaseline {
	t.Helper()
	now := time.Now().UTC()
	b := exchange.RecoveryBaseline{Environment: "demo", Coverage: "BOUNDED_HISTORY_ONLY_NOT_CONTINUOUS", StartedAt: now.Add(-300 * time.Millisecond), FinishedAt: now,
		WindowStartMS: start, WindowEndMS: now.Add(-300 * time.Millisecond).UnixMilli(),
		OpenOrders: []exchange.Order{}, OpenAlgos: []exchange.AlgoOrder{}, RecentBTCOrders: []exchange.Order{}, RecentBTCAlgos: []exchange.AlgoOrder{}, RecentBTCTrades: []exchange.AccountTrade{}, RecentIncome: []exchange.IncomeRecord{}}
	if err := json.Unmarshal([]byte(`{"canTrade":true,"totalWalletBalance":"1000","totalUnrealizedProfit":"0","totalMarginBalance":"1000","totalInitialMargin":"0","totalPositionInitialMargin":"0","totalOpenOrderInitialMargin":"0","availableBalance":"1000","assets":[{"asset":"USDT","walletBalance":"1000","availableBalance":"1000","marginBalance":"1000"}],"positions":[]}`), &b.Portfolio); err != nil {
		t.Fatal(err)
	}
	return b
}
func connectedAccountingServer(t *testing.T) *Server {
	t.Helper()
	s, e := newWithFetcher(t.TempDir(), readDemoMock{})
	if e != nil {
		t.Fatal(e)
	}
	if e := s.vault.SaveDemo("demo-only-key", "demo-only-secret", "strong-passphrase"); e != nil {
		t.Fatal(e)
	}
	if e := s.demo.Connect(context.Background(), "demo-only-key", "demo-only-secret"); e != nil {
		t.Fatal(e)
	}
	return s
}
func TestSignedAccountingOperatorOnlyAndNeverEnablesOrders(t *testing.T) {
	s := connectedAccountingServer(t)
	defer s.Close()
	path := "/api/v1/trading/accounting/generate"
	pass := `{"passphrase":"strong-passphrase","lookback_hours":24}`
	readerCalls := 0
	s.accountingReader = fakeAccountingWindow(func(_ context.Context, key, secret string, from int64) (exchange.RecoveryBaseline, error) {
		readerCalls++
		if key != "demo-only-key" || secret != "demo-only-secret" {
			t.Fatal("wrong signing credential")
		}
		return minimalSignedWindow(t, from), nil
	})
	h := s.Handler()
	if r := request(h, "POST", path, "bad", "", pass); r.Code != 403 {
		t.Fatal("csrf bypass", r.Code)
	}
	if r := request(h, "POST", path, s.csrfToken, "", `{"passphrase":"wrong","lookback_hours":24}`); r.Code != 403 {
		t.Fatal("accepted bad password", r.Code)
	}
	if r := request(h, "POST", path, s.csrfToken, "", `{"passphrase":"strong-passphrase","lookback_hours":1}`); r.Code != 400 {
		t.Fatal("accepted unsupported window", r.Code)
	}
	if r := request(h, "POST", path+"?bad=true", s.csrfToken, "", pass); r.Code != 400 {
		t.Fatal("query bypass", r.Code)
	}
	if readerCalls != 0 {
		t.Fatal("signed REST called without valid user intent")
	}
	r := request(h, "POST", path, s.csrfToken, "http://127.0.0.1:8080", pass)
	if r.Code != 200 || strings.Contains(r.Body.String(), "demo-only-key") || strings.Contains(r.Body.String(), "strong-passphrase") || !strings.Contains(r.Body.String(), `"trading_authorized":false`) {
		t.Fatal("archival result incorrect or secret disclosed", r.Code, r.Body)
	}
	records, e := trading.LoadAccountingReports(s.dataDir)
	if e != nil || len(records) != 1 || len(records[0].EvidenceSHA256) != 64 || records[0].ActualNetVerified {
		t.Fatalf("archived proof %+v %v", records, e)
	}
	if r := request(h, "POST", "/api/v1/trading/manual/orders", s.csrfToken, "", `{}`); r.Code != http.StatusLocked {
		t.Fatal("manual orders unlocked")
	}
	if r := request(h, "POST", "/api/v1/trading/auto/start", s.csrfToken, "", `{}`); r.Code != http.StatusLocked {
		t.Fatal("auto orders unlocked")
	}
}
func TestSignedAccountingSessionRaceSanitizedErrorAndConcurrency(t *testing.T) {
	s := connectedAccountingServer(t)
	defer s.Close()
	h := s.Handler()
	path := "/api/v1/trading/accounting/generate"
	pass := `{"passphrase":"strong-passphrase","lookback_hours":72}`
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	s.accountingReader = fakeAccountingWindow(func(_ context.Context, _, _ string, from int64) (exchange.RecoveryBaseline, error) {
		once.Do(func() { close(started) })
		<-release
		return minimalSignedWindow(t, from), nil
	})
	result := make(chan int, 1)
	go func() { result <- request(h, "POST", path, s.csrfToken, "", pass).Code }()
	<-started
	if r := request(h, "POST", path, s.csrfToken, "", pass); r.Code != 409 {
		t.Fatal("concurrent report allowed", r.Code)
	}
	s.demo.Clear()
	close(release)
	if code := <-result; code != 409 {
		t.Fatal("archived after Demo session switch", code)
	}
	if reports, e := trading.LoadAccountingReports(s.dataDir); e != nil || len(reports) != 0 {
		t.Fatal("race report persisted", reports, e)
	}
	if e := s.demo.Connect(context.Background(), "demo-only-key", "demo-only-secret"); e != nil {
		t.Fatal(e)
	}
	s.accountingReader = fakeAccountingWindow(func(context.Context, string, string, int64) (exchange.RecoveryBaseline, error) {
		return exchange.RecoveryBaseline{}, errors.New("secret=demo-only-secret")
	})
	r := request(h, "POST", path, s.csrfToken, "", pass)
	if r.Code != 409 || strings.Contains(r.Body.String(), "demo-only-secret") {
		t.Fatal("signed reader error leaked", r.Code, r.Body)
	}
}
