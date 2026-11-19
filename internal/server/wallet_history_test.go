package server

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"binance_trader/internal/exchange"
	"binance_trader/internal/trading"
)

func walletSignedBaseline(t *testing.T) exchange.RecoveryBaseline {
	t.Helper()
	now := time.Now().UTC()
	var portfolio exchange.AccountPortfolio
	if err := json.Unmarshal([]byte(`{"canTrade":true,"totalWalletBalance":"200","totalUnrealizedProfit":"0","totalMarginBalance":"200","totalInitialMargin":"0","totalPositionInitialMargin":"0","totalOpenOrderInitialMargin":"0","availableBalance":"150","assets":[{"asset":"USDT","walletBalance":"200","availableBalance":"150","marginBalance":"200"}],"positions":[]}`), &portfolio); err != nil {
		t.Fatal(err)
	}
	return exchange.RecoveryBaseline{Environment: "demo", Coverage: "BOUNDED_HISTORY_ONLY_NOT_CONTINUOUS",
		StartedAt: now.Add(-time.Second), FinishedAt: now, WindowStartMS: now.Add(-24 * time.Hour).UnixMilli(), WindowEndMS: now.Add(-time.Second).UnixMilli(),
		Portfolio: portfolio, OpenOrders: []exchange.Order{}, OpenAlgos: []exchange.AlgoOrder{},
		RecentBTCOrders: []exchange.Order{}, RecentBTCAlgos: []exchange.AlgoOrder{}, RecentBTCTrades: []exchange.AccountTrade{}, RecentIncome: []exchange.IncomeRecord{}}
}

func TestHistoricalWalletRequiresSignedFullAccountAndPreservesLocks(t *testing.T) {
	dir := t.TempDir()
	s, err := newWithFetcher(dir, currentWalletMock{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	const pass = "long-demo-passphrase"
	if err := s.vault.SaveDemo("key-one", "demo-secret", pass); err != nil {
		t.Fatal(err)
	}
	if err := s.demo.Connect(context.Background(), "key-one", "demo-secret"); err != nil {
		t.Fatal(err)
	}
	markerDir := filepath.Join(dir, "trading", "stream_discontinuities")
	if err := os.MkdirAll(markerDir, 0700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(markerDir, strings.Repeat("a", 32)+".json")
	if err := os.WriteFile(marker, []byte(`{"schema":1,"reason":"DISCONNECT","at":"2026-09-29T00:00:00Z"}`), 0600); err != nil {
		t.Fatal(err)
	}
	calls := 0
	baseline := walletSignedBaseline(t)
	freshBaseline := func() exchange.RecoveryBaseline {
		b := baseline
		b.FinishedAt = time.Now().UTC()
		b.StartedAt = b.FinishedAt.Add(-time.Second)
		b.WindowEndMS = b.StartedAt.UnixMilli()
		return b
	}
	s.accountingReader = fakeAccountingWindow(func(_ context.Context, key, secret string, _ int64) (exchange.RecoveryBaseline, error) {
		calls++
		if key != "key-one" || secret != "demo-secret" {
			t.Fatal("unexpected signed account credentials")
		}
		return freshBaseline(), nil
	})
	csrf := bootstrapTokenForConnected(t, s.Handler())
	get := request(s.Handler(), "GET", "/api/v1/wallet/auto", "", "", "")
	if get.Code != 200 || !strings.Contains(get.Body.String(), `"requires_reconciliation":true`) || !strings.Contains(get.Body.String(), `"can_adjust":true`) {
		t.Fatalf("history UI remains permanently locked: %d %s", get.Code, get.Body)
	}
	post := func(body string) int {
		// Bound the freshness clock to this test call, not test startup. Race
		// mode can spend >15 seconds deriving several password keys.
		if err := s.demo.Refresh(context.Background()); err != nil {
			t.Fatal(err)
		}
		return request(s.Handler(), "POST", "/api/v1/wallet/auto/change", csrf, "http://127.0.0.1:8080", body).Code
	}
	if code := post(`{"request_id":"history-change-01","operation":"ALLOCATE","amount_usdt":"10"}`); code != 400 || calls != 0 {
		t.Fatalf("missing passphrase bypass: %d calls=%d", code, calls)
	}
	if code := post(`{"request_id":"history-change-01","operation":"ALLOCATE","amount_usdt":"10","passphrase":"wrong-passphrase"}`); code != 423 || calls != 0 {
		t.Fatalf("wrong passphrase bypass: %d calls=%d", code, calls)
	}
	baseline.OpenOrders = []exchange.Order{{Symbol: "ETHUSDT", OrderID: 1}}
	if code := post(`{"request_id":"history-change-01","operation":"ALLOCATE","amount_usdt":"10","passphrase":"long-demo-passphrase"}`); code != 423 {
		t.Fatalf("foreign symbol open order accepted: %d", code)
	}
	baseline.OpenOrders = []exchange.Order{}
	baseline.Portfolio.Positions = []exchange.PortfolioPosition{{Symbol: "ETHUSDT", PositionSide: "LONG", PositionAmt: "0.1", InitialMargin: "10"}}
	if code := post(`{"request_id":"history-change-01","operation":"ALLOCATE","amount_usdt":"10","passphrase":"long-demo-passphrase"}`); code != 423 {
		t.Fatalf("foreign symbol position accepted: %d", code)
	}
	baseline.Portfolio.Positions = []exchange.PortfolioPosition{}
	// A valid-looking marker changing during the signed REST round trip must
	// invalidate the account observation before any wallet entry is written.
	s.accountingReader = fakeAccountingWindow(func(_ context.Context, _, _ string, _ int64) (exchange.RecoveryBaseline, error) {
		if err := os.WriteFile(marker, []byte(`{"schema":1,"reason":"ROTATE","at":"2026-09-29T00:00:01Z"}`), 0600); err != nil {
			t.Fatal(err)
		}
		return freshBaseline(), nil
	})
	if code := post(`{"request_id":"history-change-01","operation":"ALLOCATE","amount_usdt":"10","passphrase":"long-demo-passphrase"}`); code != 409 {
		t.Fatalf("changed local evidence accepted: %d", code)
	}
	s.accountingReader = fakeAccountingWindow(func(_ context.Context, _, _ string, _ int64) (exchange.RecoveryBaseline, error) {
		return freshBaseline(), nil
	})
	if code := post(`{"request_id":"history-change-01","operation":"ALLOCATE","amount_usdt":"10","passphrase":"long-demo-passphrase"}`); code != 200 {
		t.Fatalf("flat signed account with old marker rejected: %d", code)
	}
	status, err := s.wallet.SnapshotFor(s.demo.KeyFingerprint())
	if err != nil || status.AssignedUSDT != "10" {
		t.Fatalf("allocation not committed: %+v %v", status, err)
	}
	for _, route := range []string{"/api/v1/trading/manual/orders", "/api/v1/trading/auto/start"} {
		if code := postToWalletTestRoute(s, csrf, route); code != 423 {
			t.Fatalf("wallet reconciliation opened trade route %s: %d", route, code)
		}
	}
	if err := os.WriteFile(marker, []byte("tampered"), 0600); err != nil {
		t.Fatal(err)
	}
	if code := post(`{"request_id":"history-change-02","operation":"ALLOCATE","amount_usdt":"1","passphrase":"long-demo-passphrase"}`); code != 423 {
		t.Fatalf("tampered marker accepted: %d", code)
	}
}

func postToWalletTestRoute(s *Server, csrf, route string) int {
	return request(s.Handler(), "POST", route, csrf, "http://127.0.0.1:8080", `{}`).Code
}

func TestFirstWalletAllocationAlwaysAuditsEverySymbol(t *testing.T) {
	dir := t.TempDir()
	s, err := newWithFetcher(dir, currentWalletMock{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	const pass = "first-allocation-passphrase"
	if err := s.vault.SaveDemo("key-one", "demo-secret", pass); err != nil {
		t.Fatal(err)
	}
	if err := s.demo.Connect(context.Background(), "key-one", "demo-secret"); err != nil {
		t.Fatal(err)
	}
	baseline := walletSignedBaseline(t)
	calls := 0
	s.accountingReader = fakeAccountingWindow(func(_ context.Context, _, _ string, _ int64) (exchange.RecoveryBaseline, error) {
		calls++
		b := baseline
		b.FinishedAt = time.Now().UTC()
		b.StartedAt = b.FinishedAt.Add(-time.Second)
		b.WindowEndMS = b.StartedAt.UnixMilli()
		return b, nil
	})
	csrf := bootstrapTokenForConnected(t, s.Handler())
	change := func(passphrase string) int {
		_ = s.demo.Refresh(context.Background())
		body, _ := json.Marshal(map[string]string{"request_id": "first-signed-audit", "operation": "ALLOCATE", "amount_usdt": "10", "passphrase": passphrase})
		return request(s.Handler(), "POST", "/api/v1/wallet/auto/change", csrf, "http://127.0.0.1:8080", string(body)).Code
	}
	if code := change(""); code != 400 || calls != 0 {
		t.Fatalf("first allocation bypassed password: %d calls=%d", code, calls)
	}
	baseline.OpenOrders = []exchange.Order{{Symbol: "ETHUSDT", OrderID: 789}}
	if code := change(pass); code != 423 || calls != 1 {
		t.Fatalf("other-symbol order ignored: %d calls=%d", code, calls)
	}
	baseline.OpenOrders = []exchange.Order{}
	baseline.Portfolio.Positions = []exchange.PortfolioPosition{{Symbol: "ETHUSDT", PositionSide: "LONG", PositionAmt: "1", InitialMargin: "10"}}
	if code := change(pass); code != 423 || calls != 2 {
		t.Fatalf("other-symbol position ignored: %d calls=%d", code, calls)
	}
	baseline.Portfolio.Positions = []exchange.PortfolioPosition{}
	if code := change(pass); code != 200 || calls != 3 {
		t.Fatalf("first signed full-account allocation failed: %d calls=%d", code, calls)
	}
	state, err := s.wallet.SnapshotFor(s.demo.KeyFingerprint())
	if err != nil || state.AssignedUSDT != "10" {
		t.Fatalf("wrong saved allocation: %+v %v", state, err)
	}
	if code := request(s.Handler(), "POST", "/api/v1/trading/auto/start", csrf, "", `{}`).Code; code != 423 {
		t.Fatalf("allocation unlocked trading: %d", code)
	}
}

func TestWalletRequestCannotPassCoordinatorStartupGate(t *testing.T) {
	dir := t.TempDir()
	s, err := newWithFetcher(dir, currentWalletMock{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.vault.SaveDemo("key-one", "demo-secret", "gate-test-passphrase"); err != nil {
		t.Fatal(err)
	}
	if err := s.demo.Connect(context.Background(), "key-one", "demo-secret"); err != nil {
		t.Fatal(err)
	}
	s.accountingReader = fakeAccountingWindow(func(_ context.Context, _, _ string, _ int64) (exchange.RecoveryBaseline, error) {
		return walletSignedBaseline(t), nil
	})
	gate, err := trading.AcquireMutationGate(dir)
	if err != nil {
		t.Fatal(err)
	}
	csrf := bootstrapTokenForConnected(t, s.Handler())
	body := `{"request_id":"gate-test-request","operation":"ALLOCATE","amount_usdt":"1","passphrase":"gate-test-passphrase"}`
	if code := request(s.Handler(), "POST", "/api/v1/wallet/auto/change", csrf, "", body).Code; code != 423 {
		t.Fatalf("race gate bypass: %d", code)
	}
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	if code := request(s.Handler(), "POST", "/api/v1/wallet/auto/change", csrf, "", body).Code; code != 200 {
		t.Fatalf("gate not released: %d", code)
	}
}

func TestWalletChangeBlocksUnfinishedRecoveryAndReconnectLocksBeforeSignedAudit(t *testing.T) {
	for _, rel := range []string{
		filepath.Join("trading", "recovery_sessions", "exclusive.active"),
		filepath.Join("trading", "reconnect_watch_sessions", "exclusive.active"),
	} {
		t.Run(filepath.Base(filepath.Dir(rel)), func(t *testing.T) {
			dir := t.TempDir()
			s, err := newWithFetcher(dir, currentWalletMock{})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			const pass = "wallet-lock-passphrase"
			if err := s.vault.SaveDemo("key-one", "demo-secret", pass); err != nil {
				t.Fatal(err)
			}
			if err := s.demo.Connect(context.Background(), "key-one", "demo-secret"); err != nil {
				t.Fatal(err)
			}
			calls := 0
			s.accountingReader = fakeAccountingWindow(func(_ context.Context, _, _ string, _ int64) (exchange.RecoveryBaseline, error) {
				calls++
				return walletSignedBaseline(t), nil
			})
			if err := os.MkdirAll(filepath.Join(dir, rel), 0700); err != nil {
				t.Fatal(err)
			}
			csrf := bootstrapTokenForConnected(t, s.Handler())
			_ = s.demo.Refresh(context.Background())
			body := `{"request_id":"unfinished-diagnostic-lock","operation":"ALLOCATE","amount_usdt":"1","passphrase":"wallet-lock-passphrase"}`
			if code := request(s.Handler(), "POST", "/api/v1/wallet/auto/change", csrf, "http://127.0.0.1:8080", body).Code; code != 423 {
				t.Fatalf("unfinished diagnostic lock accepted: %s code=%d", rel, code)
			}
			if calls != 0 {
				t.Fatalf("signed account audit ran despite local unfinished lifecycle: %s calls=%d", rel, calls)
			}
		})
	}
}
