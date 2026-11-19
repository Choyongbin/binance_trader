package trading

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"binance_trader/internal/exchange"
	"binance_trader/internal/wallet"
)

func emptyIntegration(t *testing.T) (string, *wallet.Store, string, exchange.RecoveryBaseline, time.Time) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Millisecond)
	dir := t.TempDir()
	eventStore, err := OpenEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "trading", "stream_discontinuities"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := eventStore.Close(); err != nil {
		t.Fatal(err)
	}
	budget, err := wallet.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	keysum := sha256.Sum256([]byte("demo_mock_key"))
	fp := hex.EncodeToString(keysum[:])
	if _, err := budget.Change("testalloc00001", "ALLOCATE", "1000", "5000", fp); err != nil {
		t.Fatal(err)
	}
	start := now.Add(-2 * time.Second)
	b := exchange.RecoveryBaseline{Environment: "demo", Coverage: "BOUNDED_HISTORY_ONLY_NOT_CONTINUOUS",
		StartedAt: start, FinishedAt: now, WindowStartMS: start.Add(-time.Hour).UnixMilli(), WindowEndMS: start.UnixMilli(),
		Portfolio: exchange.AccountPortfolio{CanTrade: true, TotalWalletBalance: "5000", TotalUnrealizedProfit: "0", TotalMarginBalance: "5000",
			TotalInitialMargin: "0", TotalPositionInitialMargin: "0", TotalOpenOrderInitialMargin: "0", AvailableBalance: "5000",
			Assets:    []exchange.PortfolioAsset{{Asset: "USDT", WalletBalance: "5000", AvailableBalance: "5000", MarginBalance: "5000"}},
			Positions: []exchange.PortfolioPosition{},
		}, OpenOrders: []exchange.Order{}, OpenAlgos: []exchange.AlgoOrder{}, RecentBTCOrders: []exchange.Order{},
		RecentBTCAlgos: []exchange.AlgoOrder{}, RecentBTCTrades: []exchange.AccountTrade{}, RecentIncome: []exchange.IncomeRecord{},
	}
	return dir, budget, fp, b, now
}
func TestIntegratedEmptyStillNeverAuthorizes(t *testing.T) {
	dir, budget, fp, b, now := emptyIntegration(t)
	r, err := ReviewIntegratedOffline(dir, budget, fp, b, nil, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	if r.State != "OBSERVED_FLAT_NOT_CONTINUOUS" || r.TradingAuthorized || r.StreamContinuityProven ||
		!r.ManualReviewRequired || r.AutoWalletAssignedUSDT != "1000" || r.OwnerCount != 0 ||
		r.AutoFreeBudgetUSDT != "1000" || r.StreamReviewState != "NO_ORDER_SIGNALS" || r.ProtectionReviewState != "EMPTY" {
		t.Fatalf("false status %+v", r)
	}
	if err = ArchiveIntegratedReview(dir, &r); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadIntegratedReviews(dir)
	if err != nil || len(loaded) != 1 || loaded[0].SHA256 == "" || loaded[0].TradingAuthorized {
		t.Fatalf("archive %v %+v", err, loaded)
	}
	var payload map[string]any
	raw, err := os.ReadFile(filepath.Join(dir, "trading", "integrated_reviews", r.ID+".json"))
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	payload["trading_authorized"] = true
	broken, _ := json.Marshal(payload)
	if err = os.WriteFile(filepath.Join(dir, "trading", "integrated_reviews", r.ID+".json"), broken, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = LoadIntegratedReviews(dir); err == nil {
		t.Fatal("archive tamper accepted")
	}
}
func TestIntegratedGapPendingReservationAccountMismatchAndStale(t *testing.T) {
	t.Run("gap", func(t *testing.T) {
		dir, budget, fp, b, now := emptyIntegration(t)
		es := &EventStore{dir: filepath.Join(dir, "trading", "user_events")}
		if err := es.RecordDiscontinuity("DISCONNECT"); err != nil {
			t.Fatal(err)
		}
		r, err := ReviewIntegratedOffline(dir, budget, fp, b, nil, nil, now)
		if err != nil || r.State != "BLOCKED" || r.StreamGapCount != 1 || r.TradingAuthorized {
			t.Fatalf("gap not blocked %+v %v", r, err)
		}
	})
	t.Run("pending model", func(t *testing.T) {
		dir, budget, fp, b, now := emptyIntegration(t)
		co, err := OpenCoordinator(dir)
		if err != nil {
			t.Fatal(err)
		}
		basis := cleanReservationBasis(now)
		if _, _, err = co.reservePreview(modelIntent("pending-model01", "trend", Long, Enter, "0.005"), basis, now); err != nil {
			t.Fatal(err)
		}
		if err = co.Close(); err != nil {
			t.Fatal(err)
		}
		r, err := ReviewIntegratedOffline(dir, budget, fp, b, nil, nil, now)
		if err != nil || r.State != "BLOCKED" || r.ActiveModelReservations != 1 || r.PendingAutoMarginUSDT == "0" || r.AutoFreeBudgetUSDT == "1000" || r.TradingAuthorized {
			t.Fatalf("reserved collateral lost %+v %v", r, err)
		}
	})
	t.Run("wrong account", func(t *testing.T) {
		dir, budget, _, b, now := emptyIntegration(t)
		r, err := ReviewIntegratedOffline(dir, budget, strings.Repeat("b", 64), b, nil, nil, now)
		if err != nil || r.State != "BLOCKED" || r.AutoFreeBudgetUSDT == "1000" {
			t.Fatalf("wallet other account accepted %+v %v", r, err)
		}
	})
	t.Run("stale signed account", func(t *testing.T) {
		dir, budget, fp, b, now := emptyIntegration(t)
		b.FinishedAt = now.Add(-10 * time.Second)
		if _, err := ReviewIntegratedOffline(dir, budget, fp, b, nil, nil, now); err == nil {
			t.Fatal("stale signed account accepted")
		}
	})
	t.Run("missing gaps directory", func(t *testing.T) {
		dir, budget, fp, b, now := emptyIntegration(t)
		if err := os.Remove(filepath.Join(dir, "trading", "stream_discontinuities")); err != nil {
			t.Fatal(err)
		}
		if _, err := ReviewIntegratedOffline(dir, budget, fp, b, nil, nil, now); err == nil {
			t.Fatal("missing gap index accepted")
		}
	})
}
func TestWalletQuietGuardBlocksAllRelevantFiles(t *testing.T) {
	for _, d := range []string{"requests", "reservations", "fills", "protection_submissions", "algo_cancel_intents", "manual_cancels", "coordinator.active", "user_events", "stream_discontinuities"} {
		t.Run(d, func(t *testing.T) {
			root := t.TempDir()
			if err := CheckWalletEditQuietState(root); err != nil {
				t.Fatal(err)
			}
			dir := filepath.Join(root, "trading", d)
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "stale-record"), []byte("incomplete"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := CheckWalletEditQuietState(root); err == nil {
				t.Fatal("unsafe wallet adjustment accepted")
			}
		})
	}
}
func TestWalletQuietGuardBlocksEmptyCoordinatorLock(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "trading", "coordinator.active"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := CheckWalletEditQuietState(root); err == nil {
		t.Fatal("empty interrupted coordinator lock accepted")
	}
}
func TestConservativeFloorCeilBudget(t *testing.T) {
	importx := func(s string) *big.Rat { r, _ := new(big.Rat).SetString(s); return r }
	if ceilTo8(importx("0.000000001")) != "0.00000001" || floorTo8(importx("0.000000009")) != "0" {
		t.Fatal("risk rounding would overstate available or understate collateral")
	}
}

type integratedReaderMock struct {
	baseline      exchange.RecoveryBaseline
	after         func()
	baselineCalls int
}

func (m *integratedReaderMock) FetchDemoRecoveryBaseline(ctx context.Context, key, secret string, fromMS int64) (exchange.RecoveryBaseline, error) {
	m.baselineCalls++
	if key != "demo_mock_key" || secret != "demo_mock_secret" {
		return exchange.RecoveryBaseline{}, errors.New("bad demo credentials")
	}
	if m.after != nil {
		m.after()
	}
	return m.baseline, nil
}
func (m *integratedReaderMock) QueryDemoOrder(context.Context, string, string, string, string) (exchange.OrderResult, error) {
	return exchange.OrderResult{}, errors.New("unexpected order read")
}
func (m *integratedReaderMock) FetchDemoOrderTrades(context.Context, string, string, string, int64) ([]exchange.AccountTrade, error) {
	return nil, errors.New("unexpected trade read")
}
func TestIntegratedOrchestratorSignedReadAndRaceGuard(t *testing.T) {
	t.Run("mock authenticated flat never permits trading", func(t *testing.T) {
		dir, budget, fp, b, now := emptyIntegration(t)
		mock := &integratedReaderMock{baseline: b}
		r, err := DiagnoseIntegratedDemo(context.Background(), dir, budget, mock, DemoCredentials{APIKey: "demo_mock_key", Secret: "demo_mock_secret"}, fp, b.WindowStartMS, nil, func() time.Time { return now })
		if err != nil || r.State != "OBSERVED_FLAT_NOT_CONTINUOUS" || r.TradingAuthorized || r.StreamContinuityProven || mock.baselineCalls != 1 {
			t.Fatalf("unsafe diagnostic %+v %v", r, err)
		}
		reports, err := LoadIntegratedReviews(dir)
		if err != nil || len(reports) != 1 || reports[0].ID == "" {
			t.Fatalf("archive missing %+v %v", reports, err)
		}
	})
	t.Run("different API key account denied before network", func(t *testing.T) {
		dir, budget, fp, b, now := emptyIntegration(t)
		mock := &integratedReaderMock{baseline: b}
		_, err := DiagnoseIntegratedDemo(context.Background(), dir, budget, mock, DemoCredentials{APIKey: "someone_else", Secret: "demo_mock_secret"}, fp, b.WindowStartMS, nil, func() time.Time { return now })
		if err == nil || mock.baselineCalls != 0 {
			t.Fatal("wrong account reader invoked")
		}
	})
	t.Run("private account gap during REST invalidates", func(t *testing.T) {
		dir, budget, fp, b, now := emptyIntegration(t)
		mock := &integratedReaderMock{baseline: b}
		mock.after = func() {
			es := &EventStore{dir: filepath.Join(dir, "trading", "user_events")}
			if err := es.RecordDiscontinuity("DISCONNECT"); err != nil {
				t.Fatal(err)
			}
		}
		_, err := DiagnoseIntegratedDemo(context.Background(), dir, budget, mock, DemoCredentials{APIKey: "demo_mock_key", Secret: "demo_mock_secret"}, fp, b.WindowStartMS, nil, func() time.Time { return now })
		if err == nil {
			t.Fatal("changed private WS gap assumed stable")
		}
		reports, err := LoadIntegratedReviews(dir)
		if err != nil || len(reports) != 0 {
			t.Fatal("archived unreliable baseline")
		}
	})
	t.Run("local journal mutation during REST invalidates", func(t *testing.T) {
		dir, budget, fp, b, now := emptyIntegration(t)
		mock := &integratedReaderMock{baseline: b}
		mock.after = func() {
			path := filepath.Join(dir, "trading", "requests")
			if err := os.MkdirAll(path, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(path, "unexpected-inflight.json"), []byte("{}"), 0600); err != nil {
				t.Fatal(err)
			}
		}
		_, err := DiagnoseIntegratedDemo(context.Background(), dir, budget, mock, DemoCredentials{APIKey: "demo_mock_key", Secret: "demo_mock_secret"}, fp, b.WindowStartMS, nil, func() time.Time { return now })
		if err == nil {
			t.Fatal("local in-flight journal went unseen")
		}
	})
}
