package trading

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

func recoveryPortfolio() exchange.AccountPortfolio {
	return exchange.AccountPortfolio{CanTrade: true, MultiAssetsMargin: false, TotalWalletBalance: "100", TotalUnrealizedProfit: "0", TotalMarginBalance: "100", TotalInitialMargin: "0", TotalPositionInitialMargin: "0", TotalOpenOrderInitialMargin: "0", AvailableBalance: "100",
		Assets: []exchange.PortfolioAsset{{Asset: "USDT", WalletBalance: "100", AvailableBalance: "100", MarginBalance: "100"}}, Positions: []exchange.PortfolioPosition{}}
}
func recoveryBaseline(now time.Time) exchange.RecoveryBaseline {
	start := now.Add(-600 * time.Millisecond)
	return exchange.RecoveryBaseline{Environment: "demo", StartedAt: start, FinishedAt: now.Add(-300 * time.Millisecond), WindowStartMS: start.Add(-time.Hour).UnixMilli(), WindowEndMS: start.UnixMilli(),
		Portfolio: recoveryPortfolio(), OpenOrders: []exchange.Order{}, OpenAlgos: []exchange.AlgoOrder{}, RecentBTCOrders: []exchange.Order{}, RecentBTCAlgos: []exchange.AlgoOrder{}, RecentBTCTrades: []exchange.AccountTrade{}, RecentIncome: []exchange.IncomeRecord{}, Coverage: "BOUNDED_HISTORY_ONLY_NOT_CONTINUOUS"}
}
func issueHas(issues []string, fragment string) bool {
	for _, v := range issues {
		if strings.Contains(v, fragment) {
			return true
		}
	}
	return false
}
func TestPortfolioAllocationExactBudgetAndExposure(t *testing.T) {
	p := recoveryPortfolio()
	p.TotalWalletBalance = "100.1"
	p.TotalUnrealizedProfit = "-20.1"
	p.TotalMarginBalance = "80.0"
	p.TotalInitialMargin = "21"
	p.TotalPositionInitialMargin = "20"
	p.TotalOpenOrderInitialMargin = "1"
	p.Positions = []exchange.PortfolioPosition{{Symbol: "BTCUSDT", PositionSide: "LONG", PositionAmt: "0.01", InitialMargin: "20", Isolated: true}}
	r, err := AssessPortfolioAllocation(p, "80", "20", "1.5", "0.5")
	if err != nil || !r.FitsCap || r.EquityUSDT != "80" || r.ExistingConservativeInitialMarginUSDT != "21" || r.AutoWalletFreeUSDT != "58.5" || r.AutoWalletMaxEntryUSDT != "5.85" {
		t.Fatalf("wallet allocation preview incorrect: %+v %v", r, err)
	}
	r, err = AssessPortfolioAllocation(p, "80", "20", "1.5", "6")
	if err != nil || r.FitsCap || !issueHas(r.Issues, "10%") {
		t.Fatalf("10 percent model entry limit missing %+v %v", r, err)
	}
	p.TotalInitialMargin = "19"
	r, err = AssessPortfolioAllocation(p, "80", "20", "1.5", "0.5")
	if err != nil || r.FitsCap || !issueHas(r.Issues, "증거금 합계") {
		t.Fatalf("invalid margin accepted %+v %v", r, err)
	}
	p.TotalInitialMargin = "21"
	p.MultiAssetsMargin = true
	r, err = AssessPortfolioAllocation(p, "80", "20", "1.5", "0.5")
	if err != nil || r.FitsCap || !issueHas(r.Issues, "다중 자산") {
		t.Fatalf("unsupported multi asset accepted %+v %v", r, err)
	}
	legacy, err := AssessPortfolioCap(recoveryPortfolio(), "0", "0")
	if err != nil || legacy.FitsCap || !issueHas(legacy.Issues, "배정 지갑") || legacy.Cap30PercentUSDT != "" {
		t.Fatalf("obsolete 30 percent policy usable: %+v %v", legacy, err)
	}
}
func TestPortfolioAllocationMissingDuplicateAssetsAndEquityMismatch(t *testing.T) {
	p := recoveryPortfolio()
	p.Assets = nil
	if _, err := AssessPortfolioAllocation(p, "10", "0", "0", "1"); err == nil {
		t.Fatal("missing USDT accepted")
	}
	p = recoveryPortfolio()
	p.TotalMarginBalance = "101"
	r, err := AssessPortfolioAllocation(p, "10", "0", "0", "1")
	if err != nil || r.FitsCap || !issueHas(r.Issues, "일치") {
		t.Fatalf("equity mismatch accepted %+v %v", r, err)
	}
	p = recoveryPortfolio()
	p.Assets = append(p.Assets, p.Assets[0])
	if _, err := AssessPortfolioAllocation(p, "10", "0", "0", "1"); err == nil {
		t.Fatal("duplicate USDT accepted")
	}
	if _, err := AssessPortfolioAllocation(recoveryPortfolio(), "-1", "0", "0", "0"); err == nil {
		t.Fatal("negative allocation accepted")
	}
}
func TestRecoveryReviewNeverGrantsTrading(t *testing.T) {
	root := t.TempDir()
	store, err := NewOwnershipStore(root)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	b := recoveryBaseline(now)
	r, err := ReviewRecovery(root, store, b, now)
	if err != nil {
		t.Fatal(err)
	}
	if r.TradingAuthorized || !r.ManualReviewRequired || r.Ownership == nil || !r.Ownership.ReadyForAssessment || r.Risk.FitsCap || !issueHas(r.Issues, "체크포인트") {
		t.Fatalf("unsafe recovery review %+v", r)
	}
	path, err := ArchiveRecoveryReview(root, r)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(path); err != nil {
		t.Fatal(err)
	}
	reviews, err := LoadRecoveryReviews(root)
	if err != nil || len(reviews) != 1 || reviews[0].TradingAuthorized {
		t.Fatalf("invalid persistent reports %+v %v", reviews, err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString("XXX")
	f.Close()
	if _, err := LoadRecoveryReviews(root); err == nil {
		t.Fatal("tampered review accepted")
	}
}
func TestRecoveryRejectsExternalTradeOrderFundingAndHistoryGap(t *testing.T) {
	root := t.TempDir()
	store, _ := NewOwnershipStore(root)
	now := time.Now().UTC()
	b := recoveryBaseline(now)
	b.RecentBTCOrders = []exchange.Order{{Symbol: "BTCUSDT", OrderID: 77, ClientOrderID: "outside"}}
	b.RecentBTCTrades = []exchange.AccountTrade{{Symbol: "BTCUSDT", OrderID: 77, TradeID: 300, TimeMS: b.WindowEndMS - 100, Price: "100", Quantity: "0.01", Commission: "0.01", RealizedPnL: "0", CommissionAsset: "USDT", Side: "BUY", PositionSide: "LONG"}}
	b.RecentIncome = []exchange.IncomeRecord{{Type: "FUNDING_FEE", Symbol: "BTCUSDT", Asset: "USDT", Income: "-0.025", TransactionID: 1, TimeMS: b.WindowEndMS - 100}}
	r, err := ReviewRecovery(root, store, b, now)
	if err != nil {
		t.Fatal(err)
	}
	if !issueHas(r.Issues, "장부에 없는") || !issueHas(r.Issues, "미수집 체결") || !issueHas(r.Issues, "미배분 펀딩") || r.FundingByAsset["USDT"] != "-0.025" {
		t.Fatalf("missing safety blockers %+v", r)
	}
	// A marker from a previous disconnected private account stream cannot
	// be silently treated as reconciled on the next app restart.
	path := filepath.Join(root, "trading", "stream_discontinuities")
	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatal(err)
	}
	marker := struct {
		Schema int    `json:"schema"`
		Reason string `json:"reason"`
		At     string `json:"at"`
	}{1, "DISCONNECT", now.Format(time.RFC3339Nano)}
	raw, _ := json.Marshal(marker)
	if err := os.WriteFile(filepath.Join(path, strings.Repeat("a", 32)+".json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	r, err = ReviewRecovery(root, store, b, now)
	if err != nil || !issueHas(r.Issues, "불연속") {
		t.Fatalf("ignored discontinuity %+v %v", r, err)
	}
	if err := os.WriteFile(filepath.Join(path, "bad.json"), []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReviewRecovery(root, store, b, now); err == nil {
		t.Fatal("ignored corrupt marker")
	}
}
func TestRecoveryStaleIncompleteAndOtherSymbolRisk(t *testing.T) {
	root := t.TempDir()
	store, _ := NewOwnershipStore(root)
	now := time.Now().UTC()
	b := recoveryBaseline(now)
	b.FinishedAt = now.Add(-time.Minute)
	if _, err := ReviewRecovery(root, store, b, now); err == nil {
		t.Fatal("stale review accepted")
	}
	b = recoveryBaseline(now)
	b.RecentBTCTrades = nil
	if _, err := ReviewRecovery(root, store, b, now); err == nil {
		t.Fatal("incomplete history accepted")
	}
	b = recoveryBaseline(now)
	b.Portfolio.Positions = []exchange.PortfolioPosition{{Symbol: "ETHUSDT", PositionSide: "LONG", PositionAmt: "1", InitialMargin: "10", Isolated: true}}
	b.OpenOrders = []exchange.Order{{OrderID: 1, Symbol: "ETHUSDT", Status: "NEW"}}
	r, err := ReviewRecovery(root, store, b, now)
	if err != nil || !issueHas(r.Issues, "다른 종목의 미청산") || !issueHas(r.Issues, "다른 종목의 미체결") {
		t.Fatalf("ignored external market %+v %v", r, err)
	}
}
func TestArchiveRejectsTradingAuthorization(t *testing.T) {
	r := RecoveryReview{Version: 1, TradingAuthorized: true, ManualReviewRequired: true, ObservedAt: time.Now().UTC()}
	if _, err := ArchiveRecoveryReview(t.TempDir(), r); err == nil {
		t.Fatal("wrote unauthorized review")
	}
}

func TestRecoveryUsesDurableReservedMarginNotCallerClaim(t *testing.T) {
	root := t.TempDir()
	store, err := NewOwnershipStore(root)
	if err != nil {
		t.Fatal(err)
	}
	c, err := OpenCoordinator(root)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	r, _, err := c.reservePreview(modelIntent("recover-marg01", "A", Long, Enter, "0.005"), cleanReservationBasis(now), now)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	b := recoveryBaseline(now)
	review, err := ReviewRecovery(root, store, b, now)
	if err != nil {
		t.Fatal(err)
	}
	if review.Risk.PendingReservedMarginUSDT != r.MarginUSDT || review.Risk.FitsCap || !issueHas(review.Issues, "배정 지갑") {
		t.Fatalf("ignored durable reservation %+v", review.Risk)
	}
	// Failed O_EXCL reservation writes or corrupted persisted data MUST stop.
	path := filepath.Join(root, "trading", "reservations", r.ClientOrderID+".json")
	if err := os.WriteFile(path, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReviewRecovery(root, store, b, now); err == nil {
		t.Fatal("corrupt reservation ignored")
	}
}
