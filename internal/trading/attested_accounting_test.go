package trading

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

func attestedFixture(t *testing.T) ([]Record, []StoredFill, exchange.RecoveryBaseline) {
	t.Helper()
	now := time.Now().UTC()
	base := now.Add(-time.Minute).UnixMilli()
	records, fills, _ := accountFixture(base)
	start := now.Add(-2 * time.Second).UTC()
	b := exchange.RecoveryBaseline{Environment: "demo", Coverage: "BOUNDED_HISTORY_ONLY_NOT_CONTINUOUS", StartedAt: start,
		FinishedAt: now, WindowStartMS: base - 1, WindowEndMS: start.UnixMilli(), OpenOrders: []exchange.Order{},
		OpenAlgos: []exchange.AlgoOrder{}, RecentBTCOrders: []exchange.Order{}, RecentBTCAlgos: []exchange.AlgoOrder{},
		RecentBTCTrades: []exchange.AccountTrade{}, RecentIncome: []exchange.IncomeRecord{}}
	if err := json.Unmarshal([]byte(`{"canTrade":true,"totalWalletBalance":"4000","totalUnrealizedProfit":"0","totalMarginBalance":"4000","totalInitialMargin":"0","totalPositionInitialMargin":"0","totalOpenOrderInitialMargin":"0","availableBalance":"4000","assets":[{"asset":"USDT","walletBalance":"4000","availableBalance":"4000","marginBalance":"4000"}],"positions":[{"symbol":"BTCUSDT","positionSide":"LONG","positionAmt":"2.5","initialMargin":"0","isolated":true},{"symbol":"BTCUSDT","positionSide":"SHORT","positionAmt":"-0.5","initialMargin":"0","isolated":true}]}`), &b.Portfolio); err != nil {
		t.Fatal(err)
	}
	for i, r := range records {
		b.RecentBTCOrders = append(b.RecentBTCOrders, exchange.Order{Symbol: "BTCUSDT", OrderID: r.ExchangeOrderID, ClientOrderID: r.Plan.ClientOrderID, Side: r.Plan.ExchangeSide, PositionSide: string(r.Plan.PositionSide), Type: string(r.Plan.Type), Status: "FILLED", OrigQty: r.Plan.Quantity, ExecutedQty: r.Plan.Quantity})
		b.RecentBTCTrades = append(b.RecentBTCTrades, fills[i].Trade)
	}
	return records, fills, b
}

func TestAttestedWindowFullMatchingTradesAndOldArchiveCompatibility(t *testing.T) {
	records, fills, b := attestedFixture(t)
	now := time.Now().UTC()
	report, err := BuildAttestedAccounting(records, fills, b, now)
	if err != nil {
		t.Fatal(err)
	}
	if report.TradingAuthorized || report.ActualNetVerified || report.EvidenceCoverage != "BOUNDED_DEMO_REST_MATCHED_LOCAL_NOT_CONTINUOUS" || len(report.EvidenceSHA256) != 64 {
		t.Fatalf("unexpected receipt %+v", report)
	}
	// Decimal JSON representation changes do not change exact economic values.
	b.RecentBTCTrades[0].Price = "100.0000"
	b.RecentBTCTrades[0].Commission = "0.1000"
	if _, err := BuildAttestedAccounting(records, fills, b, now); err != nil {
		t.Fatalf("equal precise decimals rejected: %v", err)
	}
	dir := t.TempDir()
	if err := ArchiveAccounting(dir, &report); err != nil {
		t.Fatal(err)
	}
	read, err := LoadAccountingReports(dir)
	if err != nil || len(read) != 1 || read[0].EvidenceSHA256 != report.EvidenceSHA256 {
		t.Fatalf("archive %+v %v", read, err)
	}
}
func TestAttestedWindowRejectsForeignTradeMissingFillAndFeeMismatch(t *testing.T) {
	records, fills, b := attestedFixture(t)
	now := time.Now().UTC()
	cases := []struct {
		name   string
		change func(*exchange.RecoveryBaseline)
	}{
		{"external flat trade", func(b *exchange.RecoveryBaseline) {
			copyTrade := b.RecentBTCTrades[0]
			copyTrade.TradeID = 999
			b.RecentBTCTrades = append(b.RecentBTCTrades, copyTrade)
		}},
		{"missing signed fill", func(b *exchange.RecoveryBaseline) { b.RecentBTCTrades = b.RecentBTCTrades[:len(b.RecentBTCTrades)-1] }},
		{"commission mismatch", func(b *exchange.RecoveryBaseline) { b.RecentBTCTrades[0].Commission = "0.09" }},
		{"cumulative qty mismatch", func(b *exchange.RecoveryBaseline) { b.RecentBTCOrders[0].ExecutedQty = "0.1" }},
		{"inconsistent orig qty", func(b *exchange.RecoveryBaseline) { b.RecentBTCOrders[0].OrigQty = "2" }},
		{"unsigned incomplete order", func(b *exchange.RecoveryBaseline) { b.RecentBTCOrders[0].Status = "PARTIALLY_FILLED" }},
		{"price mismatch", func(b *exchange.RecoveryBaseline) { b.RecentBTCTrades[0].Price = "101" }},
		{"foreign recent order", func(b *exchange.RecoveryBaseline) {
			b.RecentBTCOrders = append(b.RecentBTCOrders, exchange.Order{Symbol: "BTCUSDT", OrderID: 9999, ClientOrderID: "unknown"})
		}},
		{"conditional order", func(b *exchange.RecoveryBaseline) {
			b.RecentBTCAlgos = append(b.RecentBTCAlgos, exchange.AlgoOrder{AlgoID: 7})
		}},
		{"open order", func(b *exchange.RecoveryBaseline) { b.OpenOrders = append(b.OpenOrders, exchange.Order{OrderID: 1234}) }},
		{"actual side qty mismatch", func(b *exchange.RecoveryBaseline) { b.Portfolio.Positions = []exchange.PortfolioPosition{} }},
		{"missing trades endpoint", func(b *exchange.RecoveryBaseline) { b.RecentBTCTrades = nil }},
		{"stale bracket", func(b *exchange.RecoveryBaseline) { b.FinishedAt = now.Add(-time.Minute) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			copyB := b
			copyB.RecentBTCOrders = append([]exchange.Order(nil), b.RecentBTCOrders...)
			copyB.RecentBTCTrades = append([]exchange.AccountTrade(nil), b.RecentBTCTrades...)
			copyB.RecentBTCAlgos = append([]exchange.AlgoOrder(nil), b.RecentBTCAlgos...)
			copyB.OpenOrders = append([]exchange.Order(nil), b.OpenOrders...)
			tc.change(&copyB)
			if _, err := BuildAttestedAccounting(records, fills, copyB, now); err == nil {
				t.Fatal("accepted incomplete or foreign evidence")
			}
		})
	}
	// A local fill preceding the signed account window cannot contribute to
	// owner gross while the associated income history is bounded and short.
	b.WindowStartMS = fills[0].Trade.TimeMS + 1
	if _, err := BuildAttestedAccounting(records, fills, b, now); err == nil {
		t.Fatal("accepted local pre-window fill")
	}
}

type accountingBaselineReader func(context.Context, string, string, int64) (exchange.RecoveryBaseline, error)

func (f accountingBaselineReader) FetchDemoRecoveryBaseline(ctx context.Context, k, s string, from int64) (exchange.RecoveryBaseline, error) {
	return f(ctx, k, s, from)
}
func TestAccountingFingerprintDetectsInFlightLocalChangeAndBadFile(t *testing.T) {
	dir := t.TempDir()
	first, err := LocalAccountingFingerprint(dir)
	if err != nil || first == "" {
		t.Fatalf("empty ledger failed: %q %v", first, err)
	}
	reader := accountingBaselineReader(func(_ context.Context, k, s string, from int64) (exchange.RecoveryBaseline, error) {
		if k != "demo-only-key" || s != "demo-only-secret" {
			t.Fatal("wrong credentials")
		}
		if err := os.WriteFile(filepath.Join(dir, "trading", "requests", "malformed.txt"), []byte("mutation"), 0600); err != nil {
			t.Fatal(err)
		}
		return exchange.RecoveryBaseline{}, nil
	})
	_, _, err = PrepareAttestedAccounting(context.Background(), dir, reader, DemoCredentials{APIKey: "demo-only-key", Secret: "demo-only-secret"}, time.Now().Add(-time.Hour).UnixMilli(), time.Now)
	if err == nil || !strings.Contains(err.Error(), "바뀌었거나 손상") {
		t.Fatalf("mutation not detected %v", err)
	}
	if _, err := LocalAccountingFingerprint(dir); err == nil {
		t.Fatal("bad directory allowed")
	}
	if err := os.Remove(filepath.Join(dir, "trading", "requests", "malformed.txt")); err != nil {
		t.Fatal(err)
	}
	after, err := LocalAccountingFingerprint(dir)
	if err != nil || after != first {
		t.Fatalf("cannot recover known empty ledger %q %v", after, err)
	}
	if _, _, err := PrepareAttestedAccounting(context.Background(), dir, accountingBaselineReader(func(context.Context, string, string, int64) (exchange.RecoveryBaseline, error) {
		return exchange.RecoveryBaseline{}, errors.New("signed API fails")
	}), DemoCredentials{APIKey: "k", Secret: "s"}, time.Now().Add(-time.Hour).UnixMilli(), time.Now); err == nil {
		t.Fatal("ignored signed REST failure")
	}
}

// The signed window contains only exit and short-entry fills. The long entry
// was made days earlier, so its opening quantity and cost must carry forward
// WITHOUT also counting its historical commissions or gross PnL in this range.
func TestAttestedAccountingCarriesOldOpenLotsAsExplicitLocalEstimate(t *testing.T) {
	records, fills, baseline := attestedFixture(t)
	now := time.Now().UTC()
	baseline.WindowStartMS = now.Add(-2 * time.Hour).UnixMilli()
	baseline.StartedAt = now.Add(-time.Second)
	baseline.FinishedAt = now
	baseline.WindowEndMS = baseline.StartedAt.UnixMilli()
	for index := range fills[:2] {
		fills[index].Trade.TimeMS = now.Add(-9*24*time.Hour).UnixMilli() + int64(index)
		fills[index].seal()
	}
	baseline.RecentBTCOrders = append([]exchange.Order{}, baseline.RecentBTCOrders[2:]...)
	baseline.RecentBTCTrades = append([]exchange.AccountTrade{}, fills[2].Trade, fills[3].Trade)
	baseline.RecentIncome = []exchange.IncomeRecord{incomeFixture(t, "BTCUSDT", "FUNDING_FEE", "USDT", "-3", now.Add(-90*time.Minute).UnixMilli(), 78901)}
	report, err := BuildAttestedAccounting(records, fills, baseline, time.Now().UTC())
	if err != nil {
		t.Fatalf("valid carried local lot with signed bounded exits rejected: %v", err)
	}
	if report.KnownFills != 2 || report.KnownOrders != 2 || report.ActualNetVerified || report.TradingAuthorized ||
		report.EvidenceCoverage != "BOUNDED_DEMO_REST_MATCHED_LOCAL_WITH_UNATTESTED_OPENING_LOTS_NOT_CONTINUOUS" {
		t.Fatalf("misleading carryover report: %+v", report)
	}
	a := ownerAccounting(t, report, Model, "A", Long)
	if a.OpenQuantity != "0.5" || a.AverageEntry != "100" || a.VirtualGrossRealizedUSDT != "10" ||
		a.CommissionByAsset["USDT"] != "0.06" || a.ProvisionalFundingUSDT != "-1" {
		t.Fatalf("pre-window gross or fee mixed with bounded results: %+v", a)
	}
	b := ownerAccounting(t, report, Model, "B", Long)
	if b.OpenQuantity != "2" || b.CommissionByAsset["USDT"] != "" || b.ProvisionalFundingUSDT != "-2" {
		t.Fatalf("old-only owner opening basis wrong: %+v", b)
	}
	if len(report.Warnings) == 0 {
		t.Fatal("missing provisional basis warning")
	}
	tampered := append([]StoredFill{}, fills...)
	tampered[0].Trade.Price = "999"
	if _, err := BuildAttestedAccounting(records, tampered, baseline, time.Now().UTC()); err == nil {
		t.Fatal("tampered pre-window opening basis accepted")
	}
}
