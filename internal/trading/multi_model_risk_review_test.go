package trading

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"binance_trader/internal/exchange"
	"binance_trader/internal/model"
)

func riskReviewFixture(t *testing.T, actions ...string) (*ReadOnlyMultiModelScheduler, *AutoRunControl, PreviewCycleEvidence, exchange.RecoveryBaseline) {
	t.Helper()
	reg, ids, market, signed, report, rules, now := previewFixture(t, actions...)
	scheduler, err := NewReadOnlyMultiModelScheduler(reg, ids)
	if err != nil {
		t.Fatal(err)
	}
	control, err := OpenAutoRunControl(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	in := PreviewCycleEvidence{Market: market, Signed: signed, Ownership: report, Rules: rules, AssignedUSDT: "1000", Now: now}
	baseline := exchange.RecoveryBaseline{
		Environment: "demo", StartedAt: now.Add(-time.Second), FinishedAt: now,
		OpenOrders: []exchange.Order{}, OpenAlgos: []exchange.AlgoOrder{},
		Portfolio: exchange.AccountPortfolio{CanTrade: true, TotalWalletBalance: "1000", TotalUnrealizedProfit: "0", TotalMarginBalance: "1000", TotalInitialMargin: "0", TotalPositionInitialMargin: "0", TotalOpenOrderInitialMargin: "0", AvailableBalance: "1000", Assets: []exchange.PortfolioAsset{{Asset: "USDT", WalletBalance: "1000", AvailableBalance: "1000", MarginBalance: "1000"}}, Positions: []exchange.PortfolioPosition{}},
	}
	return scheduler, control, in, baseline
}

func TestOfflineMultiModelRiskReview30PercentAndReducedCap(t *testing.T) {
	for _, tc := range []struct {
		cap                      int
		wantTotal, first, second string
	}{
		{30, "190", "PREVIEW_ONLY", "PREVIEW_ONLY"},
		{15, "100", "PREVIEW_ONLY", "BLOCKED_EQUITY_CAP"},
		{9, "90", "BLOCKED_EQUITY_CAP", "PREVIEW_ONLY"},
		{8, "0", "BLOCKED_EQUITY_CAP", "BLOCKED_EQUITY_CAP"},
	} {
		t.Run(fmt.Sprintf("%d_%s_%s", tc.cap, tc.first, tc.second), func(t *testing.T) {
			scheduler, control, in, baseline := riskReviewFixture(t, model.ActionEnterLong, model.ActionEnterShort)
			if _, err := control.Start(); err != nil {
				t.Fatal(err)
			}
			got, err := scheduler.ReviewOfflineMultiModelRiskCycle(context.Background(), control, in, baseline, tc.cap)
			if err != nil || got.State != "BOUNDED_PREVIEW_NOT_AUTHORIZED" || got.Cycle == nil || got.Cycle.Preview.ProvisionalTotalUSDT != tc.wantTotal ||
				got.Cycle.Preview.Candidates[0].State != tc.first || got.Cycle.Preview.Candidates[1].State != tc.second ||
				got.TradingAuthorized || got.ExecutionEnabled || !got.ExistingPositionsUnchanged {
				t.Fatalf("risk cap %d: %+v %v", tc.cap, got, err)
			}
			if got.EquityCapUSDT == "" || got.RunState != AutoRunRunning {
				t.Fatal(got)
			}
			for _, c := range got.Cycle.Preview.Candidates {
				if c.State == "BLOCKED_EQUITY_CAP" && c.ProvisionalMarginUSDT != "" {
					t.Fatal("blocked candidate retained margin", c)
				}
			}
			duplicate, err := scheduler.ReviewOfflineMultiModelRiskCycle(context.Background(), control, in, baseline, tc.cap)
			if err != nil || duplicate.State != "DUPLICATE_CANDLE_NO_NEW_ENTRY" || duplicate.Cycle == nil ||
				!duplicate.Cycle.DuplicateSuppressed || duplicate.SelectedProvisionalMarginUSDT != "0" {
				t.Fatalf("duplicate: %+v %v", duplicate, err)
			}
		})
	}
}

func TestOfflineMultiModelRiskReviewStopCrashAndPreserveExistingPositions(t *testing.T) {
	scheduler, control, in, baseline := riskReviewFixture(t, model.ActionEnterLong, model.ActionEnterShort)
	// A recorded owner remains observable, but never gets force-closed by STOP.
	in.Ownership.Owned = []OwnerPosition{{Source: Model, ModelID: "unselected_owner", Symbol: "BTCUSDT", Side: Long, Quantity: "0.003", AverageEntry: "100000"}}
	in.Signed.Positions = []exchange.Position{{Symbol: "BTCUSDT", Side: "LONG", Qty: "0.003"}}
	baseline.Portfolio.Positions = []exchange.PortfolioPosition{{Symbol: "BTCUSDT", PositionSide: "LONG", PositionAmt: "0.003", InitialMargin: "20", Isolated: true}}
	baseline.Portfolio.TotalInitialMargin = "20"
	baseline.Portfolio.TotalPositionInitialMargin = "20"
	stopped, err := scheduler.ReviewOfflineMultiModelRiskCycle(context.Background(), control, in, baseline, 30)
	if err != nil || stopped.State != "NEW_ENTRIES_BLOCKED_STOPPED" || scheduler.LastCandle() != 0 || stopped.ExistingPositionCount != 1 {
		t.Fatalf("stopped %+v %v", stopped, err)
	}
	lease, err := control.Start()
	if err != nil {
		t.Fatal(err)
	}
	if err := control.Stop(); err != nil {
		t.Fatal(err)
	}
	if err := control.Guard(lease)(context.Background()); !errors.Is(err, ErrAutoRunStopped) {
		t.Fatal(err)
	}
	again, err := scheduler.ReviewOfflineMultiModelRiskCycle(context.Background(), control, in, baseline, 30)
	if err != nil || again.State != "NEW_ENTRIES_BLOCKED_STOPPED" || again.ExistingPositionCount != 1 || !again.ExistingPositionsUnchanged {
		t.Fatal(again, err)
	}
	if scheduler.LastCandle() != 0 {
		t.Fatal("STOP consumed candle")
	}
	// A different process's handle cannot silently resume an orphaned RUN.
	if _, err := control.Start(); err != nil {
		t.Fatal(err)
	}
	restarted, err := OpenAutoRunControl(control.dataDir)
	if err != nil {
		t.Fatal(err)
	}
	crashed, err := scheduler.ReviewOfflineMultiModelRiskCycle(context.Background(), restarted, in, baseline, 30)
	if err != nil || crashed.State != "NEW_ENTRIES_BLOCKED_RESTART_BLOCKED" || crashed.Cycle != nil {
		t.Fatal(crashed, err)
	}
	if _, err := restarted.Start(); !errors.Is(err, ErrAutoRunRestartBlocked) {
		t.Fatal("restart permitted", err)
	}
}

func TestOfflineMultiModelRiskReviewRejectsIncompleteOrForgedAccountEvidence(t *testing.T) {
	cases := []struct {
		name   string
		change func(*PreviewCycleEvidence, *exchange.RecoveryBaseline)
	}{
		{"missing signed order inventory", func(_ *PreviewCycleEvidence, b *exchange.RecoveryBaseline) { b.OpenOrders = nil }},
		{"missing algo inventory", func(_ *PreviewCycleEvidence, b *exchange.RecoveryBaseline) { b.OpenAlgos = nil }},
		{"stale signed portfolio", func(_ *PreviewCycleEvidence, b *exchange.RecoveryBaseline) {
			b.StartedAt = b.StartedAt.Add(-time.Minute)
		}},
		{"account equity mismatch", func(_ *PreviewCycleEvidence, b *exchange.RecoveryBaseline) { b.Portfolio.TotalMarginBalance = "900" }},
		{"wallet mismatch", func(_ *PreviewCycleEvidence, b *exchange.RecoveryBaseline) {
			b.Portfolio.Assets[0].WalletBalance = "999"
		}},
		{"available mismatch", func(in *PreviewCycleEvidence, _ *exchange.RecoveryBaseline) { in.Signed.Balances[0].Available = "999" }},
		{"unexpected asset", func(_ *PreviewCycleEvidence, b *exchange.RecoveryBaseline) {
			b.Portfolio.Assets = append(b.Portfolio.Assets, exchange.PortfolioAsset{Asset: "BTC", WalletBalance: "1", AvailableBalance: "0", MarginBalance: "1"})
		}},
		{"non BTC position", func(_ *PreviewCycleEvidence, b *exchange.RecoveryBaseline) {
			b.Portfolio.Positions = append(b.Portfolio.Positions, exchange.PortfolioPosition{Symbol: "ETHUSDT", PositionSide: "LONG", PositionAmt: "1", InitialMargin: "0"})
		}},
		{"unmatched signed position", func(in *PreviewCycleEvidence, _ *exchange.RecoveryBaseline) {
			in.Signed.Positions = []exchange.Position{{Symbol: "BTCUSDT", Side: "LONG", Qty: "0.01"}}
		}},
		{"portfolio marked not tradable", func(_ *PreviewCycleEvidence, b *exchange.RecoveryBaseline) { b.Portfolio.CanTrade = false }},
		{"untracked initial margin", func(_ *PreviewCycleEvidence, b *exchange.RecoveryBaseline) {
			b.Portfolio.TotalPositionInitialMargin = "10"
			b.Portfolio.TotalInitialMargin = "10"
		}},
		{"untracked open order initial margin", func(_ *PreviewCycleEvidence, b *exchange.RecoveryBaseline) {
			b.Portfolio.TotalOpenOrderInitialMargin = "5"
			b.Portfolio.TotalInitialMargin = "5"
		}},
		{"cross margin BTC position", func(in *PreviewCycleEvidence, b *exchange.RecoveryBaseline) {
			in.Signed.Positions = []exchange.Position{{Symbol: "BTCUSDT", Side: "LONG", Qty: "0.01"}}
			b.Portfolio.Positions = []exchange.PortfolioPosition{{Symbol: "BTCUSDT", PositionSide: "LONG", PositionAmt: "0.01", InitialMargin: "0", Isolated: false}}
		}},
		{"open resting order", func(_ *PreviewCycleEvidence, b *exchange.RecoveryBaseline) {
			b.OpenOrders = []exchange.Order{{Symbol: "BTCUSDT"}}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scheduler, control, in, baseline := riskReviewFixture(t, model.ActionEnterLong, model.ActionEnterShort)
			if _, err := control.Start(); err != nil {
				t.Fatal(err)
			}
			tc.change(&in, &baseline)
			got, err := scheduler.ReviewOfflineMultiModelRiskCycle(context.Background(), control, in, baseline, 30)
			if err == nil || got.State != "BLOCKED" || got.Cycle != nil || got.TradingAuthorized || scheduler.LastCandle() != 0 {
				t.Fatalf("forged accepted: %+v %v", got, err)
			}
		})
	}
	scheduler, control, in, baseline := riskReviewFixture(t, model.ActionEnterLong, model.ActionEnterShort)
	if _, err := control.Start(); err != nil {
		t.Fatal(err)
	}
	for _, cap := range []int{-1, 0, 31, 100} {
		got, err := scheduler.ReviewOfflineMultiModelRiskCycle(context.Background(), control, in, baseline, cap)
		if err == nil || got.Cycle != nil || scheduler.LastCandle() != 0 {
			t.Fatal(cap, got, err)
		}
	}
}

func TestOfflineMultiModelRiskReviewFailClosedCorruptedControlAndCanceledContext(t *testing.T) {
	scheduler, control, in, baseline := riskReviewFixture(t, model.ActionEnterLong, model.ActionEnterShort)
	if _, err := control.Start(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got, err := scheduler.ReviewOfflineMultiModelRiskCycle(ctx, control, in, baseline, 30); err == nil || got.Cycle != nil {
		t.Fatal(got, err)
	}
	if scheduler.LastCandle() != 0 {
		t.Fatal("canceled context consumed candle")
	}
	if err := os.WriteFile(filepath.Join(control.dir, "state.00000000000000000001.json"), []byte("{corrupted"), 0600); err != nil {
		t.Fatal(err)
	}
	if got, err := scheduler.ReviewOfflineMultiModelRiskCycle(context.Background(), control, in, baseline, 30); err == nil || got.Cycle != nil {
		t.Fatal(got, err)
	}
}

func TestOfflineMultiModelRiskReviewConcurrentDuplicateSinglePreview(t *testing.T) {
	scheduler, control, in, baseline := riskReviewFixture(t, model.ActionEnterLong, model.ActionEnterShort)
	if _, err := control.Start(); err != nil {
		t.Fatal(err)
	}
	const n = 12
	var wg sync.WaitGroup
	type result struct {
		v   MultiModelRiskCycle
		err error
	}
	results := make(chan result, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, e := scheduler.ReviewOfflineMultiModelRiskCycle(context.Background(), control, in, baseline, 30)
			results <- result{v, e}
		}()
	}
	wg.Wait()
	close(results)
	newCount := 0
	for x := range results {
		if x.err != nil || x.v.TradingAuthorized || x.v.ExecutionEnabled {
			t.Fatal(x.v, x.err)
		}
		if x.v.State == "BOUNDED_PREVIEW_NOT_AUTHORIZED" {
			newCount++
		} else if x.v.State != "DUPLICATE_CANDLE_NO_NEW_ENTRY" {
			t.Fatal(x.v)
		}
	}
	if newCount != 1 {
		t.Fatalf("only one candidate preview per candle, got %d", newCount)
	}
}

func TestOfflineMultiModelRiskReviewExistingMarginOverCapNeverLiquidates(t *testing.T) {
	scheduler, control, in, baseline := riskReviewFixture(t, model.ActionEnterLong, model.ActionEnterShort)
	// Model A owns a historical LONG from a previously selected strategy.
	in.Ownership.Owned = []OwnerPosition{{Source: Model, ModelID: "old_model", Symbol: "BTCUSDT", Side: Long, Quantity: "0.03", AverageEntry: "100000"}}
	in.Signed.Positions = []exchange.Position{{Symbol: "BTCUSDT", Side: "LONG", Qty: "0.03"}}
	baseline.Portfolio.Positions = []exchange.PortfolioPosition{{Symbol: "BTCUSDT", PositionSide: "LONG", PositionAmt: "0.03", InitialMargin: "200", Isolated: true}}
	baseline.Portfolio.TotalInitialMargin = "200"
	baseline.Portfolio.TotalPositionInitialMargin = "200"
	// Equity falls; automatic collateral is now above the 30% cap.
	baseline.Portfolio.TotalUnrealizedProfit = "-500"
	baseline.Portfolio.TotalMarginBalance = "500"
	baseline.Portfolio.AvailableBalance = "300"
	baseline.Portfolio.Assets[0].MarginBalance = "500"
	baseline.Portfolio.Assets[0].AvailableBalance = "300"
	in.Signed.Balances[0].Available = "300"
	if _, err := control.Start(); err != nil {
		t.Fatal(err)
	}
	got, err := scheduler.ReviewOfflineMultiModelRiskCycle(context.Background(), control, in, baseline, 30)
	if err != nil || got.Cycle == nil || got.State != "EXISTING_MARGIN_OVER_CAP_NO_NEW_ENTRY" || got.EquityCapUSDT != "150" || got.ExistingModelMarginUSDT != "200" ||
		got.Cycle.Preview.Candidates[0].State != "BLOCKED" || got.Cycle.Preview.Candidates[1].State != "BLOCKED_EQUITY_CAP" ||
		got.SelectedProvisionalMarginUSDT != "0" || got.RemainingEquityCapUSDT != "0" || got.ExistingPositionCount != 1 || !got.ExistingPositionsUnchanged {
		t.Fatalf("existing position lost/over cap entry permitted: %+v %v", got, err)
	}
	if len(in.Ownership.Owned) != 1 || in.Ownership.Owned[0].Quantity != "0.03" || got.TradingAuthorized {
		t.Fatal("mutated current owned position", got)
	}
	if err := control.Stop(); err != nil {
		t.Fatal(err)
	}
	stopped, err := scheduler.ReviewOfflineMultiModelRiskCycle(context.Background(), control, in, baseline, 30)
	if err != nil || stopped.Cycle != nil || stopped.State != "NEW_ENTRIES_BLOCKED_STOPPED" || stopped.ExistingPositionCount != 1 {
		t.Fatal(stopped, err)
	}
}

func TestOfflineMultiModelRiskReviewCanceledAndInvalidSignedDoNotAdvanceCursor(t *testing.T) {
	scheduler, control, in, baseline := riskReviewFixture(t, model.ActionEnterLong, model.ActionEnterShort)
	if _, err := control.Start(); err != nil {
		t.Fatal(err)
	}
	baseline.Portfolio.Positions = []exchange.PortfolioPosition{{Symbol: "BTCUSDT", PositionSide: "LONG", PositionAmt: "0.002", InitialMargin: "10", Isolated: true}}
	baseline.Portfolio.TotalInitialMargin = "10"
	baseline.Portfolio.TotalPositionInitialMargin = "10"
	got, err := scheduler.ReviewOfflineMultiModelRiskCycle(context.Background(), control, in, baseline, 30)
	if err == nil || got.Cycle != nil || scheduler.LastCandle() != 0 {
		t.Fatal(got, err)
	}
	baseline.Portfolio.Positions = nil
	if _, err := scheduler.ReviewOfflineMultiModelRiskCycle(context.Background(), control, in, baseline, 30); err == nil {
		t.Fatal("nil position inventory accepted")
	}
}

type blockedRiskStrategy struct {
	entered chan struct{}
	release chan struct{}
}

func (b blockedRiskStrategy) ID() string { return "offline_blocking_strategy" }
func (b blockedRiskStrategy) EvaluateLive(_ exchange.LiveMarketSnapshot, _ model.OwnerState, now time.Time) (model.Decision, error) {
	b.entered <- struct{}{}
	<-b.release
	return model.Decision{ModelID: b.ID(), Status: model.StatusReady, Action: model.ActionEnterLong, CandleOpenMS: now.UnixMilli() / 60000 * 60000}, nil
}

func TestOfflineMultiModelRiskReviewStopDuringEvaluationDiscardsCandidates(t *testing.T) {
	_, control, in, baseline := riskReviewFixture(t, model.ActionHold, model.ActionHold)
	reg := model.NewRegistry()
	blocking := blockedRiskStrategy{entered: make(chan struct{}, 1), release: make(chan struct{})}
	if err := reg.Register(blocking); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(previewModel{id: "second", action: model.ActionEnterShort}); err != nil {
		t.Fatal(err)
	}
	if err := reg.Freeze(); err != nil {
		t.Fatal(err)
	}
	scheduler, err := NewReadOnlyMultiModelScheduler(reg, []string{blocking.ID(), "second"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := control.Start(); err != nil {
		t.Fatal(err)
	}
	result := make(chan MultiModelRiskCycle, 1)
	errs := make(chan error, 1)
	go func() {
		got, e := scheduler.ReviewOfflineMultiModelRiskCycle(context.Background(), control, in, baseline, 30)
		result <- got
		errs <- e
	}()
	select {
	case <-blocking.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("strategy never entered")
	}
	if err := control.Stop(); err != nil {
		t.Fatal(err)
	}
	close(blocking.release)
	got, e := <-result, <-errs
	if e != nil || got.State != "STOP_OR_GENERATION_CHANGED" || got.Cycle != nil || got.TradingAuthorized || got.ExecutionEnabled ||
		got.RunState != AutoRunStopped {
		t.Fatalf("STOP race permitted candidates: %+v %v", got, e)
	}
}
