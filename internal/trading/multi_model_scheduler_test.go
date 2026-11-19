package trading

import (
	"context"
	"sync"
	"testing"
	"time"

	"binance_trader/internal/model"
)

func schedulerFixture(t *testing.T) (*ReadOnlyMultiModelScheduler, PreviewCycleEvidence) {
	t.Helper()
	registry, ids, market, signed, owners, rules, now := previewFixture(t, model.ActionEnterLong, model.ActionEnterShort)
	scheduler, err := NewReadOnlyMultiModelScheduler(registry, ids)
	if err != nil {
		t.Fatal(err)
	}
	return scheduler, PreviewCycleEvidence{Market: market, Signed: signed, Ownership: owners, Rules: rules, AssignedUSDT: "1000", Now: now}
}

func TestReadOnlyMultiModelSchedulerSingleCycleAndDuplicate(t *testing.T) {
	scheduler, in := schedulerFixture(t)
	first, err := scheduler.Cycle(context.Background(), in)
	if err != nil || first.State != "PREVIEW_ONLY" || len(first.Preview.Candidates) != 2 || first.Preview.ProvisionalTotalUSDT != "190" || first.TradingAuthorized || first.ExecutionEnabled || first.Preview.ExecutionEnabled {
		t.Fatalf("first read-only preview %+v %v", first, err)
	}
	if scheduler.LastCandle() != first.CandleOpenMS {
		t.Fatal("scheduler cursor missing")
	}
	again, err := scheduler.Cycle(context.Background(), in)
	if err != nil || !again.DuplicateSuppressed || again.State != "DUPLICATE_CANDLE_SUPPRESSED" || len(again.Preview.Candidates) != 0 || again.Preview.ProvisionalTotalUSDT != "0" {
		t.Fatalf("duplicate generated new candidate %+v %v", again, err)
	}
}

func TestReadOnlyMultiModelSchedulerNeverReusesStaleEvidence(t *testing.T) {
	scheduler, in := schedulerFixture(t)
	if _, err := scheduler.Cycle(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	in.Signed.AsOf = in.Now.Add(-time.Minute).Format(time.RFC3339Nano)
	if _, err := scheduler.Cycle(context.Background(), in); err == nil {
		t.Fatal("stale signed account accepted on duplicate candle")
	}
	if scheduler.LastCandle() == 0 {
		t.Fatal("previous valid candle was erased")
	}
}

func TestReadOnlyMultiModelSchedulerRejectsReverseAndAdvances(t *testing.T) {
	scheduler, in := schedulerFixture(t)
	if _, err := scheduler.Cycle(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	old := scheduler.LastCandle()
	in.Now = in.Now.Add(time.Minute)
	in.Signed.AsOf = in.Now.Format(time.RFC3339Nano)
	in.Market.LastReceivedAt = in.Now.Add(-time.Second).Format(time.RFC3339Nano)
	in.Market.Candles[len(in.Market.Candles)-1].OpenTime = old - 60000
	if _, err := scheduler.Cycle(context.Background(), in); err == nil {
		t.Fatal("reverse candle allowed")
	}
	in.Market.Candles[len(in.Market.Candles)-1].OpenTime = old + 60000
	next, err := scheduler.Cycle(context.Background(), in)
	if err != nil || next.State != "PREVIEW_ONLY" || scheduler.LastCandle() != old+60000 {
		t.Fatalf("new candle rejected %+v %v", next, err)
	}
}

func TestReadOnlyMultiModelSchedulerImmutableRegisteredSelection(t *testing.T) {
	reg, ids, _, _, _, _, _ := previewFixture(t, model.ActionHold, model.ActionHold)
	if _, err := NewReadOnlyMultiModelScheduler(reg, []string{ids[0], ids[0]}); err == nil {
		t.Fatal("duplicate accepted")
	}
	if _, err := NewReadOnlyMultiModelScheduler(reg, []string{ids[0], "unknown_model"}); err == nil {
		t.Fatal("unregistered accepted")
	}
	if _, err := NewReadOnlyMultiModelScheduler(reg, []string{ids[0]}); err == nil {
		t.Fatal("single model accepted as multi")
	}
	selected := append([]string(nil), ids...)
	scheduler, err := NewReadOnlyMultiModelScheduler(reg, selected)
	if err != nil {
		t.Fatal(err)
	}
	selected[0] = "injected"
	if scheduler.ids[0] != ids[0] {
		t.Fatal("mutable selected IDs")
	}
}

func TestReadOnlyMultiModelSchedulerConcurrentOnePreviewPerCandle(t *testing.T) {
	scheduler, in := schedulerFixture(t)
	var wg sync.WaitGroup
	results := make(chan PreviewCycle, 30)
	errs := make(chan error, 30)
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); p, e := scheduler.Cycle(context.Background(), in); results <- p; errs <- e }()
	}
	wg.Wait()
	close(results)
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	first := 0
	for p := range results {
		if p.State == "PREVIEW_ONLY" {
			first++
		} else if p.State != "DUPLICATE_CANDLE_SUPPRESSED" {
			t.Fatal(p)
		}
	}
	if first != 1 {
		t.Fatalf("expected 1 candidate evaluation, got %d", first)
	}
}

func TestReadOnlyMultiModelSchedulerCancelDoesNotConsumeCandle(t *testing.T) {
	scheduler, in := schedulerFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := scheduler.Cycle(ctx, in); err == nil {
		t.Fatal("cancel ignored")
	}
	if scheduler.LastCandle() != 0 {
		t.Fatal("cancel consumed candle")
	}
	if _, err := scheduler.Cycle(context.Background(), in); err != nil {
		t.Fatal(err)
	}
}

func TestReadOnlyMultiModelSchedulerRejectsOldCandleEvenWhenWSIsFresh(t *testing.T) {
	scheduler, in := schedulerFixture(t)
	in.Now = in.Now.Add(2 * time.Minute)
	in.Signed.AsOf = in.Now.Format(time.RFC3339Nano)
	in.Market.LastReceivedAt = in.Now.Add(-time.Second).Format(time.RFC3339Nano)
	if _, err := scheduler.Cycle(context.Background(), in); err == nil {
		t.Fatal("stale candle accepted based only on fresh WS receive timestamp")
	}
	if scheduler.LastCandle() != 0 {
		t.Fatal("unsafe stale candle consumed")
	}
}
