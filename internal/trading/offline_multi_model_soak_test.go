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
	"binance_trader/internal/model"
)

// All timestamps advance in a fake clock. This never sleeps 24 hours and
// deliberately does not open sockets, send orders or write trading journals.
func soakSample(in PreviewCycleEvidence, baseline exchange.RecoveryBaseline, minute int, cap int) OfflineMultiModelSoakSample {
	at := in.Now.Add(time.Duration(minute) * time.Minute)
	in.Now = at
	in.Rules.FetchedAt = at
	in.Signed.AsOf = at.Format(time.RFC3339Nano)
	in.Market.LastReceivedAt = at.Add(-time.Second).Format(time.RFC3339Nano)
	in.Market.Candles = append([]exchange.Candle(nil), in.Market.Candles...)
	c := &in.Market.Candles[len(in.Market.Candles)-1]
	c.OpenTime = at.UnixMilli() / 60000 * 60000
	c.CloseTime = c.OpenTime + 59999
	baseline.StartedAt = at.Add(-time.Second)
	baseline.FinishedAt = at
	return OfflineMultiModelSoakSample{Evidence: in, Portfolio: baseline, MaxPercent: cap}
}

func TestOfflineMultiModelSoakSimulated24Hours(t *testing.T) {
	scheduler, control, in, baseline := riskReviewFixture(t, model.ActionEnterLong, model.ActionEnterShort)
	if _, err := control.Start(); err != nil {
		t.Fatal(err)
	}
	// 1440 synthetic minute events + 96 duplicate/repeated WS updates.
	steps := make([]OfflineMultiModelSoakSample, 0, 1536)
	for i := 0; i < 1440; i++ {
		sample := soakSample(in, baseline, i, 30)
		steps = append(steps, sample)
		if i%15 == 0 {
			steps = append(steps, sample)
		}
	}
	out, err := AuditOfflineMultiModelSoak(context.Background(), scheduler, control, steps)
	if err != nil || out.State != "OFFLINE_READ_ONLY" || out.Samples != 1536 || out.Reviewed != 1536 ||
		out.FreshCandles != 1440 || out.Duplicates != 96 || out.ProvisionalCandidates != 2880 ||
		out.EvidenceRejected != 0 || out.GapBlocked != 0 || out.MaxProvisionalMarginUSDT != "190" ||
		out.TradingAuthorized || out.ExecutionEnabled || out.GapRecoveryRequiresIndependentReview {
		t.Fatalf("soak summary %+v: %v", outWithoutSteps(out), err)
	}
	for i, step := range out.Steps {
		if step.TradingAuthorized || step.ExecutionEnabled {
			t.Fatalf("step %d authorization: %+v", i, step)
		}
		if step.State == "DUPLICATE_CANDLE_NO_NEW_ENTRY" && step.CandidateCount != 0 {
			t.Fatalf("duplicate candle retained entry: %+v", step)
		}
		if step.State == "BOUNDED_PREVIEW_NOT_AUTHORIZED" && step.CandidateCount != 2 {
			t.Fatalf("unexpected candidate counts: %+v", step)
		}
	}
	buf, err := json.Marshal(outWithoutSteps(out))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("simulated_24h_no_live_orders=%s", buf)
}

func outWithoutSteps(in OfflineMultiModelSoakReport) OfflineMultiModelSoakReport {
	in.Steps = nil
	return in
}

func TestOfflineMultiModelSoakReconnectStaleThenRecoveredSameCandle(t *testing.T) {
	scheduler, control, in, baseline := riskReviewFixture(t, model.ActionEnterLong, model.ActionEnterShort)
	if _, err := control.Start(); err != nil {
		t.Fatal(err)
	}
	first := soakSample(in, baseline, 0, 15)
	stale := soakSample(in, baseline, 1, 15)
	stale.Evidence.Market.LastReceivedAt = stale.Evidence.Now.Add(-15 * time.Second).Format(time.RFC3339Nano)
	repaired := soakSample(in, baseline, 1, 15)
	invalidAccount := soakSample(in, baseline, 2, 15)
	invalidAccount.Evidence.Signed.AsOf = invalidAccount.Evidence.Now.Add(-time.Minute).Format(time.RFC3339Nano)
	repairedAccount := soakSample(in, baseline, 2, 15)
	out, err := AuditOfflineMultiModelSoak(context.Background(), scheduler, control,
		[]OfflineMultiModelSoakSample{first, first, stale, repaired, invalidAccount, repairedAccount})
	if err != nil || out.FreshCandles != 3 || out.Duplicates != 1 || out.EvidenceRejected != 2 || out.GapBlocked != 0 ||
		out.State != "OFFLINE_BLOCKED_EVIDENCE_OR_GAP" || out.EquityCapBlocked != 3 || out.ProvisionalCandidates != 3 {
		t.Fatalf("reconnect %+v %v", outWithoutSteps(out), err)
	}
	if out.Steps[2].State != "EVIDENCE_REJECTED" || out.Steps[3].State != "BOUNDED_PREVIEW_NOT_AUTHORIZED" ||
		out.Steps[4].State != "EVIDENCE_REJECTED" || out.Steps[5].State != "BOUNDED_PREVIEW_NOT_AUTHORIZED" {
		t.Fatal(out.Steps)
	}
}

func TestOfflineMultiModelSoakGapIsLatchedNoAutoReconnect(t *testing.T) {
	scheduler, control, in, baseline := riskReviewFixture(t, model.ActionEnterLong, model.ActionEnterShort)
	if _, err := control.Start(); err != nil {
		t.Fatal(err)
	}
	steps := []OfflineMultiModelSoakSample{
		soakSample(in, baseline, 0, 30),
		soakSample(in, baseline, 1, 30),
		soakSample(in, baseline, 3, 30), // missing minute #2
		soakSample(in, baseline, 4, 30), // fresh market cannot erase the gap
	}
	out, err := AuditOfflineMultiModelSoak(context.Background(), scheduler, control, steps)
	if err != nil || out.FreshCandles != 2 || out.GapBlocked != 2 || !out.GapRecoveryRequiresIndependentReview ||
		out.Steps[2].State != "GAP_REQUIRES_RECONCILIATION" || out.Steps[3].State != "GAP_REQUIRES_RECONCILIATION" ||
		scheduler.LastCandle() != steps[1].Evidence.Market.Candles[0].OpenTime {
		t.Fatalf("gap %+v %v", out, err)
	}
	// A reversed clock also latches rather than rewinding the cursor.
	scheduler2, control2, in2, baseline2 := riskReviewFixture(t, model.ActionEnterLong, model.ActionEnterShort)
	if _, err := control2.Start(); err != nil {
		t.Fatal(err)
	}
	backwards := soakSample(in2, baseline2, 1, 30)
	backwards.Evidence.Market.Candles[0].OpenTime -= 60000
	o, e := AuditOfflineMultiModelSoak(context.Background(), scheduler2, control2, []OfflineMultiModelSoakSample{
		soakSample(in2, baseline2, 0, 30), soakSample(in2, baseline2, 1, 30), backwards,
	})
	if e != nil || o.GapBlocked != 1 {
		t.Fatal(o, e)
	}
}

func TestOfflineMultiModelSoakSTOPNeverClosesOrRestarts(t *testing.T) {
	scheduler, control, in, baseline := riskReviewFixture(t, model.ActionEnterLong, model.ActionEnterShort)
	in.Ownership.Owned = []OwnerPosition{{Source: Model, ModelID: "other", Symbol: "BTCUSDT", Side: Long, Quantity: "0.003", AverageEntry: "100000"}}
	in.Signed.Positions = []exchange.Position{{Symbol: "BTCUSDT", Side: "LONG", Qty: "0.003"}}
	baseline.Portfolio.Positions = []exchange.PortfolioPosition{{Symbol: "BTCUSDT", PositionSide: "LONG", PositionAmt: "0.003", InitialMargin: "20", Isolated: true}}
	baseline.Portfolio.TotalInitialMargin = "20"
	baseline.Portfolio.TotalPositionInitialMargin = "20"
	if _, err := control.Start(); err != nil {
		t.Fatal(err)
	}
	if err := control.Stop(); err != nil {
		t.Fatal(err)
	}
	before, _ := json.Marshal(in.Ownership)
	out, err := AuditOfflineMultiModelSoak(context.Background(), scheduler, control, []OfflineMultiModelSoakSample{
		soakSample(in, baseline, 0, 30), soakSample(in, baseline, 1, 30),
	})
	after, _ := json.Marshal(in.Ownership)
	if err != nil || out.StopBlocked != 2 || out.FreshCandles != 0 || out.ProvisionalCandidates != 0 ||
		string(before) != string(after) || scheduler.LastCandle() != 0 {
		t.Fatal(outWithoutSteps(out), err)
	}
	// Crash-restart with an orphaned RUN ID remains blocked, with no evaluation.
	if _, err := control.Start(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenAutoRunControl(control.dataDir)
	if err != nil {
		t.Fatal(err)
	}
	blocked, err := AuditOfflineMultiModelSoak(context.Background(), scheduler, reopened, []OfflineMultiModelSoakSample{
		soakSample(in, baseline, 2, 30),
	})
	if err != nil || blocked.StopBlocked != 1 || blocked.ProvisionalCandidates != 0 ||
		blocked.Steps[0].State != "NEW_ENTRIES_BLOCKED_RESTART_BLOCKED" {
		t.Fatal(blocked, err)
	}
}

func TestOfflineMultiModelSoakTamperAndLimit(t *testing.T) {
	scheduler, control, in, baseline := riskReviewFixture(t, model.ActionEnterLong, model.ActionEnterShort)
	if _, err := control.Start(); err != nil {
		t.Fatal(err)
	}
	first := soakSample(in, baseline, 0, 30)
	badEquity := soakSample(in, baseline, 1, 30)
	badEquity.Portfolio.Portfolio.TotalMarginBalance = "1001"
	good := soakSample(in, baseline, 1, 30)
	result, err := AuditOfflineMultiModelSoak(context.Background(), scheduler, control, []OfflineMultiModelSoakSample{first, badEquity, good})
	if err != nil || result.EvidenceRejected != 1 || result.FreshCandles != 2 || result.GapBlocked != 0 {
		t.Fatal(outWithoutSteps(result), err)
	}
	for _, n := range []int{0, maxOfflineSoakSamples + 1} {
		_, err := AuditOfflineMultiModelSoak(context.Background(), scheduler, control, make([]OfflineMultiModelSoakSample, n))
		if err == nil {
			t.Fatal("unbounded samples allowed", n)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := AuditOfflineMultiModelSoak(ctx, scheduler, control, []OfflineMultiModelSoakSample{first}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	// A damaged STOP journal is not a successful cycle.
	if err := os.WriteFile(filepath.Join(control.dir, "state.00000000000000000001.json"), []byte("{bad"), 0600); err != nil {
		t.Fatal(err)
	}
	got, err := AuditOfflineMultiModelSoak(context.Background(), scheduler, control, []OfflineMultiModelSoakSample{first})
	if err != nil || got.EvidenceRejected != 1 || got.ProvisionalCandidates != 0 {
		t.Fatal(got, err)
	}
}

func TestOfflineMultiModelSoakNoRealTimeGuarantee(t *testing.T) {
	// Trace time is user-supplied; elapsed wall-clock time has no role in the
	// result. Guard this explicit limitation in the emitted JSON contract.
	scheduler, control, in, baseline := riskReviewFixture(t, model.ActionEnterLong, model.ActionEnterShort)
	if _, err := control.Start(); err != nil {
		t.Fatal(err)
	}
	out, err := AuditOfflineMultiModelSoak(context.Background(), scheduler, control, []OfflineMultiModelSoakSample{soakSample(in, baseline, 0, 30)})
	if err != nil {
		t.Fatal(err)
	}
	js, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{`"trading_authorized":true`, `"execution_enabled":true`} {
		if strings.Contains(string(js), forbidden) {
			t.Fatal("invalid permission")
		}
	}
}
