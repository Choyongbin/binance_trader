package trading

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

type stressAutoRuntime struct {
	runs   *atomic.Int64
	closes *atomic.Int64
}

func (r *stressAutoRuntime) RunOnce(context.Context) (AutoExecutionResult, error) {
	r.runs.Add(1)
	return AutoExecutionResult{Decision: modelHoldDecision()}, nil
}
func (r *stressAutoRuntime) Close() error {
	r.closes.Add(1)
	return nil
}

func TestAutoControllerRepeatedStartStopStress(t *testing.T) {
	if testing.Short() {
		t.Skip("stress")
	}
	dir := t.TempDir()
	control, err := OpenAutoRunControl(dir)
	if err != nil {
		t.Fatal(err)
	}
	var runs, closes atomic.Int64
	controller, err := NewAutoController(control, func(AutoRunLease) (AutoRuntime, error) {
		return &stressAutoRuntime{runs: &runs, closes: &closes}, nil
	}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	before := runtime.NumGoroutine()
	const cycles = 120
	for i := 0; i < cycles; i++ {
		if err := controller.Start(); err != nil {
			t.Fatalf("cycle %d start: %v", i, err)
		}
		deadline := time.Now().Add(time.Second)
		for runs.Load() < int64(i+1) && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if runs.Load() < int64(i+1) {
			t.Fatalf("cycle %d runtime did not execute", i)
		}
		if err := controller.Stop(); err != nil {
			t.Fatalf("cycle %d stop: %v", i, err)
		}
		if st := controller.Status(); st.Running {
			t.Fatalf("cycle %d still running: %+v", i, st)
		}
	}
	if closes.Load() != cycles {
		t.Fatalf("runtime close count=%d want=%d", closes.Load(), cycles)
	}
	st, err := control.Status()
	if err != nil || st.State != AutoRunStopped || st.Generation != cycles*2 {
		t.Fatalf("durable state=%+v err=%v", st, err)
	}
	runtime.GC()
	time.Sleep(20 * time.Millisecond)
	after := runtime.NumGoroutine()
	// Runtime and os/http background goroutines can fluctuate slightly under the
	// test runner; a linear leak across 120 cycles would be far larger.
	if after > before+8 {
		t.Fatalf("possible goroutine leak: before=%d after=%d", before, after)
	}
}

func TestContinuousMonitorRepeatedStartStopStress(t *testing.T) {
	if testing.Short() {
		t.Skip("stress")
	}
	dir := t.TempDir()
	before := runtime.NumGoroutine()
	observer := func(ctx context.Context, store *EventStore, connected func()) error {
		if err := store.RecordDiscontinuity("STARTUP"); err != nil {
			return err
		}
		connected()
		<-ctx.Done()
		return ctx.Err()
	}
	verify := func(context.Context) (string, []string, error) {
		return "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", nil, nil
	}
	const sessions = 30
	for i := 0; i < sessions; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Millisecond)
		out, err := ContinuousMonitor(ctx, dir, observer, verify, time.Millisecond, nil)
		cancel()
		if err != nil {
			t.Fatalf("session %d: %v (%+v)", i, err, out)
		}
		if out.State != MonitorStopped || out.TradingAuthorized || out.StreamContinuityProven {
			t.Fatalf("session %d unsafe terminal state: %+v", i, out)
		}
	}
	if _, active, err := ReadContinuousMonitor(dir); err != nil || active {
		t.Fatalf("monitor lock/archive invalid after stress: active=%v err=%v", active, err)
	}
	runtime.GC()
	time.Sleep(20 * time.Millisecond)
	after := runtime.NumGoroutine()
	if after > before+8 {
		t.Fatalf("possible monitor goroutine leak: before=%d after=%d", before, after)
	}
}

func TestDiagnosticRetentionLargeArchiveStress(t *testing.T) {
	if testing.Short() {
		t.Skip("stress")
	}
	dir := t.TempDir()
	store, err := NewOwnershipStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	review, err := ReviewRecovery(dir, store, recoveryBaseline(now), now)
	if err != nil {
		t.Fatal(err)
	}
	const total = 80
	for i := 0; i < total; i++ {
		if _, err := ArchiveRecoveryReview(dir, review); err != nil {
			t.Fatalf("archive %d: %v", i, err)
		}
	}
	out, err := CompactDiagnosticArchives(dir, 7, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if out.Archived != total-7 || out.Removed != total-7 || out.TradingAuthorized {
		t.Fatalf("unexpected retention result: %+v", out)
	}
	all, err := LoadRecoveryReviews(dir)
	if err != nil || len(all) != total {
		t.Fatalf("logical history lost: count=%d err=%v", len(all), err)
	}
	out2, err := CompactDiagnosticArchives(dir, 7, now.Add(2*time.Second))
	if err != nil || out2.Archived != 0 || out2.Removed != 0 {
		t.Fatalf("second compaction not idempotent: %+v err=%v", out2, err)
	}
}
