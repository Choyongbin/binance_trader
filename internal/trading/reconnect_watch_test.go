package trading

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

func watchBaselineReader(calls *atomic.Int32, failOn int) diagnosticReader {
	return func(_ context.Context, _, _ string, since int64) (exchange.RecoveryBaseline, error) {
		n := int(calls.Add(1))
		if n == failOn {
			return exchange.RecoveryBaseline{}, errors.New("SECRET-REST-UPSTREAM-DETAIL")
		}
		now := time.Now().UTC()
		b := recoveryBaseline(now)
		if n > 1 {
			b.StartedAt, b.FinishedAt = now.Add(-150*time.Millisecond), now.Add(-100*time.Millisecond)
		} else {
			b.StartedAt, b.FinishedAt = now.Add(-550*time.Millisecond), now.Add(-500*time.Millisecond)
		}
		b.WindowStartMS, b.WindowEndMS = since, b.StartedAt.UnixMilli()
		return b, nil
	}
}
func stableWatchObserver(ctx context.Context, store *EventStore, connected func()) error {
	if e := store.RecordDiscontinuity("STARTUP"); e != nil {
		return e
	}
	connected()
	<-ctx.Done()
	if e := store.RecordDiscontinuity("DISCONNECT"); e != nil {
		return e
	}
	return ctx.Err()
}
func TestReconnectWatchNoDropSignedTwiceAndNeverAuthorizes(t *testing.T) {
	dir := t.TempDir()
	var calls atomic.Int32
	r, e := WatchDemoReconnect(context.Background(), watchBaselineReader(&calls, 0), DemoCredentials{APIKey: "MOCK", Secret: "MOCK"}, dir, mockSince(), stableWatchObserver, 50*time.Millisecond)
	if e != nil || r.Stage != "OBSERVED" || !r.FinalRESTReviewed || r.Reconnects != 0 || len(r.Epochs) != 1 || r.Epochs[0].EndReason != "FINAL_STOP" || calls.Load() != 2 || r.TradingAuthorized || r.StreamContinuityProven || !r.ManualReviewRequired || !r.HistoricalOnly {
		t.Fatalf("unsafe result %+v %v (%d REST)", r, e, calls.Load())
	}
	reports, e := LoadReconnectWatches(dir)
	if e != nil || len(reports) != 1 || reports[0].SHA256 != r.SHA256 {
		t.Fatalf("invalid signed archive %+v %v", reports, e)
	}
	gaps, e := scanGapReasonsForTest(dir)
	if e != nil || gaps["STARTUP"] != 1 || gaps["DISCONNECT"] != 1 {
		t.Fatalf("duplicate/missing markers %+v %v", gaps, e)
	}
}
func TestReconnectWatchDroppedEpochRestBeforeFinal(t *testing.T) {
	dir := t.TempDir()
	var calls, epoch atomic.Int32
	drop := make(chan struct{})
	obs := func(ctx context.Context, store *EventStore, connected func()) error {
		no := epoch.Add(1)
		if e := store.RecordDiscontinuity("STARTUP"); e != nil {
			return e
		}
		connected()
		if no == 1 {
			select {
			case <-drop:
				return errors.New("SECRET-PRIVATE-URL-AND-KEY")
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		<-ctx.Done()
		return ctx.Err()
	}
	r, e := watchDemoReconnect(context.Background(), watchBaselineReader(&calls, 0), DemoCredentials{APIKey: "MOCK", Secret: "MOCK"}, dir, mockSince(), obs, 800*time.Millisecond, func(n int) {
		if n == 1 {
			close(drop)
		}
	})
	if e != nil || r.Stage != "OBSERVED" || r.Reconnects != 1 || len(r.Epochs) != 2 || r.Epochs[0].EndReason != "SOCKET_DROPPED" || !r.Epochs[1].SignedRESTReviewed || !r.FinalRESTReviewed || calls.Load() != 3 || r.TradingAuthorized || r.StreamContinuityProven || !issueHas(r.Blockers, "중단") {
		t.Fatalf("reconnect bypassed independent REST %+v err %v signed calls %d", r, e, calls.Load())
	}
	gaps, ge := scanGapReasonsForTest(dir)
	if ge != nil || gaps["STARTUP"] != 2 || gaps["DISCONNECT"] != 2 {
		t.Fatalf("incorrect epoch markers %+v %v", gaps, ge)
	}
}
func TestReconnectWatchSecondSignedRESTFailureBlocksAndRedacts(t *testing.T) {
	dir := t.TempDir()
	var calls atomic.Int32
	r, e := WatchDemoReconnect(context.Background(), watchBaselineReader(&calls, 2), DemoCredentials{APIKey: "MOCK", Secret: "SECRET-CREDENTIALS"}, dir, mockSince(), stableWatchObserver, 20*time.Millisecond)
	if e == nil || r.Stage != "BLOCKED" || r.FinalRESTReviewed || r.TradingAuthorized || calls.Load() != 2 || strings.Contains(e.Error(), "SECRET") {
		t.Fatalf("failed-REST accepted or leaked %+v %v", r, e)
	}
	reports, le := LoadReconnectWatches(dir)
	if le != nil || len(reports) != 1 || reports[0].Stage != "BLOCKED" {
		t.Fatalf("blocked report missing %+v %v", reports, le)
	}
}
func TestReconnectWatchCannotUseTamperedArchive(t *testing.T) {
	dir := t.TempDir()
	var calls atomic.Int32
	r, e := WatchDemoReconnect(context.Background(), watchBaselineReader(&calls, 0), DemoCredentials{APIKey: "MOCK", Secret: "MOCK"}, dir, mockSince(), stableWatchObserver, 20*time.Millisecond)
	if e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(dir, "trading", "reconnect_watch_sessions", r.ID+".end.json")
	raw, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	var obj map[string]any
	if json.Unmarshal(raw, &obj) != nil {
		t.Fatal("JSON invalid")
	}
	obj["trading_authorized"] = true
	tampered, _ := json.Marshal(obj)
	if e := os.WriteFile(path, tampered, 0600); e != nil {
		t.Fatal(e)
	}
	if _, e := LoadReconnectWatches(dir); e == nil {
		t.Fatal("tampered archive accepted")
	}
	if _, e := openReconnectArchive(dir); e == nil {
		t.Fatal("tampered prior report did not stop new session")
	}
}
func TestReconnectWatchRejectedBeforeOpeningAccount(t *testing.T) {
	dir := t.TempDir()
	var calls atomic.Int32
	_, e := WatchDemoReconnect(context.Background(), watchBaselineReader(&calls, 0), DemoCredentials{APIKey: "MOCK", Secret: "MOCK"}, dir, mockSince(), stableWatchObserver, 4*time.Minute)
	if e == nil || calls.Load() != 0 {
		t.Fatal("invalid duration accepted")
	}
}

func TestReconnectWatchRejectsRepeatedDropsAfterRetryLimit(t *testing.T) {
	dir := t.TempDir()
	var calls, epochs atomic.Int32
	ready := make(chan int, 3)
	obs := func(ctx context.Context, store *EventStore, connected func()) error {
		no := int(epochs.Add(1))
		if e := store.RecordDiscontinuity("STARTUP"); e != nil {
			return e
		}
		connected()
		select {
		case got := <-ready:
			if got != no {
				return errors.New("test fault injection out of epoch")
			}
			return errors.New("SECRET-KEY-DROP")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	r, e := watchDemoReconnect(context.Background(), watchBaselineReader(&calls, 0), DemoCredentials{APIKey: "MOCK", Secret: "MOCK"}, dir, mockSince(), obs, 8*time.Second, func(n int) { ready <- n })
	if e == nil || r.Stage != "BLOCKED" || r.TradingAuthorized || r.FinalRESTReviewed || r.Reconnects != 2 || len(r.Epochs) != 3 || epochs.Load() != 3 || strings.Contains(e.Error(), "SECRET") {
		t.Fatalf("retry limit unsafe %+v err %v", r, e)
	}
	reports, loadErr := LoadReconnectWatches(dir)
	if loadErr != nil || len(reports) != 1 || reports[0].Stage != "BLOCKED" {
		t.Fatalf("missing blocked evidence %+v %v", reports, loadErr)
	}
}

func TestReconnectWatchDropDuringSignedRESTFailsClosed(t *testing.T) {
	dir := t.TempDir()
	stop := make(chan struct{})
	left := make(chan struct{})
	var calls atomic.Int32
	obs := func(ctx context.Context, store *EventStore, connected func()) error {
		if e := store.RecordDiscontinuity("STARTUP"); e != nil {
			return e
		}
		connected()
		select {
		case <-stop:
			close(left)
			return errors.New("SECRET-UPSTREAM-DETAIL")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	reader := diagnosticReader(func(_ context.Context, _, _ string, since int64) (exchange.RecoveryBaseline, error) {
		calls.Add(1)
		close(stop)
		<-left
		now := time.Now().UTC()
		b := recoveryBaseline(now)
		b.StartedAt, b.FinishedAt = now.Add(-550*time.Millisecond), now.Add(-500*time.Millisecond)
		b.WindowStartMS, b.WindowEndMS = since, b.StartedAt.UnixMilli()
		return b, nil
	})
	r, e := WatchDemoReconnect(context.Background(), reader, DemoCredentials{APIKey: "MOCK", Secret: "MOCK"}, dir, mockSince(), obs, 60*time.Millisecond)
	if e == nil || r.Stage != "BLOCKED" || r.TradingAuthorized || r.Reconnects != 0 || calls.Load() != 1 || strings.Contains(e.Error(), "SECRET") {
		t.Fatalf("lost socket during signed review incorrectly recovered %+v %v", r, e)
	}
}

func TestReconnectWatchExclusiveAndInterruptedArchiveBlocksRestart(t *testing.T) {
	dir := t.TempDir()
	archive, e := openReconnectArchive(dir)
	if e != nil {
		t.Fatal(e)
	}
	r := ReconnectWatchReport{Schema: 1, ID: archive.id, Stage: "STARTED", StartedAt: time.Now().UTC(), RequestedSeconds: 30, Epochs: []ReconnectEpoch{}, Blockers: []string{}, HistoricalOnly: true, ManualReviewRequired: true}
	if e := archive.write(&r, "start"); e != nil {
		t.Fatal(e)
	}
	if _, e := openReconnectArchive(dir); e == nil {
		t.Fatal("simultaneous or crash-locked watch accepted")
	}
	if e := archive.close(); e != nil {
		t.Fatal(e)
	}
	reports, e := LoadReconnectWatches(dir)
	if e != nil || len(reports) != 1 || reports[0].Stage != "INTERRUPTED" {
		t.Fatalf("crashed START not surfaced: %+v %v", reports, e)
	}
	if _, e := openReconnectArchive(dir); e == nil {
		t.Fatal("orphaned START bypassed")
	}
}

// New watch markers must be correlated as timestamp candidates in the same
// read-only forensic API as older recovery diagnostics. Association is not
// permission to clear any previous markers.
func TestReconnectWatchGapAuditHistoricalCorrelationNeverClears(t *testing.T) {
	dir := t.TempDir()
	var calls atomic.Int32
	r, e := WatchDemoReconnect(context.Background(), watchBaselineReader(&calls, 0), DemoCredentials{APIKey: "MOCK", Secret: "MOCK"}, dir, mockSince(), stableWatchObserver, 30*time.Millisecond)
	if e != nil || r.Stage != "OBSERVED" {
		t.Fatalf("watch failed %+v %v", r, e)
	}
	audit, e := AnalyzeStreamGaps(dir, time.Now().UTC())
	if e != nil || audit.MarkerCount != 2 || audit.SessionCount != 1 || audit.OutsideHistoricalSessions != 0 || audit.Sessions[0].MarkerCount != 2 || audit.TradingAuthorized || audit.StreamContinuityProven || !audit.ManualReviewRequired {
		t.Fatalf("unsafe new-epoch provenance: %+v %v", audit, e)
	}
}
