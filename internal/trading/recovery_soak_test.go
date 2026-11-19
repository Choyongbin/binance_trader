package trading

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

// Two independent exchange snapshots are simulated, never a fake trade permit.
func soakReader(t *testing.T, count *int, failSecond bool, change bool) diagnosticReader {
	return func(_ context.Context, _, _ string, since int64) (exchange.RecoveryBaseline, error) {
		*count++
		if *count == 2 && failSecond {
			return exchange.RecoveryBaseline{}, errors.New("SECRET-UPSTREAM-DETAIL")
		}
		n := time.Now().UTC()
		b := recoveryBaseline(n)
		if *count == 1 {
			b.StartedAt = n.Add(-500 * time.Millisecond)
			b.FinishedAt = n.Add(-400 * time.Millisecond)
		} else {
			b.StartedAt = n.Add(-200 * time.Millisecond)
			b.FinishedAt = n.Add(-100 * time.Millisecond)
		}
		b.WindowEndMS = b.StartedAt.UnixMilli()
		b.WindowStartMS = since
		if *count == 2 && change {
			b.Portfolio.CanTrade = false
		}
		return b, nil
	}
}
func TestReadOnlySoakArchivesTwoSignedRESTAndSingleStop(t *testing.T) {
	dir := t.TempDir()
	calls := 0
	r, err := ProbeDemoRecoveryWithSoak(context.Background(), soakReader(t, &calls, false, false), DemoCredentials{APIKey: "mock", Secret: "mock"}, dir, mockSince(), neverMissingMarker, 20*time.Millisecond)
	if err != nil || r.Stage != "REVIEWED" || calls != 2 || !r.SoakCompleted || !r.SecondRESTReviewed || r.FirstExposureSHA256 == "" || r.FirstExposureSHA256 != r.SecondExposureSHA256 || r.TradingAuthorized || !r.ManualReviewRequired || !r.StaleAfterObservation {
		t.Fatalf("unsafe finite soak %+v err %v calls %d", r, err, calls)
	}
	out, e := LoadRecoveryProbes(dir)
	if e != nil || len(out) != 1 || out[0].SHA256 != r.SHA256 {
		t.Fatalf("invalid immutable soak %v %+v", e, out)
	}
	gaps, e := scanGapReasonsForTest(dir)
	if e != nil || gaps["STARTUP"] != 1 || gaps["DISCONNECT"] != 1 {
		t.Fatalf("wrong stream markers %+v %v", gaps, e)
	}
	if _, e := os.Stat(filepath.Join(dir, "trading", "recovery_sessions", "exclusive.active")); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("soak lock persisted", e)
	}
}
func scanGapReasonsForTest(dir string) (map[string]int, error) {
	report, e := AnalyzeStreamGaps(dir, time.Now().UTC())
	return report.Reasons, e
}
func TestReadOnlySoakFailClosedWhenSecondRESTFails(t *testing.T) {
	dir := t.TempDir()
	calls := 0
	r, e := ProbeDemoRecoveryWithSoak(context.Background(), soakReader(t, &calls, true, false), DemoCredentials{APIKey: "mock", Secret: "mock"}, dir, mockSince(), neverMissingMarker, 5*time.Millisecond)
	if e == nil || r.Stage != "BLOCKED" || r.TradingAuthorized || r.SecondRESTReviewed || r.SoakCompleted || calls != 2 {
		t.Fatalf("second failure accepted %+v %v", r, e)
	}
	if strings.Contains(e.Error(), "SECRET") {
		t.Fatal("error exposed upstream detail")
	}
	loaded, e := LoadRecoveryProbes(dir)
	if e != nil || len(loaded) != 1 || loaded[0].Stage != "BLOCKED" {
		t.Fatalf("failed to archive blocked run %+v %v", loaded, e)
	}
}
func TestReadOnlySoakReportsExposureChangeWithoutAuthorizing(t *testing.T) {
	dir := t.TempDir()
	calls := 0
	r, e := ProbeDemoRecoveryWithSoak(context.Background(), soakReader(t, &calls, false, true), DemoCredentials{APIKey: "mock", Secret: "mock"}, dir, mockSince(), neverMissingMarker, 5*time.Millisecond)
	if e != nil || !r.SoakCompleted || r.FirstExposureSHA256 == r.SecondExposureSHA256 || !issueHas(r.Blockers, "지문 불일치") || r.TradingAuthorized {
		t.Fatalf("exposure change was ignored %+v %v", r, e)
	}
}
func TestReadOnlySoakRequiresConnectedEpochAndNoAutoReconnect(t *testing.T) {
	dir := t.TempDir()
	calls := 0
	observe := func(ctx context.Context, store *EventStore, onConnected func()) error {
		if e := store.RecordDiscontinuity("STARTUP"); e != nil {
			return e
		}
		onConnected()
		time.Sleep(5 * time.Millisecond)
		return errors.New("mock injected socket drop")
	}
	r, e := ProbeDemoRecoveryWithSoak(context.Background(), soakReader(t, &calls, false, false), DemoCredentials{APIKey: "mock", Secret: "mock"}, dir, mockSince(), observe, 40*time.Millisecond)
	if e == nil || r.Stage != "BLOCKED" || r.SoakCompleted || calls != 1 || r.TradingAuthorized {
		t.Fatalf("disconnected observer silently accepted %+v %v", r, e)
	}
	reports, err := LoadRecoveryProbes(dir)
	if err != nil || len(reports) != 1 || reports[0].Stage != "BLOCKED" {
		t.Fatalf("bad archive %+v %v", reports, err)
	}
}

// The original Windows failure was not a socket failure: the coordinator
// consumed the only completion message during REST/review, then ignored it
// during the dwell and incorrectly archived REVIEWED. Keep this regression
// deterministic by dropping the mock stream during the first REST read.
func TestReadOnlySoakDoesNotConsumeEarlyDisconnectAndContinue(t *testing.T) {
	dir := t.TempDir()
	stop := make(chan struct{})
	observerLeft := make(chan struct{})
	observer := func(ctx context.Context, store *EventStore, connected func()) error {
		if err := store.RecordDiscontinuity("STARTUP"); err != nil {
			return err
		}
		connected()
		select {
		case <-stop:
			defer close(observerLeft)
			// An injected observer might forget its own DISCONNECT marker.
			// The coordinator must still treat a completed observer as a
			// failed epoch without silently attempting a second REST read.
			return errors.New("SECRET-IMMEDIATE-SOCKET-LOSS")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	calls := 0
	reader := diagnosticReader(func(_ context.Context, _, _ string, since int64) (exchange.RecoveryBaseline, error) {
		calls++
		if calls == 1 {
			close(stop)
			<-observerLeft
			// Give the observer wrapper time to place its terminal result
			// in the one-slot channel, exercising the old drained-channel bug.
			time.Sleep(10 * time.Millisecond)
		}
		now := time.Now().UTC()
		b := recoveryBaseline(now)
		// Two signed reads must be chronologically valid. Otherwise the
		// old bug can be hidden by an unrelated timestamp validation error.
		if calls == 1 {
			b.StartedAt, b.FinishedAt = now.Add(-500*time.Millisecond), now.Add(-400*time.Millisecond)
		} else {
			b.StartedAt, b.FinishedAt = now.Add(-200*time.Millisecond), now.Add(-100*time.Millisecond)
		}
		b.WindowStartMS, b.WindowEndMS = since, b.StartedAt.UnixMilli()
		return b, nil
	})
	r, err := ProbeDemoRecoveryWithSoak(context.Background(), reader, DemoCredentials{APIKey: "mock", Secret: "mock"}, dir, mockSince(), observer, 40*time.Millisecond)
	if err == nil || r.Stage != "BLOCKED" || r.SoakCompleted || r.TradingAuthorized || calls != 1 {
		t.Fatalf("observer completion consumed, soak incorrectly reviewed: %+v err=%v calls=%d", r, err, calls)
	}
	if strings.Contains(err.Error(), "SECRET") || issueHas(r.Blockers, "SECRET") {
		t.Fatal("socket error included a secret upstream detail")
	}
	if archived, e := LoadRecoveryProbes(dir); e != nil || len(archived) != 1 || archived[0].Stage != "BLOCKED" || archived[0].SHA256 != r.SHA256 {
		t.Fatalf("early socket exit not safely archived: %+v error=%v", archived, e)
	}
}

func TestReadOnlySoakDisconnectDuringSecondSignedRESTIsBlocked(t *testing.T) {
	dir := t.TempDir()
	stop := make(chan struct{})
	observerLeft := make(chan struct{})
	observer := func(ctx context.Context, store *EventStore, connected func()) error {
		if err := store.RecordDiscontinuity("STARTUP"); err != nil {
			return err
		}
		connected()
		select {
		case <-stop:
			defer close(observerLeft)
			return errors.New("mock WS disconnect during signed read")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	calls := 0
	reader := diagnosticReader(func(_ context.Context, _, _ string, since int64) (exchange.RecoveryBaseline, error) {
		calls++
		if calls == 2 {
			close(stop)
			<-observerLeft
			time.Sleep(2 * time.Millisecond)
		}
		now := time.Now().UTC()
		b := recoveryBaseline(now)
		// Two signed reads must be chronologically valid. Otherwise the
		// old bug can be hidden by an unrelated timestamp validation error.
		if calls == 1 {
			b.StartedAt, b.FinishedAt = now.Add(-500*time.Millisecond), now.Add(-400*time.Millisecond)
		} else {
			b.StartedAt, b.FinishedAt = now.Add(-200*time.Millisecond), now.Add(-100*time.Millisecond)
		}
		b.WindowStartMS, b.WindowEndMS = since, b.StartedAt.UnixMilli()
		return b, nil
	})
	r, err := ProbeDemoRecoveryWithSoak(context.Background(), reader, DemoCredentials{APIKey: "mock", Secret: "mock"}, dir, mockSince(), observer, 5*time.Millisecond)
	if err == nil || r.Stage != "BLOCKED" || r.SoakCompleted || r.TradingAuthorized || calls != 2 {
		t.Fatalf("second signed read outlived disconnected epoch: %+v err=%v calls=%d", r, err, calls)
	}
	if archived, e := LoadRecoveryProbes(dir); e != nil || len(archived) != 1 || archived[0].Stage != "BLOCKED" {
		t.Fatalf("second-read disconnect not archived: %+v error=%v", archived, e)
	}
}

func TestReadOnlySoakUnexpectedGapWithoutObserverExitIsBlocked(t *testing.T) {
	dir := t.TempDir()
	var store *EventStore
	observer := func(ctx context.Context, s *EventStore, connected func()) error {
		if err := s.RecordDiscontinuity("STARTUP"); err != nil {
			return err
		}
		store = s // read only after onConnected channel receive, Go channel synchronizes
		connected()
		<-ctx.Done()
		return ctx.Err()
	}
	calls := 0
	reader := diagnosticReader(func(_ context.Context, _, _ string, since int64) (exchange.RecoveryBaseline, error) {
		calls++
		if calls == 1 {
			if err := store.RecordDiscontinuity("ROTATE"); err != nil {
				return exchange.RecoveryBaseline{}, err
			}
		}
		b := recoveryBaseline(time.Now().UTC())
		b.WindowStartMS = since
		return b, nil
	})
	r, err := ProbeDemoRecoveryWithSoak(context.Background(), reader, DemoCredentials{APIKey: "mock", Secret: "mock"}, dir, mockSince(), observer, 20*time.Millisecond)
	if err == nil || r.Stage != "BLOCKED" || r.SoakCompleted || r.TradingAuthorized || calls != 1 {
		t.Fatalf("mid-epoch gap marker ignored: %+v err=%v calls=%d", r, err, calls)
	}
}

func TestReadOnlySoakUnexpectedObserverResultOnShutdownIsBlocked(t *testing.T) {
	dir := t.TempDir()
	observer := func(ctx context.Context, store *EventStore, connected func()) error {
		if err := store.RecordDiscontinuity("STARTUP"); err != nil {
			return err
		}
		connected()
		<-ctx.Done()
		return errors.New("unexpected stream teardown failure")
	}
	calls := 0
	r, err := ProbeDemoRecoveryWithSoak(context.Background(), soakReader(t, &calls, false, false), DemoCredentials{APIKey: "mock", Secret: "mock"}, dir, mockSince(), observer, 5*time.Millisecond)
	if err == nil || r.Stage != "BLOCKED" || r.SoakCompleted || r.TradingAuthorized || calls != 2 {
		t.Fatalf("unexpected observer result after cancel accepted: %+v err=%v calls=%d", r, err, calls)
	}
	if archived, e := LoadRecoveryProbes(dir); e != nil || len(archived) != 1 || archived[0].Stage != "BLOCKED" {
		t.Fatalf("teardown failure not archived: %+v error=%v", archived, e)
	}
}
