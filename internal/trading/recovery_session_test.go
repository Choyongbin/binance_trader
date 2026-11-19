package trading

import (
	"binance_trader/internal/exchange"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type diagnosticReader func(context.Context, string, string, int64) (exchange.RecoveryBaseline, error)

func (fn diagnosticReader) FetchDemoRecoveryBaseline(ctx context.Context, key, secret string, since int64) (exchange.RecoveryBaseline, error) {
	return fn(ctx, key, secret, since)
}
func neverMissingMarker(ctx context.Context, s *EventStore, onConnected func()) error {
	if err := s.RecordDiscontinuity("STARTUP"); err != nil {
		return err
	}
	onConnected()
	<-ctx.Done()
	return ctx.Err()
}
func mockSince() int64 { return time.Now().Add(-time.Hour).UnixMilli() }
func mockBaselineReader(t *testing.T) diagnosticReader {
	return func(ctx context.Context, k, secret string, since int64) (exchange.RecoveryBaseline, error) {
		if k != "demo-test-key" || secret != "demo-test-secret" || since <= 0 {
			t.Fatal("creds were not forwarded privately")
		}
		return recoveryBaseline(time.Now().UTC()), nil
	}
}
func TestProbeOverlapsStreamAndRESTArchivesOnlyLockedReview(t *testing.T) {
	root := t.TempDir()
	r, err := ProbeDemoRecovery(context.Background(), mockBaselineReader(t), DemoCredentials{APIKey: "demo-test-key", Secret: "demo-test-secret"}, root, mockSince(), neverMissingMarker)
	if err != nil || r.Stage != "REVIEWED" || !r.ObserverConnected || !r.RESTReviewed || r.TradingAuthorized || !r.ManualReviewRequired || !r.StaleAfterObservation || r.ReviewFile == "" || r.BeforeGapDigest == "" || r.AfterGapDigest == "" || r.AtStopGapDigest == r.AfterGapDigest || !issueHas(r.Blockers, "과거 진단") {
		t.Fatalf("unsafe/failed probe %+v, err %v", r, err)
	}
	sessions, e := LoadRecoveryProbes(root)
	if e != nil || len(sessions) != 1 || sessions[0].Stage != "REVIEWED" || sessions[0].TradingAuthorized || !sessions[0].StaleAfterObservation {
		t.Fatalf("bad archive %+v %v", sessions, e)
	}
	reviews, e := LoadRecoveryReviews(root)
	if e != nil || len(reviews) != 1 || reviews[0].TradingAuthorized {
		t.Fatalf("bad independent REST archive %+v %v", reviews, e)
	}
	// A finished observation must not leave either process lock behind.
	if _, e = os.Stat(filepath.Join(root, "trading", "recovery_sessions", "exclusive.active")); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("probe lock left behind", e)
	}
	if _, e = os.Stat(filepath.Join(root, "trading", "user_events", "exclusive.active")); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("event lock left behind", e)
	}
}
func TestProbeDetectsEventArrivingDuringREST(t *testing.T) {
	root := t.TempDir()
	events := make(chan *EventStore, 1)
	saved := make(chan struct{})
	observer := func(ctx context.Context, s *EventStore, connected func()) error {
		if err := s.RecordDiscontinuity("STARTUP"); err != nil {
			return err
		}
		events <- s
		connected()
		<-ctx.Done()
		return ctx.Err()
	}
	reader := diagnosticReader(func(ctx context.Context, _, _ string, _ int64) (exchange.RecoveryBaseline, error) {
		s := <-events
		e, err := ParseAccountEvent([]byte(`{"e":"ACCOUNT_UPDATE","E":1790525396365,"T":1790525396364,"a":{"m":"ORDER","B":[],"P":[]}}`))
		if err != nil {
			t.Fatal(err)
		}
		if _, err = s.Append(e); err != nil {
			t.Fatal(err)
		}
		close(saved)
		return recoveryBaseline(time.Now().UTC()), nil
	})
	r, err := ProbeDemoRecovery(context.Background(), reader, DemoCredentials{APIKey: "k", Secret: "s"}, root, mockSince(), observer)
	<-saved
	if err != nil || r.Stage != "REVIEWED" || r.BeforeEventDigest == r.AfterEventDigest || r.EventCount != 1 || !issueHas(r.Blockers, "REST 조회 중") || r.TradingAuthorized {
		t.Fatalf("missed in-flight event %+v %v", r, err)
	}
}
func TestProbeDetectsStreamLossDuringBaseline(t *testing.T) {
	root := t.TempDir()
	leave := make(chan struct{})
	observer := func(ctx context.Context, s *EventStore, connected func()) error {
		if err := s.RecordDiscontinuity("STARTUP"); err != nil {
			return err
		}
		connected()
		<-leave
		if err := s.RecordDiscontinuity("DISCONNECT"); err != nil {
			return err
		}
		return errors.New("forced disconnect")
	}
	reader := diagnosticReader(func(ctx context.Context, _, _ string, _ int64) (exchange.RecoveryBaseline, error) {
		close(leave)
		time.Sleep(15 * time.Millisecond)
		return recoveryBaseline(time.Now().UTC()), nil
	})
	r, err := ProbeDemoRecovery(context.Background(), reader, DemoCredentials{APIKey: "k", Secret: "s"}, root, mockSince(), observer)
	if err != nil || !issueHas(r.Blockers, "스트림 중단") || r.BeforeGapDigest == r.AfterGapDigest || r.TradingAuthorized {
		t.Fatalf("lost stream not flagged %+v %v", r, err)
	}
}
func TestProbeReaderErrorNoSecretsAndBlockedDurably(t *testing.T) {
	root := t.TempDir()
	key, secret := "extremely-sensitive-key", "extremely-sensitive-secret"
	reader := diagnosticReader(func(context.Context, string, string, int64) (exchange.RecoveryBaseline, error) {
		return exchange.RecoveryBaseline{}, errors.New(key + secret)
	})
	r, err := ProbeDemoRecovery(context.Background(), reader, DemoCredentials{APIKey: key, Secret: secret}, root, mockSince(), neverMissingMarker)
	if err == nil || strings.Contains(err.Error(), key) || strings.Contains(err.Error(), secret) || r.Stage != "BLOCKED" || r.TradingAuthorized {
		t.Fatalf("secret leak / unsafe probe %+v %v", r, err)
	}
	saved, e := LoadRecoveryProbes(root)
	if e != nil || len(saved) != 1 || saved[0].Stage != "BLOCKED" || saved[0].TradingAuthorized {
		t.Fatalf("blocked archive %+v %v", saved, e)
	}
	b, _ := os.ReadFile(filepath.Join(root, "trading", "recovery_sessions", r.ID+".end.json"))
	if strings.Contains(string(b), key) || strings.Contains(string(b), secret) {
		t.Fatal("credentials were archived")
	}
}
func TestRecoveryProbeUnfinishedAndTamperFailClosed(t *testing.T) {
	root := t.TempDir()
	s, err := OpenRecoveryProbeStore(root)
	if err != nil {
		t.Fatal(err)
	}
	r := RecoveryProbe{Schema: 1, ID: s.id, Stage: "STARTED", StartedAt: time.Now(), ManualReviewRequired: true, Blockers: []string{}}
	if err := s.write(&r, "start"); err != nil {
		t.Fatal(err)
	}
	if _, err = OpenRecoveryProbeStore(root); err == nil {
		t.Fatal("duplicate session lock permitted")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	v, e := LoadRecoveryProbes(root)
	if e != nil || len(v) != 1 || v[0].Stage != "INTERRUPTED" || !v[0].ManualReviewRequired {
		t.Fatalf("forgot crash %+v %v", v, e)
	}
	if _, err = OpenRecoveryProbeStore(root); err == nil {
		t.Fatal("incomplete crash record ignored")
	}
	path := filepath.Join(root, "trading", "recovery_sessions", s.id+".start.json")
	f, e := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if e != nil {
		t.Fatal(e)
	}
	_, _ = f.WriteString("x")
	_ = f.Close()
	if _, e = LoadRecoveryProbes(root); e == nil {
		t.Fatal("tampered session accepted")
	}
}
func TestRecoveryProbeDoesNotReleaseForeignLock(t *testing.T) {
	root := t.TempDir()
	s, err := OpenRecoveryProbeStore(root)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(s.lock, "owner.json")
	if err = os.WriteFile(path, []byte(`{"schema":1,"id":"other"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err == nil {
		t.Fatal("foreign lock released")
	}
	if _, e := os.Stat(s.lock); e != nil {
		t.Fatal("lock lost")
	}
}
