package trading

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func monitorObserver(ctx context.Context, store *EventStore, connected func()) error {
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
func TestContinuousMonitorApprovesOnlyLiveCheckpointAndRevokesOnStop(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	status := make(chan MonitorCheckpoint, 40)
	type result struct {
		r MonitorCheckpoint
		e error
	}
	done := make(chan result, 1)
	var reads atomic.Int32
	go func() {
		r, e := ContinuousMonitor(ctx, dir, monitorObserver, func(context.Context) (string, []string, error) {
			reads.Add(1)
			return strings.Repeat("a", 64), []string{"wallet not independently certified"}, nil
		}, 15*time.Millisecond, func(p MonitorCheckpoint) { status <- p })
		done <- result{r, e}
	}()
	approved := false
	timeout := time.After(5 * time.Second)
	for !approved {
		select {
		case s := <-status:
			if s.TradingAuthorized || !s.ManualReviewRequired {
				t.Fatal("unsafe monitor status")
			}
			if s.StreamContinuityProven {
				if s.State != MonitorConnected || s.ContinuityScope != monitorContinuityScope || s.ContinuitySince.IsZero() || s.ContinuityGapDigest == "" {
					t.Fatalf("invalid live continuity checkpoint %+v", s)
				}
				approved = true
			}
		case <-timeout:
			t.Fatal("monitor did not approve live checkpoint")
		}
	}
	time.Sleep(40 * time.Millisecond)
	cancel()
	select {
	case res := <-done:
		if res.e != nil || res.r.State != MonitorStopped || res.r.TradingAuthorized || reads.Load() < 2 {
			t.Fatalf("bad stop %+v %v REST=%d", res.r, res.e, reads.Load())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("monitor would not stop")
	}
	stored, uncertain, e := ReadContinuousMonitor(dir)
	if e != nil || uncertain || stored.State != MonitorStopped || stored.TradingAuthorized || stored.StreamContinuityProven {
		t.Fatalf("unsafe persisted monitor %+v %v %v", stored, uncertain, e)
	}
	if _, e := os.Stat(filepath.Join(monitorRoot(dir), "exclusive.active")); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("clean stop kept active lock", e)
	}
}

func TestContinuousMonitorCheckpointRequiresTwoStableRefreshes(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	seen := make(chan MonitorCheckpoint, 32)
	done := make(chan struct{})
	go func() {
		_, _ = ContinuousMonitor(ctx, dir, monitorObserver, func(context.Context) (string, []string, error) {
			return strings.Repeat("c", 64), nil, nil
		}, 20*time.Millisecond, func(p MonitorCheckpoint) { seen <- p })
		close(done)
	}()
	connected := 0
	deadline := time.After(5 * time.Second)
	for connected < 2 {
		select {
		case cp := <-seen:
			if cp.State != MonitorConnected {
				continue
			}
			connected++
			if connected == 1 && cp.StreamContinuityProven {
				t.Fatal("first signed refresh must only establish a candidate")
			}
			if connected == 2 && !cp.StreamContinuityProven {
				t.Fatal("second stable refresh should approve process-local continuity")
			}
		case <-deadline:
			t.Fatal("timed out waiting for two signed refreshes")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("monitor did not stop")
	}
}

func TestContinuousMonitorReconcileEachReconnect(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var epochs, reads atomic.Int32
	firstRest := make(chan struct{})
	seen := make(chan MonitorCheckpoint, 32)
	observer := func(ctx context.Context, store *EventStore, connected func()) error {
		n := epochs.Add(1)
		if e := store.RecordDiscontinuity("STARTUP"); e != nil {
			return e
		}
		connected()
		if n == 1 {
			select {
			case <-firstRest:
			case <-ctx.Done():
				return ctx.Err()
			}
			return errors.New("SECRET-LISTEN-KEY-REDACT-ME")
		}
		<-ctx.Done()
		return ctx.Err()
	}
	type result struct {
		r MonitorCheckpoint
		e error
	}
	finished := make(chan result, 1)
	go func() {
		r, e := ContinuousMonitor(ctx, dir, observer, func(context.Context) (string, []string, error) {
			if reads.Add(1) == 1 {
				close(firstRest)
			}
			return strings.Repeat("b", 64), []string{}, nil
		}, 250*time.Millisecond, func(p MonitorCheckpoint) { seen <- p })
		finished <- result{r, e}
	}()
	wait := time.After(5 * time.Second)
	connectedAfterReconnect := false
	approvedAfterReconnect := false
	uncertainSeen := false
	for !approvedAfterReconnect {
		select {
		case s := <-seen:
			if s.State == MonitorUncertain && s.Reason == "SOCKET_DROPPED" {
				uncertainSeen = true
			}
			if s.State == MonitorConnected && s.Epoch == 2 {
				if !connectedAfterReconnect {
					connectedAfterReconnect = true
					if s.StreamContinuityProven {
						t.Fatal("reconnect epoch inherited old continuity checkpoint")
					}
				} else if s.StreamContinuityProven {
					approvedAfterReconnect = true
				}
			}
		case <-wait:
			t.Fatal("reconnect did not establish a fresh second-epoch checkpoint")
		}
	}
	if !uncertainSeen || epochs.Load() != 2 || reads.Load() < 1 {
		t.Fatalf("reconnect %v epochs=%d reads=%d", uncertainSeen, epochs.Load(), reads.Load())
	}
	cancel()
	select {
	case res := <-finished:
		if res.e != nil || res.r.Reconnects != 1 || res.r.State != MonitorStopped {
			t.Fatalf("unexpected stop %+v %v", res.r, res.e)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("timeout")
	}
}
func TestContinuousMonitorCrashAndTamperFailClosed(t *testing.T) {
	dir := t.TempDir()
	j, e := openMonitorJournal(dir)
	if e != nil {
		t.Fatal(e)
	}
	p := blankMonitor()
	p.State = MonitorConnected
	p.Reason = "SIGNED_REST"
	p.ExposureSHA256 = strings.Repeat("a", 64)
	if e = j.append(&p); e != nil {
		t.Fatal(e)
	}
	view, uncertain, e := ReadContinuousMonitor(dir)
	if e != nil || !uncertain || view.State != MonitorUncertain || view.ExposureSHA256 != "" {
		t.Fatalf("stale connection shown ready %+v %v %v", view, uncertain, e)
	}
	if _, e = openMonitorJournal(dir); e == nil {
		t.Fatal("crash lock silently recovered")
	}
	path := filepath.Join(j.dir, "00000001.json")
	b, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	b = append(b, []byte("\n{}")...)
	if e = os.WriteFile(path, b, 0600); e != nil {
		t.Fatal(e)
	}
	if _, _, e = ReadContinuousMonitor(dir); e == nil {
		t.Fatal("tampered archive accepted")
	}
}
func TestContinuousMonitorConservativeRESTFailure(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r, e := ContinuousMonitor(ctx, dir, monitorObserver, func(context.Context) (string, []string, error) { return "", nil, errors.New("SECRET-HTTP-URL") }, 15*time.Millisecond, nil)
	if e == nil || r.State != MonitorBlocked || r.TradingAuthorized || strings.Contains(e.Error(), "SECRET") {
		t.Fatalf("REST failure accepted/leaked %+v %v", r, e)
	}
	archived, uncertain, readErr := ReadContinuousMonitor(dir)
	if readErr != nil || uncertain || archived.State != MonitorBlocked {
		t.Fatalf("bad persisted failure %+v %v %v", archived, uncertain, readErr)
	}
}

func TestLiveMonitorContinuityFreshnessAndHistoricalSanitization(t *testing.T) {
	now := time.Now().UTC()
	cp := MonitorCheckpoint{
		Schema: 1, State: MonitorConnected, Epoch: 1, Issues: []string{}, ManualReviewRequired: true,
		StreamContinuityProven: true, ContinuityScope: monitorContinuityScope,
		ContinuitySince: now.Add(-time.Minute), ContinuityGapDigest: strings.Repeat("d", 64),
		GapDigest: strings.Repeat("d", 64), LastSignedRESTAt: now.Add(-time.Minute),
	}
	if !LiveMonitorContinuityProven(cp, now) {
		t.Fatal("fresh live checkpoint not recognized")
	}
	cp.LastSignedRESTAt = now.Add(-MonitorContinuityFreshness - time.Second)
	if LiveMonitorContinuityProven(cp, now) {
		t.Fatal("stale checkpoint remained live")
	}
	cp.LastSignedRESTAt = now
	h := monitorStatusHistorical(cp)
	if h.StreamContinuityProven || h.ContinuityScope != "" || !h.ContinuitySince.IsZero() || h.ContinuityGapDigest != "" {
		t.Fatalf("historical status retained live continuity: %+v", h)
	}
}

func TestContinuousMonitorCrashRevokesPersistedContinuity(t *testing.T) {
	dir := t.TempDir()
	j, err := openMonitorJournal(dir)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	cp := MonitorCheckpoint{
		State: MonitorConnected, Reason: "REST_REFRESH", Epoch: 1, Issues: []string{}, ManualReviewRequired: true,
		StreamContinuityProven: true, ContinuityScope: monitorContinuityScope,
		ContinuitySince: now.Add(-time.Second), ContinuityGapDigest: strings.Repeat("e", 64),
		GapDigest: strings.Repeat("e", 64), LastSignedRESTAt: now, ExposureSHA256: strings.Repeat("f", 64),
	}
	if err := j.append(&cp); err != nil {
		t.Fatal(err)
	}
	view, uncertain, err := ReadContinuousMonitor(dir)
	if err != nil || !uncertain || view.StreamContinuityProven || view.State != MonitorUncertain {
		t.Fatalf("crashed live checkpoint not revoked: %+v uncertain=%v err=%v", view, uncertain, err)
	}
}

func TestMonitorSchema1DigestBackwardCompatible(t *testing.T) {
	at := time.Date(2026, 9, 30, 15, 0, 0, 123000000, time.UTC)
	cp := MonitorCheckpoint{
		Schema: 1, SessionID: strings.Repeat("a", 32), Sequence: 1, At: at,
		State: MonitorConnected, Epoch: 1, Reconnects: 0, LastSignedRESTAt: at,
		ExposureSHA256: strings.Repeat("b", 64), GapDigest: strings.Repeat("c", 64),
		Issues: []string{"legacy"}, Reason: "REST_REFRESH", ManualReviewRequired: true,
	}
	legacy := struct {
		Schema                 int       `json:"schema"`
		SessionID              string    `json:"session_id"`
		Sequence               uint64    `json:"sequence"`
		PreviousHash           string    `json:"previous_hash"`
		At                     time.Time `json:"at"`
		State                  string    `json:"state"`
		Epoch                  int       `json:"epoch"`
		Reconnects             int       `json:"reconnects"`
		LastSignedRESTAt       time.Time `json:"last_signed_rest_at,omitempty"`
		ExposureSHA256         string    `json:"exposure_sha256,omitempty"`
		GapDigest              string    `json:"gap_digest,omitempty"`
		Issues                 []string  `json:"issues"`
		Reason                 string    `json:"reason"`
		TradingAuthorized      bool      `json:"trading_authorized"`
		StreamContinuityProven bool      `json:"stream_continuity_proven"`
		ManualReviewRequired   bool      `json:"manual_review_required"`
		SHA256                 string    `json:"sha256"`
	}{cp.Schema, cp.SessionID, cp.Sequence, cp.PreviousHash, cp.At, cp.State, cp.Epoch, cp.Reconnects,
		cp.LastSignedRESTAt, cp.ExposureSHA256, cp.GapDigest, cp.Issues, cp.Reason, false, false, true, ""}
	body, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(body)
	want := hex.EncodeToString(h[:])
	if got := cp.digest(); got != want {
		t.Fatalf("schema1 digest changed: got %s want %s", got, want)
	}
}
