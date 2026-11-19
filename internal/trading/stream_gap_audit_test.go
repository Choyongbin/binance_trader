package trading

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestGapAuditReadOnlyDeduplicatesOnlySameSessionDisconnect(t *testing.T) {
	root := t.TempDir()
	s, err := OpenEventStore(root)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate real observer and its coordinator each recording the same stop.
	if err = s.RecordDiscontinuity("STARTUP"); err != nil {
		t.Fatal(err)
	}
	if err = s.RecordDiscontinuity("DISCONNECT"); err != nil {
		t.Fatal(err)
	}
	if err = s.RecordDiscontinuity("DISCONNECT"); err != nil {
		t.Fatal(err)
	}
	first, err := AnalyzeStreamGaps(root, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if first.MarkerCount != 2 || first.Reasons["STARTUP"] != 1 || first.Reasons["DISCONNECT"] != 1 || first.OutsideHistoricalSessions != 2 || first.TradingAuthorized || first.StreamContinuityProven || !first.ManualReviewRequired {
		t.Fatalf("gap count or fail-closed invariant: %+v", first)
	}
	// Even on the same store, a new explicitly marked STARTUP starts a new
	// observation epoch and MUST NOT suppress a genuinely new disconnect.
	if err = s.RecordDiscontinuity("STARTUP"); err != nil {
		t.Fatal(err)
	}
	if err = s.RecordDiscontinuity("DISCONNECT"); err != nil {
		t.Fatal(err)
	}
	inOneStore, err := AnalyzeStreamGaps(root, time.Now())
	if err != nil || inOneStore.MarkerCount != 4 {
		t.Fatalf("lost new observation epoch %+v %v", inOneStore, err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	// A new observation MUST be recorded independently even after a clean stop.
	next, err := OpenEventStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err = next.RecordDiscontinuity("DISCONNECT"); err != nil {
		t.Fatal(err)
	}
	if err = next.Close(); err != nil {
		t.Fatal(err)
	}
	after, err := AnalyzeStreamGaps(root, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if after.MarkerCount != 5 || after.Reasons["DISCONNECT"] != 3 || after.MarkerDigest == first.MarkerDigest {
		t.Fatalf("lost historical evidence: %+v", after)
	}
}

func TestLegacyHistoricalOverlapIsOnlyCandidateNotGapClearance(t *testing.T) {
	root := t.TempDir()
	ss, err := OpenRecoveryProbeStore(root)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	start := RecoveryProbe{Schema: 1, ID: ss.id, Stage: "STARTED", StartedAt: now, ManualReviewRequired: true, Blockers: []string{}}
	if err = ss.write(&start, "start"); err != nil {
		t.Fatal(err)
	}
	es, err := OpenEventStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err = es.RecordDiscontinuity("STARTUP"); err != nil {
		t.Fatal(err)
	}
	if err = es.RecordDiscontinuity("DISCONNECT"); err != nil {
		t.Fatal(err)
	}
	// Three legacy markers per review used to be persisted by independent
	// observer/coordinator stop paths; do not delete or silently reclassify them.
	// Directly write a legacy marker to model archives created by old releases.
	markerDir := filepath.Join(root, "trading", "stream_discontinuities")
	legacy := `{"schema":1,"reason":"DISCONNECT","at":"` + time.Now().UTC().Format(time.RFC3339Nano) + `"}`
	if err = os.WriteFile(filepath.Join(markerDir, strings.Repeat("a", 32)+".json"), []byte(legacy), 0600); err != nil {
		t.Fatal(err)
	}
	if err = es.Close(); err != nil {
		t.Fatal(err)
	}
	end := start
	end.Stage = "BLOCKED"
	end.FinishedAt = time.Now().UTC().Add(time.Millisecond)
	end.Blockers = []string{"과거 진단에서 미확인 상태: 거래 잠금"}
	end.StaleAfterObservation = true
	if err = ss.write(&end, "end"); err != nil {
		t.Fatal(err)
	}
	if err = ss.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := AnalyzeStreamGaps(root, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if r.MarkerCount != 3 || r.SessionCount != 1 || r.Sessions[0].MarkerCount != 3 || !r.Sessions[0].RepeatedDisconnectCandidate || r.Sessions[0].Attribution != "HISTORICAL_TIMESTAMP_OVERLAP_ONLY" || r.OutsideHistoricalSessions != 0 || r.TradingAuthorized || r.StreamContinuityProven {
		t.Fatalf("historical candidate must not unlock: %+v %v", r, err)
	}
	// Evidence corruption may not produce a convincing, 'clean' summary.
	path := filepath.Join(markerDir, strings.Repeat("a", 32)+".json")
	if err = os.WriteFile(path, []byte(legacy+`{"schema":1}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = AnalyzeStreamGaps(root, time.Now()); err == nil {
		t.Fatal("corrupted evidence accepted")
	}
}

func TestGapAuditRejectsIncompleteSessionAndUnknownMarker(t *testing.T) {
	root := t.TempDir()
	s, err := OpenRecoveryProbeStore(root)
	if err != nil {
		t.Fatal(err)
	}
	r := RecoveryProbe{Schema: 1, ID: s.id, Stage: "STARTED", StartedAt: time.Now().UTC(), ManualReviewRequired: true, Blockers: []string{}}
	if err = s.write(&r, "start"); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	audit, err := AnalyzeStreamGaps(root, time.Now())
	if err != nil || audit.IncompleteSessionCount != 1 || audit.TradingAuthorized || audit.StreamContinuityProven {
		t.Fatalf("interrupted diagnosis must remain visible %+v %v", audit, err)
	}
	markerDir := filepath.Join(root, "trading", "stream_discontinuities")
	if err = os.MkdirAll(markerDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(markerDir, strings.Repeat("b", 32)+".json"), []byte(`{"schema":1,"reason":"INVALID","at":"2026-01-01T00:00:00Z"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = AnalyzeStreamGaps(root, time.Now()); err == nil {
		t.Fatal("unrecognized reason accepted")
	}
}

func TestRecoveryObserverAndCoordinatorWriteOnlyOneStopMarker(t *testing.T) {
	root := t.TempDir()
	observer := func(ctx context.Context, store *EventStore, connected func()) error {
		if e := store.RecordDiscontinuity("STARTUP"); e != nil {
			return e
		}
		connected()
		<-ctx.Done()
		// Real observer writes DISCONNECT; recovery defer used to add a second one.
		if e := store.RecordDiscontinuity("DISCONNECT"); e != nil {
			return e
		}
		return ctx.Err()
	}
	result, e := ProbeDemoRecovery(context.Background(), mockBaselineReader(t), DemoCredentials{APIKey: "demo-test-key", Secret: "demo-test-secret"}, root, mockSince(), observer)
	if e != nil || result.Stage != "REVIEWED" {
		t.Fatalf("mock overlapping review %+v %v", result, e)
	}
	report, e := AnalyzeStreamGaps(root, time.Now().UTC())
	if e != nil || report.MarkerCount != 2 || report.Reasons["DISCONNECT"] != 1 || report.Sessions[0].MarkerCount != 2 || report.Sessions[0].RepeatedDisconnectCandidate || report.StreamContinuityProven || report.TradingAuthorized {
		t.Fatalf("extra stop marker or unsafe summary %+v %v", report, e)
	}
}
