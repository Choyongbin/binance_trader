package trading

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRestartForensicsFreshAndStoppedReadOnly(t *testing.T) {
	dir := t.TempDir()
	now := time.Now().UTC()
	empty, err := AnalyzeRestartForensics(dir, now)
	if err != nil || empty.RestartDisposition != "NO_MONITOR_HISTORY" || empty.MonitorSessionCount != 0 ||
		empty.TradingAuthorized || empty.AutoRearmAllowed || empty.StreamContinuityProven || !empty.ManualReviewRequired || !empty.EvidenceStable {
		t.Fatalf("fresh disk falsely approved: %+v %v", empty, err)
	}
	j, err := openMonitorJournal(dir)
	if err != nil {
		t.Fatal(err)
	}
	p := blankMonitor()
	p.State, p.Reason = MonitorUncertain, "START"
	if err := j.append(&p); err != nil {
		t.Fatal(err)
	}
	p.State, p.Reason = MonitorStopped, "OPERATOR_STOP"
	if err := j.append(&p); err != nil {
		t.Fatal(err)
	}
	if err := j.close(); err != nil {
		t.Fatal(err)
	}
	report, err := AnalyzeRestartForensics(dir, time.Now().UTC())
	if err != nil || report.RestartDisposition != "HISTORICAL_ONLY_REARM_REQUIRES_PASSPHRASE" ||
		report.MonitorSessionCount != 1 || report.MonitorCheckpointCount != 2 || report.MonitorState != MonitorStopped ||
		report.InterruptedOrActive || report.MonitorLockPresent || report.AutoRearmAllowed || report.TradingAuthorized {
		t.Fatalf("clean history falsely approved: %+v %v", report, err)
	}
	jsonRaw, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(jsonRaw), j.id) || strings.Contains(string(jsonRaw), "owner.json") {
		t.Fatal("forensic output exposes a session identifier or archive path")
	}
}

func TestRestartForensicsInterruptedSessionAndLegacyGapsStayBlocked(t *testing.T) {
	dir := t.TempDir()
	j, err := openMonitorJournal(dir)
	if err != nil {
		t.Fatal(err)
	}
	p := blankMonitor()
	p.State, p.Reason = MonitorConnected, "SIGNED_REST"
	p.ExposureSHA256 = strings.Repeat("a", 64)
	if err := j.append(&p); err != nil {
		t.Fatal(err)
	}
	s, err := OpenEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.RecordDiscontinuity("STARTUP"); err != nil {
		t.Fatal(err)
	}
	// Crash before a DISCONNECT or lock cleanup, not a clean close.
	report, err := AnalyzeRestartForensics(dir, time.Now())
	if err != nil || report.RestartDisposition != "OFFLINE_MANUAL_REVIEW_REQUIRED" ||
		!report.InterruptedOrActive || !report.MonitorLockPresent || !report.EventLockPresent ||
		report.MonitorState != MonitorUncertain || report.MonitorCheckpointCount != 1 ||
		report.GapMarkerCount != 1 || report.TimestampUnmatchedGaps != 1 || report.AutoRearmAllowed {
		t.Fatalf("crash misclassified %+v %v", report, err)
	}
	if _, e := os.Stat(j.lock); e != nil {
		t.Fatal("forensics modified monitor lock", e)
	}
	if _, e := os.Stat(s.lock); e != nil {
		t.Fatal("forensics modified event lock", e)
	}
	if _, e := openMonitorJournal(dir); e == nil {
		t.Fatal("stale monitor auto-rearmed")
	}
}

func TestRestartForensicsRejectsCorruptionRatherThanPublishingSafeStatus(t *testing.T) {
	for _, target := range []string{"checkpoint", "gap", "event_lock"} {
		t.Run(target, func(t *testing.T) {
			dir := t.TempDir()
			j, err := openMonitorJournal(dir)
			if err != nil {
				t.Fatal(err)
			}
			p := blankMonitor()
			p.State, p.Reason = MonitorBlocked, "REST_FAILED"
			if err := j.append(&p); err != nil {
				t.Fatal(err)
			}
			if err := j.close(); err != nil {
				t.Fatal(err)
			}
			switch target {
			case "checkpoint":
				err = os.WriteFile(filepath.Join(j.dir, "00000001.json"), []byte(`{"trading_authorized":true}`), 0600)
			case "gap":
				path := filepath.Join(dir, "trading", "stream_discontinuities")
				if err = os.MkdirAll(path, 0700); err == nil {
					err = os.WriteFile(filepath.Join(path, strings.Repeat("a", 32)+".json"), []byte(`{"schema":1,"reason":"INVALID","at":"2026-01-01T00:00:00Z"}`), 0600)
				}
			case "event_lock":
				path := filepath.Join(dir, "trading", "user_events", "exclusive.active")
				if err = os.MkdirAll(path, 0700); err == nil {
					err = os.WriteFile(filepath.Join(path, "unknown.json"), []byte(`{}`), 0600)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if r, err := AnalyzeRestartForensics(dir, time.Now()); err == nil || r.EvidenceStable || r.AutoRearmAllowed {
				t.Fatalf("corrupt %s accepted %+v %v", target, r, err)
			}
		})
	}
}

func TestRestartForensicsRejectsEmptyOrUnexpectedMonitorLock(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(monitorRoot(dir), "exclusive.active"), 0700); err != nil {
		t.Fatal(err)
	}
	if r, err := AnalyzeRestartForensics(dir, time.Now()); err == nil || r.EvidenceStable || r.AutoRearmAllowed {
		t.Fatalf("incomplete lock accepted %+v %v", r, err)
	}
}

func TestRestartForensicsMonitorEpochsAreTimestampCandidatesNotClearance(t *testing.T) {
	root := t.TempDir()
	j, err := openMonitorJournal(root)
	if err != nil {
		t.Fatal(err)
	}
	p := blankMonitor()
	p.State, p.Reason, p.Epoch = MonitorUncertain, "START", 0
	if err := j.append(&p); err != nil {
		t.Fatal(err)
	}
	p.State, p.Reason, p.Epoch = MonitorConnecting, "HANDSHAKE", 1
	if err := j.append(&p); err != nil {
		t.Fatal(err)
	}
	e, err := OpenEventStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.RecordDiscontinuity("STARTUP"); err != nil {
		t.Fatal(err)
	}
	p.State, p.Reason = MonitorConnected, "REST_REFRESH"
	if err = j.append(&p); err != nil {
		t.Fatal(err)
	}
	if err = e.RecordDiscontinuity("DISCONNECT"); err != nil {
		t.Fatal(err)
	}
	p.State, p.Reason = MonitorStopped, "OPERATOR_STOP"
	if err = j.append(&p); err != nil {
		t.Fatal(err)
	}
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	if err = j.close(); err != nil {
		t.Fatal(err)
	}
	report, err := AnalyzeRestartForensics(root, time.Now().UTC())
	if err != nil || len(report.MonitorEpochWindows) != 1 {
		t.Fatalf("epoch inventory %+v %v", report, err)
	}
	w := report.MonitorEpochWindows[0]
	if w.Incomplete || w.Epoch != 1 || w.SignedRESTSamples != 1 || w.TimestampCandidateMarkers != 2 ||
		w.EndReason != "OPERATOR_STOP" || report.TradingAuthorized || report.StreamContinuityProven || report.AutoRearmAllowed {
		t.Fatalf("legacy timestamp classified as safe %+v %+v", w, report)
	}
	// A manual timestamp overlap does not modify or suppress original files.
	gaps, _, err := scanGapMarkers(root)
	if err != nil || len(gaps) != 2 {
		t.Fatalf("marker mutated during inspection: %d %v", len(gaps), err)
	}
}

func TestRestartForensicsUnfinishedEpochNeverAttributesMarkers(t *testing.T) {
	root := t.TempDir()
	j, err := openMonitorJournal(root)
	if err != nil {
		t.Fatal(err)
	}
	p := blankMonitor()
	p.State, p.Reason, p.Epoch = MonitorConnecting, "HANDSHAKE", 1
	if err = j.append(&p); err != nil {
		t.Fatal(err)
	}
	e, err := OpenEventStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.RecordDiscontinuity("STARTUP"); err != nil {
		t.Fatal(err)
	}
	report, err := AnalyzeRestartForensics(root, time.Now().UTC())
	if err != nil || !report.InterruptedOrActive || len(report.MonitorEpochWindows) != 1 ||
		!report.MonitorEpochWindows[0].Incomplete || report.MonitorEpochWindows[0].TimestampCandidateMarkers != 0 {
		t.Fatalf("unfinished epoch masqueraded as completed %+v %v", report, err)
	}
}
