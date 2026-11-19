package trading

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestOperatorUnifiedStatusEmptyIsHistoricalOnlyAndNeverAuthorized(t *testing.T) {
	root := t.TempDir()
	got, err := AnalyzeOperatorUnifiedStatus(root, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if got.State != OperatorStateEmpty || got.TradingAuthorized || got.StreamContinuityProven || !got.ManualReviewRequired {
		t.Fatalf("unexpected status: %+v", got)
	}
	if len(got.EvidenceSHA256) != 64 {
		t.Fatalf("bad evidence hash %q", got.EvidenceSHA256)
	}
}

func TestOperatorUnifiedStatusInterruptedRecoveryBlocks(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "trading", "recovery_sessions")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	p := RecoveryProbe{Schema: 1, ID: "0123456789abcdef0123456789abcdef", Stage: "STARTED", StartedAt: time.Now().Add(-time.Minute).UTC(), Blockers: []string{}, ManualReviewRequired: true}
	p.seal()
	body, _ := jsonMarshalIndentForTest(p)
	if err := os.WriteFile(filepath.Join(dir, p.ID+".start.json"), body, 0600); err != nil {
		t.Fatal(err)
	}
	got, err := AnalyzeOperatorUnifiedStatus(root, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if got.State != OperatorStateBlocked || got.Recovery.InterruptedCount != 1 {
		t.Fatalf("unexpected status: %+v", got)
	}
}

func jsonMarshalIndentForTest(v any) ([]byte, error) { return json.MarshalIndent(v, "", "  ") }

func writeDiagnosticOwnerForTest(t *testing.T, root, rel, id string) {
	t.Helper()
	lock := filepath.Join(root, "trading", rel, "exclusive.active")
	if err := os.MkdirAll(lock, 0700); err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"schema":1,"id":"` + id + `"}`)
	if err := os.WriteFile(filepath.Join(lock, "owner.json"), body, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestOperatorUnifiedStatusBlocksPreStartDiagnosticLocks(t *testing.T) {
	for _, tc := range []struct {
		name string
		rel  string
	}{
		{"recovery", "recovery_sessions"},
		{"reconnect", "reconnect_watch_sessions"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writeDiagnosticOwnerForTest(t, root, tc.rel, "0123456789abcdef0123456789abcdef")
			got, err := AnalyzeOperatorUnifiedStatus(root, time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}
			if got.State != OperatorStateBlocked {
				t.Fatalf("diagnostic lock not surfaced as blocker: %+v", got)
			}
		})
	}
}

func TestOperatorUnifiedStatusInterruptedReconnectBlocks(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "trading", "reconnect_watch_sessions")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	r := ReconnectWatchReport{
		Schema: 1, ID: "0123456789abcdef0123456789abcdef", Stage: "STARTED",
		StartedAt: time.Now().Add(-time.Minute).UTC(), RequestedSeconds: 30,
		Epochs: []ReconnectEpoch{}, Blockers: []string{}, HistoricalOnly: true,
		ManualReviewRequired: true,
	}
	r.seal()
	body, _ := json.MarshalIndent(r, "", "  ")
	if err := os.WriteFile(filepath.Join(dir, r.ID+".start.json"), body, 0600); err != nil {
		t.Fatal(err)
	}
	got, err := AnalyzeOperatorUnifiedStatus(root, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if got.State != OperatorStateBlocked {
		t.Fatalf("interrupted reconnect not surfaced as blocker: %+v", got)
	}
}

func TestOperatorEvidenceSealCoversReconnectMonitorEventAndGapTrees(t *testing.T) {
	root := t.TempDir()
	base, err := operatorEvidenceSHA256(root)
	if err != nil {
		t.Fatal(err)
	}
	writeDiagnosticOwnerForTest(t, root, "reconnect_watch_sessions", "0123456789abcdef0123456789abcdef")
	withReconnect, err := operatorEvidenceSHA256(root)
	if err != nil || withReconnect == base {
		t.Fatalf("reconnect tree not covered: %q %q %v", base, withReconnect, err)
	}
	if err := os.RemoveAll(filepath.Join(root, "trading", "reconnect_watch_sessions")); err != nil {
		t.Fatal(err)
	}
	j, err := openMonitorJournal(root)
	if err != nil {
		t.Fatal(err)
	}
	withMonitor, err := operatorEvidenceSHA256(root)
	if err != nil || withMonitor == base {
		t.Fatalf("monitor tree not covered: %q %q %v", base, withMonitor, err)
	}
	// Keep the intentionally active monitor lock; event/gap changes must still
	// independently perturb the seal.
	events := filepath.Join(root, "trading", "user_events")
	if err := os.MkdirAll(events, 0700); err != nil {
		t.Fatal(err)
	}
	e := AccountEvent{Kind: "ORDER_TRADE_UPDATE", EventTime: time.Now().UnixMilli(), Digest: strings.Repeat("a", 64)}
	rec := eventEnvelope{Schema: 1, Event: e}
	rec.Digest = rec.sum()
	raw, _ := json.Marshal(rec)
	if err := os.WriteFile(filepath.Join(events, e.Digest+".json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	withEvent, err := operatorEvidenceSHA256(root)
	if err != nil || withEvent == withMonitor {
		t.Fatalf("event tree not covered: %q %q %v", withMonitor, withEvent, err)
	}
	gaps := filepath.Join(root, "trading", "stream_discontinuities")
	if err := os.MkdirAll(gaps, 0700); err != nil {
		t.Fatal(err)
	}
	gapRaw := []byte(`{"schema":1,"reason":"STARTUP","at":"` + time.Now().UTC().Format(time.RFC3339Nano) + `"}`)
	if err := os.WriteFile(filepath.Join(gaps, "0123456789abcdef0123456789abcdef.json"), gapRaw, 0600); err != nil {
		t.Fatal(err)
	}
	withGap, err := operatorEvidenceSHA256(root)
	if err != nil || withGap == withEvent {
		t.Fatalf("gap tree not covered: %q %q %v", withEvent, withGap, err)
	}
	_ = j
}
