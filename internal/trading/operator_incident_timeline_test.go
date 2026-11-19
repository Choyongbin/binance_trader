package trading

import (
	"testing"
	"time"
)

func TestOperatorIncidentTimelineEmptySealedReadOnly(t *testing.T) {
	r, err := BuildOperatorIncidentTimeline(t.TempDir(), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if r.EventCount != 0 || r.TradingAuthorized || r.AutomaticRepair || r.MutationPerformed || !r.ManualReviewRequired || r.SHA256 == "" {
		t.Fatalf("bad timeline: %+v", r)
	}
}
func TestOperatorIncidentTimelineRecoveryChronology(t *testing.T) {
	root := t.TempDir()
	s, err := OpenRecoveryProbeStore(root)
	if err != nil {
		t.Fatal(err)
	}
	start := RecoveryProbe{Schema: 1, ID: s.id, Stage: "STARTED", StartedAt: time.Unix(100, 0).UTC(), Blockers: []string{}, ManualReviewRequired: true}
	if err = s.write(&start, "start"); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := BuildOperatorIncidentTimeline(root, time.Unix(200, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Events) == 0 || r.Events[0].Kind != "recovery_probe" || r.Events[0].Severity != "CRITICAL" {
		t.Fatalf("bad events: %+v", r.Events)
	}
}
