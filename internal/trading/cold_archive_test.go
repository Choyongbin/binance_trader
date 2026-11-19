package trading

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCompactDiagnosticArchivesRecoveryIsLosslessAndFailClosed(t *testing.T) {
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
	for i := 0; i < 3; i++ {
		if _, err := ArchiveRecoveryReview(dir, review); err != nil {
			t.Fatal(err)
		}
	}
	before, err := LoadRecoveryReviews(dir)
	if err != nil || len(before) != 3 {
		t.Fatalf("before %d %v", len(before), err)
	}
	out, err := CompactDiagnosticArchives(dir, 1, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if out.Archived != 2 || out.Removed != 2 || out.Bundle == "" || out.TradingAuthorized {
		t.Fatalf("bad result %+v", out)
	}
	active, err := os.ReadDir(filepath.Join(dir, "trading", "recovery_reviews"))
	if err != nil || len(active) != 1 {
		t.Fatalf("active=%d %v", len(active), err)
	}
	after, err := LoadRecoveryReviews(dir)
	if err != nil || len(after) != 3 {
		t.Fatalf("loss after compact %d %v", len(after), err)
	}
	// A second compaction is idempotent while active count is at keepRecent.
	out2, err := CompactDiagnosticArchives(dir, 1, now.Add(2*time.Second))
	if err != nil || out2.Archived != 0 {
		t.Fatalf("idempotent %+v %v", out2, err)
	}
	p := filepath.Join(dir, "trading", "cold_archives", out.Bundle)
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)/2] ^= 1
	if err := os.WriteFile(p, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRecoveryReviews(dir); err == nil {
		t.Fatal("tampered cold archive accepted")
	}
}

func TestIncidentCorrelationIDStableAndDistinct(t *testing.T) {
	a := incidentCorrelationID("execution", "bt_0123456789abcdef0123456789ab")
	b := incidentCorrelationID("execution", "bt_0123456789abcdef0123456789ab")
	c := incidentCorrelationID("protection_group", "bt_0123456789abcdef0123456789ab")
	if a == "" || a != b || a == c || len(a) != 32 {
		t.Fatalf("correlation ids %q %q %q", a, b, c)
	}
}
