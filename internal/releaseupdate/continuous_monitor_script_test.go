package releaseupdate

import (
	"os"
	"strings"
	"testing"
)

func TestContinuousMonitorCheckAcceptsValidContinuityCheckpoint(t *testing.T) {
	raw, err := os.ReadFile("../../scripts/check-continuous-monitor.ps1")
	if err != nil {
		t.Fatal(err)
	}
	s := string(raw)
	if strings.Contains(s, "$r.stream_continuity_proven -ne $false") {
		t.Fatal("script still rejects stream_continuity_proven=true")
	}
	for _, required := range []string{
		"$r.trading_authorized -ne $false",
		"$r.manual_review_required -ne $true",
		"$r.read_only -ne $true",
		"$r.restart_requires_passphrase -ne $true",
		"$r.continuity_scope -ne 'PROCESS_LOCAL_CHECKPOINT_TO_NOW'",
		"$r.state -ne 'CONNECTED'",
		"$r.stale_or_interrupted -ne $false",
	} {
		if !strings.Contains(s, required) {
			t.Fatalf("missing fail-closed monitor invariant %q", required)
		}
	}
}
