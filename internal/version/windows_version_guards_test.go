package version

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The offline recovery scripts are intentionally release-specific. Without
// this regression, older pinned versions silently strand forensic tooling
// after a cumulative upgrade even when its Go commands are still supported.
func TestOfflineWindowsScriptsPinCurrentCumulativeRelease(t *testing.T) {
	files := []string{
		"pc-external-close-review.ps1", "pc-demo-package-a.ps1",
		"pc-demo-close-dual-ledger.ps1", "pc-demo-close-posting-intent.ps1",
		"pc-demo-package-a-collect.ps1", "pc-demo-close-settlement-journal.ps1",
		"pc-demo-close-shadow-replay.ps1", "pc-external-close-proposal.ps1",
		"pc-demo-ledger-audit.ps1", "pc-demo-signed-inspect.ps1", "pc-demo-protection-signed-audit.ps1",
		"pc-demo-external-revalidate.ps1", "pc-demo-atomic-rehearsal.ps1",
	}
	for _, name := range files {
		t.Run(name, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join("..", "..", "scripts", name))
			if err != nil {
				t.Fatal(err)
			}
			s := string(raw)
			if !strings.Contains(s, Release) {
				t.Fatalf("script does not pin release %s", Release)
			}
			// A second, old release literal often indicates conflicting guards.
			for _, old := range []string{"v0.4.11-dev.3.100", "v0.4.11-dev.3.109", "v0.4.11-dev.3.110", "v0.4.11-dev.3.111", "v0.4.11-dev.3.112", "v0.4.11-dev.3.114", "v0.4.11-dev.3.115"} {
				if old != Release && strings.Contains(s, old) {
					t.Fatalf("stale release guard %s", old)
				}
			}
		})
	}
}
