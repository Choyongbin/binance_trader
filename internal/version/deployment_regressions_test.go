package version

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Static contract tests run in Go CI when PowerShell is unavailable. They do
// not replace an actual PowerShell 5.1 smoke test on a Windows machine.
func TestReviewedUpdateAuthenticatesBeforeExecutingArchiveCode(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "scripts", "pc-reviewed-update.ps1"))
	if err != nil {
		t.Fatal(err)
	}
	s := string(raw)
	required := []string{
		"[Parameter(Mandatory=$true)][ValidatePattern('^[0-9a-fA-F]{64}$')][string]$ExpectedSHA256",
		"Copy-Item -LiteralPath $pkg -Destination $stagedPkg",
		"Get-FileHash -LiteralPath $stagedPkg -Algorithm SHA256",
		"if (-not [string]::Equals($actual, $ExpectedSHA256",
		"Expand-Archive -LiteralPath $stagedPkg",
		"& $reviewedUpdater",
	}
	prev := -1
	for _, token := range required {
		at := strings.Index(s, token)
		if at < 0 {
			t.Fatalf("missing trust-boundary step %q", token)
		}
		if at <= prev {
			t.Fatalf("trust-boundary step out of order: %q", token)
		}
		prev = at
	}
	if strings.Contains(s, "Invoke-Expression") || strings.Contains(s, "ExecutionPolicy Bypass") {
		t.Fatal("reviewed updater must not bypass script validation")
	}
}

func TestRetiredDemoOneOrderCannotRunFromTonightScript(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "scripts", "pc-tonight-demo.ps1"))
	if err != nil {
		t.Fatal(err)
	}
	s := string(raw)
	if !strings.Contains(s, "$expected = '"+Release+"'") {
		t.Fatal("obsolete tonight release guard")
	}
	if !strings.Contains(s, "if ($DemoOneOrder -or $AcceptNoTPSL) { throw") {
		t.Fatal("unsafe order opt-in no longer blocked")
	}
	if strings.Contains(s, "'-mode','demo-single'") || strings.Contains(s, "DEMO_ONE_ORDER_NO_TPSL") {
		t.Fatal("unprotected demo order still reachable through tonight script")
	}
	if !strings.Contains(s, "'-mode','market'") {
		t.Fatal("safe market-only mode removed")
	}
}

func TestPhaseMajorAgreesWithPhaseLabel(t *testing.T) {
	if PhaseMajor != 4 || !strings.HasPrefix(Phase, "Phase 4.") {
		t.Fatalf("inconsistent phase major %d / label %s", PhaseMajor, Phase)
	}
}
