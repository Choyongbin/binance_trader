package trading

import (
	"os"
	"path/filepath"
	"testing"
)

// A v0.4.11-dev.3.5 immutable schema-1 START/END pair was produced by the
// previous release's public/mock ProbeDemoRecovery (no customer credentials).
// The fixture is bundled under testdata and contains no real credentials.
func TestPreviousReleaseRecoverySHACompatibility(t *testing.T) {
	fixture := os.Getenv("BINANCE_DEV35_COMPAT_FIXTURE")
	if fixture == "" {
		fixture = filepath.Join("testdata", "legacy_dev35")
	}
	probes, e := LoadRecoveryProbes(fixture)
	if e != nil || len(probes) != 1 || probes[0].Stage != "REVIEWED" || probes[0].SHA256 == "" {
		t.Fatalf("schema-1 previous release must remain valid: %+v %v", probes, e)
	}
}
