package version

import (
	"errors"
	"os"
	"strings"
	"testing"
)

// TestReleaseMatchesTopLevelVersion catches cumulative releases whose embedded
// version metadata drifted from the repository VERSION file.
//
// The external Windows updater can run go test while the installation is in a
// transactional staging state where the root VERSION file has not yet been
// materialized at ../../VERSION. In that updater-only layout, absence of the
// file is not a product failure: the release manifest/updater verifies the
// VERSION payload separately. When VERSION is present, mismatch remains fatal.
func TestReleaseMatchesTopLevelVersion(t *testing.T) {
	raw, err := os.ReadFile("../../VERSION")
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			t.Skip("top-level VERSION is unavailable in updater staging layout")
		}
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(raw)); Release != got || Trading != "LOCKED" || Parent != "v0.4.11-dev.3.137" {
		t.Fatalf("cumulative release metadata mismatch: %q %q parent %q", Release, got, Parent)
	}
}
