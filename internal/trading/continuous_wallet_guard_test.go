package trading

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestActiveOrStaleContinuousMonitorLockBlocksWalletEdits(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "trading", "continuous_monitor", "exclusive.active")
	if e := os.MkdirAll(p, 0700); e != nil {
		t.Fatal(e)
	}
	if _, _, e := CheckWalletEditHistory(dir); e == nil || !strings.Contains(e.Error(), "상시") {
		t.Fatalf("history audit ignored empty monitor lock: %v", e)
	}
	if e := CheckWalletEditQuietState(dir); e == nil {
		t.Fatal("quiet wallet guard ignored empty monitor lock")
	}
}
