//go:build windows

package trading

import "testing"

func TestAutoControlDirectorySyncWindowsDoesNotRequireDirectoryFlush(t *testing.T) {
	dir := t.TempDir()
	if err := syncAutoControlDirectory(dir); err != nil {
		t.Fatalf("windows auto-control directory durability helper: %v", err)
	}
}
