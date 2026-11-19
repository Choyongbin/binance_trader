package trading

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadOnlyProtectionSubmissionDirectoryAudit(t *testing.T) {
	dir := t.TempDir()
	r, err := ReviewProtectionSubmissionDirectory(dir)
	if err != nil || r.State != ProtectionArmingEmpty || r.TradingAuthorized || !r.ManualReviewRequired {
		t.Fatalf("empty read-only audit: %+v %v", r, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "trading")); !os.IsNotExist(err) {
		t.Fatalf("read-only audit created data tree: %v", err)
	}

	root := filepath.Join(dir, "trading", "protection_submissions")
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "unexpected.txt"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReviewProtectionSubmissionDirectory(dir); err == nil {
		t.Fatal("unknown protection archive file accepted")
	}
}
