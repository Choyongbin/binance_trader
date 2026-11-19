package trading

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func makeSymlinkOrSkip(t *testing.T, target, link string) {
	t.Helper()
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink unavailable on this platform: %v", err)
	}
}

func TestImmutableTradingStoresRejectSymlinkEvidence(t *testing.T) {
	t.Run("regular fill", func(t *testing.T) {
		dir := t.TempDir()
		store, err := NewOwnershipStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		target := filepath.Join(dir, "target.json")
		if err := os.WriteFile(target, []byte(`{}`), 0600); err != nil {
			t.Fatal(err)
		}
		makeSymlinkOrSkip(t, target, filepath.Join(store.fillDir, "BTCUSDT_1.json"))
		if _, err := store.readFills(); err == nil {
			t.Fatal("fill symlink accepted")
		}
	})

	t.Run("request journal", func(t *testing.T) {
		dir := t.TempDir()
		store, err := NewOwnershipStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		target := filepath.Join(dir, "target.json")
		if err := os.WriteFile(target, []byte(`{}`), 0600); err != nil {
			t.Fatal(err)
		}
		makeSymlinkOrSkip(t, target, filepath.Join(store.journalDir, "bt_"+strings.Repeat("a", 28)+".json"))
		if _, err := store.ScanJournal(); err == nil {
			t.Fatal("journal symlink accepted")
		}
	})

	t.Run("protection fill", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "protection_fills")
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		target := filepath.Join(filepath.Dir(dir), "target.json")
		if err := os.WriteFile(target, []byte(`{}`), 0600); err != nil {
			t.Fatal(err)
		}
		makeSymlinkOrSkip(t, target, filepath.Join(dir, "BTCUSDT_1.json"))
		if _, err := readProtectionFillsDir(dir); err == nil {
			t.Fatal("protection fill symlink accepted")
		}
	})

	t.Run("protection submission", func(t *testing.T) {
		dir := t.TempDir()
		store, err := OpenProtectionSubmissionStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		target := filepath.Join(dir, "target.json")
		if err := os.WriteFile(target, []byte(`{}`), 0600); err != nil {
			t.Fatal(err)
		}
		makeSymlinkOrSkip(t, target, filepath.Join(store.dir, "group-001.json"))
		if _, err := store.ReviewAllPlans(); err == nil {
			t.Fatal("protection submission symlink accepted")
		}
	})

	t.Run("user event", func(t *testing.T) {
		dir := t.TempDir()
		store, err := OpenEventStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		target := filepath.Join(dir, "target.json")
		if err := os.WriteFile(target, []byte(`{}`), 0600); err != nil {
			t.Fatal(err)
		}
		makeSymlinkOrSkip(t, target, filepath.Join(store.dir, strings.Repeat("a", 64)+".json"))
		if _, err := store.ReadAll(); err == nil {
			t.Fatal("user-event symlink accepted")
		}
	})
}
