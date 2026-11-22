package releaseupdate

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func fixture(t *testing.T, root string, files map[string]string) {
	t.Helper()
	var lines []string
	for name, body := range files {
		path := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		h := sha256.Sum256([]byte(body))
		lines = append(lines, fmt.Sprintf("%s  %s\n", hex.EncodeToString(h[:]), name))
	}
	manifest := filepath.Join(root, filepath.FromSlash(manifestFile))
	if err := os.MkdirAll(filepath.Dir(manifest), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifest, []byte("# fixture\n"+strings.Join(lines, "")), 0600); err != nil {
		t.Fatal(err)
	}
}
func fixtureOpts(t *testing.T) (Options, string, string) {
	t.Helper()
	parent := t.TempDir()
	src := filepath.Join(parent, "source")
	dst := filepath.Join(parent, "existing")
	for _, d := range []string{src, dst} {
		if err := os.Mkdir(d, 0700); err != nil {
			t.Fatal(err)
		}
	}
	fixture(t, src, map[string]string{"VERSION": "new", "go.mod": "module x", "internal/g.go": "package g"})
	fixture(t, dst, map[string]string{"VERSION": "old", "go.mod": "module x"})
	if err := os.MkdirAll(filepath.Join(dst, "data", "credentials"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dst, "data", "credentials", "demo_secret_key.enc"), []byte("SECRET-MUST-SURVIVE"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dst, ".git"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dst, ".git", "HEAD"), []byte("local branch"), 0600); err != nil {
		t.Fatal(err)
	}
	return Options{Source: src, Target: dst, portCheck: func() error { return nil }}, src, dst
}
func read(t *testing.T, p string) string {
	t.Helper()
	b, e := os.ReadFile(p)
	if e != nil {
		t.Fatal(e)
	}
	return string(b)
}

func TestDryRunAndFullBackupPreserveUserState(t *testing.T) {
	o, _, dst := fixtureOpts(t)
	plan, err := Run(o)
	if err != nil || plan.Installed || plan.BackedUp || read(t, filepath.Join(dst, "VERSION")) != "old" {
		t.Fatalf("dry run changed install: %+v, %v", plan, err)
	}
	o.Apply = true
	plan, err = Run(o)
	if err != nil || !plan.Installed || !plan.BackedUp || plan.Backup == "" {
		t.Fatalf("apply: %+v %v", plan, err)
	}
	if read(t, filepath.Join(dst, "VERSION")) != "new" || read(t, filepath.Join(plan.Backup, "VERSION")) != "old" {
		t.Fatal("release/backup mismatch")
	}
	for _, base := range []string{dst, plan.Backup} {
		if read(t, filepath.Join(base, "data", "credentials", "demo_secret_key.enc")) != "SECRET-MUST-SURVIVE" {
			t.Fatal("credentials lost")
		}
		if read(t, filepath.Join(base, ".git", "HEAD")) != "local branch" {
			t.Fatal("git state lost")
		}
	}
	if !strings.HasPrefix(plan.Backup, dst+".backup-") {
		t.Fatal("backup inside target or outside sibling")
	}
}
func TestSourceTamperOrUnlistedFileFailsWithoutChangingTarget(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(string) error
	}{
		{"tamper", func(src string) error { return os.WriteFile(filepath.Join(src, "VERSION"), []byte("evil"), 0600) }},
		{"unlisted", func(src string) error { return os.WriteFile(filepath.Join(src, "unlisted.go"), []byte("bad"), 0600) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o, src, dst := fixtureOpts(t)
			o.Apply = true
			if err := tc.mutate(src); err != nil {
				t.Fatal(err)
			}
			if _, err := Run(o); err == nil {
				t.Fatal("tampered release accepted")
			}
			if read(t, filepath.Join(dst, "VERSION")) != "old" {
				t.Fatal("target changed")
			}
		})
	}
}
func TestModifiedOldReleaseRefusesOverwrite(t *testing.T) {
	o, _, dst := fixtureOpts(t)
	if err := os.WriteFile(filepath.Join(dst, "VERSION"), []byte("custom-user-edit"), 0600); err != nil {
		t.Fatal(err)
	}
	o.Apply = true
	if _, err := Run(o); err == nil {
		t.Fatal("modified existing source overwritten")
	}
	if read(t, filepath.Join(dst, "VERSION")) != "custom-user-edit" {
		t.Fatal("modified data touched")
	}
}
func TestStaleRuntimeRecordFailsClosed(t *testing.T) {
	o, _, dst := fixtureOpts(t)
	record := filepath.Join(dst, "data", "runtime", "server-process.json")
	if err := os.MkdirAll(filepath.Dir(record), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(record, []byte("stale"), 0600); err != nil {
		t.Fatal(err)
	}
	o.Apply = true
	if _, err := Run(o); err == nil {
		t.Fatal("stale PID record bypassed")
	}
}
func TestInjectedPostBackupFailureDoesNotModifyExistingTarget(t *testing.T) {
	o, _, dst := fixtureOpts(t)
	o.Apply = true
	o.afterBackup = func() error { return errors.New("simulated failure") }
	plan, err := Run(o)
	if err == nil || !plan.BackedUp || plan.Installed {
		t.Fatalf("failure injection incorrectly applied: %+v %v", plan, err)
	}
	if read(t, filepath.Join(dst, "VERSION")) != "old" || read(t, filepath.Join(plan.Backup, "VERSION")) != "old" {
		t.Fatal("corrupt post-backup state")
	}
}
func TestNewInstallAndDirectorySeparation(t *testing.T) {
	o, src, _ := fixtureOpts(t)
	o.Target = filepath.Join(filepath.Dir(src), "new-install")
	if _, err := Run(o); err == nil {
		t.Fatal("new target without explicit new flag")
	}
	o.NewInstall = true
	if _, err := Run(o); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(o.Target); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("dry-run created target")
	}
	o.Apply = true
	plan, err := Run(o)
	if err != nil || !plan.Installed || plan.BackedUp || read(t, filepath.Join(o.Target, "VERSION")) != "new" {
		t.Fatalf("new install: %+v %v", plan, err)
	}
	o.Target = filepath.Join(src, "nested")
	if _, err := Run(o); err == nil {
		t.Fatal("nested update target allowed")
	}
}
func TestRejectSymlinksAndTraversal(t *testing.T) {
	if _, err := safeRel("../data/demo.enc"); err == nil {
		t.Fatal("traversal accepted")
	}
	if _, err := safeRel("data/secret.enc"); err == nil {
		t.Fatal("data inclusion accepted")
	}
	if _, err := safeRel("A/../file"); err == nil {
		t.Fatal("hidden traversal accepted")
	}
	if _, err := safeRel("docs\\..\\steal"); err == nil {
		t.Fatal("backslash accepted")
	}
	o, src, _ := fixtureOpts(t)
	if err := os.Symlink(filepath.Join(src, "VERSION"), filepath.Join(src, "unlisted-link")); err != nil {
		t.Skip(err)
	}
	if _, err := Run(o); err == nil {
		t.Fatal("symlink in release accepted")
	}
}
func TestInstallerNeverDeletesExistingSourceFiles(t *testing.T) {
	o, src, _ := fixtureOpts(t)
	if err := os.Remove(filepath.Join(src, "internal", "g.go")); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(o); err == nil {
		t.Fatal("missing tracked file accepted")
	}
}

func TestUnknownGoFilesBlockExistingUpdate(t *testing.T) {
	o, _, dst := fixtureOpts(t)
	p := filepath.Join(dst, "internal", "untracked.go")
	if err := os.MkdirAll(filepath.Dir(p), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("package x"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(o); err == nil {
		t.Fatal("unchecked historical Go file allowed to mix with new release")
	}
}

func TestMissingTopLevelVersionMarkerIsTheOnlyRepairableHistoricalDefect(t *testing.T) {
	o, src, dst := fixtureOpts(t)
	// Replace the generic fixture with a realistic version.go whose Release can
	// reconstruct the tracked VERSION bytes exactly.
	body := "package version\nconst Release = \"old\"\n"
	newBody := "package version\nconst Release = \"new\"\n"
	fixture(t, src, map[string]string{
		"VERSION":                     "new",
		"go.mod":                      "module x",
		"internal/g.go":               "package g",
		"internal/version/version.go": newBody,
	})
	if err := os.MkdirAll(filepath.Join(dst, "internal", "version"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dst, "internal", "version", "version.go"), []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	// Rebuild the old manifest so version.go is itself trusted and VERSION is
	// tracked as "old\n". This models the legacy Windows installer defect.
	fixture(t, dst, map[string]string{
		"VERSION":                     "old\n",
		"go.mod":                      "module x",
		"internal/version/version.go": body,
	})
	if err := os.Remove(filepath.Join(dst, "VERSION")); err != nil {
		t.Fatal(err)
	}
	plan, err := Run(o)
	if err != nil || !plan.VersionMarkerMissing || plan.Installed {
		t.Fatalf("dry-run should accept only reconstructible missing VERSION: %+v %v", plan, err)
	}
	o.Apply = true
	plan, err = Run(o)
	if err != nil || !plan.Installed || !plan.VersionMarkerMissing {
		t.Fatalf("apply should repair by installing the new tracked VERSION: %+v %v", plan, err)
	}
	if got := read(t, filepath.Join(dst, "VERSION")); got != "new" {
		t.Fatalf("new VERSION not installed: %q", got)
	}
}

func TestMissingVersionMarkerWithUntrustedReconstructionStillFailsClosed(t *testing.T) {
	o, _, dst := fixtureOpts(t)
	if err := os.Remove(filepath.Join(dst, "VERSION")); err != nil {
		t.Fatal(err)
	}
	// The fixture has no trusted internal/version/version.go capable of
	// reproducing the missing marker, so this must remain a hard failure.
	if _, err := Run(o); err == nil {
		t.Fatal("unreconstructible missing VERSION was accepted")
	}
}

func TestRestoreCompleteBackupAfterFailedPostInstallVerification(t *testing.T) {
	o, _, dst := fixtureOpts(t)
	o.Apply = true
	plan, err := Run(o)
	if err != nil || !plan.Installed || plan.Backup == "" {
		t.Fatalf("apply prerequisite: %+v %v", plan, err)
	}
	// Simulate a post-install verification failure that occurs after the new
	// source is already in place.
	if err := os.WriteFile(filepath.Join(dst, "VERSION"), []byte("broken-after-apply"), 0600); err != nil {
		t.Fatal(err)
	}
	quarantine, err := restoreCompleteBackup(plan.Backup, dst, func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if read(t, filepath.Join(dst, "VERSION")) != "old" {
		t.Fatal("old release was not restored")
	}
	if read(t, filepath.Join(dst, "data", "credentials", "demo_secret_key.enc")) != "SECRET-MUST-SURVIVE" {
		t.Fatal("user data was not restored")
	}
	if got := read(t, filepath.Join(quarantine, "VERSION")); got != "broken-after-apply" {
		t.Fatalf("failed install quarantine lost: %q", got)
	}
}

// A late write failure is materially different from a pre-backup rejection:
// some new files may already be in place, so the caller must use the returned
// complete backup to restore the entire previous installation.
func TestPartialInstallFailureReturnsRestorableBackup(t *testing.T) {
	o, _, dst := fixtureOpts(t)
	// A directory at the name of a *new* .go file is accepted by the old
	// tracked-file check, but cannot be replaced by a regular file.
	collide := filepath.Join(dst, "internal", "g.go")
	if err := os.MkdirAll(collide, 0700); err != nil {
		t.Fatal(err)
	}
	o.Apply = true
	plan, err := Run(o)
	if err == nil || plan.Installed || !plan.BackedUp || plan.Backup == "" {
		t.Fatalf("expected recoverable partial install, plan=%+v err=%v", plan, err)
	}
	if read(t, filepath.Join(dst, "VERSION")) != "new" {
		t.Fatal("fixture did not reach partial install state")
	}
	if read(t, filepath.Join(plan.Backup, "VERSION")) != "old" {
		t.Fatal("backup was not complete")
	}
	quarantine, err := restoreCompleteBackup(plan.Backup, dst, func() error { return nil })
	if err != nil {
		t.Fatalf("partial install restore failed: %v", err)
	}
	if read(t, filepath.Join(dst, "VERSION")) != "old" {
		t.Fatal("old version not restored")
	}
	if read(t, filepath.Join(dst, "data", "credentials", "demo_secret_key.enc")) != "SECRET-MUST-SURVIVE" {
		t.Fatal("credential lost after restoring partial install")
	}
	if read(t, filepath.Join(quarantine, "VERSION")) != "new" {
		t.Fatal("partial updated files not quarantined")
	}
}

func TestLegacyDev355SignedInspectorOnlyExactAddonMayUpgrade(t *testing.T) {
	o, _, dst := fixtureOpts(t)
	// The old source manifest tracks VERSION, but not the emergency signed
	// inspector addon. We intentionally simulate exactly that PC state.
	fixture(t, dst, map[string]string{"VERSION": "v0.4.11-dev.3.55", "go.mod": "module x"})
	addOn := filepath.Join(dst, "cmd", "demo-signed-inspect", "main.go")
	if err := os.MkdirAll(filepath.Dir(addOn), 0700); err != nil {
		t.Fatal(err)
	}
	// Reject untrusted bytes, even when the path is a known legacy addon name.
	if err := os.WriteFile(addOn, []byte("package main // modified"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(o); err == nil || !strings.Contains(err.Error(), "체크섬 관리 밖") {
		t.Fatalf("modified addon accepted: %v", err)
	}
	if err := os.Remove(addOn); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(o); err != nil {
		t.Fatalf("verified old tree without addon should still work: %v", err)
	}
	// Known hash matches are checked by rejectUntrackedGo without weakening
	// the general untracked-file protection.
	if len(legacyDev355SignedInspectorGo) != 2 {
		t.Fatal("legacy addon allowlist changed")
	}
}

// A user who installed the exact incident-inspector patch over dev.3.55
// must not be blocked by the generic untracked-Go fence when applying the
// cumulative dev.3.56 release. Byte changes must still be rejected.
func TestLegacyDev355SignedInspectorExactKnownBytesAccepted(t *testing.T) {
	o, _, dst := fixtureOpts(t)
	fixture(t, dst, map[string]string{"VERSION": "v0.4.11-dev.3.55", "go.mod": "module x"})
	for rel, expected := range legacyDev355SignedInspectorGo {
		current, err := os.ReadFile(filepath.Join("testdata", "dev356-inspector", filepath.Base(rel)))
		if err != nil {
			t.Fatal(err)
		}
		// Git stores this fixture with LF, while Windows core.autocrlf may
		// check it out with CRLF. Reconstruct the historical committed bytes.
		canonical := bytes.ReplaceAll(current, []byte("\r\n"), []byte("\n"))
		historical := bytes.ReplaceAll(canonical, []byte("v0.4.11-dev.3.56"), []byte("v0.4.11-dev.3.55"))
		sum := sha256.Sum256(historical)
		if hex.EncodeToString(sum[:]) != expected {
			t.Fatalf("legacy inspector source changed unexpectedly: %s", rel)
		}
		path := filepath.Join(dst, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, historical, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := Run(o); err != nil {
		t.Fatalf("exact legacy signed inspector addon blocked: %v", err)
	}
}
