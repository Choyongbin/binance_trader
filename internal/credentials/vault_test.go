package credentials

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEncryptionAndTamperProtection(t *testing.T) {
	vault, err := NewVault(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	pass := "this-is-a-strong-demo-file-password"
	if err := vault.SaveDemo("test-demo-key", "test-demo-secret", pass); err != nil {
		t.Fatal(err)
	}
	manifest, present, err := vault.activeManifestRead()
	if err != nil || !present {
		t.Fatalf("manifest absent: %v", err)
	}
	for _, name := range []string{"demo_api_key.enc", "demo_secret_key.enc"} {
		data, err := os.ReadFile(filepath.Join(vault.generationDir(manifest.Active), name))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(data), "test-demo") {
			t.Fatalf("plaintext in %s", name)
		}
	}
	if key, secret, err := vault.ReadDemo(pass); err != nil || key != "test-demo-key" || secret != "test-demo-secret" {
		t.Fatal("roundtrip failure", err)
	}
	if _, _, err := vault.ReadDemo("wrong-secret-string"); err == nil {
		t.Fatal("incorrect password worked")
	}
	p := filepath.Join(vault.generationDir(manifest.Active), "demo_api_key.enc")
	data, _ := os.ReadFile(p)
	data[len(data)-1] ^= 0x8
	if err := os.WriteFile(p, data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := vault.ReadDemo(pass); err == nil {
		t.Fatal("modified ciphertext worked")
	}
}
func TestRefuseUnknownFileAndMismatchedGeneration(t *testing.T) {
	vault, _ := NewVault(t.TempDir())
	path := filepath.Join(vault.Dir, "demo_api_key.enc")
	_ = os.WriteFile(path, []byte("unencrypted credential"), 0600)
	if err := vault.SaveDemo("key", "secret", "123456789012"); err == nil {
		t.Fatal("overwrote unknown format")
	}
	_ = os.Remove(path)
	_, _ = os.Create(path)
	if err := vault.SaveDemo("key", "secret", "123456789012"); err != nil {
		t.Fatal(err)
	}
	original, present, err := vault.activeManifestRead()
	if err != nil || !present {
		t.Fatal(err)
	}
	old, _ := os.ReadFile(filepath.Join(vault.generationDir(original.Active), "demo_api_key.enc"))
	if err := vault.SaveDemo("other", "secret", "123456789012"); err != nil {
		t.Fatal(err)
	}
	replacement, _, _ := vault.activeManifestRead()
	path = filepath.Join(vault.generationDir(replacement.Active), "demo_api_key.enc")
	_ = os.WriteFile(path, old, 0600)
	if _, _, err := vault.ReadDemo("123456789012"); err == nil {
		t.Fatal("accepted mismatched key pair")
	}
}

func TestAtomicGenerationsAndManualPreviousRecovery(t *testing.T) {
	vault, err := NewVault(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const oldPass = "previous-demo-passphrase"
	if err := vault.SaveDemo("old-key", "old-secret", oldPass); err != nil {
		t.Fatal(err)
	}
	first, _, err := vault.activeManifestRead()
	if err != nil {
		t.Fatal(err)
	}
	// Simulate a crash AFTER writing one encrypted key of an uncommitted
	// generation. Only the manifest may select active credentials.
	if err := os.MkdirAll(vault.generationDir(strings.Repeat("f", 32)), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(vault.generationDir(strings.Repeat("f", 32)), "demo_api_key.enc"), []byte("interrupted"), 0600); err != nil {
		t.Fatal(err)
	}
	if k, s, err := vault.ReadDemo(oldPass); err != nil || k != "old-key" || s != "old-secret" {
		t.Fatalf("partial generation changed active pair: %v", err)
	}
	const newPass = "replacement-demo-passphrase"
	if err := vault.SaveDemo("new-key", "new-secret", newPass); err != nil {
		t.Fatal(err)
	}
	next, _, err := vault.activeManifestRead()
	if err != nil || next.Active == first.Active || next.Previous != first.Active {
		t.Fatalf("wrong generation switch: %+v %v", next, err)
	}
	if _, _, err := vault.ReadDemo(oldPass); err == nil {
		t.Fatal("silently rolled back on incorrect active passphrase")
	}
	if k, s, err := vault.ReadDemo(newPass); err != nil || k != "new-key" || s != "new-secret" {
		t.Fatalf("new generation unreadable: %v", err)
	}
	if err := vault.RestorePreviousDemo("wrong-password"); err == nil {
		t.Fatal("unauthenticated rollback")
	}
	if err := vault.RestorePreviousDemo(oldPass); err != nil {
		t.Fatal(err)
	}
	if k, s, err := vault.ReadDemo(oldPass); err != nil || k != "old-key" || s != "old-secret" {
		t.Fatalf("manual rollback failed: %v", err)
	}
	// Corrupted active manifest must never be ignored in favor of legacy files.
	if err := os.WriteFile(vault.manifestPath(), []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := vault.ReadDemo(oldPass); err == nil {
		t.Fatal("corrupt manifest incorrectly ignored")
	}
	if err := vault.SaveDemo("other", "other", newPass); err == nil {
		t.Fatal("corrupt active manifest overwritten")
	}
}
