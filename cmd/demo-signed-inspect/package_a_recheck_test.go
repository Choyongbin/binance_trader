package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEntryFreshRecheckCommandRejectsUnsafeInputsBeforeCredentials(t *testing.T) {
	root := t.TempDir()
	resultDir := filepath.Join(root, "pc-results")
	if err := os.MkdirAll(resultDir, 0700); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(resultDir, "recheck.json")
	for _, tc := range []struct{ action, id, confirm string }{
		{"recheck-entry-recovery", "../private.json", ""},
		{"recheck-entry-recovery", "C:\\secrets.json", ""},
		{"recheck-entry-recovery", "entry.json", "FORGED_APPROVAL"},
		{"invalid-action", "entry.json", ""},
	} {
		err := runSignedPackageA(root, out, tc.action, tc.id, tc.confirm, 0)
		if err == nil {
			t.Fatalf("unsafe input accepted %+v", tc)
		}
		if strings.Contains(err.Error(), "credentials") {
			t.Fatalf("credentials unexpectedly read for invalid input %+v", tc)
		}
	}
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Fatal("invalid input created report")
	}
}
