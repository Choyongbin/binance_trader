package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReviewCLIRejectsMissingManifestAndUnknownAction(t *testing.T) {
	for _, action := range []string{"intent", "verify-once", "approve-settlement"} {
		var out bytes.Buffer
		if err := run(context.Background(), action, t.TempDir(), "", "", "", "", "", "", 1800000000, &out); err == nil {
			t.Errorf("%s accepted missing manifest", action)
		}
		if out.Len() != 0 {
			t.Fatal("unexpected output for invalid input")
		}
	}
}
func TestReviewCLIRequiresBoundedStrictManifest(t *testing.T) {
	root := t.TempDir()
	file := filepath.Join(t.TempDir(), "manifest.json")
	for _, body := range []string{"{}", `{"schema":1,"events":[],"untrusted":true}`, `{"schema":1,"events":[]} {}`, strings.Repeat("x", (1<<20)+1)} {
		if err := os.WriteFile(file, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		if err := run(context.Background(), "intent", root, file, "", "", "", "", "", 1800000000, &out); err == nil {
			t.Fatalf("accepted malformed manifest: %.60s", body)
		}
		if out.Len() != 0 {
			t.Fatal("wrote request despite manifest failure")
		}
	}
}
