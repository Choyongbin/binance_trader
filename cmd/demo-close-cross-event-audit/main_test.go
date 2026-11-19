package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestManifestFailsClosedWithoutSource(t *testing.T) {
	cases := []string{
		`{"schema":1,"events":[]}`,
		`{"schema":1,"events":[{"data":"x","id":"x","incident":"missing"},{"data":"y","id":"y","incident":"missing"}]}`,
		`{"schema":1,"events":[{"data":"x","id":"x","incident":"missing"}],"unknown":true}`,
		`{"schema":1,"events":[],"schema":2}`,
		`{"schema":1,"events":[{"data":"","id":"x","incident":"foo"},{"data":"y","id":"y","incident":"foo"}]}`,
	}
	for _, raw := range cases {
		p := filepath.Join(t.TempDir(), "manifest.json")
		if err := os.WriteFile(p, []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
		buf := &bytes.Buffer{}
		if _, err := run(context.Background(), p, buf); err == nil || buf.Len() != 0 {
			t.Fatalf("accepted malformed or incomplete manifest: %s", raw)
		}
	}
	p := filepath.Join(t.TempDir(), "manifest.json")
	if err := os.WriteFile(p, []byte(`{"schema":1,"events":[]}`+strings.Repeat(" ", 1<<20)), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := run(context.Background(), p, &bytes.Buffer{}); err == nil {
		t.Fatal("oversize manifest accepted")
	}
}

func TestModelPolicyModeFailsClosedWithoutRealSources(t *testing.T) {
	p := filepath.Join(t.TempDir(), "manifest.json")
	if err := os.WriteFile(p, []byte(`{"schema":1,"events":[{"data":"offline-a","id":"bt-a","incident":"missing-a"},{"data":"offline-b","id":"bt-b","incident":"missing-b"}]}`), 0600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if _, err := runWithMode(context.Background(), p, &output, true); err == nil || output.Len() != 0 {
		t.Fatal("model-policy should not issue a report when independent source is missing")
	}
}
