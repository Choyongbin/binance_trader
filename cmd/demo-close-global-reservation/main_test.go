package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGlobalReservationCLIInspectAndReserveGuard(t *testing.T) {
	root := t.TempDir()
	var out bytes.Buffer
	if err := run(context.Background(), root, "inspect", "", "", &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "GLOBAL_EVIDENCE_REGISTRY_READ_ONLY_NOT_POSTED") {
		t.Fatal(out.String())
	}
	if _, err := os.Stat(filepath.Join(root, "external_global_evidence_reservations")); !os.IsNotExist(err) {
		t.Fatal("inspect created registry files")
	}
	for _, args := range [][2]string{{"", ""}, {"nonexistent.json", ""}, {"nonexistent.json", "AUTHORIZED"}} {
		out.Reset()
		if err := run(context.Background(), root, "reserve", args[0], args[1], &out); err == nil {
			t.Fatal("unsafe reserve call accepted")
		}
	}
	path := filepath.Join(root, "bad.json")
	if err := os.WriteFile(path, []byte("{\"schema\":1,\"events\":[],\"extra\":true}"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := run(context.Background(), root, "reserve", path, "REVIEW_ONLY_NOT_SETTLEMENT", &out); err == nil {
		t.Fatal("unknown JSON accepted")
	}
}

func TestGlobalReservationCLIConsistencyStrictReadOnlyInputs(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "manifest.json")
	if err := os.WriteFile(path, []byte(`{"schema":1,"events":[{"data":"A","id":"bt_aaaaaaaaaaaaaaaaaaaaaaaaaaaa","incident":"missing-a.json"},{"data":"B","id":"bt_bbbbbbbbbbbbbbbbbbbbbbbbbbbb","incident":"missing-b.json"}]}`), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ path, ack string }{{"", ""}, {path, "REVIEW_ONLY_NOT_SETTLEMENT"}, {path, "AUTHORIZED"}} {
		var out bytes.Buffer
		if err := run(context.Background(), root, "consistency", tc.path, tc.ack, &out); err == nil || out.Len() != 0 {
			t.Fatalf("accepted invalid read-only request: %+v", tc)
		}
	}
	var out bytes.Buffer
	if err := run(context.Background(), root, "consistency", path, "", &out); err == nil || out.Len() != 0 {
		t.Fatal("missing incident source unexpectedly accepted")
	}
	if _, err := os.Stat(filepath.Join(root, "external_global_evidence_reservations")); !os.IsNotExist(err) {
		t.Fatal("read-only consistency created registry files")
	}
}
