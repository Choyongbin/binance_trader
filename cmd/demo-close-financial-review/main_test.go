package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadStrictFinancialJSONRejectsUnknownTrailingAndOversize(t *testing.T) {
	d := t.TempDir()
	for _, tt := range []struct {
		name, input string
		ok          bool
	}{
		{"valid", `{"name":"bt_abc"}`, true},
		{"unknown", `{"name":"ok","extra":"bad"}`, false},
		{"trailing", `{"name":"ok"}{"name":"again"}`, false},
		{"broken", `{"name":`, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := filepath.Join(d, tt.name+".json")
			if err := os.WriteFile(p, []byte(tt.input), 0600); err != nil {
				t.Fatal(err)
			}
			var out struct {
				Name string `json:"name"`
			}
			err := readStrictFinancialJSON(p, &out)
			if (err == nil) != tt.ok {
				t.Fatalf("unexpected parse status: %v", err)
			}
		})
	}
	oversized := filepath.Join(d, "oversized.json")
	if err := os.WriteFile(oversized, make([]byte, 1<<20+1), 0600); err != nil {
		t.Fatal(err)
	}
	var result any
	if err := readStrictFinancialJSON(oversized, &result); err == nil {
		t.Fatal("oversized assertion accepted")
	}
}
