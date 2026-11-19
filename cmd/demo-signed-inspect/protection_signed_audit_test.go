package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"binance_trader/internal/exchange"
)

type silentSignedReader struct{ reads int }

func (m *silentSignedReader) FetchDemoRecoveryBaseline(_ context.Context, _, _ string, _ int64) (exchange.RecoveryBaseline, error) {
	m.reads++
	return exchange.RecoveryBaseline{}, nil
}

func TestProtectionSignedAuditWritesBlockedAndDoesNotOverwrite(t *testing.T) {
	root := t.TempDir()
	out := filepath.Join(root, "pc-results", "signed-protection-audit.json")
	// Dummy key is only used to establish the pre-network invalid reservation
	// condition; tests never connect to a Binance host.
	if err := os.WriteFile(filepath.Join(root, "test.enc"), []byte("test-demo-key-1234\ntest-secret-1234\n"), 0600); err != nil {
		t.Fatal(err)
	}
	m := &silentSignedReader{}
	clientID := "bt_" + strings.Repeat("a", 28)
	err := runSignedProtectionAudit(root, out, clientID, 24, m)
	if err == nil {
		t.Fatal("absent reservation must block")
	}
	if m.reads != 0 {
		t.Fatal("failed local review performed signed GET")
	}
	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	var report pcProtectionReport
	if err = json.Unmarshal(raw, &report); err != nil {
		t.Fatal(err)
	}
	want, err := report.checksum()
	if err != nil {
		t.Fatal(err)
	}
	if report.SHA256 != want || report.Review.ErrorCode != "LOCAL_RESERVATION_UNAVAILABLE" || report.Review.TradingAuthorized || !report.Review.ManualReviewRequired || report.Review.SignedGETAttempted {
		t.Fatalf("unexpected report: %+v", report)
	}
	if strings.Contains(string(raw), "test-secret") || strings.Contains(string(raw), "test-demo-key") {
		t.Fatal("credential leaked")
	}
	if err := runSignedProtectionAudit(root, out, clientID, 24, m); err == nil {
		t.Fatal("report overwrite permitted")
	}
	if after, _ := os.ReadFile(out); string(raw) != string(after) {
		t.Fatal("report overwritten")
	}
}
func TestProtectionSignedAuditBlocksUnsafeArgumentsBeforeNetwork(t *testing.T) {
	root := t.TempDir()
	m := &silentSignedReader{}
	for _, tc := range []struct {
		client, out string
		hours       int
	}{
		{"bt_bad", filepath.Join(root, "pc-results", "bad.json"), 24},
		{"bt_" + strings.Repeat("a", 28), filepath.Join(root, "outside.json"), 24},
		{"bt_" + strings.Repeat("a", 28), filepath.Join(root, "pc-results", "bad.json"), 0},
		{"bt_" + strings.Repeat("a", 28), filepath.Join(root, "pc-results", "bad.json"), 121},
	} {
		if err := runSignedProtectionAudit(root, tc.out, tc.client, tc.hours, m); err == nil {
			t.Fatalf("unsafe accepted: %+v", tc)
		}
	}
	if m.reads != 0 {
		t.Fatal("unsafe CLI arguments issued signed GET")
	}
}
