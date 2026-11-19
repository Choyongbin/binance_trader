package trading

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeJSONFixture(t *testing.T, path string, v any) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(raw, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestDiagnosticRetentionKeepsNewestIntegratedAndAccountingByReportTime(t *testing.T) {
	dir := t.TempDir()
	base := time.Now().UTC().Add(-6 * time.Hour).Truncate(time.Millisecond)
	ids := []string{
		"ffffffffffffffffffffffffffffffff", // oldest but lexicographically largest
		"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		"00000000000000000000000000000000", // newest but lexicographically smallest
	}

	for i, id := range ids {
		at := base.Add(time.Duration(i) * time.Hour)
		integrated := IntegratedReview{
			Schema: 1, ID: id, CheckedAt: at, State: "BLOCKED",
			EvidenceBefore: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			EvidenceAfter:  "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			Blockers:       []string{"historical diagnostic"}, ManualReviewRequired: true,
		}
		integrated.SHA256 = integrated.digest()
		writeJSONFixture(t, filepath.Join(dir, "trading", "integrated_reviews", id+".json"), integrated)

		accounting := AccountingReport{
			Schema: 1, ID: id, CreatedAt: at,
			WindowStartMS: at.Add(-2 * time.Hour).UnixMilli(), WindowEndMS: at.Add(-time.Second).UnixMilli(),
			Owners: []OwnerAccounting{}, AccountFundingByAsset: map[string]string{}, UnallocatedFundingByAsset: map[string]string{}, OtherIncomeByTypeAsset: map[string]string{},
			FundingAttribution: "PROVISIONAL_OR_UNALLOCATED", Coverage: "BOUNDED_SIGNED_INCOME_AND_LOCAL_VERIFIED_FILLS_NOT_CONTINUOUS",
			Warnings: []string{"historical", "read-only"},
		}
		accounting.seal()
		writeJSONFixture(t, filepath.Join(dir, "trading", "accounting_reports", id+".json"), accounting)
	}

	out, err := CompactDiagnosticArchives(dir, 1, base.Add(5*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if out.Archived != 4 || out.Removed != 4 || out.TradingAuthorized {
		t.Fatalf("unexpected retention result: %+v", out)
	}
	newest := base.Add(2 * time.Hour)

	integratedFiles, err := os.ReadDir(filepath.Join(dir, "trading", "integrated_reviews"))
	if err != nil || len(integratedFiles) != 1 {
		t.Fatalf("integrated hot set: %d %v", len(integratedFiles), err)
	}
	var integrated IntegratedReview
	raw, _ := os.ReadFile(filepath.Join(dir, "trading", "integrated_reviews", integratedFiles[0].Name()))
	if err := json.Unmarshal(raw, &integrated); err != nil || !integrated.CheckedAt.Equal(newest) || integrated.ID != ids[2] {
		t.Fatalf("retention did not keep newest integrated report: %+v err=%v", integrated, err)
	}

	accountingFiles, err := os.ReadDir(filepath.Join(dir, "trading", "accounting_reports"))
	if err != nil || len(accountingFiles) != 1 {
		t.Fatalf("accounting hot set: %d %v", len(accountingFiles), err)
	}
	var accounting AccountingReport
	raw, _ = os.ReadFile(filepath.Join(dir, "trading", "accounting_reports", accountingFiles[0].Name()))
	if err := json.Unmarshal(raw, &accounting); err != nil || !accounting.CreatedAt.Equal(newest) || accounting.ID != ids[2] {
		t.Fatalf("retention did not keep newest accounting report: %+v err=%v", accounting, err)
	}

	allIntegrated, err := LoadIntegratedReviews(dir)
	if err != nil || len(allIntegrated) != 3 {
		t.Fatalf("integrated history lost: %d %v", len(allIntegrated), err)
	}
	allAccounting, err := LoadAccountingReports(dir)
	if err != nil || len(allAccounting) != 3 {
		t.Fatalf("accounting history lost: %d %v", len(allAccounting), err)
	}
}

func TestRecoveryAndReconnectArchiveTieBreakByID(t *testing.T) {
	at := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	ids := []string{"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}

	recoveryDir := t.TempDir()
	for _, id := range ids {
		start := RecoveryProbe{Schema: 1, ID: id, Stage: "STARTED", StartedAt: at, Blockers: []string{}, ManualReviewRequired: true}
		start.seal()
		writeJSONFixture(t, filepath.Join(recoveryDir, "trading", "recovery_sessions", id+".start.json"), start)
		end := start
		end.Stage = "BLOCKED"
		end.FinishedAt = at.Add(time.Second)
		end.Blockers = []string{"historical diagnostic"}
		end.seal()
		writeJSONFixture(t, filepath.Join(recoveryDir, "trading", "recovery_sessions", id+".end.json"), end)
	}
	for n := 0; n < 50; n++ {
		got, err := LoadRecoveryProbes(recoveryDir)
		if err != nil || len(got) != 2 || got[0].ID != ids[1] || got[1].ID != ids[0] {
			t.Fatalf("recovery loader ordering changed at iteration %d: %+v err=%v", n, got, err)
		}
	}

	reconnectDir := t.TempDir()
	for _, id := range ids {
		start := ReconnectWatchReport{Schema: 1, ID: id, Stage: "STARTED", StartedAt: at, RequestedSeconds: 30, Epochs: []ReconnectEpoch{}, Blockers: []string{}, HistoricalOnly: true, ManualReviewRequired: true}
		start.seal()
		writeJSONFixture(t, filepath.Join(reconnectDir, "trading", "reconnect_watch_sessions", id+".start.json"), start)
		end := start
		end.Stage = "BLOCKED"
		end.FinishedAt = at.Add(time.Second)
		end.Blockers = []string{"historical diagnostic"}
		end.seal()
		writeJSONFixture(t, filepath.Join(reconnectDir, "trading", "reconnect_watch_sessions", id+".end.json"), end)
	}
	for n := 0; n < 50; n++ {
		got, err := LoadReconnectWatches(reconnectDir)
		if err != nil || len(got) != 2 || got[0].ID != ids[1] || got[1].ID != ids[0] {
			t.Fatalf("reconnect loader ordering changed at iteration %d: %+v err=%v", n, got, err)
		}
	}
}

func TestDiagnosticRetentionRejectsCorruptSourceBeforeDeletion(t *testing.T) {
	dir := t.TempDir()
	at := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	ids := []string{"11111111111111111111111111111111", "22222222222222222222222222222222"}
	for i, id := range ids {
		r := IntegratedReview{
			Schema: 1, ID: id, CheckedAt: at.Add(time.Duration(i) * time.Minute), State: "BLOCKED",
			EvidenceBefore: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			EvidenceAfter:  "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			Blockers:       []string{"historical diagnostic"}, ManualReviewRequired: true,
		}
		r.SHA256 = r.digest()
		if i == 0 {
			r.SHA256 = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
		}
		writeJSONFixture(t, filepath.Join(dir, "trading", "integrated_reviews", id+".json"), r)
	}
	if _, err := CompactDiagnosticArchives(dir, 1, at.Add(time.Hour)); err == nil {
		t.Fatal("corrupt source was moved into retention instead of failing closed")
	}
	files, err := os.ReadDir(filepath.Join(dir, "trading", "integrated_reviews"))
	if err != nil || len(files) != 2 {
		t.Fatalf("source deletion occurred after validation failure: count=%d err=%v", len(files), err)
	}
	if _, err := os.Stat(filepath.Join(dir, "trading", "cold_archives")); !os.IsNotExist(err) {
		t.Fatalf("cold archive should not be created on invalid source: %v", err)
	}
}
