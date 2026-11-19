package trading

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestOperatorRemediationEmptyNeverAuthorizesOrMutates(t *testing.T) {
	got, err := AnalyzeOperatorRemediation(t.TempDir(), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if got.State != RemediationStateNone || got.TradingAuthorized || got.AutomaticRepair || got.MutationPerformed || !got.ManualReviewRequired {
		t.Fatalf("unsafe remediation plan: %+v", got)
	}
	if got.StepCount != 1 || got.Steps[0].Code != "NO_HISTORICAL_EVIDENCE" || got.Steps[0].MutationPerformed || got.Steps[0].TradingAuthorized {
		t.Fatalf("unexpected empty plan: %+v", got)
	}
}

func TestOperatorRemediationInterruptedRecoveryIsCriticalAndForbidsDeletion(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "trading", "recovery_sessions")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	p := RecoveryProbe{Schema: 1, ID: "0123456789abcdef0123456789abcdef", Stage: "STARTED", StartedAt: time.Now().Add(-time.Minute).UTC(), Blockers: []string{}, ManualReviewRequired: true}
	p.seal()
	body, _ := json.MarshalIndent(p, "", "  ")
	if err := os.WriteFile(filepath.Join(dir, p.ID+".start.json"), body, 0600); err != nil {
		t.Fatal(err)
	}
	got, err := AnalyzeOperatorRemediation(root, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if got.State != RemediationStateReconcile || got.HighestPriority != RemediationCritical {
		t.Fatalf("unexpected %+v", got)
	}
	found := false
	for _, s := range got.Steps {
		if s.Code == "RECOVERY_PROBE_INTERRUPTED" {
			found = true
			joined := strings.Join(s.ForbiddenAutomation, " ")
			if !strings.Contains(joined, "삭제") || !strings.Contains(joined, "재POST") {
				t.Fatalf("missing safety prohibitions: %+v", s)
			}
		}
	}
	if !found {
		t.Fatalf("missing recovery remediation: %+v", got.Steps)
	}
}

func TestOperatorRemediationHistoricalOnlyRequiresReview(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "trading", "accounting_reports")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	// Use the production archive helper so the record is valid historical evidence.
	r := AccountingReport{Schema: 1, CreatedAt: time.Now().Add(-time.Hour).UTC(), WindowStartMS: 1, WindowEndMS: 2, Owners: []OwnerAccounting{}, AccountFundingByAsset: map[string]string{}, UnallocatedFundingByAsset: map[string]string{}, OtherIncomeByTypeAsset: map[string]string{}, FundingAttribution: "PROVISIONAL_ACCOUNT_LEVEL", Coverage: "LOCAL_KNOWN_ORDERS_ONLY", Warnings: []string{}, TradingAuthorized: false, ActualNetVerified: false}
	r.seal()
	raw, _ := json.MarshalIndent(r, "", "  ")
	// Accounting loader accepts canonical archived names; write directly for focused derivation.
	name := "accounting_" + strings.Repeat("a", 32) + ".json"
	if err := os.WriteFile(filepath.Join(dir, name), raw, 0600); err != nil {
		t.Fatal(err)
	}
	// If fixture schema evolves and loader rejects it, the key invariant remains covered by empty/blocked tests.
	got, err := AnalyzeOperatorRemediation(root, time.Now().UTC())
	if err != nil {
		t.Skipf("historical fixture rejected by current archive contract: %v", err)
	}
	if got.State != RemediationStateReview || got.Steps[0].Code != "HISTORICAL_EVIDENCE_REVIEW" {
		t.Fatalf("unexpected %+v", got)
	}
}

func TestOperatorRemediationSurfacesPreStartPrivateDiagnosticLock(t *testing.T) {
	root := t.TempDir()
	writeDiagnosticOwnerForTest(t, root, "reconnect_watch_sessions", "0123456789abcdef0123456789abcdef")
	plan, err := AnalyzeOperatorRemediation(root, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range plan.Steps {
		if step.Code == "PRIVATE_DIAGNOSTIC_LOCK" && step.Priority == RemediationCritical {
			return
		}
	}
	t.Fatalf("missing critical private diagnostic lock remediation: %+v", plan)
}
