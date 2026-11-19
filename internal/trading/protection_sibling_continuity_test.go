package trading

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func siblingContinuityFixture(t *testing.T) (*ProtectionSubmissionStore, *CancelIntentStore, ProtectionEvidence, time.Time, string) {
	t.Helper()
	now := time.Now().UTC()
	preview, evidence := evidencePlan(t, now)
	evidence.TP.Status = "FINISHED"
	evidence.TP.ActualOrderID = "77123"
	evidence.TP.ActualQty = "0.010"
	evidence.RemainingOwnerQty = "0"
	dir := t.TempDir()
	submissions, err := OpenProtectionSubmissionStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	cancels, err := NewCancelIntentStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := submissions.Prepare("group-p-001", evidence.Owner, preview); err != nil {
		t.Fatal(err)
	}
	return submissions, cancels, evidence, now, dir
}

func TestSiblingCancelContinuityCrashAndRestart(t *testing.T) {
	submissions, cancels, evidence, now, _ := siblingContinuityFixture(t)
	group := "group-p-001"
	first, err := InspectSiblingCancelContinuity(submissions, cancels, group, evidence, now)
	if err != nil || first.State != "REQUIRES_OPERATOR_CANCEL_REVIEW" || first.CancelAllowed || first.ReplayAllowed || first.TradingAuthorized {
		t.Fatalf("no intent: %+v %v", first, err)
	}
	intent, err := cancels.Plan(group, evidence.SL.AlgoID, evidence.SL.ClientID)
	if err != nil {
		t.Fatal(err)
	}
	second, err := InspectSiblingCancelContinuity(submissions, cancels, group, evidence, now)
	if err != nil || second.State != "INTENT_ONLY_NOT_DISPATCHED" || !second.IntentRecorded || second.DispatchMayHaveBeenSent {
		t.Fatalf("intent only: %+v %v", second, err)
	}
	if err := cancels.MarkMayHaveSent(intent); err != nil {
		t.Fatal(err)
	}
	third, err := InspectSiblingCancelContinuity(submissions, cancels, group, evidence, now)
	if err != nil || third.State != "MAY_HAVE_SENT_RECHECK_REQUIRED" || !third.DispatchMayHaveBeenSent || third.ReplayAllowed || third.CancelAllowed {
		t.Fatalf("dispatch: %+v %v", third, err)
	}
	evidence.SL.Status = "CANCELED"
	fourth, err := InspectSiblingCancelContinuity(submissions, cancels, group, evidence, now)
	if err != nil || fourth.State != "OBSERVED_TERMINAL_NOT_SETTLED" || fourth.FinanciallySettled || fourth.TradingAuthorized {
		t.Fatalf("terminal: %+v %v", fourth, err)
	}
}

func TestSiblingCancelContinuityRejectsWrongTargetAndTampering(t *testing.T) {
	submissions, cancels, evidence, now, dir := siblingContinuityFixture(t)
	group := "group-p-001"
	if _, err := cancels.Plan(group, evidence.TP.AlgoID, evidence.TP.ClientID); err != nil {
		t.Fatal(err)
	}
	bad, err := InspectSiblingCancelContinuity(submissions, cancels, group, evidence, now)
	if err == nil || bad.State != "HALT" || !bad.IntentRecorded || bad.CancelAllowed {
		t.Fatalf("wrong sibling accepted: %+v %v", bad, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "trading", "algo_cancel_intents", group+".json"), []byte("broken"), 0600); err != nil {
		t.Fatal(err)
	}
	bad, err = InspectSiblingCancelContinuity(submissions, cancels, group, evidence, now)
	if err == nil || bad.State != "HALT" {
		t.Fatalf("tamper accepted: %+v %v", bad, err)
	}
}

func TestSiblingCancelContinuityRejectsAmbiguousEvidence(t *testing.T) {
	submissions, cancels, evidence, now, _ := siblingContinuityFixture(t)
	group := "group-p-001"
	cases := []struct {
		name   string
		mutate func(*ProtectionEvidence)
	}{
		{"duplicate algo ID", func(e *ProtectionEvidence) { e.SL.AlgoID = e.TP.AlgoID }},
		{"stale sibling", func(e *ProtectionEvidence) { e.SL.ObservedAt = now.Add(-10 * time.Second) }},
		{"simultaneous execution", func(e *ProtectionEvidence) { e.SL.Status = "TRIGGERED" }},
		{"unverified account", func(e *ProtectionEvidence) { e.SignedAccountAsOf = time.Time{} }},
		{"unverified ownership", func(e *ProtectionEvidence) { e.FullSignedOwnershipAudit = false }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := evidence
			tc.mutate(&snapshot)
			out, err := InspectSiblingCancelContinuity(submissions, cancels, group, snapshot, now)
			if err == nil || out.State != "HALT" || out.CancelAllowed || out.ReplayAllowed {
				t.Fatalf("accepted: %+v %v", out, err)
			}
		})
	}
}

func TestProtectionIdenticalAlgoIDCannotCancelSibling(t *testing.T) {
	now := time.Now().UTC()
	preview, evidence := evidencePlan(t, now)
	evidence.SL.AlgoID = evidence.TP.AlgoID
	evidence.TP.Status = "FINISHED"
	if got := EvaluateProtection(preview, evidence, now); got.State != ProtectionHalt {
		t.Fatalf("duplicate ID cancel recommendation: %+v", got)
	}
}
