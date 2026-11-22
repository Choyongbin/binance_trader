package trading

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func incidentHasCode(inv PendingProtectionInventory, code string) bool {
	for _, i := range inv.Incidents {
		if i.Code == code && i.ManualReviewRequired {
			return true
		}
	}
	return false
}

func TestProductProtectionArtifactsOrphanPlanBlocksAfterRestart(t *testing.T) {
	f := newEntryLifecycleFixture(t, Long)
	f.armPair(t)
	product := productInspectorFixture(t, f)
	healthy, err := product.InspectOutstandingProtections(context.Background())
	if err != nil || healthy.State != PendingProtectionInspectionObserved {
		t.Fatalf("baseline: %+v %v", healthy, err)
	}
	orphan := f.plan
	orphan.GroupID = "entry_missing_order_0001"
	orphan.Digest = orphan.digest()
	if err := orphan.validate(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(f.root, "trading", "protection_submissions", orphan.GroupID+".json")
	// Deliberately simulate a crash leaving a durable plan without a reservation.
	if err := os.WriteFile(path, mustMarshalProtectionArtifact(t, orphan), 0600); err != nil {
		t.Fatal(err)
	}
	product = productInspectorFixture(t, f)
	got, err := product.InspectOutstandingProtections(context.Background())
	if err == nil || got.State != PendingProtectionInspectionBlocked ||
		!incidentHasCode(got, "ORPHAN_PROTECTION_PLAN_NO_ENTRY") || got.InspectedEntries != 1 {
		t.Fatalf("orphan plan escaped cross-ledger audit: %+v %v", got, err)
	}
	assertNonAuthorizingPortfolio(t, got)
	if f.signed.submitCalls != 0 || f.signed.cancelCalls != 0 {
		t.Fatal("inspection wrote to exchange")
	}
}

func TestProductProtectionArtifactsOrphanPropagatesToWatcher(t *testing.T) {
	f := newEntryLifecycleFixture(t, Long)
	f.armPair(t)
	orphan := f.plan
	orphan.GroupID = "entry_untracked_0002"
	orphan.Digest = orphan.digest()
	if err := os.WriteFile(filepath.Join(f.root, "trading", "protection_submissions", orphan.GroupID+".json"),
		mustMarshalProtectionArtifact(t, orphan), 0600); err != nil {
		t.Fatal(err)
	}
	product := productInspectorFixture(t, f)
	poller, err := NewProductProtectionPoller(product.InspectOutstandingProtections, time.Minute, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	poller.check(context.Background())
	status := poller.Status()
	if status.State != ProtectionWatchBlocked || status.LastErrorCategory != "PROTECTION_INCIDENT_REQUIRES_RECONCILIATION" ||
		!incidentHasCode(status.Inventory, "ORPHAN_PROTECTION_PLAN_NO_ENTRY") || status.TradingAuthorized || status.ContinuousProtection {
		t.Fatalf("watcher accepted orphan artifact: %+v", status)
	}
}

func TestProductProtectionArtifactsOrphanCancelBlocks(t *testing.T) {
	f := newEntryLifecycleFixture(t, Short)
	f.armPair(t)
	if _, err := f.cancels.Plan("missing_cancel_group_01", 990001, f.plan.SL.ClientAlgoID); err != nil {
		t.Fatal(err)
	}
	product := productInspectorFixture(t, f)
	got, err := product.InspectOutstandingProtections(context.Background())
	if err == nil || got.State != PendingProtectionInspectionBlocked || !incidentHasCode(got, "ORPHAN_SIBLING_CANCEL_INTENT") {
		t.Fatalf("orphan sibling cancel escaped: %+v %v", got, err)
	}
	assertNonAuthorizingPortfolio(t, got)
}

func TestProductProtectionArtifactsMismatchedCancelTargetBlocks(t *testing.T) {
	f := newEntryLifecycleFixture(t, Long)
	f.armPair(t)
	if _, err := f.cancels.Plan(f.plan.GroupID, 990002, "foreign_algo_001"); err != nil {
		t.Fatal(err)
	}
	got, err := productInspectorFixture(t, f).InspectOutstandingProtections(context.Background())
	if err == nil || !incidentHasCode(got, "SIBLING_CANCEL_TARGET_MISMATCH") {
		t.Fatalf("unowned sibling cancel target escaped: %+v %v", got, err)
	}
	assertNonAuthorizingPortfolio(t, got)
}

func TestProductProtectionArtifactsForgedAlgoIDBlocks(t *testing.T) {
	f := newEntryLifecycleFixture(t, Long)
	f.armPair(t)
	// This clientAlgoId is real, but the numeric order ID is not the
	// original signed-observed AlgoID. Never cancel that other order.
	if _, err := f.cancels.Plan(f.plan.GroupID, 999999, f.plan.SL.ClientAlgoID); err != nil {
		t.Fatal(err)
	}
	got, err := productInspectorFixture(t, f).InspectOutstandingProtections(context.Background())
	if err == nil || !incidentHasCode(got, "SIBLING_CANCEL_ALGO_ID_UNVERIFIED") {
		t.Fatalf("forged sibling AlgoID escaped: %+v %v", got, err)
	}
	assertNonAuthorizingPortfolio(t, got)
}

func TestProductProtectionArtifactsInvalidPlanOrMarkerBlocks(t *testing.T) {
	for _, scenario := range []string{"bad_plan", "missing_cancel_parent", "unknown_cancel_filename"} {
		t.Run(scenario, func(t *testing.T) {
			f := newEntryLifecycleFixture(t, Long)
			f.armPair(t)
			var path string
			switch scenario {
			case "bad_plan":
				path = filepath.Join(f.root, "trading", "protection_submissions", f.plan.GroupID+".json")
			case "missing_cancel_parent":
				path = filepath.Join(f.root, "trading", "algo_cancel_intents", f.plan.GroupID+".dispatch.json")
			default:
				path = filepath.Join(f.root, "trading", "algo_cancel_intents", "unexpected.txt")
			}
			if err := os.WriteFile(path, []byte("{}"), 0600); err != nil {
				t.Fatal(err)
			}
			got, err := productInspectorFixture(t, f).InspectOutstandingProtections(context.Background())
			if err == nil || got.State != PendingProtectionInspectionBlocked || len(got.Blockers) == 0 {
				t.Fatalf("corrupt journal escaped: %+v %v", got, err)
			}
			if scenario == "missing_cancel_parent" && !incidentHasCode(got, "INCOMPLETE_SIBLING_CANCEL_JOURNAL") {
				t.Fatalf("missing cancel base not classified: %+v", got)
			}
			assertNonAuthorizingPortfolio(t, got)
		})
	}
}

func TestProductProtectionArtifactsOwnerMismatchBlocks(t *testing.T) {
	f := newEntryLifecycleFixture(t, Long)
	f.armPair(t)
	plan := f.plan
	plan.ModelID = "different_model_v3"
	plan.Digest = plan.digest()
	if err := plan.validate(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(f.root, "trading", "protection_submissions", plan.GroupID+".json")
	if err := os.WriteFile(path, mustMarshalProtectionArtifact(t, plan), 0600); err != nil {
		t.Fatal(err)
	}
	got, err := productInspectorFixture(t, f).InspectOutstandingProtections(context.Background())
	if err == nil || !incidentHasCode(got, "PROTECTION_PLAN_ENTRY_OWNER_MISMATCH") {
		t.Fatalf("mismatched plan escaped: %+v %v", got, err)
	}
	assertNonAuthorizingPortfolio(t, got)
}

// Use the same JSON serializer as the production plan so test corruption is
// deliberate, not an artifact of a different representation.
func mustMarshalProtectionArtifact(t *testing.T, p ProtectionDispatchPlan) []byte {
	t.Helper()
	b, err := json.Marshal(p)
	if err != nil || !strings.Contains(string(b), "group_id") {
		t.Fatalf("serialize plan: %v", err)
	}
	return b
}
