package trading

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func draftProtectionPlan(t *testing.T, owner OwnerPosition, group string, now time.Time) ProtectionDispatchPlan {
	t.Helper()
	q := Quote{Symbol: "BTCUSDT", Price: "100000", ReceivedAt: now}
	draft, err := PreviewProtection(owner, sizingRules(now), q, q, group, now)
	if err != nil {
		t.Fatal(err)
	}
	p := ProtectionDispatchPlan{Schema: 1, GroupID: group, Source: owner.Source, ModelID: owner.ModelID, PositionSide: owner.Side,
		TP: draft.TP, SL: draft.SL, CreatedAt: now.Format(time.RFC3339Nano)}
	p.Digest = p.digest()
	return p
}

func TestProtectionArmingPolicyBlocksPooledSameSideOwners(t *testing.T) {
	now := time.Now().UTC()
	one := draftProtectionPlan(t, OwnerPosition{Source: Model, ModelID: "trend", Symbol: "BTCUSDT", Side: Long, Quantity: "0.01", AverageEntry: "100000"}, "protect-trend-001", now)
	two := draftProtectionPlan(t, OwnerPosition{Source: Model, ModelID: "counter", Symbol: "BTCUSDT", Side: Long, Quantity: "0.02", AverageEntry: "100000"}, "protect-counter-001", now)

	r := ReviewProtectionArmingPlans([]ProtectionDispatchPlan{one, two})
	if r.State != ProtectionArmingPooledConflict || !r.SameSideMultiOwnerPresent || r.PlanCount != 2 || r.PerOwnerAlgoArmingAllowed || r.TradingAuthorized || !r.ManualReviewRequired || len(r.Sides) != 1 {
		t.Fatalf("same-side plans not blocked: %+v", r)
	}
	s := r.Sides[0]
	if s.Side != Long || s.OwnerGroups != 2 || s.OwnedBTC != "0.03" || s.IndependentConditionalLegs != 4 || s.WorstCaseRequestedCloseBTC != "0.06" || s.ExcessIfAllIndependentLegsFire != "0.03" {
		t.Fatalf("wrong pooled exposure: %+v", s)
	}
}

func TestProtectionArmingPolicyAllowsOnlyDraftReviewAcrossOppositeSides(t *testing.T) {
	now := time.Now().UTC()
	long := draftProtectionPlan(t, OwnerPosition{Source: Manual, Symbol: "BTCUSDT", Side: Long, Quantity: "0.01", AverageEntry: "100000"}, "protect-long-001", now)
	short := draftProtectionPlan(t, OwnerPosition{Source: Model, ModelID: "shorter", Symbol: "BTCUSDT", Side: Short, Quantity: "0.005", AverageEntry: "100000"}, "protect-short-001", now)
	r := ReviewProtectionArmingPlans([]ProtectionDispatchPlan{short, long})
	if r.State != ProtectionArmingSingleOwnerPairOnly || r.SameSideMultiOwnerPresent || r.PerOwnerAlgoArmingAllowed || r.TradingAuthorized || len(r.Sides) != 2 {
		t.Fatalf("opposite-side draft review wrong: %+v", r)
	}
	if r.Sides[0].Side != Long || r.Sides[1].Side != Short {
		t.Fatalf("non-deterministic side order: %+v", r.Sides)
	}
}

func TestProtectionSubmissionStoreRejectsSecondSameSideGroup(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenProtectionSubmissionStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	quote := Quote{Symbol: "BTCUSDT", Price: "100000", ReceivedAt: now}
	longA := OwnerPosition{Source: Model, ModelID: "trend", Symbol: "BTCUSDT", Side: Long, Quantity: "0.01", AverageEntry: "100000"}
	draftA, err := PreviewProtection(longA, sizingRules(now), quote, quote, "protect-long-a", now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Prepare("protect-long-a", longA, draftA); err != nil {
		t.Fatal(err)
	}

	longB := OwnerPosition{Source: Model, ModelID: "counter", Symbol: "BTCUSDT", Side: Long, Quantity: "0.02", AverageEntry: "100000"}
	draftB, err := PreviewProtection(longB, sizingRules(now), quote, quote, "protect-long-b", now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Prepare("protect-long-b", longB, draftB); !errors.Is(err, ErrPooledProtectionConflict) {
		t.Fatalf("second same-side group accepted: %v", err)
	}

	short := OwnerPosition{Source: Manual, Symbol: "BTCUSDT", Side: Short, Quantity: "0.005", AverageEntry: "100000"}
	draftShort, err := PreviewProtection(short, sizingRules(now), quote, quote, "protect-short-a", now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Prepare("protect-short-a", short, draftShort); err != nil {
		t.Fatalf("opposite Hedge side should remain separately stageable: %v", err)
	}
	if review, err := store.ReviewAllPlans(); err != nil || review.State != ProtectionArmingSingleOwnerPairOnly || review.PlanCount != 2 {
		t.Fatalf("unexpected store review: %+v %v", review, err)
	}
}

func TestProtectionAuditDetectsLegacySameSidePlansWithoutDeletingThem(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenProtectionSubmissionStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	a := draftProtectionPlan(t, OwnerPosition{Source: Model, ModelID: "old-a", Symbol: "BTCUSDT", Side: Long, Quantity: "0.01", AverageEntry: "100000"}, "legacy-long-a", now)
	b := draftProtectionPlan(t, OwnerPosition{Source: Model, ModelID: "old-b", Symbol: "BTCUSDT", Side: Long, Quantity: "0.02", AverageEntry: "100000"}, "legacy-long-b", now)
	root := filepath.Join(dir, "trading", "protection_submissions")
	for _, p := range []ProtectionDispatchPlan{a, b} {
		raw, _ := json.MarshalIndent(p, "", "  ")
		if err := os.WriteFile(filepath.Join(root, p.GroupID+".json"), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	r, err := store.ReviewAllPlans()
	if err != nil || r.State != ProtectionArmingPooledConflict || r.PlanCount != 2 {
		t.Fatalf("legacy conflict not surfaced: %+v %v", r, err)
	}
	if _, err := store.AuditAll(); !errors.Is(err, ErrPooledProtectionConflict) {
		t.Fatalf("legacy unsafe plans not fail-closed: %v", err)
	}
	for _, p := range []ProtectionDispatchPlan{a, b} {
		if _, err := os.Stat(filepath.Join(root, p.GroupID+".json")); err != nil {
			t.Fatalf("audit mutated historical plan %s: %v", p.GroupID, err)
		}
	}
}
