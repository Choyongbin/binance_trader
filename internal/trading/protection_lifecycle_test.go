package trading

import (
	"testing"
	"time"
)

func evidencePlan(t *testing.T, now time.Time) (ProtectionPreview, ProtectionEvidence) {
	t.Helper()
	owner := OwnerPosition{Source: Model, ModelID: "A", Side: Long, Symbol: "BTCUSDT", Quantity: "0.010", AverageEntry: "100000"}
	q := Quote{Symbol: "BTCUSDT", Price: "100000", ReceivedAt: now}
	p, e := PreviewProtection(owner, sizingRules(now), q, q, "group-p-001", now)
	if e != nil {
		t.Fatal(e)
	}
	return p, ProtectionEvidence{Owner: owner, TP: AlgoObservation{ClientID: p.TP.ClientAlgoID, AlgoID: 111, Symbol: "BTCUSDT", PositionSide: Long, Status: "NEW", Quantity: "0.010", ObservedAt: now, FromSignedGet: true}, SL: AlgoObservation{ClientID: p.SL.ClientAlgoID, AlgoID: 112, Symbol: "BTCUSDT", PositionSide: Long, Status: "NEW", Quantity: "0.010", ObservedAt: now, FromSignedGet: true}, RemainingOwnerQty: "0.010", FullSignedOwnershipAudit: true, SignedAccountAsOf: now}
}
func TestProtectionSignedReconciliationFlow(t *testing.T) {
	now := time.Now().UTC()
	p, e := evidencePlan(t, now)
	a := EvaluateProtection(p, e, now)
	if a.State != ProtectionMonitoring {
		t.Fatalf("monitor %+v", a)
	}
	e.TP.Status = "TRIGGERED"
	a = EvaluateProtection(p, e, now)
	if a.State != ProtectionCancelSibling || a.SiblingAlgoID != 112 {
		t.Fatalf("sibling intent %+v", a)
	}
	e.SL.Status = "TRIGGERED"
	if a = EvaluateProtection(p, e, now); a.State != ProtectionHalt {
		t.Fatalf("simultaneous trigger not blocked %+v", a)
	}
	e.SL.Status = "CANCELED"
	if a = EvaluateProtection(p, e, now); a.State != ProtectionHalt {
		t.Fatalf("unimported fills falsely closed %+v", a)
	}
	e.TP.ActualOrderID = "889"
	e.TP.ActualQty = "0.010"
	e.RemainingOwnerQty = "0"
	if a = EvaluateProtection(p, e, now); a.State != ProtectionClosed {
		t.Fatalf("verified closure %+v", a)
	}
	e.FullSignedOwnershipAudit = false
	if a = EvaluateProtection(p, e, now); a.State != ProtectionHalt {
		t.Fatalf("no audit falsely closed %+v", a)
	}
}
func TestProtectionStaleOrForeignLegFails(t *testing.T) {
	now := time.Now().UTC()
	p, e := evidencePlan(t, now)
	e.SL.ObservedAt = now.Add(-10 * time.Second)
	if a := EvaluateProtection(p, e, now); a.State != ProtectionHalt {
		t.Fatal("stale signed GET passed")
	}
	e.SL.ObservedAt = now
	e.SL.ClientID = "other-owner"
	if a := EvaluateProtection(p, e, now); a.State != ProtectionHalt {
		t.Fatal("foreign order accepted")
	}
	e.SL.ClientID = p.SL.ClientAlgoID
	e.SL.Status = "REJECTED"
	if a := EvaluateProtection(p, e, now); a.State != ProtectionHalt {
		t.Fatal("missing SL accepted")
	}
}
func TestProtectionPartialOwnerShrinkCannotReuseFullSizedLegs(t *testing.T) {
	now := time.Now().UTC()
	p, e := evidencePlan(t, now)
	e.RemainingOwnerQty = "0.003"
	if a := EvaluateProtection(p, e, now); a.State != ProtectionHalt {
		t.Fatalf("oversized live protection accepted: %+v", a)
	}
	e.TP.Status = "FINISHED"
	e.SL.Status = "CANCELED"
	e.RemainingOwnerQty = "0"
	e.TP.ActualOrderID = "123"
	e.TP.ActualQty = "0.003"
	if a := EvaluateProtection(p, e, now); a.State != ProtectionHalt {
		t.Fatalf("partial fill falsely closed whole owner %+v", a)
	}
}
