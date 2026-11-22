package trading

import (
	"context"
	"strings"
	"testing"

	"binance_trader/internal/exchange"
)

func assertNonAuthorizingPortfolio(t *testing.T, result PendingProtectionInventory) {
	t.Helper()
	if result.TradingAuthorized || result.SubmitAuthorized || result.CancelAuthorized ||
		result.SettlementAuthorized || result.ContinuousMonitoring || !result.ManualReviewRequired {
		t.Fatalf("inventory escaped safety boundary: %+v", result)
	}
}

func TestProductProtectionInventoryLongShortStopRestartReadOnly(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			f := newEntryLifecycleFixture(t, side)
			product := productInspectorFixture(t, f)
			control, err := OpenAutoRunControl(f.root)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := control.Start(); err != nil {
				t.Fatal(err)
			}
			if err := control.Stop(); err != nil {
				t.Fatal(err)
			}
			before, err := product.InspectOutstandingProtections(context.Background())
			if err == nil || before.State != PendingProtectionInspectionBlocked || before.InspectedEntries != 1 ||
				len(before.Entries) != 1 || before.Entries[0].State != EntryLifecyclePreparedOnly ||
				len(before.Incidents) != 1 || before.Incidents[0].Code != "ENTRY_TP_SL_NOT_SUBMITTED" {
				t.Fatalf("STOP inventory: %+v %v", before, err)
			}
			assertNonAuthorizingPortfolio(t, before)
			f.armPair(t)
			product = productInspectorFixture(t, f)
			after, err := product.InspectOutstandingProtections(context.Background())
			if err != nil || after.State != PendingProtectionInspectionObserved || after.InspectedEntries != 1 ||
				len(after.Entries) != 1 || after.Entries[0].State != EntryLifecyclePairActive {
				t.Fatalf("restarted inventory: %+v %v", after, err)
			}
			assertNonAuthorizingPortfolio(t, after)
			assertReadOnlyLifecycle(t, f, after.Entries[0])
		})
	}
}

func TestProductProtectionInventorySignedFailureAndAmbiguityBlock(t *testing.T) {
	f := newEntryLifecycleFixture(t, Long)
	product := productInspectorFixture(t, f)
	if err := f.protection.MarkMayHaveSent(f.plan.GroupID, "tp", true); err != nil {
		t.Fatal(err)
	}
	unknown, err := product.InspectOutstandingProtections(context.Background())
	if err == nil || unknown.Entries[0].State != EntryLifecycleUnknownSubmission || unknown.State != PendingProtectionInspectionBlocked {
		t.Fatalf("ambiguous POST must be observed without replay: %+v %v", unknown, err)
	}
	assertNonAuthorizingPortfolio(t, unknown)
	f.signed.failGET = true
	blocked, err := product.InspectOutstandingProtections(context.Background())
	if err == nil || blocked.State != PendingProtectionInspectionBlocked || len(blocked.Blockers) == 0 ||
		len(blocked.Entries) != 1 || blocked.Entries[0].State != EntryLifecycleBlocked {
		t.Fatalf("signed GET failure accepted: %+v %v", blocked, err)
	}
	assertNonAuthorizingPortfolio(t, blocked)
	if f.signed.submitCalls != 0 || f.signed.cancelCalls != 0 || f.transport.m.submitCalls != 0 {
		t.Fatal("inventory triggered exchange writes")
	}
}

func TestProductProtectionInventoryInvalidContextOrDependency(t *testing.T) {
	f := newEntryLifecycleFixture(t, Long)
	product := productInspectorFixture(t, f)
	for _, ctx := range []context.Context{nil, canceledProtectionContext()} {
		got, err := product.InspectOutstandingProtections(ctx)
		if err == nil || got.State != PendingProtectionInspectionBlocked || !got.ManualReviewRequired {
			t.Fatalf("bad context: %+v %v", got, err)
		}
		assertNonAuthorizingPortfolio(t, got)
	}
	var absent *DemoProtectionRuntimeCoordinator
	got, err := absent.InspectOutstandingProtections(context.Background())
	if err == nil || !strings.Contains(got.Blockers[0], "missing") {
		t.Fatalf("nil runtime: %+v %v", got, err)
	}
	assertNonAuthorizingPortfolio(t, got)
}

func canceledProtectionContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func TestProductProtectionInventoryOpenSiblingAndCancelAckAmbiguityAreIncidents(t *testing.T) {
	f := newEntryLifecycleFixture(t, Long)
	f.armPair(t)
	f.trigger(t, "tp", "0.01")
	product := productInspectorFixture(t, f)
	first, err := product.InspectOutstandingProtections(context.Background())
	if err == nil || first.State != PendingProtectionInspectionBlocked || len(first.Incidents) != 1 ||
		first.Incidents[0].Code != "TRIGGERED_SIBLING_STILL_ACTIVE" || first.Entries[0].State != EntryLifecycleCancelReview {
		t.Fatalf("triggered sibling must be incident: %+v %v", first, err)
	}
	if first.Incidents[0].ClientOrderID != f.reservation.ClientOrderID || !first.Incidents[0].ManualReviewRequired {
		t.Fatal("incident is not bound to durable order and operator review")
	}
	intent, err := f.cancels.Plan(f.plan.GroupID, 2202, f.plan.SL.ClientAlgoID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.cancels.MarkMayHaveSent(intent); err != nil {
		t.Fatal(err)
	}
	// Reopening cancels simulates a process restart after a lost DELETE ACK.
	f.cancels, err = NewCancelIntentStore(f.root)
	if err != nil {
		t.Fatal(err)
	}
	product = productInspectorFixture(t, f)
	ambiguous, err := product.InspectOutstandingProtections(context.Background())
	if err == nil || ambiguous.State != PendingProtectionInspectionBlocked || len(ambiguous.Incidents) != 1 ||
		ambiguous.Incidents[0].Code != "SIBLING_DELETE_MAY_HAVE_SENT" || !ambiguous.Entries[0].CancelMayHaveBeenSent {
		t.Fatalf("ambiguous DELETE must not appear healthy: %+v %v", ambiguous, err)
	}
	assertNonAuthorizingPortfolio(t, ambiguous)
	if f.signed.submitCalls != 0 || f.signed.cancelCalls != 0 || f.transport.m.submitCalls != 0 {
		t.Fatal("read-only portfolio issued exchange POST/DELETE")
	}
}

func TestProductProtectionInventoryPartialExitAndDoubleTriggerIncident(t *testing.T) {
	for _, scenario := range []struct {
		name, side string
		code       string
	}{
		{"partial", "LONG", "PARTIAL_EXIT_UNRECONCILED"},
		{"double", "SHORT", "TP_SL_DOUBLE_TRIGGER"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			side := Long
			if scenario.side == "SHORT" {
				side = Short
			}
			f := newEntryLifecycleFixture(t, side)
			f.armPair(t)
			if scenario.name == "partial" {
				f.trigger(t, "tp", "0.004")
			} else {
				// Each separate leg is FINISHED and has signed trades, no OCO assumption.
				f.trigger(t, "tp", "0.004")
				inst := f.plan.SL
				a := f.signed.armingTransportAdapter.m.algos[2202]
				a.Status, a.ActualOrderID, a.ActualQty = "FINISHED", "95612", "0.006"
				f.signed.armingTransportAdapter.m.algos[2202] = a
				f.signed.armingTransportAdapter.m.order = nil
				f.signed.trades[95612] = []exchange.AccountTrade{protectionTrade(95612, 55672, inst.Side, string(inst.PositionSide), "0.006", "99900", "-2")}
				f.signed.zeroSide = true
			}
			product := productInspectorFixture(t, f)
			got, err := product.InspectOutstandingProtections(context.Background())
			if err == nil || got.State != PendingProtectionInspectionBlocked || len(got.Incidents) != 1 ||
				got.Incidents[0].Code != scenario.code {
				t.Fatalf("%s not classified: %+v %v", scenario.name, got, err)
			}
			assertNonAuthorizingPortfolio(t, got)
			if f.signed.submitCalls != 0 || f.signed.cancelCalls != 0 || f.transport.m.submitCalls != 0 {
				t.Fatal("portfolio inspection issued exchange mutation")
			}
		})
	}
}

func TestProtectionIncidentClassificationFailClosedMatrix(t *testing.T) {
	for _, tc := range []struct{ state, code string }{
		{EntryLifecyclePreparedOnly, "ENTRY_TP_SL_NOT_SUBMITTED"},
		{EntryLifecycleUnknownSubmission, "PROTECTION_POST_MAY_HAVE_SENT"},
		{EntryLifecycleSingleLeg, "SINGLE_PROTECTION_LEG"},
		{EntryLifecycleCancelReview, "TRIGGERED_SIBLING_STILL_ACTIVE"},
		{EntryLifecycleCancelAmbiguous, "SIBLING_DELETE_MAY_HAVE_SENT"},
		{EntryLifecycleClosedNotSettled, "CLOSED_FINANCIAL_SETTLEMENT_PENDING"},
		{EntryLifecyclePartialHalt, "PARTIAL_EXIT_UNRECONCILED"},
		{EntryLifecycleDoubleTriggerHalt, "TP_SL_DOUBLE_TRIGGER"},
		{EntryLifecycleBlocked, "SIGNED_PROTECTION_REVIEW_BLOCKED"},
		{"FUTURE_UNKNOWN_STATE", "UNKNOWN_PROTECTION_LIFECYCLE_STATE"},
	} {
		t.Run(tc.state, func(t *testing.T) {
			incident, flagged := protectionIncidentForReview(EntryProtectionLifecycleReview{State: tc.state,
				ClientOrderID: "client", GroupID: "entry_client", Reason: "signed review incident"})
			if !flagged || incident.Code != tc.code || incident.ClientOrderID != "client" || !incident.ManualReviewRequired {
				t.Fatalf("classification not fail closed: %+v flagged=%v", incident, flagged)
			}
		})
	}
	if incident, flagged := protectionIncidentForReview(EntryProtectionLifecycleReview{State: EntryLifecyclePairActive}); flagged || incident.Code != "" {
		t.Fatal("active pair should be non-authorizing observed, not a fabricated incident")
	}
}
