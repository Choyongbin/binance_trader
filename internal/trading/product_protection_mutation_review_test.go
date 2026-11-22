package trading

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func testDeniedProductMutation(t *testing.T, f *entryLifecycleFixture, got ProductProtectionMutationReview) {
	t.Helper()
	if got.ClientOrderID != f.reservation.ClientOrderID || got.GroupID != f.plan.GroupID ||
		!got.ManualReviewRequired || got.TradingAuthorized || got.SubmitAuthorized ||
		got.CancelAuthorized || got.SettlementAuthorized || len(got.Blockers) == 0 ||
		f.signed.submitCalls != 0 || f.signed.cancelCalls != 0 || f.transport.m.submitCalls != 0 {
		t.Fatalf("product review authorized exchange side effect: %+v POST=%d DELETE=%d", got, f.signed.submitCalls, f.signed.cancelCalls)
	}
}

func TestProductMutationBoundaryPreparedAndRestartBothSides(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			f := newEntryLifecycleFixture(t, side)
			product := productInspectorFixture(t, f)
			// The STOP generation is not used as a GET-only review grant.
			control, err := OpenAutoRunControl(f.root)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = control.Start(); err != nil {
				t.Fatal(err)
			}
			if err = control.Stop(); err != nil {
				t.Fatal(err)
			}
			got, err := product.ReviewProtectionMutation(context.Background(), f.reservation, ProductProtectionArm)
			if err != nil || got.LifecycleState != EntryLifecyclePreparedOnly || got.State != ProductMutationReviewBlocked {
				t.Fatalf("prepared: %+v %v", got, err)
			}
			testDeniedProductMutation(t, f, got)
			// A new product handle must re-read signed account state.
			f.armPair(t)
			product = productInspectorFixture(t, f)
			got, err = product.ReviewProtectionMutation(context.Background(), f.reservation, ProductProtectionArm)
			if err != nil || got.LifecycleState != EntryLifecyclePairActive || got.State != ProductMutationReviewBlocked {
				t.Fatalf("active pair: %+v %v", got, err)
			}
			testDeniedProductMutation(t, f, got)
			got, err = product.ReviewProtectionMutation(context.Background(), f.reservation, ProductProtectionCancel)
			if err != nil || got.LifecycleState != EntryLifecyclePairActive || got.State != ProductMutationReviewIncident {
				t.Fatalf("no cancelable sibling: %+v %v", got, err)
			}
			testDeniedProductMutation(t, f, got)
			armed, err := product.Arm(context.Background(), f.plan.GroupID, OwnerPosition{})
			if !errors.Is(err, ErrProtectionApproval) || armed.Armed {
				t.Fatalf("actual Arm enabled: %+v %v", armed, err)
			}
			if _, err = product.CancelTriggeredSibling(context.Background(), f.plan.GroupID, ProtectionEvidence{}); !errors.Is(err, ErrProtectionApproval) {
				t.Fatalf("actual cancel enabled: %v", err)
			}
		})
	}
}

func TestProductMutationBoundaryAmbiguityForeignAndFailClosed(t *testing.T) {
	f := newEntryLifecycleFixture(t, Long)
	product := productInspectorFixture(t, f)
	if err := f.protection.MarkMayHaveSent(f.plan.GroupID, "tp", true); err != nil {
		t.Fatal(err)
	}
	got, err := product.ReviewProtectionMutation(context.Background(), f.reservation, ProductProtectionArm)
	if err != nil || got.State != ProductMutationReviewReadOnly || !strings.Contains(strings.Join(got.Blockers, ","), "NO_POST_RETRY") {
		t.Fatalf("ambiguous POST: %+v %v", got, err)
	}
	testDeniedProductMutation(t, f, got)
	f.signed.failGET = true
	got, err = product.ReviewProtectionMutation(context.Background(), f.reservation, ProductProtectionArm)
	if err == nil || got.State != ProductMutationReviewIncident {
		t.Fatalf("GET failure: %+v %v", got, err)
	}
	testDeniedProductMutation(t, f, got)
	f.signed.failGET = false
	f.signed.foreignAlgo = true
	got, err = product.ReviewProtectionMutation(context.Background(), f.reservation, ProductProtectionArm)
	if err == nil || got.State != ProductMutationReviewIncident {
		t.Fatalf("foreign Algo: %+v %v", got, err)
	}
	testDeniedProductMutation(t, f, got)
}

func TestProductMutationBoundaryRejectsUntrustedInputsWithoutNetwork(t *testing.T) {
	f := newEntryLifecycleFixture(t, Short)
	product := productInspectorFixture(t, f)
	startGets := f.signed.getCalls
	cases := []struct {
		name   string
		ctx    context.Context
		entry  Reservation
		action ProductProtectionMutation
	}{
		{"unknown-action", context.Background(), f.reservation, "POST_ANYTHING"},
		{"nil-context", nil, f.reservation, ProductProtectionArm},
		{"invalid-entry", context.Background(), Reservation{}, ProductProtectionCancel},
		{"nil-runtime", context.Background(), f.reservation, ProductProtectionArm},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			target := product
			if tc.name == "nil-runtime" {
				target = nil
			}
			got, err := target.ReviewProtectionMutation(tc.ctx, tc.entry, tc.action)
			if err == nil || got.TradingAuthorized || got.SubmitAuthorized || got.CancelAuthorized || got.SettlementAuthorized || !got.ManualReviewRequired {
				t.Fatalf("invalid input was accepted: %+v %v", got, err)
			}
		})
	}
	if f.signed.getCalls != startGets || f.signed.submitCalls != 0 || f.signed.cancelCalls != 0 {
		t.Fatal("invalid input triggered signed or mutation I/O")
	}
}

func TestProductMutationBoundaryTriggeredCancelNeverGrantsDELETE(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			f := newEntryLifecycleFixture(t, side)
			f.armPair(t)
			f.trigger(t, "tp", "0.01")
			product := productInspectorFixture(t, f)
			got, err := product.ReviewProtectionMutation(context.Background(), f.reservation, ProductProtectionCancel)
			if err != nil || got.State != ProductMutationReviewBlocked || got.LifecycleState != EntryLifecycleCancelReview {
				t.Fatalf("signed fully closed exit cancel review: %+v %v", got, err)
			}
			if !strings.Contains(strings.Join(got.Blockers, ","), "INDEPENDENT_ACCOUNT_WIDE_OWNERSHIP_PROOF_MISSING") {
				t.Fatalf("did not require independent history: %+v", got)
			}
			testDeniedProductMutation(t, f, got)
			// Even with a durable MAY_HAVE_SENT DELETE marker, the only safe
			// operation is signed GET, not an automatic second DELETE.
			cancelIntent, err := f.cancels.Plan(f.plan.GroupID, 2202, f.plan.SL.ClientAlgoID)
			if err != nil {
				t.Fatal(err)
			}
			if err = f.cancels.MarkMayHaveSent(cancelIntent); err != nil {
				t.Fatal(err)
			}
			got, err = product.ReviewProtectionMutation(context.Background(), f.reservation, ProductProtectionCancel)
			if err != nil || got.State != ProductMutationReviewReadOnly || got.LifecycleState != EntryLifecycleCancelAmbiguous {
				t.Fatalf("ambiguous DELETE review: %+v %v", got, err)
			}
			testDeniedProductMutation(t, f, got)
		})
	}
}

func TestProductMutationBoundaryPartialExitRemainsIncident(t *testing.T) {
	f := newEntryLifecycleFixture(t, Long)
	f.armPair(t)
	f.trigger(t, "tp", "0.004")
	product := productInspectorFixture(t, f)
	for _, action := range []ProductProtectionMutation{ProductProtectionArm, ProductProtectionCancel} {
		got, err := product.ReviewProtectionMutation(context.Background(), f.reservation, action)
		if err != nil || got.State != ProductMutationReviewIncident || got.LifecycleState != EntryLifecyclePartialHalt {
			t.Fatalf("partial exit %s: %+v %v", action, got, err)
		}
		testDeniedProductMutation(t, f, got)
	}
}
