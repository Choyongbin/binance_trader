package trading

import (
	"context"
	"testing"
)

// These are transitions after an independently fresh signed read; no case
// authorizes a production exchange mutation, including a fully closed exit.
func TestProtectionTransitionPolicySharedProductAndOffline(t *testing.T) {
	cases := []struct {
		name           string
		state          string
		recorded, sent bool
		kind           protectionTransitionKind
		action         EntryProtectionRehearsalAction
		recoverOnly    bool
		reject         bool
	}{
		{"prepared", EntryLifecyclePreparedOnly, false, false, protectionNeedArmReview, EntryRehearsalArm, false, false},
		{"post-unknown", EntryLifecycleUnknownSubmission, false, false, protectionRecoverPostOnly, EntryRehearsalArm, true, false},
		{"pair", EntryLifecyclePairActive, false, false, protectionMonitorPair, EntryRehearsalInspect, false, false},
		{"cancel-review", EntryLifecycleCancelReview, false, false, protectionNeedCancelReview, EntryRehearsalCancel, false, false},
		{"delete-may-have-sent", EntryLifecycleCancelAmbiguous, true, true, protectionRecoverDeleteOnly, EntryRehearsalCancel, true, false},
		{"closed", EntryLifecycleClosedNotSettled, true, false, protectionNeedSettlement, EntryRehearsalInspect, false, false},
		{"closed-delete-recovery", EntryLifecycleClosedNotSettled, true, true, protectionRecoverDeleteOnly, EntryRehearsalCancel, true, false},
		{"single-leg", EntryLifecycleSingleLeg, false, false, "", "", false, true},
		{"partial", EntryLifecyclePartialHalt, false, false, "", "", false, true},
		{"double", EntryLifecycleDoubleTriggerHalt, false, false, "", "", false, true},
		{"blocked", EntryLifecycleBlocked, false, false, "", "", false, true},
		{"future", "FUTURE_STATE", false, false, "", "", false, true},
		{"missing-delete-marker", EntryLifecycleCancelAmbiguous, false, true, "", "", false, true},
		{"cancel-without-may-have-sent", EntryLifecycleCancelAmbiguous, true, false, "", "", false, true},
		{"cancel-recorded-while-pair-active", EntryLifecyclePairActive, true, false, "", "", false, true},
		{"delete-sent-while-prepared", EntryLifecyclePreparedOnly, true, true, "", "", false, true},
		{"delete-sent-while-unattempted", EntryLifecycleCancelReview, true, true, "", "", false, true},
		{"closed-delete-without-intent", EntryLifecycleClosedNotSettled, false, true, "", "", false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := EntryProtectionLifecycleReview{State: tc.state,
				CancelIntentRecorded: tc.recorded, CancelMayHaveBeenSent: tc.sent}
			policy, err := classifyProtectionTransition(input)
			if tc.reject {
				if err == nil {
					t.Fatalf("unsafe state accepted: %+v", policy)
				}
				action, _, aerr := nextOfflineSupervisorAction(input, nil)
				if aerr == nil || action != "" {
					t.Fatalf("unsafe offline mutation: %q, %v", action, aerr)
				}
				return
			}
			if err != nil || policy.kind != tc.kind || policy.offlineAction != tc.action || policy.signedRecoveryOnly != tc.recoverOnly {
				t.Fatalf("wrong policy %+v, err %v", policy, err)
			}
			action, _, offlineErr := nextOfflineSupervisorAction(input, nil)
			if offlineErr != nil {
				t.Fatal(offlineErr)
			}
			if tc.kind == protectionNeedCancelReview {
				if action != EntryRehearsalInspect {
					t.Fatalf("cancel without proof allowed: %q", action)
				}
			} else if action != tc.action {
				t.Fatalf("offline/product divergence for %s: %q versus %q", tc.name, action, tc.action)
			}
		})
	}
	privileged := EntryProtectionLifecycleReview{State: EntryLifecyclePairActive, TradingAuthorized: true}
	if got, err := classifyProtectionTransition(privileged); err == nil {
		t.Fatalf("invented product authorization accepted: %+v", got)
	}
}

func TestProductMutationReviewMatchesSharedPolicyWithoutWrites(t *testing.T) {
	f := newEntryLifecycleFixture(t, Long)
	p := productInspectorFixture(t, f)
	cases := []struct {
		action ProductProtectionMutation
		kind   protectionTransitionKind
	}{
		{ProductProtectionArm, protectionNeedArmReview},
		{ProductProtectionCancel, protectionNeedArmReview},
	}
	for _, tc := range cases {
		got, err := p.ReviewProtectionMutation(context.Background(), f.reservation, tc.action)
		if err != nil || got.NextStep != string(tc.kind) || got.SubmitAuthorized || got.CancelAuthorized ||
			got.TradingAuthorized || got.SettlementAuthorized || got.SignedRecoveryOnly || !got.ManualReviewRequired {
			t.Fatalf("bad product transition %+v, %v", got, err)
		}
	}
	if f.signed.submitCalls != 0 || f.signed.cancelCalls != 0 {
		t.Fatalf("product review sent live mutations: POST %d DELETE %d", f.signed.submitCalls, f.signed.cancelCalls)
	}
	f.armPair(t)
	got, err := p.ReviewProtectionMutation(context.Background(), f.reservation, ProductProtectionArm)
	if err != nil || got.NextStep != string(protectionMonitorPair) || got.State != ProductMutationReviewBlocked || got.SubmitAuthorized {
		t.Fatalf("active pair treated as mutation approval %+v, %v", got, err)
	}
}

func TestProductMutationReviewAmbiguousPostAndTriggeredDeleteAreNeverReplayable(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			f := newEntryLifecycleFixture(t, side)
			product := productInspectorFixture(t, f)
			if err := f.protection.MarkMayHaveSent(f.plan.GroupID, "tp", true); err != nil {
				t.Fatal(err)
			}
			got, err := product.ReviewProtectionMutation(context.Background(), f.reservation, ProductProtectionArm)
			if err != nil || got.NextStep != string(protectionRecoverPostOnly) || !got.SignedRecoveryOnly ||
				got.State != ProductMutationReviewReadOnly || got.SubmitAuthorized || got.CancelAuthorized {
				t.Fatalf("POST ACK loss was replayable: %+v, %v", got, err)
			}
			if f.signed.submitCalls != 0 || f.signed.cancelCalls != 0 {
				t.Fatal("signed-only preview posted to exchange")
			}
		})
	}
}
