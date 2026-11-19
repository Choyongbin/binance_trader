package trading

import (
	"context"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

// The test follows ONE imported entry through both durable protection legs.
// Each restart inspection is a pure read: no POST, DELETE or retry is issued.
func TestEntryProtectionContinuityMockEndToEnd(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			r, lifecycle, owner, _, _ := handoffFixture(t, side)
			store, plan, _, _ := testProtectionGroup(t, side)
			mock := &armingMock{owner: owner, plan: plan, algos: map[int64]exchange.DemoAlgoResult{}}
			transport := &armingTransportAdapter{m: mock}
			inspect := func(want string) EntryProtectionContinuity {
				t.Helper()
				snapshot, err := transport.FetchDemoExecutionPreflight(context.Background(), "k", "s", "BTCUSDT")
				if err != nil {
					t.Fatal(err)
				}
				got, err := InspectEntryProtectionContinuityGroup(store, plan.GroupID, r, lifecycle, owner, snapshot)
				if err != nil || got.State != want || got.TradingAuthorized || got.ProtectionSubmitAuthorized || got.RetryAuthorized || got.SettlementAuthorized || !got.ManualReviewRequired {
					t.Fatalf("state=%+v err=%v want=%s", got, err, want)
				}
				return got
			}
			inspect("ENTRY_IMPORTED_PROTECTION_UNSENT_REVIEW")
			if err := store.MarkMayHaveSent(plan.GroupID, "tp", true); err != nil {
				t.Fatal(err)
			}
			inspect("AMBIGUOUS_SUBMISSION_NO_RETRY")
			// A mock signed read-back of the TP leg, then restart and inspect.
			mock.algos[901] = algoFromInstruction(901, plan.TP, "NEW")
			mock.order = append(mock.order, 901)
			if err := store.RecordObserved(context.Background(), plan.GroupID, "tp", 901, transport, "k", "s"); err != nil {
				t.Fatal(err)
			}
			inspect("ONE_LEG_OBSERVED_MANUAL_REVIEW")
			if err := store.MarkMayHaveSent(plan.GroupID, "sl", true); err != nil {
				t.Fatal(err)
			}
			inspect("AMBIGUOUS_SUBMISSION_NO_RETRY")
			mock.algos[902] = algoFromInstruction(902, plan.SL, "NEW")
			mock.order = append(mock.order, 902)
			if err := store.RecordObserved(context.Background(), plan.GroupID, "sl", 902, transport, "k", "s"); err != nil {
				t.Fatal(err)
			}
			if !inspect("TWO_LEGS_SIGNED_RECHECK_MANUAL_REVIEW").SignedSnapshotMatched {
				t.Fatal("signed view missing")
			}
			before := mock.submitCalls
			inspect("TWO_LEGS_SIGNED_RECHECK_MANUAL_REVIEW")
			if mock.submitCalls != before {
				t.Fatal("read-only inspection submitted an order")
			}
			// A stale or incomplete account snapshot is never interpreted as armed.
			signed, _ := transport.FetchDemoExecutionPreflight(context.Background(), "k", "s", "BTCUSDT")
			signed.FinishedAt = time.Now().UTC().Add(-time.Minute)
			blocked, err := InspectEntryProtectionContinuityGroup(store, plan.GroupID, r, lifecycle, owner, signed)
			if err != nil || blocked.State != "BLOCKED" || blocked.SignedSnapshotMatched {
				t.Fatalf("stale accepted: %+v %v", blocked, err)
			}
			signed, _ = transport.FetchDemoExecutionPreflight(context.Background(), "k", "s", "BTCUSDT")
			signed.OpenAlgos = nil
			blocked, err = InspectEntryProtectionContinuityGroup(store, plan.GroupID, r, lifecycle, owner, signed)
			if err != nil || blocked.State != "BLOCKED" {
				t.Fatalf("missing order list accepted: %+v %v", blocked, err)
			}
			corrupt := lifecycle
			corrupt.Events = append([]ExecutionLifecycleEvent{}, lifecycle.Events...)
			corrupt.Events[0].At = "2020-01-01T00:00:00Z"
			signed, _ = transport.FetchDemoExecutionPreflight(context.Background(), "k", "s", "BTCUSDT")
			blocked, err = InspectEntryProtectionContinuityGroup(store, plan.GroupID, r, corrupt, owner, signed)
			if err != nil || blocked.State != "BLOCKED" {
				t.Fatalf("broken lifecycle accepted: %+v %v", blocked, err)
			}
		})
	}
}

func TestEntryProtectionContinuityUnexpectedProtectionLegBlocks(t *testing.T) {
	r, lifecycle, owner, _, _ := handoffFixture(t, Long)
	store, plan, _, _ := testProtectionGroup(t, Long)
	mock := &armingMock{owner: owner, plan: plan, algos: map[int64]exchange.DemoAlgoResult{}}
	transport := &armingTransportAdapter{m: mock}
	signed, _ := transport.FetchDemoExecutionPreflight(context.Background(), "k", "s", "BTCUSDT")
	signed.OpenAlgos = []exchange.AlgoOrder{{AlgoID: 9999}}
	got, err := InspectEntryProtectionContinuityGroup(store, plan.GroupID, r, lifecycle, owner, signed)
	if err != nil || got.State != "BLOCKED" || got.SignedSnapshotMatched {
		t.Fatalf("extra algo accepted: %+v %v", got, err)
	}
}

func TestEntryProtectionContinuityActualMockArmingAndAmbiguousSecondLeg(t *testing.T) {
	for _, failedSecond := range []bool{false, true} {
		r, lifecycle, owner, _, _ := handoffFixture(t, Short)
		store, plan, _, _ := testProtectionGroup(t, Short)
		mock := &armingMock{owner: owner, plan: plan, algos: map[int64]exchange.DemoAlgoResult{}, failSecond: failedSecond}
		result, err := armWithJSONSnapshot(context.Background(), plan, owner, store, mock, failedSecond)
		if failedSecond {
			if err == nil || result.Armed {
				t.Fatalf("expected ambiguous SL result: %+v %v", result, err)
			}
		} else if err != nil || !result.Armed {
			t.Fatalf("mock arming failed: %+v %v", result, err)
		}
		if mock.submitCalls != 2 {
			t.Fatalf("want two one-shot mock submits, got %d", mock.submitCalls)
		}
		transport := &armingTransportAdapter{m: mock}
		signed, err := transport.FetchDemoExecutionPreflight(context.Background(), "k", "s", "BTCUSDT")
		if err != nil {
			t.Fatal(err)
		}
		review, err := InspectEntryProtectionContinuityGroup(store, plan.GroupID, r, lifecycle, owner, signed)
		if err != nil {
			t.Fatal(err)
		}
		if failedSecond {
			if review.State != "AMBIGUOUS_SUBMISSION_NO_RETRY" || review.RetryAuthorized {
				t.Fatalf("unsafe retry review %+v", review)
			}
		} else if review.State != "TWO_LEGS_SIGNED_RECHECK_MANUAL_REVIEW" || !review.SignedSnapshotMatched {
			t.Fatalf("unexpected arming review %+v", review)
		}
		review, err = InspectEntryProtectionContinuityGroup(store, plan.GroupID, r, lifecycle, owner, signed)
		if err != nil || review.TradingAuthorized || review.ProtectionSubmitAuthorized || review.SettlementAuthorized || mock.submitCalls != 2 {
			t.Fatalf("repeat inspection performed side effects: %+v err=%v calls=%d", review, err, mock.submitCalls)
		}
	}
}

// A self-consistent but incorrect entry lifecycle must not be paired with a
// different owner even if the signed pooled-side position equals that owner.
func TestEntryProtectionContinuityRejectsMismatchedEntryEvidence(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			r, life, owner, _, _ := handoffFixture(t, side)
			store, plan, _, _ := testProtectionGroup(t, side)
			mock := &armingMock{owner: owner, plan: plan, algos: map[int64]exchange.DemoAlgoResult{}}
			signed, err := (&armingTransportAdapter{m: mock}).FetchDemoExecutionPreflight(context.Background(), "k", "s", "BTCUSDT")
			if err != nil {
				t.Fatal(err)
			}
			check := func(name string, mutate func(*Reservation, *ExecutionLifecycle)) {
				t.Helper()
				t.Run(name, func(t *testing.T) {
					rr, ll := r, life
					ll.Events = append([]ExecutionLifecycleEvent(nil), life.Events...)
					mutate(&rr, &ll)
					got, err := InspectEntryProtectionContinuityGroup(store, plan.GroupID, rr, ll, owner, signed)
					if err != nil || got.State != "BLOCKED" || got.SignedSnapshotMatched != true && len(got.Blockers) == 0 ||
						got.TradingAuthorized || got.ProtectionSubmitAuthorized || got.RetryAuthorized || got.SettlementAuthorized {
						t.Fatalf("%s unsafe: %+v err=%v", name, got, err)
					}
				})
			}
			check("fully-rehashed-wrong-fill-quantity", func(_ *Reservation, l *ExecutionLifecycle) {
				l.Events[1].ExecutedQty = "0.02"
				l.Events[1].seal()
				l.Events[2].PreviousChecksum = l.Events[1].Checksum
				l.Events[2].ExecutedQty = "0.02"
				l.Events[2].seal()
			})
			check("root-plan-swapped-and-resealed", func(_ *Reservation, l *ExecutionLifecycle) {
				l.Root.Plan.Quantity = "0.02"
				l.Root.seal()
				l.Events[0].PreviousChecksum = l.Root.Checksum
				for i := range l.Events {
					if i > 0 {
						l.Events[i].PreviousChecksum = l.Events[i-1].Checksum
					}
					l.Events[i].seal()
				}
			})
			check("reservation-intent-digest-replaced", func(r *Reservation, l *ExecutionLifecycle) {
				r.IntentDigest = "fake-other-intent"
				r.seal()
				l.Root.ReservationChecksum = r.Checksum
				l.Root.IntentDigest = r.IntentDigest
				l.Root.seal()
				l.Events[0].PreviousChecksum = l.Root.Checksum
				for i := range l.Events {
					if i > 0 {
						l.Events[i].PreviousChecksum = l.Events[i-1].Checksum
					}
					l.Events[i].seal()
				}
			})
		})
	}
}
