package trading

import (
	"context"
	"testing"

	"binance_trader/internal/exchange"
)

func TestProtectionArmingCheckpointNoPostNoAuthorization(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			store, plan, _, _ := testProtectionGroup(t, side)
			check := func(want string) {
				t.Helper()
				r, err := InspectProtectionArmingCheckpoint(store, plan.GroupID)
				if err != nil || r.State != want || r.TradingAuthorized || r.ProtectionSubmitAuthorized || r.RetryAuthorized || !r.ManualReviewRequired {
					t.Fatalf("state=%+v err=%v want=%s", r, err, want)
				}
			}
			check("PREPARED_NEVER_DISPATCHED")
			if err := store.MarkMayHaveSent(plan.GroupID, "tp", true); err != nil {
				t.Fatal(err)
			}
			check("TP_MAY_HAVE_SENT_UNRESOLVED")
			owner := OwnerPosition{Source: Manual, Symbol: "BTCUSDT", Side: side, Quantity: "0.01", AverageEntry: "100000"}
			mock := &armingMock{owner: owner, plan: plan, algos: map[int64]exchange.DemoAlgoResult{}}
			mock.algos[901] = algoFromInstruction(901, plan.TP, "NEW")
			if err := store.RecordObserved(context.Background(), plan.GroupID, "tp", 901, mock, "k", "s"); err != nil {
				t.Fatal(err)
			}
			check("TP_OBSERVED_SL_NOT_DISPATCHED")
			if err := store.MarkMayHaveSent(plan.GroupID, "sl", true); err != nil {
				t.Fatal(err)
			}
			check("SL_MAY_HAVE_SENT_UNRESOLVED")
			mock.algos[902] = algoFromInstruction(902, plan.SL, "NEW")
			if err := store.RecordObserved(context.Background(), plan.GroupID, "sl", 902, mock, "k", "s"); err != nil {
				t.Fatal(err)
			}
			check("TWO_LEGS_PREVIOUSLY_OBSERVED_RECHECK_REQUIRED")
		})
	}
}

func TestProtectionArmingCheckpointCorruptOrMissingFailClosed(t *testing.T) {
	store, plan, _, _ := testProtectionGroup(t, Long)
	if _, err := InspectProtectionArmingCheckpoint(store, "no-such-group"); err == nil {
		t.Fatal("missing plan accepted")
	}
	if err := store.MarkMayHaveSent(plan.GroupID, "tp", true); err != nil {
		t.Fatal(err)
	}
	_, observed, err := store.legPaths(plan.GroupID, "sl")
	if err != nil {
		t.Fatal(err)
	}
	// A durable orphan marker is invalid even if it parses.
	if err := exclusiveDurable(observed, []byte(`{"unexpected":true}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := InspectProtectionArmingCheckpoint(store, plan.GroupID); err == nil {
		t.Fatal("orphan marker accepted")
	}
}
