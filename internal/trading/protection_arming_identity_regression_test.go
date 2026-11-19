package trading

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"binance_trader/internal/exchange"
)

// A damaged but recomputed marker checksum must not allow two independently
// planned legs to alias the same exchange Algo order during restart.
func TestRecoverArmingRejectsDuplicateDurableAlgoIdentity(t *testing.T) {
	store, plan, _, _ := testProtectionGroup(t, Long)
	owner := OwnerPosition{Source: Manual, Symbol: "BTCUSDT", Side: Long, Quantity: "0.01", AverageEntry: "100000"}
	mock := &armingMock{owner: owner, plan: plan, algos: map[int64]exchange.DemoAlgoResult{}}
	armed, err := armWithJSONSnapshot(context.Background(), plan, owner, store, mock, false)
	if err != nil || !armed.Armed {
		t.Fatalf("fixture arming failed: %+v %v", armed, err)
	}

	_, path, err := store.legPaths(plan.GroupID, "sl")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var marker observedProtectionMarker
	if err := json.Unmarshal(raw, &marker); err != nil {
		t.Fatal(err)
	}
	marker.AlgoID = armed.TPAlgoID
	marker.Checksum = marker.digest()
	raw, err = json.Marshal(marker)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}

	// The coordinator owns only this temporary test fixture.
	coord := testRecoveryCoordinator(t, store)
	res, err := RecoverDemoProtectionArming(context.Background(), plan.GroupID, owner, coord, store,
		&armingTransportAdapter{m: mock}, DemoCredentials{"k", "s"})
	if err == nil || res.Armed || !res.ManualReviewRequired || mock.submitCalls != 2 {
		t.Fatalf("duplicate exchange Algo ID must fail-closed: %+v err=%v posts=%d", res, err, mock.submitCalls)
	}
}
